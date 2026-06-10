import {
  ComputeBudgetProgram,
  Connection,
  Keypair,
  PublicKey,
  SystemProgram,
  TransactionMessage,
  VersionedTransaction,
} from '@solana/web3.js';
import {
  createAssociatedTokenAccountIdempotentInstruction,
  createCloseAccountInstruction,
  createSyncNativeInstruction,
  getAccount,
  getAssociatedTokenAddress,
  NATIVE_MINT,
  RawAccount,
  TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import { Liquidity, LiquidityPoolKeysV4, LiquidityStateV4, Percent, Token, TokenAmount } from '@raydium-io/raydium-sdk';
import { MarketCache, PoolCache, SnipeListCache } from './cache';
import { PoolFilters } from './filters';
import { TransactionExecutor } from './transactions';
import { createPoolKeys, logger, NETWORK, sleep } from './helpers';
import { Mutex } from 'async-mutex';
import BN from 'bn.js';
import { WarpTransactionExecutor } from './transactions/warp-transaction-executor';
import { JitoTransactionExecutor } from './transactions/jito-rpc-transaction-executor';

export interface BotConfig {
  wallet: Keypair;
  checkRenounced: boolean;
  checkFreezable: boolean;
  checkBurned: boolean;
  minPoolSize: TokenAmount;
  maxPoolSize: TokenAmount;
  quoteToken: Token;
  quoteAmount: TokenAmount;
  quoteAta: PublicKey;
  // When true the quote is native SOL: the WSOL account is wrapped on buy and
  // unwrapped (closed) on sell, so the wallet only needs to hold native SOL.
  wrapSol: boolean;
  oneTokenAtATime: boolean;
  useSnipeList: boolean;
  autoSell: boolean;
  autoBuyDelay: number;
  autoSellDelay: number;
  maxBuyRetries: number;
  maxSellRetries: number;
  unitLimit: number;
  unitPrice: number;
  dynamicUnitPrice: boolean;
  maxUnitPrice: number;
  simulateSellBeforeBuy: boolean;
  takeProfit: number;
  stopLoss: number;
  trailingStopLoss: number;
  partialTakeProfit: number;
  partialSellPercent: number;
  buySlippage: number;
  sellSlippage: number;
  priceCheckInterval: number;
  priceCheckDuration: number;
  filterCheckInterval: number;
  filterCheckDuration: number;
  consecutiveMatchCount: number;
}

export class Bot {
  private readonly poolFilters: PoolFilters;

  // snipe list
  private readonly snipeListCache?: SnipeListCache;

  // one token at the time
  private readonly mutex: Mutex;
  private sellExecutionCount = 0;

  // guards against the wallet listener re-entering sell() for the same mint
  // while a (possibly multi-stage) sell is already in progress
  private readonly sellingMints = new Set<string>();
  public readonly isWarp: boolean = false;
  public readonly isJito: boolean = false;

  constructor(
    private readonly connection: Connection,
    private readonly marketStorage: MarketCache,
    private readonly poolStorage: PoolCache,
    private readonly txExecutor: TransactionExecutor,
    readonly config: BotConfig,
  ) {
    this.isWarp = txExecutor instanceof WarpTransactionExecutor;
    this.isJito = txExecutor instanceof JitoTransactionExecutor;

    this.mutex = new Mutex();
    this.poolFilters = new PoolFilters(connection, {
      quoteToken: this.config.quoteToken,
      minPoolSize: this.config.minPoolSize,
      maxPoolSize: this.config.maxPoolSize,
    });

    if (this.config.useSnipeList) {
      this.snipeListCache = new SnipeListCache();
      this.snipeListCache.init();
    }
  }

  async validate() {
    // When wrapping native SOL the WSOL account is created per trade, so it does
    // not need to exist up front — just make sure the wallet holds some SOL.
    if (this.config.wrapSol) {
      try {
        const balance = await this.connection.getBalance(this.config.wallet.publicKey, this.connection.commitment);

        if (balance <= 0) {
          logger.error(`Wallet has no SOL balance: ${this.config.wallet.publicKey.toString()}`);
          return false;
        }
      } catch (error) {
        logger.error(`Failed to fetch SOL balance for wallet: ${this.config.wallet.publicKey.toString()}`);
        return false;
      }

      return true;
    }

    try {
      await getAccount(this.connection, this.config.quoteAta, this.connection.commitment);
    } catch (error) {
      logger.error(
        `${this.config.quoteToken.symbol} token account not found in wallet: ${this.config.wallet.publicKey.toString()}`,
      );
      return false;
    }

    return true;
  }

  public async buy(accountId: PublicKey, poolState: LiquidityStateV4) {
    logger.trace({ mint: poolState.baseMint }, `Processing new pool...`);

    if (this.config.useSnipeList && !this.snipeListCache?.isInList(poolState.baseMint.toString())) {
      logger.debug({ mint: poolState.baseMint.toString() }, `Skipping buy because token is not in a snipe list`);
      return;
    }

    if (this.config.autoBuyDelay > 0) {
      logger.debug({ mint: poolState.baseMint }, `Waiting for ${this.config.autoBuyDelay} ms before buy`);
      await sleep(this.config.autoBuyDelay);
    }

    if (this.config.oneTokenAtATime) {
      if (this.mutex.isLocked() || this.sellExecutionCount > 0) {
        logger.debug(
          { mint: poolState.baseMint.toString() },
          `Skipping buy because one token at a time is turned on and token is already being processed`,
        );
        return;
      }

      await this.mutex.acquire();
    }

    try {
      const [market, mintAta] = await Promise.all([
        this.marketStorage.get(poolState.marketId.toString()),
        getAssociatedTokenAddress(poolState.baseMint, this.config.wallet.publicKey),
      ]);
      const poolKeys: LiquidityPoolKeysV4 = createPoolKeys(accountId, poolState, market);

      if (!this.config.useSnipeList) {
        const match = await this.filterMatch(poolKeys);

        if (!match) {
          logger.trace({ mint: poolKeys.baseMint.toString() }, `Skipping buy because pool doesn't match filters`);
          return;
        }
      }

      if (this.config.simulateSellBeforeBuy) {
        const sellable = await this.checkSellable(poolKeys, mintAta);

        if (!sellable) {
          logger.trace(
            { mint: poolKeys.baseMint.toString() },
            `Skipping buy because the sell simulation failed (likely honeypot)`,
          );
          return;
        }
      }

      for (let i = 0; i < this.config.maxBuyRetries; i++) {
        try {
          logger.info(
            { mint: poolState.baseMint.toString() },
            `Send buy transaction attempt: ${i + 1}/${this.config.maxBuyRetries}`,
          );
          const tokenOut = new Token(TOKEN_PROGRAM_ID, poolKeys.baseMint, poolKeys.baseDecimals);
          const result = await this.swap(
            poolKeys,
            this.config.quoteAta,
            mintAta,
            this.config.quoteToken,
            tokenOut,
            this.config.quoteAmount,
            this.config.buySlippage,
            this.config.wallet,
            'buy',
          );

          if (result.confirmed) {
            logger.info(
              {
                mint: poolState.baseMint.toString(),
                signature: result.signature,
                url: `https://solscan.io/tx/${result.signature}?cluster=${NETWORK}`,
              },
              `Confirmed buy tx`,
            );

            break;
          }

          logger.info(
            {
              mint: poolState.baseMint.toString(),
              signature: result.signature,
              error: result.error,
            },
            `Error confirming buy tx`,
          );
        } catch (error) {
          logger.debug({ mint: poolState.baseMint.toString(), error }, `Error confirming buy transaction`);
        }
      }
    } catch (error) {
      logger.error({ mint: poolState.baseMint.toString(), error }, `Failed to buy token`);
    } finally {
      if (this.config.oneTokenAtATime) {
        this.mutex.release();
      }
    }
  }

  public async sell(accountId: PublicKey, rawAccount: RawAccount) {
    const mint = rawAccount.mint.toString();

    // A partial take-profit changes the token balance, which makes the wallet
    // listener fire sell() again for the same mint. Skip those re-entrant calls.
    if (this.sellingMints.has(mint)) {
      return;
    }

    if (this.config.oneTokenAtATime) {
      this.sellExecutionCount++;
    }

    this.sellingMints.add(mint);

    try {
      logger.trace({ mint: rawAccount.mint }, `Processing new token...`);

      const poolData = await this.poolStorage.get(mint);

      if (!poolData) {
        logger.trace({ mint }, `Token pool data is not found, can't sell`);
        return;
      }

      const tokenIn = new Token(TOKEN_PROGRAM_ID, poolData.state.baseMint, poolData.state.baseDecimal.toNumber());
      const tokenAmountIn = new TokenAmount(tokenIn, rawAccount.amount, true);

      if (tokenAmountIn.isZero()) {
        logger.info({ mint }, `Empty balance, can't sell`);
        return;
      }

      if (this.config.autoSellDelay > 0) {
        logger.debug({ mint: rawAccount.mint }, `Waiting for ${this.config.autoSellDelay} ms before sell`);
        await sleep(this.config.autoSellDelay);
      }

      const market = await this.marketStorage.get(poolData.state.marketId.toString());
      const poolKeys: LiquidityPoolKeysV4 = createPoolKeys(new PublicKey(poolData.id), poolData.state, market);

      // When price checks are disabled, sell the whole balance immediately.
      if (this.config.priceCheckDuration === 0 || this.config.priceCheckInterval === 0) {
        await this.executeSell(accountId, poolKeys, tokenIn, tokenAmountIn, true);
        return;
      }

      // Otherwise monitor price and exit using take profit, stop loss, an optional
      // partial take-profit, an optional trailing stop, and the timed fallback.
      let remaining = tokenAmountIn;
      let partialTaken = false;

      const timesToCheck = this.config.priceCheckDuration / this.config.priceCheckInterval;
      const slippage = new Percent(this.config.sellSlippage, 100);
      const takeProfit = this.percentOf(this.config.quoteAmount, this.config.takeProfit, 'add');
      const stopLoss = this.percentOf(this.config.quoteAmount, this.config.stopLoss, 'subtract');
      const partialTarget =
        this.config.partialTakeProfit > 0
          ? this.percentOf(this.config.quoteAmount, this.config.partialTakeProfit, 'add')
          : undefined;

      let peak: TokenAmount | undefined;
      let timesChecked = 0;
      let exited = false;

      do {
        try {
          const poolInfo = await Liquidity.fetchInfo({ connection: this.connection, poolKeys });

          // Use the full original position as the price signal so the PnL
          // comparison against the cost basis stays consistent after a partial sell.
          const amountOut = Liquidity.computeAmountOut({
            poolKeys,
            poolInfo,
            amountIn: tokenAmountIn,
            currencyOut: this.config.quoteToken,
            slippage,
          }).amountOut as TokenAmount;

          if (!peak || amountOut.gt(peak)) {
            peak = amountOut;
          }

          const trailingStop =
            this.config.trailingStopLoss > 0 && peak.gt(this.config.quoteAmount)
              ? this.percentOf(peak, this.config.trailingStopLoss, 'subtract')
              : undefined;

          logger.debug(
            { mint: poolKeys.baseMint.toString() },
            `Take profit: ${takeProfit.toFixed()} | Stop loss: ${stopLoss.toFixed()} | ` +
              `${trailingStop ? `Trailing: ${trailingStop.toFixed()} | ` : ''}Current: ${amountOut.toFixed()}`,
          );

          if (amountOut.lt(stopLoss) || amountOut.gt(takeProfit) || (trailingStop && amountOut.lt(trailingStop))) {
            await this.executeSell(accountId, poolKeys, tokenIn, remaining, true);
            exited = true;
            break;
          }

          if (!partialTaken && partialTarget && this.config.partialSellPercent > 0 && amountOut.gt(partialTarget)) {
            const portion = new TokenAmount(tokenIn, remaining.raw.muln(this.config.partialSellPercent).divn(100), true);

            if (!portion.isZero()) {
              logger.info(
                { mint: poolKeys.baseMint.toString() },
                `Partial take profit hit, selling ${this.config.partialSellPercent}% and trailing the rest`,
              );

              const sold = await this.executeSell(accountId, poolKeys, tokenIn, portion, false);

              if (sold) {
                remaining = new TokenAmount(tokenIn, remaining.raw.sub(portion.raw), true);
                partialTaken = true;
              }
            }
          }

          await sleep(this.config.priceCheckInterval);
        } catch (e) {
          logger.trace({ mint: poolKeys.baseMint.toString(), e }, `Failed to check token price`);
        } finally {
          timesChecked++;
        }
      } while (timesChecked < timesToCheck);

      // Timed exit: never hit a target, so sell whatever is left and close the account.
      if (!exited && !remaining.isZero()) {
        await this.executeSell(accountId, poolKeys, tokenIn, remaining, true);
      }
    } catch (error) {
      logger.error({ mint, error }, `Failed to sell token`);
    } finally {
      this.sellingMints.delete(mint);
      if (this.config.oneTokenAtATime) {
        this.sellExecutionCount--;
      }
    }
  }

  private async executeSell(
    accountId: PublicKey,
    poolKeys: LiquidityPoolKeysV4,
    tokenIn: Token,
    amountIn: TokenAmount,
    closeAccount: boolean,
  ): Promise<boolean> {
    const mint = tokenIn.mint.toString();

    for (let i = 0; i < this.config.maxSellRetries; i++) {
      try {
        logger.info({ mint }, `Send sell transaction attempt: ${i + 1}/${this.config.maxSellRetries}`);

        const result = await this.swap(
          poolKeys,
          accountId,
          this.config.quoteAta,
          tokenIn,
          this.config.quoteToken,
          amountIn,
          this.config.sellSlippage,
          this.config.wallet,
          'sell',
          closeAccount,
        );

        if (result.confirmed) {
          logger.info(
            {
              dex: `https://dexscreener.com/solana/${mint}?maker=${this.config.wallet.publicKey}`,
              mint,
              signature: result.signature,
              url: `https://solscan.io/tx/${result.signature}?cluster=${NETWORK}`,
            },
            `Confirmed sell tx`,
          );
          return true;
        }

        logger.info({ mint, signature: result.signature, error: result.error }, `Error confirming sell tx`);
      } catch (error) {
        logger.debug({ mint, error }, `Error confirming sell transaction`);
      }
    }

    return false;
  }

  // Returns `amount` increased or decreased by `percent` percent.
  private percentOf(amount: TokenAmount, percent: number, op: 'add' | 'subtract'): TokenAmount {
    const fraction = amount.mul(percent).numerator.div(new BN(100));
    const delta = new TokenAmount(this.config.quoteToken, fraction, true);
    return op === 'add' ? amount.add(delta) : amount.subtract(delta);
  }

  // noinspection JSUnusedLocalSymbols
  private async swap(
    poolKeys: LiquidityPoolKeysV4,
    ataIn: PublicKey,
    ataOut: PublicKey,
    tokenIn: Token,
    tokenOut: Token,
    amountIn: TokenAmount,
    slippage: number,
    wallet: Keypair,
    direction: 'buy' | 'sell',
    closeAccount: boolean = true,
  ) {
    const slippagePercent = new Percent(slippage, 100);
    const poolInfo = await Liquidity.fetchInfo({
      connection: this.connection,
      poolKeys,
    });

    const computedAmountOut = Liquidity.computeAmountOut({
      poolKeys,
      poolInfo,
      amountIn,
      currencyOut: tokenOut,
      slippage: slippagePercent,
    });

    const useComputeBudget = !this.isWarp && !this.isJito;
    const unitPrice =
      useComputeBudget && this.config.dynamicUnitPrice
        ? await this.getDynamicUnitPrice(poolKeys)
        : this.config.unitPrice;

    const latestBlockhash = await this.connection.getLatestBlockhash();
    const { innerTransaction } = Liquidity.makeSwapFixedInInstruction(
      {
        poolKeys: poolKeys,
        userKeys: {
          tokenAccountIn: ataIn,
          tokenAccountOut: ataOut,
          owner: wallet.publicKey,
        },
        amountIn: amountIn.raw,
        minAmountOut: computedAmountOut.minAmountOut.raw,
      },
      poolKeys.version,
    );

    // `quoteAta` is the WSOL account. On buy it is the input, on sell the output.
    const wrapSol = this.config.wrapSol;
    const quoteAta = this.config.quoteAta;
    // Unwrap (close the WSOL account back to native SOL) after a full trade, never
    // after a partial sell (proceeds stay wrapped until the final sell closes it).
    const unwrapSol = wrapSol && (direction === 'buy' || (direction === 'sell' && closeAccount));

    const messageV0 = new TransactionMessage({
      payerKey: wallet.publicKey,
      recentBlockhash: latestBlockhash.blockhash,
      instructions: [
        ...(!useComputeBudget
          ? []
          : [
              ComputeBudgetProgram.setComputeUnitPrice({ microLamports: unitPrice }),
              ComputeBudgetProgram.setComputeUnitLimit({ units: this.config.unitLimit }),
            ]),
        ...(direction === 'buy'
          ? [
              createAssociatedTokenAccountIdempotentInstruction(
                wallet.publicKey,
                ataOut,
                wallet.publicKey,
                tokenOut.mint,
              ),
            ]
          : []),
        // Native SOL: make sure the WSOL account exists, and on buy wrap the input amount into it.
        ...(wrapSol
          ? [
              createAssociatedTokenAccountIdempotentInstruction(wallet.publicKey, quoteAta, wallet.publicKey, NATIVE_MINT),
              ...(direction === 'buy'
                ? [
                    SystemProgram.transfer({
                      fromPubkey: wallet.publicKey,
                      toPubkey: quoteAta,
                      lamports: BigInt(amountIn.raw.toString()),
                    }),
                    createSyncNativeInstruction(quoteAta),
                  ]
                : []),
            ]
          : []),
        ...innerTransaction.instructions,
        ...(direction === 'sell' && closeAccount
          ? [createCloseAccountInstruction(ataIn, wallet.publicKey, wallet.publicKey)]
          : []),
        // Native SOL: unwrap by closing the WSOL account, returning lamports to the wallet.
        ...(unwrapSol ? [createCloseAccountInstruction(quoteAta, wallet.publicKey, wallet.publicKey)] : []),
      ],
    }).compileToV0Message();

    const transaction = new VersionedTransaction(messageV0);
    transaction.sign([wallet, ...innerTransaction.signers]);

    return this.txExecutor.executeAndConfirm(transaction, wallet, latestBlockhash);
  }

  // Picks a compute unit price based on recent network congestion for the pool's
  // accounts. Clamped between the configured floor (unitPrice) and maxUnitPrice.
  private async getDynamicUnitPrice(poolKeys: LiquidityPoolKeysV4): Promise<number> {
    try {
      const fees = await this.connection.getRecentPrioritizationFees({
        lockedWritableAccounts: [poolKeys.id, poolKeys.baseVault, poolKeys.quoteVault],
      });

      const nonZero = fees
        .map((f) => f.prioritizationFee)
        .filter((f) => f > 0)
        .sort((a, b) => a - b);

      if (nonZero.length === 0) {
        return this.config.unitPrice;
      }

      // 75th percentile, bumped 25% to stay ahead of the pack.
      const index = Math.min(Math.floor(nonZero.length * 0.75), nonZero.length - 1);
      const target = Math.ceil(nonZero[index] * 1.25);
      const price = Math.min(Math.max(target, this.config.unitPrice), this.config.maxUnitPrice);

      logger.trace({ mint: poolKeys.baseMint.toString() }, `Dynamic compute unit price: ${price} micro lamports`);
      return price;
    } catch (e) {
      logger.trace({ mint: poolKeys.baseMint.toString(), e }, `Failed to fetch dynamic priority fee, using static`);
      return this.config.unitPrice;
    }
  }

  // Simulates a buy->sell round trip on-chain before buying. If the sell leg fails,
  // the token is almost certainly a honeypot, so we skip it. If the simulation
  // itself cannot run (RPC issue), we proceed rather than miss every trade.
  private async checkSellable(poolKeys: LiquidityPoolKeysV4, tokenAta: PublicKey): Promise<boolean> {
    try {
      const tokenMint = new Token(TOKEN_PROGRAM_ID, poolKeys.baseMint, poolKeys.baseDecimals);
      const slippage = new Percent(this.config.buySlippage, 100);
      const poolInfo = await Liquidity.fetchInfo({ connection: this.connection, poolKeys });

      const buyOut = Liquidity.computeAmountOut({
        poolKeys,
        poolInfo,
        amountIn: this.config.quoteAmount,
        currencyOut: tokenMint,
        slippage,
      });

      // We are guaranteed to receive at least minAmountOut, so use that as the sell input.
      const sellAmount = new TokenAmount(tokenMint, buyOut.minAmountOut.raw, true);

      const { innerTransaction: buyIx } = Liquidity.makeSwapFixedInInstruction(
        {
          poolKeys,
          userKeys: { tokenAccountIn: this.config.quoteAta, tokenAccountOut: tokenAta, owner: this.config.wallet.publicKey },
          amountIn: this.config.quoteAmount.raw,
          minAmountOut: buyOut.minAmountOut.raw,
        },
        poolKeys.version,
      );

      const { innerTransaction: sellIx } = Liquidity.makeSwapFixedInInstruction(
        {
          poolKeys,
          userKeys: { tokenAccountIn: tokenAta, tokenAccountOut: this.config.quoteAta, owner: this.config.wallet.publicKey },
          amountIn: sellAmount.raw,
          minAmountOut: new BN(0),
        },
        poolKeys.version,
      );

      const latestBlockhash = await this.connection.getLatestBlockhash();
      const messageV0 = new TransactionMessage({
        payerKey: this.config.wallet.publicKey,
        recentBlockhash: latestBlockhash.blockhash,
        instructions: [
          ComputeBudgetProgram.setComputeUnitLimit({ units: 1_400_000 }),
          createAssociatedTokenAccountIdempotentInstruction(
            this.config.wallet.publicKey,
            tokenAta,
            this.config.wallet.publicKey,
            tokenMint.mint,
          ),
          // Native SOL: wrap the buy amount so the simulated buy has WSOL to spend.
          ...(this.config.wrapSol
            ? [
                createAssociatedTokenAccountIdempotentInstruction(
                  this.config.wallet.publicKey,
                  this.config.quoteAta,
                  this.config.wallet.publicKey,
                  NATIVE_MINT,
                ),
                SystemProgram.transfer({
                  fromPubkey: this.config.wallet.publicKey,
                  toPubkey: this.config.quoteAta,
                  lamports: BigInt(this.config.quoteAmount.raw.toString()),
                }),
                createSyncNativeInstruction(this.config.quoteAta),
              ]
            : []),
          ...buyIx.instructions,
          ...sellIx.instructions,
        ],
      }).compileToV0Message();

      const transaction = new VersionedTransaction(messageV0);
      transaction.sign([this.config.wallet, ...buyIx.signers, ...sellIx.signers]);

      const simulation = await this.connection.simulateTransaction(transaction, {
        sigVerify: false,
        replaceRecentBlockhash: true,
        commitment: this.connection.commitment,
      });

      if (simulation.value.err) {
        logger.debug(
          { mint: poolKeys.baseMint.toString(), err: simulation.value.err },
          `Sellability simulation failed -> likely honeypot`,
        );
        return false;
      }

      return true;
    } catch (e) {
      logger.trace({ mint: poolKeys.baseMint.toString(), e }, `Sellability simulation could not run, proceeding`);
      return true;
    }
  }

  private async filterMatch(poolKeys: LiquidityPoolKeysV4) {
    if (this.config.filterCheckInterval === 0 || this.config.filterCheckDuration === 0) {
      return true;
    }

    const timesToCheck = this.config.filterCheckDuration / this.config.filterCheckInterval;
    let timesChecked = 0;
    let matchCount = 0;

    do {
      try {
        const shouldBuy = await this.poolFilters.execute(poolKeys);

        if (shouldBuy) {
          matchCount++;

          if (this.config.consecutiveMatchCount <= matchCount) {
            logger.debug(
              { mint: poolKeys.baseMint.toString() },
              `Filter match ${matchCount}/${this.config.consecutiveMatchCount}`,
            );
            return true;
          }
        } else {
          matchCount = 0;
        }

        await sleep(this.config.filterCheckInterval);
      } finally {
        timesChecked++;
      }
    } while (timesChecked < timesToCheck);

    return false;
  }
}
