import { Filter, FilterResult } from './pool-filters';
import { Connection } from '@solana/web3.js';
import { LiquidityPoolKeysV4 } from '@raydium-io/raydium-sdk';
import BN from 'bn.js';
import { logger } from '../helpers';

/**
 * Skips pools where a single wallet (other than the pool itself) holds a large
 * share of the supply. A concentrated holder is the classic setup for a dump.
 *
 * The pool's own base vault holds most of the supply at launch, so it is
 * excluded from the calculation.
 */
export class TopHoldersFilter implements Filter {
  constructor(
    private readonly connection: Connection,
    private readonly maxPercentage: number,
  ) {}

  async execute(poolKeys: LiquidityPoolKeysV4): Promise<FilterResult> {
    try {
      const [supply, largestAccounts] = await Promise.all([
        this.connection.getTokenSupply(poolKeys.baseMint, this.connection.commitment),
        this.connection.getTokenLargestAccounts(poolKeys.baseMint, this.connection.commitment),
      ]);

      const totalSupply = new BN(supply.value.amount);

      if (totalSupply.isZero()) {
        return { ok: false, message: 'TopHolders -> Token supply is zero' };
      }

      let maxHolderPercentage = 0;

      for (const account of largestAccounts.value) {
        // The pool's own liquidity vault is expected to hold most of the supply.
        if (account.address.equals(poolKeys.baseVault)) {
          continue;
        }

        const amount = new BN(account.amount);
        const percentage = amount.muln(10000).div(totalSupply).toNumber() / 100;

        if (percentage > maxHolderPercentage) {
          maxHolderPercentage = percentage;
        }
      }

      if (maxHolderPercentage > this.maxPercentage) {
        return {
          ok: false,
          message: `TopHolders -> A wallet holds ${maxHolderPercentage.toFixed(2)}% of supply (max ${this.maxPercentage}%)`,
        };
      }

      return { ok: true };
    } catch (e) {
      logger.error({ mint: poolKeys.baseMint }, `TopHolders -> Failed to check holder concentration`);
    }

    return { ok: false, message: 'TopHolders -> Failed to check holder concentration' };
  }
}
