import { Filter, FilterResult } from './pool-filters';
import { Connection, PublicKey } from '@solana/web3.js';
import { LiquidityPoolKeysV4 } from '@raydium-io/raydium-sdk';
import {
  TOKEN_2022_PROGRAM_ID,
  unpackMint,
  getTransferFeeConfig,
  getTransferHook,
  getPermanentDelegate,
  getNonTransferable,
  getDefaultAccountState,
  AccountState,
} from '@solana/spl-token';
import { logger } from '../helpers';

/**
 * Detects tokens that are honeypots or that charge a high transfer tax.
 *
 * Classic SPL tokens (owned by the Token program) cannot carry these traps,
 * so they pass immediately. Token-2022 mints are inspected for the extensions
 * that let a creator block or seize your tokens, or skim a fee on every sell.
 */
export class TokenSecurityFilter implements Filter {
  constructor(
    private readonly connection: Connection,
    private readonly checkHoneypot: boolean,
    private readonly maxTaxBasisPoints: number,
  ) {}

  async execute(poolKeys: LiquidityPoolKeysV4): Promise<FilterResult> {
    try {
      const accountInfo = await this.connection.getAccountInfo(poolKeys.baseMint, this.connection.commitment);

      if (!accountInfo) {
        return { ok: false, message: 'TokenSecurity -> Failed to fetch mint account' };
      }

      // Token-2022 traps only exist on the Token-2022 program. Classic SPL tokens are safe here.
      if (!accountInfo.owner.equals(TOKEN_2022_PROGRAM_ID)) {
        return { ok: true };
      }

      const mint = unpackMint(poolKeys.baseMint, accountInfo, TOKEN_2022_PROGRAM_ID);
      const problems: string[] = [];

      if (this.checkHoneypot) {
        if (getNonTransferable(mint)) {
          problems.push('token is non-transferable (cannot be sold)');
        }

        const hook = getTransferHook(mint);
        if (hook && !hook.programId.equals(PublicKey.default)) {
          problems.push('token has a transfer hook (sells can be blocked)');
        }

        if (getPermanentDelegate(mint)) {
          problems.push('token has a permanent delegate (creator can seize your tokens)');
        }

        const defaultState = getDefaultAccountState(mint);
        if (defaultState && defaultState.state === AccountState.Frozen) {
          problems.push('accounts are frozen by default');
        }
      }

      const transferFee = getTransferFeeConfig(mint);
      if (transferFee) {
        const bps = Math.max(
          transferFee.newerTransferFee.transferFeeBasisPoints,
          transferFee.olderTransferFee.transferFeeBasisPoints,
        );

        if (bps > this.maxTaxBasisPoints) {
          problems.push(`transfer tax is ${(bps / 100).toFixed(2)}% (max allowed ${(this.maxTaxBasisPoints / 100).toFixed(2)}%)`);
        }
      }

      if (problems.length > 0) {
        return { ok: false, message: `TokenSecurity -> ${problems.join('; ')}` };
      }

      return { ok: true };
    } catch (e) {
      logger.error({ mint: poolKeys.baseMint }, `TokenSecurity -> Failed to check token security`);
    }

    return { ok: false, message: 'TokenSecurity -> Failed to check token security' };
  }
}
