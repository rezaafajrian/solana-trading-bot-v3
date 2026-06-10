import { Filter, FilterResult } from './pool-filters';
import { Connection } from '@solana/web3.js';
import { LIQUIDITY_STATE_LAYOUT_V4, LiquidityPoolKeysV4 } from '@raydium-io/raydium-sdk';
import BN from 'bn.js';
import { logger } from '../helpers';

/**
 * Checks how much of the LP supply has been burned or locked.
 *
 * `lpReserve` in the pool state is the total LP minted at creation. Any LP that
 * is no longer circulating (current supply is lower) has been burned/locked and
 * can no longer be pulled by the creator. This is a more precise version of the
 * binary "is LP burned" check.
 */
export class LiquidityLockedFilter implements Filter {
  constructor(
    private readonly connection: Connection,
    private readonly minLockedPercentage: number,
  ) {}

  async execute(poolKeys: LiquidityPoolKeysV4): Promise<FilterResult> {
    try {
      const [poolAccount, lpSupply] = await Promise.all([
        this.connection.getAccountInfo(poolKeys.id, this.connection.commitment),
        this.connection.getTokenSupply(poolKeys.lpMint, this.connection.commitment),
      ]);

      if (!poolAccount) {
        return { ok: false, message: 'LiquidityLocked -> Failed to fetch pool account' };
      }

      const poolState = LIQUIDITY_STATE_LAYOUT_V4.decode(poolAccount.data);
      const lpReserve = new BN(poolState.lpReserve.toString());
      const actualSupply = new BN(lpSupply.value.amount);

      if (lpReserve.isZero()) {
        return { ok: false, message: 'LiquidityLocked -> Unknown LP reserve' };
      }

      // Burned/locked LP = minted LP that is no longer circulating.
      const burned = lpReserve.sub(actualSupply);
      const lockedPercentage = burned.muln(10000).div(lpReserve).toNumber() / 100;
      const clamped = Math.max(0, Math.min(100, lockedPercentage));

      if (clamped < this.minLockedPercentage) {
        return {
          ok: false,
          message: `LiquidityLocked -> Only ${clamped.toFixed(2)}% of LP burned/locked (min ${this.minLockedPercentage}%)`,
        };
      }

      return { ok: true };
    } catch (e) {
      logger.error({ mint: poolKeys.baseMint }, `LiquidityLocked -> Failed to check LP lock`);
    }

    return { ok: false, message: 'LiquidityLocked -> Failed to check LP lock' };
  }
}
