/** Transaction-fee-aware net stats for a backtest result.
 *
 * Inputs: the period chain from a backtest run plus the per-exchange
 * one-way fee table the user configured on /fees. Output: a `NetStats`
 * struct holding the same headline figures the gross summary surfaces
 * (total return, annualized, max DD, Sharpe), plus a yearly breakdown
 * map and the underlying net period chain so callers can recompute
 * custom-range returns without duplicating the model.
 *
 * Trade-aware model:
 *   - A holding pays the buy fee only on the period it first appears in
 *     (vs the previous period's holdings).
 *   - It pays the sell fee only on the period after which it disappears
 *     from the portfolio.
 *   - Rollovers (held in N-1, N, N+1) pay nothing in period N.
 *   - The trailing open period never charges sell fee — those positions
 *     haven't actually been sold.
 *   - First period: all holdings are "new entrants" → pay buy fee.
 *   - Last CLOSED period: all holdings are "departing" → pay sell fee.
 *
 * Per holding the net forward return becomes:
 *     (exit * (1 - sell_f)) / (entry * (1 + buy_f)) - 1
 * with buy_f / sell_f either the exchange's f or zero per the rules above.
 *
 * Portfolio net period return is the equal-weighted mean of per-holding
 * net returns; long-short books take mean(long) − mean(short).
 *
 * Returns `null` when no fees are configured (every entry in the fee map
 * is 0 or missing) so callers can skip rendering the parenthetical.
 */
import type { DailyRecord, Holding, PeriodRecord } from '../../../lib/stores/momentum';

export type NetStats = {
  total_return_pct: number;
  annualized_return_pct: number;
  max_drawdown_pct: number;
  sharpe_ratio: number | null;
  /** YYYY-MM or YYYY-MM-DD strings per closed period in order. */
  dates: string[];
  /** Per-period net portfolio returns in pct (e.g. 1.23 = +1.23%). */
  period_returns: number[];
  /** Cumulative growth factor at the end of each period. Parallel to `dates`. */
  cum_factors: number[];
  /** Per-year compound returns derived from the net period chain.
   *
   * NOTE: bucketed by period START date — for sub-monthly cadences or
   * monthly rebalances that don't land on the 1st, this span drifts a
   * few days off from the calendar-year baseline the gross yearly uses
   * (Jan 1 → Dec 31 via prevCum on the gross daily curve). When the
   * displayed gross is calendar-aligned, prefer `period_drag_factors`
   * over this field: callers can derive a calendar-aligned net yearly
   * as `(1 + gross_yearly_Y) * prod(fee_factor where exit_date in Y) - 1`
   * which is ≤ gross_yearly_Y by construction. Kept for back-compat
   * with the custom-range walker that needs net cum_factors. */
  yearly: Record<string, number>;
  /** Per closed period: the exit date (when the sell fee is paid) and
   * the period's fee-drag factor `(1 + net_return) / (1 + gross_return)`.
   * Guaranteed ≤ 1.0 per period because net ≤ gross per holding and the
   * mean preserves the inequality. EquityCurveCard uses this to build a
   * calendar-aligned net yearly that anchors to its gross yearly. */
  period_drag_factors: Array<{ exit_date: string; fee_factor: number }>;
};

/** Per-period fee math for one holding. Returns the holding's net
 * forward-return in pct, or null when the inputs are degenerate
 * (missing prices). Open-period sell fee is zeroed by the caller.
 *
 * Rounded to 2 decimals to match the backend's per-holding rounding
 * in `make_period_holding` (`round((exit / entry - 1) * 100, 2)`).
 * Without the rounding, a frontend-computed gross (fees=0) would
 * differ from backend.gross by ~0.005% on average — small but enough
 * to let `displayed_net > displayed_gross` slip through on a
 * low-turnover US-only backtest where fee impact is also tiny. */
function netForwardReturn(
  h: Holding,
  buyFee: number,
  sellFee: number,
): number | null {
  const entry = h.entry_price_eur;
  const exit = h.exit_price_eur;
  if (entry == null || exit == null || !(entry > 0)) return null;
  const grossRatio = (exit * (1 - sellFee)) / (entry * (1 + buyFee));
  return Math.round((grossRatio - 1) * 100 * 100) / 100;
}

/** Equal-weighted aggregation of per-holding net returns into a single
 * portfolio period return. Mirrors the backend's long-short math:
 *   long_only:  mean(long_returns)
 *   long_short: mean(long_returns) − mean(short_returns)
 * Either side coming back empty falls back to whichever side has data
 * (treated as a one-sided period), same as the backend's behavior. */
function aggregatePortfolio(
  longReturns: number[],
  shortReturns: number[],
  hasShortLeg: boolean,
): number | null {
  // Final per-period port_return is rounded to 2 decimals to match
  // backend's `round(float(np.mean(...)), 2)`. This and the per-holding
  // rounding in netForwardReturn together ensure that with fees=0 our
  // chain produces the same numbers backend's summary would — which
  // is the only way to guarantee `displayed_net ≤ displayed_gross`
  // when fees are non-zero.
  const round2 = (v: number) => Math.round(v * 100) / 100;
  const longMean = longReturns.length > 0
    ? round2(longReturns.reduce((a, b) => a + b, 0) / longReturns.length)
    : null;
  const shortMean = shortReturns.length > 0
    ? round2(shortReturns.reduce((a, b) => a + b, 0) / shortReturns.length)
    : null;
  if (!hasShortLeg) return longMean;
  if (longMean != null && shortMean != null) return round2(longMean - shortMean);
  if (longMean != null) return longMean;
  if (shortMean != null) return round2(-shortMean);
  return null;
}

/** Periods/year inferred from the closed-period date span. Matches the
 * approach in `EquityCurveCard.alignedSeries` so cadence is detected the
 * same way for the parenthetical figure and the chart's points-derived
 * fallback. Falls back to 12 (monthly) when fewer than two dated periods
 * are available — too few points to compute anything meaningful anyway. */
function periodsPerYear(dates: string[]): number {
  if (dates.length < 2) return 12;
  const first = new Date(dates[0]);
  const last = new Date(dates[dates.length - 1]);
  const years = (last.getTime() - first.getTime()) / (365.25 * 86400 * 1000);
  if (!(years > 0)) return 12;
  return dates.length / years;
}

/** Format a "(net X%)" parenthetical given a net stat. Returns empty
 * string when `net` is null so the caller can concatenate
 * `"${gross}${parenStat(net)}"` without conditionals. */
export function parenPct(value: number | null | undefined, decimals = 2): string {
  if (value == null || !Number.isFinite(value)) return '';
  return ` (${value >= 0 ? '+' : ''}${value.toFixed(decimals)}%)`;
}
