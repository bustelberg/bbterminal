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
import type { Holding, PeriodRecord } from '../../../lib/stores/momentum';

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
  /** Per-year compound returns derived from the net period chain. */
  yearly: Record<string, number>;
};

/** Per-period fee math for one holding. Returns the holding's net
 * forward-return in pct, or null when the inputs are degenerate
 * (missing prices). Open-period sell fee is zeroed by the caller. */
function netForwardReturn(
  h: Holding,
  buyFee: number,
  sellFee: number,
): number | null {
  const entry = h.entry_price_eur;
  const exit = h.exit_price_eur;
  if (entry == null || exit == null || !(entry > 0)) return null;
  const grossRatio = (exit * (1 - sellFee)) / (entry * (1 + buyFee));
  return (grossRatio - 1) * 100;
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
  const longMean = longReturns.length > 0
    ? longReturns.reduce((a, b) => a + b, 0) / longReturns.length
    : null;
  const shortMean = shortReturns.length > 0
    ? shortReturns.reduce((a, b) => a + b, 0) / shortReturns.length
    : null;
  if (!hasShortLeg) return longMean;
  if (longMean != null && shortMean != null) return longMean - shortMean;
  if (longMean != null) return longMean;
  if (shortMean != null) return -shortMean;
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

/** Drives the trade-aware loop: walks `monthly_records` once, classifies
 * each holding as new/continuing/departing vs its period-N±1 neighbors,
 * and pipes the per-holding net forward-returns through portfolio
 * aggregation. Empty / open-period boundaries are honored. */
export function computeNetStats(
  monthlyRecords: PeriodRecord[] | undefined,
  feesByExchange: Map<string, number>,
  exchangeByCompany: Map<number, string>,
): NetStats | null {
  if (!monthlyRecords || monthlyRecords.length === 0) return null;

  // Skip the entire computation when fees are zero across the board —
  // net == gross by definition. Caller uses null to mean "don't render
  // the parens".
  let anyFee = false;
  for (const v of feesByExchange.values()) {
    if (v > 0) { anyFee = true; break; }
  }
  if (!anyFee) return null;

  const feeFor = (cid: number): number => {
    const exch = exchangeByCompany.get(cid);
    if (!exch) return 0;
    const bps = feesByExchange.get(exch) ?? 0;
    return bps / 10000;
  };

  // Helper: a closed period whose holdings can be aggregated. Empty /
  // skipped periods don't get scored but still count toward the
  // entry/exit set lineage of their neighbors.
  type Scored = {
    record: PeriodRecord;
    company_ids: Set<number>;
    hasShortLeg: boolean;
  };
  const scored: Scored[] = monthlyRecords.map((r) => ({
    record: r,
    company_ids: new Set(r.holdings.map((h) => h.company_id)),
    hasShortLeg: r.holdings.some((h) => h.side === 'short'),
  }));

  const periodReturns: number[] = [];
  const dates: string[] = [];

  for (let i = 0; i < scored.length; i++) {
    const cur = scored[i];
    if (cur.record.holdings.length === 0) continue;
    const prevSet = i > 0 ? scored[i - 1].company_ids : new Set<number>();
    const nextSet = i + 1 < scored.length ? scored[i + 1].company_ids : new Set<number>();
    const isOpen = !!cur.record.is_open;
    const isLastClosed = !isOpen && i + 1 >= scored.length;
    // For the trailing CLOSED period (no next), every holding "departs"
    // and pays a sell fee. For the OPEN period (no actual sale yet) the
    // sell fee is suppressed regardless of nextSet.
    const treatAllAsDeparting = isLastClosed;

    const longReturns: number[] = [];
    const shortReturns: number[] = [];
    for (const h of cur.record.holdings) {
      const f = feeFor(h.company_id);
      const isEntry = !prevSet.has(h.company_id);
      const isDeparture = !isOpen && (treatAllAsDeparting || !nextSet.has(h.company_id));
      const buyFee = isEntry ? f : 0;
      const sellFee = isDeparture ? f : 0;
      const r = netForwardReturn(h, buyFee, sellFee);
      if (r == null) continue;
      if (h.side === 'short') shortReturns.push(r);
      else longReturns.push(r);
    }

    const portRet = aggregatePortfolio(longReturns, shortReturns, cur.hasShortLeg);
    if (portRet == null) continue;
    periodReturns.push(portRet);
    dates.push(cur.record.date);
  }

  if (periodReturns.length === 0) {
    return {
      total_return_pct: 0,
      annualized_return_pct: 0,
      max_drawdown_pct: 0,
      sharpe_ratio: null,
      dates: [],
      period_returns: [],
      cum_factors: [],
      yearly: {},
    };
  }

  // Cumulative growth factors per period — used for max DD and yearly
  // breakdown alike.
  const cumFactors: number[] = [];
  let cum = 1.0;
  for (const r of periodReturns) {
    cum *= 1 + r / 100;
    cumFactors.push(cum);
  }

  // Max drawdown (peak-to-trough) on the cumulative net curve.
  let peak = 1.0;
  let maxDd = 0;
  for (const f of cumFactors) {
    if (f > peak) peak = f;
    const dd = (f / peak - 1) * 100;
    if (dd < maxDd) maxDd = dd;
  }

  const totalReturn = (cum - 1) * 100;
  const ppy = periodsPerYear(dates);
  const firstDate = dates[0];
  const lastDate = dates[dates.length - 1];
  let years = 0;
  if (firstDate && lastDate) {
    const start = new Date(firstDate).getTime();
    const end = new Date(lastDate).getTime();
    years = (end - start) / (365.25 * 86400 * 1000);
  }
  const annualized = years > 0 ? (Math.pow(cum, 1 / years) - 1) * 100 : 0;

  // Sharpe — require at least one full year of period observations so
  // a tiny backtest doesn't emit a noise-dominated number. Mirrors the
  // backend's gate (`>= int(_periods_per_year)`).
  let sharpe: number | null = null;
  if (periodReturns.length >= Math.max(12, Math.round(ppy))) {
    const mean = periodReturns.reduce((a, b) => a + b, 0) / periodReturns.length;
    const variance = periodReturns.reduce((a, b) => a + (b - mean) ** 2, 0) / periodReturns.length;
    const std = Math.sqrt(variance);
    if (std > 0) sharpe = (mean / std) * Math.sqrt(ppy);
  }

  // Yearly breakdown — group period_returns by calendar year (period
  // start), compound within each year. Mirrors the existing gross
  // yearly breakdown's first-of-January gate so a partial first year
  // doesn't report a spurious 0%.
  const yearly: Record<string, number> = {};
  // Index periods into year buckets, capturing the cumulative factor at
  // each year-end so we can compute yearOver-year compound returns.
  let yearStartFactor = 1.0;
  let currentYear: string | null = null;
  let lastFactorThisYear = 1.0;
  let firstMonthThisYear = '';
  let prevYearCompletedFromStart = false;
  for (let i = 0; i < dates.length; i++) {
    const yr = dates[i].slice(0, 4);
    const mon = dates[i].slice(5, 7);
    if (yr !== currentYear) {
      if (currentYear !== null) {
        // Year just ended — finalize.
        const completeFromStart: boolean = prevYearCompletedFromStart || firstMonthThisYear === '01';
        if (completeFromStart) {
          yearly[currentYear] = ((lastFactorThisYear / yearStartFactor) - 1) * 100;
        }
        yearStartFactor = lastFactorThisYear;
        prevYearCompletedFromStart = completeFromStart;
      }
      currentYear = yr;
      firstMonthThisYear = mon;
    }
    lastFactorThisYear = cumFactors[i];
  }
  // Flush the trailing year.
  if (currentYear !== null) {
    const completeFromStart: boolean = prevYearCompletedFromStart || firstMonthThisYear === '01';
    if (completeFromStart) {
      yearly[currentYear] = ((lastFactorThisYear / yearStartFactor) - 1) * 100;
    }
  }

  return {
    total_return_pct: totalReturn,
    annualized_return_pct: annualized,
    max_drawdown_pct: maxDd,
    sharpe_ratio: sharpe,
    dates,
    period_returns: periodReturns,
    cum_factors: cumFactors,
    yearly,
  };
}

/** Convenience: build the lookup map from the /api/exchange-fees payload. */
export function buildFeeMap(rows: { exchange_code: string; fee_bps: number }[]): Map<string, number> {
  const m = new Map<string, number>();
  for (const r of rows) {
    if (r.fee_bps > 0) m.set(r.exchange_code, r.fee_bps);
  }
  return m;
}

/** Format a "(net X%)" parenthetical given a net stat. Returns empty
 * string when `net` is null so the caller can concatenate
 * `"${gross}${parenStat(net)}"` without conditionals. */
export function parenPct(value: number | null | undefined, decimals = 2): string {
  if (value == null || !Number.isFinite(value)) return '';
  return ` (${value >= 0 ? '+' : ''}${value.toFixed(decimals)}%)`;
}
