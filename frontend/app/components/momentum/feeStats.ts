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

/** Drives the trade-aware loop: walks `monthly_records` once, classifies
 * each holding as new/continuing/departing vs its period-N±1 neighbors,
 * and pipes the per-holding net forward-returns through portfolio
 * aggregation. Empty / open-period boundaries are honored. */
export function computeNetStats(
  monthlyRecords: PeriodRecord[] | undefined,
  feesByExchange: Map<string, number>,
  exchangeByCompany: Map<number, string>,
  dailyRecords?: DailyRecord[],
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

  // Helper: a scored period. We keep ALL records in this array (open
  // included) so the entry/exit lineage tracking — which compares
  // adjacent period sets to decide whether a holding paid a buy/sell
  // fee — sees the correct neighbors. But only CLOSED records
  // contribute their port_return to the headline-stat accumulation
  // below. This mirrors the backend's `closed_records = [r for r in
  // period_records if not r.is_open]` in `_summary.py`; without it,
  // a positive open-period return could push net cumulative above
  // gross cumulative even when every closed period's net is ≤ gross,
  // violating the "fees can only reduce returns" invariant.
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

  // Find the index of the last CLOSED period (skipping any trailing
  // open). Used by `treatAllAsDeparting` so the last closed period's
  // holdings still pay a sell fee at backtest end.
  const lastClosedIdx = (() => {
    for (let i = scored.length - 1; i >= 0; i--) {
      if (!scored[i].record.is_open) return i;
    }
    return -1;
  })();

  const periodReturns: number[] = [];
  const dates: string[] = [];
  const periodDragFactors: Array<{ exit_date: string; fee_factor: number }> = [];

  for (let i = 0; i < scored.length; i++) {
    const cur = scored[i];
    if (cur.record.holdings.length === 0) continue;
    const prevSet = i > 0 ? scored[i - 1].company_ids : new Set<number>();
    const nextSet = i + 1 < scored.length ? scored[i + 1].company_ids : new Set<number>();
    const isOpen = !!cur.record.is_open;
    // The last CLOSED period charges sell fees on every holding (no
    // future period to roll into). An open period later in the array
    // doesn't change this — open periods don't trigger sales, so the
    // last closed period is still the true "exit" boundary.
    const treatAllAsDeparting = !isOpen && i === lastClosedIdx;

    const longReturns: number[] = [];
    const shortReturns: number[] = [];
    // Parallel arrays of gross (fee=0) returns so we can derive a
    // per-period fee_factor = (1 + net) / (1 + gross). We can't compare
    // against backend's `record.port_return` here because backend
    // rounding + our holding-filter (we skip holdings with missing
    // prices via netForwardReturn returning null) can drift them
    // apart by hundredths of a percent — enough to occasionally let
    // fee_factor exceed 1.0 and violate the net ≤ gross invariant.
    // Computing both with the same aggregation guarantees the bound.
    const longReturnsGross: number[] = [];
    const shortReturnsGross: number[] = [];
    for (const h of cur.record.holdings) {
      const f = feeFor(h.company_id);
      const isEntry = !prevSet.has(h.company_id);
      const isDeparture = !isOpen && (treatAllAsDeparting || !nextSet.has(h.company_id));
      const buyFee = isEntry ? f : 0;
      const sellFee = isDeparture ? f : 0;
      const r = netForwardReturn(h, buyFee, sellFee);
      if (r == null) continue;
      const rGross = netForwardReturn(h, 0, 0);
      if (rGross == null) continue;
      if (h.side === 'short') {
        shortReturns.push(r);
        shortReturnsGross.push(rGross);
      } else {
        longReturns.push(r);
        longReturnsGross.push(rGross);
      }
    }

    const portRetRaw = aggregatePortfolio(longReturns, shortReturns, cur.hasShortLeg);
    if (portRetRaw == null) continue;
    // Daily tit-for-tat swap cost: each full-book cash<->stocks trade this
    // period pays the held book's AVERAGE per-exchange fee (the whole book
    // is sold/bought, so the leg fraction is 1). Applied on top of the
    // period's rebalance buy/sell fees. No-op when timing is off (0 swaps).
    let portRet = portRetRaw;
    const nSwaps = cur.record.daily_timing_swaps ?? 0;
    if (nSwaps > 0 && cur.record.holdings.length > 0) {
      let feeSum = 0;
      for (const h of cur.record.holdings) feeSum += feeFor(h.company_id);
      const avgFee = feeSum / cur.record.holdings.length;
      if (avgFee > 0) {
        portRet = ((1 + portRetRaw / 100) * Math.pow(1 - avgFee, nSwaps) - 1) * 100;
      }
    }
    // CRITICAL: only closed periods feed the headline-stat
    // accumulation. Open periods still go through the loop above so
    // their company_ids influence the trade-aware fee classification
    // of the LAST CLOSED period (a name rolling from closed-N into
    // open doesn't pay a sell fee), but their own returns are
    // excluded from total / annualized / max-DD / Sharpe / yearly.
    if (isOpen) continue;
    periodReturns.push(portRet);
    dates.push(cur.record.date);

    // Capture the per-period fee_factor and the date the fees realize
    // (= when the sell happens = max holding.exit_date in this period,
    // falling back to the next period's record.date for periods with
    // no per-holding exit data). For yearly bucketing on calendar-year
    // boundaries we want the date the cash leaves the portfolio.
    const portRetGross = aggregatePortfolio(longReturnsGross, shortReturnsGross, cur.hasShortLeg);
    if (portRetGross == null) continue;
    let exitDateIso: string | null = null;
    for (const h of cur.record.holdings) {
      if (h.exit_date && (exitDateIso == null || h.exit_date > exitDateIso)) {
        exitDateIso = h.exit_date;
      }
    }
    if (!exitDateIso) {
      const nextRec = i + 1 < scored.length ? scored[i + 1].record : null;
      if (nextRec?.date) {
        exitDateIso = nextRec.date.length === 7 ? `${nextRec.date}-28` : nextRec.date;
      }
    }
    if (!exitDateIso) {
      // Last possible fallback: this period's own date — keeps the
      // factor in the chain even if the year bucket is approximate.
      exitDateIso = cur.record.date.length === 7 ? `${cur.record.date}-28` : cur.record.date;
    }
    const feeFactor = (1 + portRet / 100) / (1 + portRetGross / 100);
    // Clamp to ≤ 1 against floating-point slop. Per-holding math is
    // built so net ≤ gross always; this guards against ULP rounding.
    periodDragFactors.push({
      exit_date: exitDateIso,
      fee_factor: Math.min(1, feeFactor),
    });
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
      period_drag_factors: [],
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

  // Max drawdown AND Sharpe must be computed on the SAME granularity as
  // the gross figures they sit next to. The backend derives both from the
  // closed-period daily curve (see `_summary.py`:
  //   - max DD: `_find_drawdown_periods(closed_curve)` → intra-month
  //     troughs are caught.
  //   - Sharpe: `closed_daily_returns` mean/std × √252, falling back to
  //     period × √(periods/year) only when fewer than 21 daily returns
  //     are available.
  // Re-deriving either from the monthly chain alone would smooth intra-
  // month moves away — for max DD that lets the (net) value drift
  // misleadingly LESS negative than gross (impossible: fees can only
  // reduce wealth at each point). For Sharpe the cadence mismatch
  // (×√12 vs ×√252) makes (net) drift unpredictably above or below
  // gross even at vanishingly small fees, breaking the like-for-like
  // comparison.
  //
  // Two fee models for the daily curve:
  //
  //   - Max DD uses the CONCENTRATED-drag model: cumDrag is a step
  //     function that drops by `fee_factor` at each closed period's
  //     exit_date. The net daily curve has a one-day "fee blip" at every
  //     rebalance — that's closer to what the real equity curve does, and
  //     it correctly compounds any fee landing inside a drawdown into
  //     deeper trough/peak ratios. The invariant `net_max_dd ≤ gross_max_dd`
  //     (more negative) holds because every daily factor is ≤ gross.
  //
  //   - Sharpe uses the UNIFORM-drag model: spread the same total
  //     cumulative fee impact evenly across every day as a constant
  //     `daily_drag = -log(cum_fee_factor) / N`. With this model
  //     `net_return_d = gross_return_d - daily_drag` everywhere, so
  //     gross std is preserved (no artificial outliers) and net Sharpe
  //     drops monotonically with fees. The concentrated model can leave
  //     net Sharpe HIGHER than gross when a small number of rebalances
  //     land on above-average return days — variance drops faster than
  //     mean, so the ratio creeps up. For tiny fees (a few bps) that
  //     drift is just rounding noise but it visibly contradicts the
  //     "fees can only worsen risk-adjusted return" invariant. Uniform
  //     drag is the standard cost-of-trading adjustment for Sharpe and
  //     guarantees monotonicity.
  //
  // The monthly chain stays as a fallback for both, used only when the
  // result doesn't ship `daily_records` (older saved bundles).
  let maxDd = 0;
  let sharpe: number | null = null;
  let usedDailyBranch = false;
  if (dailyRecords && dailyRecords.length > 0 && periodDragFactors.length > 0) {
    const sortedDrags = periodDragFactors
      .slice()
      .sort((a, b) => a.exit_date.localeCompare(b.exit_date));
    const lastClosedExitDate = sortedDrags[sortedDrags.length - 1].exit_date;
    let dragIdx = 0;
    let cumDrag = 1.0;
    let peak = 0;
    let seededPeak = false;
    let prevGrossFactor: number | null = null;
    const grossReturns: number[] = [];
    for (const d of dailyRecords) {
      // Truncate to the closed-curve window so we don't pull in open-
      // period daily values backend's gross figures don't include.
      if (d.date > lastClosedExitDate) break;
      while (dragIdx < sortedDrags.length && sortedDrags[dragIdx].exit_date <= d.date) {
        cumDrag *= sortedDrags[dragIdx].fee_factor;
        dragIdx++;
      }
      const grossFactor = 1 + d.cumulative_return_pct / 100;
      // Concentrated-drag net factor — only used for max DD.
      const netFactor = grossFactor * cumDrag;
      if (!seededPeak || netFactor > peak) {
        peak = netFactor;
        seededPeak = true;
      }
      const dd = peak > 0 ? (netFactor / peak - 1) * 100 : 0;
      if (dd < maxDd) maxDd = dd;
      // Gross daily returns collected separately for the uniform-drag
      // Sharpe below.
      if (prevGrossFactor != null && prevGrossFactor > 0) {
        grossReturns.push(grossFactor / prevGrossFactor - 1);
      }
      prevGrossFactor = grossFactor;
    }
    // Mirror backend's `if len(closed_daily_returns) >= 21` gate. At
    // that point Sharpe = mean/std × √252; below it we fall through to
    // the monthly-period fallback below.
    if (grossReturns.length >= 21) {
      const grossMean =
        grossReturns.reduce((a, b) => a + b, 0) / grossReturns.length;
      const variance =
        grossReturns.reduce((a, b) => a + (b - grossMean) ** 2, 0) /
        grossReturns.length;
      const std = Math.sqrt(variance);
      // Uniform daily drag derived from the cumulative product of
      // per-period fee_factors. `cumDrag` already holds this product
      // after the loop above (all closed periods' drags compounded).
      // Guard against degenerate cumDrag (≤ 0 can't happen with valid
      // fee_factors ≤ 1 and starting at 1.0, but be defensive).
      const dailyDrag =
        cumDrag > 0 ? -Math.log(cumDrag) / grossReturns.length : 0;
      const netMean = grossMean - dailyDrag;
      if (std > 0) sharpe = (netMean / std) * Math.sqrt(252);
    }
    usedDailyBranch = true;
  }
  if (!usedDailyBranch) {
    // Fallback: monthly-chain max DD when no daily records are available.
    let peak = 1.0;
    for (const f of cumFactors) {
      if (f > peak) peak = f;
      const dd = (f / peak - 1) * 100;
      if (dd < maxDd) maxDd = dd;
    }
  }

  const totalReturn = (cum - 1) * 100;
  const ppy = periodsPerYear(dates);
  // Match the backend's annualization year-count, which spans the
  // closed daily curve from the first holding's entry to the last
  // holding's exit (~first period start → last period end). Using
  // record-date labels alone produces a year-count that's ~1 period
  // SHORT (e.g. "2020-01" → "2024-12" is 4.92 years, but the strategy
  // is actually held through ~2025-01-01, so backend reports ~4.99
  // years). The shorter year-count inflates annualized; with tight
  // fees that can push displayed net annualized above backend gross,
  // violating "fees can only reduce returns".
  let spanStart = Number.POSITIVE_INFINITY;
  let spanEnd = Number.NEGATIVE_INFINITY;
  for (const r of monthlyRecords) {
    if (r.is_open) continue;
    for (const h of r.holdings) {
      if (h.entry_date) {
        const t = Date.parse(h.entry_date);
        if (Number.isFinite(t) && t < spanStart) spanStart = t;
      }
      if (h.exit_date) {
        const t = Date.parse(h.exit_date);
        if (Number.isFinite(t) && t > spanEnd) spanEnd = t;
      }
    }
  }
  let years = 0;
  if (Number.isFinite(spanStart) && Number.isFinite(spanEnd) && spanEnd > spanStart) {
    years = (spanEnd - spanStart) / (365.25 * 86400 * 1000);
  } else if (dates.length >= 2) {
    // Fallback for saved backtests that didn't ship entry/exit dates
    // on holdings — pad the label-only span by one period so the
    // year-count still approximates the backend's metric.
    const firstStr = dates[0].length === 7 ? `${dates[0]}-01` : dates[0];
    const lastStr = dates[dates.length - 1].length === 7 ? `${dates[dates.length - 1]}-01` : dates[dates.length - 1];
    const spanMs = Date.parse(lastStr) - Date.parse(firstStr);
    years = spanMs / (365.25 * 86400 * 1000) + 1 / ppy;
  }
  const annualized = years > 0 ? (Math.pow(cum, 1 / years) - 1) * 100 : 0;

  // Sharpe — monthly fallback when the daily branch above didn't fire
  // (no daily_records, or fewer than 21 daily returns after truncating
  // to the closed-curve window). Mirrors backend's elif:
  // `period_mean / period_std × √(periods/year)` once one full year of
  // observations is available.
  if (sharpe == null && periodReturns.length >= Math.max(12, Math.round(ppy))) {
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
    period_drag_factors: periodDragFactors,
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
