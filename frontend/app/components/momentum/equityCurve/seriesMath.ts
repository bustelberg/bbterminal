/**
 * Pure math + alignment helpers for the EquityCurveCard. Everything in
 * this module is a side-effect-free transformation of input data into
 * derived shapes — no React, no fetch, no state. Moved out of
 * EquityCurveCard.tsx (which previously inlined ~300 lines of these
 * inside `useMemo` blocks) so the main component is just orchestration.
 *
 * Public entry points:
 *
 *   - `resolveSeries(active, comparisons, palette, activeLabel)` — turn
 *     the active backtest + every added comparison into a uniform
 *     `ResolvedSeries[]` keyed on `factorByMonth` lookup maps.
 *   - `alignSeries(resolved)` — find the [maxStart, minEnd] window
 *     across every series and produce per-series rebased points + stats
 *     + top drawdowns.
 *   - `computeYearlyBreakdown(aligned, activeNetStats, comparisons,
 *     comparisonNetStats)` — per-calendar-year compound return per
 *     series, with optional net-fee overlay.
 *   - `computeCustomRangeReturn(...)` — total return from a user-picked
 *     month to the end of the aligned window, with net overlay.
 *   - `buildChartData(aligned)` — wide-format chart rows.
 *   - `computeChartYDomain(displayChartData, aligned)` — padded
 *     [min, max] domain for the Y-axis.
 */
import type {
  BacktestResult,
  DailyRecord,
  DrawdownPeriod,
  PeriodRecord,
  Summary,
} from '../../../../lib/stores/momentum';
import type { BenchmarkPrice, ComparisonItem } from '../types';
import { computeTopDrawdowns } from '../utils';
import { chartTheme } from '../../../../lib/chartTheme';
import type { NetStats } from '../feeStats';
import type { FeeBreakdownRow } from '../feeModel';

// ─── Types ─────────────────────────────────────────────────────────

export type ResolvedSeries = {
  id: string;
  label: string;
  color: string;
  kind: 'active' | 'saved' | 'benchmark';
  removable: boolean;
  factorByMonth: Map<string, number>;
  months: string[]; // sorted YYYY-MM-DD
  // Backend-computed canonical stats. Present for any series that came out
  // of a backtest run (active or saved); benchmarks fall back to
  // points-derived stats since there's no run to attribute to them.
  summary?: Summary;
};

export type SeriesPoint = { date: string; cumReturnPct: number | null };
export type SeriesStats = {
  totalReturn: number;
  annualized: number;
  maxDd: number;
  sharpe: number | null;
  months: number;
};
export type AlignedSeries = ResolvedSeries & {
  points: SeriesPoint[];
  stats: SeriesStats;
  topDrawdowns: DrawdownPeriod[];
};
export type AlignedResult = {
  series: AlignedSeries[];
  windowStart: string | null;
  windowEnd: string | null;
  allMonths: string[];
};

// ─── Date / series helpers ─────────────────────────────────────────

/** Promote a YYYY-MM monthly_records date to end-of-month YYYY-MM-DD so
 * it can be sorted/compared against daily YYYY-MM-DD dates correctly.
 * Using "28" for Feb is conservative — we just need a stable last-day-
 * of-month-ish anchor, not the exact trading day. */
export function endOfMonth(yyyymm: string): string {
  if (yyyymm.length !== 7) return yyyymm; // already YYYY-MM-DD
  const [y, m] = yyyymm.split('-').map(Number);
  // JavaScript: new Date(y, m, 0) gives the last day of month m (1-indexed)
  const last = new Date(y, m, 0).getDate();
  return `${yyyymm}-${String(last).padStart(2, '0')}`;
}

export function seriesFromMonthly(
  monthly: PeriodRecord[],
): { map: Map<string, number>; months: string[] } {
  const map = new Map<string, number>();
  const months: string[] = [];
  for (const r of monthly) {
    const key = endOfMonth(r.date);
    const factor = 1 + r.cumulative_return_pct / 100;
    map.set(key, factor);
    months.push(key);
  }
  months.sort();
  return { map, months };
}

/** Build a {date -> factor} map for the universe equal-weight baseline.
 *
 * Prefers `universe_daily_records` (one entry per trading day, same shape
 * as the strategy's `daily_records`) when present so the gray baseline
 * line matches the strategy's daily granularity in the chart, max DD
 * detection, and Sharpe.
 *
 * Falls back to `monthly_records[i].universe_cumulative_return_pct` —
 * one point per rebalance, anchored to end-of-month — for legacy saved
 * runs predating the daily baseline. Returns an empty map when neither
 * source carries data. */
export function seriesFromUniverseBaseline(
  monthly: PeriodRecord[],
  daily?: DailyRecord[] | null,
): { map: Map<string, number>; months: string[] } {
  if (daily && daily.length > 0) {
    const map = new Map<string, number>();
    const dates: string[] = [];
    for (const d of daily) {
      map.set(d.date, 1 + d.cumulative_return_pct / 100);
      dates.push(d.date);
    }
    return { map, months: dates };
  }
  const map = new Map<string, number>();
  const months: string[] = [];
  for (const r of monthly) {
    const v = r.universe_cumulative_return_pct;
    if (v == null) continue;
    const key = endOfMonth(r.date);
    map.set(key, 1 + v / 100);
    months.push(key);
  }
  months.sort();
  return { map, months };
}

/** Prefer daily_records when present so the chart line, max DD, and
 * Sharpe all reflect intra-period moves. Falls back to monthly_records
 * for older saved runs that don't carry the daily curve. Dates are
 * normalized to YYYY-MM-DD either way. */
export function seriesFromResult(
  r: { monthly_records: PeriodRecord[]; daily_records?: DailyRecord[] | null | undefined },
): { map: Map<string, number>; months: string[] } {
  if (r.daily_records && r.daily_records.length > 0) {
    const map = new Map<string, number>();
    const dates: string[] = [];
    for (const d of r.daily_records) {
      map.set(d.date, 1 + d.cumulative_return_pct / 100);
      dates.push(d.date);
    }
    return { map, months: dates };
  }
  return seriesFromMonthly(r.monthly_records);
}

export function seriesFromPrices(
  prices: BenchmarkPrice[],
): { map: Map<string, number>; months: string[] } {
  // Daily granularity. Each entry's factor is `price[t] / price[firstDay]`
  // so the series rebases to 1.0 on its earliest day; alignment shifts
  // that basis to the common window start later.
  if (prices.length === 0) return { map: new Map(), months: [] };
  const sorted = prices.slice().sort((a, b) => a.target_date.localeCompare(b.target_date));
  const map = new Map<string, number>();
  const dates: string[] = [];
  const p0 = sorted[0].price;
  if (!p0 || p0 <= 0) return { map: new Map(), months: [] };
  for (const p of sorted) {
    if (!map.has(p.target_date)) {
      map.set(p.target_date, p.price / p0);
      dates.push(p.target_date);
    }
  }
  return { map, months: dates };
}

// ─── Alignment ─────────────────────────────────────────────────────

/** Find the [maxStart, minEnd] alignment window across every series and
 * compute per-series aligned points (rebased to 0% on windowStart),
 * stats, and top drawdowns. */
export function alignSeries(resolvedSeries: ResolvedSeries[]): AlignedResult {
  if (resolvedSeries.length === 0) return { series: [], windowStart: null, windowEnd: null, allMonths: [] };

  let maxStart = '';
  let minEnd = '9999-99';
  for (const s of resolvedSeries) {
    if (s.months.length === 0) continue;
    const first = s.months[0];
    const last = s.months[s.months.length - 1];
    if (first > maxStart) maxStart = first;
    if (last < minEnd) minEnd = last;
  }
  if (!maxStart || minEnd === '9999-99' || maxStart > minEnd) {
    return { series: [], windowStart: null, windowEnd: null, allMonths: [] };
  }

  // Union of all months each series has within [maxStart, minEnd]. If one
  // series lacks a given month, that series reports null for it.
  const monthSet = new Set<string>();
  for (const s of resolvedSeries) {
    for (const m of s.months) {
      if (m >= maxStart && m <= minEnd) monthSet.add(m);
    }
  }
  const allMonths = Array.from(monthSet).sort();

  const series: AlignedSeries[] = resolvedSeries.map((s) => {
    // baseFactor anchors this series to the common window start. If the
    // series doesn't have an exact entry on maxStart (a monthly
    // benchmark vs a daily strategy whose first date is mid-month, for
    // example), fall back to the earliest entry on or after maxStart so
    // the series isn't blanked out by alignment.
    let baseFactor = s.factorByMonth.get(maxStart);
    if (baseFactor == null) {
      for (const m of s.months) {
        if (m >= maxStart) { baseFactor = s.factorByMonth.get(m); break; }
      }
    }
    const points: SeriesPoint[] = allMonths.map((m) => {
      const f = s.factorByMonth.get(m);
      if (f == null || baseFactor == null) return { date: m, cumReturnPct: null };
      return { date: m, cumReturnPct: (f / baseFactor - 1) * 100 };
    });

    // Period-over-period rebased returns for stats. Cadence (daily vs
    // monthly) is detected from the actual date range — `points` may be
    // daily for the strategy and monthly for a benchmark; both are
    // handled uniformly by deriving periodsPerYear from observations
    // per actual year of the aligned window. This is what makes max DD
    // honor intra-month moves on a monthly strategy whose daily curve
    // is now available.
    const periodReturns: number[] = [];
    let prev: number | null = null;
    for (const p of points) {
      if (p.cumReturnPct == null) continue;
      const factor = 1 + p.cumReturnPct / 100;
      if (prev != null && prev > 0) {
        periodReturns.push((factor / prev - 1) * 100);
      }
      prev = factor;
    }
    const lastNonNull = [...points].reverse().find((p) => p.cumReturnPct != null);
    const totalReturn = lastNonNull?.cumReturnPct ?? 0;
    const cumFactor = 1 + totalReturn / 100;
    const firstPointDate = points.find((p) => p.cumReturnPct != null)?.date ?? null;
    const lastPointDate = lastNonNull?.date ?? null;
    const yearsByDate = firstPointDate && lastPointDate
      ? (new Date(lastPointDate).getTime() - new Date(firstPointDate).getTime()) / (365.25 * 86400000)
      : 0;
    const annualized = yearsByDate > 0 ? (Math.pow(cumFactor, 1 / yearsByDate) - 1) * 100 : 0;
    const periodsPerYear = yearsByDate > 0 ? periodReturns.length / yearsByDate : 12;

    let peakDd = 1.0, maxDd = 0, ddFactor = 1.0;
    for (const r of periodReturns) {
      ddFactor *= (1 + r / 100);
      peakDd = Math.max(peakDd, ddFactor);
      const dd = (ddFactor / peakDd - 1) * 100;
      maxDd = Math.min(maxDd, dd);
    }

    let sharpe: number | null = null;
    // Need at least one full year of observations regardless of cadence
    // (so a 5-month daily run still doesn't get a Sharpe — same guard
    // as the backend for the active strategy).
    if (periodReturns.length >= Math.max(12, Math.round(periodsPerYear))) {
      const mean = periodReturns.reduce((a, b) => a + b, 0) / periodReturns.length;
      const std = Math.sqrt(periodReturns.reduce((a, b) => a + (b - mean) ** 2, 0) / periodReturns.length);
      if (std > 0) sharpe = (mean / std) * Math.sqrt(periodsPerYear);
    }

    const ddValues = points
      .filter((p) => p.cumReturnPct != null)
      .map((p) => ({ date: p.date, value: 1 + (p.cumReturnPct as number) / 100 }));
    const topDrawdowns = computeTopDrawdowns(ddValues, 3);

    // Prefer the backend's canonical summary stats whenever the series
    // came from an actual backtest run. The backend computes Sharpe + max
    // drawdown from the daily curve (so a 12-month-rebalance strategy
    // still gets full intra-period detail) — re-deriving them from the
    // few rebased monthly points here would under-count volatility and
    // produce a different number for the same strategy depending on
    // which table it's shown in. Points-derived stats are only used as a
    // fallback for benchmarks (which have no summary attached).
    const useBackendStats = s.summary != null;
    const finalStats: SeriesStats = useBackendStats
      ? {
          totalReturn: s.summary!.total_return_pct,
          annualized: s.summary!.annualized_return_pct,
          maxDd: s.summary!.max_drawdown_pct,
          sharpe: s.summary!.sharpe_ratio,
          months: periodReturns.length,
        }
      : { totalReturn, annualized, maxDd, sharpe, months: periodReturns.length };

    return {
      ...s,
      points,
      stats: finalStats,
      topDrawdowns,
    };
  });

  return { series, windowStart: maxStart, windowEnd: minEnd, allMonths };
}

// ─── Yearly breakdown ──────────────────────────────────────────────

export type YearlyBreakdown = {
  years: string[];
  bySeries: Record<string, Record<string, number | null>>;
  netYearlyBySeries: Record<string, Record<string, number | null>>;
};

/** Per-calendar-year compound return per series. The optional net-fee
 * overlay (built from each holdings-bearing series' NetStats) is keyed
 * by `id` alongside the gross `bySeries`. */
export function computeYearlyBreakdown(
  aligned: AlignedResult,
  feeBreakdownsBySeries: Record<string, FeeBreakdownRow[] | null>,
): YearlyBreakdown {
  const { series } = aligned;
  if (series.length === 0) return { years: [], bySeries: {}, netYearlyBySeries: {} };

  const yearsSet = new Set<string>();
  const bySeries: Record<string, Record<string, number | null>> = {};

  for (const s of series) {
    // Track first + last observation per year so we can detect partial
    // years. A year is "complete from the start" only when either:
    //   - there's a prior year of observations (so prev_year_end is the
    //     proper baseline), or
    //   - the first observation in this year falls in January.
    // Otherwise the series's earliest point in that year is the rebase
    // baseline (cumReturnPct == 0), and computing "cum_at_year_end / 1 - 1"
    // produces a spurious 0% for what's really a partial year.
    type YearStat = { firstDate: string; firstCum: number; lastCum: number; count: number };
    const yearStats = new Map<string, YearStat>();
    for (const p of s.points) {
      if (p.cumReturnPct == null) continue;
      const y = p.date.slice(0, 4);
      const existing = yearStats.get(y);
      if (existing) {
        existing.lastCum = p.cumReturnPct;
        existing.count++;
      } else {
        yearStats.set(y, { firstDate: p.date, firstCum: p.cumReturnPct, lastCum: p.cumReturnPct, count: 1 });
      }
    }
    const ys = Array.from(yearStats.keys()).sort();
    let prevCum: number | null = null;
    const rowMap: Record<string, number | null> = {};
    for (const y of ys) {
      const stat = yearStats.get(y)!;
      const firstMonth = stat.firstDate.slice(5, 7);
      // A year is shown as "—" when either:
      //   - it's a partial first year (series's first observation is past
      //     January AND there's no prior year of observations to baseline
      //     against), or
      //   - the year has MULTIPLE observations that are all the same value
      //     (e.g., a momentum strategy still in its 12-month warmup period
      //     with no positions held). Single-observation years (annual or
      //     longer rebalance cadences) can't tell us whether the cum moved
      //     within the year, so they're always rendered.
      const isCompleteFromStart = prevCum !== null || firstMonth === '01';
      const flatYear = stat.count > 1 && Math.abs(stat.lastCum - stat.firstCum) < 1e-9;
      if (!isCompleteFromStart || flatYear) {
        rowMap[y] = null;
      } else {
        const baseline = prevCum ?? 0;
        rowMap[y] = ((1 + stat.lastCum / 100) / (1 + baseline / 100) - 1) * 100;
      }
      prevCum = stat.lastCum;
      yearsSet.add(y);
    }
    bySeries[s.id] = rowMap;
  }

  const years = Array.from(yearsSet).sort();
  // Backfill missing years with null so the column count matches.
  for (const s of series) {
    for (const y of years) if (!(y in bySeries[s.id])) bySeries[s.id][y] = null;
  }

  // Per-year net (and the matching gross) for holdings-bearing series
  // comes straight from the fee model's per-year breakdown, which
  // attributes each year's fees to that year — the annual / management /
  // performance fees all crystallize at year-end, so they belong wholly
  // to the year that just closed. We OVERRIDE the displayed gross with
  // the breakdown's per-year gross too, so each row reconciles
  // (gross − fees = net) and is identical to the Fee waterfall detail.
  //
  // The previous approach bucketed per-period drag factors by exit_date,
  // which mis-assigned the year-end crystallization to whichever rebalance
  // period straddled Dec 31 — leaving the closing year showing only the
  // transaction drag and dumping annual+mgmt+perf onto the next year.
  const netYearlyBySeries: Record<string, Record<string, number | null>> = {};
  for (const [seriesId, bd] of Object.entries(feeBreakdownsBySeries)) {
    if (!bd || !bySeries[seriesId]) continue;
    const netRow: Record<string, number | null> = {};
    for (const r of bd) {
      const y = r.label.slice(0, 4);
      bySeries[seriesId][y] = r.gross_return_pct;
      netRow[y] = r.net_return_pct;
    }
    netYearlyBySeries[seriesId] = netRow;
  }
  return { years, bySeries, netYearlyBySeries };
}

// ─── Per-year subplots (cumulative + alpha vs universe) ────────────

export type YearSubplotPoint = {
  date: string;                  // YYYY-MM-DD (or YYYY-MM if monthly)
  strategyCum: number | null;    // rebased so year-start = 0%
  universeCum: number | null;    // rebased so year-start = 0%
  alpha: number | null;          // strategyCum − universeCum, percentage points
};

export type YearSubplot = {
  year: string;
  points: YearSubplotPoint[];
};

/** Per-calendar-year sub-charts: strategy and universe cumulative
 * returns rebased so each year starts at 0%, plus a derived alpha
 * (arithmetic excess return in percentage points).
 *
 * The baseline used to rebase year Y is the previous year's last
 * non-null cumReturnPct (or 0% when there is no prior year). This
 * matches `computeYearlyBreakdown`'s semantics so totals at year-end
 * agree with the Yearly Performance table.
 *
 * Returns `[]` when the universe baseline isn't available (very old
 * saved runs predating `universe_daily_records` /
 * `universe_cumulative_return_pct`). The grid component checks this
 * and hides itself in that case.
 */
export function computeYearlySubplots(aligned: AlignedResult): YearSubplot[] {
  const active = aligned.series.find((s) => s.kind === 'active');
  const universe = aligned.series.find((s) => s.id === 'universe');
  if (!active || !universe) return [];

  // Build year → prior-year-end baseline for each series so the
  // first day of each year sits at 0% and the curve through the year
  // is a true within-year cumulative.
  const yearBaseline = (
    points: SeriesPoint[],
  ): Map<string, number> => {
    const lastByYear = new Map<string, number>();
    for (const p of points) {
      if (p.cumReturnPct == null) continue;
      lastByYear.set(p.date.slice(0, 4), p.cumReturnPct);
    }
    const years = Array.from(lastByYear.keys()).sort();
    const out = new Map<string, number>();
    for (let i = 0; i < years.length; i++) {
      out.set(years[i], i > 0 ? lastByYear.get(years[i - 1])! : 0);
    }
    return out;
  };
  const stratBase = yearBaseline(active.points);
  const uniBase = yearBaseline(universe.points);

  // Universe points by date for O(1) lookup while iterating the
  // strategy's points (which drives the per-year x-axis).
  const uniByDate = new Map<string, number | null>();
  for (const p of universe.points) uniByDate.set(p.date, p.cumReturnPct);

  const byYear = new Map<string, YearSubplotPoint[]>();
  for (const p of active.points) {
    const y = p.date.slice(0, 4);
    const sb = stratBase.get(y) ?? 0;
    const ub = uniBase.get(y) ?? 0;
    const sCum = p.cumReturnPct;
    const uCum = uniByDate.get(p.date) ?? null;
    const sR = sCum == null ? null : ((1 + sCum / 100) / (1 + sb / 100) - 1) * 100;
    const uR = uCum == null ? null : ((1 + uCum / 100) / (1 + ub / 100) - 1) * 100;
    const alpha = sR != null && uR != null ? sR - uR : null;
    const bucket = byYear.get(y) ?? [];
    bucket.push({ date: p.date, strategyCum: sR, universeCum: uR, alpha });
    byYear.set(y, bucket);
  }

  return Array.from(byYear.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([year, points]) => ({ year, points }))
    // Drop empty years (no overlap with active series within window).
    .filter((s) => s.points.some((p) => p.strategyCum != null || p.universeCum != null));
}

// ─── Custom range ──────────────────────────────────────────────────

export type CustomRangeReturn = {
  perSeries: Array<{ id: string; label: string; color: string; ret: number | null; netRet: number | null }>;
  fromDate: string;
  toDate: string;
};

/** Cumulative return from `fromMonth` through the end of the aligned
 * window, per series, with a per-holdings-series net-fee overlay. */
export function computeCustomRangeReturn(
  aligned: AlignedResult,
  fromMonth: string,
  activeNetStats: NetStats | null,
  comparisons: ComparisonItem[],
  comparisonNetStats: Map<string, NetStats | null>,
): CustomRangeReturn | null {
  const { series } = aligned;
  if (!fromMonth || series.length === 0) return null;
  const last = series[0].points[series[0].points.length - 1];
  if (!last) return null;
  // Walk each holdings-bearing series' net cumulative chain for the
  // window so both the active row and every saved comparison get a
  // (net) parenthetical alongside their gross figure. Mirrors the
  // gross math: start factor = cum at the last date < fromMonth (or
  // 1.0 if none), end factor = cum at the final date.
  const netRetById = new Map<string, number | null>();
  const walkNet = (ns: NetStats | null | undefined): number | null => {
    if (!ns) return null;
    let startFactor = 1.0;
    let endFactor: number | null = null;
    for (let i = 0; i < ns.dates.length; i++) {
      if (ns.dates[i] < fromMonth) startFactor = ns.cum_factors[i];
      endFactor = ns.cum_factors[i];
    }
    if (endFactor == null || startFactor <= 0) return null;
    return (endFactor / startFactor - 1) * 100;
  };
  netRetById.set('active', walkNet(activeNetStats));
  for (const c of comparisons) {
    if (c.kind === 'saved') netRetById.set(c.id, walkNet(comparisonNetStats.get(c.id)));
  }
  const perSeries = series.map((s) => {
    let start: number | null = null;
    let end: number | null = null;
    for (const p of s.points) {
      if (p.cumReturnPct == null) continue;
      if (p.date < fromMonth) start = p.cumReturnPct;
      end = p.cumReturnPct;
    }
    if (end == null) return { id: s.id, label: s.label, color: s.color, ret: null, netRet: null };
    const s0 = start ?? 0;
    return {
      id: s.id,
      label: s.label,
      color: s.color,
      ret: ((1 + end / 100) / (1 + s0 / 100) - 1) * 100,
      netRet: s.kind !== 'benchmark' ? netRetById.get(s.id) ?? null : null,
    };
  });
  return { perSeries, fromDate: fromMonth, toDate: last.date };
}

// ─── Chart data prep ───────────────────────────────────────────────

/** Wide-format per month, one key per series id, plus a 0% origin row
 * so every line starts from the same reference point. */
export function buildChartData(aligned: AlignedResult): Record<string, string | number | null>[] {
  const { series, allMonths, windowStart } = aligned;
  if (series.length === 0 || allMonths.length === 0) return [];

  const rows: Record<string, string | number | null>[] = [];
  if (windowStart) {
    const origin: Record<string, string | number | null> = { date: `${windowStart} (start)` };
    for (const s of series) origin[s.id] = 0;
    rows.push(origin);
  }
  for (const m of allMonths) {
    const row: Record<string, string | number | null> = { date: m };
    for (const s of series) {
      const pt = s.points.find((p) => p.date === m);
      row[s.id] = pt?.cumReturnPct ?? null;
    }
    rows.push(row);
  }
  return rows;
}

/** Padded [min, max] domain for the Y-axis given the (possibly
 * log-scaled) display chart data + the series id list. Falls back to
 * `[-100, 100]` when no numeric data is present. */
export function computeChartYDomain(
  displayChartData: Record<string, string | number | null>[],
  aligned: AlignedResult,
): [number, number] {
  if (!displayChartData.length) return [-100, 100];
  const seriesIds = aligned.series.map((s) => s.id);
  let min = Infinity, max = -Infinity;
  for (const p of displayChartData) {
    for (const id of seriesIds) {
      const v = p[id];
      if (typeof v === 'number') {
        if (v < min) min = v;
        if (v > max) max = v;
      }
    }
  }
  if (min === Infinity || max === -Infinity) return [-100, 100];
  const pad = Math.max((max - min) * 0.05, 5);
  return [Math.floor(min - pad), Math.ceil(max + pad)];
}

// ─── Resolved-series builder ───────────────────────────────────────

/** Build the uniform `ResolvedSeries[]` from the active backtest result
 * + every added `ComparisonItem`. The first series in the returned
 * array is always the active one. Takes the resolved active label as a
 * parameter so the caller can compose it from its own state. */
export function resolveSeries(
  result: BacktestResult,
  comparisons: ComparisonItem[],
  palette: readonly string[],
  activeLabel: string,
): ResolvedSeries[] {
  const out: ResolvedSeries[] = [];
  let colorIdx = 0;
  const nextColor = () => palette[colorIdx++ % palette.length];

  const { map, months } = seriesFromResult(result);
  out.push({
    id: 'active',
    label: activeLabel || 'Strategy',
    color: nextColor(),
    kind: 'active',
    removable: false,
    factorByMonth: map,
    months,
    summary: result.summary,
  });

  // Universe (equal-weight) baseline — a non-removable gray line that
  // shows what a no-skill "hold the entire eligible universe equally"
  // strategy would have returned over the same window. Prefers the
  // daily curve when present (matches the strategy line's
  // granularity); falls back to per-period monthly when not. Skipped
  // when neither source carries data (very old saved runs).
  const universe = seriesFromUniverseBaseline(
    result.monthly_records,
    result.universe_daily_records,
  );
  if (universe.map.size > 0) {
    out.push({
      id: 'universe',
      label: 'Universe (equal-weight)',
      color: chartTheme.universe,
      kind: 'benchmark',
      removable: false,
      factorByMonth: universe.map,
      months: universe.months,
    });
  }

  for (const c of comparisons) {
    if (c.kind === 'saved') {
      const { map, months } = c.daily && c.daily.length > 0
        ? seriesFromResult({ monthly_records: c.monthly, daily_records: c.daily })
        : seriesFromMonthly(c.monthly);
      out.push({
        id: c.id,
        label: c.label,
        color: nextColor(),
        kind: 'saved',
        removable: true,
        factorByMonth: map,
        months,
        summary: c.summary,
      });
    } else {
      const { map, months } = seriesFromPrices(c.prices);
      out.push({
        id: c.id,
        label: c.label,
        color: nextColor(),
        kind: 'benchmark',
        removable: true,
        factorByMonth: map,
        months,
      });
    }
  }
  return out;
}
