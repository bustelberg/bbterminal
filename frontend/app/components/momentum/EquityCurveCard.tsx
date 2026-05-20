'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, Legend, ReferenceArea,
} from 'recharts';
import type {
  BacktestResult,
  DailyRecord,
  DrawdownPeriod,
  PeriodRecord,
  Summary,
} from '../../../lib/stores/momentum';
import type { Column } from '../../../lib/tableExport';
import TableDownloadButton from '../TableDownloadButton';
import CellInfoTip from './CellInfoTip';
import CollapsibleCard from './CollapsibleCard';
import { SERIES_COLORS, computeTopDrawdowns, fmtPct, tooltipStyle } from './utils';
import type { BenchmarkOption, BenchmarkPrice, ComparisonItem, SavedRun, SavedVariant } from './types';
import { buildFeeMap, computeNetStats, parenPct, type NetStats } from './feeStats';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type Props = {
  /** Active backtest result (the strategy being run). The card is only
   * meaningful when this is set; the parent guards on `result &&`. */
  result: BacktestResult;
  /** Loaded saved run id, if the active strategy was loaded from disk —
   * used to label the active series and to disable that row in the
   * "add saved" dropdown. */
  loadedRunId: number | null;
  /** Saved backtests available for comparison. */
  savedRuns: SavedRun[];
  /** Per-company exchange lookup. When provided alongside non-zero
   * `/api/exchange-fees`, every stat surfaced on the active strategy
   * gets a `gross (net)` parenthetical computed from the fee model in
   * `feeStats.ts`. Optional so the card stays usable when the parent
   * doesn't pipe it through (e.g. saved-bundle reload flows). */
  exchangeByCompany?: Map<number, string>;
  /** Full label for the active strategy row — same format as a saved
   * comparison's label (e.g. "ACWI-mei · Momentum · 2002-2026 · Every
   * 12 months · Long-only"). Parent computes this from the actual
   * strategy params + active variant key so the row reads like a saved
   * backtest rather than a generic "Strategy" placeholder. */
  activeStrategyLabel?: string;
};

/** "Equity Curve" card cluster: comparison pill row, summary stats,
 * yearly breakdown + custom-range picker, and the chart itself. Owns its
 * own benchmark/saved comparison state and all the chart-derived memos
 * — the parent only feeds it the active strategy plus the saved-run
 * list. */
export default function EquityCurveCard({ result, loadedRunId, savedRuns, exchangeByCompany, activeStrategyLabel }: Props) {
  const [benchmarkOptions, setBenchmarkOptions] = useState<BenchmarkOption[]>([]);
  const [comparisons, setComparisons] = useState<ComparisonItem[]>([]);
  // Per-exchange fees (bps) for the (net) parenthetical. Fetched once
  // on mount; null when the user hasn't configured any non-zero fees.
  const [feesByExchange, setFeesByExchange] = useState<Map<string, number> | null>(null);
  const [addSeriesOpen, setAddSeriesOpen] = useState(false);
  const addSeriesRef = useRef<HTMLDivElement>(null);
  const [logScale, setLogScale] = useState(false);
  const [hoveredDrawdown, setHoveredDrawdown] = useState<number | null>(null);
  const [customFromMonth, setCustomFromMonth] = useState('');
  // Identifier of an "Add series" operation currently in flight — used to
  // show a small spinner pill while we fetch the backend payload. Cleared
  // once the comparison item lands in `comparisons`.
  const [addingSeriesId, setAddingSeriesId] = useState<string | null>(null);

  // Load benchmark options for the "add series" dropdown
  useEffect(() => {
    fetch(`${API_URL}/api/benchmarks`)
      .then((r) => r.json())
      .then((data) => setBenchmarkOptions(data))
      .catch(() => {});
  }, []);

  // Load per-exchange fees once on mount. Stays null when nothing is
  // configured so all the `parenPct(...)` calls below render as empty
  // strings (no parens, no visual noise).
  useEffect(() => {
    fetch(`${API_URL}/api/exchange-fees`)
      .then((r) => (r.ok ? r.json() : []))
      .then((rows) => {
        const m = buildFeeMap(rows ?? []);
        setFeesByExchange(m.size > 0 ? m : null);
      })
      .catch(() => {});
  }, []);

  // Net stats for the active strategy + every saved comparison. We
  // reuse the parent's `exchangeByCompany` as the lookup for all of
  // them — its fallback fetch hits `/api/companies` which covers the
  // entire directory, so a saved run on a different universe still
  // resolves its holdings' exchanges. Benchmarks have no holdings to
  // trade so they always sit out (net == gross by definition).
  const activeNetStats = useMemo<NetStats | null>(() => {
    if (!feesByExchange || !exchangeByCompany) return null;
    return computeNetStats(result.monthly_records, feesByExchange, exchangeByCompany, result.daily_records);
  }, [feesByExchange, exchangeByCompany, result.monthly_records, result.daily_records]);

  /** Map of comparison series id → NetStats. Computed per-comparison so
   * saved runs added via "Add series" get the same `gross (net)` treatment
   * the active row does. Benchmarks aren't keyed here at all. */
  const comparisonNetStats = useMemo<Map<string, NetStats | null>>(() => {
    const m = new Map<string, NetStats | null>();
    if (!feesByExchange || !exchangeByCompany) return m;
    for (const c of comparisons) {
      if (c.kind !== 'saved') continue;
      m.set(c.id, computeNetStats(c.monthly, feesByExchange, exchangeByCompany, c.daily));
    }
    return m;
  }, [comparisons, feesByExchange, exchangeByCompany]);

  // Close "add series" dropdown on outside click
  useEffect(() => {
    if (!addSeriesOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (addSeriesRef.current && !addSeriesRef.current.contains(e.target as Node)) {
        setAddSeriesOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [addSeriesOpen]);

  // Helpers to add/remove comparison series
  const addSavedSeries = async (runId: number) => {
    if (comparisons.some((c) => c.kind === 'saved' && c.runId === runId)) return;
    setAddingSeriesId(`saved:${runId}`);
    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtests/${runId}`);
      if (!resp.ok) return;
      const data = await resp.json();
      const saved = data.result ?? data;
      // Variant bundles (Random multi-trial sweeps, frequency sweeps, etc.)
      // store records under each variant rather than at the top level. Pick
      // the first variant — mirrors how loadBacktest chooses `firstKey`
      // when rehydrating a sweep as the active strategy.
      const baseLabel = data.name ?? `Backtest ${runId}`;
      let monthly: PeriodRecord[] = [];
      let daily: DailyRecord[] | undefined;
      let summary: Summary | undefined;
      let label = baseLabel;
      let allVariants: SavedVariant[] | undefined;
      let variantIndex: number | undefined;
      if (saved?.kind === 'variants' && Array.isArray(saved.variants) && saved.variants.length > 0) {
        allVariants = saved.variants as SavedVariant[];
        variantIndex = 0;
        const v = allVariants[0];
        monthly = v?.monthly_records ?? [];
        daily = v?.daily_records;
        summary = v?.summary;
        if (v?.label) label = `${baseLabel} · ${v.label}`;
      } else {
        monthly = saved.monthly_records ?? [];
        daily = saved.daily_records;
        summary = saved.summary;
      }
      setComparisons((prev) => [
        ...prev,
        {
          id: `saved:${runId}`,
          kind: 'saved',
          runId,
          label,
          monthly,
          daily,
          summary,
          allVariants,
          variantIndex,
          baseLabel,
        },
      ]);
    } catch {} finally {
      setAddingSeriesId(null);
    }
  };

  const addBenchmarkSeries = async (benchmarkId: number) => {
    if (comparisons.some((c) => c.kind === 'benchmark' && c.benchmarkId === benchmarkId)) return;
    const opt = benchmarkOptions.find((b) => b.benchmark_id === benchmarkId);
    const label = opt ? opt.ticker : `Benchmark ${benchmarkId}`;
    setAddingSeriesId(`bench:${benchmarkId}`);
    // Widen fetch window so any later series can still overlap.
    try {
      const resp = await fetch(
        `${API_URL}/api/benchmarks/${benchmarkId}/prices?start_date=1990-01-01&end_date=2099-12-31`,
      );
      if (!resp.ok) return;
      const prices: BenchmarkPrice[] = await resp.json();
      setComparisons((prev) => [
        ...prev,
        { id: `bench:${benchmarkId}`, kind: 'benchmark', benchmarkId, label, prices },
      ]);
    } catch {} finally {
      setAddingSeriesId(null);
    }
  };

  const removeSeries = (id: string) => {
    setComparisons((prev) => prev.filter((c) => c.id !== id));
  };

  /** Switch which variant of a saved variant-bundle comparison is overlaid.
   * No-op for benchmarks or saved single-runs (which don't carry
   * `allVariants`). The series's label/monthly/daily/summary are rederived
   * from the chosen variant in place, preserving the badge's slot and color. */
  const switchVariant = (id: string, newIndex: number) => {
    setComparisons((prev) =>
      prev.map((c) => {
        if (c.id !== id || c.kind !== 'saved' || !c.allVariants || !c.allVariants[newIndex]) return c;
        const v = c.allVariants[newIndex];
        const base = c.baseLabel ?? c.label;
        return {
          ...c,
          variantIndex: newIndex,
          monthly: v.monthly_records ?? [],
          daily: v.daily_records,
          summary: v.summary,
          label: v.label ? `${base} · ${v.label}` : base,
        };
      }),
    );
  };

  // One global "variant picker open" per comparison id — keeps clicks
  // outside the picker from leaving multiple pickers open.
  const [variantPickerOpen, setVariantPickerOpen] = useState<string | null>(null);
  const variantPickerRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!variantPickerOpen) return;
    const onDoc = (e: MouseEvent) => {
      if (!variantPickerRef.current?.contains(e.target as Node)) setVariantPickerOpen(null);
    };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, [variantPickerOpen]);

  // Resolve every series into a (date → growth factor) map. Date keys are
  // normalized to YYYY-MM-DD so that string comparison correctly orders dates
  // across mixed cadences — a saved variant with monthly_records ("2002-01")
  // sits at the *end of January* on the timeline, lining up with a daily
  // strategy's "2002-01-31" rather than colliding lexicographically.
  type ResolvedSeries = {
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

  const resolvedSeries = useMemo<ResolvedSeries[]>(() => {
    const out: ResolvedSeries[] = [];
    let colorIdx = 0;
    const nextColor = () => SERIES_COLORS[colorIdx++ % SERIES_COLORS.length];

    // Promote a YYYY-MM monthly_records date to end-of-month YYYY-MM-DD so
    // it can be sorted/compared against daily YYYY-MM-DD dates correctly.
    // Using "28" for Feb is conservative — we just need a stable last-day-
    // of-month-ish anchor, not the exact trading day.
    const endOfMonth = (yyyymm: string): string => {
      if (yyyymm.length !== 7) return yyyymm; // already YYYY-MM-DD
      const [y, m] = yyyymm.split('-').map(Number);
      // JavaScript: new Date(y, m, 0) gives the last day of month m (1-indexed)
      const last = new Date(y, m, 0).getDate();
      return `${yyyymm}-${String(last).padStart(2, '0')}`;
    };

    const fromMonthly = (monthly: PeriodRecord[]): { map: Map<string, number>; months: string[] } => {
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
    };

    /** Prefer daily_records when present so the chart line, max DD, and
     * Sharpe all reflect intra-period moves. Falls back to monthly_records
     * for older saved runs that don't carry the daily curve. Dates are
     * normalized to YYYY-MM-DD either way. */
    const fromResult = (r: BacktestResult): { map: Map<string, number>; months: string[] } => {
      if (r.daily_records && r.daily_records.length > 0) {
        const map = new Map<string, number>();
        const dates: string[] = [];
        for (const d of r.daily_records) {
          map.set(d.date, 1 + d.cumulative_return_pct / 100);
          dates.push(d.date);
        }
        return { map, months: dates };
      }
      return fromMonthly(r.monthly_records);
    };

    const fromPrices = (prices: BenchmarkPrice[]): { map: Map<string, number>; months: string[] } => {
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
    };

    const { map, months } = fromResult(result);
    // Label precedence: the parent's computed full label (which folds in
    // the active variant suffix + uses defaultVariantsBundleName for
    // unsaved runs), then the loaded saved-run name on its own, then a
    // generic placeholder.
    const loadedName = loadedRunId != null
      ? savedRuns.find((r) => r.run_id === loadedRunId)?.name
      : undefined;
    const activeName = activeStrategyLabel || loadedName;
    out.push({
      id: 'active',
      label: activeName || 'Strategy',
      color: nextColor(),
      kind: 'active',
      removable: false,
      factorByMonth: map,
      months,
      summary: result.summary,
    });

    for (const c of comparisons) {
      if (c.kind === 'saved') {
        const { map, months } = c.daily && c.daily.length > 0
          ? fromResult({ monthly_records: c.monthly, summary: {} as Summary, daily_records: c.daily })
          : fromMonthly(c.monthly);
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
        const { map, months } = fromPrices(c.prices);
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
  }, [result, comparisons, loadedRunId, savedRuns, activeStrategyLabel]);

  // Determine the [maxStart, minEnd] alignment window over all series and
  // compute per-series aligned points + summary stats, rebased so each series
  // starts at 0% on windowStart.
  type SeriesPoint = { date: string; cumReturnPct: number | null };
  type SeriesStats = {
    totalReturn: number;
    annualized: number;
    maxDd: number;
    sharpe: number | null;
    months: number;
  };
  type AlignedSeries = ResolvedSeries & {
    points: SeriesPoint[];
    stats: SeriesStats;
    topDrawdowns: DrawdownPeriod[];
  };

  const alignedSeries = useMemo<{ series: AlignedSeries[]; windowStart: string | null; windowEnd: string | null; allMonths: string[] }>(() => {
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
      const periodDates: string[] = [];
      let prev: number | null = null;
      for (const p of points) {
        if (p.cumReturnPct == null) continue;
        const factor = 1 + p.cumReturnPct / 100;
        if (prev != null && prev > 0) {
          periodReturns.push((factor / prev - 1) * 100);
          periodDates.push(p.date);
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
            // Periods reflects observation count in the aligned window
            // (informational, not used for any math now).
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
  }, [resolvedSeries]);

  // Yearly performance breakdown — per-series compound return for each calendar year.
  const yearlyBreakdown = useMemo(() => {
    const { series } = alignedSeries;
    if (series.length === 0) return { years: [] as string[], bySeries: {} as Record<string, Record<string, number | null>> };

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
        const isCompleteFromStart = prevCum !== null || firstMonth === "01";
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

    // Calendar-aligned net yearly per series with holdings (active +
    // every saved comparison). Anchoring on each row's gross yearly
    // value guarantees `net ≤ gross` per row: each closed period
    // contributes a `fee_factor ≤ 1`, and we multiply gross_yearly_Y by
    // the product of fee_factors for periods whose exit_date lands in
    // calendar year Y. Using period-start-bucketed `yearly` straight
    // from NetStats could drift above gross when rebalances don't
    // align to Jan 1 (e.g., monthly cadence rebalancing on day 5).
    const netYearlyBySeries: Record<string, Record<string, number | null>> = {};
    const seriesNetMap: Record<string, NetStats | null> = { active: activeNetStats };
    for (const c of comparisons) {
      if (c.kind === 'saved') seriesNetMap[c.id] = comparisonNetStats.get(c.id) ?? null;
    }
    for (const [seriesId, ns] of Object.entries(seriesNetMap)) {
      const row = bySeries[seriesId];
      if (!row || !ns?.period_drag_factors?.length) continue;
      const dragByYear = new Map<string, number>();
      for (const pdf of ns.period_drag_factors) {
        const y = pdf.exit_date.slice(0, 4);
        dragByYear.set(y, (dragByYear.get(y) ?? 1) * pdf.fee_factor);
      }
      const netRow: Record<string, number | null> = {};
      for (const y of years) {
        const grossY = row[y];
        const drag = dragByYear.get(y) ?? 1;
        netRow[y] = grossY == null ? null : ((1 + grossY / 100) * drag - 1) * 100;
      }
      netYearlyBySeries[seriesId] = netRow;
    }
    return { years, bySeries, netYearlyBySeries };
  }, [alignedSeries, activeNetStats, comparisons, comparisonNetStats]);

  // Export columns for the Summary stats table. One row per aligned
  // series; columns mirror what's visible.
  type AlignedSeriesItem = (typeof alignedSeries)['series'][number];
  const summaryExportColumns = useMemo<Column<AlignedSeriesItem>[]>(() => [
    { key: 'label', header: 'Series', accessor: (s) => s.label },
    { key: 'kind', header: 'Kind', accessor: (s) => s.kind },
    { key: 'total_return_pct', header: 'Total Return (%)', accessor: (s) => s.stats.totalReturn },
    { key: 'annualized_pct', header: 'Annualized (%)', accessor: (s) => s.stats.annualized },
    { key: 'max_drawdown_pct', header: 'Max Drawdown (%)', accessor: (s) => s.stats.maxDd },
    { key: 'sharpe', header: 'Sharpe', accessor: (s) => s.stats.sharpe ?? null },
    { key: 'months', header: 'Periods', accessor: (s) => s.stats.months },
  ], []);

  // Export rows for the Yearly Performance table — one row per
  // (year × series). Long format > wide format for spreadsheets.
  type YearlyExportRow = { year: string; series: string; return_pct: number | null };
  const yearlyExportRows = useMemo<YearlyExportRow[]>(() => {
    const out: YearlyExportRow[] = [];
    for (const y of yearlyBreakdown.years) {
      for (const s of alignedSeries.series) {
        out.push({
          year: y,
          series: s.label,
          return_pct: yearlyBreakdown.bySeries[s.id]?.[y] ?? null,
        });
      }
    }
    return out;
  }, [yearlyBreakdown, alignedSeries]);
  const yearlyExportColumns = useMemo<Column<YearlyExportRow>[]>(() => [
    { key: 'year', header: 'Year', accessor: (r) => r.year },
    { key: 'series', header: 'Series', accessor: (r) => r.series },
    { key: 'return_pct', header: 'Return (%)', accessor: (r) => r.return_pct },
  ], []);

  // Cumulative return from customFromMonth through end of aligned window, per series.
  const customRangeReturn = useMemo(() => {
    const { series } = alignedSeries;
    if (!customFromMonth || series.length === 0) return null;
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
        if (ns.dates[i] < customFromMonth) startFactor = ns.cum_factors[i];
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
        if (p.date < customFromMonth) start = p.cumReturnPct;
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
    return { perSeries, fromDate: customFromMonth, toDate: last.date };
  }, [alignedSeries, customFromMonth, activeNetStats, comparisons, comparisonNetStats]);

  // Chart data — wide-format per month, one key per series id, plus a 0%
  // origin row so every line starts from the same reference point.
  const chartData = useMemo(() => {
    const { series, allMonths, windowStart } = alignedSeries;
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
  }, [alignedSeries]);

  // Log-scale chart data: ln(1 + cumReturn/100) * 100 applied to every series key.
  const displayChartData = useMemo(() => {
    if (!logScale) return chartData;
    const seriesIds = alignedSeries.series.map((s) => s.id);
    return chartData.map((p) => {
      const out: Record<string, string | number | null> = { date: p.date as string };
      for (const id of seriesIds) {
        const v = p[id];
        out[id] = typeof v === 'number' ? Math.log(1 + v / 100) * 100 : null;
      }
      return out;
    });
  }, [chartData, logScale, alignedSeries]);

  // Y-axis domain for chart — used by ReferenceArea to span full height
  const chartYDomain = useMemo<[number, number]>(() => {
    if (!displayChartData.length) return [-100, 100];
    const seriesIds = alignedSeries.series.map((s) => s.id);
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
  }, [displayChartData, alignedSeries]);

  return (
    <>
      {/* Comparison panel — active strategy + any added backtests/benchmarks */}
      <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-4 py-3">
        <div className="flex items-center gap-3 flex-wrap">
          <span className="text-gray-400 text-sm mr-1">Comparison</span>
          {alignedSeries.series.map((s) => {
            // Look up the underlying ComparisonItem so the badge can offer a
            // variant picker when the saved run is a variant bundle.
            const cmp = comparisons.find((c) => c.id === s.id);
            const hasVariantPicker =
              cmp != null && cmp.kind === 'saved' && cmp.allVariants != null && cmp.allVariants.length > 1;
            const pickerOpen = variantPickerOpen === s.id;
            return (
              <span
                key={s.id}
                className="relative inline-flex items-center gap-2 bg-[#0f1117] border border-gray-800 rounded-full pl-2 pr-1 py-1 text-xs"
                ref={pickerOpen ? variantPickerRef : undefined}
              >
                <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                {hasVariantPicker ? (
                  <button
                    type="button"
                    onClick={() => setVariantPickerOpen(pickerOpen ? null : s.id)}
                    className="text-gray-200 hover:text-white inline-flex items-center gap-1"
                    title="Switch variant"
                  >
                    <span>{s.label}</span>
                    <svg className={`w-3 h-3 text-gray-500 transition-transform ${pickerOpen ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                      <path fillRule="evenodd" d="M5.23 7.21a.75.75 0 011.06.02L10 11.06l3.71-3.83a.75.75 0 111.08 1.04l-4.25 4.39a.75.75 0 01-1.08 0L5.21 8.27a.75.75 0 01.02-1.06z" clipRule="evenodd" />
                    </svg>
                  </button>
                ) : (
                  <span className="text-gray-200">{s.label}</span>
                )}
                {s.removable && (
                  <button
                    type="button"
                    onClick={() => removeSeries(s.id)}
                    className="ml-0.5 w-4 h-4 rounded-full text-gray-500 hover:text-rose-400 hover:bg-rose-500/10 transition-colors flex items-center justify-center"
                    title="Remove from comparison"
                  >
                    <svg className="w-2.5 h-2.5" viewBox="0 0 20 20" fill="currentColor">
                      <path fillRule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clipRule="evenodd" />
                    </svg>
                  </button>
                )}
                {hasVariantPicker && pickerOpen && cmp.kind === 'saved' && cmp.allVariants && (
                  <div className="absolute left-0 top-full mt-1 w-64 bg-[#151821] border border-gray-700 rounded-lg shadow-xl z-50 max-h-80 overflow-auto">
                    <div className="px-3 py-1.5 text-[10px] uppercase tracking-wider text-gray-500 border-b border-gray-800/60">
                      Variants ({cmp.allVariants.length})
                    </div>
                    {cmp.allVariants.map((v, idx) => {
                      const isCurrent = idx === (cmp.variantIndex ?? 0);
                      return (
                        <button
                          key={v.key}
                          type="button"
                          disabled={isCurrent}
                          onClick={() => { switchVariant(s.id, idx); setVariantPickerOpen(null); }}
                          className={`w-full text-left px-3 py-2 text-xs hover:bg-white/[0.03] disabled:opacity-100 disabled:cursor-default flex items-center justify-between gap-2 ${isCurrent ? 'bg-indigo-500/10 text-indigo-300' : 'text-gray-200'}`}
                        >
                          <span className="truncate">{v.label}</span>
                          {isCurrent && <span className="text-[10px] text-indigo-400">current</span>}
                        </button>
                      );
                    })}
                  </div>
                )}
              </span>
            );
          })}
          <div className="relative" ref={addSeriesRef}>
            <button
              type="button"
              onClick={() => setAddSeriesOpen((o) => !o)}
              disabled={addingSeriesId != null}
              className="inline-flex items-center gap-1.5 text-xs text-indigo-300 hover:text-indigo-200 border border-indigo-500/40 hover:border-indigo-400/60 bg-indigo-500/10 hover:bg-indigo-500/20 rounded-full px-3 py-1 transition-colors disabled:opacity-70 disabled:cursor-wait"
            >
              {addingSeriesId != null ? (
                <>
                  <svg className="animate-spin w-3 h-3 text-indigo-300" viewBox="0 0 24 24" fill="none">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                  </svg>
                  Adding…
                </>
              ) : (
                <>+ Add series</>
              )}
            </button>
            {addSeriesOpen && (
              <div className="absolute left-0 mt-1 w-72 bg-[#151821] border border-gray-700 rounded-lg shadow-xl z-50 max-h-80 overflow-auto">
                {benchmarkOptions.length > 0 && (
                  <div>
                    <div className="px-3 py-1.5 text-[10px] uppercase tracking-wider text-gray-500 border-b border-gray-800/60">Benchmarks</div>
                    {benchmarkOptions.map((b) => {
                      const already = comparisons.some((c) => c.kind === 'benchmark' && c.benchmarkId === b.benchmark_id);
                      return (
                        <button
                          key={b.benchmark_id}
                          type="button"
                          disabled={already}
                          onClick={() => { addBenchmarkSeries(b.benchmark_id); setAddSeriesOpen(false); }}
                          className="w-full text-left px-3 py-2 text-xs hover:bg-white/[0.03] disabled:opacity-40 disabled:cursor-not-allowed text-gray-200 flex items-center gap-2"
                        >
                          <span className="font-mono text-amber-300">{b.ticker}</span>
                          <span className="text-gray-500 truncate">{b.name}</span>
                          {already && <span className="ml-auto text-[10px] text-gray-600">added</span>}
                        </button>
                      );
                    })}
                  </div>
                )}
                {savedRuns.length > 0 && (
                  <div>
                    <div className="px-3 py-1.5 text-[10px] uppercase tracking-wider text-gray-500 border-t border-b border-gray-800/60">Saved Backtests</div>
                    {savedRuns.map((r) => {
                      const already = comparisons.some((c) => c.kind === 'saved' && c.runId === r.run_id);
                      const isLoaded = r.run_id === loadedRunId;
                      return (
                        <button
                          key={r.run_id}
                          type="button"
                          disabled={already || isLoaded}
                          onClick={() => { addSavedSeries(r.run_id); setAddSeriesOpen(false); }}
                          className="w-full text-left px-3 py-2 text-xs hover:bg-white/[0.03] disabled:opacity-40 disabled:cursor-not-allowed text-gray-200 flex items-center gap-2"
                          title={isLoaded ? 'Currently loaded as the active strategy' : undefined}
                        >
                          <span className="truncate">{r.name}</span>
                          {isLoaded && <span className="ml-auto text-[10px] text-gray-600">active</span>}
                          {!isLoaded && already && <span className="ml-auto text-[10px] text-gray-600">added</span>}
                        </button>
                      );
                    })}
                  </div>
                )}
                {benchmarkOptions.length === 0 && savedRuns.length === 0 && (
                  <div className="px-3 py-4 text-xs text-gray-500">No benchmarks or saved backtests available.</div>
                )}
              </div>
            )}
          </div>
          {alignedSeries.windowStart && alignedSeries.windowEnd && alignedSeries.series.length > 1 && (
            <span className="text-[11px] text-gray-500 font-mono ml-auto">
              aligned {alignedSeries.windowStart} → {alignedSeries.windowEnd}
            </span>
          )}
        </div>
      </div>

      {/* Summary Stats */}
      <CollapsibleCard
        title="Summary"
        rightSlot={
          <TableDownloadButton
            rows={alignedSeries.series}
            columns={summaryExportColumns}
            filename="backtest_summary"
            title={`Download ${alignedSeries.series.length} series as CSV / XLSX`}
          />
        }
      >
        <table className="w-full text-sm border-t border-gray-800/40">
          <thead>
            <tr className="border-b border-gray-800/40 text-gray-500 text-xs">
              <th className="px-4 py-2.5 text-left font-medium"></th>
              <th className="px-3 py-2.5 text-right font-medium">
                Total Return<CellInfoTip>Cumulative return over the entire backtest period: (1 + r₁)(1 + r₂)…(1 + rₙ) − 1.</CellInfoTip>
              </th>
              <th className="px-3 py-2.5 text-right font-medium">
                Annualized<CellInfoTip>Geometric annual return: (1 + total_return)^(1/years) − 1. Years are derived from the actual span of dates in the curve, not the period count.</CellInfoTip>
              </th>
              <th className="px-3 py-2.5 text-right font-medium">
                Max Drawdown<CellInfoTip>Largest peak-to-trough decline observed during the backtest, expressed as a negative percentage of the prior peak. Computed daily when the strategy ships a daily curve (so intra-month moves on a monthly strategy are caught), monthly otherwise.</CellInfoTip>
              </th>
              <th className="px-3 py-2.5 text-right font-medium">
                Sharpe<CellInfoTip>Annualized Sharpe ratio of period returns (risk-free rate = 0): mean ÷ std × √(periods/year). Auto-detects cadence — daily curves use √252, monthly √12. Computed only when at least one full year of observations is available.</CellInfoTip>
              </th>
              <th className="px-3 py-2.5 text-right font-medium">
                Periods<CellInfoTip>Number of return observations in the aligned window. Equals trading days when the curve is daily, calendar months when the curve is monthly.</CellInfoTip>
              </th>
            </tr>
          </thead>
          <tbody>
            {alignedSeries.series.map((s) => {
              // The (net) parenthetical applies to the active strategy
              // AND every saved comparison — both have holdings whose
              // exchanges we can resolve via `exchangeByCompany`.
              // Benchmarks (kind === 'benchmark') have no holdings to
              // trade so they always render gross-only.
              const net = s.kind === 'active'
                ? activeNetStats
                : s.kind === 'saved'
                  ? comparisonNetStats.get(s.id) ?? null
                  : null;
              return (
              <tr key={s.id} className="border-b border-gray-800/30">
                <td className="px-4 py-2.5 font-medium flex items-center gap-2">
                  <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                  <span className="text-gray-200">{s.label}</span>
                </td>
                <td className={`px-3 py-2.5 text-right font-mono ${s.stats.totalReturn >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(s.stats.totalReturn)}<span className="text-gray-500">{parenPct(net?.total_return_pct)}</span></td>
                <td className={`px-3 py-2.5 text-right font-mono ${s.stats.annualized >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(s.stats.annualized)}<span className="text-gray-500">{parenPct(net?.annualized_return_pct)}</span></td>
                <td className="px-3 py-2.5 text-right font-mono text-rose-400">{fmtPct(s.stats.maxDd)}<span className="text-gray-500">{parenPct(net?.max_drawdown_pct)}</span></td>
                <td className="px-3 py-2.5 text-right font-mono text-white">{s.stats.sharpe != null ? s.stats.sharpe.toFixed(2) : '—'}<span className="text-gray-500">{net?.sharpe_ratio != null ? ` (${net.sharpe_ratio.toFixed(2)})` : ''}</span></td>
                <td className="px-3 py-2.5 text-right font-mono text-gray-300">{s.stats.months}</td>
              </tr>
              );
            })}
          </tbody>
        </table>
        {/* Active strategy — raw (non-aligned) metrics, including turnover + holdings */}
        <div className="px-4 py-3 border-t border-gray-800/40 text-xs text-gray-500 flex flex-wrap gap-x-6 gap-y-1">
          <span>Strategy (full range): <span className="font-mono text-gray-300">Turnover {fmtPct(result.summary.avg_monthly_turnover_pct)}</span></span>
          <span><span className="font-mono text-gray-300">Avg Holdings {result.summary.avg_holdings.toFixed(1)}</span></span>
          <span><span className="font-mono text-gray-300">Months {result.summary.total_months}</span></span>
        </div>
        {/* Multi-trial cross-trial statistics — backend means ± std.
            These are the numbers to compare a momentum run against,
            NOT the per-series stats above (which derive from the mean
            equity curve and understate volatility). */}
        {result.summary.n_trials != null && result.summary.n_trials > 1 && (
          <div className="px-4 py-3 border-t border-gray-800/40">
            <div className="text-xs font-medium text-gray-400 mb-2">
              Cross-trial statistics ({result.summary.n_trials} random trials, mean ± std)
            </div>
            <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-xs">
              <div className="bg-[#0f1117] rounded-lg px-3 py-2">
                <div className="text-gray-500">Total Return</div>
                <div className="font-mono text-gray-200">
                  {fmtPct(result.summary.total_return_pct)}
                  <span className="text-gray-500"> ± {(result.summary.total_return_pct_std ?? 0).toFixed(2)}%</span>
                </div>
              </div>
              <div className="bg-[#0f1117] rounded-lg px-3 py-2">
                <div className="text-gray-500">Annualized</div>
                <div className="font-mono text-gray-200">
                  {fmtPct(result.summary.annualized_return_pct)}
                  <span className="text-gray-500"> ± {(result.summary.annualized_return_pct_std ?? 0).toFixed(2)}%</span>
                </div>
              </div>
              <div className="bg-[#0f1117] rounded-lg px-3 py-2">
                <div className="text-gray-500">Max Drawdown</div>
                <div className="font-mono text-gray-200">
                  {fmtPct(result.summary.max_drawdown_pct)}
                  <span className="text-gray-500"> ± {(result.summary.max_drawdown_pct_std ?? 0).toFixed(2)}%</span>
                </div>
              </div>
              <div className="bg-[#0f1117] rounded-lg px-3 py-2">
                <div className="text-gray-500">Sharpe</div>
                <div className="font-mono text-gray-200">
                  {result.summary.sharpe_ratio != null ? result.summary.sharpe_ratio.toFixed(2) : '—'}
                  {result.summary.sharpe_ratio_std != null && (
                    <span className="text-gray-500"> ± {result.summary.sharpe_ratio_std.toFixed(2)}</span>
                  )}
                </div>
              </div>
              <div className="bg-[#0f1117] rounded-lg px-3 py-2">
                <div className="text-gray-500">Turnover</div>
                <div className="font-mono text-gray-200">
                  {fmtPct(result.summary.avg_monthly_turnover_pct)}
                  <span className="text-gray-500"> ± {(result.summary.avg_monthly_turnover_pct_std ?? 0).toFixed(2)}%</span>
                </div>
              </div>
            </div>
          </div>
        )}
        {alignedSeries.series.some((s) => s.topDrawdowns.length > 0) && (
          <div className="px-4 py-3 border-t border-gray-800/40 space-y-3">
            {alignedSeries.series.map((s) => (
              s.topDrawdowns.length > 0 && (
                <div key={s.id}>
                  <div className="text-xs font-medium mb-2 flex items-center gap-2">
                    <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                    <span className="text-gray-400">{s.label} — Top Drawdowns</span>
                  </div>
                  <div className="grid grid-cols-3 gap-3">
                    {s.topDrawdowns.map((dd, i) => {
                      const alpha = [1.0, 0.6, 0.3][i] ?? 0.3;
                      return (
                        <div key={i} className="bg-[#0f1117] rounded-lg px-3 py-2">
                          <div className="flex items-center gap-2 mb-1">
                            <div className="w-2 h-2 rounded-full" style={{ background: s.color, opacity: alpha }} />
                            <span className="font-mono text-sm font-medium" style={{ color: s.color }}>{dd.drawdown_pct.toFixed(1)}%</span>
                          </div>
                          <div className="text-[10px] text-gray-500 font-mono">
                            {dd.peak_date} to {dd.trough_date}
                            {dd.recovery_date ? ` (recovered ${dd.recovery_date})` : ' (ongoing)'}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )
            ))}
          </div>
        )}
      </CollapsibleCard>

      {/* Yearly Performance + Custom Range */}
      {yearlyBreakdown.years.length > 0 && (
        <CollapsibleCard
          title="Yearly Performance"
          rightSlot={
            <TableDownloadButton
              rows={yearlyExportRows}
              columns={yearlyExportColumns}
              filename="backtest_yearly_performance"
              title={`Download ${yearlyExportRows.length} years × ${alignedSeries.series.length} series as CSV / XLSX`}
            />
          }
        >
          <div className="overflow-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-800/40 text-gray-500 text-xs">
                  <th className="px-5 py-2.5 text-left font-medium">
                    Year<CellInfoTip>Calendar year. Each cell shows the series&apos; cumulative return across that year (Jan 1 → Dec 31, or partial for the latest year).</CellInfoTip>
                  </th>
                  {alignedSeries.series.map((s) => (
                    <th key={s.id} className="px-3 py-2.5 text-right font-medium">
                      <span className="inline-flex items-center gap-1.5">
                        <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: s.color }} />
                        <span className="truncate max-w-[140px]">{s.label}</span>
                      </span>
                      <CellInfoTip>Annual return for this series. Strategy returns chain-link the monthly portfolio returns; benchmark returns chain-link daily closes.</CellInfoTip>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {yearlyBreakdown.years.map((y) => (
                  <tr key={y} className="border-b border-gray-800/20 hover:bg-white/[0.02]">
                    <td className="px-5 py-2 text-gray-200 font-mono">{y}</td>
                    {alignedSeries.series.map((s) => {
                      const v = yearlyBreakdown.bySeries[s.id]?.[y];
                      // Active strategy + saved comparisons both get
                      // the (net) parenthetical when fees are
                      // configured; benchmarks stay gross-only.
                      // Uses `netYearlyBySeries` (gross × per-year
                      // fee-factor drag) rather than NetStats.yearly so
                      // the parenthetical can never exceed displayed
                      // gross — the period-start-bucketed `yearly` could
                      // drift above gross for rebalances that don't
                      // align to Jan 1.
                      const netY = s.kind !== 'benchmark'
                        ? yearlyBreakdown.netYearlyBySeries?.[s.id]?.[y]
                        : undefined;
                      return (
                        <td
                          key={s.id}
                          className={`px-3 py-2 text-right font-mono ${v != null ? (v >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}
                        >
                          {v != null ? fmtPct(v) : '—'}
                          {netY != null && <span className="text-gray-500">{parenPct(netY)}</span>}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="px-5 py-3 border-t border-gray-800/40 flex items-center gap-4 flex-wrap">
            <label className="text-xs text-gray-400 font-medium">From month:</label>
            <input
              type="month"
              value={customFromMonth}
              min={alignedSeries.windowStart ?? undefined}
              max={alignedSeries.windowEnd ?? undefined}
              onChange={(e) => setCustomFromMonth(e.target.value)}
              className="bg-[#0f1117] border border-gray-700 rounded-lg px-2 py-1 text-xs text-gray-200 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
            />
            {customFromMonth && (
              <button
                onClick={() => setCustomFromMonth('')}
                className="text-xs text-gray-500 hover:text-gray-300 transition-colors"
              >
                clear
              </button>
            )}
            {customRangeReturn ? (
              <div className="flex items-center gap-4 text-xs ml-auto flex-wrap">
                <span className="text-gray-500 font-mono">{customRangeReturn.fromDate} → {customRangeReturn.toDate}</span>
                {customRangeReturn.perSeries.map((s) => (
                  <span key={s.id} className="text-gray-400 inline-flex items-center gap-1.5">
                    <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: s.color }} />
                    {s.label}:{' '}
                    {s.ret != null ? (
                      <span className={`font-mono ${s.ret >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                        {fmtPct(s.ret)}
                        {s.netRet != null && <span className="text-gray-500">{parenPct(s.netRet)}</span>}
                      </span>
                    ) : (
                      <span className="font-mono text-gray-600">—</span>
                    )}
                  </span>
                ))}
              </div>
            ) : (
              <span className="text-xs text-gray-500">Cumulative return from picked month through end of aligned window.</span>
            )}
          </div>
        </CollapsibleCard>
      )}

      {/* Equity Curve */}
      <CollapsibleCard
        title={`Equity Curve (${logScale ? 'Log' : 'Cumulative'} Return %)`}
        rightSlot={
          <label
            className="flex items-center gap-2 cursor-pointer select-none"
            onClick={(e) => e.stopPropagation()}
          >
            <input
              type="checkbox"
              checked={logScale}
              onChange={(e) => setLogScale(e.target.checked)}
              className="accent-indigo-500 w-3.5 h-3.5"
            />
            Log scale
          </label>
        }
        bodyClassName="px-5 pb-5"
      >
        <ResponsiveContainer width="100%" height={350}>
          <LineChart data={displayChartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
            <XAxis
              dataKey="date"
              tick={{ fill: '#6b7280', fontSize: 11 }}
              tickLine={false}
              interval={Math.max(0, Math.floor(displayChartData.length / 12) - 1)}
            />
            <YAxis
              tick={{ fill: '#6b7280', fontSize: 11 }}
              tickLine={false}
              tickFormatter={(v: number) => `${v}%`}
              domain={chartYDomain}
            />
            <Tooltip
              {...tooltipStyle}
              formatter={(value, name) => {
                const v = Number(value);
                const s = alignedSeries.series.find((x) => x.id === name);
                return [`${v >= 0 ? '+' : ''}${v.toFixed(2)}%`, s?.label ?? String(name)];
              }}
            />
            {alignedSeries.series.length > 1 && (
              <Legend
                wrapperStyle={{ fontSize: 12, color: '#9ca3af' }}
                formatter={(value) => {
                  const s = alignedSeries.series.find((x) => x.id === value);
                  return s?.label ?? String(value);
                }}
              />
            )}
            {/* Drawdown overlays: only for the active strategy (first series) */}
            {alignedSeries.series[0]?.topDrawdowns.map((dd, i) => {
              const base = [0.25, 0.15, 0.10];
              const hovered = hoveredDrawdown === i;
              const opacity = hovered ? (base[i] ?? 0.10) + 0.15 : (base[i] ?? 0.10);
              return (
                <ReferenceArea
                  key={`dd-${i}`}
                  x1={dd.peak_date}
                  x2={dd.recovery_date ?? (displayChartData[displayChartData.length - 1]?.date as string | undefined)}
                  y1={chartYDomain[0]}
                  y2={chartYDomain[1]}
                  fill={`rgba(244,63,94,${opacity})`}
                  strokeOpacity={0}
                  style={{ cursor: 'pointer' }}
                  onMouseEnter={() => setHoveredDrawdown(i)}
                  onMouseLeave={() => setHoveredDrawdown(null)}
                />
              );
            })}
            {alignedSeries.series.map((s, i) => (
              <Line
                key={s.id}
                type="monotone"
                dataKey={s.id}
                stroke={s.color}
                strokeWidth={i === 0 ? 2 : 1.5}
                strokeDasharray={i === 0 ? undefined : '4 3'}
                dot={false}
                name={s.id}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </CollapsibleCard>
    </>
  );
}
