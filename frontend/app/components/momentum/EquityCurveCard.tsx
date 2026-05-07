'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, Legend, ReferenceArea,
} from 'recharts';
import type {
  BacktestResult,
  DrawdownPeriod,
  PeriodRecord,
} from '../../../lib/stores/momentum';
import CellInfoTip from './CellInfoTip';
import { SERIES_COLORS, computeTopDrawdowns, fmtPct, tooltipStyle } from './utils';
import type { BenchmarkOption, BenchmarkPrice, ComparisonItem, SavedRun } from './types';

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
};

/** "Equity Curve" card cluster: comparison pill row, summary stats,
 * yearly breakdown + custom-range picker, and the chart itself. Owns its
 * own benchmark/saved comparison state and all the chart-derived memos
 * — the parent only feeds it the active strategy plus the saved-run
 * list. */
export default function EquityCurveCard({ result, loadedRunId, savedRuns }: Props) {
  const [benchmarkOptions, setBenchmarkOptions] = useState<BenchmarkOption[]>([]);
  const [comparisons, setComparisons] = useState<ComparisonItem[]>([]);
  const [addSeriesOpen, setAddSeriesOpen] = useState(false);
  const addSeriesRef = useRef<HTMLDivElement>(null);
  const [logScale, setLogScale] = useState(false);
  const [hoveredDrawdown, setHoveredDrawdown] = useState<number | null>(null);
  const [customFromMonth, setCustomFromMonth] = useState('');

  // Load benchmark options for the "add series" dropdown
  useEffect(() => {
    fetch(`${API_URL}/api/benchmarks`)
      .then((r) => r.json())
      .then((data) => setBenchmarkOptions(data))
      .catch(() => {});
  }, []);

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
    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtests/${runId}`);
      if (!resp.ok) return;
      const data = await resp.json();
      const saved = data.result ?? data;
      const monthly: PeriodRecord[] = saved.monthly_records ?? [];
      const label = data.name ?? `Backtest ${runId}`;
      setComparisons((prev) => [...prev, { id: `saved:${runId}`, kind: 'saved', runId, label, monthly }]);
    } catch {}
  };

  const addBenchmarkSeries = async (benchmarkId: number) => {
    if (comparisons.some((c) => c.kind === 'benchmark' && c.benchmarkId === benchmarkId)) return;
    const opt = benchmarkOptions.find((b) => b.benchmark_id === benchmarkId);
    const label = opt ? opt.ticker : `Benchmark ${benchmarkId}`;
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
    } catch {}
  };

  const removeSeries = (id: string) => {
    setComparisons((prev) => prev.filter((c) => c.id !== id));
  };

  // Resolve every series into a (YYYY-MM → growth factor) map, rebased to 1.0
  // at the series' first observed month. The factor embeds the forward return
  // earned during each month, so factor[month[i]] == cumProduct of returns
  // through month[i] (consistent with strategy `cumulative_return_pct`).
  type ResolvedSeries = {
    id: string;
    label: string;
    color: string;
    kind: 'active' | 'saved' | 'benchmark';
    removable: boolean;
    factorByMonth: Map<string, number>;
    months: string[]; // sorted
  };

  const resolvedSeries = useMemo<ResolvedSeries[]>(() => {
    const out: ResolvedSeries[] = [];
    let colorIdx = 0;
    const nextColor = () => SERIES_COLORS[colorIdx++ % SERIES_COLORS.length];

    const fromMonthly = (monthly: PeriodRecord[]): { map: Map<string, number>; months: string[] } => {
      const map = new Map<string, number>();
      const months: string[] = [];
      for (const r of monthly) {
        const factor = 1 + r.cumulative_return_pct / 100;
        map.set(r.date, factor);
        months.push(r.date);
      }
      return { map, months };
    };

    const fromPrices = (prices: BenchmarkPrice[]): { map: Map<string, number>; months: string[] } => {
      // Pick first price per month.
      const firstByMonth = new Map<string, number>();
      for (const p of prices) {
        const ym = p.target_date.slice(0, 7);
        if (!firstByMonth.has(ym)) firstByMonth.set(ym, p.price);
      }
      const months = Array.from(firstByMonth.keys()).sort();
      if (months.length === 0) return { map: new Map(), months: [] };
      const map = new Map<string, number>();
      const p0 = firstByMonth.get(months[0])!;
      // Shift index so each month's "factor" reflects return through end of month
      // (same convention as strategy records: cumReturn at month[i] includes
      // the price change month[i] → month[i+1]).
      for (let i = 0; i < months.length - 1; i++) {
        const pn = firstByMonth.get(months[i + 1])!;
        map.set(months[i], pn / p0);
      }
      // Last month: no forward price available — leave out.
      return { map, months: months.slice(0, -1) };
    };

    const { map, months } = fromMonthly(result.monthly_records);
    const activeName = loadedRunId != null
      ? savedRuns.find((r) => r.run_id === loadedRunId)?.name
      : undefined;
    out.push({
      id: 'active',
      label: activeName || 'Strategy',
      color: nextColor(),
      kind: 'active',
      removable: false,
      factorByMonth: map,
      months,
    });

    for (const c of comparisons) {
      if (c.kind === 'saved') {
        const { map, months } = fromMonthly(c.monthly);
        out.push({
          id: c.id,
          label: c.label,
          color: nextColor(),
          kind: 'saved',
          removable: true,
          factorByMonth: map,
          months,
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
  }, [result, comparisons, loadedRunId, savedRuns]);

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
      const baseFactor = s.factorByMonth.get(maxStart);
      const points: SeriesPoint[] = allMonths.map((m) => {
        const f = s.factorByMonth.get(m);
        if (f == null || baseFactor == null) return { date: m, cumReturnPct: null };
        return { date: m, cumReturnPct: (f / baseFactor - 1) * 100 };
      });

      // Monthly rebased returns for stats.
      const monthlyRets: number[] = [];
      let prev: number | null = null;
      for (const p of points) {
        if (p.cumReturnPct == null) continue;
        const factor = 1 + p.cumReturnPct / 100;
        if (prev != null && prev > 0) monthlyRets.push((factor / prev - 1) * 100);
        prev = factor;
      }
      const lastNonNull = [...points].reverse().find((p) => p.cumReturnPct != null);
      const totalReturn = lastNonNull?.cumReturnPct ?? 0;
      const years = monthlyRets.length / 12;
      const cumFactor = 1 + totalReturn / 100;
      const annualized = years > 0 ? (Math.pow(cumFactor, 1 / years) - 1) * 100 : 0;

      let peak = 1.0, maxDd = 0, factor = 1.0;
      for (const r of monthlyRets) {
        factor *= (1 + r / 100);
        peak = Math.max(peak, factor);
        const dd = (factor / peak - 1) * 100;
        maxDd = Math.min(maxDd, dd);
      }

      let sharpe: number | null = null;
      if (monthlyRets.length >= 12) {
        const mean = monthlyRets.reduce((a, b) => a + b, 0) / monthlyRets.length;
        const std = Math.sqrt(monthlyRets.reduce((a, b) => a + (b - mean) ** 2, 0) / monthlyRets.length);
        if (std > 0) sharpe = (mean / std) * Math.sqrt(12);
      }

      const ddValues = points
        .filter((p) => p.cumReturnPct != null)
        .map((p) => ({ date: p.date, value: 1 + (p.cumReturnPct as number) / 100 }));
      const topDrawdowns = computeTopDrawdowns(ddValues, 3);

      return {
        ...s,
        points,
        stats: { totalReturn, annualized, maxDd, sharpe, months: monthlyRets.length },
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
      const lastByYear = new Map<string, number>();
      for (const p of s.points) {
        if (p.cumReturnPct == null) continue;
        const y = p.date.slice(0, 4);
        lastByYear.set(y, p.cumReturnPct);
      }
      const ys = Array.from(lastByYear.keys()).sort();
      let prev = 0;
      const rowMap: Record<string, number | null> = {};
      for (const y of ys) {
        const cum = lastByYear.get(y)!;
        rowMap[y] = ((1 + cum / 100) / (1 + prev / 100) - 1) * 100;
        prev = cum;
        yearsSet.add(y);
      }
      bySeries[s.id] = rowMap;
    }

    const years = Array.from(yearsSet).sort();
    // Backfill missing years with null so the column count matches.
    for (const s of series) {
      for (const y of years) if (!(y in bySeries[s.id])) bySeries[s.id][y] = null;
    }
    return { years, bySeries };
  }, [alignedSeries]);

  // Cumulative return from customFromMonth through end of aligned window, per series.
  const customRangeReturn = useMemo(() => {
    const { series } = alignedSeries;
    if (!customFromMonth || series.length === 0) return null;
    const last = series[0].points[series[0].points.length - 1];
    if (!last) return null;
    const perSeries = series.map((s) => {
      let start: number | null = null;
      let end: number | null = null;
      for (const p of s.points) {
        if (p.cumReturnPct == null) continue;
        if (p.date < customFromMonth) start = p.cumReturnPct;
        end = p.cumReturnPct;
      }
      if (end == null) return { id: s.id, label: s.label, color: s.color, ret: null };
      const s0 = start ?? 0;
      return { id: s.id, label: s.label, color: s.color, ret: ((1 + end / 100) / (1 + s0 / 100) - 1) * 100 };
    });
    return { perSeries, fromDate: customFromMonth, toDate: last.date };
  }, [alignedSeries, customFromMonth]);

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
          {alignedSeries.series.map((s) => (
            <span
              key={s.id}
              className="inline-flex items-center gap-2 bg-[#0f1117] border border-gray-800 rounded-full pl-2 pr-1 py-1 text-xs"
            >
              <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
              <span className="text-gray-200">{s.label}</span>
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
            </span>
          ))}
          <div className="relative" ref={addSeriesRef}>
            <button
              type="button"
              onClick={() => setAddSeriesOpen((o) => !o)}
              className="inline-flex items-center gap-1 text-xs text-indigo-300 hover:text-indigo-200 border border-indigo-500/40 hover:border-indigo-400/60 bg-indigo-500/10 hover:bg-indigo-500/20 rounded-full px-3 py-1 transition-colors"
            >
              + Add series
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
      <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800/40 text-gray-500 text-xs">
              <th className="px-4 py-2.5 text-left font-medium"></th>
              <th className="px-3 py-2.5 text-right font-medium">
                Total Return<CellInfoTip>Cumulative return over the entire backtest period: (1 + r₁)(1 + r₂)…(1 + rₙ) − 1.</CellInfoTip>
              </th>
              <th className="px-3 py-2.5 text-right font-medium">
                Annualized<CellInfoTip>Geometric annual return: (1 + total_return)^(1/years) − 1, where years = months ÷ 12.</CellInfoTip>
              </th>
              <th className="px-3 py-2.5 text-right font-medium">
                Max Drawdown<CellInfoTip>Largest peak-to-trough decline observed during the backtest, expressed as a negative percentage of the prior peak.</CellInfoTip>
              </th>
              <th className="px-3 py-2.5 text-right font-medium">
                Sharpe<CellInfoTip>Annualized Sharpe ratio of monthly returns (risk-free rate = 0): mean ÷ std × √12. Computed only when ≥12 months of returns are available.</CellInfoTip>
              </th>
              <th className="px-3 py-2.5 text-right font-medium">
                Months<CellInfoTip>Number of monthly rebalance periods in the backtest.</CellInfoTip>
              </th>
            </tr>
          </thead>
          <tbody>
            {alignedSeries.series.map((s) => (
              <tr key={s.id} className="border-b border-gray-800/30">
                <td className="px-4 py-2.5 font-medium flex items-center gap-2">
                  <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                  <span className="text-gray-200">{s.label}</span>
                </td>
                <td className={`px-3 py-2.5 text-right font-mono ${s.stats.totalReturn >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(s.stats.totalReturn)}</td>
                <td className={`px-3 py-2.5 text-right font-mono ${s.stats.annualized >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(s.stats.annualized)}</td>
                <td className="px-3 py-2.5 text-right font-mono text-rose-400">{fmtPct(s.stats.maxDd)}</td>
                <td className="px-3 py-2.5 text-right font-mono text-white">{s.stats.sharpe != null ? s.stats.sharpe.toFixed(2) : '—'}</td>
                <td className="px-3 py-2.5 text-right font-mono text-gray-300">{s.stats.months}</td>
              </tr>
            ))}
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
      </div>

      {/* Yearly Performance + Custom Range */}
      {yearlyBreakdown.years.length > 0 && (
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
          <div className="px-5 py-3 border-b border-gray-800/40">
            <h3 className="text-white text-sm font-medium">Yearly Performance</h3>
          </div>
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
                      return (
                        <td
                          key={s.id}
                          className={`px-3 py-2 text-right font-mono ${v != null ? (v >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}
                        >
                          {v != null ? fmtPct(v) : '—'}
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
        </div>
      )}

      {/* Equity Curve */}
      <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-white text-sm font-medium">Equity Curve ({logScale ? 'Log' : 'Cumulative'} Return %)</h3>
          <label className="flex items-center gap-2 text-xs text-gray-400 cursor-pointer select-none">
            <input
              type="checkbox"
              checked={logScale}
              onChange={(e) => setLogScale(e.target.checked)}
              className="accent-indigo-500 w-3.5 h-3.5"
            />
            Log scale
          </label>
        </div>
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
      </div>
    </>
  );
}
