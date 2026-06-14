'use client';

import { memo, useMemo, useRef, useState } from 'react';
import dynamic from 'next/dynamic';
import type {
  BacktestResult,
  DailyRecord,
  PeriodRecord,
  Summary,
} from '../../../lib/stores/momentum';
import type { Column } from '../../../lib/tableExport';
import { SERIES_COLORS } from './utils';
import type { BenchmarkOption, BenchmarkPrice, ComparisonItem, SavedRun, SavedVariant } from './types';
import { type NetStats } from './feeStats';
import { computeFeeWaterfall, type FeeBreakdownRow, type FeeWaterfall } from './feeModel';
import { useClickOutside } from '../../../lib/hooks/useClickOutside';
import { useBenchmarks, useFeeConfig } from '../../../lib/hooks/apiData';
import { API_URL } from '../../../lib/apiUrl';
import { apiFetch } from '../../../lib/apiFetch';
import {
  alignSeries,
  buildChartData,
  computeChartYDomain,
  computeCustomRangeReturn,
  computeYearlyBreakdown,
  computeYearlySubplots,
  resolveSeries,
} from './equityCurve/seriesMath';
// Recharts is ~100KB gzipped and only used by this chart. next/dynamic splits
// it into a chunk that ships only when the user actually runs a backtest and
// EquityChart mounts. ssr:false because Recharts uses browser-only ResizeObserver.
const EquityChart = dynamic(() => import('./equityCurve/EquityChart'), { ssr: false });
import YearlyBreakdown from './equityCurve/YearlyBreakdown';
import SummaryStats from './equityCurve/SummaryStats';
import CollapsibleCard from './CollapsibleCard';
// Recharts is split off into the EquityChart chunk above; the yearly
// subplots grid uses the same library, so dynamic-loading it keeps the
// initial /backtest bundle untouched even though the grid renders
// inline below the main chart.
const YearlySubplotsGrid = dynamic(() => import('./equityCurve/YearlySubplotsGrid'), { ssr: false });

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
  /** Full label for the active strategy row — same format as a saved
   * comparison's label (e.g. "ACWI-mei · Momentum · 2002-2026 · Every
   * 12 months · Long-only"). Parent computes this from the actual
   * strategy params + active variant key so the row reads like a saved
   * backtest rather than a generic "Strategy" placeholder. */
  activeStrategyLabel?: string;
  /** Optional "go-live" date (YYYY-MM-DD) drawn as a red dashed vertical
   * marker on the equity chart. Used by /schedule to mark where a
   * scheduled strategy went live. */
  markerDate?: string;
  /** Start collapsed (the /schedule strategy detail collapses every card).
   * Defaults to expanded so /backtest is unchanged. */
  defaultCollapsed?: boolean;
  /** Let the active strategy define the alignment window's right edge
   * instead of clamping to the earliest-ending series. /schedule sets this
   * so a live-extended strategy curve isn't truncated back to the frozen
   * universe baseline. */
  activeDefinesWindowEnd?: boolean;
};

/** "Equity Curve" card cluster: comparison pill row, summary stats,
 * yearly breakdown + custom-range picker, and the chart itself. Owns its
 * own benchmark/saved comparison state and all the chart-derived memos
 * — the parent only feeds it the active strategy plus the saved-run
 * list. */
function EquityCurveCardInner({ result, loadedRunId, savedRuns, activeStrategyLabel, markerDate, defaultCollapsed = false, activeDefinesWindowEnd = false }: Props) {
  // Benchmark options for the "add series" dropdown — fetched via the
  // shared cached hook so a sibling component (e.g. MomentumBacktester's
  // sector-ETF lookup) reuses the same fetch instead of re-requesting.
  const { data: _benchmarks } = useBenchmarks();
  const benchmarkOptions = (_benchmarks ?? []) as BenchmarkOption[];
  const [comparisons, setComparisons] = useState<ComparisonItem[]>([]);
  // Global fee config for the net-to-client `(net)` parenthetical. The
  // parenthetical now means "after Leonteq + Bustelberg fees" (see the
  // Fee waterfall panel), computed via the layered model in feeModel.ts.
  const feeConfig = useFeeConfig();
  const [addSeriesOpen, setAddSeriesOpen] = useState(false);
  const addSeriesRef = useRef<HTMLDivElement>(null);
  const [logScale, setLogScale] = useState(true);
  const [hoveredDrawdown, setHoveredDrawdown] = useState<number | null>(null);
  const [customFromMonth, setCustomFromMonth] = useState('');
  // Identifier of an "Add series" operation currently in flight — used to
  // show a small spinner pill while we fetch the backend payload. Cleared
  // once the comparison item lands in `comparisons`.
  const [addingSeriesId, setAddingSeriesId] = useState<string | null>(null);

  // Net stats for the active strategy + every saved comparison. We
  // reuse the parent's `exchangeByCompany` as the lookup for all of
  // them — its fallback fetch hits `/api/companies` which covers the
  // entire directory, so a saved run on a different universe still
  // resolves its holdings' exchanges. Benchmarks have no holdings to
  // trade so they always sit out (net == gross by definition).
  // Full fee waterfall for the active strategy — its `.net` feeds the
  // per-stat `(net)` parentheticals + custom-range walker, and its
  // `.breakdown` (per-year, reconciled) drives the yearly table so the
  // two views agree exactly.
  const activeWaterfall = useMemo<FeeWaterfall | null>(
    () =>
      computeFeeWaterfall(result.monthly_records, result.daily_records, feeConfig, {
        grossTotalReturnPct: result.summary?.total_return_pct,
      }),
    [feeConfig, result.monthly_records, result.daily_records, result.summary?.total_return_pct],
  );
  const activeNetStats = activeWaterfall?.net ?? null;

  /** Per-comparison fee waterfall (saved runs only — benchmarks have no
   * holdings to fee). Keyed by series id. */
  const comparisonWaterfalls = useMemo<Map<string, FeeWaterfall | null>>(() => {
    const m = new Map<string, FeeWaterfall | null>();
    for (const c of comparisons) {
      if (c.kind !== 'saved') continue;
      m.set(c.id, computeFeeWaterfall(c.monthly, c.daily, feeConfig));
    }
    return m;
  }, [comparisons, feeConfig]);
  const comparisonNetStats = useMemo<Map<string, NetStats | null>>(() => {
    const m = new Map<string, NetStats | null>();
    for (const [id, wf] of comparisonWaterfalls) m.set(id, wf?.net ?? null);
    return m;
  }, [comparisonWaterfalls]);

  /** Per-year fee breakdowns keyed by series id, for the yearly table. */
  const feeBreakdownsBySeries = useMemo<Record<string, FeeBreakdownRow[] | null>>(() => {
    const m: Record<string, FeeBreakdownRow[] | null> = { active: activeWaterfall?.breakdown ?? null };
    for (const [id, wf] of comparisonWaterfalls) m[id] = wf?.breakdown ?? null;
    return m;
  }, [activeWaterfall, comparisonWaterfalls]);

  useClickOutside(addSeriesRef, () => setAddSeriesOpen(false), addSeriesOpen);

  // Helpers to add/remove comparison series
  const addSavedSeries = async (runId: number) => {
    if (comparisons.some((c) => c.kind === 'saved' && c.runId === runId)) return;
    setAddingSeriesId(`saved:${runId}`);
    try {
      const resp = await apiFetch(`${API_URL}/api/momentum/backtests/${runId}`);
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
      const resp = await apiFetch(
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
  useClickOutside(variantPickerRef, () => setVariantPickerOpen(null), !!variantPickerOpen);

  // Resolve every series into a (date → growth factor) map. Date keys are
  // normalized to YYYY-MM-DD so that string comparison correctly orders dates
  // across mixed cadences — a saved variant with monthly_records ("2002-01")
  // sits at the *end of January* on the timeline, lining up with a daily
  // strategy's "2002-01-31" rather than colliding lexicographically.
  // Resolve every series (active + comparisons) into uniform shape.
  // Label precedence: parent's full label, then loaded saved-run name,
  // then generic placeholder.
  const resolvedSeries = useMemo(() => {
    const loadedName = loadedRunId != null
      ? savedRuns.find((r) => r.run_id === loadedRunId)?.name
      : undefined;
    const activeLabel = activeStrategyLabel || loadedName || 'Strategy';
    return resolveSeries(result, comparisons, SERIES_COLORS, activeLabel);
  }, [result, comparisons, loadedRunId, savedRuns, activeStrategyLabel]);

  // Alignment window + per-series rebased points + stats — moved to
  // `./equityCurve/seriesMath.ts:alignSeries`.
  const alignedSeries = useMemo(
    () => alignSeries(resolvedSeries, activeDefinesWindowEnd),
    [resolvedSeries, activeDefinesWindowEnd],
  );

  // The original ~125-line block below is dead code from the old inline
  // implementation — kept temporarily for ref while extraction stabilizes.
  // Will be deleted in the next cleanup pass.

  // Yearly performance breakdown — moved to seriesMath:computeYearlyBreakdown.
  const yearlyBreakdown = useMemo(
    () => computeYearlyBreakdown(alignedSeries, feeBreakdownsBySeries),
    [alignedSeries, feeBreakdownsBySeries],
  );

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
  // Cumulative return from customFromMonth — moved to seriesMath:computeCustomRangeReturn.
  const customRangeReturn = useMemo(
    () => computeCustomRangeReturn(alignedSeries, customFromMonth, activeNetStats, comparisons, comparisonNetStats),
    [alignedSeries, customFromMonth, activeNetStats, comparisons, comparisonNetStats],
  );

  // Chart data — wide-format per month, one key per series id, plus a 0%
  // origin row so every line starts from the same reference point.
  // Wide-format chart data — moved to seriesMath:buildChartData.
  const chartData = useMemo(() => buildChartData(alignedSeries), [alignedSeries]);

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
  // Y-axis domain for chart — moved to seriesMath:computeChartYDomain.
  const chartYDomain = useMemo<[number, number]>(
    () => computeChartYDomain(displayChartData, alignedSeries),
    [displayChartData, alignedSeries],
  );

  // Per-calendar-year mini-charts (strategy vs universe + alpha). Built
  // off the same aligned series the main chart uses so years agree with
  // the Yearly Performance table. Empty when no universe baseline is
  // available — the grid component then renders nothing.
  const yearlySubplots = useMemo(() => computeYearlySubplots(alignedSeries), [alignedSeries]);
  const strategyColor = alignedSeries.series[0]?.color;

  return (
    <CollapsibleCard
      title="Equity curve"
      defaultCollapsed={defaultCollapsed}
      bodyClassName="px-4 pb-4 space-y-4"
    >
      {/* Comparison panel — active strategy + any added backtests/benchmarks */}
      <div className="bg-card rounded-xl border border-neutral-800/40 px-4 py-3">
        <div className="flex items-center gap-3 flex-wrap">
          <span className="text-fg-muted text-sm mr-1">Comparison</span>
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
                className="relative inline-flex items-center gap-2 bg-page border border-neutral-800 rounded-full pl-2 pr-1 py-1 text-xs"
                ref={pickerOpen ? variantPickerRef : undefined}
              >
                <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                {hasVariantPicker ? (
                  <button
                    type="button"
                    onClick={() => setVariantPickerOpen(pickerOpen ? null : s.id)}
                    className="text-fg hover:text-fg-strong inline-flex items-center gap-1"
                    title="Switch variant"
                  >
                    <span>{s.label}</span>
                    <svg className={`w-3 h-3 text-fg-subtle transition-transform ${pickerOpen ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                      <path fillRule="evenodd" d="M5.23 7.21a.75.75 0 011.06.02L10 11.06l3.71-3.83a.75.75 0 111.08 1.04l-4.25 4.39a.75.75 0 01-1.08 0L5.21 8.27a.75.75 0 01.02-1.06z" clipRule="evenodd" />
                    </svg>
                  </button>
                ) : (
                  <span className="text-fg">{s.label}</span>
                )}
                {s.removable && (
                  <button
                    type="button"
                    onClick={() => removeSeries(s.id)}
                    className="ml-0.5 w-4 h-4 rounded-full text-fg-subtle hover:text-neg-400 hover:bg-neg-500/10 transition-colors flex items-center justify-center"
                    title="Remove from comparison"
                  >
                    <svg className="w-2.5 h-2.5" viewBox="0 0 20 20" fill="currentColor">
                      <path fillRule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clipRule="evenodd" />
                    </svg>
                  </button>
                )}
                {hasVariantPicker && pickerOpen && cmp.kind === 'saved' && cmp.allVariants && (
                  <div className="absolute left-0 top-full mt-1 w-64 bg-card border border-neutral-700 rounded-lg shadow-xl z-50 max-h-80 overflow-auto">
                    <div className="px-3 py-1.5 text-[10px] uppercase tracking-wider text-fg-subtle border-b border-neutral-800/60">
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
                          className={`w-full text-left px-3 py-2 text-xs hover:bg-overlay/[0.03] disabled:opacity-100 disabled:cursor-default flex items-center justify-between gap-2 ${isCurrent ? 'bg-accent-500/10 text-accent-300' : 'text-fg'}`}
                        >
                          <span className="truncate">{v.label}</span>
                          {isCurrent && <span className="text-[10px] text-accent-400">current</span>}
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
              className="inline-flex items-center gap-1.5 text-xs text-accent-300 hover:text-accent-200 border border-accent-500/40 hover:border-accent-400/60 bg-accent-500/10 hover:bg-accent-500/20 rounded-full px-3 py-1 transition-colors disabled:opacity-70 disabled:cursor-wait"
            >
              {addingSeriesId != null ? (
                <>
                  <svg className="animate-spin w-3 h-3 text-accent-300" viewBox="0 0 24 24" fill="none">
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
              <div className="absolute left-0 mt-1 w-72 bg-card border border-neutral-700 rounded-lg shadow-xl z-50 max-h-80 overflow-auto">
                {benchmarkOptions.length > 0 && (
                  <div>
                    <div className="px-3 py-1.5 text-[10px] uppercase tracking-wider text-fg-subtle border-b border-neutral-800/60">Benchmarks</div>
                    {benchmarkOptions.map((b) => {
                      const already = comparisons.some((c) => c.kind === 'benchmark' && c.benchmarkId === b.benchmark_id);
                      return (
                        <button
                          key={b.benchmark_id}
                          type="button"
                          disabled={already}
                          onClick={() => { addBenchmarkSeries(b.benchmark_id); setAddSeriesOpen(false); }}
                          className="w-full text-left px-3 py-2 text-xs hover:bg-overlay/[0.03] disabled:opacity-40 disabled:cursor-not-allowed text-fg flex items-center gap-2"
                        >
                          <span className="font-mono text-warn-300">{b.ticker}</span>
                          <span className="text-fg-subtle truncate">{b.name}</span>
                          {already && <span className="ml-auto text-[10px] text-fg-faint">added</span>}
                        </button>
                      );
                    })}
                  </div>
                )}
                {savedRuns.length > 0 && (
                  <div>
                    <div className="px-3 py-1.5 text-[10px] uppercase tracking-wider text-fg-subtle border-t border-b border-neutral-800/60">Saved Backtests</div>
                    {savedRuns.map((r) => {
                      const already = comparisons.some((c) => c.kind === 'saved' && c.runId === r.run_id);
                      const isLoaded = r.run_id === loadedRunId;
                      return (
                        <button
                          key={r.run_id}
                          type="button"
                          disabled={already || isLoaded}
                          onClick={() => { addSavedSeries(r.run_id); setAddSeriesOpen(false); }}
                          className="w-full text-left px-3 py-2 text-xs hover:bg-overlay/[0.03] disabled:opacity-40 disabled:cursor-not-allowed text-fg flex items-center gap-2"
                          title={isLoaded ? 'Currently loaded as the active strategy' : undefined}
                        >
                          <span className="truncate">{r.name}</span>
                          {isLoaded && <span className="ml-auto text-[10px] text-fg-faint">active</span>}
                          {!isLoaded && already && <span className="ml-auto text-[10px] text-fg-faint">added</span>}
                        </button>
                      );
                    })}
                  </div>
                )}
                {benchmarkOptions.length === 0 && savedRuns.length === 0 && (
                  <div className="px-3 py-4 text-xs text-fg-subtle">No benchmarks or saved backtests available.</div>
                )}
              </div>
            )}
          </div>
          {alignedSeries.windowStart && alignedSeries.windowEnd && alignedSeries.series.length > 1 && (
            <span className="text-[11px] text-fg-subtle font-mono ml-auto">
              aligned {alignedSeries.windowStart} → {alignedSeries.windowEnd}
            </span>
          )}
        </div>
      </div>

      {/* Summary Stats */}
      <SummaryStats
        result={result}
        alignedSeries={alignedSeries}
        activeNetStats={activeNetStats}
        comparisonNetStats={comparisonNetStats}
        summaryExportColumns={summaryExportColumns}
      />

      {/* Yearly Performance + Custom Range */}
      <YearlyBreakdown
        yearlyBreakdown={yearlyBreakdown}
        alignedSeries={alignedSeries}
        customRangeReturn={customRangeReturn}
        customFromMonth={customFromMonth}
        setCustomFromMonth={setCustomFromMonth}
        yearlyExportRows={yearlyExportRows}
        yearlyExportColumns={yearlyExportColumns}
        markerDate={markerDate}
      />

      {/* Equity Curve */}
      <EquityChart
        displayChartData={displayChartData}
        alignedSeries={alignedSeries}
        chartYDomain={chartYDomain}
        logScale={logScale}
        setLogScale={setLogScale}
        hoveredDrawdown={hoveredDrawdown}
        setHoveredDrawdown={setHoveredDrawdown}
        markerDate={markerDate}
      />

      {/* Per-year small multiples — cumulative + alpha. Both reuse the
          same `computeYearlySubplots` result; the grid component branches
          on `mode`. Renders nothing on saved runs without a universe
          baseline. */}
      <YearlySubplotsGrid subplots={yearlySubplots} mode="cumulative" strategyColor={strategyColor} markerDate={markerDate} />
      <YearlySubplotsGrid subplots={yearlySubplots} mode="alpha" strategyColor={strategyColor} markerDate={markerDate} />
    </CollapsibleCard>
  );
}

/** React.memo barrier — the chart card is one of the more expensive
 * renders on /backtest (Recharts SVG + Summary table + Yearly
 * breakdown). Default shallow-compare matches our use site: the
 * caller passes a memoized `exchangeByCompany`, the result object
 * is stable per backtest, and `activeStrategyLabel` is a string
 * (value-equal). Skipping renders when parent state changes for
 * unrelated reasons (slider drags, axis toggles, run-time ticker)
 * keeps scrolling smooth even with many variants loaded. */
const EquityCurveCard = memo(EquityCurveCardInner);
export default EquityCurveCard;
