'use client';

import { memo, useEffect, useMemo, useState } from 'react';
import {
  momentumStore,
  setActiveVariant,
  VARIANT_DEFS,
  parseVariantKey,
  variantLabel,
  type RebalanceFrequency,
  type StrategyType,
  type VariantKey,
  type VariantOutcome,
  type VariantParams,
  type VariantsRunState,
} from '../../../lib/stores/momentum';
import type { Column } from '../../../lib/tableExport';
import TableDownloadButton from '../TableDownloadButton';
import CellInfoTip from './CellInfoTip';
import CollapsibleCard from './CollapsibleCard';
import { fmtPct } from './utils';
import { parenPct } from './feeStats';
import { computeFeeWaterfall, type FeeConfig } from './feeModel';
import { useFeeConfig } from '../../../lib/hooks/apiData';

// One row per VARIANT_DEFS entry. Click to make that variant the active one
// (the detail views below — equity curve, holdings table, sector timeline —
// switch to it). Pending/running rows show a spinner; errored rows surface
// the message inline so the user doesn't have to open the network tab.
//
// Variants in this table render in `sortedRows` order (defaults to the
// canonical VARIANT_DEFS order; flips to whatever column the user
// clicked in the header). Active row is highlighted and clickable,
// everything else is muted but stays visible so the running sweep is
// observable.

type Props = {
  /** Called when the user clicks the "+ Schedule" button on an OK-status
   * row. Parent has the base BacktestRequest state and builds the schedule
   * `config` by merging base + variant overrides. */
  onAddToSchedule?: (variantKey: VariantKey, variantLabel: string) => void;
};

// All sortable columns in display order. `variant` is text; everything
// from `annualized` onward is numeric and "higher is better" (including
// `max_dd`, where -5 > -20 — closer to zero is the winning value).
type ColumnKey =
  | 'variant' | 'start' | 'end'
  | 'annualized' | 'universe_annualized'
  | 'sharpe' | 'sortino'
  | 'win_rate' | 'median' | 'total' | 'max_dd';

// Columns that get gold/silver/bronze rank borders on the top three
// distinct values. Per spec: every metric column, NOT the Variant /
// Start / End labels (those don't have a "better" direction in this
// table). Universe annualized is included so two variants on the same
// universe tie for the medal — at-a-glance "this universe was the
// rising-tide-lifts-all-boats year" call-out.
const RANKABLE_COLUMNS: ReadonlySet<ColumnKey> = new Set([
  'annualized', 'universe_annualized', 'sharpe', 'sortino',
  'win_rate', 'median', 'total', 'max_dd',
]);

type SortDir = 'asc' | 'desc';

/** Per-row accessor — returns the value used for both sorting AND
 * ranking. Numbers for metric columns, strings for label/date columns,
 * null for rows whose variant didn't finish successfully (those sink
 * to the bottom of a sorted view and never receive a rank medal). */
function rowValueFor(
  row: { key: VariantKey; label: string },
  variants: Partial<Record<VariantKey, VariantOutcome>>,
  col: ColumnKey,
): number | string | null {
  if (col === 'variant') return row.label;
  const o = variants[row.key];
  if (!o || o.status !== 'ok') return null;
  const r = o.result;
  if (col === 'start') {
    const m = r.monthly_records ?? [];
    return m.length > 0 ? m[0].date.slice(0, 7) : null;
  }
  if (col === 'end') {
    const m = r.monthly_records ?? [];
    return m.length > 0 ? m[m.length - 1].date.slice(0, 7) : null;
  }
  const s = r.summary;
  if (s == null) return null;
  switch (col) {
    case 'annualized':          return s.annualized_return_pct ?? null;
    case 'universe_annualized': return s.universe_annualized_return_pct ?? null;
    case 'sharpe':              return s.sharpe_ratio ?? null;
    case 'sortino':             return s.sortino_ratio ?? null;
    case 'win_rate':            return s.win_rate_pct ?? null;
    case 'median':              return s.median_period_return_pct ?? null;
    case 'total':               return s.total_return_pct ?? null;
    case 'max_dd':              return s.max_drawdown_pct ?? null;
  }
  return null;
}

/** Tailwind class string for the gold/silver/bronze rank decoration.
 * Combines a colored left border with a faint background tint so the
 * medal reads at a glance against the dark-theme table background. */
function rankBorderClass(rank: number | undefined): string {
  if (rank === 1) return 'border-l-2 border-amber-400/90 bg-amber-500/[0.06]';
  if (rank === 2) return 'border-l-2 border-slate-300/80 bg-slate-300/[0.05]';
  if (rank === 3) return 'border-l-2 border-orange-700/90 bg-orange-700/[0.06]';
  return '';
}

function VariantSummaryTableInner({ onAddToSchedule }: Props) {
  const variants = momentumStore.use((s) => s.variants);
  const active = momentumStore.use((s) => s.activeVariantKey);
  const run = momentumStore.use((s) => s.variantsRun);

  // Global fee config — the (net) column means "net to client, after
  // Leonteq + Bustelberg fees" (same layered model as the Fee waterfall).
  const feeConfig = useFeeConfig();

  // Hide entirely when no sweep has ever run. The wrapper component decides
  // when to render us based on whether `variants` has any entries.
  const anyVariant = Object.values(variants).some((v) => v != null);

  // ── Sort state. null = the default `orderedVariantRows` order
  //    (VARIANT_DEFS frequency → strategy → dials). Clicking a header
  //    sorts by that column; clicking the same column toggles
  //    direction. Default direction = descending for metrics (best
  //    first), ascending for text/dates (chronological / alphabetical).
  const [sortColumn, setSortColumn] = useState<ColumnKey | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>('desc');
  const onSortClick = (col: ColumnKey) => {
    if (sortColumn === col) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortColumn(col);
      setSortDir(RANKABLE_COLUMNS.has(col) ? 'desc' : 'asc');
    }
  };

  // Stable display order across legacy 2-segment keys
  // (`monthly__long_only`) and cross-product keys
  // (`monthly__long_only__s4__p6__m30`): sort by VARIANT_DEFS' frequency
  // index, then strategy, then the numeric dials so a sweep renders
  // contiguously per (freq, strategy) block.
  const orderedVariantRows = useMemo<{ key: VariantKey; params: VariantParams; label: string }[]>(() => {
    const freqOrder = new Map<RebalanceFrequency, number>();
    VARIANT_DEFS.forEach((v, i) => { if (!freqOrder.has(v.frequency)) freqOrder.set(v.frequency, i); });
    const stratOrder: Record<StrategyType, number> = { long_only: 0, long_short: 1 };
    const entries: { key: VariantKey; params: VariantParams; label: string }[] = [];
    for (const k of Object.keys(variants)) {
      if (variants[k as VariantKey] == null) continue;
      const p = parseVariantKey(k as VariantKey);
      if (!p) continue;
      // Prefer the canonical label for legacy keys (e.g. "Monthly · Long-only"),
      // else fall back to the auto-generated label that includes the dials.
      const canonical = VARIANT_DEFS.find((v) => v.key === k)?.label;
      entries.push({ key: k as VariantKey, params: p, label: canonical ?? variantLabel(p) });
    }
    entries.sort((a, b) => {
      let d = (freqOrder.get(a.params.frequency) ?? 999) - (freqOrder.get(b.params.frequency) ?? 999);
      if (d !== 0) return d;
      d = stratOrder[a.params.strategy] - stratOrder[b.params.strategy];
      if (d !== 0) return d;
      d = (a.params.top_n_sectors ?? -1) - (b.params.top_n_sectors ?? -1);
      if (d !== 0) return d;
      d = (a.params.top_n_per_sector ?? -1) - (b.params.top_n_per_sector ?? -1);
      if (d !== 0) return d;
      return ((a.params.min_price_score ?? -Infinity) as number) - ((b.params.min_price_score ?? -Infinity) as number);
    });
    return entries;
  }, [variants]);

  // Apply the user's chosen sort. Variants without a successful
  // outcome (pending/error/cancelled) always sink to the bottom no
  // matter the direction — their stats are null and ranking them
  // ahead of completed runs would be misleading.
  const sortedRows = useMemo(() => {
    if (sortColumn == null) return orderedVariantRows;
    const out = orderedVariantRows.slice();
    out.sort((a, b) => {
      const va = rowValueFor(a, variants, sortColumn);
      const vb = rowValueFor(b, variants, sortColumn);
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (typeof va === 'number' && typeof vb === 'number') {
        return sortDir === 'asc' ? va - vb : vb - va;
      }
      const cmp = String(va).localeCompare(String(vb));
      return sortDir === 'asc' ? cmp : -cmp;
    });
    return out;
  }, [orderedVariantRows, variants, sortColumn, sortDir]);

  // Top-3 distinct values per rankable column → gold/silver/bronze
  // medal. "Distinct" so ties share a medal (two variants both at
  // Sharpe=1.42 both get gold). Higher-is-better for every column
  // (max_dd's -5 vs -20 already sorts numerically the way we want).
  const ranksByColumn = useMemo<Map<ColumnKey, Map<number, number>>>(() => {
    const result = new Map<ColumnKey, Map<number, number>>();
    for (const col of RANKABLE_COLUMNS) {
      const distinct = new Set<number>();
      for (const r of orderedVariantRows) {
        const v = rowValueFor(r, variants, col);
        if (typeof v === 'number') distinct.add(v);
      }
      const top3 = Array.from(distinct).sort((a, b) => b - a).slice(0, 3);
      const m = new Map<number, number>();
      top3.forEach((val, i) => m.set(val, i + 1));
      result.set(col, m);
    }
    return result;
  }, [orderedVariantRows, variants]);

  // Flatten each ready variant's summary into one row for the export.
  type VariantExportRow = {
    variant: string;
    start: string;
    end: string;
    annualized_pct: number | null;
    universe_annualized_pct: number | null;
    sharpe: number | null;
    sortino: number | null;
    win_rate_pct: number | null;
    median_period_return_pct: number | null;
    total_return_pct: number | null;
    max_drawdown_pct: number | null;
  };
  const exportRows = useMemo<VariantExportRow[]>(() => {
    const out: VariantExportRow[] = [];
    for (const v of sortedRows) {
      const o = variants[v.key];
      if (!o || o.status !== 'ok') continue;
      const r = o.result;
      const months = r.monthly_records ?? [];
      out.push({
        variant: v.label,
        start: months[0]?.date ?? '',
        end: months[months.length - 1]?.date ?? '',
        annualized_pct: r.summary?.annualized_return_pct ?? null,
        universe_annualized_pct: r.summary?.universe_annualized_return_pct ?? null,
        sharpe: r.summary?.sharpe_ratio ?? null,
        sortino: r.summary?.sortino_ratio ?? null,
        win_rate_pct: r.summary?.win_rate_pct ?? null,
        median_period_return_pct: r.summary?.median_period_return_pct ?? null,
        total_return_pct: r.summary?.total_return_pct ?? null,
        max_drawdown_pct: r.summary?.max_drawdown_pct ?? null,
      });
    }
    return out;
  }, [sortedRows, variants]);
  const exportColumns = useMemo<Column<VariantExportRow>[]>(() => [
    { key: 'variant', header: 'Variant', accessor: (r) => r.variant },
    { key: 'start', header: 'Start', accessor: (r) => r.start },
    { key: 'end', header: 'End', accessor: (r) => r.end },
    { key: 'annualized_pct', header: 'Annualized return (%)', accessor: (r) => r.annualized_pct },
    { key: 'universe_annualized_pct', header: 'Universe annualized (%)', accessor: (r) => r.universe_annualized_pct },
    { key: 'sharpe', header: 'Sharpe', accessor: (r) => r.sharpe },
    { key: 'sortino', header: 'Sortino', accessor: (r) => r.sortino },
    { key: 'win_rate_pct', header: 'Win rate (%)', accessor: (r) => r.win_rate_pct },
    { key: 'median_period_return_pct', header: 'Median month return (%)', accessor: (r) => r.median_period_return_pct },
    { key: 'total_return_pct', header: 'Total return (%)', accessor: (r) => r.total_return_pct },
    { key: 'max_drawdown_pct', header: 'Max drawdown (%)', accessor: (r) => r.max_drawdown_pct },
  ], []);

  if (!anyVariant) return null;

  return (
    <CollapsibleCard
      title="Variants"
      rightSlot={
        <div className="flex items-center gap-3">
          <span>Click a row to switch the equity curve, holdings, and sector timeline below.</span>
          {run && <SweepStatus run={run} />}
          <TableDownloadButton
            rows={exportRows}
            columns={exportColumns}
            filename="variant_summary"
            title={`Download ${exportRows.length} variant rows as CSV / XLSX`}
          />
        </div>
      }
    >
      <table className="w-full text-sm border-t border-gray-800/40">
        <thead>
          <tr className="text-gray-500 text-xs border-b border-gray-800/40">
            <SortableTh col="variant" align="left" label="Variant"
              tooltip="The strategy + per-variant axis overrides this row represents. Click the row to make this variant the active one in the equity curve, holdings, and sector timeline below."
              sortColumn={sortColumn} sortDir={sortDir} onSort={onSortClick} />
            <SortableTh col="start" align="right" label="Start"
              tooltip="First closed period in the backtest — the month-end of the first completed rebalance, as YYYY-MM."
              sortColumn={sortColumn} sortDir={sortDir} onSort={onSortClick} />
            <SortableTh col="end" align="right" label="End"
              tooltip="Last closed period in the backtest, as YYYY-MM. The currently-open period (if any) is excluded so headline stats aren't biased by a partial window."
              sortColumn={sortColumn} sortDir={sortDir} onSort={onSortClick} />
            <SortableTh col="annualized" align="right" label="Annualized return"
              tooltip="Geometric annualized return derived from the period-chain cumulative factor over the closed window: (cumulative_factor)^(1/years) − 1. The (parenthetical) is the same number net of per-trade fees configured on /fees."
              sortColumn={sortColumn} sortDir={sortDir} onSort={onSortClick} />
            <SortableTh col="universe_annualized" align="right" label="Universe ann."
              tooltip="Annualized return of an EQUAL-WEIGHT portfolio holding every eligible company in this variant's universe over the same closed window. The 'no-skill baseline' — a strategy whose annualized return exceeds this added value beyond just being in the universe; a strategy that lags it underperformed the universe's intrinsic drift. Two variants on the same universe share this number, so it surfaces universe-level biases when comparing across universes."
              sortColumn={sortColumn} sortDir={sortDir} onSort={onSortClick} />
            <SortableTh col="sharpe" align="right" label="Sharpe"
              tooltip="Annualized risk-adjusted return: (mean daily return / std of all daily returns) × √252. Risk-free rate is treated as 0. Computed from the closed-period daily curve so it's comparable across rebalance cadences. The (parenthetical) recomputes Sharpe on the net-of-fees daily curve."
              sortColumn={sortColumn} sortDir={sortDir} onSort={onSortClick} />
            <SortableTh col="sortino" align="right" label="Sortino"
              tooltip="Same as Sharpe but volatility only counts negative daily returns: (mean / std-of-downside) × √252. Higher than Sharpe = upside vol is dominating the variance, which Sharpe over-penalizes. Lower than Sharpe = drawdowns are concentrated."
              sortColumn={sortColumn} sortDir={sortDir} onSort={onSortClick} />
            <SortableTh col="win_rate" align="right" label="Win rate"
              tooltip="% of calendar months with strictly positive return. Computed from the closed daily equity curve resampled to month-end and chained month-over-month — independent of rebalance cadence so a daily and a 12-month variant land on the same scale."
              sortColumn={sortColumn} sortDir={sortDir} onSort={onSortClick} />
            <SortableTh col="median" align="right" label="Median month"
              tooltip="Median calendar-month return — same monthly series as Win rate. Independent of rebalance cadence. Far below the annualized mean → headline return is carried by a few outlier months rather than steady ones."
              sortColumn={sortColumn} sortDir={sortDir} onSort={onSortClick} />
            <SortableTh col="total" align="right" label="Total return"
              tooltip="Cumulative return over the entire closed window: (cumulative_factor − 1) × 100. Geometric compound of every closed period's return. The (parenthetical) is the same number net of per-trade fees."
              sortColumn={sortColumn} sortDir={sortDir} onSort={onSortClick} />
            <SortableTh col="max_dd" align="right" label="Max drawdown"
              tooltip="Worst peak-to-trough decline on the closed-period daily equity curve, as a negative %. The open period is excluded so an in-progress drawdown doesn't inflate the historical max. The (parenthetical) is the net-of-fees max DD on the same window."
              sortColumn={sortColumn} sortDir={sortDir} onSort={onSortClick} />
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((v) => (
            <VariantRow
              key={v.key}
              variantKey={v.key}
              label={v.label}
              outcome={variants[v.key]}
              isActive={active === v.key}
              feeConfig={feeConfig}
              ranksByColumn={ranksByColumn}
              onAddToSchedule={onAddToSchedule}
            />
          ))}
        </tbody>
      </table>
    </CollapsibleCard>
  );
}

/** React.memo barrier — see MonthlyHoldingsTable for the rationale.
 * This component subscribes to the `momentumStore` for the variants
 * map + active key directly; its only prop is the `onAddToSchedule`
 * callback (the fee config comes from the shared cached hook). */
const VariantSummaryTable = memo(VariantSummaryTableInner);
export default VariantSummaryTable;

function SweepStatus({ run }: { run: VariantsRunState }) {
  // Tick once per second while a variant is in flight so the elapsed
  // counter updates between variant transitions. Reading Date.now()
  // directly during render would be impure (and ESLint's react-hooks/purity
  // rule rightly flags it).
  const [now, setNow] = useState<number>(() => run.startedAt);
  const isRunning = run.current != null;
  // The leading setNow snaps the elapsed counter to "live time" the
  // instant a sweep starts (otherwise it lags by up to one full second
  // before the first interval tick). React 19's set-state-in-effect
  // lint dislikes this synchronous setter, but the alternative — a
  // ref-based "first tick" toggle — adds more noise than it saves.
  useEffect(() => {
    if (!isRunning) return;
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setNow(Date.now());
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [isRunning]);
  const elapsed = Math.round((now - run.startedAt) / 1000);
  const done = run.completed;
  const total = run.total;
  return (
    <div className="text-xs text-gray-400 flex items-center gap-2">
      {isRunning && (
        <svg className="animate-spin w-3 h-3 text-indigo-400" viewBox="0 0 24 24" fill="none">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
      )}
      <span className="font-mono">
        {done}/{total}
        {isRunning && <span className="text-gray-600"> · {elapsed}s</span>}
      </span>
    </div>
  );
}

/** Clickable column header. Sort indicator (↑/↓) appears when the
 * column is the active sort target. The `CellInfoTip` is rendered as
 * a sibling rather than a child of the click target so hovering the
 * "i" icon doesn't double as a sort click. */
function SortableTh({
  col,
  label,
  tooltip,
  align,
  sortColumn,
  sortDir,
  onSort,
}: {
  col: ColumnKey;
  label: string;
  tooltip: string;
  align: 'left' | 'right';
  sortColumn: ColumnKey | null;
  sortDir: SortDir;
  onSort: (col: ColumnKey) => void;
}) {
  const active = sortColumn === col;
  const arrow = active ? (sortDir === 'asc' ? '↑' : '↓') : '';
  const alignClass = align === 'left' ? 'text-left' : 'text-right';
  return (
    <th className={`${alignClass} font-medium px-3 py-2 whitespace-nowrap`}>
      <span
        onClick={() => onSort(col)}
        className={`cursor-pointer select-none hover:text-gray-200 ${active ? 'text-indigo-300' : ''}`}
        title={`Sort by ${label}`}
      >
        {label}
        {arrow && <span className="ml-1 text-indigo-400">{arrow}</span>}
      </span>
      <CellInfoTip>{tooltip}</CellInfoTip>
    </th>
  );
}

function VariantRow({
  variantKey,
  label,
  outcome,
  isActive,
  feeConfig,
  ranksByColumn,
  onAddToSchedule,
}: {
  variantKey: VariantKey;
  label: string;
  outcome: VariantOutcome | undefined;
  isActive: boolean;
  feeConfig: FeeConfig;
  ranksByColumn: Map<ColumnKey, Map<number, number>>;
  onAddToSchedule?: (variantKey: VariantKey, label: string) => void;
}) {
  const status = outcome?.status ?? 'pending';
  const clickable = status === 'ok';
  const handleClick = () => {
    if (clickable) setActiveVariant(variantKey);
  };

  // Active row: chevron indicator + slightly brighter label, no left border
  // and no background tint (those were the "ugly selector").
  // Clickable but not active: subtle hover bg.
  // Pending/running/errored: no hover.
  const rowClass = clickable && !isActive
    ? 'cursor-pointer hover:bg-white/[0.02]'
    : '';

  return (
    <tr className={`border-b border-gray-800/20 group ${rowClass}`} onClick={handleClick}>
      <td className="px-3 py-2 text-gray-200">
        <div className="flex items-center gap-2">
          <span className={`inline-block w-3 text-indigo-400 ${isActive ? '' : 'opacity-0'}`}>
            ›
          </span>
          <StatusBadge status={status} />
          <span className={isActive ? 'text-white font-medium' : ''}>{label}</span>
          {status === 'cancelled' && (
            <span className="text-[10px] text-gray-500 italic">cancelled</span>
          )}
          {clickable && onAddToSchedule && (
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();  // don't trigger setActiveVariant
                onAddToSchedule(variantKey, label);
              }}
              title="Save this variant as a scheduled strategy. The pipeline will keep its current-picks snapshot up to date on every tick."
              className="ml-2 text-[10px] font-medium uppercase tracking-wider px-1.5 py-0.5 rounded border border-indigo-500/30 text-indigo-300 bg-indigo-500/10 hover:bg-indigo-500/20 opacity-0 group-hover:opacity-100 transition-opacity"
            >
              + Schedule
            </button>
          )}
        </div>
        {status === 'error' && outcome?.status === 'error' && (
          <div className="text-[10px] text-rose-400 mt-0.5 font-mono">{outcome.message}</div>
        )}
      </td>
      <SummaryCells
        outcome={outcome}
        feeConfig={feeConfig}
        ranksByColumn={ranksByColumn}
      />
    </tr>
  );
}

function SummaryCells({
  outcome,
  feeConfig,
  ranksByColumn,
}: {
  outcome: VariantOutcome | undefined;
  feeConfig: FeeConfig;
  ranksByColumn: Map<ColumnKey, Map<number, number>>;
}) {
  // 6 placeholder cells (Start, End, Annualized, Sharpe, Total, DD) so the
  // row width stays stable across statuses.
  // Note: hooks must be called unconditionally — `net` is computed before
  // any early return.
  const net = useMemo(() => {
    if (outcome?.status !== 'ok') return null;
    return computeFeeWaterfall(
      outcome.result.monthly_records,
      outcome.result.daily_records,
      feeConfig,
      { grossTotalReturnPct: outcome.result.summary?.total_return_pct },
    )?.net ?? null;
  }, [outcome, feeConfig]);

  if (outcome?.status !== 'ok') {
    // 10 placeholders: Start, End, Annualized, Universe annualized,
    // Sharpe, Sortino, Win rate, Median, Total, Max DD. Keeps row width
    // consistent across statuses.
    return (
      <>
        {[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map((i) => (
          <td key={i} className="px-3 py-2 text-right text-gray-700 font-mono">—</td>
        ))}
      </>
    );
  }
  const s = outcome.result.summary;
  const records = outcome.result.monthly_records;
  // First/last record dates → YYYY-MM. Records are emitted in chronological
  // order by the backtest loop, so first = period start, last = period end.
  const firstDate = records.length > 0 ? records[0].date.slice(0, 7) : '—';
  const lastDate = records.length > 0 ? records[records.length - 1].date.slice(0, 7) : '—';
  const colorize = (v: number | null | undefined) =>
    v == null ? 'text-gray-500'
      : v >= 0 ? 'text-emerald-400'
      : 'text-rose-400';
  // Per-cell rank decoration. Looks up the column's medal map (built
  // once at the parent level) and returns the gold/silver/bronze
  // border + tint when this row's value lands in the top three.
  const medal = (col: ColumnKey, val: number | null | undefined): string => {
    if (val == null) return '';
    return rankBorderClass(ranksByColumn.get(col)?.get(val));
  };
  return (
    <>
      <td className="px-3 py-2 text-right font-mono text-gray-400">{firstDate}</td>
      <td className="px-3 py-2 text-right font-mono text-gray-400">{lastDate}</td>
      <td className={`px-3 py-2 text-right font-mono ${colorize(s.annualized_return_pct)} ${medal('annualized', s.annualized_return_pct)}`}>
        {fmtPct(s.annualized_return_pct)}
        <span className="text-gray-500">{parenPct(net?.annualized_return_pct)}</span>
      </td>
      <td className={`px-3 py-2 text-right font-mono ${colorize(s.universe_annualized_return_pct)} ${medal('universe_annualized', s.universe_annualized_return_pct)}`}>
        {s.universe_annualized_return_pct != null ? fmtPct(s.universe_annualized_return_pct) : '—'}
      </td>
      <td className={`px-3 py-2 text-right font-mono text-gray-200 ${medal('sharpe', s.sharpe_ratio)}`}>
        {s.sharpe_ratio != null ? s.sharpe_ratio.toFixed(2) : '—'}
        <span className="text-gray-500">{net?.sharpe_ratio != null ? ` (${net.sharpe_ratio.toFixed(2)})` : ''}</span>
      </td>
      <td className={`px-3 py-2 text-right font-mono text-gray-200 ${medal('sortino', s.sortino_ratio)}`}>
        {s.sortino_ratio != null ? s.sortino_ratio.toFixed(2) : '—'}
      </td>
      <td className={`px-3 py-2 text-right font-mono text-gray-300 ${medal('win_rate', s.win_rate_pct)}`}>
        {s.win_rate_pct != null ? `${s.win_rate_pct.toFixed(0)}%` : '—'}
      </td>
      <td className={`px-3 py-2 text-right font-mono ${colorize(s.median_period_return_pct)} ${medal('median', s.median_period_return_pct)}`}>
        {s.median_period_return_pct != null ? fmtPct(s.median_period_return_pct) : '—'}
      </td>
      <td className={`px-3 py-2 text-right font-mono ${colorize(s.total_return_pct)} ${medal('total', s.total_return_pct)}`}>
        {fmtPct(s.total_return_pct)}
        <span className="text-gray-500">{parenPct(net?.total_return_pct)}</span>
      </td>
      <td className={`px-3 py-2 text-right font-mono text-rose-400 ${medal('max_dd', s.max_drawdown_pct)}`}>
        {fmtPct(s.max_drawdown_pct)}
        <span className="text-gray-500">{parenPct(net?.max_drawdown_pct)}</span>
      </td>
    </>
  );
}

function StatusBadge({ status }: { status: VariantOutcome['status'] }) {
  if (status === 'pending') {
    return <span className="inline-block w-2 h-2 rounded-full bg-gray-700" title="Pending" />;
  }
  if (status === 'running') {
    return (
      <svg className="animate-spin w-3 h-3 text-indigo-400" viewBox="0 0 24 24" fill="none" aria-label="Running">
        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
      </svg>
    );
  }
  if (status === 'ok') {
    return <span className="inline-block w-2 h-2 rounded-full bg-emerald-400" title="Done" />;
  }
  if (status === 'cancelled') {
    return <span className="inline-block w-2 h-2 rounded-full bg-gray-500" title="Cancelled" />;
  }
  return <span className="inline-block w-2 h-2 rounded-full bg-rose-500" title="Error" />;
}
