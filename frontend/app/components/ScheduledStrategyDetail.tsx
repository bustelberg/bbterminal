'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import SnapshotHoldings from './SnapshotHoldings';
import { StrategyConfigDetail, type IngestRun } from './Schedule';
import { colorForSector } from '../../lib/sectorColors';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type RunHistoryEntry = {
  snapshot_id: number;
  created_at: string;
  as_of_date: string;
  latest_price_date: string | null;
  holdings_count: number;
  /** 'rebalance' = strategy was due and computed fresh picks at this
   * tick. 'price_update' = strategy wasn't due, prior rebalance's
   * holdings re-priced through this tick's close. */
  kind: 'rebalance' | 'price_update' | null;
  /** True for the 3 synthetic snapshots created on add. These are
   * "what would have happened" previews, not real pipeline runs.
   * `ingest_run` is null on these rows. */
  is_backfill: boolean;
  /** % gain of this rebalance's picks over its holding period
   * (rebalance → next rebalance / now). Null when the engine couldn't
   * produce one (no holdings, missing exit prices). */
  period_return_pct: number | null;
  /** Holdings grouped by sector — used by the UI's per-row sector
   * grid. Vertically aligned across rows (via a shared column set)
   * so persistent sectors line up. */
  sector_counts: Record<string, number>;
  ingest_run: IngestRun | null;
};

type BackfillState = {
  status: 'running' | 'done' | 'error' | null;
  progress_pct: number | null;
  message: string | null;
  error: string | null;
  started_at: string | null;
  finished_at: string | null;
};

export type StrategyRunHistory = {
  id: number;
  name: string;
  frequency: string | null;
  config: Record<string, unknown>;
  enabled: boolean;
  created_at: string;
  last_run_at: string | null;
  next_due_at: string | null;
  backfill: BackfillState | null;
  runs: RunHistoryEntry[];
};

function fmtTimestamp(iso: string | null): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString(undefined, {
      year: 'numeric', month: 'short', day: '2-digit',
      hour: '2-digit', minute: '2-digit',
    });
  } catch {
    return iso;
  }
}

function StatusBadge({ status }: { status: 'running' | 'ok' | 'error' }) {
  const cls = status === 'ok'
    ? 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30'
    : status === 'error'
      ? 'bg-rose-500/10 text-rose-300 border-rose-500/30'
      : 'bg-amber-500/15 text-amber-300 border-amber-500/30';
  return (
    <span className={`inline-flex items-center text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border ${cls}`}>
      {status}
    </span>
  );
}

/** Per-strategy expanded view: shows the strategy's params + the list of
 * pipeline runs that produced a snapshot for it. Each row is clickable
 * — click to expand the snapshot's holdings inline. */
export default function ScheduledStrategyDetail({
  strategyId,
  initialData,
  onLoaded,
  onMutated: _onMutated,
}: {
  strategyId: number;
  /** Parent-supplied cache hit. When non-null we render immediately and
   * fetch silently in the background (stale-while-revalidate). */
  initialData?: StrategyRunHistory | null;
  /** Called after every successful fetch so the parent can update its
   * cache. */
  onLoaded?: (data: StrategyRunHistory) => void;
  onMutated?: () => void;
}) {
  const [data, setData] = useState<StrategyRunHistory | null>(initialData ?? null);
  // Only show the spinner on a cold load. Cache hits render the previous
  // payload immediately while the silent revalidate runs.
  const [loading, setLoading] = useState(initialData == null);
  const [error, setError] = useState<string | null>(null);
  // Latest onLoaded held in a ref so its identity changing on every
  // parent render doesn't invalidate `load` and re-trigger the fetch
  // effect — that would loop, since onLoaded mutates parent state.
  const onLoadedRef = useRef(onLoaded);
  useEffect(() => { onLoadedRef.current = onLoaded; }, [onLoaded]);
  const [expandedSnapshotId, setExpandedSnapshotId] = useState<number | null>(null);
  const [showConfig, setShowConfig] = useState(false);

  /** Per-row sector → column assignment for the run-history grid.
   *
   * Returns an array (one entry per row) of `Map<sector, columnIdx>`.
   * Two-stage algorithm:
   *
   *   STAGE 1 (global): rank sectors by frequency desc → assign each
   *   a `preferredColumn` = its rank mod N. The top-N most frequent
   *   sectors each get a unique preferred column; less-frequent
   *   sectors cycle back to col 0.
   *
   *   STAGE 2 (per row): for THIS row's sectors (sorted by global
   *   frequency desc — most stable first), place each in its preferred
   *   column if free, otherwise the next-lowest available column.
   *   This guarantees every sector in the row renders (no hidden chips)
   *   while keeping the same sector in the same column whenever a
   *   collision doesn't force a swap.
   *
   * Effect on the user's case (5 quarterly rows, top_n_sectors=4):
   *   - 4 stable sectors → 4 columns, identical placement on every row.
   *   - A 5th sector swapping in for one of the 4 → takes the freed
   *     column. The 4 chips fill cleanly with no gap. */
  const SECTOR_COL_COUNT = 4;
  const rowSectorAssignments = useMemo(() => {
    const rows = data?.runs ?? [];
    const rowSectorLists: string[][] = rows.map((r) =>
      Object.entries(r.sector_counts ?? {})
        .filter(([, c]) => (c ?? 0) > 0)
        .map(([sec]) => sec),
    );
    // Global frequency = number of rows each sector appears in.
    const freq = new Map<string, number>();
    for (const sectors of rowSectorLists) {
      for (const s of sectors) freq.set(s, (freq.get(s) ?? 0) + 1);
    }
    const rankedSectors = [...freq.keys()].sort(
      (a, b) => (freq.get(b)! - freq.get(a)!) || a.localeCompare(b),
    );
    const preferred = new Map<string, number>();
    rankedSectors.forEach((sec, idx) => preferred.set(sec, idx % SECTOR_COL_COUNT));

    return rowSectorLists.map((sectors) => {
      // Sort this row's sectors by global frequency so highest-rank
      // gets first pick of its preferred column.
      const sorted = [...sectors].sort(
        (a, b) => (freq.get(b)! - freq.get(a)!) || a.localeCompare(b),
      );
      const assignment = new Map<string, number>();
      const taken = new Set<number>();
      for (const sec of sorted) {
        const pref = preferred.get(sec) ?? 0;
        if (!taken.has(pref)) {
          assignment.set(sec, pref);
          taken.add(pref);
          continue;
        }
        // Fall through: pick the next available column (left-to-right).
        for (let c = 0; c < SECTOR_COL_COUNT; c++) {
          if (!taken.has(c)) {
            assignment.set(sec, c);
            taken.add(c);
            break;
          }
        }
      }
      return assignment;
    });
  }, [data?.runs]);

  const load = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/api/scheduled-strategies/${strategyId}/runs?limit=100`);
      if (!r.ok) {
        setError(`Failed to load run history (${r.status})`);
        // Clear data so the polling effect below sees a non-running
        // status and stops firing. Otherwise a deleted strategy (404)
        // keeps its stale `backfill.status='running'` indefinitely and
        // the interval hammers the endpoint forever.
        setData(null);
        return;
      }
      const body = (await r.json()) as StrategyRunHistory;
      setData(body);
      setError(null);
      onLoadedRef.current?.(body);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [strategyId]);

  useEffect(() => {
    void load();
  }, [load]);

  // Poll while the backfill is running so the progress bar advances
  // in near real-time. 2s comfortably catches the backend's 0.3s
  // write throttle. Stops as soon as status moves to 'done' / 'error'
  // — or when `data` is cleared (e.g. on 404 from a deleted strategy).
  useEffect(() => {
    if (data?.backfill?.status !== 'running') return;
    const id = setInterval(() => { void load(); }, 2000);
    return () => clearInterval(id);
  }, [data?.backfill?.status, load]);

  if (loading) {
    return <div className="px-5 py-4 bg-[#0b0d13] text-xs text-gray-500 border-t border-gray-800/30">Loading run history…</div>;
  }
  if (error) {
    return <div className="px-5 py-4 bg-[#0b0d13] text-xs text-rose-300 border-t border-gray-800/30">{error}</div>;
  }
  if (!data) return null;

  return (
    <div className="px-5 py-4 bg-[#0b0d13] border-t border-gray-800/30 space-y-4">
      {/* Strategy params (collapsible — verbose enough to want to hide
          unless the user is checking what's actually scheduled). */}
      <div>
        <button
          type="button"
          onClick={() => setShowConfig((v) => !v)}
          className="text-xs text-gray-400 hover:text-white transition-colors mb-2"
        >
          {showConfig ? '▾' : '▸'} Strategy params
        </button>
        {showConfig && data.config && <StrategyConfigDetail cfg={data.config} />}
      </div>

      {/* Backfill progress bar — visible while the backfill is in
          flight (status='running'), shows the engine's live message
          + percentage. Hidden once the backfill lands or errors. */}
      {data.backfill && data.backfill.status === 'running' && (
        <div className="bg-indigo-500/5 border border-indigo-500/20 rounded-lg px-4 py-3 space-y-2">
          <div className="flex items-center justify-between gap-3 text-xs">
            <span className="text-indigo-300 font-medium">Running backfill…</span>
            <span className="text-indigo-300/80 font-mono">{data.backfill.progress_pct ?? 0}%</span>
          </div>
          <div className="h-1 bg-indigo-500/15 rounded-full overflow-hidden">
            <div
              className="h-full bg-indigo-500 transition-all duration-300"
              style={{ width: `${data.backfill.progress_pct ?? 0}%` }}
            />
          </div>
          {data.backfill.message && (
            <div className="text-[11px] text-indigo-200/70 font-mono truncate" title={data.backfill.message}>
              {data.backfill.message}
            </div>
          )}
        </div>
      )}

      {data.backfill && data.backfill.status === 'error' && (
        <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-xs text-rose-300">
          <div className="font-medium mb-0.5">Backfill failed</div>
          <div className="font-mono whitespace-pre-wrap">{data.backfill.error ?? 'Unknown error'}</div>
        </div>
      )}

      {/* Run history */}
      <div>
        <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">
          Run history ({data.runs.length})
        </div>
        {data.runs.length === 0 ? (
          <div className="text-xs text-gray-500">
            No runs yet. The backfill kicks off on add — refresh in a moment to see it land. Real pipeline ticks fire every Tuesday 02:00 UTC.
          </div>
        ) : (
          <div className="bg-[#151821] border border-gray-800/40 rounded-lg overflow-hidden">
            <div className="divide-y divide-gray-800/30">
              {data.runs.map((entry, rowIdx) => {
                const isExpanded = expandedSnapshotId === entry.snapshot_id;
                const run = entry.ingest_run;
                const isRebalance = entry.kind === 'rebalance';
                const kindCls = isRebalance
                  ? 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30'
                  : 'bg-gray-500/15 text-gray-300 border-gray-500/30';
                // Period return chip — green when ≥0, rose when <0,
                // neutral grey when null/missing. The value is the
                // weighted gain of the picks during the period this
                // snapshot covers.
                const ret = entry.period_return_pct;
                const retCls = ret == null
                  ? 'text-gray-500'
                  : ret >= 0
                    ? 'text-emerald-400'
                    : 'text-rose-400';
                const retLabel = ret == null
                  ? '—'
                  : `${ret >= 0 ? '+' : ''}${ret.toFixed(2)}%`;
                return (
                  <div key={entry.snapshot_id}>
                    <button
                      type="button"
                      onClick={() => setExpandedSnapshotId(isExpanded ? null : entry.snapshot_id)}
                      className="w-full px-4 py-2 flex items-center gap-3 text-left hover:bg-white/[0.02] transition-colors flex-wrap"
                    >
                      <span className="text-gray-500 font-mono text-xs w-4 shrink-0">{isExpanded ? '▾' : '▸'}</span>
                      <span
                        className="text-gray-200 font-mono text-xs w-28 shrink-0"
                        title="Rebalance date (first Monday of the period per the strategy's frequency)"
                      >
                        {entry.as_of_date}
                      </span>
                      {entry.kind && (
                        <span className={`text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border ${kindCls}`}>
                          {isRebalance ? 'rebalanced' : 'price update'}
                        </span>
                      )}
                      {entry.is_backfill && (
                        <span
                          className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-amber-500/15 text-amber-300 border-amber-500/30"
                          title="Synthetic preview — what the strategy would have produced. NOT a real pipeline run."
                        >
                          backfill
                        </span>
                      )}
                      <span
                        className={`text-xs font-mono font-medium ${retCls}`}
                        title="Period return: weighted % gain of the picks from this rebalance through the period (or live close for the open period)."
                      >
                        {retLabel}
                      </span>
                      {run && <StatusBadge status={run.status} />}
                      <span className="text-gray-400 text-xs font-mono">
                        {entry.holdings_count} holdings
                      </span>
                      <span
                        className="text-gray-600 text-xs ml-auto font-mono"
                        title="Latest close-price date used to compute returns on this snapshot"
                      >
                        data through {entry.latest_price_date ?? entry.as_of_date}
                        {run && (
                          <span className="text-gray-700">
                            {' · '}run #{run.run_id}
                          </span>
                        )}
                      </span>
                    </button>
                    {/* Per-row sector strip. Always exactly
                        SECTOR_COL_COUNT (4) columns, each evenly sized
                        via `repeat(4, minmax(0, 1fr))`. Column choice
                        per sector is computed per-row (see
                        `rowSectorAssignments`) with a global preferred
                        column from frequency-rank — same sector stays
                        in the same column wherever it can, but a
                        swap-in sector that displaces another claims
                        the freed column instead of being hidden. */}
                    {entry.sector_counts && Object.keys(entry.sector_counts).length > 0 && (
                      <div
                        className="px-4 pb-2 grid gap-1 text-[11px]"
                        style={{ gridTemplateColumns: `repeat(${SECTOR_COL_COUNT}, minmax(0, 1fr))` }}
                      >
                        {Array.from({ length: SECTOR_COL_COUNT }).map((_, colIdx) => {
                          const rowAssignment = rowSectorAssignments[rowIdx];
                          // Find this row's sector assigned to this
                          // column (at most one, by construction).
                          let chosen: { sec: string; count: number } | null = null;
                          if (rowAssignment) {
                            for (const [sec, col] of rowAssignment) {
                              if (col !== colIdx) continue;
                              const count = entry.sector_counts[sec] ?? 0;
                              if (count > 0) chosen = { sec, count };
                              break;
                            }
                          }
                          if (!chosen) return <div key={colIdx} />;
                          const color = colorForSector(chosen.sec, colIdx);
                          // Two-digit hex alpha appended to the 6-digit
                          // color → semi-transparent fill/border that
                          // works on the dark theme.
                          return (
                            <div
                              key={colIdx}
                              // `min-w-0` on the flex container + its
                              // truncating child is load-bearing —
                              // without it the chip's intrinsic content
                              // width overrides the grid cell's width
                              // and overflows to the right.
                              className="border rounded px-1.5 py-0.5 flex items-baseline gap-1 min-w-0 overflow-hidden"
                              style={{
                                backgroundColor: `${color}1a`,
                                borderColor: `${color}66`,
                                color,
                              }}
                              title={`${chosen.sec}: ${chosen.count} holding${chosen.count === 1 ? '' : 's'}`}
                            >
                              <span className="font-mono shrink-0">{chosen.count}</span>
                              <span className="text-[10px] opacity-90 truncate min-w-0">{chosen.sec}</span>
                            </div>
                          );
                        })}
                      </div>
                    )}
                    {isExpanded && (
                      <div className="px-4 py-3 border-t border-gray-800/30 bg-[#0f1117]">
                        {entry.is_backfill && (
                          <div className="mb-3 text-[11px] text-amber-300/90 bg-amber-500/10 border border-amber-500/20 rounded-lg px-3 py-2">
                            Backfill preview — these are synthetic picks the strategy would have produced if it had been on schedule. The next real pipeline run (Tuesday 02:00 UTC) is the first one that actually fires.
                          </div>
                        )}
                        <SnapshotHoldings snapshotId={entry.snapshot_id} />
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
