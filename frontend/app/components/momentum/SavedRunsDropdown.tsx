'use client';

import { useEffect, useRef, useState } from 'react';

import LoadingDots from '../LoadingDots';
import Spinner from '../Spinner';
import { dialog } from '../../../lib/dialog';
import { useClickOutside } from '../../../lib/hooks/useClickOutside';
import type { SavedRun } from './types';

// Compact labels for each momentum signal key so dropdown subtext stays
// scannable. Unknown keys fall through unchanged.
const SIGNAL_ABBREV: Record<string, string> = {
  mom_12_1: '12-1',
  mom_6m: '6m',
  volatility_adjusted_return_6m: 'vAdj',
  drawdown_from_recent_high_pct: 'DD',
  above_200ma: '200ma',
  vol_20d_vs_60d: 'vSrg',
  vol_trend_3m: 'vT3m',
};

/** Key strategy params formatted as compact subtext for one dropdown row.
 *  Two saved backtests that produce the same default name (universe ·
 *  strategy · range) typically still differ in top-N, category weights,
 *  grouping, or floor — surfacing those inline lets the user tell otherwise-
 *  identical entries apart without opening each one. */
function describeBacktestParams(cfg: Record<string, unknown> | undefined): string {
  if (!cfg) return '';
  const parts: string[] = [];
  const tnS = cfg.top_n_sectors as number | undefined;
  const tnP = cfg.top_n_per_sector as number | undefined;
  if (tnS != null && tnP != null) parts.push(`${tnS}×${tnP}`);
  const sd = cfg.start_date as string | undefined;
  const ed = cfg.end_date as string | undefined;
  if (sd && ed) {
    const s = sd.slice(0, 7);
    const e = ed.slice(0, 7);
    parts.push(s === e ? s : `${s} → ${e}`);
  }
  const mode = cfg.selection_mode as string | undefined;
  if (mode && mode !== 'momentum') {
    const trials = cfg.n_trials as number | undefined;
    const seed = cfg.random_seed as number | undefined;
    if (mode === 'random') {
      parts.push(`random×${trials ?? 1}${seed != null ? `@${seed}` : ''}`);
    } else if (mode === 'all') {
      parts.push('all-universe');
    } else if (mode === 'sector_etf') {
      parts.push('sector-ETF');
    } else {
      parts.push(mode);
    }
  }
  const grouping = cfg.grouping as string | undefined;
  if (grouping && grouping !== 'sector') parts.push(`group:${grouping}`);
  const floor = cfg.min_price_score;
  if (floor != null && floor !== '') parts.push(`price≥${floor}`);
  const catW = cfg.category_weights as Record<string, number> | undefined;
  if (catW) {
    const p = catW.price;
    const v = catW.volume;
    if (typeof p === 'number' || typeof v === 'number') {
      const pp = Math.round((p ?? 0) * 100);
      const vv = Math.round((v ?? 0) * 100);
      if (pp + vv > 0) parts.push(`P${pp}/V${vv}`);
    }
  }
  return parts.join(' · ');
}

/** Active signal weights as compact tokens (e.g. `12-1:3 6m:2 DD`).
 *  Skips signals weighted at 0; omits the trailing `:N` when weight is 1
 *  to reduce visual noise. Returns '' when nothing's active. */
function describeBacktestSignals(cfg: Record<string, unknown> | undefined): string {
  if (!cfg) return '';
  const sigW = cfg.signal_weights as Record<string, number> | undefined;
  if (!sigW) return '';
  const tokens: string[] = [];
  for (const [k, w] of Object.entries(sigW)) {
    if (typeof w !== 'number' || w === 0) continue;
    const label = SIGNAL_ABBREV[k] ?? k;
    tokens.push(w === 1 ? label : `${label}:${w}`);
  }
  return tokens.join(' ');
}

export type SavedRunsDropdownProps = {
  savedRuns: SavedRun[];
  /** First-fetch state — renders a spinner + "Loading saved …" label in the
   * trigger so the dropdown doesn't appear to materialize out of thin air. */
  loading: boolean;
  /** When the user has loaded a saved run, that run_id; otherwise null. Used
   * to highlight the active row and show the loaded name in the trigger. */
  loadedRunId: number | null;
  /** Per-row spinner discriminators. Each is the run_id currently being
   * acted on (or null when idle); the matching row renders a spinner. */
  loadingRunId: number | null;
  deletingRunId: number | null;
  renamingRunId: number | null;
  /** Disables the bulk-delete button while in flight. */
  bulkDeleting: boolean;
  onLoad: (runId: number) => void;
  onDelete: (runId: number) => void;
  onRename: (runId: number, currentName: string) => void;
  /** Must return a promise — the dropdown awaits completion before clearing
   * its multi-select state. */
  onBulkDelete: (ids: number[]) => Promise<void>;
};

/** Header dropdown listing every saved backtest. Owns its open/close +
 * multi-select state internally; the parent only needs to wire up the
 * fetched runs list and the four action callbacks. Extracted from
 * MomentumBacktester.tsx (~2,300 lines) so saved-runs UI changes don't
 * require scanning the whole strategy backtester component. */
export default function SavedRunsDropdown({
  savedRuns,
  loading,
  loadedRunId,
  loadingRunId,
  deletingRunId,
  renamingRunId,
  bulkDeleting,
  onLoad,
  onDelete,
  onRename,
  onBulkDelete,
}: SavedRunsDropdownProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  // Multi-select for bulk delete. Cleared whenever the dropdown closes so
  // a stale "5 selected" state doesn't greet the user on the next peek.
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());

  useClickOutside(ref, () => setOpen(false), open);
  useEffect(() => {
    // Clear the multi-select when the dropdown closes. React 19's
    // set-state-in-effect rule flags this; the alternatives (clearing
    // in the close handler in two places, or remounting via key) are
    // more error-prone for a one-line reset.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    if (!open) setSelectedIds(new Set());
  }, [open]);

  const empty = !loading && savedRuns.length === 0;
  const triggerLabel = loading
    ? <LoadingDots label="Loading saved backtests" />
    : empty
      ? 'No saved backtests yet'
      : (loadedRunId
          ? savedRuns.find((r) => r.run_id === loadedRunId)?.name ?? 'Load saved backtest...'
          : 'Load saved backtest...');

  const toggleSelected = (runId: number) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(runId)) next.delete(runId); else next.add(runId);
      return next;
    });
  };

  const handleBulkDelete = async () => {
    const ids = Array.from(selectedIds);
    if (ids.length === 0) return;
    await onBulkDelete(ids);
    // Clear regardless of failure — failed entries stay in `savedRuns`
    // (parent doesn't remove them from the list on error) so the user
    // can re-select if they want to retry.
    setSelectedIds(new Set());
  };

  return (
    <div className="relative" ref={ref}>
      <button
        type="button"
        onClick={() => { if (!loading && !empty) setOpen((o) => !o); }}
        disabled={loading || empty}
        className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white flex items-center gap-2 hover:border-indigo-500 focus:outline-none focus:border-indigo-500 transition-colors min-w-[220px] disabled:opacity-70 disabled:cursor-default disabled:hover:border-gray-700"
      >
        {(loading || loadingRunId != null) && <Spinner />}
        <span className="truncate">{triggerLabel}</span>
        <svg className={`w-3.5 h-3.5 text-gray-500 ml-auto transition-transform ${open ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
          <path fillRule="evenodd" d="M5.23 7.21a.75.75 0 011.06.02L10 11.06l3.71-3.83a.75.75 0 111.08 1.04l-4.25 4.39a.75.75 0 01-1.08 0L5.21 8.27a.75.75 0 01.02-1.06z" clipRule="evenodd" />
        </svg>
      </button>
      {open && (
        <div className="absolute right-0 mt-1 w-max min-w-[280px] max-w-[90vw] bg-[#151821] border border-gray-700 rounded-lg shadow-xl z-50 max-h-96 overflow-auto">
          {selectedIds.size > 0 && (
            <div className="sticky top-0 z-10 bg-[#1a1d27] border-b border-gray-700 px-3 py-2 flex items-center justify-between gap-2">
              <span className="text-xs text-gray-300">{selectedIds.size} selected</span>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => setSelectedIds(new Set())}
                  className="text-[11px] text-gray-500 hover:text-gray-300 px-2 py-1 rounded transition-colors"
                >
                  clear
                </button>
                <button
                  type="button"
                  onClick={() => void handleBulkDelete()}
                  disabled={bulkDeleting}
                  className="text-[11px] font-medium px-2 py-1 rounded bg-rose-500/15 text-rose-300 border border-rose-500/30 hover:bg-rose-500/25 transition-colors disabled:opacity-50 disabled:cursor-not-allowed inline-flex items-center gap-1.5"
                >
                  {bulkDeleting && <Spinner size={12} />}
                  Delete {selectedIds.size}
                </button>
              </div>
            </div>
          )}
          {savedRuns.map((r) => {
            const isActive = r.run_id === loadedRunId;
            const isLoadingThis = loadingRunId === r.run_id;
            const isDeletingThis = deletingRunId === r.run_id;
            const isRenamingThis = renamingRunId === r.run_id;
            const isSelected = selectedIds.has(r.run_id);
            const paramsLine = describeBacktestParams(r.config);
            const signalsLine = describeBacktestSignals(r.config);
            return (
              <div
                key={r.run_id}
                className={`group flex items-start gap-2 px-3 py-2 border-b border-gray-800/40 last:border-b-0 hover:bg-white/[0.03] transition-colors ${isActive ? 'bg-indigo-500/10' : ''} ${isSelected ? 'bg-rose-500/[0.06]' : ''}`}
              >
                <input
                  type="checkbox"
                  checked={isSelected}
                  onChange={(e) => { e.stopPropagation(); toggleSelected(r.run_id); }}
                  onClick={(e) => e.stopPropagation()}
                  className="accent-indigo-500 w-3.5 h-3.5 shrink-0 cursor-pointer mt-1"
                  title="Select for bulk delete"
                />
                <button
                  type="button"
                  onClick={() => { onLoad(r.run_id); setOpen(false); }}
                  disabled={isLoadingThis || isDeletingThis}
                  className="flex-1 text-left disabled:opacity-60 min-w-0"
                >
                  <div className={`text-sm flex items-center gap-1.5 whitespace-nowrap ${isActive ? 'text-indigo-300' : 'text-gray-200'}`}>
                    {isLoadingThis && <Spinner />}
                    <span>{r.name}</span>
                  </div>
                  {paramsLine && (
                    <div className="text-[11px] text-gray-400 mt-0.5 whitespace-nowrap">{paramsLine}</div>
                  )}
                  {signalsLine && (
                    <div className="text-[10px] text-gray-500 font-mono mt-0.5 whitespace-nowrap">{signalsLine}</div>
                  )}
                  <div className="text-[10px] text-gray-600 font-mono mt-0.5">{new Date(r.created_at).toLocaleDateString()}</div>
                </button>
                <button
                  type="button"
                  onClick={(e) => { e.stopPropagation(); onRename(r.run_id, r.name); }}
                  disabled={isRenamingThis || isDeletingThis}
                  className="p-1.5 rounded text-gray-500 hover:text-indigo-400 hover:bg-white/5 opacity-0 group-hover:opacity-100 transition-opacity disabled:opacity-100 disabled:cursor-wait"
                  title="Rename"
                >
                  {isRenamingThis ? (
                    <Spinner size={14} />
                  ) : (
                    <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                      <path d="M13.586 3.586a2 2 0 112.828 2.828l-.793.793-2.828-2.828.793-.793zM11.379 5.793L3 14.172V17h2.828l8.38-8.379-2.83-2.828z" />
                    </svg>
                  )}
                </button>
                <button
                  type="button"
                  onClick={async (e) => {
                    e.stopPropagation();
                    if (await dialog.confirm(`Delete "${r.name}"?`, { destructive: true, confirmLabel: 'Delete' })) {
                      onDelete(r.run_id);
                    }
                  }}
                  disabled={isDeletingThis || isRenamingThis}
                  className="p-1.5 rounded text-gray-500 hover:text-rose-400 hover:bg-rose-500/10 opacity-0 group-hover:opacity-100 transition-opacity disabled:opacity-100 disabled:cursor-wait"
                  title="Delete"
                >
                  {isDeletingThis ? (
                    <Spinner size={14} />
                  ) : (
                    <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                      <path fillRule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clipRule="evenodd" />
                    </svg>
                  )}
                </button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
