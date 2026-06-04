'use client';

import Stat from './Stat';
import MonthlySparkline from './MonthlySparkline';
import SectorBreakdown from './SectorBreakdown';
import { buildFilterPills } from './filterConfig';
import type { DerivedCriterionSpec, UniverseRow } from './types';

export type UniverseCardProps = {
  u: UniverseRow;
  expanded: boolean;
  onToggle: () => void;
  renamingId: number | null;
  renameValue: string;
  setRenameValue: (v: string) => void;
  startRename: (u: UniverseRow) => void;
  cancelRename: () => void;
  saveRename: (u: UniverseRow) => void;
  confirmDelete: string | null;
  setConfirmDelete: (label: string | null) => void;
  deleteOne: (label: string) => void;
  busyLabel: string | null;
  onTighten?: () => void;
  tightening?: boolean;
  derivedSpecs?: DerivedCriterionSpec[];
};

/** One saved-universe card: header (rename inline / delete confirm /
 * tighten·rename·delete actions), filter pills for derived universes, a
 * 7-stat grid, and — when expanded — the monthly sparkline + sector
 * breakdown. */
export default function UniverseCard({
  u, expanded, onToggle,
  renamingId, renameValue, setRenameValue, startRename, cancelRename, saveRename,
  confirmDelete, setConfirmDelete, deleteOne, busyLabel,
  onTighten, tightening, derivedSpecs,
}: UniverseCardProps) {
  const isRenaming = renamingId === u.universe_id;
  const isConfirming = confirmDelete === u.label;
  const isBusy = busyLabel === u.label;

  const createdLabel = u.created_at ? new Date(u.created_at).toISOString().slice(0, 10) : '—';
  const monthRange = u.start_month && u.end_month
    ? (u.start_month === u.end_month ? u.start_month : `${u.start_month} → ${u.end_month}`)
    : '—';

  const filterPills = u.is_derived && u.filter_config
    ? buildFilterPills(u.filter_config, derivedSpecs ?? [])
    : [];

  return (
    <div className={`rounded-xl border overflow-hidden ${u.is_derived ? 'bg-card-alt border-accent-900/30' : 'bg-card border-neutral-800/40'}`}>
      <div className="flex items-start justify-between gap-3 px-5 py-4">
        <div className="flex items-start gap-3 min-w-0 flex-1">
          <button
            type="button"
            onClick={onToggle}
            className="mt-0.5 text-fg-subtle hover:text-fg-soft transition-colors shrink-0"
            aria-label={expanded ? 'Collapse' : 'Expand'}
          >
            <span className="font-mono text-sm">{expanded ? '▾' : '▸'}</span>
          </button>
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-3 flex-wrap">
              {isRenaming ? (
                <input
                  autoFocus
                  value={renameValue}
                  onChange={e => setRenameValue(e.target.value)}
                  onKeyDown={e => {
                    if (e.key === 'Enter') saveRename(u);
                    if (e.key === 'Escape') cancelRename();
                  }}
                  className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-base text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
                />
              ) : (
                <h3 className="text-fg-strong text-base font-semibold">{u.label}</h3>
              )}
              {u.is_derived ? (
                <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded bg-accent-500/15 text-accent-300 border border-accent-500/30">
                  Derived{u.parent_label ? ` · from ${u.parent_label}` : ''}
                </span>
              ) : (
                <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded bg-neutral-700/40 text-fg-soft border border-neutral-700/60">
                  Base
                </span>
              )}
              <span className="text-fg-subtle text-xs font-mono">id:{u.universe_id}</span>
              <span className="text-fg-subtle text-xs">created {createdLabel}</span>
            </div>
            {u.description && (
              <p className="text-fg-muted text-xs mt-1">{u.description}</p>
            )}
            {filterPills.length > 0 && (
              <div className="flex flex-wrap gap-1.5 mt-2">
                {filterPills.map((p, i) => (
                  <span key={i} className="text-[11px] px-1.5 py-0.5 rounded bg-accent-500/10 border border-accent-500/20 text-accent-300 font-mono">
                    {p}
                  </span>
                ))}
              </div>
            )}
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-3 mt-3">
              <Stat label="Months" value={u.month_count} />
              <Stat label="Range" value={monthRange} mono />
              <Stat label="Unique companies" value={u.unique_companies} />
              <Stat label="Unique tickers" value={u.unique_tickers} />
              <Stat label="Total rows" value={u.total_rows} />
              <Stat label="Avg / month" value={u.avg_per_month} />
              <Stat label="First / last mo" value={`${u.first_month_count} / ${u.last_month_count}`} />
            </div>
          </div>
        </div>

        <div className="flex items-center gap-1 shrink-0">
          {isRenaming ? (
            <>
              <button
                onClick={() => saveRename(u)}
                disabled={isBusy}
                className="px-2.5 py-1 rounded-lg text-xs font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-50"
              >
                Save
              </button>
              <button
                onClick={cancelRename}
                className="px-2.5 py-1 rounded-lg text-xs font-medium text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors"
              >
                Cancel
              </button>
            </>
          ) : isConfirming ? (
            <>
              <span className="text-xs text-neg-400">Delete?</span>
              <button
                onClick={() => deleteOne(u.label)}
                disabled={isBusy}
                className="px-2.5 py-1 rounded-lg text-xs font-medium bg-neg-600 hover:bg-neg-500 text-fg-strong transition-colors disabled:opacity-50"
              >
                {isBusy ? '...' : 'Yes'}
              </button>
              <button
                onClick={() => setConfirmDelete(null)}
                className="px-2.5 py-1 rounded-lg text-xs font-medium text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors"
              >
                Cancel
              </button>
            </>
          ) : (
            <>
              {onTighten && (
                <button
                  onClick={onTighten}
                  className={`px-2.5 py-1 rounded-lg text-xs font-medium transition-colors ${tightening ? 'bg-accent-600 text-fg-strong' : 'text-accent-300 hover:text-accent-200 hover:bg-accent-500/10'}`}
                >
                  {tightening ? 'Close' : 'Tighten'}
                </button>
              )}
              <button
                onClick={() => startRename(u)}
                className="px-2.5 py-1 rounded-lg text-xs font-medium text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors"
              >
                Rename
              </button>
              <button
                onClick={() => setConfirmDelete(u.label)}
                className="px-2.5 py-1 rounded-lg text-xs font-medium text-fg-muted hover:text-neg-400 hover:bg-neg-500/10 transition-colors"
              >
                Delete
              </button>
            </>
          )}
        </div>
      </div>

      {expanded && (
        <div className="border-t border-neutral-800/40 grid grid-cols-1 lg:grid-cols-2 gap-0">
          <MonthlySparkline monthly={u.monthly_counts} />
          <SectorBreakdown sectors={u.sectors} totalRows={u.total_rows} />
        </div>
      )}
    </div>
  );
}
