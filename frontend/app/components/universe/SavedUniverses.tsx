'use client';

import LoadingDots from '../LoadingDots';
import UniverseCard from './UniverseCard';
import TightenPanel from './TightenPanel';
import type { UseUniversesResult } from './useUniverses';

/** The "Saved Universes" section: the count header + delete-all confirm,
 * the loading/error/empty states, and the grouped list (base universes,
 * their inline tighten panels + derived children, then any orphans). All
 * state + handlers come from the `useUniverses` controller. */
export default function SavedUniverses({ ctl }: { ctl: UseUniversesResult }) {
  const {
    universes, loading, error, grouped, derivedSpecs, defaultConfig, loadUniverses,
    expandedId, setExpandedId, tighteningId, setTighteningId,
    renamingId, renameValue, setRenameValue, startRename, cancelRename, saveRename,
    confirmDelete, setConfirmDelete, deleteOne, confirmDeleteAll, setConfirmDeleteAll, deleteAll,
    busyLabel,
  } = ctl;

  // Shared card props for rename/delete wiring (everything except the
  // per-card identity + expand/tighten bits set at each call site).
  const cardCommon = {
    renamingId, renameValue, setRenameValue, startRename, cancelRename, saveRename,
    confirmDelete, setConfirmDelete, deleteOne, busyLabel,
  };

  return (
    <>
      <div className="flex items-center justify-between">
        <h2 className="text-fg-strong text-sm font-medium">
          Saved Universes {universes.length > 0 && <span className="text-fg-subtle font-normal">({universes.length})</span>}
        </h2>
        {universes.length > 0 && (
          confirmDeleteAll ? (
            <div className="flex items-center gap-2">
              <span className="text-sm text-neg-400">Delete all {universes.length}?</span>
              <button
                onClick={deleteAll}
                disabled={busyLabel === '__all__'}
                className="px-3 py-1.5 rounded-lg text-xs font-medium bg-neg-600 hover:bg-neg-500 text-fg-strong transition-colors disabled:opacity-50"
              >
                {busyLabel === '__all__' ? 'Deleting...' : 'Yes, delete all'}
              </button>
              <button
                onClick={() => setConfirmDeleteAll(false)}
                className="px-3 py-1.5 rounded-lg text-xs font-medium text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors"
              >
                Cancel
              </button>
            </div>
          ) : (
            <button
              onClick={() => setConfirmDeleteAll(true)}
              className="px-3 py-1.5 rounded-lg text-xs font-medium text-fg-subtle hover:text-neg-400 hover:bg-neg-500/10 transition-colors"
            >
              Delete all
            </button>
          )
        )}
      </div>

      {loading ? (
        <div className="bg-card rounded-xl border border-neutral-800/40 px-5 py-8 text-sm text-fg-subtle">
          <LoadingDots label="Loading" />
        </div>
      ) : error ? (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-5 py-4 text-sm text-neg-400">
          Failed to load: {error}
        </div>
      ) : universes.length === 0 ? (
        <div className="bg-card rounded-xl border border-neutral-800/40 px-5 py-8 text-sm text-fg-subtle">
          No universes saved yet.
        </div>
      ) : (
        <div className="space-y-3">
          {grouped.baseRows.map(u => {
            const children = grouped.childrenByParent.get(u.universe_id) ?? [];
            return (
              <div key={u.universe_id} className="space-y-2">
                <UniverseCard
                  u={u}
                  expanded={expandedId === u.universe_id}
                  onToggle={() => setExpandedId(expandedId === u.universe_id ? null : u.universe_id)}
                  {...cardCommon}
                  onTighten={() => setTighteningId(tighteningId === u.universe_id ? null : u.universe_id)}
                  tightening={tighteningId === u.universe_id}
                />
                {tighteningId === u.universe_id && derivedSpecs.length > 0 && (
                  <TightenPanel
                    base={u}
                    specs={derivedSpecs}
                    defaults={defaultConfig}
                    onClose={() => setTighteningId(null)}
                    onCreated={async () => {
                      setTighteningId(null);
                      await loadUniverses();
                    }}
                  />
                )}
                {children.map(child => (
                  <div key={child.universe_id} className="ml-6">
                    <UniverseCard
                      u={child}
                      expanded={expandedId === child.universe_id}
                      onToggle={() => setExpandedId(expandedId === child.universe_id ? null : child.universe_id)}
                      {...cardCommon}
                      derivedSpecs={derivedSpecs}
                    />
                  </div>
                ))}
              </div>
            );
          })}
          {grouped.orphans.length > 0 && (
            <div className="pt-2">
              <div className="text-xs text-fg-subtle mb-2">Derived universes with no matching parent</div>
              {grouped.orphans.map(u => (
                <UniverseCard
                  key={u.universe_id}
                  u={u}
                  expanded={expandedId === u.universe_id}
                  onToggle={() => setExpandedId(expandedId === u.universe_id ? null : u.universe_id)}
                  {...cardCommon}
                  derivedSpecs={derivedSpecs}
                />
              ))}
            </div>
          )}
        </div>
      )}
    </>
  );
}
