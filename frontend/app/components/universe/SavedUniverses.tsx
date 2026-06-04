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
        <h2 className="text-white text-sm font-medium">
          Saved Universes {universes.length > 0 && <span className="text-gray-500 font-normal">({universes.length})</span>}
        </h2>
        {universes.length > 0 && (
          confirmDeleteAll ? (
            <div className="flex items-center gap-2">
              <span className="text-sm text-rose-400">Delete all {universes.length}?</span>
              <button
                onClick={deleteAll}
                disabled={busyLabel === '__all__'}
                className="px-3 py-1.5 rounded-lg text-xs font-medium bg-rose-600 hover:bg-rose-500 text-white transition-colors disabled:opacity-50"
              >
                {busyLabel === '__all__' ? 'Deleting...' : 'Yes, delete all'}
              </button>
              <button
                onClick={() => setConfirmDeleteAll(false)}
                className="px-3 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors"
              >
                Cancel
              </button>
            </div>
          ) : (
            <button
              onClick={() => setConfirmDeleteAll(true)}
              className="px-3 py-1.5 rounded-lg text-xs font-medium text-gray-500 hover:text-rose-400 hover:bg-rose-500/10 transition-colors"
            >
              Delete all
            </button>
          )
        )}
      </div>

      {loading ? (
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-5 py-8 text-sm text-gray-500">
          <LoadingDots label="Loading" />
        </div>
      ) : error ? (
        <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-5 py-4 text-sm text-rose-400">
          Failed to load: {error}
        </div>
      ) : universes.length === 0 ? (
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-5 py-8 text-sm text-gray-500">
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
              <div className="text-xs text-gray-500 mb-2">Derived universes with no matching parent</div>
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
