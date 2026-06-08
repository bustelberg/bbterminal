'use client';

import type { ReactNode } from 'react';

/** Reusable checkbox-list column with All / None controls and a per-
 * column scrollable list. Used by the variant-sweep picker on
 * `/backtest` for each of the four selectable axes (frequency,
 * strategy, universe, grouping). Generic in `T` so the parent passes
 * native enum / string types without casting. */
export default function AxisColumn<T>({
  label,
  options,
  selected,
  onAll,
  onNone,
  renderItem,
  maxHClass,
  loading = false,
}: {
  label: string;
  options: readonly T[];
  selected: ReadonlySet<T>;
  onAll: () => void;
  onNone: () => void;
  renderItem: (option: T) => ReactNode;
  /** Tailwind class for the inner `<ul>`'s max-height — varies between
   * the four axes since they have very different option counts
   * (frequency: 14, strategy: 2, universe: a few, grouping: 2). */
  maxHClass: string;
  /** Show a spinner instead of "No options" while options are still
   * being fetched (used by the async Universe axis). */
  loading?: boolean;
}) {
  return (
    <div>
      <div className="flex items-center justify-between mb-1.5">
        <span className="text-fg-subtle text-xs">
          {label}{' '}
          <span className="text-fg-faint text-[10px]">
            ({selected.size}/{options.length})
          </span>
        </span>
        <div className="flex items-center gap-2 text-[11px]">
          <button type="button" onClick={onAll} className="text-accent-400 hover:text-accent-300">
            All
          </button>
          <span className="text-fg-dim">·</span>
          <button type="button" onClick={onNone} className="text-fg-muted hover:text-fg">
            None
          </button>
        </div>
      </div>
      <ul className={`border border-neutral-800/60 rounded-lg p-1 overflow-auto ${maxHClass}`}>
        {options.length === 0 ? (
          <li className="px-3 py-2 text-xs text-fg-faint flex items-center gap-2">
            {loading ? (
              <>
                <span className="inline-block w-3 h-3 rounded-full border-2 border-neutral-700 border-t-accent-500 animate-spin" />
                Loading…
              </>
            ) : 'No options'}
          </li>
        ) : (
          options.map((opt) => <li key={String(opt)}>{renderItem(opt)}</li>)
        )}
      </ul>
    </div>
  );
}
