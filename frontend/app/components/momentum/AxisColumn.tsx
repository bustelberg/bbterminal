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
}) {
  return (
    <div>
      <div className="flex items-center justify-between mb-1.5">
        <span className="text-gray-500 text-xs">
          {label}{' '}
          <span className="text-gray-600 text-[10px]">
            ({selected.size}/{options.length})
          </span>
        </span>
        <div className="flex items-center gap-2 text-[11px]">
          <button type="button" onClick={onAll} className="text-indigo-400 hover:text-indigo-300">
            All
          </button>
          <span className="text-gray-700">·</span>
          <button type="button" onClick={onNone} className="text-gray-400 hover:text-gray-200">
            None
          </button>
        </div>
      </div>
      <ul className={`border border-gray-800/60 rounded-lg p-1 overflow-auto ${maxHClass}`}>
        {options.length === 0 ? (
          <li className="px-3 py-2 text-xs text-gray-600">No options</li>
        ) : (
          options.map((opt) => <li key={String(opt)}>{renderItem(opt)}</li>)
        )}
      </ul>
    </div>
  );
}
