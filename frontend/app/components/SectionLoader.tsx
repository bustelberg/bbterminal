'use client';

import Spinner from './Spinner';

/**
 * Centered spinner + "Loading {label}…" caption. Use this as a
 * placeholder block while a page section's data is being fetched —
 * e.g., a chart panel waiting on its metric series, or a table waiting
 * on its rows.
 *
 * For inline / single-line loading text prefer `<LoadingDots label="…"
 * />`. SectionLoader is the bigger, centered variant that anchors a
 * whole card panel.
 */
export default function SectionLoader({ label }: { label: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-10 gap-3">
      <div className="flex items-center gap-2">
        <Spinner className="h-4 w-4 text-indigo-400" />
        <span className="text-gray-400 text-sm">Loading {label}...</span>
      </div>
    </div>
  );
}
