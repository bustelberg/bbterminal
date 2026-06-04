'use client';

import CriteriaCard from './universe/CriteriaCard';
import SavedUniverses from './universe/SavedUniverses';
import { useUniverses } from './universe/useUniverses';

// This page was decomposed (2026-06-04) into `app/components/universe/`:
// all fetching + the rename/delete mutations + the base/derived grouping
// live in the `useUniverses` controller hook, shared shapes in `types.ts`,
// filter-pill/config helpers in `filterConfig.ts`, and each render piece
// (criteria card, saved-universes list, universe card, tighten panel,
// sparkline, sector breakdown) is its own component. When extending
// /universe, add/extend the hook or a component — don't regrow this
// orchestrator.

export default function UniverseScreener() {
  const ctl = useUniverses();

  return (
    <div className="h-full flex flex-col bg-[#0f1117]">
      <div className="px-8 py-5 border-b border-gray-800/60">
        <h1 className="text-white text-xl font-semibold">Universe Overview</h1>
        <p className="text-gray-500 text-sm mt-1">Quality criteria reference and detailed stats for every saved universe.</p>
      </div>

      <div className="flex-1 overflow-auto px-8 py-5 space-y-5">
        <CriteriaCard criteria={ctl.criteria} />
        <SavedUniverses ctl={ctl} />
      </div>
    </div>
  );
}
