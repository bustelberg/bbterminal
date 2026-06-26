'use client';

import SavedPortfoliosSection from '../diversifier/SavedPortfoliosSection';
import { useDiversifiedPortfolios } from './useDiversifiedPortfolios';

/** /schedule lane for LIVE-tracked diversified portfolios (overlays on a
 * scheduled strategy). Reuses the diversifier's portfolio list/state display.
 * Renders nothing when there are none (created from the diversifier page). */
export default function DiversifiedPortfoliosCard() {
  const { portfolios, state, error, viewState, remove } = useDiversifiedPortfolios();
  if (portfolios.length === 0) return null;
  return (
    <div>
      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-2.5 text-sm text-neg-300 mb-2">{error}</div>
      )}
      <SavedPortfoliosSection
        portfolios={portfolios}
        state={state}
        onView={viewState}
        onDelete={remove}
        title="Diversified portfolios (live)"
      />
    </div>
  );
}
