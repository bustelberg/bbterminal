'use client';

import SmartPipelineActivity from './schedule/SmartPipelineActivity';
import ScheduledStrategiesCard from './schedule/ScheduledStrategiesCard';
import DiversifiedPortfoliosCard from './schedule/DiversifiedPortfoliosCard';
import { useScheduledStrategies } from './schedule/useScheduledStrategies';
import { useIsAdmin } from '../../lib/hooks/useEffectiveRole';

// Two sections only:
//   1. Scheduled strategies — the strategies the user has pinned.
//   2. Smart pipeline activity — the single dependency-driven automation
//      that derives, from those strategies, exactly what's needed
//      (which universe to refresh, which companies to price, which
//      strategies are due to rebalance) and runs only that, observably.
//      It subsumes the old per-job / template-universe / daily-MTD cards.
//
// When adding to /schedule, add/extend a hook or section component under
// `app/components/schedule/` — don't regrow this orchestrator.

export default function Schedule() {
  const sched = useScheduledStrategies();
  const { error, setError, latestPriceDate } = sched;
  // Non-admins get a read-only view: only the strategies an admin flagged
  // visible, with no mutation controls and none of the admin automation cards.
  const isAdmin = useIsAdmin();

  return (
    <div className="min-h-screen bg-page text-fg">
      <div className="px-8 py-5 border-b border-neutral-800/40 flex items-end justify-between gap-4">
        <div>
          <h1 className="text-xl font-semibold text-fg-strong">Schedule</h1>
          <p className="text-sm text-fg-subtle mt-1">
            {isAdmin
              ? 'Your scheduled strategies and the automation that keeps just them up to date.'
              : 'The scheduled strategies shared with you (read-only).'}
          </p>
        </div>
        {latestPriceDate && (
          <div className="text-xs text-fg-subtle shrink-0">
            price data through <span className="text-fg-soft font-mono">{latestPriceDate}</span>
          </div>
        )}
      </div>

      <div className="px-8 py-6 space-y-6 max-w-screen-2xl">
        {error && (
          <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3 text-sm text-neg-300 flex items-center justify-between">
            <span>{error}</span>
            <button type="button" onClick={() => setError(null)} className="text-neg-200 hover:text-fg-strong text-xs">dismiss</button>
          </div>
        )}

        {/* Scheduled strategies — the user's pinned strategies. Read-only for
            non-admins (controls hidden; list already filtered server-side). */}
        <ScheduledStrategiesCard sched={sched} readOnly={!isAdmin} />

        {/* Admin-only lanes: live diversified-portfolio overlays + the pipeline
            automation. Hidden from the read-only user view. */}
        {isAdmin && (
          <>
            <DiversifiedPortfoliosCard />
            <SmartPipelineActivity />
          </>
        )}
      </div>
    </div>
  );
}
