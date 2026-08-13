'use client';

import AutomaticJobsCard from './schedule/AutomaticJobsCard';
import DailyHoldingsSection from './schedule/DailyHoldingsSection';
import ScheduledStrategiesCard from './schedule/ScheduledStrategiesCard';
import DiversifiedPortfoliosCard from './schedule/DiversifiedPortfoliosCard';
import { useScheduledStrategies } from './schedule/useScheduledStrategies';
import { useIsAdmin } from '../../lib/hooks/useEffectiveRole';
import { useBenchmarks } from '../../lib/hooks/apiData';

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
  // Warm `/api/benchmarks` up front so ETF ISINs/currencies are cached before a
  // strategy is expanded — otherwise CurrentPortfolioCard / SnapshotHoldings hit
  // a cache miss on that niche endpoint and the ETF's ISIN flashes in a beat
  // after the (already-warm /api/companies) stock ISINs. Fire-and-forget.
  useBenchmarks();
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

      <div className="px-8 py-6 space-y-6">
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
            {/* ⚠⚠ ONE SECTION FOR EVERY AUTOMATIC JOB — the "Smart pipeline activity" card is gone
                (2026-08-13). It was a second list of jobs with its own countdowns, read from the
                same `list_scheduled_jobs()` this table reads, so the page ran two clocks for one
                fire time. Its four panels now live behind the rows they belong to
                (`JOB_PANELS`), and this is the only reader of the scheduler on the page. */}
            <AutomaticJobsCard />
            {/* ⚠ NOT A JOB, WHICH IS WHY IT IS STILL A CARD OF ITS OWN. It has no schedule, no run
                row and writes nothing — an on-demand question ABOUT the pipeline — so it has no
                row to hide behind and would have been lost with the card that used to host it. It
                takes `strategies` from the page's existing hook rather than opening the activity
                stream for one read. */}
            <DailyHoldingsSection strategies={sched.strategies} />
          </>
        )}
      </div>
    </div>
  );
}
