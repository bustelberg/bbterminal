'use client';

import DailyMtdRefreshCard from './DailyMtdRefreshCard';
import PipelineActivityCard from './schedule/PipelineActivityCard';
import TemplateUniversesCard from './schedule/TemplateUniversesCard';
import ScheduledStrategiesCard from './schedule/ScheduledStrategiesCard';
import { useScheduledStrategies } from './schedule/useScheduledStrategies';

// The pipeline still fires once a week (Tuesday 02:00 UTC) via the
// in-process APScheduler in `backend/scheduler.py`. The per-job cards
// and the global "Recent runs" view that used to live in this page
// have been removed — each scheduled strategy's run history is shown
// in its own expandable detail view (see ScheduledStrategyDetail).
//
// This component was decomposed (2026-06-04) into `app/components/schedule/`:
// data shapes live in `types.ts`, the pipeline-timeline derivation in
// `timeline.ts`, display helpers in `utils.ts`, the strategies-list state +
// mutations in `useScheduledStrategies.ts`, and each section card in its own
// file. When adding to /schedule, add/extend a hook or section card here —
// don't regrow this orchestrator.

export default function Schedule() {
  const sched = useScheduledStrategies();
  const { error, setError, latestPriceDate } = sched;

  return (
    <div className="min-h-screen bg-[#0f1117] text-gray-200">
      <div className="px-8 py-5 border-b border-gray-800/40 flex items-end justify-between gap-4">
        <div>
          <h1 className="text-xl font-semibold text-white">Schedule</h1>
          <p className="text-sm text-gray-500 mt-1">
            The automated pipeline and the strategies it keeps up to date.
          </p>
        </div>
        {latestPriceDate && (
          <div className="text-xs text-gray-500 shrink-0">
            price data through <span className="text-gray-300 font-mono">{latestPriceDate}</span>
          </div>
        )}
      </div>

      <div className="px-8 py-6 space-y-6 max-w-screen-2xl">
        {error && (
          <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-sm text-rose-300 flex items-center justify-between">
            <span>{error}</span>
            <button type="button" onClick={() => setError(null)} className="text-rose-200 hover:text-white text-xs">dismiss</button>
          </div>
        )}

        {/* Pipeline activity — what's running right now + what fires next.
            Polls the live scheduler so the user has a single at-a-glance
            oversight strip: running jobs (spinner + live phase) on top,
            then every scheduled job in chronological fire order. */}
        <PipelineActivityCard />

        {/* Template universes — visibility into the canonical universes
            (ACWI, LEONTEQ, ACWI_LEONTEQ, ...) and whether any of them
            need an initial refresh in this env. Placed above Misc jobs
            because "is this environment fully set up" is the question a
            user asks first when something looks wrong on /companies or
            /backtest. */}
        <TemplateUniversesCard />

        {/* Misc jobs — recurring side-tasks distinct from the
            per-strategy momentum compute. Today this hosts the daily
            held-companies price refresh; designed as a section so future
            misc jobs slot in alongside without churning the layout. */}
        <DailyMtdRefreshCard />

        {/* Scheduled strategies */}
        <ScheduledStrategiesCard sched={sched} />
      </div>
    </div>
  );
}
