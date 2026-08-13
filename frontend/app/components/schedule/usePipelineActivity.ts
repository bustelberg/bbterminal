'use client';

import { useEffect, useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { useEventStream } from '../../../lib/hooks/useEventStream';
import { useNow } from '../../../lib/hooks/useNow';
import { usePollingFetch } from '../../../lib/hooks/usePollingFetch';
import type {
  HeldCompaniesResponse, IngestRun, RunningJob, ScheduledStrategy, ScheduleUpcoming,
} from './types';

/**
 * ⚠ THE PAYLOAD TYPES LIVE WITH THE FETCH, NOT WITH THE CARDS THAT DRAW THEM — so the dependency
 * runs one way (panels → hook) and there is no import cycle. They were declared inside the panels
 * file back when it also did the fetching.
 */
export type ApiUsage = { usa: number; europe: number; asia: number; month: string };

/** Freshest / most-stale company by latest close-price date — from `/api/data/price-coverage`.
 *  Lets the month-end refresh show prices actually moved. */
export type CoverageCompany = {
  company_id: number;
  company_name: string | null;
  ticker: string | null;
  exchange: string | null;
  date: string;
};
export type PriceCoverage = {
  newest: CoverageCompany | null;
  oldest: CoverageCompany | null;
  priced_companies: number;
};

/** Per-universe price + volume freshness — from `/api/data/universe-coverage`. For each STATIC
 *  (frozen) universe, the min (most-stale) / max (freshest) latest close-price and volume date
 *  across its active members, each with the company responsible. */
export type CoverageEndpoint = {
  date: string;
  company_id: number;
  ticker: string | null;
  exchange: string | null;
  company_name: string | null;
};
export type CoverageRange = { min: CoverageEndpoint | null; max: CoverageEndpoint | null; priced: number };
export type UniverseCoverageRow = {
  universe_id: number;
  label: string | null;
  frozen_from: string | null;
  members: number;
  price: CoverageRange;
  volume: CoverageRange;
};
export type UniverseCoverage = { universes: UniverseCoverageRow[] };

/**
 * EVERYTHING THE PIPELINE PANELS ARE DRAWN FROM — one stream, read once.
 *
 * ⚠⚠ EXTRACTED SO THE PANELS CAN LIVE BEHIND THEIR OWN ROWS. Before, the data layer and the four
 * cards were one component, so a card could only be rendered where that component was — which is
 * why the jobs table had to link DOWN to them instead of containing them. Splitting the fetch from
 * the drawing is what makes a per-job panel registry possible at all.
 *
 * ⚠ CALL IT ONCE PER PAGE. It opens the multi-topic SSE stream and (on failure) six polls. It is a
 * hook rather than a context because there is exactly one consumer — the jobs table — and a
 * provider would be ceremony around a single call.
 */
export type PipelineCtx = ReturnType<typeof usePipelineActivity>;

export function usePipelineActivity() {
  const [active, setActive] = useState(true);
  // PRIMARY transport: ONE SSE stream that pushes each topic only when it changes
  // (routers/_sse_stream.py) — no idle polling, closes when the tab is hidden. Server-side each
  // topic recomputes fast while a run is active, slow when idle. Polling below is a FALLBACK,
  // enabled only if the stream can't connect, so the page still works if SSE is blocked.
  const { data: stream, failed: sseFailed } = useEventStream('/api/schedule/stream');
  const triggerInterval = active ? 3000 : 30000;
  const statusInterval = active ? 3000 : 120000;
  const coverageInterval = active ? 30000 : 300000;
  const fb = (p: string) => (sseFailed ? `${API_URL}${p}` : null);
  const { data: upPoll, error: upErr } = usePollingFetch<ScheduleUpcoming>(fb('/api/schedule/upcoming'), triggerInterval);
  const { data: heldPoll, error: heldErr } = usePollingFetch<HeldCompaniesResponse>(fb('/api/scheduled-strategies/held-companies'), statusInterval);
  const { data: stratPoll } = usePollingFetch<ScheduledStrategy[]>(fb('/api/scheduled-strategies'), statusInterval);
  const { data: runsPoll } = usePollingFetch<IngestRun[]>(fb('/api/ingest/runs?limit=20'), statusInterval);
  const { data: usagePoll } = usePollingFetch<ApiUsage>(fb('/api/usage'), statusInterval);
  const { data: covPoll } = usePollingFetch<PriceCoverage>(fb('/api/data/price-coverage'), coverageInterval);
  const { data: uCovPoll } = usePollingFetch<UniverseCoverage>(fb('/api/data/universe-coverage'), coverageInterval);

  // Prefer the streamed payload; the poll value is only populated on SSE failure.
  const upcoming = (stream.upcoming as ScheduleUpcoming | undefined) ?? upPoll ?? null;
  const held = (stream.held as HeldCompaniesResponse | undefined) ?? heldPoll ?? null;
  // Guard against a non-array payload (e.g. an error body if the endpoint 500s) so a panel can't
  // crash on `.filter`.
  const strategies: ScheduledStrategy[] | null =
    (Array.isArray(stream.strategies) ? (stream.strategies as ScheduledStrategy[]) : null)
    ?? (Array.isArray(stratPoll) ? stratPoll : null);
  const recentRuns = (stream.runs as IngestRun[] | undefined) ?? runsPoll ?? null;
  const usage = (stream.usage as ApiUsage | undefined) ?? usagePoll ?? null;
  const coverage = (stream.price_coverage as PriceCoverage | undefined) ?? covPoll ?? null;
  const universeCoverage = (stream.universe_coverage as UniverseCoverage | undefined) ?? uCovPoll ?? null;
  const loadError = upErr ?? heldErr;
  // 1s tick so every relative-time / countdown display advances to the second — an at-a-glance
  // "is it live?" signal. Only lightweight status text depends on it.
  const nowMs = useNow(1000);

  useEffect(() => {
    // ⚠ STATE, NOT A DERIVED VALUE, AND IT CANNOT BE ONE. `active` picks the polling intervals,
    // and the polls are what produce `upcoming` — so deriving it would be circular on the fallback
    // path. One render's lag on switching cadence is the cost, and it is invisible.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setActive((upcoming?.running?.length ?? 0) > 0);
  }, [upcoming]);

  const running = (job: string): RunningJob | null =>
    upcoming?.running?.find((r) => r.job_name === job) ?? null;
  const lastRun = (job: string): IngestRun | null =>
    recentRuns?.find((r) => r.job_name === job) ?? null;

  // ⚠ THE ONLY SCHEDULER LOOKUP LEFT, AND IT IS NOT A SCHEDULE. The Automatic jobs table is the
  // single reader of `list_scheduled_jobs()`, so no panel draws a second countdown to a fire time.
  // This one survives because it is a CONDITION: the backend schedules a one-shot +3h retry only
  // when held prices are still behind after a price_update, so its presence IS the finding.
  const retryAt = upcoming?.jobs?.find((j) => j.id === 'price_update_retry')?.next_run_at ?? null;
  const schedulerOff = upcoming?.scheduler_enabled === false;
  const loading = upcoming == null && held == null;

  // Earliest upcoming rebalance across enabled strategies. ⚠ A STRATEGY fact, not a scheduler one —
  // when a rebalance will next have something to DO, which no job table can know.
  const nextDue = (strategies ?? [])
    .filter((s) => s.enabled && s.next_due_at)
    .map((s) => s.next_due_at as string)
    .sort()[0] ?? null;

  return {
    held, strategies, usage, coverage, universeCoverage, nowMs,
    running, lastRun, retryAt, schedulerOff, loading, loadError, nextDue,
  };
}
