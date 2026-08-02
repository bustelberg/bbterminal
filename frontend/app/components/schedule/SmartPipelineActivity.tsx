'use client';

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Spinner from '../Spinner';
import LoadingDots from '../LoadingDots';
import { API_URL } from '../../../lib/apiUrl';
import { apiFetch } from '../../../lib/apiFetch';
import { dialog } from '../../../lib/dialog';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import { useNow } from '../../../lib/hooks/useNow';
import { usePollingFetch } from '../../../lib/hooks/usePollingFetch';
import { useEventStream } from '../../../lib/hooks/useEventStream';
import { watchRun, type RunRow } from '../../../lib/watchRun';
import CollapsibleCard from '../momentum/CollapsibleCard';
import DailyHoldingsSection from './DailyHoldingsSection';
import { PriceRefreshPanel, useStockRefresh } from './priceRefresh';
import { tailRunToConsole } from './runConsole';
import { relTime, formatExecAt, countdownLeft, formatDur } from './utils';
import type {
  ScheduleUpcoming,
  HeldCompaniesResponse,
  HeldCompany,
  RunningJob,
  IngestRun,
  ScheduledStrategy,
} from './types';

/** GuruFocus monthly request cap per region — mirrors `ApiUsageBadge.LIMIT`
 * and the backend `MONTHLY_API_LIMIT`. */
const API_LIMIT = 20000;
type ApiUsage = { usa: number; europe: number; asia: number; month: string };

/** Freshest / most-stale company by latest close-price date — from
 * `/api/data/price-coverage`. Lets the month-end refresh show prices moved. */
type CoverageCompany = {
  company_id: number;
  company_name: string | null;
  ticker: string | null;
  exchange: string | null;
  date: string;
};
type PriceCoverage = {
  newest: CoverageCompany | null;
  oldest: CoverageCompany | null;
  priced_companies: number;
};

/** Per-universe price + volume freshness — from `/api/data/universe-coverage`.
 * For each STATIC (frozen) universe, the min (most-stale) / max (freshest)
 * latest close-price and volume date across its active members, each with the
 * company responsible, plus a per-universe manual refresh button. */
type CoverageEndpoint = {
  date: string;
  company_id: number;
  ticker: string | null;
  exchange: string | null;
  company_name: string | null;
};
type CoverageRange = { min: CoverageEndpoint | null; max: CoverageEndpoint | null; priced: number };
type UniverseCoverageRow = {
  universe_id: number;
  label: string | null;
  frozen_from: string | null;
  members: number;
  price: CoverageRange;
  volume: CoverageRange;
};
type UniverseCoverage = { universes: UniverseCoverageRow[] };

/** On-demand depth + gap check — from `/api/data/universe-history?label=`. Per
 * metric: earliest date, how many members have data / <1yr history / a >14-day
 * hole in the trailing year, and the worst offender of each. */
type CoverageWorst = {
  company_id: number;
  ticker: string | null;
  exchange: string | null;
  company_name: string | null;
  gap_days?: number;
  earliest?: string | null;
};
type HistoryMetric = {
  start: string | null;
  covered: number;
  no_data: number;
  short: number;
  gaps: number;
  worst_gap: CoverageWorst | null;
  worst_short: CoverageWorst | null;
};
type UniverseHistory = {
  label: string;
  members: number;
  since: string;
  error?: string;
  price?: HistoryMetric;
  volume?: HistoryMetric;
};

/** Per-company freshness breakdown for ONE universe — from
 * `/api/data/universe-staleness?universe_id=`. Lets a manual refresh be
 * verified: which members are up to date (`fresh`), which we failed to get
 * recent price/volume for (`flagged`), and which are expected-stale markers
 * (`excluded` — delisted / out-of-scope / illiquid). */
type StalenessCompany = {
  company_id: number;
  ticker: string | null;
  exchange: string | null;
  company_name: string | null;
  latest_close: string | null;
  latest_volume: string | null;
  close_days_behind: number | null;
  volume_days_behind: number | null;
  price_stale: boolean;
  volume_stale: boolean;
  marker: 'delisted' | 'out_of_scope' | 'illiquid' | null;
  status: 'fresh' | 'flagged' | 'excluded';
};
type UniverseStaleness = {
  universe_id: number;
  label: string | null;
  frozen_from: string | null;
  members: number;
  reference_date: string | null;
  stale_after: number;
  counts: { fresh: number; flagged: number; excluded: number };
  companies: StalenessCompany[];
};

/** Three independent operations of the split pipeline, stacked:
 *   1. Price update — re-prices the held companies + refreshes MTD (daily).
 *   2. Rebalance    — rebalances strategies that are due, from a fresh
 *      universe (runs when due; no-op otherwise).
 * They never run concurrently — the backend serializes them, so triggering
 * one while the other runs just queues it. Each section has its own status,
 * Run-now button, and detail. */
export default function SmartPipelineActivity() {
  const [active, setActive] = useState(true);
  // PRIMARY transport: ONE SSE stream that pushes each topic only when it
  // changes (routers/_sse_stream.py) — no idle polling, closes when the tab is
  // hidden. Server-side each topic recomputes fast while a run is active, slow
  // when idle (coverage stays at 30s/5min). Polling below is a FALLBACK, enabled
  // only if the stream can't connect, so the page still works if SSE is blocked.
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
  // Guard against a non-array payload (e.g. an error body if the endpoint 500s)
  // so the whole card can't crash on `.filter`.
  const strategies: ScheduledStrategy[] | null =
    (Array.isArray(stream.strategies) ? (stream.strategies as ScheduledStrategy[]) : null)
    ?? (Array.isArray(stratPoll) ? stratPoll : null);
  const recentRuns = (stream.runs as IngestRun[] | undefined) ?? runsPoll ?? null;
  const usage = (stream.usage as ApiUsage | undefined) ?? usagePoll ?? null;
  const coverage = (stream.price_coverage as PriceCoverage | undefined) ?? covPoll ?? null;
  const universeCoverage = (stream.universe_coverage as UniverseCoverage | undefined) ?? uCovPoll ?? null;
  const loadError = upErr ?? heldErr;
  // 1s tick so every relative-time / countdown display (next run, due, last run,
  // retry) advances to the second — an at-a-glance "is it live?" signal. Only
  // this lightweight header/status text depends on it; the heavy tables re-render
  // trivially since their props don't change.
  const nowMs = useNow(1000);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setActive((upcoming?.running?.length ?? 0) > 0);
  }, [upcoming]);

  const running = (job: string): RunningJob | null =>
    upcoming?.running?.find((r) => r.job_name === job) ?? null;
  const lastRun = (job: string): IngestRun | null =>
    recentRuns?.find((r) => r.job_name === job) ?? null;

  // Both ops fire off the one daily tick — its next fire time drives "next run".
  const dailyJob = upcoming?.jobs?.find((j) => j.id === 'daily_pipeline') ?? null;
  // One-shot stale-held-price retry (scheduled +3h out by the backend when held
  // prices are still behind after a price_update). Present only while a retry is
  // pending — drives the "Trying again in" countdown on the price-update card.
  const retryJob = upcoming?.jobs?.find((j) => j.id === 'price_update_retry') ?? null;
  // The month-end full-price-refresh job (its own monthly cron).
  const monthEndJob = upcoming?.jobs?.find((j) => j.id === 'month_end_price_refresh') ?? null;
  const schedulerOff = upcoming?.scheduler_enabled === false;
  const loading = upcoming == null && held == null;

  // Earliest upcoming rebalance across enabled strategies.
  const nextDue = (strategies ?? [])
    .filter((s) => s.enabled && s.next_due_at)
    .map((s) => s.next_due_at as string)
    .sort()[0] ?? null;

  return (
    <div className="space-y-3">
      <h2 className="text-sm uppercase tracking-wider text-fg-muted font-medium">
        Smart pipeline activity
      </h2>

      {loading && !loadError && (
        <div className="bg-card rounded-xl border border-neutral-800/40 px-5 py-3">
          <LoadingDots label="Loading" />
        </div>
      )}
      {loadError && loading && (
        <div className="bg-card rounded-xl border border-neutral-800/40 px-5 py-3">
          <span className="text-xs text-neg-300">Failed to load: {loadError}</span>
        </div>
      )}

      {!loading && (
        <>
          <PriceUpdateSection
            running={running('price_update')}
            lastRun={lastRun('price_update')}
            nextRunAt={dailyJob?.next_run_at ?? null}
            retryAt={retryJob?.next_run_at ?? null}
            schedulerOff={schedulerOff}
            held={held}
            nowMs={nowMs}
          />
          <RebalanceSection
            running={running('rebalance')}
            lastRun={lastRun('rebalance')}
            nextDue={nextDue}
            schedulerOff={schedulerOff}
            nowMs={nowMs}
          />
          <FullPriceRefreshSection
            running={running('full_price_refresh')}
            lastRun={lastRun('full_price_refresh')}
            nextRunAt={monthEndJob?.next_run_at ?? null}
            schedulerOff={schedulerOff}
            usage={usage}
            coverage={coverage}
            universeCoverage={universeCoverage}
            universeRefreshRunning={running('universe_price_refresh')}
            nowMs={nowMs}
          />
          {/* Not a pipeline operation — an on-demand question ABOUT one. It sits
              here because it is read from the same place the operations are, but
              it has no schedule, no run row and writes nothing. */}
          <DailyHoldingsSection strategies={strategies} />
        </>
      )}
    </div>
  );
}

/** Fire the trigger endpoint and tail the run's step transcript into the
 * browser console: every phase, every company the price refresh touched, every
 * computation, and the holdings each strategy ends up with. The card keeps its
 * one-line status; the detail goes where detail belongs.
 *
 * Fire-and-forget by design — the tail is a VIEW of the run, so a closed tab or
 * a failed poll must not touch the pipeline, which is running server-side
 * regardless. */
function startRun(url: string, label: string): void {
  void (async () => {
    try {
      const r = await apiFetch(url, { method: 'POST' });
      const body = (await r.json().catch(() => null)) as { run_id?: number } | null;
      if (!body?.run_id) {
        console.warn(`[${label}] trigger returned no run_id (HTTP ${r.status}) — nothing to tail`);
        return;
      }
      await tailRunToConsole(body.run_id, `${label} #${body.run_id}`);
    } catch (e) {
      // The polling card still surfaces the run (or its absence); the console
      // gets the diagnostic.
      console.warn(`[${label}] could not start or tail the run:`, e);
    }
  })();
}

/** Trigger one split-pipeline operation via its Run-now button. `universe`
 * (a label) is appended for the per-universe price refresh. */
function useRunNow(job: string, busy: boolean, universe?: string) {
  const [pending, setPending] = useState(false);
  const run = useCallback(async () => {
    if (pending || busy) return;
    setPending(true);
    try {
      const u = universe ? `&universe=${encodeURIComponent(universe)}` : '';
      startRun(`${API_URL}/api/ingest/scheduled-refresh/trigger?job_name=${job}${u}`, job);
    } finally {
      // Leave a brief window so the run row appears before re-enabling.
      setTimeout(() => setPending(false), 1500);
    }
  }, [job, pending, busy, universe]);
  return { run, pending };
}

function RunNowButton({ job, busy }: { job: string; busy: boolean }) {
  const { run, pending } = useRunNow(job, busy);
  const disabled = pending || busy;
  return (
    <button
      // stopPropagation so clicking Run-now inside the CollapsibleCard header
      // doesn't also toggle the card open/closed.
      onClick={(e) => { e.stopPropagation(); void run(); }}
      disabled={disabled}
      className="text-xs px-2.5 py-1 rounded-lg bg-accent-600 hover:bg-accent-500 text-white
                 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
    >
      {busy ? 'Running…' : pending ? 'Starting…' : 'Run now'}
    </button>
  );
}

/** Force re-rebalance: overrides the per-period LOCK — re-decides the current
 * period for EVERY enabled strategy, even ones already rebalanced this period.
 * Confirms first (it changes the of-record decision; originals stay in history). */
function ForceRebalanceButton({ busy }: { busy: boolean }) {
  const [pending, setPending] = useState(false);
  const disabled = pending || busy;
  const run = useCallback(async () => {
    if (disabled) return;
    const ok = await dialog.confirm(
      'Re-decide the CURRENT period for every enabled strategy — including ones already '
      + 'rebalanced this period — using the latest prices in the DB? By default a decided '
      + 'period is locked so it stays reproducible; this overrides that. The original '
      + 'decisions are kept in the run history.',
      { title: 'Force re-rebalance', confirmLabel: 'Force re-rebalance' },
    );
    if (!ok) return;
    setPending(true);
    try {
      startRun(
        `${API_URL}/api/ingest/scheduled-refresh/trigger?job_name=rebalance&force=true`,
        'rebalance (forced)',
      );
    } finally {
      setTimeout(() => setPending(false), 1500);
    }
  }, [disabled]);
  return (
    <button
      onClick={(e) => { e.stopPropagation(); void run(); }}
      disabled={disabled}
      title="Re-decide the current period for all enabled strategies (overrides the per-period lock). Originals stay in the run history."
      className="text-xs px-2.5 py-1 rounded-lg border border-warn-500/40 text-warn-300 hover:bg-warn-500/10
                 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
    >
      {pending ? 'Starting…' : 'Force re-rebalance'}
    </button>
  );
}

/** Exact next-execution time (the viewer's local timezone, with the tz
 * abbreviation) + a precise "Xd Yh left" countdown — shown in each pipeline
 * section's header bar. */
function NextRun({ at, nowMs }: { at: string | null; nowMs: number }) {
  if (!at) return null;
  return (
    <span className="flex items-center gap-1.5">
      <span className="font-mono text-fg-soft">{formatExecAt(at)}</span>
      <span className="text-fg-faint">·</span>
      <span className="font-mono text-accent-300">{countdownLeft(at, nowMs)}</span>
    </span>
  );
}

/** Live "Xh Ym Zs" countdown to `at`, ticking every SECOND on its own so the
 * visible seconds make it instantly obvious the timer is live (not stale). It
 * owns a 1s `useNow` — isolated to this tiny node so only it re-renders each
 * second, not the whole activity tree with its big tables. Renders nothing when
 * `at` is null/unparseable. */
function RetryCountdown({ at }: { at: string | null }) {
  const now = useNow(1000);
  if (!at) return null;
  const t = Date.parse(at);
  if (Number.isNaN(t)) return null;
  const diffSec = Math.round((t - now) / 1000);
  return <span className="font-mono">{diffSec <= 0 ? 'now' : formatDur(diffSec)}</span>;
}

/** Outcome chip for the most recent finished run of an operation. */
function LastResult({ run, nowMs }: { run: IngestRun | null; nowMs: number }) {
  if (!run || run.status === 'running') return null;
  const when = run.finished_at ?? run.started_at;
  const tone = run.status === 'error' ? 'text-neg-300' : 'text-fg-subtle';
  return (
    <span className={tone}>
      last run {relTime(when, nowMs)}{run.status === 'error' ? ' · failed' : ''}
    </span>
  );
}

/** Status portion of a section header summary: a live spinner + progress
 * count while running, "manual only" + last result when the scheduler is
 * off, or a green idle dot + the supplied idle node otherwise. */
function HeaderStatus({
  running, schedulerOff, lastRun, idleNode, nowMs,
}: {
  running: RunningJob | null;
  schedulerOff: boolean;
  lastRun: IngestRun | null;
  idleNode: React.ReactNode;
  nowMs: number;
}) {
  if (running) {
    const total = running.companies_total ?? 0;
    const done = running.companies_processed ?? 0;
    const showCount = running.current_phase === 'prices' && total > 0;
    return (
      <span className="flex items-center gap-1.5 text-accent-300">
        <Spinner className="h-3 w-3 shrink-0" />
        {showCount ? <span className="font-mono">{done}/{total}</span> : 'running…'}
      </span>
    );
  }
  if (schedulerOff) {
    return (
      <span className="flex items-center gap-2">
        <span className="text-warn-300/90">manual only</span>
        <LastResult run={lastRun} nowMs={nowMs} />
      </span>
    );
  }
  return (
    <span className="flex items-center gap-1.5">
      <span className="h-1.5 w-1.5 rounded-full bg-pos-500 shrink-0" />
      {idleNode}
    </span>
  );
}

/** Sortable columns of the held-companies table. */
type HeldSortKey = 'ticker' | 'exchange' | 'company' | 'sector' | 'price' | 'fx' | 'price_eur' | 'date';
type HeldSort = { key: HeldSortKey; dir: 'asc' | 'desc' };

/** A clickable column header that toggles sort on `k`. Shows ▲/▼ when active. */
function SortHeader({ label, k, sort, onSort, align = 'left', title }: {
  label: string;
  k: HeldSortKey;
  sort: HeldSort;
  onSort: (k: HeldSortKey) => void;
  align?: 'left' | 'right';
  title?: string;
}) {
  const active = sort.key === k;
  return (
    <th className={`px-3 py-1.5 font-medium ${align === 'right' ? 'text-right' : 'text-left'}`}>
      <button
        type="button"
        onClick={() => onSort(k)}
        title={title}
        className={`inline-flex items-center gap-0.5 hover:text-fg-soft transition-colors
                    ${active ? 'text-fg-soft' : ''} ${align === 'right' ? 'flex-row-reverse' : ''}`}
      >
        <span>{label}</span>
        <span className="text-[8px] w-2 leading-none">{active ? (sort.dir === 'asc' ? '▲' : '▼') : ''}</span>
      </button>
    </th>
  );
}

/** Sort held companies by the chosen column. Empty strings / null values always
 * sink to the bottom regardless of direction (they carry no comparable data). */
function sortHeld(companies: HeldCompany[], { key, dir }: HeldSort): HeldCompany[] {
  const val = (c: HeldCompany): string | number | null => {
    switch (key) {
      case 'ticker': return c.ticker ?? '';
      case 'exchange': return c.exchange ?? '';
      case 'company': return c.company_name ?? '';
      case 'sector': return c.sector ?? '';
      case 'price': return c.latest_close_price;
      case 'fx': return c.fx_rate_per_eur;
      case 'price_eur': return c.latest_close_price_eur;
      case 'date': return c.latest_close_price_date;
    }
  };
  return [...companies].sort((a, b) => {
    const av = val(a); const bv = val(b);
    const aNull = av == null || av === '';
    const bNull = bv == null || bv === '';
    if (aNull && bNull) return 0;
    if (aNull) return 1;
    if (bNull) return -1;
    const r = (typeof av === 'number' && typeof bv === 'number')
      ? av - bv
      : String(av).localeCompare(String(bv));
    return dir === 'asc' ? r : -r;
  });
}

function PriceUpdateSection({
  running, lastRun, nextRunAt, retryAt, schedulerOff, held, nowMs,
}: {
  running: RunningJob | null;
  lastRun: IngestRun | null;
  nextRunAt: string | null;
  retryAt: string | null;
  schedulerOff: boolean;
  held: HeldCompaniesResponse | null | undefined;
  nowMs: number;
}) {
  const fresh = held?.freshness_summary;
  const staleish = (fresh?.stale_count ?? 0) + (fresh?.missing_count ?? 0);
  const total = running?.companies_total ?? 0;
  const done = running?.companies_processed ?? 0;
  const showBar = !!running && total > 0 && running.current_phase === 'prices';
  const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;

  // Column sort — click a header to toggle asc/desc. Default: oldest close
  // first (the price-update view is about finding what's behind).
  const [sort, setSort] = useState<HeldSort>({ key: 'date', dir: 'asc' });
  const onSort = useCallback((k: HeldSortKey) => {
    setSort((s) => (s.key === k ? { key: k, dir: s.dir === 'asc' ? 'desc' : 'asc' } : { key: k, dir: 'asc' }));
  }, []);
  const sortedCompanies = useMemo(
    () => (held?.companies ? sortHeld(held.companies, sort) : []),
    [held, sort],
  );

  return (
    <CollapsibleCard
      title="Price update"
      defaultCollapsed
      bodyClassName="px-5 py-4 space-y-3"
      rightSlot={
        <>
          <HeaderStatus
            running={running}
            schedulerOff={schedulerOff}
            lastRun={lastRun}
            nowMs={nowMs}
            idleNode={nextRunAt ? <NextRun at={nextRunAt} nowMs={nowMs} /> : <LastResult run={lastRun} nowMs={nowMs} />}
          />
          {held && <span className="text-fg-faint">{held.total_companies} held</span>}
          {staleish > 0
            ? <span className="text-warn-300">{staleish} stale</span>
            : (held && held.total_companies > 0)
              ? <span className="text-pos-400">fresh</span>
              : null}
          {retryAt && (
            <span
              className="text-warn-300 flex items-center gap-1"
              title="Held prices are still behind the latest close (GuruFocus publish lag). An automatic re-price is scheduled — it retries every 3h (up to 3× a day), then the next daily tick takes over."
            >
              ↻ trying again in <RetryCountdown at={retryAt} />
            </span>
          )}
          {fresh?.latest_close_date && <span className="text-fg-faint font-mono">through {fresh.latest_close_date}</span>}
          <RunNowButton job="price_update" busy={!!running} />
        </>
      }
    >
      {showBar && (
        <div className="flex items-center gap-2">
          <div className="flex-1 h-1.5 rounded-full bg-inset overflow-hidden">
            <div className="h-full bg-accent-500 transition-all" style={{ width: `${pct}%` }} />
          </div>
          <span className="text-[11px] font-mono text-fg-faint shrink-0">{done}/{total}</span>
        </div>
      )}
      {held && (
        <>
          <div className="flex items-center justify-between gap-2">
            <span className="text-xs text-fg-soft">
              Held companies <span className="text-fg-faint">· {held.total_companies}</span>
            </span>
            <span className="flex items-center gap-2 text-[11px] font-mono">
              {fresh?.latest_close_date && <span className="text-fg-faint">through {fresh.latest_close_date}</span>}
              {(fresh?.fresh_count ?? 0) > 0 && <span className="text-pos-400">{fresh!.fresh_count} fresh</span>}
              {(fresh?.stale_count ?? 0) > 0 && <span className="text-warn-300">{fresh!.stale_count} stale</span>}
              {(fresh?.missing_count ?? 0) > 0 && <span className="text-neg-400">{fresh!.missing_count} missing</span>}
            </span>
          </div>

          {held.total_companies === 0 ? (
            <div className="text-xs text-fg-subtle">No holdings yet.</div>
          ) : (
            <div className="max-h-80 overflow-auto rounded-lg border border-neutral-800/40">
              <table className="w-full text-xs">
                <thead className="sticky top-0 bg-card z-10">
                  <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                    <SortHeader label="Ticker" k="ticker" sort={sort} onSort={onSort} />
                    <SortHeader label="Exch" k="exchange" sort={sort} onSort={onSort} title="Listing exchange" />
                    <SortHeader label="Company" k="company" sort={sort} onSort={onSort} />
                    <SortHeader label="Sector" k="sector" sort={sort} onSort={onSort} />
                    <SortHeader label="Price" k="price" sort={sort} onSort={onSort} align="right" />
                    <SortHeader label="FX /€" k="fx" sort={sort} onSort={onSort} align="right" title="Listing currency per 1 EUR — latest stored rate (same source as the FX page)" />
                    <SortHeader label="Price €" k="price_eur" sort={sort} onSort={onSort} align="right" title="Close converted to EUR (local ÷ FX rate)" />
                    <SortHeader label="Date" k="date" sort={sort} onSort={onSort} align="right" />
                    <th className="px-3 py-1.5 text-right font-medium" title="For a stale/missing held price we auto-retry the fetch every 3h (GuruFocus publish lag). Blank when the price is fresh or no retry is currently scheduled.">Trying again in</th>
                    <th className="px-3 py-1.5 text-right font-medium" title="Force a fresh GuruFocus price fetch for this one stock now — shows the actual request + response.">Refresh</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-neutral-800/20">
                  {sortedCompanies.map((c) => (
                    <HeldRow key={c.company_id} c={c} expected={fresh?.expected_close_date ?? null} retryAt={retryAt} />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </CollapsibleCard>
  );
}

function RebalanceSection({
  running, lastRun, nextDue, schedulerOff, nowMs,
}: {
  running: RunningJob | null;
  lastRun: IngestRun | null;
  nextDue: string | null;
  schedulerOff: boolean;
  nowMs: number;
}) {
  // Strategies actually rebalanced in the last run (status ok).
  const rebalanced = (lastRun?.momentum_summary ?? []).filter(
    (m) => m.kind === 'rebalance' && m.status === 'ok',
  );
  // Live progress: which phase + step the rebalance is on right now. The counter
  // only maps to a bar during the price fetch; every other phase (freshness,
  // momentum compute, …) is conveyed via the current_message so the card is
  // never just "running…" with no detail.
  const total = running?.companies_total ?? 0;
  const done = running?.companies_processed ?? 0;
  const showBar = !!running && total > 0 && running.current_phase === 'prices';
  const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;
  // Friendly label for the current pipeline phase.
  const PHASE_LABEL: Record<string, string> = {
    plan: 'Planning', templates: 'Refreshing universes', dedupe: 'Deduping listings',
    prices: 'Fetching prices', freshness: 'Checking freshness', momentum: 'Computing rebalance',
    deferred: 'Waiting on data', done: 'Finishing',
  };
  return (
    <CollapsibleCard
      title="Rebalance"
      defaultCollapsed
      bodyClassName="px-5 py-4 text-xs space-y-1.5"
      rightSlot={
        <>
          <HeaderStatus
            running={running}
            schedulerOff={schedulerOff}
            lastRun={lastRun}
            nowMs={nowMs}
            idleNode={nextDue ? <NextRun at={nextDue} nowMs={nowMs} /> : <LastResult run={lastRun} nowMs={nowMs} />}
          />
          <ForceRebalanceButton busy={!!running} />
          <RunNowButton job="rebalance" busy={!!running} />
        </>
      }
    >
      {nextDue && (
        <div className="text-fg-soft">
          Next rebalance due <span className="font-mono text-fg">{nextDue.slice(0, 10)}</span>
          <span className="text-fg-faint"> ({relTime(nextDue, nowMs)})</span>
        </div>
      )}
      {running && (
        <div className="space-y-1.5 rounded-lg bg-inset/60 px-3 py-2 border border-neutral-800/40">
          <div className="flex items-center gap-2">
            <Spinner className="h-3 w-3 shrink-0 text-accent-300" />
            <span className="text-[10px] uppercase tracking-wide px-1.5 py-0.5 rounded bg-accent-500/15 text-accent-300 border border-accent-500/30 shrink-0">
              {PHASE_LABEL[running.current_phase ?? ''] ?? (running.current_phase || 'Running')}
            </span>
            <span className="text-fg-subtle truncate">{running.current_message ?? 'Working…'}</span>
          </div>
          {showBar && (
            <div className="flex items-center gap-2">
              <div className="flex-1 h-1.5 rounded-full bg-inset overflow-hidden">
                <div className="h-full bg-accent-500 transition-all" style={{ width: `${pct}%` }} />
              </div>
              <span className="text-[11px] font-mono text-fg-faint shrink-0">{done}/{total}</span>
            </div>
          )}
        </div>
      )}
      {/* Last-run OUTCOME — always states what happened + why, so clicking
          "Run now" never flips silently back to idle with no explanation. */}
      {!running && (lastRun ? (() => {
        const when = relTime(lastRun.finished_at ?? lastRun.started_at, nowMs);
        const trigger = lastRun.triggered_by === 'manual' ? 'Run now' : 'Scheduled run';
        if (lastRun.status === 'error') {
          return (
            <div className="rounded-lg border border-neg-500/25 bg-neg-500/10 px-3 py-2">
              <div className="font-medium text-neg-300">✗ {trigger} failed {when}</div>
              {(lastRun.error_summary || lastRun.current_message) && (
                <div className="text-neg-300/80 mt-0.5 whitespace-pre-line">{lastRun.error_summary ?? lastRun.current_message}</div>
              )}
            </div>
          );
        }
        if (rebalanced.length > 0) {
          return (
            <div className="rounded-lg border border-pos-500/25 bg-pos-500/10 px-3 py-2">
              <div className="font-medium text-pos-300">
                ✓ Rebalanced {when} — {rebalanced.map((m) => `${m.strategy_name} (${m.holdings_count})`).join(', ')}
              </div>
              {lastRun.current_message && <div className="text-fg-subtle mt-0.5">{lastRun.current_message}</div>}
            </div>
          );
        }
        // Ran but rebalanced nothing — state exactly why (the backend's message).
        return (
          <div className="rounded-lg border border-neutral-800/40 bg-inset/60 px-3 py-2">
            <div className="font-medium text-fg-soft">{trigger} finished {when} — nothing to rebalance</div>
            <div className="text-fg-subtle mt-0.5">
              {lastRun.current_message
                ?? (lastRun.triggered_by === 'manual'
                  ? 'No enabled strategies to rebalance.'
                  : 'No strategies were due to rebalance.')}
            </div>
          </div>
        );
      })() : (
        <div className="text-fg-subtle">No rebalance has run yet.</div>
      ))}
    </CollapsibleCard>
  );
}

/** Month-end full-price refresh: re-prices EVERY company, bounded by the
 * monthly GuruFocus quota that's about to reset. Shows per-region budget left
 * (so you can see how much it has to spend), the next month-end run, a live
 * progress bar, and a Run-now button to spend the budget on demand. */
/** One freshest/most-stale coverage line: date + ticker (GuruFocus link) +
 * exchange + company name. When `onMark` is supplied, a "mark illiquid" button
 * lets the user flag a stale-but-dead listing so it drops out of the measure. */
function CoverageLine({ label, c, tone, marked, onMark }: {
  label: string;
  c: CoverageCompany | null;
  tone: string;
  marked?: boolean;
  onMark?: () => void;
}) {
  if (!c) return null;
  const href = c.ticker ? guruFocusUrl(c.ticker, c.exchange ?? '') : null;
  return (
    <div className="flex items-center gap-2 flex-wrap">
      <span className="text-fg-faint w-12 shrink-0">{label}</span>
      <span className={`font-mono ${tone}`}>{c.date}</span>
      <span className="text-fg-faint">·</span>
      {href ? (
        <a href={href} target="_blank" rel="noopener noreferrer" className="font-mono text-accent-400 hover:text-accent-300 hover:underline">
          {c.ticker}
        </a>
      ) : (
        <span className="font-mono text-fg">{c.ticker ?? '—'}</span>
      )}
      {c.exchange && <span className="text-fg-faint">·{c.exchange}</span>}
      <span className="text-fg-soft truncate max-w-[200px]">{c.company_name ?? '—'}</span>
      {onMark && (marked ? (
        <span className="text-[10px] text-warn-300">✓ illiquid · refreshing…</span>
      ) : (
        <button
          type="button"
          onClick={onMark}
          title="Mark as illiquid — trades rarely, so its stale GuruFocus price isn't a valid freshness measure. Excluded from this measure (still priced)."
          className="text-[10px] px-1.5 py-0.5 rounded border border-neutral-700 text-fg-muted hover:text-warn-300 hover:border-warn-500/50 transition-colors"
        >
          Mark illiquid
        </button>
      ))}
    </div>
  );
}

/** One min/max endpoint: its date + the company responsible (ticker GuruFocus
 * link + exchange; full name on hover). */
function CovEnd({ e }: { e: CoverageEndpoint | null }) {
  if (!e) return <span className="text-fg-faint">—</span>;
  const href = e.ticker ? guruFocusUrl(e.ticker, e.exchange ?? '') : null;
  return (
    <span className="font-mono" title={e.company_name ?? ''}>
      <span className="text-fg-subtle">{e.date}</span>{' '}
      {href ? (
        <a href={href} target="_blank" rel="noopener noreferrer" className="text-accent-400 hover:text-accent-300">{e.ticker}</a>
      ) : (
        <span className="text-fg-soft">{e.ticker ?? '—'}</span>
      )}
      {e.exchange && <span className="text-fg-faint">·{e.exchange}</span>}
    </span>
  );
}

/** A depth/gap readout line for one metric: history start, members-with-data,
 * <1yr count, gap count — green ✓ when complete, amber ⚠ otherwise. */
function HistoryLine({ label, m, members }: { label: string; m: HistoryMetric; members: number }) {
  const ok = m.no_data === 0 && m.short === 0 && m.gaps === 0;
  const yr = (m.start ?? '') && m.start! <= new Date(Date.now() - 365 * 864e5).toISOString().slice(0, 10);
  return (
    <div className="flex items-center gap-1.5 flex-wrap pl-1 text-[11px]">
      <span className={ok ? 'text-pos-400' : 'text-warn-300'}>{ok ? '✓' : '⚠'}</span>
      <span className="text-fg-muted w-9 shrink-0">{label}</span>
      <span className="text-fg-subtle">from</span>
      <span className={`font-mono ${yr ? 'text-fg-soft' : 'text-warn-300'}`}>{m.start ?? '—'}</span>
      {m.no_data > 0 && <span className="text-warn-300">· {m.no_data} no data</span>}
      <span className="text-fg-faint">· {m.covered}/{members} have data</span>
      <span className={m.short > 0 ? 'text-warn-300' : 'text-fg-faint'}>· {m.short} &lt;1yr</span>
      <span className={m.gaps > 0 ? 'text-warn-300' : 'text-fg-faint'}>
        · {m.gaps} gaps{m.worst_gap ? ` (worst ${m.worst_gap.ticker ?? '?'} ${m.worst_gap.gap_days}d)` : ''}
      </span>
    </div>
  );
}

/** One company row in the freshness breakdown: ticker·exchange, name, and the
 * latest close / volume date with trading-days-behind (or "no data"). */
function FreshnessRow({ c }: { c: StalenessCompany }) {
  const dateCell = (d: string | null, behind: number | null, stale: boolean) => {
    if (!d) return <span className="text-neg-300">no data</span>;
    return (
      <span className={stale ? 'text-warn-300' : 'text-fg-faint'}>
        {d}{behind != null && behind > 0 ? ` (−${behind}d)` : ''}
      </span>
    );
  };
  return (
    <div className="flex items-center gap-2 py-0.5 text-[11px]">
      <span className="font-mono text-fg-soft shrink-0 w-28 truncate" title={`${c.ticker ?? '?'}·${c.exchange ?? '?'}`}>
        {c.ticker ?? '?'}<span className="text-fg-faint">·{c.exchange ?? '?'}</span>
      </span>
      <span className="text-fg-muted truncate flex-1 min-w-0" title={c.company_name ?? ''}>{c.company_name ?? '—'}</span>
      <span className="font-mono shrink-0 w-24 text-right">{dateCell(c.latest_close, c.close_days_behind, c.price_stale)}</span>
      <span className="font-mono shrink-0 w-24 text-right">{dateCell(c.latest_volume, c.volume_days_behind, c.volume_stale)}</span>
      <span className="text-[10px] uppercase text-fg-faint shrink-0 w-16 text-right">{c.marker ? c.marker.replace('_', ' ') : ''}</span>
    </div>
  );
}

/** A collapsible group of company rows (Flagged / Fresh / Excluded), with a
 * sticky column header and a scroll cap so a 1,400-name fresh list stays sane. */
function FreshnessGroup({ title, color, rows, defaultOpen }: {
  title: string; color: string; rows: StalenessCompany[]; defaultOpen: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  if (rows.length === 0) return null;
  return (
    <div className="pt-1">
      <button type="button" onClick={() => setOpen((o) => !o)}
        className={`text-[11px] font-medium flex items-center gap-1 ${color}`}>
        <span className="font-mono">{open ? '▾' : '▸'}</span>
        {title} ({rows.length})
      </button>
      {open && (
        <div className="mt-1 max-h-56 overflow-auto border border-neutral-800/30 rounded-lg px-2 py-1">
          <div className="flex items-center gap-2 py-0.5 text-[10px] uppercase tracking-wide text-fg-faint sticky top-0 bg-card z-10">
            <span className="w-28 shrink-0">ticker·exch</span>
            <span className="flex-1 min-w-0">company</span>
            <span className="w-24 text-right shrink-0">close</span>
            <span className="w-24 text-right shrink-0">vol</span>
            <span className="w-16 shrink-0" />
          </div>
          {rows.map((c) => <FreshnessRow key={c.company_id} c={c} />)}
        </div>
      )}
    </div>
  );
}

/** The on-demand per-company freshness panel for one universe. Lists FLAGGED
 * (missing / stale price or volume), FRESH (up to date), and EXCLUDED
 * (delisted / out-of-scope / illiquid — expected stale) members. */
function StalenessDetail({ detail }: { detail: UniverseStaleness }) {
  const flagged = detail.companies.filter((c) => c.status === 'flagged');
  const fresh = detail.companies.filter((c) => c.status === 'fresh');
  const excluded = detail.companies.filter((c) => c.status === 'excluded');
  return (
    <div className="space-y-0.5">
      <div className="text-[11px] text-fg-muted flex flex-wrap gap-x-2 gap-y-0.5">
        <span className="text-pos-300">{detail.counts.fresh} fresh</span>
        <span className={detail.counts.flagged > 0 ? 'text-warn-300' : 'text-fg-faint'}>{detail.counts.flagged} flagged</span>
        {detail.counts.excluded > 0 && <span className="text-fg-faint">{detail.counts.excluded} excluded</span>}
        <span className="text-fg-faint">· vs {detail.reference_date ?? '—'} · &gt;{detail.stale_after} trading-days behind = stale</span>
      </div>
      <FreshnessGroup title="Flagged — missing / stale price or volume" color="text-warn-300" rows={flagged} defaultOpen />
      <FreshnessGroup title="Fresh — up to date" color="text-pos-300" rows={fresh} defaultOpen={false} />
      <FreshnessGroup title="Excluded — delisted / out-of-scope / illiquid (expected stale)" color="text-fg-faint" rows={excluded} defaultOpen={false} />
    </div>
  );
}

/** One static-universe coverage block: label + member count + Check/Refresh,
 * the latest-close & latest-volume freshness (min=most-stale → max=freshest,
 * each with the company), an on-demand history depth/gap check, an on-demand
 * per-company freshness breakdown (Inspect freshness), and — while THIS
 * universe is refreshing — a live progress bar + the job's message. */
function UniverseCoverageRow({ u, busy, progress, onTrigger }: {
  u: UniverseCoverageRow;
  busy: boolean;                // any universe refresh is running
  progress: RunningJob | null;  // the running job, iff it's THIS universe
  onTrigger: () => void;
}) {
  const { run, pending } = useRunNow('universe_price_refresh', busy, u.label ?? undefined);
  const [hist, setHist] = useState<UniverseHistory | null>(null);
  const [checking, setChecking] = useState(false);
  const [detail, setDetail] = useState<UniverseStaleness | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [detailErr, setDetailErr] = useState<string | null>(null);
  const [showDetail, setShowDetail] = useState(false);
  // "Flag if > N trading days behind" — lower it to catch the 1-day laggards.
  const [staleAfter, setStaleAfter] = useState(3);
  const total = progress?.companies_total ?? 0;
  const done = progress?.companies_processed ?? 0;
  const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;

  const check = async () => {
    if (checking || !u.label) return;
    setChecking(true);
    try {
      const r = await apiFetch(`${API_URL}/api/data/universe-history?label=${encodeURIComponent(u.label)}`);
      setHist(r.ok ? ((await r.json()) as UniverseHistory) : null);
    } catch {
      setHist(null);
    } finally {
      setChecking(false);
    }
  };

  const loadDetail = useCallback(async (threshold: number = staleAfter) => {
    setLoadingDetail(true);
    setDetailErr(null);
    try {
      const r = await apiFetch(`${API_URL}/api/data/universe-staleness?universe_id=${u.universe_id}&stale_after=${threshold}`);
      if (r.ok) setDetail((await r.json()) as UniverseStaleness);
      else { setDetail(null); setDetailErr(`${r.status}`); }
    } catch (e) {
      setDetail(null);
      setDetailErr(e instanceof Error ? e.message : String(e));
    } finally {
      setLoadingDetail(false);
    }
  }, [u.universe_id, staleAfter]);

  const toggleDetail = () => {
    setShowDetail((s) => {
      const next = !s;
      if (next && !detail && !loadingDetail) void loadDetail();
      return next;
    });
  };

  // Change the staleness threshold and re-classify (re-fetch with the new
  // value immediately so we don't read stale state).
  const changeThreshold = (n: number) => {
    if (n === staleAfter) return;
    setStaleAfter(n);
    if (showDetail) void loadDetail(n);
  };

  // Re-price ONLY the flagged (stale) companies — far cheaper than the whole
  // universe. We track the spawned run by id and poll it to completion so the
  // outcome is always reported inline (the job can finish between the parent's
  // 3s polls, so the shared progress bar alone isn't reliable for 1 company).
  type StaleRun = {
    runId: number;
    status: string;            // running | ok | error
    message: string | null;
    prices: number;
    volumes: number;
    forbidden: number;
    errors: number;
    errorSummary: string | null;
  };
  const [staleRun, setStaleRun] = useState<StaleRun | null>(null);
  const refreshingStale = staleRun?.status === 'running';
  const flaggedIds = detail?.companies.filter((c) => c.status === 'flagged').map((c) => c.company_id) ?? [];

  const refreshStale = async () => {
    if (refreshingStale || busy || flaggedIds.length === 0) return;
    onTrigger();
    setStaleRun({ runId: -1, status: 'running', message: 'Starting…', prices: 0, volumes: 0, forbidden: 0, errors: 0, errorSummary: null });
    try {
      const r = await apiFetch(`${API_URL}/api/ingest/scheduled-refresh/trigger?job_name=companies_price_refresh`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ company_ids: flaggedIds }),
      });
      if (!r.ok) {
        const t = await r.text().catch(() => '');
        setStaleRun({ runId: -1, status: 'error', message: null, prices: 0, volumes: 0, forbidden: 0, errors: 0, errorSummary: `${r.status} ${t.slice(0, 200)}` });
        return;
      }
      const j = (await r.json()) as { run_id?: number };
      if (j.run_id == null) {
        setStaleRun({ runId: -1, status: 'error', message: null, prices: 0, volumes: 0, forbidden: 0, errors: 0, errorSummary: 'No run id returned' });
        return;
      }
      setStaleRun({ runId: j.run_id, status: 'running', message: 'Queued — waiting for the pipeline…', prices: 0, volumes: 0, forbidden: 0, errors: 0, errorSummary: null });
    } catch (e) {
      setStaleRun({ runId: -1, status: 'error', message: null, prices: 0, volumes: 0, forbidden: 0, errors: 0, errorSummary: e instanceof Error ? e.message : String(e) });
    }
  };

  // Poll the spawned run until it reaches a terminal state, then reload the
  // breakdown so the flagged/fresh split reflects the freshly-fetched prices.
  const staleRunId = staleRun?.runId ?? null;
  const stalePolling = staleRun?.status === 'running' && (staleRun?.runId ?? -1) >= 0;
  useEffect(() => {
    if (staleRunId == null || staleRunId < 0 || !stalePolling) return;
    const abort = new AbortController();
    void watchRun(staleRunId, (row) => {
      const status = (row.status as string) ?? 'running';
      setStaleRun((prev) => (prev && prev.runId === staleRunId ? {
        ...prev,
        status,
        message: (row.current_message as string) ?? prev.message,
        prices: (row.prices_refreshed as number) ?? prev.prices,
        volumes: (row.volumes_refreshed as number) ?? prev.volumes,
        forbidden: (row.forbidden_count as number) ?? prev.forbidden,
        errors: (row.error_count as number) ?? prev.errors,
        errorSummary: (row.error_summary as string) ?? prev.errorSummary,
      } : prev));
      if (status !== 'running') void loadDetail();
    }, abort.signal);
    return () => { abort.abort(); };
  }, [staleRunId, stalePolling, loadDetail]);

  // Auto-reload the breakdown when THIS universe's refresh finishes, so the
  // flagged/fresh split reflects the freshly-fetched prices without a click.
  const wasRefreshing = useRef(false);
  useEffect(() => {
    const now = !!progress;
    if (wasRefreshing.current && !now && showDetail) void loadDetail();
    wasRefreshing.current = now;
  }, [progress, showDetail, loadDetail]);

  return (
    <div className="py-2 border-b border-neutral-800/20 last:border-0 space-y-1">
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-fg-soft font-medium" title={u.label ?? ''}>{u.label ?? '—'}</span>
        <span className="text-fg-faint font-mono">{u.members}co</span>
        <div className="ml-auto flex items-center gap-1.5">
          <button
            type="button"
            onClick={(e) => { e.stopPropagation(); void check(); }}
            disabled={checking}
            title={`Check that every ${u.label} member has ≥1yr of price/volume history with no >14-day gaps`}
            className="text-[11px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-muted
                       hover:text-accent-300 hover:border-accent-500/50 disabled:opacity-40
                       disabled:cursor-not-allowed transition-colors flex items-center gap-1.5"
          >
            {checking && <Spinner className="h-3 w-3" />}
            {checking ? 'Checking…' : 'Check history'}
          </button>
          <button
            type="button"
            onClick={(e) => { e.stopPropagation(); toggleDetail(); }}
            disabled={loadingDetail}
            title={`List every ${u.label} member's latest price/volume date — flagged (stale) vs fresh`}
            className="text-[11px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-muted
                       hover:text-accent-300 hover:border-accent-500/50 disabled:opacity-40
                       disabled:cursor-not-allowed transition-colors flex items-center gap-1.5"
          >
            {loadingDetail && <Spinner className="h-3 w-3" />}
            {showDetail ? 'Hide freshness' : 'Inspect freshness'}
          </button>
          <button
            type="button"
            onClick={(e) => { e.stopPropagation(); onTrigger(); void run(); }}
            disabled={pending || busy}
            title={`Re-fetch prices + volumes for every company in ${u.label} (within the monthly GuruFocus budget)`}
            className="text-[11px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-muted
                       hover:text-accent-300 hover:border-accent-500/50 disabled:opacity-40
                       disabled:cursor-not-allowed transition-colors flex items-center gap-1.5"
          >
            {progress && <Spinner className="h-3 w-3" />}
            {progress ? 'Refreshing…' : pending ? 'Starting…' : 'Refresh'}
          </button>
        </div>
      </div>

      {/* Freshness: latest close / volume, most-stale → freshest. */}
      <div className="flex items-center gap-1.5 flex-wrap pl-1 text-[11px]">
        <span className="text-fg-muted w-16 shrink-0">latest close</span>
        <CovEnd e={u.price.min} /><span className="text-fg-faint">→</span><CovEnd e={u.price.max} />
      </div>
      <div className="flex items-center gap-1.5 flex-wrap pl-1 text-[11px]">
        <span className="text-fg-muted w-16 shrink-0">latest vol</span>
        <CovEnd e={u.volume.min} /><span className="text-fg-faint">→</span><CovEnd e={u.volume.max} />
      </div>

      {/* On-demand depth + gap check. */}
      {hist?.price && <HistoryLine label="price" m={hist.price} members={hist.members} />}
      {hist?.volume && <HistoryLine label="vol" m={hist.volume} members={hist.members} />}
      {hist?.error && <div className="pl-1 text-[11px] text-neg-300">{hist.error}</div>}

      {/* On-demand per-company freshness breakdown (flagged vs fresh). */}
      {showDetail && (
        <div className="pl-1 pt-0.5">
          {loadingDetail && !detail ? (
            <div className="text-[11px] text-fg-faint flex items-center gap-1.5"><Spinner className="h-3 w-3" /> Loading freshness…</div>
          ) : detailErr ? (
            <div className="text-[11px] text-neg-300">Failed to load freshness: {detailErr}</div>
          ) : detail ? (
            <div className="space-y-1">
              <div className="flex items-center gap-1.5 flex-wrap text-[11px]">
                <span className="text-fg-faint">Flag if &gt;</span>
                {[0, 1, 2, 3, 5].map((n) => (
                  <button
                    key={n}
                    type="button"
                    onClick={(e) => { e.stopPropagation(); changeThreshold(n); }}
                    className={`px-1.5 py-0.5 rounded transition-colors ${
                      n === staleAfter
                        ? 'bg-accent-500/20 text-accent-200 border border-accent-500/40'
                        : 'text-fg-muted border border-neutral-700 hover:border-accent-500/50'
                    }`}
                  >
                    {n}d
                  </button>
                ))}
                <span className="text-fg-faint">trading days behind</span>
              </div>
              <div className="flex items-center gap-2 flex-wrap">
                {detail.counts.flagged > 0 && (
                  <button
                    type="button"
                    onClick={(e) => { e.stopPropagation(); void refreshStale(); }}
                    disabled={refreshingStale || busy}
                    title={`Re-fetch prices + volumes for ONLY the ${detail.counts.flagged} flagged compan${detail.counts.flagged === 1 ? 'y' : 'ies'} (within budget)`}
                    className="text-[11px] px-2 py-0.5 rounded-lg border border-warn-500/40 text-warn-300
                               hover:bg-warn-500/10 disabled:opacity-40 disabled:cursor-not-allowed
                               transition-colors flex items-center gap-1.5"
                  >
                    {refreshingStale && <Spinner className="h-3 w-3" />}
                    {refreshingStale ? 'Refreshing…' : `Refresh ${detail.counts.flagged} stale`}
                  </button>
                )}
                {staleRun && (
                  staleRun.status === 'running' ? (
                    <span className="text-[11px] text-accent-300 flex items-center gap-1.5">
                      <Spinner className="h-3 w-3" />{staleRun.message ?? 'Refreshing…'}
                    </span>
                  ) : staleRun.status === 'error' ? (
                    <span className="text-[11px] text-neg-300">✗ Refresh failed: {staleRun.errorSummary ?? staleRun.message ?? 'unknown error'}</span>
                  ) : (
                    <span className={`text-[11px] ${staleRun.errors || staleRun.forbidden ? 'text-warn-300' : 'text-pos-300'}`}>
                      ✓ Refreshed {staleRun.prices} price / {staleRun.volumes} volume series
                      {staleRun.forbidden ? `, ${staleRun.forbidden} forbidden` : ''}
                      {staleRun.errors ? `, ${staleRun.errors} errors` : ''}
                      {` · ${detail.counts.flagged} still flagged`}
                      {detail.counts.flagged > 0 ? ' (GuruFocus has no newer data — consider Mark illiquid / delisted)' : ''}
                    </span>
                  )
                )}
              </div>
              <StalenessDetail detail={detail} />
            </div>
          ) : null}
        </div>
      )}

      {/* Live refresh progress (this universe). */}
      {progress && (
        <div className="pl-1 space-y-1 pt-0.5">
          <div className="flex items-center gap-2">
            <div className="flex-1 h-1.5 rounded-full bg-inset overflow-hidden">
              <div className="h-full bg-accent-500 transition-all" style={{ width: `${total > 0 ? pct : 8}%` }} />
            </div>
            <span className="text-[11px] font-mono text-fg-faint shrink-0">{total > 0 ? `${done}/${total}` : '…'}</span>
          </div>
          <div className="text-[11px] text-accent-300">{progress.current_message ?? 'Queued — waiting for the pipeline…'}</div>
        </div>
      )}
    </div>
  );
}

/** The per-universe coverage list. Tracks which universe is mid-refresh so its
 * row shows live progress — only one refresh runs at a time (the backend
 * serializes the pipeline). Falls back to matching the running job's message on
 * reload (its opening line names the universe). */
function UniverseCoverageList({ universes, runningJob }: {
  universes: UniverseCoverageRow[];
  runningJob: RunningJob | null;
}) {
  const [refreshingLabel, setRefreshingLabel] = useState<string | null>(null);
  useEffect(() => { if (!runningJob) setRefreshingLabel(null); }, [runningJob]);

  const progressFor = (label: string | null): RunningJob | null => {
    if (!runningJob || !label) return null;
    if (refreshingLabel === label) return runningJob;
    return (runningJob.current_message ?? '').includes(label) ? runningJob : null;
  };

  return (
    <div className="space-y-1 pt-1 border-t border-neutral-800/30">
      <div className="text-[10px] uppercase tracking-wide text-fg-faint">
        Per static-universe coverage — most-stale (min) &amp; freshest (max) close-price &amp; volume date + the company responsible. Refresh re-fetches one universe within budget.
      </div>
      {universes.map((u) => (
        <UniverseCoverageRow
          key={u.universe_id}
          u={u}
          busy={!!runningJob}
          progress={progressFor(u.label)}
          onTrigger={() => setRefreshingLabel(u.label)}
        />
      ))}
    </div>
  );
}

/** One row of the month-end stale-price worklist (`/api/data/stale-prices`). */
type StaleCompany = {
  company_id: number;
  company_name: string | null;
  ticker: string | null;
  exchange: string | null;
  date: string;
  days_behind: number | null;
};
type StalePrices = {
  companies: StaleCompany[];
  reference_date: string | null;
  total_stale: number;
  priced_companies: number;
  limit: number;
  error?: string;
};

/** Per-company outcome of a refresh: whether that name's latest-close date
 * actually moved forward, so the user knows if the fetch changed anything. */
type RefreshOutcome = {
  company_id: number;
  ticker: string | null;
  exchange: string | null;
  before: string;
  after: string | null;   // null = caught up / off the worklist
  status: 'updated' | 'no_change';
};

/** Compare each refreshed company's before-date against its ACTUAL new latest
 * close date (from `/api/data/latest-close`, so caught-up names carry a real
 * date too). Newer date → `updated`; same/older/missing → GuruFocus had nothing
 * newer (`no_change`). */
function computeOutcomes(
  before: Map<number, { ticker: string | null; exchange: string | null; before: string }>,
  afterById: Map<number, string | null>,
): RefreshOutcome[] {
  return [...before.entries()].map(([id, m]) => {
    const after = afterById.get(id) ?? null;
    return {
      company_id: id, ticker: m.ticker, exchange: m.exchange, before: m.before, after,
      status: after && after > m.before ? ('updated' as const) : ('no_change' as const),
    };
  });
}

/** The month-end refresh worklist: up to 50 most-outdated ACTIVE companies, each
 * with its latest close date + trading-days behind the market's freshest close.
 * "Refresh all" re-fetches prices+volumes for every listed name; each row has its
 * own "Refresh" and "Illiquid". Refreshes run the `companies_price_refresh` job
 * (bounded by the GuruFocus budget), are polled to completion, then the list
 * reloads to show which names caught up. `busy` disables actions while another
 * pipeline op is running (the backend serializes them). */
function StalePricesPanel({ busy }: { busy: boolean }) {
  const [data, setData] = useState<StalePrices | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshingIds, setRefreshingIds] = useState<Set<number>>(new Set());
  const [runId, setRunId] = useState<number | null>(null);
  const [runMsg, setRunMsg] = useState<string | null>(null);
  const [marked, setMarked] = useState<Set<number>>(new Set());
  const [lastResults, setLastResults] = useState<RefreshOutcome[] | null>(null);
  // Snapshot of each refreshed company's latest-close date BEFORE the run, so
  // once it finishes we can report per-company whether the date actually moved.
  const pendingBeforeRef = useRef<Map<number, { ticker: string | null; exchange: string | null; before: string }>>(new Map());

  const load = useCallback(async (): Promise<StalePrices | null> => {
    setLoading(true);
    setError(null);
    try {
      const r = await apiFetch(`${API_URL}/api/data/stale-prices?limit=50`);
      if (r.ok) { const d = (await r.json()) as StalePrices; setData(d); return d; }
      setData(null); setError(`${r.status}`); return null;
    } catch (e) {
      setData(null); setError(e instanceof Error ? e.message : String(e)); return null;
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  // Poll the spawned refresh run to completion, then reload the worklist so the
  // caught-up names drop off. (The job can finish between the parent's polls, so
  // we track it ourselves rather than rely on the shared running-job state.)
  useEffect(() => {
    if (runId == null) return;
    const abort = new AbortController();
    let done = false;
    const finalize = async (row: RunRow) => {
      if (done) return;
      done = true;
      const status = (row.status as string) ?? 'ok';
      setRunMsg(status === 'error'
        ? `Refresh failed: ${(row.error_summary as string) ?? 'error'}`
        : `Fetched ${(row.prices_refreshed as number) ?? 0} price / ${(row.volumes_refreshed as number) ?? 0} volume series from GuruFocus`);
      setRunId(null);
      setRefreshingIds(new Set());
      // Look up each refreshed company's ACTUAL new latest-close date (so
      // caught-up names carry a real "through" date), report per-company what
      // moved, then reload the worklist so the fixed ones drop off.
      const before = pendingBeforeRef.current;
      if (before.size > 0) {
        const ids = [...before.keys()];
        let afterById = new Map<number, string | null>();
        try {
          const lc = await apiFetch(`${API_URL}/api/data/latest-close?ids=${ids.join(',')}`);
          if (lc.ok) {
            const j = (await lc.json()) as { dates: Record<string, string | null> };
            afterById = new Map(Object.entries(j.dates).map(([k, v]) => [Number(k), v]));
          }
        } catch { /* fall back to before-only */ }
        setLastResults(computeOutcomes(before, afterById));
        pendingBeforeRef.current = new Map();
      }
      void load();
    };
    // Watch the run over SSE (server closes the stream on terminal status).
    void watchRun(runId, (row) => {
      const status = (row.status as string) ?? 'running';
      if (status === 'running') setRunMsg((row.current_message as string) ?? 'Refreshing…');
      else void finalize(row);
    }, abort.signal).then((last) => {
      // Safety net if the stream ended without a terminal frame we handled.
      if (!done && last) void finalize(last);
    });
    return () => { abort.abort(); };
  }, [runId, load]);

  const anyRefreshing = runId != null;
  const disabled = anyRefreshing || busy;

  const refresh = useCallback(async (ids: number[]) => {
    if (ids.length === 0 || anyRefreshing || busy) return;
    // Snapshot before-dates from the current list so we can report, per company,
    // whether the refresh actually moved the latest-close date forward.
    const idSet = new Set(ids);
    pendingBeforeRef.current = new Map(
      (data?.companies ?? [])
        .filter((c) => idSet.has(c.company_id))
        .map((c) => [c.company_id, { ticker: c.ticker, exchange: c.exchange, before: c.date }]),
    );
    setLastResults(null);
    setRefreshingIds(new Set(ids));
    setRunMsg('Starting…');
    try {
      const r = await apiFetch(`${API_URL}/api/ingest/scheduled-refresh/trigger?job_name=companies_price_refresh`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ company_ids: ids }),
      });
      if (!r.ok) {
        const t = await r.text().catch(() => '');
        setRunMsg(`Failed: ${r.status} ${t.slice(0, 160)}`);
        setRefreshingIds(new Set());
        return;
      }
      const j = (await r.json()) as { run_id?: number };
      if (j.run_id == null) { setRunMsg('No run id returned'); setRefreshingIds(new Set()); return; }
      setRunMsg('Queued — waiting for the pipeline…');
      setRunId(j.run_id);
    } catch (e) {
      setRunMsg(e instanceof Error ? e.message : String(e));
      setRefreshingIds(new Set());
    }
  }, [anyRefreshing, busy, data]);

  const markIlliquid = useCallback(async (cid: number) => {
    setMarked((s) => new Set(s).add(cid));  // optimistic hide
    try {
      await apiFetch(`${API_URL}/api/admin/company-illiquid`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ company_id: cid, illiquid: true }),
      });
    } catch { /* the reload reconciles */ }
    void load();
  }, [load]);

  const rows = (data?.companies ?? []).filter((c) => !marked.has(c.company_id));
  const allIds = rows.map((c) => c.company_id);

  return (
    <div className="space-y-1.5 pt-1 border-t border-neutral-800/30">
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-[10px] uppercase tracking-wide text-fg-faint">
          Outdated prices{data ? ` · ${data.total_stale} behind${data.total_stale > (data.companies?.length ?? 0) ? ` (top ${data.companies.length} shown)` : ''}` : ''}
          {data?.reference_date && <> · vs {data.reference_date}</>}
        </span>
        <div className="ml-auto flex items-center gap-1.5">
          <button type="button" onClick={() => void load()} disabled={loading}
            className="text-[11px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 hover:border-accent-500/50 disabled:opacity-40 transition-colors">
            {loading ? 'Loading…' : 'Reload'}
          </button>
          {allIds.length > 0 && (
            <button type="button" onClick={() => void refresh(allIds)} disabled={disabled}
              title="Re-fetch prices + volumes for every listed company (within the monthly GuruFocus budget)"
              className="text-[11px] px-2 py-0.5 rounded-lg border border-warn-500/40 text-warn-300 hover:bg-warn-500/10 disabled:opacity-40 disabled:cursor-not-allowed transition-colors flex items-center gap-1.5">
              {anyRefreshing && <Spinner className="h-3 w-3" />}
              {anyRefreshing ? 'Refreshing…' : `Refresh all ${allIds.length}`}
            </button>
          )}
        </div>
      </div>

      {runMsg && (
        <div className={`text-[11px] flex items-center gap-1.5 ${runMsg.startsWith('Refresh failed') || runMsg.startsWith('Failed') ? 'text-neg-300' : anyRefreshing ? 'text-accent-300' : 'text-fg-subtle'}`}>
          {anyRefreshing && <Spinner className="h-3 w-3" />}{runMsg}
        </div>
      )}

      {lastResults && lastResults.length > 0 && (() => {
        const updated = lastResults.filter((r) => r.status === 'updated');
        const unchanged = lastResults.filter((r) => r.status === 'no_change');
        return (
          <div className="rounded-lg border border-neutral-800/40 bg-inset px-2.5 py-2 space-y-1">
            <div className="flex items-center justify-between">
              <span className="text-[10px] uppercase tracking-wide text-fg-faint">Refresh result</span>
              <button type="button" onClick={() => setLastResults(null)} className="text-[10px] text-fg-muted hover:text-fg">dismiss</button>
            </div>
            <div className="text-[11px] flex items-center gap-3 flex-wrap">
              <span className={updated.length > 0 ? 'text-pos-300' : 'text-fg-faint'}>✓ {updated.length} updated</span>
              <span className={unchanged.length > 0 ? 'text-warn-300' : 'text-fg-faint'}>— {unchanged.length} no newer data</span>
            </div>
            <div className="max-h-40 overflow-auto space-y-0.5 pt-0.5">
              {lastResults.map((r) => (
                <div key={r.company_id} className="text-[11px] flex items-center gap-2">
                  <span className={r.status === 'updated' ? 'text-pos-400' : 'text-warn-300'}>{r.status === 'updated' ? '✓' : '—'}</span>
                  <span className="font-mono whitespace-nowrap">{r.ticker ?? '—'}{r.exchange && <span className="text-fg-faint">·{r.exchange}</span>}</span>
                  {r.status === 'updated' ? (
                    <span className="text-fg-subtle">
                      was through <span className="font-mono text-fg-faint">{r.before}</span> · now through <span className="font-mono text-pos-300">{r.after}</span>
                    </span>
                  ) : (
                    <span className="text-warn-300/90">no newer data · still through <span className="font-mono">{r.before}</span></span>
                  )}
                </div>
              ))}
            </div>
          </div>
        );
      })()}

      {error ? (
        <div className="text-[11px] text-neg-300">Failed to load: {error}</div>
      ) : loading && !data ? (
        <div className="text-[11px] text-fg-faint flex items-center gap-1.5"><Spinner className="h-3 w-3" /> Loading…</div>
      ) : rows.length === 0 ? (
        <div className="text-[11px] text-pos-300">All active prices are up to date.</div>
      ) : (
        <div className="max-h-80 overflow-auto rounded-lg border border-neutral-800/40">
          <table className="w-full text-[11px]">
            <thead className="sticky top-0 bg-card z-10">
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                <th className="px-2 py-1 text-left font-medium">Ticker</th>
                <th className="px-2 py-1 text-left font-medium">Company</th>
                <th className="px-2 py-1 text-right font-medium">Latest close</th>
                <th className="px-2 py-1 text-right font-medium" />
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {rows.map((c) => {
                const href = c.ticker ? guruFocusUrl(c.ticker, c.exchange ?? '') : null;
                const one = refreshingIds.has(c.company_id);
                return (
                  <tr key={c.company_id} className="hover:bg-overlay/[0.02]">
                    <td className="px-2 py-1 font-mono whitespace-nowrap">
                      {href ? <a href={href} target="_blank" rel="noopener noreferrer" className="text-accent-400 hover:text-accent-300 hover:underline">{c.ticker}</a> : (c.ticker ?? '—')}
                      {c.exchange && <span className="text-fg-faint">·{c.exchange}</span>}
                    </td>
                    <td className="px-2 py-1 text-fg-soft truncate max-w-[220px]" title={c.company_name ?? ''}>{c.company_name ?? '—'}</td>
                    <td className="px-2 py-1 text-right font-mono whitespace-nowrap text-warn-300">
                      {c.date}{c.days_behind != null && c.days_behind > 0 ? ` (−${c.days_behind}d)` : ''}
                    </td>
                    <td className="px-2 py-1 text-right whitespace-nowrap">
                      <div className="flex items-center justify-end gap-1">
                        <button type="button" onClick={() => void refresh([c.company_id])} disabled={disabled}
                          title="Re-fetch this company's prices + volumes (within budget)"
                          className="text-[10px] px-1.5 py-0.5 rounded border border-neutral-700 text-fg-muted hover:text-accent-300 hover:border-accent-500/50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors inline-flex items-center gap-1">
                          {one && <Spinner className="h-2.5 w-2.5" />}Refresh
                        </button>
                        <button type="button" onClick={() => void markIlliquid(c.company_id)} disabled={disabled}
                          title="Mark illiquid — trades rarely, so its stale price isn't a valid freshness signal. Excluded from this measure (still priced)."
                          className="text-[10px] px-1.5 py-0.5 rounded border border-neutral-700 text-fg-muted hover:text-warn-300 hover:border-warn-500/50 disabled:opacity-40 transition-colors">
                          Illiquid
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function FullPriceRefreshSection({
  running, lastRun, nextRunAt, schedulerOff, usage, coverage, universeCoverage,
  universeRefreshRunning, nowMs,
}: {
  running: RunningJob | null;
  lastRun: IngestRun | null;
  nextRunAt: string | null;
  schedulerOff: boolean;
  usage: ApiUsage | null | undefined;
  coverage: PriceCoverage | null | undefined;
  universeCoverage: UniverseCoverage | null | undefined;
  universeRefreshRunning: RunningJob | null;
  nowMs: number;
}) {
  const total = running?.companies_total ?? 0;
  const done = running?.companies_processed ?? 0;
  const showBar = !!running && total > 0 && running.current_phase === 'prices';
  const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;

  const regions = usage
    ? ([
        { key: 'USA', used: usage.usa ?? 0 },
        { key: 'EU', used: usage.europe ?? 0 },
        { key: 'Asia', used: usage.asia ?? 0 },
      ] as const)
    : [];
  const totalLeft = regions.reduce((s, r) => s + Math.max(0, API_LIMIT - r.used), 0);

  return (
    <CollapsibleCard
      title="Month-end full price refresh"
      defaultCollapsed
      bodyClassName="px-5 py-4 text-xs space-y-3"
      rightSlot={
        <>
          <HeaderStatus
            running={running}
            schedulerOff={schedulerOff}
            lastRun={lastRun}
            nowMs={nowMs}
            idleNode={nextRunAt ? <NextRun at={nextRunAt} nowMs={nowMs} /> : <LastResult run={lastRun} nowMs={nowMs} />}
          />
          {usage && <span className="text-fg-faint font-mono">{totalLeft.toLocaleString()} calls left</span>}
          <RunNowButton job="full_price_refresh" busy={!!running} />
        </>
      }
    >
      <div className="text-fg-soft">
        Re-prices <span className="text-fg">every company</span>{' '}in the database (most-stale first), capped by the
        monthly GuruFocus quota that resets on the 1st — so the remaining budget is spent before it&apos;s lost.
        {nextRunAt && <> Runs automatically <span className="font-mono text-fg">{relTime(nextRunAt, nowMs)}</span> (last day of the month).</>}
      </div>

      {showBar && (
        <div className="flex items-center gap-2">
          <div className="flex-1 h-1.5 rounded-full bg-inset overflow-hidden">
            <div className="h-full bg-accent-500 transition-all" style={{ width: `${pct}%` }} />
          </div>
          <span className="text-[11px] font-mono text-fg-faint shrink-0">{done}/{total}</span>
        </div>
      )}

      {usage && (
        <div className="space-y-1.5">
          <div className="text-[10px] uppercase tracking-wide text-fg-faint">
            Budget left this month ({usage.month})
          </div>
          {regions.map((r) => {
            const left = Math.max(0, API_LIMIT - r.used);
            const usedPct = Math.min(100, Math.round((r.used / API_LIMIT) * 100));
            const tone = usedPct >= 90 ? 'bg-neg-500' : usedPct >= 70 ? 'bg-warn-500' : 'bg-accent-500';
            return (
              <div key={r.key} className="flex items-center gap-2">
                <span className="w-10 text-fg-muted">{r.key}</span>
                <div className="flex-1 h-1.5 rounded-full bg-inset overflow-hidden">
                  <div className={`h-full rounded-full ${tone}`} style={{ width: `${usedPct}%` }} />
                </div>
                <span className="font-mono text-fg-subtle whitespace-nowrap">
                  {left.toLocaleString()} left
                </span>
              </div>
            );
          })}
        </div>
      )}

      {coverage?.newest && (
        <div className="space-y-1.5 pt-1 border-t border-neutral-800/30">
          <div className="text-[10px] uppercase tracking-wide text-fg-faint">
            Prices on file · {coverage.priced_companies.toLocaleString()} active companies — freshest close (delisted / out-of-scope excluded)
          </div>
          <CoverageLine label="Newest" c={coverage.newest} tone="text-pos-400" />
        </div>
      )}

      {/* Month-end worklist: the most-outdated companies, refresh all or any one. */}
      <StalePricesPanel busy={!!running || !!universeRefreshRunning} />

      {universeCoverage && universeCoverage.universes.length > 0 && (
        <UniverseCoverageList
          universes={universeCoverage.universes}
          runningJob={universeRefreshRunning ?? null}
        />
      )}

      {running?.current_message && (
        <div className="text-fg-subtle">{running.current_message}</div>
      )}
      {!running && lastRun?.current_message && (
        <div className="text-fg-subtle">
          Last run {relTime(lastRun.finished_at ?? lastRun.started_at, nowMs)}: {lastRun.current_message}
        </div>
      )}
    </CollapsibleCard>
  );
}

function HeldRow({ c, expected, retryAt }: {
  c: HeldCompany;
  expected: string | null;
  retryAt: string | null;
}) {
  const d = c.latest_close_price_date;
  // Fresh when the close is at/after the last settled trading day; stale when
  // behind it (new prices to fetch); missing when there's no close at all.
  const isFresh = d != null && !!expected && d >= expected;
  const tone = d == null ? 'text-neg-400' : isFresh ? 'text-pos-400' : 'text-warn-300';
  // The auto-retry re-prices ONLY the stale/missing held names — so the "trying
  // again in" countdown belongs on those rows, not the up-to-date ones.
  const showRetry = !isFresh && !!retryAt;
  const price = c.latest_close_price;
  // Native-currency close: thousands-grouped, 2 decimals, with the currency code.
  const priceLabel = price == null
    ? '—'
    : `${price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}${c.currency ? ` ${c.currency}` : ''}`;
  // FX rate as of the price date: {ccy}/EUR (units per 1 EUR). Decimal
  // precision scales with magnitude (CHF ~0.95 vs JPY ~170) like /fx-rates.
  const fx = c.fx_rate_per_eur;
  const fxLabel = fx == null
    ? '—'
    : fx.toLocaleString(undefined, {
        minimumFractionDigits: fx < 10 ? 4 : fx < 1000 ? 2 : 0,
        maximumFractionDigits: fx < 10 ? 4 : fx < 1000 ? 2 : 0,
      });
  // Close converted to EUR (local ÷ rate).
  const eur = c.latest_close_price_eur;
  const eurLabel = eur == null
    ? '—'
    : `€${eur.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  // Per-asset price refresh. A holding priced by ≥1 strategy passes a
  // strategy_id so the fetch also re-prices that basket; the request/response
  // shows inline in a detail row below.
  const { refreshing, results, refresh, clear } = useStockRefresh();
  const strategyId = c.held_by?.[0]?.strategy_id ?? null;
  const busy = refreshing.has(c.company_id);
  const detail = results.get(c.company_id);
  // Highlight the action on stale/missing rows; subtle otherwise.
  const btnTone = isFresh
    ? 'text-fg-faint hover:text-accent-300 border-neutral-700'
    : 'text-warn-300 hover:text-warn-200 border-warn-500/40';
  return (
    <Fragment>
      <tr className="hover:bg-overlay/[0.02]">
        <td className="px-3 py-1.5 font-mono whitespace-nowrap">
          {c.gurufocus_url ? (
            <a
              href={c.gurufocus_url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-accent-400 hover:text-accent-300 hover:underline"
            >
              {c.ticker ?? '—'}
            </a>
          ) : (
            <span className="text-fg">{c.ticker ?? '—'}</span>
          )}
        </td>
        <td className="px-3 py-1.5 font-mono whitespace-nowrap text-fg-subtle">{c.exchange || '—'}</td>
        <td className="px-3 py-1.5 text-fg-soft truncate max-w-[240px]">{c.company_name ?? '—'}</td>
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">{c.sector ?? '—'}</td>
        <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap text-fg">{priceLabel}</td>
        <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap text-fg-subtle">{fxLabel}</td>
        <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap text-fg">{eurLabel}</td>
        <td className={`px-3 py-1.5 text-right font-mono whitespace-nowrap ${tone}`}>{d ?? 'none'}</td>
        <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap text-warn-300">
          {showRetry ? <RetryCountdown at={retryAt} /> : <span className="text-fg-faint">—</span>}
        </td>
        <td className="px-3 py-1.5 text-right whitespace-nowrap">
          <button
            type="button"
            onClick={() => void refresh(c.company_id, strategyId)}
            disabled={busy}
            title="Fetch this stock's price from GuruFocus now (bypasses cache) and show the request + response"
            className={`text-[11px] px-2 py-0.5 rounded-lg border ${btnTone} disabled:opacity-40 disabled:cursor-not-allowed transition-colors inline-flex items-center gap-1`}
          >
            {busy && <Spinner className="h-3 w-3" />}
            {busy ? 'Fetching…' : '↻ Refresh'}
          </button>
        </td>
      </tr>
      {detail && (
        <tr>
          <td colSpan={10} className="px-3 pb-2 pt-0">
            <PriceRefreshPanel result={detail} onClose={() => clear(c.company_id)} />
          </td>
        </tr>
      )}
    </Fragment>
  );
}
