'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import Spinner from '../Spinner';
import LoadingDots from '../LoadingDots';
import { API_URL } from '../../../lib/apiUrl';
import { apiFetch } from '../../../lib/apiFetch';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import { useNow } from '../../../lib/hooks/useNow';
import { usePollingFetch } from '../../../lib/hooks/usePollingFetch';
import CollapsibleCard from '../momentum/CollapsibleCard';
import { relTime, formatExecAt, countdownLeft } from './utils';
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
  // Poll fast (3s) only while a run is in flight so progress updates live;
  // back off to 30s when idle. Idle is the common case (nobody's running
  // anything), and these light status endpoints barely change then -- a slower
  // idle cadence cuts continuous Supabase load with no real UX cost (a scheduled
  // run is still detected within ~30s, after which polling jumps to 3s).
  const [active, setActive] = useState(true);
  const interval = active ? 3000 : 30000;
  const { data: upcoming, error: upErr } = usePollingFetch<ScheduleUpcoming>(`${API_URL}/api/schedule/upcoming`, interval);
  const { data: held, error: heldErr } = usePollingFetch<HeldCompaniesResponse>(`${API_URL}/api/scheduled-strategies/held-companies`, interval);
  const { data: strategies } = usePollingFetch<ScheduledStrategy[]>(`${API_URL}/api/scheduled-strategies`, interval);
  const { data: recentRuns } = usePollingFetch<IngestRun[]>(`${API_URL}/api/ingest/runs?limit=20`, interval);
  const { data: usage } = usePollingFetch<ApiUsage>(`${API_URL}/api/usage`, interval);
  // Coverage drives the freshest/most-stale display. Its aggregation is the
  // HEAVY part of this page (RPC scans over metric_data dates + every frozen
  // universe's membership), and it only changes when a price refresh runs. So
  // poll it briskly only while a run is active; when idle, drop to every 5 min
  // -- the coverage can't move without a run, so polling it 120x/hr idle is pure
  // wasted Disk IO.
  const coverageInterval = active ? 30000 : 300000;
  const { data: coverage } = usePollingFetch<PriceCoverage>(`${API_URL}/api/data/price-coverage`, coverageInterval);
  const { data: universeCoverage } = usePollingFetch<UniverseCoverage>(`${API_URL}/api/data/universe-coverage`, coverageInterval);
  const loadError = upErr ?? heldErr;
  const nowMs = useNow(15000);

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
        </>
      )}
    </div>
  );
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
      await apiFetch(`${API_URL}/api/ingest/scheduled-refresh/trigger?job_name=${job}${u}`, { method: 'POST' });
    } catch {
      // Polling surfaces the run (or its absence) — no inline error needed.
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

function PriceUpdateSection({
  running, lastRun, nextRunAt, schedulerOff, held, nowMs,
}: {
  running: RunningJob | null;
  lastRun: IngestRun | null;
  nextRunAt: string | null;
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
                    <th className="px-3 py-1.5 text-left font-medium">Ticker</th>
                    <th className="px-3 py-1.5 text-left font-medium">Company</th>
                    <th className="px-3 py-1.5 text-left font-medium">Sector</th>
                    <th className="px-3 py-1.5 text-right font-medium">Price</th>
                    <th className="px-3 py-1.5 text-right font-medium" title="Listing currency per 1 EUR — latest stored rate (same source as the FX page)">FX /€</th>
                    <th className="px-3 py-1.5 text-right font-medium" title="Close converted to EUR (local ÷ FX rate)">Price €</th>
                    <th className="px-3 py-1.5 text-right font-medium">Date</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-neutral-800/20">
                  {held.companies.map((c) => (
                    <HeldRow key={c.company_id} c={c} expected={fresh?.expected_close_date ?? null} />
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
      {lastRun ? (
        rebalanced.length > 0 ? (
          <div className="text-fg-subtle">
            Last rebalance {relTime(lastRun.finished_at ?? lastRun.started_at, nowMs)} ·{' '}
            {rebalanced.map((m) => `${m.strategy_name} (${m.holdings_count})`).join(', ')}
          </div>
        ) : (
          <div className="text-fg-subtle">
            Last run {relTime(lastRun.finished_at ?? lastRun.started_at, nowMs)} — no strategies were due.
          </div>
        )
      ) : (
        <div className="text-fg-subtle">No rebalance has run yet.</div>
      )}
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
    let cancelled = false;
    const tick = async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/ingest/runs/${staleRunId}`);
        if (cancelled || !r.ok) return;
        const row = await r.json() as Record<string, unknown>;
        if (cancelled) return;
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
      } catch {
        // transient — keep polling
      }
    };
    void tick();
    const id = setInterval(tick, 2000);
    return () => { cancelled = true; clearInterval(id); };
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

  // Companies the user just marked illiquid this session — hidden optimistically
  // (shown as "✓ illiquid · refreshing…") until the 30s coverage poll drops them.
  const [markedIlliquid, setMarkedIlliquid] = useState<Set<number>>(new Set());
  const markIlliquid = useCallback(async (cid: number) => {
    try {
      const r = await apiFetch(`${API_URL}/api/admin/company-illiquid`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ company_id: cid, illiquid: true }),
      });
      if (r.ok) setMarkedIlliquid((s) => new Set(s).add(cid));
    } catch {
      // The coverage poll reconciles regardless — no inline error needed.
    }
  }, []);

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
        Re-prices <span className="text-fg">every company</span> in the database (most-stale first), capped by the
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

      {coverage && (coverage.newest || coverage.oldest) && (
        <div className="space-y-1.5 pt-1 border-t border-neutral-800/30">
          <div className="text-[10px] uppercase tracking-wide text-fg-faint">
            Prices on file · {coverage.priced_companies.toLocaleString()} active companies — newest &amp; most-stale latest close (delisted / out-of-scope excluded)
          </div>
          <CoverageLine label="Newest" c={coverage.newest} tone="text-pos-400" />
          <CoverageLine
            label="Oldest"
            c={coverage.oldest}
            tone="text-warn-300"
            marked={coverage.oldest ? markedIlliquid.has(coverage.oldest.company_id) : false}
            onMark={coverage.oldest ? () => markIlliquid(coverage.oldest!.company_id) : undefined}
          />
        </div>
      )}

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

function HeldRow({ c, expected }: { c: HeldCompany; expected: string | null }) {
  const d = c.latest_close_price_date;
  // Fresh when the close is at/after the last settled trading day; stale when
  // behind it (new prices to fetch); missing when there's no close at all.
  const tone = d == null ? 'text-neg-400' : (expected && d >= expected) ? 'text-pos-400' : 'text-warn-300';
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
  return (
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
        {c.exchange && <span className="text-fg-faint">·{c.exchange}</span>}
      </td>
      <td className="px-3 py-1.5 text-fg-soft truncate max-w-[240px]">{c.company_name ?? '—'}</td>
      <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">{c.sector ?? '—'}</td>
      <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap text-fg">{priceLabel}</td>
      <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap text-fg-subtle">{fxLabel}</td>
      <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap text-fg">{eurLabel}</td>
      <td className={`px-3 py-1.5 text-right font-mono whitespace-nowrap ${tone}`}>{d ?? 'none'}</td>
    </tr>
  );
}
