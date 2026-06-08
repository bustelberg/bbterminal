'use client';

import { useCallback, useEffect, useState } from 'react';
import Spinner from '../Spinner';
import LoadingDots from '../LoadingDots';
import { API_URL } from '../../../lib/apiUrl';
import { apiFetch } from '../../../lib/apiFetch';
import { useNow } from '../../../lib/hooks/useNow';
import { usePollingFetch } from '../../../lib/hooks/usePollingFetch';
import CollapsibleCard from '../momentum/CollapsibleCard';
import { relTime } from './utils';
import type {
  ScheduleUpcoming,
  HeldCompaniesResponse,
  HeldCompany,
  RunningJob,
  IngestRun,
  ScheduledStrategy,
} from './types';

/** Two independent operations of the split pipeline, stacked:
 *   1. Price update — re-prices the held companies + refreshes MTD (daily).
 *   2. Rebalance    — rebalances strategies that are due, from a fresh
 *      universe (runs when due; no-op otherwise).
 * They never run concurrently — the backend serializes them, so triggering
 * one while the other runs just queues it. Each section has its own status,
 * Run-now button, and detail. */
export default function SmartPipelineActivity() {
  // Poll fast (3s) only while a run is in flight so progress updates live;
  // back off to 15s when idle.
  const [active, setActive] = useState(true);
  const interval = active ? 3000 : 15000;
  const { data: upcoming, error: upErr } = usePollingFetch<ScheduleUpcoming>(`${API_URL}/api/schedule/upcoming`, interval);
  const { data: held, error: heldErr } = usePollingFetch<HeldCompaniesResponse>(`${API_URL}/api/scheduled-strategies/held-companies`, interval);
  const { data: strategies } = usePollingFetch<ScheduledStrategy[]>(`${API_URL}/api/scheduled-strategies`, interval);
  const { data: recentRuns } = usePollingFetch<IngestRun[]>(`${API_URL}/api/ingest/runs?limit=20`, interval);
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
        </>
      )}
    </div>
  );
}

/** Trigger one split-pipeline operation via its Run-now button. */
function useRunNow(job: string, busy: boolean) {
  const [pending, setPending] = useState(false);
  const run = useCallback(async () => {
    if (pending || busy) return;
    setPending(true);
    try {
      await apiFetch(`${API_URL}/api/ingest/scheduled-refresh/trigger?job_name=${job}`, { method: 'POST' });
    } catch {
      // Polling surfaces the run (or its absence) — no inline error needed.
    } finally {
      // Leave a brief window so the run row appears before re-enabling.
      setTimeout(() => setPending(false), 1500);
    }
  }, [job, pending, busy]);
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
            idleNode={<span className="font-mono">{nextRunAt ? `next ${relTime(nextRunAt, nowMs)}` : <LastResult run={lastRun} nowMs={nowMs} />}</span>}
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
            idleNode={<span className="font-mono">{nextDue ? `next due ${nextDue.slice(0, 10)}` : <LastResult run={lastRun} nowMs={nowMs} />}</span>}
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
      <td className={`px-3 py-1.5 text-right font-mono whitespace-nowrap ${tone}`}>{d ?? 'none'}</td>
    </tr>
  );
}
