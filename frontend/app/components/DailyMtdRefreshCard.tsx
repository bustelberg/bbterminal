'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { useApiData } from '../../lib/hooks/useApiData';
import { fmtTimestamp, fmtDateTime } from '../../lib/format';
import type { IngestRun } from './Schedule';
import LoadingDots from './LoadingDots';
import Spinner from './Spinner';
import { API_URL } from '../../lib/apiUrl';

/** One strategy's attribution for a held company. The daily refresh
 * will write fresh closes against the holdings on this snapshot. */
type HeldByEntry = {
  strategy_id: number;
  strategy_name: string;
  snapshot_id: number;
  snapshot_kind: 'rebalance' | 'price_update' | string;
  as_of_date: string | null;
  latest_price_date: string | null;
  target_weight: number;
  score: number | null;
  entry_price_local: number | null;
  entry_date: string | null;
};

type HeldCompany = {
  company_id: number;
  ticker: string;
  exchange: string;
  company_name: string | null;
  sector: string | null;
  /** Max(target_date) in metric_data.close_price for this company.
   * Null when no close_price observation exists yet (newly added
   * ticker the prices phase hasn't reached). */
  latest_close_price_date: string | null;
  held_by: HeldByEntry[];
};

/** Aggregate freshness across all held companies, surfaced as a
 * top-of-card "Data freshness" panel so the user can answer the
 * "are we missing today's data?" question at a glance. */
type FreshnessSummary = {
  /** Max(latest_close_price_date) across the held set. The freshest
   * close-price observation that exists for any company we hold. */
  latest_close_date: string | null;
  fresh_count: number;    // companies whose latest matches latest_close_date
  stale_count: number;    // companies with an older latest target_date
  missing_count: number;  // companies with no close_price observation at all
};

type HeldCompaniesResponse = {
  total_companies: number;
  total_strategies: number;
  freshness_summary?: FreshnessSummary;
  companies: HeldCompany[];
};

// Daily job fires every day EXCEPT Tuesday (the weekly full pipeline
// covers Tue), at 02:00 UTC. UTC weekday: 0=Sun, 1=Mon, … 6=Sat — so the
// tick days are everything but 2 (Tue). Plus a startup catch-up fires it
// immediately whenever held snapshots are stale.
const DAILY_TICK_DAYS = new Set([0, 1, 3, 4, 5, 6]);

function computeNextDailyTickUtc(now: Date): Date {
  const next = new Date(Date.UTC(
    now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 2, 0, 0, 0,
  ));
  for (let i = 0; i < 14; i++) {
    if (DAILY_TICK_DAYS.has(next.getUTCDay()) && next.getTime() > now.getTime()) {
      return next;
    }
    next.setUTCDate(next.getUTCDate() + 1);
  }
  return next;
}

function statusBadgeCls(status: IngestRun['status'] | undefined): string {
  if (status === 'ok') return 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30';
  if (status === 'error') return 'bg-rose-500/10 text-rose-300 border-rose-500/30';
  if (status === 'running') return 'bg-amber-500/15 text-amber-300 border-amber-500/30';
  return 'bg-gray-500/10 text-gray-400 border-gray-500/30';
}

export default function DailyMtdRefreshCard() {
  const [runs, setRuns] = useState<IngestRun[] | null>(null);
  const [triggering, setTriggering] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState(false);
  // Held-companies panel — fetched lazily once the card is expanded, and
  // re-fetched on re-expand / while a run is active (via reloadHeld).
  const {
    data: held,
    loading: heldLoading,
    error: heldError,
    reload: reloadHeld,
  } = useApiData<HeldCompaniesResponse>('/api/scheduled-strategies/held-companies', {
    enabled: expanded,
  });
  // Which run is currently click-expanded for per-strategy detail.
  // null = none. Survives revalidate fetches.
  const [expandedRunId, setExpandedRunId] = useState<number | null>(null);

  const load = useCallback(async () => {
    try {
      const r = await fetch(
        `${API_URL}/api/ingest/runs?job_name=daily_holdings_refresh&limit=10`,
      );
      if (!r.ok) return;
      const data = (await r.json()) as IngestRun[];
      setRuns(Array.isArray(data) ? data : []);
    } catch {
      // silent — the card will just say "No runs yet"
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  // Poll every 5s while the last run is in 'running' so the UI moves
  // through the phases live. Idle otherwise.
  const lastStatus = runs?.[0]?.status;
  useEffect(() => {
    if (lastStatus !== 'running') return;
    const id = setInterval(() => void load(), 5_000);
    return () => clearInterval(id);
  }, [lastStatus, load]);

  // Also revalidate held companies + freshness while a run is active.
  // 10s cadence — heavier query than the runs probe but still cheap;
  // makes the freshness panel actually extend as the prices phase
  // writes new closes into metric_data. Only fires when the card is
  // expanded so we don't waste round-trips when no one's looking.
  useEffect(() => {
    if (!expanded || lastStatus !== 'running') return;
    const id = setInterval(() => reloadHeld(), 10_000);
    return () => clearInterval(id);
  }, [expanded, lastStatus, reloadHeld]);

  const runNow = useCallback(async () => {
    setTriggering(true);
    setError(null);
    try {
      const r = await apiFetch(
        `${API_URL}/api/ingest/scheduled-refresh/trigger?job_name=daily_holdings_refresh`,
        { method: 'POST' },
      );
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Trigger failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setTriggering(false);
    }
  }, [load]);

  const last = runs?.[0] ?? null;
  const isRunning = last?.status === 'running';
  const nextTick = !isRunning ? computeNextDailyTickUtc(new Date()) : null;

  const summaryLine = (() => {
    if (!last) return 'No runs yet — fires daily (except Tue) at 02:00 UTC, catches up automatically on startup when stale, or click Run now';
    if (last.status === 'running') return last.current_message || 'running…';
    if (last.status === 'error') return last.error_summary || 'failed';
    const mom = last.momentum_summary || [];
    const ok = mom.filter((m) => m.status === 'ok').length;
    const err = mom.filter((m) => m.status === 'error').length;
    const denom = last.companies_total ?? '?';
    return `${ok} strateg${ok === 1 ? 'y' : 'ies'} refreshed${err > 0 ? `, ${err} failed` : ''} · ${last.companies_processed}/${denom} companies`;
  })();

  return (
    <div className="bg-[#151821] rounded-xl border border-gray-800/40">
      <div className="px-5 py-3 border-b border-gray-800/40 flex items-center justify-between gap-3">
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          className="flex items-center gap-3 flex-1 min-w-0 text-left"
        >
          <span className="text-gray-500 font-mono text-xs w-4 shrink-0">{expanded ? '▾' : '▸'}</span>
          <div className="flex-1 min-w-0">
            <h3 className="text-sm font-medium text-white">Daily MTD refresh</h3>
            <p className="text-xs text-gray-500 mt-0.5">
              Daily (except Tue) 02:00 UTC + an automatic catch-up on startup when snapshots are stale · pools held companies across all enabled strategies, refreshes prices, then persists MTD onto each strategy&apos;s latest snapshot
            </p>
          </div>
        </button>
        <div className="flex items-center gap-3 shrink-0">
          {last && (
            <span className={`inline-flex items-center text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border ${statusBadgeCls(last.status)}`}>
              {last.status}
            </span>
          )}
          <button
            type="button"
            onClick={() => void runNow()}
            disabled={triggering || isRunning}
            className="text-xs px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {triggering ? 'Starting…' : isRunning ? 'Running…' : 'Run now'}
          </button>
        </div>
      </div>
      <div className="px-5 py-3 text-xs text-gray-400 flex items-center gap-6 flex-wrap">
        <div>
          <span className="text-gray-500">Last:</span>{' '}
          <span className="text-gray-300 font-mono">{fmtTimestamp(last?.finished_at ?? last?.started_at ?? null)}</span>
        </div>
        <div>
          <span className="text-gray-500">Next:</span>{' '}
          <span className="text-gray-300 font-mono">{nextTick ? fmtDateTime(nextTick) : '—'}</span>
        </div>
        <div className="text-gray-400 truncate flex-1 min-w-0" title={summaryLine}>
          {summaryLine}
        </div>
      </div>
      {error && (
        <div className="px-5 py-2 bg-rose-500/10 border-t border-rose-500/20 text-xs text-rose-300 flex items-center justify-between">
          <span>{error}</span>
          <button type="button" onClick={() => setError(null)} className="text-rose-200 hover:text-white">dismiss</button>
        </div>
      )}
      {expanded && (
        <div className="border-t border-gray-800/30 divide-y divide-gray-800/30">
          {/* ── In-flight live progress (only while a run is active) ── */}
          {last && last.status === 'running' && (
            <LiveProgressPanel run={last} />
          )}
          {/* ── Data freshness summary ─────────────────────────── */}
          <DataFreshnessPanel
            summary={held?.freshness_summary ?? null}
            companyCount={held?.total_companies ?? null}
            loading={heldLoading && !held}
          />
          {/* ── Sub-section 1: Currently held companies ───────── */}
          <div>
            <div className="px-5 py-3 flex items-baseline justify-between gap-3">
              <div>
                <h4 className="text-xs font-medium text-gray-300 uppercase tracking-wider">
                  Currently held companies
                </h4>
                <p className="text-[11px] text-gray-500 mt-0.5">
                  Pooled across every enabled strategy&apos;s latest snapshot — these are the rows the next daily refresh will price.
                </p>
              </div>
              {held && (
                <div className="text-[11px] text-gray-500 shrink-0 font-mono">
                  {held.total_companies} compan{held.total_companies === 1 ? 'y' : 'ies'} · {held.total_strategies} strateg{held.total_strategies === 1 ? 'y' : 'ies'}
                </div>
              )}
            </div>
            <HeldCompaniesTable
              data={held}
              loading={heldLoading}
              error={heldError}
              onRetry={() => reloadHeld()}
            />
          </div>

          {/* ── Sub-section 2: Recent runs (click to expand) ──── */}
          <div>
            <div className="px-5 py-3">
              <h4 className="text-xs font-medium text-gray-300 uppercase tracking-wider">
                Recent runs
              </h4>
              <p className="text-[11px] text-gray-500 mt-0.5">
                Click a row to see per-strategy outcome, error messages, and Python tracebacks.
              </p>
            </div>
            {runs === null ? (
              <div className="px-5 py-4 text-sm text-gray-500"><LoadingDots label="Loading" /></div>
            ) : runs.length === 0 ? (
              <div className="px-5 py-4 text-sm text-gray-500">No runs yet.</div>
            ) : (
              <div className="divide-y divide-gray-800/30">
                {runs.map((r) => (
                  <DailyRunRow
                    key={r.run_id}
                    run={r}
                    expanded={expandedRunId === r.run_id}
                    onToggle={() =>
                      setExpandedRunId((cur) => (cur === r.run_id ? null : r.run_id))
                    }
                  />
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

/** Table of pooled held companies. Each row carries one or more
 * strategy chips showing which scheduled strategies hold the company
 * and as of which rebalance date. */
function HeldCompaniesTable({
  data,
  loading,
  error,
  onRetry,
}: {
  data: HeldCompaniesResponse | null;
  loading: boolean;
  error: string | null;
  onRetry: () => void;
}) {
  if (error) {
    return (
      <div className="px-5 py-4 text-xs flex items-center justify-between bg-rose-500/5 border-t border-rose-500/20 text-rose-300">
        <span>Couldn&apos;t load held companies: {error}</span>
        <button
          type="button"
          onClick={onRetry}
          className="text-rose-200 hover:text-white"
        >
          Retry
        </button>
      </div>
    );
  }
  if (loading && !data) {
    return (
      <div className="px-5 py-4 text-sm text-gray-500 inline-flex items-center gap-2">
        <Spinner size={12} />
        <span>Loading held companies…</span>
      </div>
    );
  }
  if (!data || data.companies.length === 0) {
    return (
      <div className="px-5 py-4 text-sm text-gray-500">
        No companies held — either no enabled strategies, or none have produced a snapshot yet.
      </div>
    );
  }

  const latestCloseDate = data.freshness_summary?.latest_close_date ?? null;
  return (
    <div className="px-5 pb-4">
      <div className="overflow-x-auto rounded-lg border border-gray-800/40">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-gray-800/40 text-gray-500 text-left">
              <th className="px-3 py-2 font-medium">Company</th>
              <th className="px-3 py-2 font-medium w-32">Sector</th>
              <th className="px-3 py-2 font-medium w-28">Latest price</th>
              <th className="px-3 py-2 font-medium">Held by</th>
            </tr>
          </thead>
          <tbody>
            {data.companies.map((c) => (
              <tr key={c.company_id} className="border-b border-gray-800/20 last:border-0 hover:bg-white/[0.02]">
                <td className="px-3 py-2 align-top">
                  <div className="text-gray-200 font-medium truncate max-w-[20rem]" title={c.company_name ?? undefined}>
                    {c.company_name ?? '—'}
                  </div>
                  <div className="text-[10px] text-gray-500 font-mono mt-0.5">
                    {c.exchange ? `${c.exchange}:${c.ticker}` : c.ticker}
                  </div>
                </td>
                <td className="px-3 py-2 align-top text-gray-400 text-[11px]">
                  {c.sector ?? '—'}
                </td>
                <td className="px-3 py-2 align-top">
                  <LatestPriceCell
                    latestDate={c.latest_close_price_date}
                    freshestAcrossSet={latestCloseDate}
                  />
                </td>
                <td className="px-3 py-2 align-top">
                  <div className="flex flex-wrap gap-1">
                    {c.held_by.map((hb) => (
                      <HeldByChip key={`${c.company_id}-${hb.strategy_id}`} entry={hb} />
                    ))}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/** One strategy chip with hover popover showing snapshot details. */
function HeldByChip({ entry }: { entry: HeldByEntry }) {
  const weightPct = (entry.target_weight * 100).toFixed(2);
  const tooltipLines = [
    `Opened: ${entry.as_of_date ?? '—'}`,
    `Last priced: ${entry.latest_price_date ?? '—'}`,
    `Weight: ${weightPct}%`,
    entry.entry_price_local != null ? `Entry: ${entry.entry_price_local}` : null,
    entry.score != null ? `Score: ${entry.score.toFixed(2)}` : null,
    `Snapshot #${entry.snapshot_id} · ${entry.snapshot_kind}`,
  ].filter(Boolean).join('\n');

  return (
    <span
      title={tooltipLines}
      className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded border bg-indigo-500/10 border-indigo-500/25 text-indigo-200 text-[10px] font-medium cursor-help"
    >
      <span className="truncate max-w-[10rem]">{entry.strategy_name}</span>
      <span className="text-indigo-400/70 font-mono">
        {entry.as_of_date ? `· ${entry.as_of_date}` : ''}
      </span>
    </span>
  );
}

/** One row in the daily-run history list. Collapsed shows the
 * one-line summary; expanded shows per-strategy outcome blobs with
 * collapsible error tracebacks. */
function DailyRunRow({
  run,
  expanded,
  onToggle,
}: {
  run: IngestRun;
  expanded: boolean;
  onToggle: () => void;
}) {
  const mom = run.momentum_summary || [];
  const okStrat = mom.filter((m) => m.status === 'ok').length;
  const detail = [
    run.companies_total
      ? `${run.companies_processed}/${run.companies_total} companies · ${run.prices_refreshed}p / ${run.volumes_refreshed}v`
      : null,
    mom.length > 0 ? `${okStrat}/${mom.length} strategies ok` : null,
    run.error_summary,
  ].filter(Boolean).join(' · ');

  return (
    <div>
      <button
        type="button"
        onClick={onToggle}
        className="w-full px-5 py-2 text-xs flex items-center gap-3 hover:bg-white/[0.02] text-left"
      >
        <span className="text-gray-500 font-mono text-[10px] w-3 shrink-0">{expanded ? '▾' : '▸'}</span>
        <span className={`inline-flex items-center text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border shrink-0 ${statusBadgeCls(run.status)}`}>
          {run.status}
        </span>
        <span className="text-gray-300 font-mono shrink-0">{fmtTimestamp(run.finished_at ?? run.started_at)}</span>
        <span className="text-gray-500 text-[10px] uppercase tracking-wider shrink-0">{run.triggered_by}</span>
        <span className="text-gray-400 truncate flex-1 min-w-0" title={detail}>{detail}</span>
      </button>
      {expanded && (
        <div className="px-8 py-3 bg-[#0f1117]/40 space-y-3">
          {/* Top-line counters */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-[11px]">
            <CounterCell label="Processed" value={`${run.companies_processed}${run.companies_total ? ` / ${run.companies_total}` : ''}`} />
            <CounterCell label="Prices refreshed" value={String(run.prices_refreshed)} />
            <CounterCell label="Volumes refreshed" value={String(run.volumes_refreshed)} />
            <CounterCell label="Errors" value={String(run.error_count)} tone={run.error_count > 0 ? 'rose' : undefined} />
          </div>

          {/* Top-level error_summary (first ~5 errors, captured by the orchestrator) */}
          {run.error_summary && (
            <div className="bg-rose-500/5 border border-rose-500/20 rounded-lg p-3 text-[11px] text-rose-200">
              <div className="text-rose-300 font-medium mb-1">Pipeline-level error summary</div>
              <pre className="whitespace-pre-wrap font-mono text-rose-200/90">{run.error_summary}</pre>
            </div>
          )}

          {/* Per-strategy results */}
          {mom.length === 0 ? (
            <div className="text-[11px] text-gray-500">No per-strategy results recorded — the prices phase may not have reached the momentum phase.</div>
          ) : (
            <div className="space-y-2">
              <div className="text-[10px] uppercase tracking-wider text-gray-500">Per-strategy outcomes ({mom.length})</div>
              {mom.map((m, i) => (
                <PerStrategyResult key={`${run.run_id}-${m.strategy_id ?? i}`} entry={m} />
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function CounterCell({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone?: 'rose' | 'amber' | 'emerald';
}) {
  const valueCls = tone === 'rose'
    ? 'text-rose-300'
    : tone === 'amber'
    ? 'text-amber-300'
    : tone === 'emerald'
    ? 'text-emerald-300'
    : 'text-gray-200';
  return (
    <div className="bg-[#151821] border border-gray-800/40 rounded-lg px-3 py-2">
      <div className="text-[10px] uppercase tracking-wider text-gray-500">{label}</div>
      <div className={`font-mono text-sm mt-0.5 ${valueCls}`}>{value}</div>
    </div>
  );
}

/** One per-strategy row inside an expanded run. Shows status, holdings
 * count, latest_price_date, and on failure a collapsible traceback. */
function PerStrategyResult({
  entry,
}: {
  entry: NonNullable<IngestRun['momentum_summary']>[number];
}) {
  const [showTraceback, setShowTraceback] = useState(false);
  const isError = entry.status === 'error';
  return (
    <div className={`rounded-lg border px-3 py-2 ${isError ? 'bg-rose-500/5 border-rose-500/20' : 'bg-[#151821] border-gray-800/40'}`}>
      <div className="flex items-center gap-3 text-[11px]">
        <span className={`inline-flex items-center text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border shrink-0 ${
          isError
            ? 'bg-rose-500/15 text-rose-300 border-rose-500/30'
            : 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30'
        }`}>
          {entry.status}
        </span>
        <span className="text-gray-200 font-medium truncate flex-1 min-w-0">{entry.strategy_name}</span>
        {entry.frequency && (
          <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-gray-500/10 text-gray-400 border-gray-500/30 shrink-0">
            {entry.frequency}
          </span>
        )}
        <span className="text-gray-500 text-[10px] uppercase tracking-wider shrink-0">{entry.kind ?? '—'}</span>
      </div>
      <div className="text-[11px] text-gray-400 mt-1 flex flex-wrap items-baseline gap-x-4 gap-y-1">
        <span>
          <span className="text-gray-500">Holdings:</span>{' '}
          <span className="font-mono text-gray-300">{entry.holdings_count}</span>
        </span>
        <span>
          <span className="text-gray-500">Latest price:</span>{' '}
          <span className="font-mono text-gray-300">{entry.latest_price_date ?? '—'}</span>
        </span>
        {entry.snapshot_id != null && (
          <span>
            <span className="text-gray-500">Snapshot:</span>{' '}
            <span className="font-mono text-gray-300">#{entry.snapshot_id}</span>
          </span>
        )}
      </div>
      {entry.error_message && (
        <div className="mt-2 text-[11px]">
          <div className="text-rose-300 font-medium">Error</div>
          <div className="text-rose-200 font-mono whitespace-pre-wrap break-words">{entry.error_message}</div>
          {entry.error_traceback && (
            <div className="mt-1">
              <button
                type="button"
                onClick={() => setShowTraceback((v) => !v)}
                className="text-[10px] uppercase tracking-wider text-rose-300 hover:text-rose-100"
              >
                {showTraceback ? '▾ hide' : '▸ show'} Python traceback
              </button>
              {showTraceback && (
                <pre className="mt-1 text-[10px] font-mono text-rose-200/80 bg-[#0b0d13] border border-rose-500/20 rounded p-2 max-h-64 overflow-auto whitespace-pre">
                  {entry.error_traceback}
                </pre>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/** Live progress panel shown at the top of the expanded card while a
 * run is in-flight. Surfaces the phase, current message, and counter
 * progress so the user can SEE the run advancing instead of staring
 * at a static summary line. Re-renders every 5s via the parent's
 * polling effect. */
function LiveProgressPanel({ run }: { run: IngestRun }) {
  const phase = run.current_phase ?? 'starting';
  const phaseLabel = ({
    starting: 'starting',
    acquisition: 'acquiring sources',
    templates: 'refreshing templates',
    prune: 'pruning orphans',
    prices: 'refreshing prices',
    momentum: 'persisting MTD',
    done: 'finishing',
  } as Record<string, string>)[phase] ?? phase;

  const denom = run.companies_total ?? 0;
  const num = run.companies_processed ?? 0;
  const pctCompanies = denom > 0 ? Math.min(100, Math.round((num / denom) * 100)) : 0;

  // Elapsed seconds since the run started — gives an "is this stuck?"
  // signal even when current_message hasn't changed in a while.
  // `now` is tracked in state and updated every second by an effect, so
  // the elapsed counter stays current between parent polls without
  // breaking React 19's render-purity rule (calling Date.now() at
  // render time does — same pattern SweepStatus uses).
  const startedAt = useMemo(() => {
    try { return Date.parse(run.started_at); } catch { return null; }
  }, [run.started_at]);
  const [now, setNow] = useState<number>(() => startedAt ?? 0);
  useEffect(() => {
    if (startedAt == null) return;
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setNow(Date.now());
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [startedAt]);
  const elapsedSec = startedAt != null
    ? Math.max(0, Math.round((now - startedAt) / 1000))
    : null;

  return (
    <div className="px-5 py-3 bg-amber-500/5">
      <div className="flex items-center gap-2 mb-2">
        <Spinner size={12} className="h-3 w-3 text-amber-400" />
        <span className="text-xs font-medium text-amber-300">
          Run #{run.run_id} in progress · phase: {phaseLabel}
        </span>
        {elapsedSec != null && (
          <span className="text-[10px] text-gray-500 ml-auto">
            elapsed {elapsedSec < 60 ? `${elapsedSec}s` : `${Math.floor(elapsedSec / 60)}m ${elapsedSec % 60}s`}
          </span>
        )}
      </div>
      {run.current_message && (
        <div className="text-[11px] text-gray-300 font-mono mb-2 break-words">
          {run.current_message}
        </div>
      )}
      {denom > 0 && (
        <>
          <div className="h-1.5 bg-gray-800/60 rounded-full overflow-hidden">
            <div
              className="h-full bg-amber-400/70 transition-all duration-500"
              style={{ width: `${pctCompanies}%` }}
            />
          </div>
          <div className="flex items-center gap-4 text-[10px] text-gray-500 mt-1.5">
            <span><span className="font-mono text-gray-300">{num}/{denom}</span> companies</span>
            <span><span className="font-mono text-gray-300">{run.prices_refreshed}</span> prices refreshed</span>
            <span><span className="font-mono text-gray-300">{run.volumes_refreshed}</span> volumes refreshed</span>
            {run.error_count > 0 && (
              <span className="text-rose-400">
                <span className="font-mono">{run.error_count}</span> errors
              </span>
            )}
          </div>
        </>
      )}
    </div>
  );
}

/** Top-of-expanded panel summarizing what close-price data we have
 * across the held set, so the user can tell at a glance whether the
 * daily refresh job is filling in the gap to today. */
function DataFreshnessPanel({
  summary,
  companyCount,
  loading,
}: {
  summary: FreshnessSummary | null;
  companyCount: number | null;
  loading: boolean;
}) {
  if (loading) {
    return (
      <div className="px-5 py-3 text-xs text-gray-500 inline-flex items-center gap-2">
        <Spinner size={10} className="h-2.5 w-2.5 text-gray-500" />
        <span>Computing data freshness…</span>
      </div>
    );
  }
  if (!summary || summary.latest_close_date == null) {
    return (
      <div className="px-5 py-3 text-xs text-gray-500">
        <div className="font-medium uppercase tracking-wider text-gray-400 mb-1">Data freshness</div>
        No close-price data found for any held company yet.
      </div>
    );
  }

  const total = companyCount ?? (summary.fresh_count + summary.stale_count + summary.missing_count);
  // Gap to today: how many calendar days behind the freshest observation
  // is. Doesn't account for weekends/holidays but answers "is this very
  // out of date?" cleanly. Counts negative when latest > today (e.g.
  // timezone edge); clamp at 0.
  let calDaysBehind: number | null = null;
  try {
    const latestMs = Date.parse(`${summary.latest_close_date}T00:00:00Z`);
    const todayMs = Date.parse(new Date().toISOString().slice(0, 10) + 'T00:00:00Z');
    calDaysBehind = Math.max(0, Math.round((todayMs - latestMs) / (1000 * 60 * 60 * 24)));
  } catch { /* leave null */ }

  return (
    <div className="px-5 py-3">
      <div className="flex items-baseline justify-between gap-3 mb-2">
        <div>
          <h4 className="text-xs font-medium text-gray-300 uppercase tracking-wider">
            Data freshness
          </h4>
          <p className="text-[11px] text-gray-500 mt-0.5">
            Latest close-price observation across held companies. Daily refresh extends this by one trading day.
          </p>
        </div>
        {calDaysBehind != null && (
          <div className={`text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border shrink-0 ${
            calDaysBehind <= 1
              ? 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30'
              : calDaysBehind <= 4
              ? 'bg-amber-500/15 text-amber-300 border-amber-500/30'
              : 'bg-rose-500/15 text-rose-300 border-rose-500/30'
          }`}>
            {calDaysBehind === 0 ? "today's date" : `${calDaysBehind} day${calDaysBehind === 1 ? '' : 's'} behind`}
          </div>
        )}
      </div>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-[11px]">
        <CounterCell label="Latest close" value={summary.latest_close_date} />
        <CounterCell
          label="At latest date"
          value={`${summary.fresh_count} / ${total}`}
          tone={summary.fresh_count === total ? 'emerald' : undefined}
        />
        <CounterCell
          label="Stale"
          value={String(summary.stale_count)}
          tone={summary.stale_count > 0 ? 'amber' : undefined}
        />
        <CounterCell
          label="Missing"
          value={String(summary.missing_count)}
          tone={summary.missing_count > 0 ? 'rose' : undefined}
        />
      </div>
    </div>
  );
}

/** Per-row "Latest price" cell. Renders the date with a colored dot
 * indicating freshness relative to the held set's max date. */
function LatestPriceCell({
  latestDate,
  freshestAcrossSet,
}: {
  latestDate: string | null;
  freshestAcrossSet: string | null;
}) {
  if (!latestDate) {
    return (
      <span className="inline-flex items-center gap-1.5">
        <span className="w-1.5 h-1.5 rounded-full bg-rose-400" />
        <span className="text-rose-300 text-[10px] font-mono">no data</span>
      </span>
    );
  }
  const isFresh = freshestAcrossSet != null && latestDate === freshestAcrossSet;
  return (
    <span className="inline-flex items-center gap-1.5" title={isFresh ? 'At latest date' : `Stale vs latest (${freshestAcrossSet ?? '—'})`}>
      <span className={`w-1.5 h-1.5 rounded-full ${isFresh ? 'bg-emerald-400' : 'bg-amber-400'}`} />
      <span className={`text-[10px] font-mono ${isFresh ? 'text-emerald-300' : 'text-amber-300'}`}>
        {latestDate}
      </span>
    </span>
  );
}
