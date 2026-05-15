'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import ScheduleRunDetail from './ScheduleRunDetail';
import { type StepDef, type StepState } from './ProgressTimeline';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

export type IngestRun = {
  run_id: number;
  job_name: string;
  triggered_by: 'auto' | 'manual';
  started_at: string;
  finished_at: string | null;
  status: 'running' | 'ok' | 'error';
  // Phase tracking added with the pipeline expansion. Older pre-pipeline
  // rows (only price/volume) won't have these populated.
  current_phase: 'acwi' | 'prices' | 'momentum' | 'done' | null;
  // Per-phase results — each phase writes its own column(s) as it lands.
  acwi_universe_id: number | null;
  acwi_target_month: string | null;
  acwi_summary: AcwiSummary | null;
  momentum_snapshot_id: number | null;
  momentum_summary: MomentumSummary | null;
  // Price/volume counters from the prices phase.
  companies_processed: number;
  /** Total companies the prices phase plans to walk — written once at
   * phase start so the UI can render "X of Y processed" before the
   * first checkpoint. Null on older rows + non-prices phases. */
  companies_total: number | null;
  prices_refreshed: number;
  volumes_refreshed: number;
  forbidden_count: number;
  delisted_count: number;
  error_count: number;
  error_summary: string | null;
  /** Live free-text status the active phase is emitting. ACWI passes
   * its on_progress messages here; prices renders "X of Y processed";
   * momentum routes the inner backtest stream's progress events
   * through so the user sees "Computing signals for 2025-04…" etc. */
  current_message: string | null;
};

export type AcwiSummary = {
  this_month: string;
  prev_month: string;
  additions_count: number;
  removals_count: number;
  renames_count: number;
  additions: Array<{ company_id: number; ticker: string; name: string | null; sector: string | null }>;
  removals: Array<{ company_id: number; ticker: string; name: string | null; sector: string | null }>;
  renames: Array<{ company_id: number; old_ticker: string; new_ticker: string; name: string | null }>;
};

export type MomentumSummary = {
  holdings_count: number;
  latest_price_date: string | null;
  strategy_run_id: number | null;
  strategy_name: string | null;
};

type ScheduleConfig = {
  selected_run_id: number | null;
  selected_run_name: string | null;
  /** Full config blob of the currently-selected backtest. Shaped like a
   * BacktestRequest but with arbitrary keys — we cast to a permissive
   * record and read the fields we display below. Null when no run is
   * selected. */
  selected_run_config: Record<string, unknown> | null;
  updated_at: string | null;
  available_runs: Array<{ run_id: number; name: string; created_at: string }>;
};

type JobSpec = {
  key: string;
  label: string;
  cron: string;
  cronExplain: string;
  description: string;
};

const JOBS: JobSpec[] = [
  {
    key: 'weekly_price_volume',
    label: 'Weekly pipeline',
    cron: 'Tue 02:00 UTC',
    cronExplain: 'APScheduler in-process',
    description:
      "Three phases: ACWI universe refresh → price + volume refresh → momentum compute. Captures the previous Monday's worldwide closes.",
  },
  {
    key: 'monthly_price_volume',
    label: 'Monthly pipeline',
    cron: '2nd of month 02:00 UTC',
    cronExplain: 'APScheduler in-process',
    description:
      "Same three phases. Captures the first trading day of the month. Weekends/holidays still fire but freshness checks no-op; the next weekly tick catches up.",
  },
];

function fmtDuration(startIso: string, endIso: string | null): string {
  if (!endIso) return '—';
  const secs = Math.max(0, Math.round((Date.parse(endIso) - Date.parse(startIso)) / 1000));
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

function fmtTimestamp(iso: string | null): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString(undefined, {
      year: 'numeric', month: 'short', day: '2-digit',
      hour: '2-digit', minute: '2-digit',
    });
  } catch {
    return iso;
  }
}

function StatusBadge({ status }: { status: IngestRun['status'] }) {
  const cls = status === 'ok'
    ? 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30'
    : status === 'error'
      ? 'bg-rose-500/10 text-rose-300 border-rose-500/30'
      : 'bg-amber-500/15 text-amber-300 border-amber-500/30';
  return (
    <span className={`inline-flex items-center text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border ${cls}`}>
      {status}
    </span>
  );
}

/** The three pipeline phases the /schedule UI knows about. Same labels
 * the backend writes to `ingest_run.current_phase`. Exported so the
 * detail expander can render the same step list with messages. */
export const PIPELINE_STEPS: StepDef[] = [
  { key: 'acwi', label: 'ACWI universe refresh' },
  { key: 'prices', label: 'Price + volume refresh' },
  { key: 'momentum', label: 'Momentum compute' },
];

/** Translate an `ingest_run` row into the shape `ProgressTimeline` wants.
 * Each phase's status comes from the row's own per-phase columns: data
 * present → `done`, `current_phase` matches → `in_progress`, pipeline
 * finished without that phase's data → either `done` (with a "skipped"
 * message, e.g. momentum when no strategy is selected) or pending.
 * The percentage is derived linearly from those statuses so a running
 * pipeline animates through 33% / 50% / 66% / 100% as phases land. */
export function runToTimelineProps(run: IngestRun): {
  state: Record<string, StepState>;
  pct: number;
  running: boolean;
  doneSummary: string | null;
  errorMessage: string | null;
  totalElapsedMs: number | null;
} {
  const finished = run.status === 'ok' || run.status === 'error';
  const phase = run.current_phase;
  const state: Record<string, StepState> = {
    acwi: { status: 'pending' },
    prices: { status: 'pending' },
    momentum: { status: 'pending' },
  };

  // When the active phase has streamed a `current_message`, prefer it
  // over the count-based fallback — gives the user the same level of
  // detail /momentum's ProgressTimeline shows ("Loading universe…",
  // "Computing signals for 2025-04…", etc.).
  const liveMessage = run.current_message ?? undefined;

  // Phase 1 — ACWI
  if (run.acwi_summary || run.acwi_target_month) {
    const s = run.acwi_summary;
    state.acwi = {
      status: 'done',
      message: s
        ? `+${s.additions_count} / −${s.removals_count}${s.renames_count > 0 ? ` / ${s.renames_count} renames` : ''}`
        : undefined,
    };
  } else if (phase === 'acwi') {
    state.acwi = {
      status: 'in_progress',
      message: liveMessage ?? 'reconstructing ACWI universe…',
    };
  } else if (finished) {
    // Pipeline ended without ACWI landing → must have errored. The
    // accumulated error_summary at the bottom has the detail.
    state.acwi = { status: 'error', message: 'failed' };
  }

  // Phase 2 — Prices
  if (run.companies_processed > 0 && (phase === 'momentum' || phase === 'done' || finished)) {
    const denominator = run.companies_total ? ` of ${run.companies_total}` : '';
    state.prices = {
      status: 'done',
      message: `${run.companies_processed}${denominator} processed · ${run.prices_refreshed}p / ${run.volumes_refreshed}v · ${run.forbidden_count} forbidden`,
    };
  } else if (phase === 'prices') {
    // Backend writes a per-checkpoint current_message that already
    // includes the X of Y / counter summary; surface it verbatim. If
    // it hasn't arrived yet (the very first ms of the phase), build a
    // best-effort message from whatever counters we have.
    let msg = liveMessage;
    if (!msg) {
      const denom = run.companies_total ? ` of ${run.companies_total}` : '';
      msg = run.companies_processed > 0
        ? `${run.companies_processed}${denom} processed…`
        : `starting${denom}…`;
    }
    state.prices = { status: 'in_progress', message: msg };
  } else if (finished) {
    state.prices = { status: 'error', message: 'failed' };
  }

  // Phase 3 — Momentum (skippable when no strategy is selected)
  if (run.momentum_snapshot_id != null) {
    const m = run.momentum_summary;
    state.momentum = {
      status: 'done',
      message: m
        ? `${m.holdings_count} holdings · ${m.strategy_name ?? 'unnamed strategy'}`
        : 'snapshot saved',
    };
  } else if (phase === 'momentum') {
    state.momentum = {
      status: 'in_progress',
      message: liveMessage ?? 'computing holdings…',
    };
  } else if (finished) {
    // No snapshot but pipeline finished — most often "no strategy
    // selected, skip this phase". Mark as done with a clarifying note.
    state.momentum = { status: 'done', message: 'skipped (no scheduled strategy)' };
  }

  let score = 0;
  for (const s of Object.values(state)) {
    if (s.status === 'done') score += 1;
    else if (s.status === 'in_progress') score += 0.5;
    else if (s.status === 'error') score += 1; // counts toward "completed" even if errored
  }
  const pct = Math.round((score / PIPELINE_STEPS.length) * 100);

  let elapsedMs: number | null = null;
  try {
    const startMs = Date.parse(run.started_at);
    const endMs = run.finished_at ? Date.parse(run.finished_at) : Date.now();
    elapsedMs = Math.max(0, endMs - startMs);
  } catch {
    elapsedMs = null;
  }

  return {
    state,
    pct,
    running: run.status === 'running',
    doneSummary:
      run.status === 'ok' && phase === 'done'
        ? 'Pipeline complete'
        : null,
    errorMessage:
      run.status === 'error' && run.error_summary
        ? run.error_summary
        : null,
    totalElapsedMs: elapsedMs,
  };
}

/** Three-dot phase indicator. Each dot reflects whether its phase has
 * landed (data written), is currently active, or hasn't run yet. */
function PhasePips({ run }: { run: IngestRun }) {
  const phases = [
    { key: 'acwi' as const,     hasData: run.acwi_target_month != null || run.acwi_summary != null },
    { key: 'prices' as const,   hasData: run.companies_processed > 0 || run.prices_refreshed > 0 || run.volumes_refreshed > 0 },
    { key: 'momentum' as const, hasData: run.momentum_snapshot_id != null },
  ];
  const currentPhase = run.current_phase;
  return (
    <span className="inline-flex items-center gap-1" title={`Phases: ACWI · prices · momentum (current: ${currentPhase ?? '—'})`}>
      {phases.map((p) => {
        const isCurrent = run.status === 'running' && currentPhase === p.key;
        let cls = 'bg-gray-700';
        if (p.hasData) cls = 'bg-emerald-500';
        if (isCurrent) cls = 'bg-amber-400 animate-pulse';
        return (
          <span key={p.key} className={`inline-block w-2 h-2 rounded-full ${cls}`} />
        );
      })}
    </span>
  );
}

export default function Schedule() {
  const [runs, setRuns] = useState<IngestRun[]>([]);
  const [config, setConfig] = useState<ScheduleConfig | null>(null);
  const [savingConfig, setSavingConfig] = useState(false);
  const [loading, setLoading] = useState(true);
  const [triggering, setTriggering] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expandedRunId, setExpandedRunId] = useState<number | null>(null);

  const loadRuns = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/api/ingest/runs?limit=50`);
      if (!r.ok) {
        setError(`Failed to load runs (${r.status})`);
        return;
      }
      const data = (await r.json()) as IngestRun[];
      setRuns(Array.isArray(data) ? data : []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  const loadConfig = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/api/schedule-config`);
      if (!r.ok) return;
      const data = (await r.json()) as ScheduleConfig;
      setConfig(data);
    } catch {
      // Silent — config card just won't render the dropdown
    }
  }, []);

  useEffect(() => {
    void loadRuns();
    void loadConfig();
  }, [loadRuns, loadConfig]);

  // Poll while any run is in flight so the user sees per-phase progress.
  useEffect(() => {
    const anyRunning = runs.some((r) => r.status === 'running');
    if (!anyRunning && !triggering) return;
    const interval = setInterval(() => {
      void loadRuns();
    }, 3000);
    return () => clearInterval(interval);
  }, [runs, triggering, loadRuns]);

  const lastByJob = useMemo<Record<string, IngestRun | undefined>>(() => {
    const out: Record<string, IngestRun | undefined> = {};
    for (const job of JOBS) {
      out[job.key] = runs.find((r) => r.job_name === job.key);
    }
    out.manual = runs.find((r) => r.job_name === 'manual');
    return out;
  }, [runs]);

  const triggerJob = async (jobKey: string) => {
    setTriggering(jobKey);
    try {
      const r = await fetch(
        `${API_URL}/api/ingest/scheduled-refresh/trigger?job_name=${encodeURIComponent(jobKey)}`,
        { method: 'POST' },
      );
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Trigger failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      await loadRuns();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setTriggering(null);
    }
  };

  const setSelectedStrategy = async (runId: number | null) => {
    setSavingConfig(true);
    try {
      const r = await fetch(`${API_URL}/api/schedule-config`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ selected_run_id: runId }),
      });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Save failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      await loadConfig();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSavingConfig(false);
    }
  };

  const selectedRunName = useMemo(() => {
    if (!config?.selected_run_id || !config.available_runs) return null;
    return config.available_runs.find((r) => r.run_id === config.selected_run_id)?.name ?? null;
  }, [config]);

  return (
    <div className="min-h-screen bg-[#0f1117] text-gray-200">
      <div className="px-8 py-5 border-b border-gray-800/40">
        <h1 className="text-xl font-semibold text-white">Schedule</h1>
        <p className="text-sm text-gray-500 mt-1">
          Pipeline: ACWI refresh → price/volume refresh → momentum compute. Backtests stay read-only against the DB; this is the canonical refresher.
        </p>
      </div>

      <div className="px-8 py-6 space-y-6 max-w-6xl">
        {error && (
          <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-sm text-rose-300">
            {error}
          </div>
        )}

        {/* Scheduled strategy picker — drives the momentum phase. */}
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-5 py-4">
          <div className="min-w-0">
            <h3 className="text-sm font-medium text-white">Which strategy should the pipeline compute today&apos;s holdings for?</h3>
            <p className="text-xs text-gray-500 mt-1 leading-relaxed">
              Every time the pipeline runs (scheduled or manual), the final phase calculates &quot;what would this strategy be holding right now?&quot; against the freshly-refreshed data. Pick one of your saved backtests below — its signal weights, sectors, and top-N config will be used. The result is saved as a new current-picks snapshot, viewable in the run row below.
              {' '}
              <span className="text-gray-600">Pick &quot;(none)&quot; if you only want the pipeline to refresh data and skip the holdings computation.</span>
            </p>
          </div>
          <div className="mt-3 flex items-center gap-3 flex-wrap">
            <select
              value={config?.selected_run_id ?? ''}
              disabled={savingConfig || !config}
              onChange={(e) => {
                const v = e.target.value;
                void setSelectedStrategy(v === '' ? null : Number(v));
              }}
              className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-1.5 text-xs text-gray-200 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 focus:outline-none min-w-[320px]"
            >
              <option value="">(none — only refresh data, skip holdings compute)</option>
              {(config?.available_runs ?? []).map((r) => (
                <option key={r.run_id} value={r.run_id}>
                  {r.name} (#{r.run_id})
                </option>
              ))}
            </select>
            {savingConfig && <span className="text-xs text-gray-500">Saving…</span>}
            {!savingConfig && selectedRunName && (
              <span className="text-xs text-emerald-400">
                ✓ Pipeline will compute holdings for <span className="text-gray-200 font-medium">{selectedRunName}</span>
              </span>
            )}
            {!savingConfig && config && config.selected_run_id == null && (config.available_runs?.length ?? 0) > 0 && (
              <span className="text-xs text-amber-400">
                Pick one of your {config.available_runs.length} saved backtests above to enable holdings computation.
              </span>
            )}
            {!savingConfig && config && (config.available_runs?.length ?? 0) === 0 && (
              <span className="text-xs text-amber-400">
                No saved backtests yet — go to <a href="/momentum" className="text-indigo-400 hover:underline">/momentum</a>, run a backtest you like, and it&apos;ll appear here.
              </span>
            )}
          </div>

          {/* Once a strategy is selected, render its full config so the
              user can verify weights / sectors / top-N match what they
              expect before letting it auto-run on a schedule. */}
          {!savingConfig && config?.selected_run_config && (
            <StrategyConfigDetail cfg={config.selected_run_config} />
          )}
        </div>

        {/* Scheduled job cards */}
        <div className="grid gap-4 md:grid-cols-2">
          {JOBS.map((job) => {
            const last = lastByJob[job.key];
            const isTriggering = triggering === job.key;
            const isRunning = last?.status === 'running';
            return (
              <div key={job.key} className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <h3 className="text-sm font-medium text-white">{job.label}</h3>
                    <div className="text-xs text-gray-500 mt-0.5 font-mono">
                      {job.cron} <span className="text-gray-600">·</span> {job.cronExplain}
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={() => triggerJob(job.key)}
                    disabled={isTriggering || isRunning}
                    className="text-xs px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white transition-colors shrink-0"
                  >
                    {isTriggering ? 'Starting…' : isRunning ? 'Running…' : 'Run now'}
                  </button>
                </div>
                <p className="text-xs text-gray-500 mt-2">{job.description}</p>

                <div className="mt-4 pt-3 border-t border-gray-800/40 text-xs space-y-1.5">
                  {last ? (
                    <>
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-gray-500">Last run:</span>
                        <span className="text-gray-300 font-mono">{fmtTimestamp(last.started_at)}</span>
                        <StatusBadge status={last.status} />
                        <PhasePips run={last} />
                      </div>
                      {last.acwi_summary && (
                        <div className="text-gray-400 font-mono">
                          ACWI: +{last.acwi_summary.additions_count} / −{last.acwi_summary.removals_count}
                          {last.acwi_summary.renames_count > 0 && <span> / {last.acwi_summary.renames_count} renames</span>}
                        </div>
                      )}
                      <div className="text-gray-400 font-mono">
                        Prices: {last.companies_processed} processed · {last.prices_refreshed}p / {last.volumes_refreshed}v
                      </div>
                      {last.momentum_summary && (
                        <div className="text-gray-400 font-mono">
                          Momentum: {last.momentum_summary.holdings_count} holdings · latest {last.momentum_summary.latest_price_date ?? '—'}
                        </div>
                      )}
                      <div className="text-gray-500 font-mono">
                        {fmtDuration(last.started_at, last.finished_at)}
                      </div>
                    </>
                  ) : (
                    <div className="text-gray-500">No runs yet.</div>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        {/* Recent runs list */}
        <div className="bg-[#151821] rounded-xl border border-gray-800/40">
          <div className="px-5 py-3 border-b border-gray-800/40 flex items-center justify-between">
            <h3 className="text-sm font-medium text-white">Recent runs</h3>
            <button
              type="button"
              onClick={() => void loadRuns()}
              className="text-xs text-gray-500 hover:text-gray-300 transition-colors"
            >
              {loading ? 'Loading…' : 'Refresh'}
            </button>
          </div>
          {runs.length === 0 && !loading ? (
            <div className="px-5 py-6 text-sm text-gray-500">No pipeline runs in history yet.</div>
          ) : (
            <div className="divide-y divide-gray-800/30">
              {runs.map((r) => {
                const isExpanded = expandedRunId === r.run_id;
                const tp = runToTimelineProps(r);
                return (
                  <div key={r.run_id}>
                    <button
                      type="button"
                      onClick={() => setExpandedRunId(isExpanded ? null : r.run_id)}
                      className="w-full px-5 py-2.5 flex flex-col gap-2 text-left hover:bg-white/[0.02] transition-colors"
                    >
                      <div className="flex items-center gap-4 w-full">
                        <span className="text-gray-500 font-mono text-xs w-4 shrink-0">{isExpanded ? '▾' : '▸'}</span>
                        <span className="text-gray-300 font-mono text-sm w-44 shrink-0">{fmtTimestamp(r.started_at)}</span>
                        <PhasePips run={r} />
                        <StatusBadge status={r.status} />
                        <span className="text-gray-400 text-xs">{r.job_name}</span>
                        <span className="text-gray-500 text-xs">{r.triggered_by}</span>
                        <span className="text-gray-400 text-xs font-mono ml-auto flex items-center gap-4 flex-wrap justify-end">
                          {r.acwi_summary && (
                            <span>ACWI +{r.acwi_summary.additions_count}/−{r.acwi_summary.removals_count}</span>
                          )}
                          <span>{r.companies_processed} co</span>
                          {r.momentum_summary && (
                            <span>{r.momentum_summary.holdings_count} hldg</span>
                          )}
                          <span>{fmtDuration(r.started_at, r.finished_at)}</span>
                        </span>
                      </div>
                      {/* Inline progress bar for running rows — same
                          look as /momentum's ProgressTimeline. Hidden
                          for done/error rows since the status badge +
                          phase pips already convey final state. */}
                      {r.status === 'running' && (
                        <div className="pl-9 pr-1 w-full">
                          <div className="flex items-center justify-between gap-3 text-[10px] text-gray-500 mb-0.5">
                            {/* Prefer the live current_message so the
                                collapsed row reads as "Refreshing 320
                                of 1837 companies · 250p / 250v
                                refreshed · 0 forbidden, 0 errors"
                                rather than the static phase label. */}
                            <span className="truncate">
                              {r.current_message ?? (
                                r.current_phase === 'acwi' ? 'ACWI universe refresh' :
                                r.current_phase === 'prices' ? 'Price + volume refresh' :
                                r.current_phase === 'momentum' ? 'Momentum compute' :
                                'starting…'
                              )}
                            </span>
                            <span className="font-mono shrink-0">{tp.pct}%</span>
                          </div>
                          <div className="h-1 bg-gray-800 rounded-full overflow-hidden">
                            <div
                              className="h-full bg-indigo-500 transition-all duration-300"
                              style={{ width: `${tp.pct}%` }}
                            />
                          </div>
                        </div>
                      )}
                    </button>
                    {isExpanded && <ScheduleRunDetail run={r} />}
                  </div>
                );
              })}
            </div>
          )}

          {(() => {
            const lastErrored = runs.find((r) => r.status === 'error' && r.error_summary);
            if (!lastErrored) return null;
            return (
              <div className="px-5 py-3 border-t border-gray-800/40 text-xs">
                <div className="text-gray-500 mb-1">
                  Most recent failed run (#{lastErrored.run_id}, {lastErrored.job_name}):
                </div>
                <pre className="bg-[#0f1117] border border-gray-800/60 rounded-lg px-3 py-2 text-rose-300 whitespace-pre-wrap text-[11px] font-mono overflow-x-auto">
                  {lastErrored.error_summary}
                </pre>
              </div>
            );
          })()}
        </div>
      </div>
    </div>
  );
}

/** Renders the selected backtest's config so the user can verify what
 * the pipeline will actually compute. We curate to the fields that
 * matter operationally (selection mode, universe, top-N, signal +
 * category weights); the underlying blob may have extra fields from
 * older runs which we just ignore. */
function StrategyConfigDetail({ cfg }: { cfg: Record<string, unknown> }) {
  const selection = (cfg.selection_mode as string | undefined) ?? 'momentum';
  const strategy = (cfg.strategy_type as string | undefined) ?? 'long_only';
  const indexUniverse = (cfg.index_universe as string | null | undefined) ?? null;
  const universeLabel = (cfg.universe_label as string | null | undefined) ?? null;
  const startDate = (cfg.start_date as string | undefined) ?? '—';
  const endDate = (cfg.end_date as string | undefined) ?? '—';
  const topNSectors = cfg.top_n_sectors as number | undefined;
  const topNPerSector = cfg.top_n_per_sector as number | undefined;
  const maxCompanies = cfg.max_companies as number | null | undefined;
  const minPriceScore = cfg.min_price_score as number | null | undefined;
  const rebalanceFrequency = (cfg.rebalance_frequency as string | undefined) ?? 'monthly';
  const randomSeed = cfg.random_seed as number | null | undefined;
  const nTrials = cfg.n_trials as number | undefined;
  const signalWeights = (cfg.signal_weights as Record<string, number> | undefined) ?? {};
  const categoryWeights = (cfg.category_weights as Record<string, number> | undefined) ?? {};

  const universe = indexUniverse ?? universeLabel ?? 'All companies (no universe filter)';
  // Signal-weight grouping is a nicety for the user; the actual weights
  // are flat so we just sort high-to-low for visual clarity.
  const sortedSignals = Object.entries(signalWeights).sort((a, b) => b[1] - a[1]);
  const sortedCategories = Object.entries(categoryWeights).sort((a, b) => b[1] - a[1]);

  return (
    <div className="mt-4 pt-4 border-t border-gray-800/40">
      <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">
        Selected strategy config
      </div>
      <div className="grid gap-x-6 gap-y-2 grid-cols-1 sm:grid-cols-2 md:grid-cols-3 text-xs">
        <ConfigRow label="Selection mode" value={selection} />
        <ConfigRow label="Strategy type" value={strategy} />
        <ConfigRow label="Universe" value={universe} />
        <ConfigRow label="Date range" value={`${startDate} → ${endDate}`} />
        <ConfigRow label="Rebalance" value={rebalanceFrequency} />
        <ConfigRow label="Top N sectors" value={topNSectors != null ? String(topNSectors) : '—'} />
        <ConfigRow label="Top N per sector" value={topNPerSector != null ? String(topNPerSector) : '—'} />
        <ConfigRow label="Max companies" value={maxCompanies != null ? String(maxCompanies) : 'unlimited'} />
        <ConfigRow label="Min price score" value={minPriceScore != null ? `> ${minPriceScore}` : 'off'} />
        {selection === 'random' && (
          <>
            <ConfigRow label="Random seed" value={randomSeed != null ? String(randomSeed) : '—'} />
            <ConfigRow label="Trials" value={nTrials != null ? String(nTrials) : '1'} />
          </>
        )}
      </div>

      {sortedCategories.length > 0 && (
        <div className="mt-4">
          <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Category weights</div>
          <div className="flex flex-wrap gap-3 text-xs">
            {sortedCategories.map(([cat, w]) => (
              <span key={cat} className="bg-[#0f1117] border border-gray-800/60 rounded px-2.5 py-1">
                <span className="text-gray-400 capitalize">{cat}</span>
                <span className="text-gray-500 mx-2">·</span>
                <span className="font-mono text-gray-200">{w.toFixed(2)}</span>
              </span>
            ))}
          </div>
        </div>
      )}

      {sortedSignals.length > 0 && (
        <div className="mt-3">
          <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">
            Signal weights ({sortedSignals.filter(([, w]) => w > 0).length} active)
          </div>
          <div className="grid gap-x-4 gap-y-1 grid-cols-1 sm:grid-cols-2 md:grid-cols-3 text-xs">
            {sortedSignals.map(([sig, w]) => (
              <div
                key={sig}
                className={`flex items-baseline justify-between gap-2 ${w === 0 ? 'opacity-50' : ''}`}
              >
                <span className="text-gray-400 truncate">{sig}</span>
                <span className="font-mono text-gray-200 shrink-0">{w.toFixed(2)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function ConfigRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline justify-between gap-2 border-b border-gray-800/20 pb-1">
      <span className="text-gray-500">{label}</span>
      <span className="font-mono text-gray-200 text-right truncate">{value}</span>
    </div>
  );
}
