'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import ScheduleRunDetail from './ScheduleRunDetail';
import ScheduledStrategyDetail from './ScheduledStrategyDetail';
import { type StepDef, type StepState } from './ProgressTimeline';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

export type IngestRun = {
  run_id: number;
  job_name: string;
  triggered_by: 'auto' | 'manual';
  started_at: string;
  finished_at: string | null;
  status: 'running' | 'ok' | 'error';
  current_phase: 'acwi' | 'prices' | 'momentum' | 'done' | null;
  acwi_universe_id: number | null;
  acwi_target_month: string | null;
  acwi_summary: AcwiSummary | null;
  // Now an array — one entry per scheduled strategy the pipeline tried.
  momentum_summary: MomentumStrategyResult[] | null;
  companies_processed: number;
  companies_total: number | null;
  prices_refreshed: number;
  volumes_refreshed: number;
  forbidden_count: number;
  delisted_count: number;
  error_count: number;
  error_summary: string | null;
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

/** One entry per scheduled strategy in `ingest_run.momentum_summary`. */
export type MomentumStrategyResult = {
  strategy_id: number | null;
  backtest_run_id: number;
  strategy_name: string;
  snapshot_id: number | null;
  holdings_count: number;
  latest_price_date: string | null;
  status: 'ok' | 'error';
  error_message: string | null;
};

export type ScheduledStrategy = {
  id: number;
  backtest_run_id: number;
  enabled: boolean;
  created_at: string;
  updated_at: string;
  backtest_name: string | null;
  backtest_config: Record<string, unknown> | null;
  last_snapshot: {
    snapshot_id: number;
    ingest_run_id: number | null;
    created_at: string;
    latest_price_date: string | null;
    holdings_count: number;
  } | null;
};

type AvailableBacktest = {
  run_id: number;
  name: string;
  created_at: string;
  config: Record<string, unknown>;
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
    cronExplain: 'after Monday global close',
    description:
      "Three phases: ACWI universe refresh → price + volume refresh → momentum compute for every scheduled strategy. Captures the previous Monday's worldwide closes.",
  },
  {
    key: 'monthly_price_volume',
    label: 'Monthly pipeline',
    cron: '2nd of month 02:00 UTC',
    cronExplain: 'after 1st-of-month global close',
    description:
      "Same three phases. Captures the first trading day of the month's closes. Weekends/holidays still fire but freshness checks no-op.",
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

function fmtDate(iso: string | null): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleDateString(undefined, {
      year: 'numeric', month: 'short', day: '2-digit',
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

export const PIPELINE_STEPS: StepDef[] = [
  { key: 'acwi', label: 'ACWI universe refresh' },
  { key: 'prices', label: 'Price + volume refresh' },
  { key: 'momentum', label: 'Momentum compute' },
];

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
    state.acwi = { status: 'in_progress', message: liveMessage ?? 'reconstructing ACWI universe…' };
  } else if (finished) {
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

  // Phase 3 — Momentum
  const mom = run.momentum_summary ?? [];
  const successCount = mom.filter((m) => m.status === 'ok').length;
  const errorCount = mom.filter((m) => m.status === 'error').length;
  if (mom.length > 0 && (phase === 'done' || finished)) {
    const parts = [`${successCount} ok`];
    if (errorCount > 0) parts.push(`${errorCount} failed`);
    state.momentum = {
      status: errorCount > 0 && successCount === 0 ? 'error' : 'done',
      message: `${parts.join(' · ')} of ${mom.length} strateg${mom.length === 1 ? 'y' : 'ies'}`,
    };
  } else if (phase === 'momentum') {
    state.momentum = { status: 'in_progress', message: liveMessage ?? 'computing holdings…' };
  } else if (finished) {
    // Pipeline finished without producing any momentum results — usually
    // means no scheduled strategies are enabled.
    state.momentum = { status: 'done', message: 'skipped (no scheduled strategies)' };
  }

  let score = 0;
  for (const s of Object.values(state)) {
    if (s.status === 'done') score += 1;
    else if (s.status === 'in_progress') score += 0.5;
    else if (s.status === 'error') score += 1;
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
    doneSummary: run.status === 'ok' && phase === 'done' ? 'Pipeline complete' : null,
    errorMessage: run.status === 'error' && run.error_summary ? run.error_summary : null,
    totalElapsedMs: elapsedMs,
  };
}

function PhasePips({ run }: { run: IngestRun }) {
  const phases = [
    { key: 'acwi' as const,     hasData: run.acwi_target_month != null || run.acwi_summary != null },
    { key: 'prices' as const,   hasData: run.companies_processed > 0 || run.prices_refreshed > 0 || run.volumes_refreshed > 0 },
    { key: 'momentum' as const, hasData: (run.momentum_summary?.length ?? 0) > 0 },
  ];
  const currentPhase = run.current_phase;
  return (
    <span className="inline-flex items-center gap-1" title={`Phases: ACWI · prices · momentum (current: ${currentPhase ?? '—'})`}>
      {phases.map((p) => {
        const isCurrent = run.status === 'running' && currentPhase === p.key;
        let cls = 'bg-gray-700';
        if (p.hasData) cls = 'bg-emerald-500';
        if (isCurrent) cls = 'bg-amber-400 animate-pulse';
        return <span key={p.key} className={`inline-block w-2 h-2 rounded-full ${cls}`} />;
      })}
    </span>
  );
}

/** Curated subset of the strategy config to display on the strategy list
 * row + the add picker. The full breakdown lives in the detail view. */
function strategySummary(cfg: Record<string, unknown> | null): string {
  if (!cfg) return '';
  const selection = (cfg.selection_mode as string | undefined) ?? 'momentum';
  const universe = (cfg.index_universe as string | null | undefined) ?? (cfg.universe_label as string | null | undefined) ?? 'all';
  const topSectors = cfg.top_n_sectors as number | undefined;
  const topPer = cfg.top_n_per_sector as number | undefined;
  const parts: string[] = [selection];
  if (universe) parts.push(`${universe}`);
  if (topSectors != null && topPer != null) parts.push(`top ${topSectors}×${topPer}`);
  return parts.join(' · ');
}

export default function Schedule() {
  const [runs, setRuns] = useState<IngestRun[]>([]);
  const [strategies, setStrategies] = useState<ScheduledStrategy[]>([]);
  const [strategiesLoading, setStrategiesLoading] = useState(true);
  const [loading, setLoading] = useState(true);
  const [triggering, setTriggering] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expandedRunId, setExpandedRunId] = useState<number | null>(null);
  const [expandedStrategyId, setExpandedStrategyId] = useState<number | null>(null);
  const [addingPickerOpen, setAddingPickerOpen] = useState(false);

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

  const loadStrategies = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/api/scheduled-strategies`);
      if (!r.ok) return;
      const data = (await r.json()) as ScheduledStrategy[];
      setStrategies(Array.isArray(data) ? data : []);
    } catch {
      // Silent — strategies card just shows empty state
    } finally {
      setStrategiesLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadRuns();
    void loadStrategies();
  }, [loadRuns, loadStrategies]);

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

  const toggleStrategy = useCallback(async (id: number, enabled: boolean) => {
    try {
      const r = await fetch(`${API_URL}/api/scheduled-strategies/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled }),
      });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Toggle failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      await loadStrategies();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [loadStrategies]);

  const removeStrategy = useCallback(async (id: number) => {
    if (!confirm('Remove this strategy from the schedule? Existing snapshots will be preserved.')) return;
    try {
      const r = await fetch(`${API_URL}/api/scheduled-strategies/${id}`, { method: 'DELETE' });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Delete failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      if (expandedStrategyId === id) setExpandedStrategyId(null);
      await loadStrategies();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [expandedStrategyId, loadStrategies]);

  return (
    <div className="min-h-screen bg-[#0f1117] text-gray-200">
      <div className="px-8 py-5 border-b border-gray-800/40">
        <h1 className="text-xl font-semibold text-white">Schedule</h1>
        <p className="text-sm text-gray-500 mt-1">
          The pipeline fires Tuesday 02:00 UTC (after Monday global close) and the 2nd of each month 02:00 UTC (after 1st-of-month global close). Every strategy on the schedule below gets a fresh holdings snapshot on every tick.
        </p>
      </div>

      <div className="px-8 py-6 space-y-6 max-w-6xl">
        {error && (
          <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-sm text-rose-300 flex items-center justify-between">
            <span>{error}</span>
            <button type="button" onClick={() => setError(null)} className="text-rose-200 hover:text-white text-xs">dismiss</button>
          </div>
        )}

        {/* Scheduled strategies */}
        <div className="bg-[#151821] rounded-xl border border-gray-800/40">
          <div className="px-5 py-3 border-b border-gray-800/40 flex items-center justify-between">
            <div>
              <h3 className="text-sm font-medium text-white">Scheduled strategies</h3>
              <p className="text-xs text-gray-500 mt-0.5">
                Each pipeline run computes a fresh holdings snapshot for every enabled strategy. Click a strategy to see its run history.
              </p>
            </div>
            <button
              type="button"
              onClick={() => setAddingPickerOpen((v) => !v)}
              className="text-xs px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
            >
              {addingPickerOpen ? 'Cancel' : '+ Add strategy'}
            </button>
          </div>

          {addingPickerOpen && (
            <AddStrategyPicker
              onAdded={async () => {
                setAddingPickerOpen(false);
                await loadStrategies();
              }}
              onCancel={() => setAddingPickerOpen(false)}
            />
          )}

          {strategiesLoading ? (
            <div className="px-5 py-5 text-sm text-gray-500">Loading…</div>
          ) : strategies.length === 0 ? (
            <div className="px-5 py-6 text-sm text-gray-500">
              No strategies scheduled yet. Click <span className="text-gray-300">+ Add strategy</span> to pin one of your saved backtests from <a href="/momentum" className="text-indigo-400 hover:underline">/momentum</a>.
            </div>
          ) : (
            <div className="divide-y divide-gray-800/30">
              {strategies.map((s) => {
                const isExpanded = expandedStrategyId === s.id;
                return (
                  <div key={s.id}>
                    <div className="px-5 py-3 flex items-center gap-3 hover:bg-white/[0.02] transition-colors">
                      <button
                        type="button"
                        onClick={() => setExpandedStrategyId(isExpanded ? null : s.id)}
                        className="flex items-center gap-3 flex-1 min-w-0 text-left"
                      >
                        <span className="text-gray-500 font-mono text-xs w-4 shrink-0">{isExpanded ? '▾' : '▸'}</span>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 flex-wrap">
                            <span className={`text-sm font-medium truncate ${s.enabled ? 'text-white' : 'text-gray-500'}`}>
                              {s.backtest_name ?? `Backtest #${s.backtest_run_id}`}
                            </span>
                            {!s.enabled && (
                              <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-gray-500/10 text-gray-400 border-gray-500/30">
                                paused
                              </span>
                            )}
                          </div>
                          <div className="text-xs text-gray-500 mt-0.5 font-mono">
                            {strategySummary(s.backtest_config)}
                            {s.last_snapshot && (
                              <span className="text-gray-600">
                                {' · '}last run {fmtDate(s.last_snapshot.created_at)} · {s.last_snapshot.holdings_count} holdings
                              </span>
                            )}
                          </div>
                        </div>
                      </button>
                      <label className="flex items-center gap-1.5 text-xs text-gray-400 cursor-pointer">
                        <input
                          type="checkbox"
                          checked={s.enabled}
                          onChange={(e) => void toggleStrategy(s.id, e.target.checked)}
                          className="accent-indigo-500"
                        />
                        enabled
                      </label>
                      <button
                        type="button"
                        onClick={() => void removeStrategy(s.id)}
                        className="text-xs px-2 py-1 rounded-lg text-rose-300 hover:bg-rose-500/10 transition-colors"
                        title="Remove from schedule"
                      >
                        ×
                      </button>
                    </div>
                    {isExpanded && (
                      <ScheduledStrategyDetail
                        strategyId={s.id}
                        onMutated={() => void loadStrategies()}
                      />
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Pipeline job cards */}
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
                      {last.momentum_summary && last.momentum_summary.length > 0 && (
                        <div className="text-gray-400 font-mono">
                          Momentum: {last.momentum_summary.filter((m) => m.status === 'ok').length} of {last.momentum_summary.length} strateg{last.momentum_summary.length === 1 ? 'y' : 'ies'} ok
                        </div>
                      )}
                      <div className="text-gray-500 font-mono">{fmtDuration(last.started_at, last.finished_at)}</div>
                    </>
                  ) : (
                    <div className="text-gray-500">No runs yet.</div>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        {/* Recent runs */}
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
                const momCount = r.momentum_summary?.length ?? 0;
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
                          {momCount > 0 && (
                            <span>{momCount} strateg{momCount === 1 ? 'y' : 'ies'}</span>
                          )}
                          <span>{fmtDuration(r.started_at, r.finished_at)}</span>
                        </span>
                      </div>
                      {r.status === 'running' && (
                        <div className="pl-9 pr-1 w-full">
                          <div className="flex items-center justify-between gap-3 text-[10px] text-gray-500 mb-0.5">
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

/** Inline picker: list of available (not-yet-scheduled) backtests with
 * params preview, plus a confirm button. Shows the config blob for the
 * currently-selected backtest so the user can verify weights / sectors
 * / top-N before adding it to the schedule. */
function AddStrategyPicker({
  onAdded,
  onCancel,
}: {
  onAdded: () => Promise<void> | void;
  onCancel: () => void;
}) {
  const [available, setAvailable] = useState<AvailableBacktest[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedRunId, setSelectedRunId] = useState<number | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const r = await fetch(`${API_URL}/api/scheduled-strategies/available-backtests`);
        if (!r.ok) {
          if (!cancelled) setError(`Failed to load (${r.status})`);
          return;
        }
        const data = (await r.json()) as AvailableBacktest[];
        if (cancelled) return;
        setAvailable(data);
        if (data.length > 0) setSelectedRunId(data[0].run_id);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, []);

  const selected = useMemo(
    () => available.find((b) => b.run_id === selectedRunId) ?? null,
    [available, selectedRunId],
  );

  const confirm = async () => {
    if (selectedRunId == null) return;
    setSaving(true);
    setError(null);
    try {
      const r = await fetch(`${API_URL}/api/scheduled-strategies`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ backtest_run_id: selectedRunId }),
      });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Add failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      await onAdded();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="px-5 py-4 border-b border-gray-800/40 bg-[#0f1117]">
      <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">Pick a saved backtest to put on schedule</div>
      {loading ? (
        <div className="text-xs text-gray-500">Loading available backtests…</div>
      ) : available.length === 0 ? (
        <div className="text-xs text-gray-400">
          No saved backtests to add. All your saved backtests are already on schedule, or you don&apos;t have any yet — head over to <a href="/momentum" className="text-indigo-400 hover:underline">/momentum</a>, run a backtest, and save it.
        </div>
      ) : (
        <div className="space-y-3">
          <select
            value={selectedRunId ?? ''}
            disabled={saving}
            onChange={(e) => setSelectedRunId(Number(e.target.value))}
            className="bg-[#151821] border border-gray-700 rounded-lg px-3 py-1.5 text-xs text-gray-200 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 focus:outline-none w-full max-w-md"
          >
            {available.map((b) => (
              <option key={b.run_id} value={b.run_id}>
                {b.name} (#{b.run_id})
              </option>
            ))}
          </select>
          {selected && <StrategyConfigDetail cfg={selected.config} />}
          {error && <div className="text-xs text-rose-300">{error}</div>}
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => void confirm()}
              disabled={saving || selectedRunId == null}
              className="text-xs px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white transition-colors"
            >
              {saving ? 'Adding…' : 'Confirm + add to schedule'}
            </button>
            <button
              type="button"
              onClick={onCancel}
              disabled={saving}
              className="text-xs px-3 py-1.5 rounded-lg text-gray-400 hover:bg-white/5 transition-colors"
            >
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

/** Read-only breakdown of a backtest's config blob. Re-used by the add
 * picker and the per-strategy detail view. */
export function StrategyConfigDetail({ cfg }: { cfg: Record<string, unknown> }) {
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
  const signalWeights = (cfg.signal_weights as Record<string, number> | undefined) ?? {};
  const categoryWeights = (cfg.category_weights as Record<string, number> | undefined) ?? {};

  const universe = indexUniverse ?? universeLabel ?? 'All companies (no universe filter)';
  const sortedSignals = Object.entries(signalWeights).sort((a, b) => b[1] - a[1]);
  const sortedCategories = Object.entries(categoryWeights).sort((a, b) => b[1] - a[1]);

  return (
    <div className="bg-[#151821] border border-gray-800/40 rounded-lg p-3">
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
      </div>

      {sortedCategories.length > 0 && (
        <div className="mt-3">
          <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Category weights</div>
          <div className="flex flex-wrap gap-2 text-xs">
            {sortedCategories.map(([cat, w]) => (
              <span key={cat} className="bg-[#0f1117] border border-gray-800/60 rounded px-2 py-0.5">
                <span className="text-gray-400 capitalize">{cat}</span>
                <span className="text-gray-500 mx-1.5">·</span>
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
