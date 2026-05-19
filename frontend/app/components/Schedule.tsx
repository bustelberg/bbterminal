'use client';

import { useCallback, useEffect, useState } from 'react';
import AddScheduledStrategyForm from './AddScheduledStrategyForm';
import ScheduledStrategyDetail from './ScheduledStrategyDetail';
import { type StepDef, type StepState } from './ProgressTimeline';
import { apiFetch } from '../../lib/apiFetch';
import { dialog } from '../../lib/dialog';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

export type IngestRun = {
  run_id: number;
  job_name: string;
  triggered_by: 'auto' | 'manual';
  started_at: string;
  finished_at: string | null;
  status: 'running' | 'ok' | 'error';
  current_phase: 'templates' | 'prices' | 'momentum' | 'done' | null;
  // Array — one entry per template-managed universe the pipeline
  // refreshed in phase 1. Each entry carries that template's per-run
  // diff (additions/removals/renames). Empty when no templates are
  // registered for this run.
  templates_summary: TemplateDiff[] | null;
  // Array — one entry per scheduled strategy the pipeline tried.
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

/** One entry per template-managed universe in `templates_summary`.
 * Carries the universe identity (template_key + universe_id) and the
 * per-run diff. `error` is set when the template's refresh failed for
 * this run; in that case the count/list fields will be zero/empty. */
export type TemplateDiff = {
  template_key: string;
  universe_id: number | null;
  this_month: string | null;
  prev_month: string | null;
  additions_count: number;
  removals_count: number;
  renames_count: number;
  additions: Array<{ company_id: number; ticker: string; name: string | null; sector: string | null }>;
  removals: Array<{ company_id: number; ticker: string; name: string | null; sector: string | null }>;
  renames: Array<{ company_id: number; old_ticker: string; new_ticker: string; name: string | null }>;
  error?: string | null;
};

/** One entry per scheduled strategy in `ingest_run.momentum_summary`. */
export type MomentumStrategyResult = {
  strategy_id: number | null;
  strategy_name: string;
  frequency: string | null;
  /** Snapshot of the strategy's config at the time the pipeline ran it.
   * Shown in the run-detail view so the user can verify what was
   * actually computed — useful when the schedule entry has been edited
   * after the run. */
  config: Record<string, unknown>;
  snapshot_id: number | null;
  holdings_count: number;
  latest_price_date: string | null;
  status: 'ok' | 'error';
  error_message: string | null;
  /** Full Python traceback captured server-side on failure. Long;
   * rendered inside a collapsible <pre> on the run-detail view. */
  error_traceback: string | null;
};

export type ScheduledStrategy = {
  id: number;
  name: string;
  frequency: 'daily' | 'weekly' | 'monthly' | 'bimonthly' | 'quarterly' | null;
  config: Record<string, unknown>;
  enabled: boolean;
  created_at: string;
  updated_at: string;
  last_run_at: string | null;
  next_due_at: string | null;
  last_snapshot: {
    snapshot_id: number;
    ingest_run_id: number | null;
    created_at: string;
    latest_price_date: string | null;
    holdings_count: number;
  } | null;
};

// The pipeline still fires once a week (Tuesday 02:00 UTC) via the
// in-process APScheduler in `backend/scheduler.py`. The per-job cards
// and the global "Recent runs" view that used to live in this page
// have been removed — each scheduled strategy's run history is shown
// in its own expandable detail view (see ScheduledStrategyDetail).

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
  { key: 'templates', label: 'Template universe refresh' },
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
    templates: { status: 'pending' },
    prices: { status: 'pending' },
    momentum: { status: 'pending' },
  };

  const liveMessage = run.current_message ?? undefined;

  // Phase 1 — Templates
  const templates = run.templates_summary ?? [];
  const tplErr = templates.filter((t) => t.error).length;
  const tplOk = templates.length - tplErr;
  if (templates.length > 0 && (phase === 'prices' || phase === 'momentum' || phase === 'done' || finished)) {
    // Aggregate diff across templates for the inline message.
    const totAdd = templates.reduce((a, t) => a + (t.additions_count || 0), 0);
    const totRem = templates.reduce((a, t) => a + (t.removals_count || 0), 0);
    const totRen = templates.reduce((a, t) => a + (t.renames_count || 0), 0);
    state.templates = {
      status: tplErr > 0 && tplOk === 0 ? 'error' : 'done',
      message: `${tplOk}/${templates.length} ok · +${totAdd} / −${totRem}${totRen > 0 ? ` / r${totRen}` : ''}`,
    };
  } else if (phase === 'templates') {
    state.templates = { status: 'in_progress', message: liveMessage ?? 'reconstructing template universes…' };
  } else if (finished) {
    state.templates = { status: 'error', message: 'failed' };
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
    { key: 'templates' as const, hasData: (run.templates_summary?.length ?? 0) > 0 },
    { key: 'prices' as const,    hasData: run.companies_processed > 0 || run.prices_refreshed > 0 || run.volumes_refreshed > 0 },
    { key: 'momentum' as const,  hasData: (run.momentum_summary?.length ?? 0) > 0 },
  ];
  const currentPhase = run.current_phase;
  return (
    <span className="inline-flex items-center gap-1" title={`Phases: templates · prices · momentum (current: ${currentPhase ?? '—'})`}>
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
  const [strategies, setStrategies] = useState<ScheduledStrategy[]>([]);
  const [strategiesLoading, setStrategiesLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedStrategyId, setExpandedStrategyId] = useState<number | null>(null);
  const [addingPickerOpen, setAddingPickerOpen] = useState(false);

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
    void loadStrategies();
  }, [loadStrategies]);

  const toggleStrategy = useCallback(async (id: number, enabled: boolean) => {
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies/${id}`, {
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
    const ok = await dialog.confirm(
      'Remove this strategy from the schedule? Existing snapshots will be preserved.',
      { title: 'Remove scheduled strategy', confirmLabel: 'Remove', destructive: true },
    );
    if (!ok) return;
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies/${id}`, { method: 'DELETE' });
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

  const removeAllStrategies = useCallback(async () => {
    const count = strategies.length;
    if (count === 0) return;
    const ok = await dialog.confirm(
      `Remove all ${count} scheduled strateg${count === 1 ? 'y' : 'ies'}? Existing snapshots will be preserved (their schedule-strategy link goes NULL via cascade, but the holdings stay inspectable).`,
      { title: 'Remove all', confirmLabel: `Remove ${count}`, destructive: true },
    );
    if (!ok) return;
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies`, { method: 'DELETE' });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Delete all failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      setExpandedStrategyId(null);
      await loadStrategies();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [strategies.length, loadStrategies]);

  return (
    <div className="min-h-screen bg-[#0f1117] text-gray-200">
      <div className="px-8 py-5 border-b border-gray-800/40">
        <h1 className="text-xl font-semibold text-white">Schedule</h1>
        <p className="text-sm text-gray-500 mt-1">
          The pipeline fires Tuesday 02:00 UTC (after Monday global close). Each strategy either rebalances (per its frequency) or has its prior holdings re-priced — every tick produces exactly one snapshot per strategy.
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
            <div className="flex items-center gap-2 shrink-0">
              {strategies.length > 0 && (
                <button
                  type="button"
                  onClick={() => void removeAllStrategies()}
                  className="text-xs px-3 py-1.5 rounded-lg text-rose-300 hover:bg-rose-500/10 transition-colors"
                  title="Delete every scheduled strategy (snapshots stay)"
                >
                  Remove all
                </button>
              )}
              <button
                type="button"
                onClick={() => setAddingPickerOpen((v) => !v)}
                className="text-xs px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
              >
                {addingPickerOpen ? 'Cancel' : '+ Add strategy'}
              </button>
            </div>
          </div>

          {addingPickerOpen && (
            <AddScheduledStrategyForm
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
              No strategies scheduled yet. Click <span className="text-gray-300">+ Add strategy</span> to add one inline.
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
                              {s.name || `Strategy #${s.id}`}
                            </span>
                            {s.frequency && (
                              <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-indigo-500/10 text-indigo-300 border-indigo-500/30">
                                {s.frequency}
                              </span>
                            )}
                            {!s.enabled && (
                              <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-gray-500/10 text-gray-400 border-gray-500/30">
                                paused
                              </span>
                            )}
                          </div>
                          <div className="text-xs text-gray-500 mt-0.5 font-mono">
                            {strategySummary(s.config)}
                            {s.last_run_at ? (
                              <span className="text-gray-600">
                                {' · '}last run {fmtTimestamp(s.last_run_at)}
                              </span>
                            ) : (
                              <span className="text-gray-600">{' · '}not run yet</span>
                            )}
                            {s.next_due_at && (
                              <span className="text-gray-600">
                                {' · '}next {fmtTimestamp(s.next_due_at)}
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

      </div>
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
