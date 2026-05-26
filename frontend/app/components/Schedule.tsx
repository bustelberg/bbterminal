'use client';

import { useCallback, useEffect, useState } from 'react';
import AddScheduledStrategyForm from './AddScheduledStrategyForm';
import DailyMtdRefreshCard from './DailyMtdRefreshCard';
import ScheduledStrategyDetail, { type StrategyRunHistory } from './ScheduledStrategyDetail';
import LoadingDots from './LoadingDots';
import { type StepDef, type StepState } from './ProgressTimeline';
import { apiFetch } from '../../lib/apiFetch';
import { dialog } from '../../lib/dialog';

import { API_URL } from '../../lib/apiUrl';

export type IngestRun = {
  run_id: number;
  job_name: string;
  triggered_by: 'auto' | 'manual';
  started_at: string;
  finished_at: string | null;
  status: 'running' | 'ok' | 'error';
  current_phase: 'acquisition' | 'templates' | 'prune' | 'prices' | 'momentum' | 'done' | null;
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
  /** Which kind of operation this run executed for the strategy:
   * `rebalance` (fresh holdings) or `price_update` (last rebalance
   * re-priced). Missing on older rows that pre-date the field. */
  kind?: 'rebalance' | 'price_update' | string | null;
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

/** One row from `GET /api/universe-templates`. Subset of the backend
 * `_summary()` payload — only the fields the schedule section consumes.
 * `last_refreshed_at === null` is the signal that a template was added
 * but has never been refreshed in this env; the scheduler's bootstrap
 * path is supposed to kick off the first refresh on app start but we
 * still surface the state here so the user knows what's happening. */
export type UniverseTemplateSummary = {
  template_key: string;
  label: string;
  description: string;
  earliest_date: string;
  universe_id: number | null;
  months_captured: number;
  latest_captured_month: string | null;
  latest_membership_count: number;
  last_refreshed_at: string | null;
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
    /** Distinct sectors held in the latest snapshot, ordered by count desc
     * (ties broken alphabetically). Empty list when no sectors are populated
     * on the holdings (e.g. very early backfill rows). */
    sectors: { sector: string; count: number }[];
    /** Month-to-date / year-to-date return for the strategy as of `as_of_date`,
     * computed server-side from the snapshot equity curve. Null when there
     * isn't enough history (e.g. brand-new strategy with no closed period). */
    mtd_return_pct: number | null;
    ytd_return_pct: number | null;
    /** The latest_price_date of the newest snapshot — i.e. the date these
     * returns are "as of". Hoisted out for easy display. */
    as_of_date: string | null;
  } | null;
};

// The pipeline still fires once a week (Tuesday 02:00 UTC) via the
// in-process APScheduler in `backend/scheduler.py`. The per-job cards
// and the global "Recent runs" view that used to live in this page
// have been removed — each scheduled strategy's run history is shown
// in its own expandable detail view (see ScheduledStrategyDetail).

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

export const PIPELINE_STEPS: StepDef[] = [
  { key: 'acquisition', label: 'Source acquisition' },
  { key: 'templates', label: 'Template universe refresh' },
  { key: 'prune', label: 'Orphan company prune' },
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
    acquisition: { status: 'pending' },
    templates: { status: 'pending' },
    prune: { status: 'pending' },
    prices: { status: 'pending' },
    momentum: { status: 'pending' },
  };

  const liveMessage = run.current_message ?? undefined;

  // Phase 0 — Acquisition. There's no per-run summary column on
  // `ingest_run` for acquisition results — current_message carries the
  // status line. Once we move past this phase we mark it done.
  if (phase === 'templates' || phase === 'prune' || phase === 'prices' || phase === 'momentum' || phase === 'done' || finished) {
    state.acquisition = { status: 'done', message: 'sources acquired' };
  } else if (phase === 'acquisition') {
    state.acquisition = { status: 'in_progress', message: liveMessage ?? 'probing upstream sources…' };
  }

  // Phase 1 — Templates
  const templates = run.templates_summary ?? [];
  const tplErr = templates.filter((t) => t.error).length;
  const tplOk = templates.length - tplErr;
  if (templates.length > 0 && (phase === 'prune' || phase === 'prices' || phase === 'momentum' || phase === 'done' || finished)) {
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

  // Phase 2 — Prune (no per-run summary column; current_message
  // carries the count line. Once we move past prune the step is done
  // unless the phase errored, which would land in error_summary).
  if (phase === 'prices' || phase === 'momentum' || phase === 'done' || finished) {
    state.prune = { status: 'done', message: 'orphan companies pruned' };
  } else if (phase === 'prune') {
    state.prune = { status: 'in_progress', message: liveMessage ?? 'pruning orphan companies…' };
  }

  // Phase 3 — Prices
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

  // Phase 4 — Momentum
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
  // Per-strategy run-history cache. Survives collapse/re-expand so the
  // detail view renders instantly on a second click; the detail still
  // fires a silent revalidate fetch on every mount to pick up updates.
  const [historyCache, setHistoryCache] = useState<Map<number, StrategyRunHistory>>(new Map());
  const cacheRunHistory = useCallback((id: number, data: StrategyRunHistory) => {
    setHistoryCache((prev) => {
      const next = new Map(prev);
      next.set(id, data);
      return next;
    });
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

      <div className="px-8 py-6 space-y-6 max-w-screen-2xl">
        {error && (
          <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-sm text-rose-300 flex items-center justify-between">
            <span>{error}</span>
            <button type="button" onClick={() => setError(null)} className="text-rose-200 hover:text-white text-xs">dismiss</button>
          </div>
        )}

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
        <div className="space-y-3">
          <div className="flex items-baseline justify-between">
            <h2 className="text-sm uppercase tracking-wider text-gray-400 font-medium">
              Misc jobs
            </h2>
            <p className="text-xs text-gray-600">
              Recurring side-tasks (not per-strategy compute)
            </p>
          </div>
          <DailyMtdRefreshCard />
        </div>

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
            <div className="px-5 py-5 text-sm text-gray-500"><LoadingDots label="Loading" /></div>
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
                          {s.last_snapshot && (
                            <div className="text-xs text-gray-500 mt-0.5 font-mono flex flex-wrap items-center gap-x-2 gap-y-1">
                              {s.last_snapshot.sectors.length > 0 && (
                                <span className="text-gray-400">
                                  {s.last_snapshot.sectors
                                    .map((sec) => `${sec.sector} ×${sec.count}`)
                                    .join(' · ')}
                                </span>
                              )}
                              {(s.last_snapshot.mtd_return_pct != null
                                || s.last_snapshot.ytd_return_pct != null) && (
                                <span className="text-gray-600">|</span>
                              )}
                              {s.last_snapshot.mtd_return_pct != null && (
                                <span>
                                  <span className="text-gray-500">MTD </span>
                                  <span className={s.last_snapshot.mtd_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}>
                                    {s.last_snapshot.mtd_return_pct >= 0 ? '+' : ''}
                                    {s.last_snapshot.mtd_return_pct.toFixed(2)}%
                                  </span>
                                </span>
                              )}
                              {s.last_snapshot.ytd_return_pct != null && (
                                <span>
                                  <span className="text-gray-500">YTD </span>
                                  <span className={s.last_snapshot.ytd_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}>
                                    {s.last_snapshot.ytd_return_pct >= 0 ? '+' : ''}
                                    {s.last_snapshot.ytd_return_pct.toFixed(2)}%
                                  </span>
                                </span>
                              )}
                              {s.last_snapshot.as_of_date && (
                                <span className="text-gray-600">
                                  (as of {s.last_snapshot.as_of_date})
                                </span>
                              )}
                            </div>
                          )}
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
                        initialData={historyCache.get(s.id) ?? null}
                        onLoaded={(d) => cacheRunHistory(s.id, d)}
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

/** Template universes section on /schedule. Lists every registered
 * `UniverseTemplate` with its current state and surfaces the
 * `last_refreshed_at IS NULL` case so the dev→prod gap (new template
 * deployed but pipeline hasn't run yet) is visible at a glance instead
 * of silently producing empty membership chips on /companies.
 *
 * Pairs with `_maybe_bootstrap_templates` in `backend/scheduler.py` —
 * the bootstrap should normally fix any never-refreshed templates
 * within ~10 minutes of a fresh deploy, but if it gets short-circuited
 * (another pipeline already running, etc.) the banner here gives the
 * user a manual "Run pipeline now" button. */
function TemplateUniversesCard() {
  const [templates, setTemplates] = useState<UniverseTemplateSummary[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [triggering, setTriggering] = useState(false);
  const [triggerError, setTriggerError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/api/universe-templates`);
      if (!r.ok) {
        setLoadError(`${r.status} ${r.statusText}`);
        return;
      }
      const data = (await r.json()) as UniverseTemplateSummary[];
      setTemplates(Array.isArray(data) ? data : []);
    } catch (e) {
      setLoadError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const triggerPipeline = useCallback(async () => {
    setTriggering(true);
    setTriggerError(null);
    try {
      const r = await apiFetch(
        `${API_URL}/api/ingest/scheduled-refresh/trigger?job_name=manual`,
        { method: 'POST' },
      );
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setTriggerError(`${r.status} ${body.slice(0, 200)}`);
        return;
      }
      // Refetch after a short delay so `last_refreshed_at` updates once
      // the pipeline writes its first checkpoint. The user can also
      // expand the run via the existing strategy-history view.
      window.setTimeout(() => void load(), 5000);
    } catch (e) {
      setTriggerError(e instanceof Error ? e.message : String(e));
    } finally {
      setTriggering(false);
    }
  }, [load]);

  const unrefreshed = templates?.filter((t) => t.last_refreshed_at == null) ?? [];

  return (
    <div className="space-y-3">
      <div className="flex items-baseline justify-between">
        <h2 className="text-sm uppercase tracking-wider text-gray-400 font-medium">
          Template universes
        </h2>
        <p className="text-xs text-gray-600">
          Canonical universes refreshed by the pipeline
        </p>
      </div>

      {unrefreshed.length > 0 && (
        <div className="bg-amber-500/10 border border-amber-500/30 rounded-lg px-4 py-3 text-sm">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="text-amber-300 font-medium mb-1">
                {unrefreshed.length} template{unrefreshed.length === 1 ? '' : 's'} never refreshed in this environment
              </div>
              <div className="text-gray-300 text-xs leading-relaxed">
                {unrefreshed.map((t) => t.template_key).join(', ')} —
                the pipeline will populate {unrefreshed.length === 1 ? 'it' : 'them'} on
                the next Tuesday 02:00 UTC tick, or click below to trigger now.
                The in-process scheduler also auto-fires a bootstrap on app
                start; if you just deployed, give it a minute.
              </div>
              {triggerError && (
                <div className="text-rose-300 text-xs mt-2">Trigger failed: {triggerError}</div>
              )}
            </div>
            <button
              type="button"
              onClick={() => void triggerPipeline()}
              disabled={triggering}
              className="text-xs px-3 py-1.5 rounded-lg bg-amber-500/20 hover:bg-amber-500/30 text-amber-100 border border-amber-500/40 transition-colors disabled:opacity-50 disabled:cursor-not-allowed shrink-0"
            >
              {triggering ? 'Triggering…' : 'Run pipeline now'}
            </button>
          </div>
        </div>
      )}

      <div className="bg-[#151821] rounded-xl border border-gray-800/40">
        {loadError && (
          <div className="px-5 py-3 text-xs text-rose-300">
            Failed to load templates: {loadError}
          </div>
        )}
        {!loadError && templates == null && (
          <div className="px-5 py-3 text-sm text-gray-500">
            <LoadingDots label="Loading" />
          </div>
        )}
        {!loadError && templates?.length === 0 && (
          <div className="px-5 py-4 text-sm text-gray-500">
            No templates registered.
          </div>
        )}
        {templates && templates.length > 0 && (
          <div className="divide-y divide-gray-800/30">
            {templates.map((t) => {
              const neverRefreshed = t.last_refreshed_at == null;
              return (
                <div key={t.template_key} className="px-5 py-3 flex items-center gap-3 flex-wrap">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className={`text-sm font-medium ${neverRefreshed ? 'text-amber-200' : 'text-white'}`}>
                        {t.label}
                      </span>
                      <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-gray-500/10 text-gray-400 border-gray-500/30 font-mono">
                        {t.template_key}
                      </span>
                      {neverRefreshed && (
                        <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-amber-500/15 text-amber-300 border-amber-500/40">
                          never refreshed
                        </span>
                      )}
                    </div>
                    <div className="text-xs text-gray-500 mt-0.5 font-mono">
                      {neverRefreshed ? (
                        <span className="text-amber-300/80">no membership data yet</span>
                      ) : (
                        <>
                          {t.latest_membership_count} member{t.latest_membership_count === 1 ? '' : 's'}
                          {t.latest_captured_month && ` · latest month ${t.latest_captured_month}`}
                          {' · last refresh '}
                          {fmtTimestamp(t.last_refreshed_at)}
                        </>
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
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
