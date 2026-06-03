'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import DailyMtdRefreshCard from './DailyMtdRefreshCard';
import Spinner from './Spinner';
import ScheduledStrategyDetail, { type StrategyRunHistory } from './ScheduledStrategyDetail';
import LoadingDots from './LoadingDots';
import { type StepDef, type StepState } from './ProgressTimeline';
import { apiFetch } from '../../lib/apiFetch';
import { dialog } from '../../lib/dialog';
import { WEEKDAY_LABELS } from './momentum/utils';

import { API_URL } from '../../lib/apiUrl';

export type IngestRun = {
  run_id: number;
  job_name: string;
  triggered_by: 'auto' | 'manual';
  started_at: string;
  finished_at: string | null;
  status: 'running' | 'ok' | 'error';
  current_phase: 'acquisition' | 'templates' | 'prune' | 'dedupe' | 'prices' | 'momentum' | 'done' | null;
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
  /** Post-XLS MSCI additions the pipeline couldn't verify on GuruFocus.
   * Each needs a manual override before the security can land in the
   * universe. Only populated for the ACWI template today. */
  unresolved_additions?: Array<{
    name: string;
    country: string;
    eff_date: string | null;
    reason: string;
    gf_url: string | null;
    openfigi_candidate?: { exch_code?: string; ticker?: string; name?: string } | null;
    msci_href?: string;
    detail?: string;
  }>;
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
  { key: 'dedupe', label: 'Duplicate company merge' },
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
    dedupe: { status: 'pending' },
    prices: { status: 'pending' },
    momentum: { status: 'pending' },
  };

  const liveMessage = run.current_message ?? undefined;

  // Phase 0 — Acquisition. There's no per-run summary column on
  // `ingest_run` for acquisition results — current_message carries the
  // status line. Once we move past this phase we mark it done.
  if (phase === 'templates' || phase === 'prune' || phase === 'dedupe' || phase === 'prices' || phase === 'momentum' || phase === 'done' || finished) {
    state.acquisition = { status: 'done', message: 'sources acquired' };
  } else if (phase === 'acquisition') {
    state.acquisition = { status: 'in_progress', message: liveMessage ?? 'probing upstream sources…' };
  }

  // Phase 1 — Templates
  const templates = run.templates_summary ?? [];
  const tplErr = templates.filter((t) => t.error).length;
  const tplOk = templates.length - tplErr;
  if (templates.length > 0 && (phase === 'prune' || phase === 'dedupe' || phase === 'prices' || phase === 'momentum' || phase === 'done' || finished)) {
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
  if (phase === 'dedupe' || phase === 'prices' || phase === 'momentum' || phase === 'done' || finished) {
    state.prune = { status: 'done', message: 'orphan companies pruned' };
  } else if (phase === 'prune') {
    state.prune = { status: 'in_progress', message: liveMessage ?? 'pruning orphan companies…' };
  }

  // Phase 2.5 — Dedupe (no per-run summary column; current_message
  // carries the merge counts).
  if (phase === 'prices' || phase === 'momentum' || phase === 'done' || finished) {
    state.dedupe = { status: 'done', message: 'duplicates merged' };
  } else if (phase === 'dedupe') {
    state.dedupe = { status: 'in_progress', message: liveMessage ?? 'merging duplicate companies…' };
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
  // Latest available close-price date across all companies (the freshest
  // data the pipeline could compute against). Shown on every strategy row
  // so the user can tell at a glance how current the underlying data is.
  const [latestPriceDate, setLatestPriceDate] = useState<string | null>(null);
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

  // Fetch the latest available close-price date once on mount.
  useEffect(() => {
    let cancelled = false;
    fetch(`${API_URL}/api/data/latest-price-date`)
      .then((r) => (r.ok ? r.json() : null))
      .then((d: { date?: string | null } | null) => {
        if (!cancelled && d?.date) setLatestPriceDate(d.date);
      })
      .catch(() => { /* non-critical — row just omits the date */ });
    return () => { cancelled = true; };
  }, []);

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

  const setRebalanceDay = useCallback(async (id: number, weekday: number) => {
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rebalance_weekday: weekday }),
      });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Rebalance-day update failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      await loadStrategies();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [loadStrategies]);

  const renameStrategy = useCallback(async (id: number, currentName: string) => {
    const next = await dialog.prompt('New name for this scheduled strategy:', {
      title: 'Rename strategy',
      defaultValue: currentName,
    });
    if (next == null) return; // cancelled
    const trimmed = next.trim();
    if (!trimmed || trimmed === currentName) return; // no change
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: trimmed }),
      });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Rename failed: ${r.status} ${body.slice(0, 200)}`);
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
        <div className="bg-[#151821] rounded-xl border border-gray-800/40">
          <div className="px-5 py-3 border-b border-gray-800/40 flex items-center justify-between">
            <h3 className="text-sm font-medium text-white">Scheduled strategies</h3>
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
              <a
                href="/backtest"
                className="text-xs px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
                title="Strategies can only be scheduled from a backtested variant — run a sweep on /backtest, then use the '+ Schedule' button on any OK variant row."
              >
                Add via /backtest →
              </a>
            </div>
          </div>

          {strategiesLoading ? (
            <div className="px-5 py-5 text-sm text-gray-500"><LoadingDots label="Loading" /></div>
          ) : strategies.length === 0 ? (
            <div className="px-5 py-6 text-sm text-gray-500">
              No strategies scheduled yet. Strategies must originate from a backtested variant: run a sweep on <a href="/backtest" className="text-indigo-300 hover:text-indigo-200 underline">/backtest</a>, then click <span className="text-gray-300">+ Schedule</span> on any OK variant row in the Variants table.
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
                      <label className="flex items-center gap-1.5 text-xs text-gray-400 shrink-0">
                        <span className="text-gray-500">rebal</span>
                        <select
                          value={(s.config.rebalance_weekday as number | undefined) ?? 0}
                          onChange={(e) => void setRebalanceDay(s.id, Number(e.target.value))}
                          className="bg-[#0f1117] border border-gray-700 rounded-lg px-2 py-1 text-gray-200 text-xs focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                          title="Weekday each rebalance period enters on (first <day> of the period)"
                        >
                          {WEEKDAY_LABELS.map((label, i) => (
                            <option key={i} value={i}>{label.slice(0, 3)}</option>
                          ))}
                        </select>
                      </label>
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
                        onClick={() => void renameStrategy(s.id, s.name || `Strategy #${s.id}`)}
                        className="text-xs px-2 py-1 rounded-lg text-gray-400 hover:bg-white/5 hover:text-gray-200 transition-colors"
                        title="Rename strategy"
                      >
                        Rename
                      </button>
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

/** One scheduled job from `GET /api/schedule/upcoming`'s `jobs` array. */
type UpcomingJob = {
  id: string;
  fires: string;
  next_run_at: string | null;
  label: string;
  description: string;
  cadence: string;
  running: boolean;
};

/** One in-flight run from the same endpoint's `running` array. */
type RunningJob = {
  run_id: number;
  job_name: string;
  triggered_by: 'auto' | 'manual' | string;
  started_at: string;
  current_phase: string | null;
  current_message: string | null;
  label: string;
};

type ScheduleUpcoming = {
  now: string;
  scheduler_enabled: boolean;
  jobs: UpcomingJob[];
  running: RunningJob[];
};

/** Compact "in 18h" / "in 6d" / "in 12m" / "now" relative formatter for a
 * future ISO timestamp, relative to `nowMs`. Returns '—' when null. */
function relTime(iso: string | null, nowMs: number): string {
  if (!iso) return '—';
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return '—';
  const diffSec = Math.round((t - nowMs) / 1000);
  if (diffSec <= 0) return 'now';
  const m = Math.round(diffSec / 60);
  if (m < 60) return `in ${m}m`;
  const h = Math.round(diffSec / 3600);
  if (h < 48) return `in ${h}h`;
  const d = Math.round(diffSec / 86400);
  return `in ${d}d`;
}

/** Short clock label for a future ISO timestamp (local time, HH:MM). */
function fmtClock(iso: string | null): string {
  if (!iso) return '';
  try {
    return new Date(iso).toLocaleString(undefined, {
      weekday: 'short', hour: '2-digit', minute: '2-digit',
    });
  } catch {
    return '';
  }
}

/** "Pipeline activity" strip at the top of /schedule. Running jobs (with
 * a live spinner + phase) render first; then every scheduled job in
 * chronological fire order. Polls the live scheduler every 3s so the view
 * reflects ANY activity — scheduled ticks, the startup catch-up one-shots,
 * or a manual Run-now — not just something this tab kicked off. */
function PipelineActivityCard() {
  const [data, setData] = useState<ScheduleUpcoming | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  // Local clock so relative times tick down between polls.
  const [nowMs, setNowMs] = useState<number>(() => Date.now());

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch(`${API_URL}/api/schedule/upcoming`);
        if (!r.ok || cancelled) return;
        const d = (await r.json()) as ScheduleUpcoming;
        if (!cancelled) { setData(d); setLoadError(null); }
      } catch (e) {
        if (!cancelled) setLoadError(e instanceof Error ? e.message : String(e));
      }
    };
    void poll();
    const id = window.setInterval(poll, 3000);
    return () => { cancelled = true; window.clearInterval(id); };
  }, []);

  // Tick the local clock every 30s so "in 18h" stays roughly current
  // without hammering re-renders.
  useEffect(() => {
    const id = window.setInterval(() => setNowMs(Date.now()), 30000);
    return () => window.clearInterval(id);
  }, []);

  const running = data?.running ?? [];
  const jobs = data?.jobs ?? [];

  return (
    <div className="space-y-3">
      <div className="flex items-baseline justify-between">
        <h2 className="text-sm uppercase tracking-wider text-gray-400 font-medium">
          Pipeline activity
        </h2>
        <p className="text-xs text-gray-600">
          What&apos;s running now + what fires next
        </p>
      </div>

      <div className="bg-[#151821] rounded-xl border border-gray-800/40">
        {loadError && data == null && (
          <div className="px-5 py-3 text-xs text-rose-300">
            Failed to load schedule activity: {loadError}
          </div>
        )}
        {!loadError && data == null && (
          <div className="px-5 py-3 text-sm text-gray-500">
            <LoadingDots label="Loading" />
          </div>
        )}

        {data && data.scheduler_enabled === false && running.length === 0 && (
          <div className="px-5 py-3 text-xs text-amber-300/90">
            In-process scheduler is disabled (DISABLE_SCHEDULER) — no jobs will fire automatically.
          </div>
        )}

        {data && (
          <div>
            {/* Running now */}
            {running.length > 0 && (
              <div className="px-5 py-3 border-b border-gray-800/30">
                <div className="text-[10px] uppercase tracking-wider text-indigo-300/80 mb-2">
                  Running now
                </div>
                <div className="space-y-2">
                  {running.map((r) => (
                    <div key={r.run_id} className="flex items-center gap-3 text-sm">
                      <Spinner className="h-3.5 w-3.5 text-indigo-400 shrink-0" />
                      <span className="text-white font-medium shrink-0">{r.label}</span>
                      {r.current_phase && (
                        <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-indigo-500/10 text-indigo-300 border-indigo-500/30 font-mono shrink-0">
                          {r.current_phase}
                        </span>
                      )}
                      <span className="text-xs text-gray-500 truncate min-w-0" title={r.current_message ?? ''}>
                        {r.current_message ?? ''}
                      </span>
                      <span className="text-[10px] text-gray-600 font-mono ml-auto shrink-0">
                        run #{r.run_id} · {r.triggered_by}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Upcoming */}
            <div className="px-5 py-3">
              <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">
                Upcoming
              </div>
              {jobs.length === 0 ? (
                <div className="text-xs text-gray-500">
                  No jobs scheduled.
                </div>
              ) : (
                <div className="space-y-1.5">
                  {jobs.map((j) => (
                    <div key={j.id} className="flex items-center gap-3 text-sm">
                      <span className="shrink-0 w-4 flex justify-center">
                        {j.running
                          ? <Spinner className="h-3.5 w-3.5 text-indigo-400" />
                          : <span className="text-gray-600">•</span>}
                      </span>
                      <span className={`shrink-0 ${j.running ? 'text-white' : 'text-gray-200'}`}>
                        {j.label}
                      </span>
                      <span className="text-xs text-gray-600 truncate min-w-0" title={j.cadence}>
                        {j.description}
                      </span>
                      <span className="ml-auto shrink-0 flex items-baseline gap-2 font-mono text-xs">
                        <span className={j.running ? 'text-indigo-300' : 'text-gray-300'}>
                          {j.running ? 'running' : relTime(j.next_run_at, nowMs)}
                        </span>
                        {!j.running && (
                          <span className="text-gray-600">{fmtClock(j.next_run_at)}</span>
                        )}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        )}
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
  // Live per-template refresh status, polled from the in-process registry
  // so the busy spinner + progress bar reflect ANY refresh (manual click,
  // scheduled month-end tick, or the daily pipeline) — not just one the
  // current tab triggered.
  const [statuses, setStatuses] = useState<Record<string, TemplateRefreshStatus>>({});

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

  // Poll the cheap (no-DB) status endpoint. When a template transitions
  // out of 'running' (a refresh just finished), refetch the summary so its
  // latest-month / last-refreshed / member-count update.
  const runningKeysRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch(`${API_URL}/api/universe-templates/refresh-status`);
        if (!r.ok || cancelled) return;
        const data = (await r.json()) as Record<string, TemplateRefreshStatus>;
        if (cancelled) return;
        setStatuses(data);
        const nowRunning = new Set(
          Object.entries(data).filter(([, s]) => s.status === 'running').map(([k]) => k),
        );
        // A key that was running last poll but isn't now → just finished.
        let finished = false;
        runningKeysRef.current.forEach((k) => { if (!nowRunning.has(k)) finished = true; });
        runningKeysRef.current = nowRunning;
        if (finished) void load();
      } catch {
        // Network blip — next tick retries.
      }
    };
    void poll();
    const id = window.setInterval(poll, 3000);
    return () => { cancelled = true; window.clearInterval(id); };
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
            {templates.map((t) => (
              <TemplateRow key={t.template_key} t={t} status={statuses[t.template_key]} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}


/** Live refresh status for one template, from the in-process registry
 * (`GET /api/universe-templates/refresh-status`). Absent when the template
 * hasn't been refreshed since the backend process started. */
type TemplateRefreshStatus = {
  status: 'running' | 'done' | 'error';
  message?: string;
  pct?: number | null;
  started_at?: string;
  finished_at?: string | null;
  error?: string | null;
};

/** Per-template row in the templates section. Collapsible — click to
 * expand into a live progress bar (while refreshing) + the recent
 * additions/removals diff. Shows a busy spinner whenever a refresh is in
 * flight (manual or scheduled), last refresh + next-refresh ETA, and a
 * per-row "Refresh" button that triggers a catch-up on demand. */
function TemplateRow({ t, status }: { t: UniverseTemplateSummary; status?: TemplateRefreshStatus }) {
  const [expanded, setExpanded] = useState(false);
  const [triggering, setTriggering] = useState(false);
  const neverRefreshed = t.last_refreshed_at == null;
  const busy = status?.status === 'running' || triggering;

  // Once the poll confirms this template is running, drop the optimistic
  // local flag (the registry status now drives the spinner).
  useEffect(() => {
    if (status?.status === 'running') setTriggering(false);
  }, [status?.status]);

  const triggerRefresh = () => {
    setTriggering(true);
    setExpanded(true); // reveal the progress bar immediately
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/universe-templates/${encodeURIComponent(t.template_key)}/refresh`,
          { method: 'POST', headers: { Accept: 'text/event-stream' } },
        );
        // Drain + discard so the connection closes cleanly; the status
        // poll in the parent drives the spinner / progress bar / refetch.
        const reader = r.body?.getReader();
        if (reader) { while (true) { const { done } = await reader.read(); if (done) break; } }
      } catch {
        // The status poll surfaces any error state.
      } finally {
        setTriggering(false);
      }
    })();
  };

  return (
    <div>
      <div className="w-full px-5 py-3 flex items-center gap-3 hover:bg-white/[0.02] transition-colors">
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          className="flex items-center gap-3 flex-1 min-w-0 text-left"
        >
          <span className="text-gray-500 text-xs font-mono w-3 inline-block shrink-0">
            {expanded ? '▾' : '▸'}
          </span>
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 flex-wrap">
              <span className={`text-sm font-medium ${neverRefreshed ? 'text-amber-200' : 'text-white'}`}>
                {t.label}
              </span>
              <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-gray-500/10 text-gray-400 border-gray-500/30 font-mono">
                {t.template_key}
              </span>
              {busy && <Spinner className="h-3.5 w-3.5 text-indigo-400" />}
              {neverRefreshed && !busy && (
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
        </button>
        <button
          type="button"
          onClick={triggerRefresh}
          disabled={busy}
          className="shrink-0 text-xs px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white transition-colors"
        >
          {busy ? 'Refreshing…' : 'Refresh'}
        </button>
      </div>
      {expanded && (
        <>
          {status && (status.status === 'running' || status.status === 'error') && (
            <div className="px-5 pt-3 pb-1 space-y-1">
              <div className="flex items-center justify-between text-[11px]">
                <span className={status.status === 'error' ? 'text-rose-300' : 'text-indigo-300'}>
                  {status.status === 'error'
                    ? 'Refresh failed'
                    : (status.message || 'Refreshing…')}
                </span>
                {status.status === 'running' && status.pct != null && (
                  <span className="font-mono text-gray-400">{status.pct}%</span>
                )}
              </div>
              <div className="h-1.5 bg-gray-800 rounded-full overflow-hidden">
                <div
                  className={`h-full transition-all duration-300 ${status.status === 'error' ? 'bg-rose-500' : 'bg-indigo-500'}`}
                  // Indeterminate (no pct) → a partial bar so the user still
                  // sees motion via the changing message above.
                  style={{ width: `${status.status === 'error' ? 100 : (status.pct ?? 35)}%` }}
                />
              </div>
              {status.status === 'error' && status.error && (
                <div className="text-[11px] text-rose-300/80 font-mono truncate" title={status.error}>
                  {status.error}
                </div>
              )}
            </div>
          )}
          <TemplateRecentChanges templateKey={t.template_key} />
        </>
      )}
    </div>
  );
}

/** Recent additions/removals for one template. Fetched lazily on
 * expand from `GET /api/universe-templates/{key}/recent-changes`. */
function TemplateRecentChanges({ templateKey }: { templateKey: string }) {
  type ChangeEntry = {
    run_id: number;
    started_at: string;
    status: string;
    this_month: string | null;
    prev_month: string | null;
    additions_count: number;
    removals_count: number;
    renames_count: number;
    additions: Array<{ company_id: number; ticker?: string; name?: string | null; sector?: string | null }>;
    removals: Array<{ company_id: number; ticker?: string; name?: string | null; sector?: string | null }>;
    renames: Array<{ company_id: number; old_ticker?: string; new_ticker?: string; name?: string | null }>;
  };
  const [data, setData] = useState<ChangeEntry[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    fetch(`${API_URL}/api/universe-templates/${encodeURIComponent(templateKey)}/recent-changes?limit=5`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((rows: ChangeEntry[]) => { if (!cancelled) setData(rows); })
      .catch((e: unknown) => { if (!cancelled) setError(e instanceof Error ? e.message : String(e)); });
    return () => { cancelled = true; };
  }, [templateKey]);

  if (error) {
    return (
      <div className="px-12 pb-3 text-xs text-rose-300">Failed to load recent changes: {error}</div>
    );
  }
  if (data == null) {
    return (
      <div className="px-12 pb-3 text-xs text-gray-500"><LoadingDots label="Loading recent changes" /></div>
    );
  }
  if (data.length === 0) {
    return (
      <div className="px-12 pb-3 text-xs text-gray-500">No pipeline runs have refreshed this template yet — recent additions/removals will appear here after the next tick.</div>
    );
  }
  return (
    <div className="px-12 pb-4 space-y-3">
      {data.map((entry) => {
        const noChanges = entry.additions_count === 0 && entry.removals_count === 0 && entry.renames_count === 0;
        return (
          <div key={`${entry.run_id}-${entry.this_month}`} className="bg-[#0f1117] border border-gray-800/40 rounded-lg overflow-hidden">
            <div className="px-3 py-2 border-b border-gray-800/40 flex items-baseline gap-3 flex-wrap text-xs">
              <span className="text-gray-300 font-mono">{fmtTimestamp(entry.started_at)}</span>
              <span className="text-gray-500 font-mono">
                {entry.this_month ?? '—'}
                {entry.prev_month && <span className="text-gray-600"> vs {entry.prev_month}</span>}
              </span>
              <span className="text-[10px] text-gray-500 font-mono ml-auto">run #{entry.run_id}</span>
              <span className="text-xs text-gray-400 font-mono">
                +{entry.additions_count} / −{entry.removals_count}
                {entry.renames_count > 0 && <span> / r{entry.renames_count}</span>}
              </span>
            </div>
            {noChanges ? (
              <div className="px-3 py-2 text-[11px] text-gray-500 italic">No constituent changes vs the prior month.</div>
            ) : (
              <div className="grid gap-2 md:grid-cols-3 p-3">
                {entry.additions_count > 0 && (
                  <DiffPanel
                    color="emerald" label="Additions" count={entry.additions_count}
                    items={entry.additions.map((a) => ({ key: a.company_id, primary: a.ticker ?? '?', secondary: a.name ?? null }))}
                  />
                )}
                {entry.removals_count > 0 && (
                  <DiffPanel
                    color="rose" label="Removals" count={entry.removals_count}
                    items={entry.removals.map((a) => ({ key: a.company_id, primary: a.ticker ?? '?', secondary: a.name ?? null }))}
                  />
                )}
                {entry.renames_count > 0 && (
                  <DiffPanel
                    color="amber" label="Renames" count={entry.renames_count}
                    items={entry.renames.map((r) => ({ key: r.company_id, primary: `${r.old_ticker} → ${r.new_ticker}`, secondary: r.name ?? null }))}
                  />
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function DiffPanel({
  color, label, count, items,
}: {
  color: 'emerald' | 'rose' | 'amber';
  label: string;
  count: number;
  items: Array<{ key: number; primary: string; secondary: string | null }>;
}) {
  const colorCls = color === 'emerald'
    ? 'text-emerald-300 bg-emerald-500/10 border-emerald-500/30'
    : color === 'rose'
      ? 'text-rose-300 bg-rose-500/10 border-rose-500/30'
      : 'text-amber-300 bg-amber-500/10 border-amber-500/30';
  return (
    <div className="bg-[#151821] rounded border border-gray-800/40">
      <div className="px-2.5 py-1 flex items-center gap-2 border-b border-gray-800/40">
        <span className={`text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border ${colorCls}`}>{label}</span>
        <span className="text-xs text-gray-300 font-mono">{count}</span>
      </div>
      <div className="max-h-48 overflow-auto divide-y divide-gray-800/30">
        {items.slice(0, 50).map((it) => (
          <div key={it.key} className="px-2.5 py-1">
            <div className="text-xs font-mono text-gray-200">{it.primary}</div>
            {it.secondary && <div className="text-[10px] text-gray-500 truncate">{it.secondary}</div>}
          </div>
        ))}
        {items.length > 50 && (
          <div className="px-2.5 py-1 text-[10px] text-gray-600 italic">+ {items.length - 50} more (open the run on /schedule for the full list)</div>
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
  const rebalanceWeekday = (cfg.rebalance_weekday as number | undefined) ?? 0;
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
        <ConfigRow label="Rebalance day" value={`first ${WEEKDAY_LABELS[rebalanceWeekday] ?? 'Monday'}`} />
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
