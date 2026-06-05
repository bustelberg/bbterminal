/**
 * Shared types for the `/schedule` page and its sub-components.
 *
 * Lifted out of `Schedule.tsx` so the data shapes that cross component
 * boundaries (and that `DailyMtdRefreshCard` / `ScheduledStrategyDetail`
 * / `ScheduleRunDetail` also import) live in one neutral place instead of
 * the god-component. No runtime logic here — types only.
 */

export type IngestRun = {
  run_id: number;
  job_name: string;
  triggered_by: 'auto' | 'manual';
  started_at: string;
  finished_at: string | null;
  status: 'running' | 'ok' | 'error';
  current_phase: 'plan' | 'acquisition' | 'templates' | 'prune' | 'dedupe' | 'prices' | 'momentum' | 'done' | null;
  /** Smart-pipeline derived plan (only on `smart_daily` runs). */
  plan_summary?: SmartPlan | null;
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

/** One scheduled job from `GET /api/schedule/upcoming`'s `jobs` array. */
export type UpcomingJob = {
  id: string;
  fires: string;
  next_run_at: string | null;
  label: string;
  description: string;
  cadence: string;
  running: boolean;
};

/** One in-flight run from the same endpoint's `running` array. */
export type RunningJob = {
  run_id: number;
  job_name: string;
  triggered_by: 'auto' | 'manual' | string;
  started_at: string;
  current_phase: string | null;
  current_message: string | null;
  label: string;
  plan_summary?: SmartPlan | null;
  // Live price-refresh counters (present while the prices phase runs).
  companies_processed?: number | null;
  companies_total?: number | null;
  prices_refreshed?: number | null;
  volumes_refreshed?: number | null;
  forbidden_count?: number | null;
  error_count?: number | null;
};

/** One pooled held company from `GET /api/scheduled-strategies/held-companies`. */
export type HeldCompany = {
  company_id: number;
  ticker: string | null;
  exchange: string;
  company_name: string | null;
  sector: string | null;
  latest_close_price_date: string | null;
  held_by: Array<{
    strategy_id: number;
    strategy_name: string;
    snapshot_kind: string | null;
    as_of_date: string | null;
    latest_price_date: string | null;
    target_weight: number;
  }>;
};

export type HeldCompaniesResponse = {
  total_companies: number;
  total_strategies: number;
  freshness_summary?: {
    latest_close_date: string | null;
    /** Reference the fresh/stale split is measured against (last settled
     * trading day). A holding is stale when its close is behind this. */
    expected_close_date?: string | null;
    fresh_count: number;
    stale_count: number;
    missing_count: number;
  };
  companies: HeldCompany[];
};

/** Per-strategy entry in a smart-pipeline plan (`SmartPlan.strategies`). */
export type SmartPlanStrategy = {
  strategy_id: number;
  strategy_name: string;
  frequency: string | null;
  rebalance_weekday: number;
  /** Raw index_universe/universe_label from the strategy's config. */
  label: string | null;
  resolved_template_key: string | null;
  resolved_universe_id: number | null;
  is_due: boolean;
  due_reason: 'first_run' | 'due' | 'not_due' | 'unresolved' | string;
};

/** The derived plan a `smart_daily` tick produced, from
 * `ingest_run.plan_summary` / `GET /api/schedule/plan`. */
export type SmartPlan = {
  as_of: string;
  needed_template_keys: string[];
  unresolved_labels: string[];
  due_strategy_ids: number[];
  strategies: SmartPlanStrategy[];
  universes_refreshed: string[];
  held_company_count: number | null;
  universe_company_count: number | null;
};

/** `GET /api/schedule/plan` — the latest smart-pipeline run + its plan. */
export type SchedulePlanResponse = {
  run: {
    run_id: number;
    status: 'running' | 'ok' | 'error';
    current_phase: string | null;
    started_at: string;
    finished_at: string | null;
    error_summary: string | null;
    plan_summary: SmartPlan | null;
    triggered_by: 'auto' | 'manual' | string;
  } | null;
  plan: SmartPlan | null;
};

export type ScheduleUpcoming = {
  now: string;
  scheduler_enabled: boolean;
  jobs: UpcomingJob[];
  running: RunningJob[];
};

/** Live refresh status for one template, from the in-process registry
 * (`GET /api/universe-templates/refresh-status`). Absent when the template
 * hasn't been refreshed since the backend process started. */
export type TemplateRefreshStatus = {
  status: 'running' | 'done' | 'error';
  message?: string;
  pct?: number | null;
  started_at?: string;
  finished_at?: string | null;
  error?: string | null;
};
