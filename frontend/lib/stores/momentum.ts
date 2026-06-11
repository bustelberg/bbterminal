import { createStore } from '../store';
import { runSSE } from '../stream';
import { API_URL } from '../apiUrl';
import type {
  BacktestRequest as ApiBacktestRequest,
  VariantSpec as ApiVariantSpec,
} from '../types/api';

export type Holding = {
  company_id: number;
  ticker: string;
  company_name: string;
  sector: string;
  score: number;
  category_scores: Record<string, number | null>;
  weight: number;
  forward_return_pct: number | null;
  currency?: string | null;
  entry_price_local?: number | null;
  exit_price_local?: number | null;
  entry_price_eur?: number | null;
  exit_price_eur?: number | null;
  entry_date?: string | null;
  exit_date?: string | null;
  // "long" or "short". Long-only backtests omit this (treated as long);
  // long-short backtests include it on every holding so the dashboard can
  // split sector breakdowns / equity contributions by side.
  side?: 'long' | 'short';
  // 1-indexed sector position within the period's chosen sectors
  // (1 = top sector). Optional because legacy snapshots persisted before
  // ranks existed don't carry it, and sector-ETF mode also leaves it
  // null. Renders as a small "1·2" badge in the holdings table.
  sector_rank?: number | null;
  // 1-indexed position within the sector (1 = top company in that
  // sector). Same nullability rules as `sector_rank`.
  company_rank?: number | null;
};

export type PeriodRecord = {
  date: string;
  holdings: Holding[];
  portfolio_return_pct: number | null;
  cumulative_return_pct: number;
  empty_reason?: string;
  // True when this is the trailing "open" period — the strategy's current
  // (still in flight) holding. Returns reflect the partial window from the
  // last scheduled rebalance through the most recent available close. The
  // backend excludes open periods from Sharpe/annualization; the UI should
  // tag the row visually so users can tell it's a YTD snapshot rather than
  // a closed period.
  is_open?: boolean;
  // Effective end date for an open period: the most recent date common to
  // every held company (min of per-holding max trade dates). Surfaced next
  // to the "open" badge so users see how stale the displayed return is
  // when some names stopped reporting earlier than others.
  as_of_date?: string;
  // "What if you held the entire eligible universe equal-weighted?" —
  // the no-skill baseline this strategy compares itself against. Per-
  // period return + chain-linked cumulative, computed over the SAME
  // entry→exit window the strategy used. Null when no eligible company
  // had usable prices for the window OR on legacy results predating
  // this column.
  universe_return_pct?: number | null;
  universe_cumulative_return_pct?: number | null;
  // Number of companies that actually contributed to universe_return_pct
  // this period (signals minus missing-price drops). Diagnostic only.
  universe_constituents?: number | null;
  // Book-level exposure multiplier applied this period by the vol-target
  // and/or regime overlays (1.0 = fully invested). Absent when no overlay
  // scaled this period (back-compat: treat missing as 1.0).
  exposure_scale?: number;
  // Composite 0..1 market-health score that drove the regime filter this
  // period (trend + 6-mo momentum + drawdown breadth). Present only when
  // the regime filter was active; used to chart the raw signal.
  market_health?: number | null;
  // Per-component breakdown of the health score (each 0..1), present
  // alongside market_health. Lets the Regime Detector chart which
  // sub-signal leads a crisis.
  market_health_components?: {
    trend?: number;
    momentum?: number;
    drawdown?: number;
    composite?: number;
  } | null;
  // Average RSI(14) across the universe this period, two ways (each
  // 0..100) — a momentum-breadth gauge charted separately on the Regime
  // Detector. `simple` = SMA-style; `wilder` = Wilder's smoothing.
  universe_rsi?: { simple?: number; wilder?: number } | null;
  // Daily cash<->stocks swaps the tit-for-tat overlay made this period
  // (each = one full-book trade). The fee model charges these at the held
  // book's per-exchange fees. Absent/0 when timing is off.
  daily_timing_swaps?: number;
};

export type DrawdownPeriod = {
  drawdown_pct: number;
  peak_date: string;
  trough_date: string;
  recovery_date: string | null;
};

export type Summary = {
  total_return_pct: number;
  annualized_return_pct: number;
  max_drawdown_pct: number;
  sharpe_ratio: number | null;
  /** Sortino: like Sharpe but only penalizes downside vol (std of
   * negative daily returns × √252). Higher than Sharpe = upside vol
   * dominates the variance. Null on degenerate runs / pre-feature
   * saved results. */
  sortino_ratio?: number | null;
  /** % of closed periods with strictly positive return. Combined with
   * median_period_return_pct, distinguishes "many small wins" from
   * "a few outlier months" strategies. */
  win_rate_pct?: number | null;
  median_period_return_pct?: number | null;
  avg_monthly_turnover_pct: number;
  total_months: number;
  avg_holdings: number;
  top_drawdowns?: DrawdownPeriod[];
  // Universe (equal-weighted-everything) baseline. The "did we
  // outperform?" reference. Chain-linked over closed periods using
  // the same entry/exit windows as the strategy. Null on degenerate
  // runs and on saved results from before this feature.
  universe_total_return_pct?: number | null;
  universe_annualized_return_pct?: number | null;
  // Populated only for multi-trial random backtests; headline stats above
  // are then means and these are the cross-trial std-devs.
  n_trials?: number | null;
  total_return_pct_std?: number | null;
  annualized_return_pct_std?: number | null;
  max_drawdown_pct_std?: number | null;
  sharpe_ratio_std?: number | null;
  avg_monthly_turnover_pct_std?: number | null;
};

export type DailyRecord = {
  date: string;                      // YYYY-MM-DD
  cumulative_return_pct: number;
};

export type BacktestResult = {
  monthly_records: PeriodRecord[];
  summary: Summary;
  // Daily portfolio equity curve, chain-linked across rebalances. The
  // equity curve chart, max-drawdown overlays, and Sharpe all derive from
  // this. Empty for degenerate runs / older saved results — the chart
  // falls back to monthly_records in that case.
  daily_records?: DailyRecord[];
  // Daily universe equal-weight baseline curve. Same shape as
  // `daily_records` but built from every eligible cid in each period
  // (the no-skill baseline) rather than the strategy's picks. The chart
  // prefers this over the per-period `universe_cumulative_return_pct`
  // on monthly_records so the gray baseline line matches the strategy
  // line's daily granularity. Absent on legacy saved runs predating
  // this feature.
  universe_daily_records?: DailyRecord[];
};

export type UniverseEntry = {
  company_id: number;
  ticker: string;
  exchange: string;
  company_name: string;
  sector: string;
  country?: string;  // populated by the backend snapshot; absent on older saves
};

export type ProgressEntry = { pct: number; message: string; t: number };
export type WarningEntry = { scope: string; message: string; id?: string };
export type InfoEntry = { scope: string; message: string };

// Phase 3 — variants. The "Run variants" button fans out to a fixed
// cross-product of rebalance frequencies × strategy types so the user can
// compare them side-by-side. Variants are run sequentially against the
// same base config (signal/category weights, sectors, top-N, universe).
// Derived from the wire shape — `VariantSpec` carries the canonical
// frequency + strategy enums and both are required there, so we use it
// (instead of the optional fields on `BacktestRequest`) to avoid
// `NonNullable<...>` gymnastics.
export type RebalanceFrequency = ApiVariantSpec['frequency'];
export type StrategyType = ApiVariantSpec['strategy_type'];
// Loose enough to accept both legacy 2-segment keys
// (`monthly__long_only`) and the cross-product sweep's extended form
// (`monthly__long_only__s4__p6__m30__uACWI__gsector`). The parser /
// builder below are the canonical encoders.
export type VariantKey = string;
export type VariantDef = {
  key: VariantKey;
  frequency: RebalanceFrequency;
  strategy: StrategyType;
  label: string;          // human-readable, e.g. "Monthly · Long-only"
};

/** All possible per-variant axes — five total: frequency, strategy,
 *  top_n_sectors, top_n_per_sector, min_price_score, universe, grouping.
 *  Any field left `undefined` means "inherit from the base request"
 *  (matches the backend `VariantSpec` field defaults). `min_price_score`
 *  uses `null` for "explicitly off" so the user can disable the filter
 *  for specific variants while leaving it on for the base. */
export type VariantParams = {
  frequency: RebalanceFrequency;
  strategy: StrategyType;
  top_n_sectors?: number;
  top_n_per_sector?: number;
  min_price_score?: number | null;
  universe?: string;
  grouping?: 'sector' | 'industry';
  // Weekday each rebalance lands on (0=Mon..6=Sun). Undefined inherits the
  // base request's rebalance weekday; set it to sweep first-Monday vs
  // first-Wednesday etc. Signals are computed strict-`<` the rebalance
  // date, so a Wednesday variant decides on Tuesday's close.
  rebalance_weekday?: number;
  // Annualized volatility target (percent, e.g. 12). Undefined = no vol
  // targeting (the original momentum strategy, fully invested); a number
  // makes this a distinct vol-targeted variant that scales exposure
  // toward the target each rebalance (de-risk only, holds cash in
  // turbulent regimes). Off and on appear as separate rows in the table.
  vol_target?: number;
  // Market-regime trend filter: risk-off exposure floor (0 = all cash,
  // 0.5 = half) applied when universe breadth (% above 200-MA) drops below
  // the base threshold. Undefined = no filter (original strategy). 0 is a
  // valid floor, so callers must guard with `!= null`, not truthiness.
  regime_floor?: number;
  // Daily "tit-for-tat" timing overlay: hold the strategy today only if
  // yesterday's daily return was >= 0, else cash. Undefined/false = off.
  daily_timing?: boolean;
};

/** Short weekday labels indexed 0=Mon..6=Sun (Python `date.weekday()`).
 * Local copy to avoid a store→component import cycle with momentum/utils. */
const VARIANT_WEEKDAY_SHORT = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'] as const;

/** Encode a VariantParams into the canonical sweep key. The backend
 * mirrors this exact ordering in `variants.py`. Legacy 2-axis variants
 * still produce `${frequency}__${strategy}` so saved bundles keep their
 * keys. */
export function makeVariantKey(p: VariantParams): VariantKey {
  const parts: string[] = [p.frequency, p.strategy];
  if (p.top_n_sectors != null) parts.push(`s${p.top_n_sectors}`);
  if (p.top_n_per_sector != null) parts.push(`p${p.top_n_per_sector}`);
  if (p.min_price_score != null) parts.push(`m${p.min_price_score}`);
  if (p.universe != null) parts.push(`u${p.universe}`);
  if (p.grouping != null) parts.push(`g${p.grouping}`);
  if (p.rebalance_weekday != null) parts.push(`w${p.rebalance_weekday}`);
  if (p.vol_target != null) parts.push(`v${p.vol_target}`);
  if (p.regime_floor != null) parts.push(`r${p.regime_floor}`);
  if (p.daily_timing) parts.push('t1');
  return parts.join('__');
}

/** Round-trip inverse of `makeVariantKey`. Returns null for malformed
 * input. The first two segments are frequency / strategy; subsequent
 * segments are tag-prefixed (s/p/m/u/g). Unknown tags pass through
 * silently — the table just won't display them. */
export function parseVariantKey(key: VariantKey): VariantParams | null {
  const parts = key.split('__');
  if (parts.length < 2) return null;
  const out: VariantParams = {
    frequency: parts[0] as RebalanceFrequency,
    strategy: parts[1] as StrategyType,
  };
  for (let i = 2; i < parts.length; i++) {
    const seg = parts[i];
    const tag = seg[0];
    const rest = seg.slice(1);
    if (tag === 's') { const n = Number(rest); if (Number.isFinite(n)) out.top_n_sectors = n; }
    else if (tag === 'p') { const n = Number(rest); if (Number.isFinite(n)) out.top_n_per_sector = n; }
    else if (tag === 'm') { const n = Number(rest); if (Number.isFinite(n)) out.min_price_score = n; }
    else if (tag === 'u') { out.universe = rest; }
    else if (tag === 'g') { if (rest === 'sector' || rest === 'industry') out.grouping = rest; }
    else if (tag === 'w') { const n = Number(rest); if (Number.isFinite(n)) out.rebalance_weekday = n; }
    else if (tag === 'v') { const n = Number(rest); if (Number.isFinite(n)) out.vol_target = n; }
    else if (tag === 'r') { const n = Number(rest); if (Number.isFinite(n)) out.regime_floor = n; }
    else if (tag === 't') { out.daily_timing = rest === '1'; }
  }
  return out;
}

/** Human-readable label for a sweep variant. Used in the permutations
 * preview, variant tabs, and the variants table. Mirrors the legacy
 * VARIANT_DEFS labels for the 2-axis case so saved bundles still
 * render identically. */
export function variantLabel(p: VariantParams): string {
  const freqLabel = (
    p.frequency === 'daily' ? 'Daily' :
    p.frequency === 'weekly' ? 'Weekly' :
    p.frequency === 'monthly' ? 'Monthly' :
    p.frequency.replace(/^every_(\d+)_months$/, 'Every $1 months')
  );
  const stratLabel = p.strategy === 'long_only' ? 'Long-only' : 'Long-short';
  const parts: string[] = [freqLabel, stratLabel];
  if (p.universe != null) parts.push(p.universe);
  if (p.grouping != null) parts.push(p.grouping === 'industry' ? 'by industry' : 'by sector');
  const bucketSingular = p.grouping === 'industry' ? 'industry' : 'sector';
  const bucketPlural = p.grouping === 'industry' ? 'industries' : 'sectors';
  if (p.top_n_sectors != null) parts.push(`top ${p.top_n_sectors} ${p.top_n_sectors === 1 ? bucketSingular : bucketPlural}`);
  if (p.top_n_per_sector != null) parts.push(`${p.top_n_per_sector} per ${bucketSingular}`);
  if (p.min_price_score != null) parts.push(`min ${p.min_price_score}`);
  if (p.rebalance_weekday != null) parts.push(`${VARIANT_WEEKDAY_SHORT[p.rebalance_weekday] ?? `wd${p.rebalance_weekday}`} rebal`);
  if (p.vol_target != null) parts.push(`vol ${p.vol_target}%`);
  if (p.regime_floor != null) parts.push(`regime ${p.regime_floor === 0 ? '→cash' : `→${p.regime_floor}×`}`);
  if (p.daily_timing) parts.push('tit-for-tat');
  return parts.join(' · ');
}

// Insertion order = display order in tables and tabs. Long-only group comes
// first (largest period → smallest), then the long-short group in the same
// frequency order. Reads as "all the directional bets first, then their
// market-neutral counterparts" with rebalance cost decreasing down each
// group.
export const VARIANT_DEFS: readonly VariantDef[] = [
  { key: 'every_12_months__long_only', frequency: 'every_12_months', strategy: 'long_only',  label: 'Every 12 months · Long-only' },
  { key: 'every_11_months__long_only', frequency: 'every_11_months', strategy: 'long_only',  label: 'Every 11 months · Long-only' },
  { key: 'every_10_months__long_only', frequency: 'every_10_months', strategy: 'long_only',  label: 'Every 10 months · Long-only' },
  { key: 'every_9_months__long_only',  frequency: 'every_9_months',  strategy: 'long_only',  label: 'Every 9 months · Long-only'  },
  { key: 'every_8_months__long_only',  frequency: 'every_8_months',  strategy: 'long_only',  label: 'Every 8 months · Long-only'  },
  { key: 'every_7_months__long_only',  frequency: 'every_7_months',  strategy: 'long_only',  label: 'Every 7 months · Long-only'  },
  { key: 'every_6_months__long_only',  frequency: 'every_6_months',  strategy: 'long_only',  label: 'Every 6 months · Long-only'  },
  { key: 'every_5_months__long_only',  frequency: 'every_5_months',  strategy: 'long_only',  label: 'Every 5 months · Long-only'  },
  { key: 'every_4_months__long_only',  frequency: 'every_4_months',  strategy: 'long_only',  label: 'Every 4 months · Long-only'  },
  { key: 'every_3_months__long_only',  frequency: 'every_3_months',  strategy: 'long_only',  label: 'Every 3 months · Long-only'  },
  { key: 'every_2_months__long_only',  frequency: 'every_2_months',  strategy: 'long_only',  label: 'Every 2 months · Long-only'  },
  { key: 'monthly__long_only',         frequency: 'monthly',         strategy: 'long_only',  label: 'Monthly · Long-only'         },
  { key: 'weekly__long_only',          frequency: 'weekly',          strategy: 'long_only',  label: 'Weekly · Long-only'          },
  { key: 'daily__long_only',           frequency: 'daily',           strategy: 'long_only',  label: 'Daily · Long-only'           },
  { key: 'every_12_months__long_short', frequency: 'every_12_months', strategy: 'long_short', label: 'Every 12 months · Long-short' },
  { key: 'every_11_months__long_short', frequency: 'every_11_months', strategy: 'long_short', label: 'Every 11 months · Long-short' },
  { key: 'every_10_months__long_short', frequency: 'every_10_months', strategy: 'long_short', label: 'Every 10 months · Long-short' },
  { key: 'every_9_months__long_short',  frequency: 'every_9_months',  strategy: 'long_short', label: 'Every 9 months · Long-short'  },
  { key: 'every_8_months__long_short',  frequency: 'every_8_months',  strategy: 'long_short', label: 'Every 8 months · Long-short'  },
  { key: 'every_7_months__long_short',  frequency: 'every_7_months',  strategy: 'long_short', label: 'Every 7 months · Long-short'  },
  { key: 'every_6_months__long_short',  frequency: 'every_6_months',  strategy: 'long_short', label: 'Every 6 months · Long-short'  },
  { key: 'every_5_months__long_short',  frequency: 'every_5_months',  strategy: 'long_short', label: 'Every 5 months · Long-short'  },
  { key: 'every_4_months__long_short',  frequency: 'every_4_months',  strategy: 'long_short', label: 'Every 4 months · Long-short'  },
  { key: 'every_3_months__long_short',  frequency: 'every_3_months',  strategy: 'long_short', label: 'Every 3 months · Long-short'  },
  { key: 'every_2_months__long_short',  frequency: 'every_2_months',  strategy: 'long_short', label: 'Every 2 months · Long-short'  },
  { key: 'monthly__long_short',         frequency: 'monthly',         strategy: 'long_short', label: 'Monthly · Long-short'         },
  { key: 'weekly__long_short',          frequency: 'weekly',          strategy: 'long_short', label: 'Weekly · Long-short'          },
  { key: 'daily__long_short',           frequency: 'daily',           strategy: 'long_short', label: 'Daily · Long-short'           },
];

export type VariantOutcome =
  | { status: 'pending' }
  | { status: 'running' }
  | { status: 'ok'; result: BacktestResult }
  | { status: 'error'; message: string }
  | { status: 'cancelled' };

export type VariantsRunState = {
  /** Currently-executing variant key, or null between runs. */
  current: VariantKey | null;
  /** How many variants have a terminal outcome (ok or error). */
  completed: number;
  /** Total variants in the current sweep (= VARIANT_DEFS.length). */
  total: number;
  /** ms-since-epoch when the sweep started. */
  startedAt: number;
};

export type MomentumState = {
  running: boolean;
  progress: ProgressEntry[];
  result: BacktestResult | null;
  universe: UniverseEntry[];
  error: string | null;
  warnings: WarningEntry[];
  infos: InfoEntry[];
  loadedRunId: number | null;
  /** ms-since-epoch when the current/last run started, or null if no run has begun. */
  runStartedAt: number | null;
  /** ms-since-epoch when the current/last run finished (success or error), or null while running. */
  runEndedAt: number | null;
  /** Per-variant outcomes from the most recent variants sweep. Empty until the
   * user clicks "Run variants". Cleared when a fresh sweep starts. */
  variants: Partial<Record<VariantKey, VariantOutcome>>;
  /** Which variant the detail views (equity curve, holdings, sector timeline)
   * are showing. Set by clicking a row in the summary table or a tab. */
  activeVariantKey: VariantKey | null;
  /** Sweep progress, or null when no sweep is in flight. */
  variantsRun: VariantsRunState | null;
};

/**
 * Wire shape for `POST /api/momentum/backtest`. Re-exported from the
 * generated `BacktestRequest` so the frontend can't drift from the
 * Pydantic model — see `lib/types/api.ts` for the pipeline and
 * `backend/openapi.json` for the source of truth.
 *
 * Field semantics (kept here because they're frontend-relevant context
 * the generated types don't carry):
 *   - `min_price_score`: 0-100 floor on each candidate's `score_price`;
 *     the long bucket only picks companies whose price-category score
 *     strictly exceeds it. Null disables.
 *   - `grouping`: 'sector' (default) works for every universe;
 *     'industry' only works for Leonteq-derived universes.
 *   - `sector_etfs`: {sector: benchmark_id}, required when
 *     `selection_mode === 'sector_etf'`.
 *   - `db_only`: true (backend default) skips GuruFocus / ECB. The
 *     Recompute button passes false to refresh upstream data.
 *   - `variants`: when set, the request becomes a sweep — backend
 *     loads data once and runs the backtest per variant, streaming a
 *     separate `variant_result` event per cross-product entry.
 */
export type BacktestStartConfig = ApiBacktestRequest;

export const momentumStore = createStore<MomentumState>({
  running: false,
  progress: [],
  result: null,
  universe: [],
  error: null,
  warnings: [],
  infos: [],
  loadedRunId: null,
  runStartedAt: null,
  runEndedAt: null,
  variants: {},
  activeVariantKey: null,
  variantsRun: null,
});

// ─── Variants sweep ──────────────────────────────────────────────────────
//
// `startVariantsBacktest` fires ONE SSE call to `/api/momentum/backtest` with
// `variants: [...]` set. The backend loads the universe / prices / volumes /
// FX once, then runs the backtest computation per variant against the same
// in-memory frames, streaming `variant_start` / `variant_result` /
// `variant_error` events as each completes. Per-variant progress messages
// are prefixed with the variant key by the backend so the shared
// ProgressTimeline reads naturally.

let variantsAbortController: AbortController | null = null;

/** Run the selected variants against the given base config. `keys` defaults
 * to every variant in `VARIANT_DEFS`. The backend handles the sequencing —
 * frontend just dispatches each `variant_*` event into the store.
 *
 * Caller is responsible for filtering out incompatible variants — the
 * backend rejects `long_short` + `random` (long-short selection without
 * a signal-driven score is meaningless), so the UI must not pass those
 * pairs in `keys` when `base.selection_mode === 'random'`.
 *
 * Sweeps drive the same `running` / `progress` / `runStartedAt` fields as
 * a single backtest so the existing ProgressTimeline lights up while the
 * sweep is in flight. */
export async function startVariantsBacktest(
  base: Omit<BacktestStartConfig, 'rebalance_frequency' | 'strategy_type' | 'mode' | 'variants'>,
  variants: VariantParams[],
): Promise<void> {
  variantsAbortController?.abort();
  const controller = new AbortController();
  variantsAbortController = controller;

  const targets = variants;
  if (targets.length === 0) return;

  // Reset sweep state. Only pre-seed variants we plan to run — un-selected
  // keys stay absent from `variants` so the summary table doesn't render
  // ghost rows for them.
  const initial: Partial<Record<VariantKey, VariantOutcome>> = {};
  for (const v of targets) initial[makeVariantKey(v)] = { status: 'pending' };
  const startedAt = Date.now();
  momentumStore.set({
    variants: initial,
    activeVariantKey: null,
    variantsRun: {
      current: null,
      completed: 0,
      total: targets.length,
      startedAt,
    },
    // Drive the shared progress UI as if this were a single long run.
    running: true,
    progress: [],
    error: null,
    warnings: [],
    infos: [],
    runStartedAt: startedAt,
    runEndedAt: null,
  });

  const cfg: BacktestStartConfig = {
    ...base,
    variants: targets.map((v) => ({
      frequency: v.frequency,
      strategy_type: v.strategy,
      ...(v.top_n_sectors != null ? { top_n_sectors: v.top_n_sectors } : {}),
      ...(v.top_n_per_sector != null ? { top_n_per_sector: v.top_n_per_sector } : {}),
      ...(v.min_price_score != null ? { min_price_score: v.min_price_score } : {}),
      ...(v.universe != null ? { index_universe: v.universe } : {}),
      ...(v.grouping != null ? { grouping: v.grouping } : {}),
      ...(v.rebalance_weekday != null ? { rebalance_weekday: v.rebalance_weekday } : {}),
      ...(v.vol_target != null ? { vol_target: v.vol_target } : {}),
      ...(v.regime_floor != null ? { regime_floor: v.regime_floor } : {}),
      ...(v.daily_timing ? { daily_timing: true } : {}),
    })),
  };

  let aborted = false;
  let topLevelError: string | null = null;

  try {
    await runSSE(
      `${API_URL}/api/momentum/backtest`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cfg),
      },
      (raw) => {
        const data = raw as {
          type: string;
          pct?: number;
          message?: string;
          scope?: string;
          variant_key?: string;
          data?: BacktestResult;
        };
        if (data.type === 'progress') {
          momentumStore.set((s) => ({
            progress: [...s.progress, {
              pct: data.pct ?? 0,
              message: data.message ?? '',
              t: Date.now(),
            }],
          }));
        } else if (data.type === 'warning') {
          momentumStore.set((s) => ({
            warnings: [...s.warnings, {
              scope: data.scope ?? 'backtest',
              message: data.message ?? '',
            }],
          }));
        } else if (data.type === 'info') {
          momentumStore.set((s) => ({
            infos: [...s.infos, {
              scope: data.scope ?? 'backtest',
              message: data.message ?? '',
            }],
          }));
        } else if (data.type === 'variant_start' && data.variant_key) {
          const key = data.variant_key as VariantKey;
          momentumStore.set((s) => ({
            variants: { ...s.variants, [key]: { status: 'running' } },
            variantsRun: s.variantsRun
              ? { ...s.variantsRun, current: key }
              : null,
          }));
        } else if (data.type === 'variant_result' && data.variant_key) {
          const key = data.variant_key as VariantKey;
          const result = (data.data as BacktestResult) ?? null;
          if (result) {
            momentumStore.set((s) => {
              // Auto-select the first variant that completes so detail
              // views have something to show without a click.
              const nextActive = s.activeVariantKey ?? key;
              return {
                variants: { ...s.variants, [key]: { status: 'ok', result } },
                activeVariantKey: nextActive,
                variantsRun: s.variantsRun
                  ? { ...s.variantsRun, completed: s.variantsRun.completed + 1 }
                  : null,
              };
            });
          }
        } else if (data.type === 'variant_error' && data.variant_key) {
          const key = data.variant_key as VariantKey;
          const msg = data.message ?? 'Unknown error';
          momentumStore.set((s) => ({
            variants: { ...s.variants, [key]: { status: 'error', message: msg } },
            variantsRun: s.variantsRun
              ? { ...s.variantsRun, completed: s.variantsRun.completed + 1 }
              : null,
          }));
        } else if (data.type === 'error') {
          topLevelError = data.message ?? 'Unknown error';
        }
        // Ignore done — the sweep tracks its own completion.
      },
      controller.signal,
    );
  } catch (e) {
    if (controller.signal.aborted || (e as { name?: string })?.name === 'AbortError') {
      aborted = true;
    } else {
      topLevelError = e instanceof Error ? e.message : String(e);
    }
  }

  if (topLevelError) {
    // Pipeline-level failure (data load, FX sync, etc.) — every variant
    // that hadn't already errored out gets the failure as its message so
    // the table doesn't leave them spinning.
    momentumStore.set((s) => {
      const next = { ...s.variants };
      for (const v of targets) {
        const k = makeVariantKey(v);
        const cur = next[k];
        if (cur?.status === 'running' || cur?.status === 'pending') {
          next[k] = { status: 'error', message: topLevelError! };
        }
      }
      return { variants: next, error: topLevelError };
    });
  } else if (aborted) {
    // User-cancelled the sweep — mark the in-flight + still-pending
    // variants as cancelled so the table doesn't leave a spinner running.
    momentumStore.set((s) => {
      const next = { ...s.variants };
      for (const v of targets) {
        const k = makeVariantKey(v);
        const cur = next[k];
        if (cur?.status === 'running' || cur?.status === 'pending') {
          next[k] = { status: 'cancelled' };
        }
      }
      return { variants: next };
    });
  }

  momentumStore.set((s) => ({
    variantsRun: s.variantsRun ? { ...s.variantsRun, current: null } : null,
    running: false,
    runEndedAt: Date.now(),
  }));
  if (variantsAbortController === controller) variantsAbortController = null;
}

export function cancelVariantsBacktest(): void {
  variantsAbortController?.abort();
  // Don't null out `variantsAbortController` here — `startVariantsBacktest`
  // is still inside its loop and will clear it from the `finally` path
  // once the AbortError unwinds. Resetting `current` here only flips the
  // sweep header out of "running"; the loop's post-abort cleanup handles
  // the per-row 'cancelled' transitions.
  momentumStore.set((s) => ({
    variantsRun: s.variantsRun ? { ...s.variantsRun, current: null } : null,
  }));
}

export function setActiveVariant(key: VariantKey | null): void {
  momentumStore.set({ activeVariantKey: key });
}

/** Wipe variant state. Call this from the single-variant "Run Backtest"
 * path so the variants UI doesn't linger as stale rows after the user
 * switches back to single-run mode. */
export function clearVariants(): void {
  momentumStore.set({ variants: {}, activeVariantKey: null, variantsRun: null });
}
