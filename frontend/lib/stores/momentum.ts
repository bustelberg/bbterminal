import { createStore } from '../store';
import { runSSE } from '../stream';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

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
};

export type PeriodRecord = {
  date: string;
  holdings: Holding[];
  portfolio_return_pct: number | null;
  cumulative_return_pct: number;
  empty_reason?: string;
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
  avg_monthly_turnover_pct: number;
  total_months: number;
  avg_holdings: number;
  top_drawdowns?: DrawdownPeriod[];
  // Populated only for multi-trial random backtests; headline stats above
  // are then means and these are the cross-trial std-devs.
  n_trials?: number | null;
  total_return_pct_std?: number | null;
  annualized_return_pct_std?: number | null;
  max_drawdown_pct_std?: number | null;
  sharpe_ratio_std?: number | null;
  avg_monthly_turnover_pct_std?: number | null;
};

export type BacktestResult = {
  monthly_records: PeriodRecord[];
  summary: Summary;
};

// Per-day pick holding has the same shape as a Holding for newer snapshots
// (price/return fields populated). Older snapshots persist a slim version
// where the price/return/category_scores fields are absent — all extra
// fields are therefore optional.
export type DailyPickHolding = {
  company_id: number;
  ticker: string;
  company_name: string;
  sector: string;
  score: number;
  category_scores?: Record<string, number | null>;
  weight?: number;
  forward_return_pct?: number | null;
  currency?: string | null;
  entry_price_local?: number | null;
  exit_price_local?: number | null;
  entry_price_eur?: number | null;
  exit_price_eur?: number | null;
  entry_date?: string | null;
  exit_date?: string | null;
};

export type DailyPick = {
  date: string;                   // YYYY-MM-DD
  holdings: DailyPickHolding[];
  turnover_abs: number;           // # stocks added (= removed for fixed-size) vs previous day
  turnover_pct: number;           // turnover_abs / portfolio_size * 100
  portfolio_return_pct?: number | null;  // chain-linked cumulative MTD through this day
  next_day_return_pct?: number | null;   // 1-day forward return of THIS day's portfolio (null on the latest day)
};

export type CurrentPortfolio = {
  as_of_date: string;             // YYYY-MM-01 — start of current month
  latest_price_date: string | null; // most recent observed price across holdings
  holdings: Holding[];
  daily_picks?: DailyPick[];      // current-month days produced by the most recent compute
  daily_picks_history?: DailyPick[]; // all stored days for this strategy across months
  snapshot_id?: number;           // present when loaded from DB (or persisted post-compute)
  strategy_hash?: string;
  from_cache?: boolean;           // true when the backend served from cache
};

export type CurrentPicksSnapshotMeta = {
  snapshot_id: number;
  created_at: string;             // ISO timestamp
  triggered_by: 'auto' | 'manual';
  as_of_date: string;
  latest_price_date: string | null;
};

export type UniverseEntry = {
  company_id: number;
  ticker: string;
  exchange: string;
  company_name: string;
  sector: string;
};

export type ProgressEntry = { pct: number; message: string; t: number };
export type WarningEntry = { scope: string; message: string };
export type InfoEntry = { scope: string; message: string };

// Phase 3 — variants. The "Run variants" button fans out to a fixed
// cross-product of rebalance frequencies × strategy types so the user can
// compare them side-by-side. Variants are run sequentially against the
// same base config (signal/category weights, sectors, top-N, universe).
export type RebalanceFrequency =
  | 'daily' | 'weekly' | 'monthly' | 'every_2_months' | 'every_3_months';
export type StrategyType = 'long_only' | 'long_short';
export type VariantKey = `${RebalanceFrequency}__${StrategyType}`;
export type VariantDef = {
  key: VariantKey;
  frequency: RebalanceFrequency;
  strategy: StrategyType;
  label: string;          // human-readable, e.g. "Monthly · Long-only"
};

// Insertion order = display order in tables and tabs. Frequencies cluster
// together so you can read "monthly long-only vs monthly long-short" as
// adjacent rows; strategies alternate within each frequency.
export const VARIANT_DEFS: readonly VariantDef[] = [
  { key: 'monthly__long_only',         frequency: 'monthly',         strategy: 'long_only',  label: 'Monthly · Long-only'        },
  { key: 'monthly__long_short',        frequency: 'monthly',         strategy: 'long_short', label: 'Monthly · Long-short'       },
  { key: 'every_2_months__long_only',  frequency: 'every_2_months',  strategy: 'long_only',  label: 'Every 2 months · Long-only' },
  { key: 'every_2_months__long_short', frequency: 'every_2_months',  strategy: 'long_short', label: 'Every 2 months · Long-short'},
  { key: 'every_3_months__long_only',  frequency: 'every_3_months',  strategy: 'long_only',  label: 'Every 3 months · Long-only' },
  { key: 'every_3_months__long_short', frequency: 'every_3_months',  strategy: 'long_short', label: 'Every 3 months · Long-short'},
  { key: 'weekly__long_only',          frequency: 'weekly',          strategy: 'long_only',  label: 'Weekly · Long-only'         },
  { key: 'weekly__long_short',         frequency: 'weekly',          strategy: 'long_short', label: 'Weekly · Long-short'        },
  { key: 'daily__long_only',           frequency: 'daily',           strategy: 'long_only',  label: 'Daily · Long-only'          },
  { key: 'daily__long_short',          frequency: 'daily',           strategy: 'long_short', label: 'Daily · Long-short'         },
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
  currentPortfolio: CurrentPortfolio | null;
  currentPicksSnapshots: CurrentPicksSnapshotMeta[];
  refreshingMTD: boolean;
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

export type BacktestStartConfig = {
  start_date: string;
  end_date: string;
  signal_weights: Record<string, number>;
  category_weights: Record<string, number>;
  top_n_sectors: number;
  top_n_per_sector: number;
  max_companies: number;
  universe_label: string | null;
  index_universe: string | null;
  selection_mode: 'momentum' | 'random';
  random_seed: number | null;
  n_trials: number;
  mode?: 'backtest' | 'current_portfolio';
  force_recompute?: boolean;
  // Optional variant axes — backend defaults match Phase 1/2 (long-only,
  // monthly). Variant sweep populates both per request.
  rebalance_frequency?: RebalanceFrequency;
  strategy_type?: StrategyType;
};

export const momentumStore = createStore<MomentumState>({
  running: false,
  progress: [],
  result: null,
  currentPortfolio: null,
  currentPicksSnapshots: [],
  refreshingMTD: false,
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

let abortController: AbortController | null = null;

export async function startBacktest(cfg: BacktestStartConfig): Promise<void> {
  abortController?.abort();
  const controller = new AbortController();
  abortController = controller;

  momentumStore.set({
    running: true,
    progress: [],
    result: cfg.mode === 'current_portfolio' ? momentumStore.get().result : null,
    currentPortfolio: cfg.mode === 'current_portfolio' ? null : momentumStore.get().currentPortfolio,
    universe: [],
    error: null,
    warnings: [],
    infos: [],
    loadedRunId: null,
    runStartedAt: Date.now(),
    runEndedAt: null,
  });

  let receivedDone = false;
  let receivedResult = false;
  let lastEventTime = Date.now();

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
          data?: BacktestResult | CurrentPortfolio;
          universe?: UniverseEntry[];
        };
        lastEventTime = Date.now();
        if (data.type === 'progress') {
          momentumStore.set((s) => ({
            progress: [...s.progress, { pct: data.pct ?? 0, message: data.message ?? '', t: Date.now() }],
          }));
        } else if (data.type === 'warning') {
          momentumStore.set((s) => ({
            warnings: [...s.warnings, { scope: data.scope ?? 'backtest', message: data.message ?? '' }],
          }));
        } else if (data.type === 'info') {
          momentumStore.set((s) => ({
            infos: [...s.infos, { scope: data.scope ?? 'backtest', message: data.message ?? '' }],
          }));
        } else if (data.type === 'result') {
          receivedResult = true;
          momentumStore.set({ result: (data.data as BacktestResult) ?? null });
          if (data.universe) momentumStore.set({ universe: data.universe });
        } else if (data.type === 'current_portfolio') {
          receivedResult = true;
          momentumStore.set({ currentPortfolio: (data.data as CurrentPortfolio) ?? null });
          if (data.universe) momentumStore.set({ universe: data.universe });
        } else if (data.type === 'done') {
          receivedDone = true;
          momentumStore.set({ running: false, runEndedAt: Date.now() });
          if (cfg.mode === 'current_portfolio') {
            maybeAutoRefreshCurrentMonthMTD(momentumStore.get().currentPortfolio);
          }
        } else if (data.type === 'error') {
          momentumStore.set({ error: data.message ?? 'Unknown error', running: false, runEndedAt: Date.now() });
        }
      },
      controller.signal,
    );

    if (!receivedDone) {
      if (receivedResult) {
        momentumStore.set({ running: false, runEndedAt: Date.now() });
      } else {
        const elapsed = Math.round((Date.now() - lastEventTime) / 1000);
        momentumStore.set({
          error: `Stream disconnected unexpectedly (last event ${elapsed}s ago). This can happen due to proxy timeouts — try again, the replay cache should make the second attempt fast.`,
          running: false,
          runEndedAt: Date.now(),
        });
      }
    }
  } catch (e) {
    // Chrome can surface an aborted fetch as `TypeError: Failed to
    // fetch` rather than a DOMException with name=AbortError. Trust the
    // signal state to distinguish a user cancel from a real failure.
    const isCancel =
      controller.signal.aborted || (e as { name?: string })?.name === 'AbortError';
    if (!isCancel) {
      momentumStore.set({ error: e instanceof Error ? e.message : 'Unknown error' });
    }
    momentumStore.set({ running: false, runEndedAt: Date.now() });
  } finally {
    if (abortController === controller) abortController = null;
  }
}

export function cancelBacktest(): void {
  abortController?.abort();
  abortController = null;
  momentumStore.set({ running: false });
}

// ─── Variants sweep ──────────────────────────────────────────────────────
//
// `startVariantsBacktest` fans out the user's base config over the
// `VARIANT_DEFS` cross-product (5 frequencies × 2 strategy types). Each
// variant is its own SSE call to the same endpoint; the backend's
// strategy_hash includes both axes, so cache hits for repeat sweeps are
// cheap. Sequential, not parallel — to keep API-load predictable and
// because cache hits make it fast in steady state.

let variantsAbortController: AbortController | null = null;

/** Drain one backtest SSE call to completion, returning the BacktestResult.
 * Throws on error or abort. Forwards progress / warning / info events into
 * the shared momentumStore (prefixed with `variantLabel`) so the existing
 * ProgressTimeline panel reflects what's happening for the in-flight
 * variant. */
async function _runOneVariant(
  cfg: BacktestStartConfig,
  variantLabel: string,
  signal: AbortSignal,
): Promise<BacktestResult> {
  let result: BacktestResult | null = null;
  let errorMsg: string | null = null;
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
        data?: BacktestResult;
      };
      if (data.type === 'progress') {
        momentumStore.set((s) => ({
          progress: [...s.progress, {
            pct: data.pct ?? 0,
            message: `[${variantLabel}] ${data.message ?? ''}`,
            t: Date.now(),
          }],
        }));
      } else if (data.type === 'warning') {
        momentumStore.set((s) => ({
          warnings: [...s.warnings, {
            scope: data.scope ?? 'backtest',
            message: `[${variantLabel}] ${data.message ?? ''}`,
          }],
        }));
      } else if (data.type === 'info') {
        momentumStore.set((s) => ({
          infos: [...s.infos, {
            scope: data.scope ?? 'backtest',
            message: `[${variantLabel}] ${data.message ?? ''}`,
          }],
        }));
      } else if (data.type === 'result') {
        result = (data.data as BacktestResult) ?? null;
      } else if (data.type === 'error') {
        errorMsg = data.message ?? 'Unknown error';
      }
      // Ignore done — the sweep tracks its own completion.
    },
    signal,
  );
  if (errorMsg) throw new Error(errorMsg);
  if (!result) throw new Error('Stream ended without a result');
  return result;
}

/** Run the selected variants sequentially against the given base config.
 * `keys` defaults to every variant in `VARIANT_DEFS`. `selection_mode`,
 * `random_seed`, `n_trials`, and `force_recompute` carry over from `base`;
 * `rebalance_frequency` and `strategy_type` are overwritten per variant.
 *
 * Caller is responsible for filtering out incompatible variants — the
 * backend rejects `long_short` + `random` (long-short selection without
 * a signal-driven score is meaningless), so the UI must not pass those
 * pairs in `keys` when `base.selection_mode === 'random'`.
 *
 * Sweeps drive the same `running` / `progress` / `runStartedAt` fields as
 * a single backtest so the existing ProgressTimeline lights up while a
 * sweep is in flight; per-variant log lines are prefixed with the variant
 * label. */
export async function startVariantsBacktest(
  base: Omit<BacktestStartConfig, 'rebalance_frequency' | 'strategy_type' | 'mode'>,
  keys?: readonly VariantKey[],
): Promise<void> {
  variantsAbortController?.abort();
  const controller = new AbortController();
  variantsAbortController = controller;

  // Resolve which variants to run, preserving VARIANT_DEFS display order so
  // the summary table reads consistently.
  const selectedSet = new Set<VariantKey>(keys ?? VARIANT_DEFS.map((v) => v.key));
  const targets = VARIANT_DEFS.filter((v) => selectedSet.has(v.key));
  if (targets.length === 0) return;

  // Reset sweep state. Only pre-seed variants we plan to run — un-selected
  // keys stay absent from `variants` so the summary table doesn't render
  // ghost rows for them.
  const initial: Partial<Record<VariantKey, VariantOutcome>> = {};
  for (const v of targets) initial[v.key] = { status: 'pending' };
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

  let aborted = false;

  for (const variant of targets) {
    if (controller.signal.aborted) { aborted = true; break; }

    momentumStore.set((s) => ({
      variants: { ...s.variants, [variant.key]: { status: 'running' } },
      variantsRun: s.variantsRun
        ? { ...s.variantsRun, current: variant.key }
        : null,
    }));

    const cfg: BacktestStartConfig = {
      ...base,
      rebalance_frequency: variant.frequency,
      strategy_type: variant.strategy,
    };

    try {
      const result = await _runOneVariant(cfg, variant.label, controller.signal);
      momentumStore.set((s) => {
        // Auto-select the first variant that completes so detail views have
        // something to show without a click.
        const nextActive = s.activeVariantKey ?? variant.key;
        return {
          variants: { ...s.variants, [variant.key]: { status: 'ok', result } },
          activeVariantKey: nextActive,
          variantsRun: s.variantsRun
            ? { ...s.variantsRun, completed: s.variantsRun.completed + 1 }
            : null,
        };
      });
    } catch (e) {
      // Chrome can surface an aborted fetch as `TypeError: Failed to
      // fetch` rather than a DOMException with name=AbortError, so trust
      // the signal state — it's the only reliable cancellation signal.
      if (controller.signal.aborted || (e as { name?: string })?.name === 'AbortError') {
        aborted = true;
        break;
      }
      const msg = e instanceof Error ? e.message : String(e);
      momentumStore.set((s) => ({
        variants: { ...s.variants, [variant.key]: { status: 'error', message: msg } },
        variantsRun: s.variantsRun
          ? { ...s.variantsRun, completed: s.variantsRun.completed + 1 }
          : null,
      }));
    }
  }

  if (aborted) {
    // Mark the in-flight variant + every still-pending variant as
    // cancelled so the table doesn't leave a spinner spinning forever.
    momentumStore.set((s) => {
      const next = { ...s.variants };
      for (const v of targets) {
        const cur = next[v.key];
        if (cur?.status === 'running' || cur?.status === 'pending') {
          next[v.key] = { status: 'cancelled' };
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

// ─── Current Picks snapshots ─────────────────────────────────────────────

export async function loadCurrentPicksSnapshots(): Promise<void> {
  try {
    const resp = await fetch(`${API_URL}/api/momentum/current-picks`);
    if (!resp.ok) return;
    const rows = (await resp.json()) as CurrentPicksSnapshotMeta[];
    momentumStore.set({ currentPicksSnapshots: rows });
  } catch {
    // non-fatal — UI just won't show the snapshot picker
  }
}

export async function loadCurrentPicksSnapshot(snapshotId: number): Promise<void> {
  try {
    const resp = await fetch(`${API_URL}/api/momentum/current-picks/${snapshotId}`);
    if (!resp.ok) {
      momentumStore.set({ error: `Failed to load snapshot ${snapshotId}` });
      return;
    }
    const row = await resp.json();
    const cp: CurrentPortfolio = {
      snapshot_id: row.snapshot_id,
      as_of_date: row.as_of_date,
      latest_price_date: row.latest_price_date,
      holdings: row.holdings ?? [],
      daily_picks: row.daily_picks ?? [],
      daily_picks_history: row.daily_picks_history ?? row.daily_picks ?? [],
      strategy_hash: row.strategy_hash ?? undefined,
    };
    momentumStore.set({ currentPortfolio: cp, error: null });
    maybeAutoRefreshCurrentMonthMTD(cp);
  } catch (e) {
    momentumStore.set({ error: e instanceof Error ? e.message : 'Failed to load snapshot' });
  }
}

// Local YYYY-MM-DD — using local time so the staleness check matches the
// user's wall-clock day (avoids spurious "stale" reads near UTC midnight).
function todayIsoLocal(): string {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

// Fire-and-forget MTD refresh when a current-month snapshot's prices lag today.
// Past-month snapshots are read-only, so we never refresh them. If price data
// is already current (e.g. a fresh Recompute just finished), this is a no-op.
function maybeAutoRefreshCurrentMonthMTD(cp: CurrentPortfolio | null): void {
  if (!cp || cp.snapshot_id == null) return;
  const today = todayIsoLocal();
  if (cp.as_of_date.slice(0, 7) !== today.slice(0, 7)) return;
  if (cp.latest_price_date && cp.latest_price_date >= today) return;
  void refreshCurrentPicksMTD(cp.snapshot_id);
}

export async function refreshCurrentPicksMTD(snapshotId: number): Promise<void> {
  momentumStore.set({ refreshingMTD: true, error: null });
  try {
    const resp = await fetch(`${API_URL}/api/momentum/current-picks/${snapshotId}/refresh-mtd`, { method: 'POST' });
    if (!resp.ok) {
      momentumStore.set({ error: `MTD refresh failed (${resp.status})`, refreshingMTD: false });
      return;
    }
    const data = await resp.json();
    const cur = momentumStore.get().currentPortfolio;
    // Only update if the same snapshot is still loaded
    if (cur && cur.snapshot_id === snapshotId) {
      momentumStore.set({
        currentPortfolio: {
          ...cur,
          latest_price_date: data.latest_price_date ?? cur.latest_price_date,
          holdings: data.holdings ?? cur.holdings,
        },
      });
    }
  } catch (e) {
    momentumStore.set({ error: e instanceof Error ? e.message : 'MTD refresh failed' });
  } finally {
    momentumStore.set({ refreshingMTD: false });
  }
}
