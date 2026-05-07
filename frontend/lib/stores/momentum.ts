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
  // "long" or "short". Long-only backtests omit this (treated as long);
  // long-short backtests include it on every holding so the dashboard can
  // split sector breakdowns / equity contributions by side.
  side?: 'long' | 'short';
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
  name?: string | null;           // optional custom label set via rename
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

// Insertion order = display order in tables and tabs. Long-only group comes
// first (largest period → smallest), then the long-short group in the same
// frequency order. Reads as "all the directional bets first, then their
// market-neutral counterparts" with rebalance cost decreasing down each
// group.
export const VARIANT_DEFS: readonly VariantDef[] = [
  { key: 'every_3_months__long_only',  frequency: 'every_3_months',  strategy: 'long_only',  label: 'Every 3 months · Long-only' },
  { key: 'every_2_months__long_only',  frequency: 'every_2_months',  strategy: 'long_only',  label: 'Every 2 months · Long-only' },
  { key: 'monthly__long_only',         frequency: 'monthly',         strategy: 'long_only',  label: 'Monthly · Long-only'        },
  { key: 'weekly__long_only',          frequency: 'weekly',          strategy: 'long_only',  label: 'Weekly · Long-only'         },
  { key: 'daily__long_only',           frequency: 'daily',           strategy: 'long_only',  label: 'Daily · Long-only'          },
  { key: 'every_3_months__long_short', frequency: 'every_3_months',  strategy: 'long_short', label: 'Every 3 months · Long-short'},
  { key: 'every_2_months__long_short', frequency: 'every_2_months',  strategy: 'long_short', label: 'Every 2 months · Long-short'},
  { key: 'monthly__long_short',        frequency: 'monthly',         strategy: 'long_short', label: 'Monthly · Long-short'       },
  { key: 'weekly__long_short',         frequency: 'weekly',          strategy: 'long_short', label: 'Weekly · Long-short'        },
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
  /** Snapshot id currently being loaded from the current-picks dropdown, or
   * null when idle. Drives the per-row spinner in the header dropdown. */
  loadingSnapshotId: number | null;
  /** Snapshot id currently being deleted (so the row can show a spinner
   * + the rest of the dropdown stays interactive). */
  deletingSnapshotId: number | null;
  /** Snapshot id currently being renamed. */
  renamingSnapshotId: number | null;
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
  // When true (the backend default), the run uses only data already in
  // the DB — no GuruFocus / ECB calls. The Recompute button overrides
  // this to fetch fresh data when prices have fallen behind.
  db_only?: boolean;
  // Optional variant axes — backend defaults match Phase 1/2 (long-only,
  // monthly). Variant sweep populates both per request.
  rebalance_frequency?: RebalanceFrequency;
  strategy_type?: StrategyType;
  // When set, the request becomes a sweep: backend loads data once and
  // runs the backtest computation per variant, streaming a separate
  // `variant_result` event per (frequency × strategy_type). Replaces
  // the per-variant POST loop the frontend used to do.
  variants?: { frequency: RebalanceFrequency; strategy_type: StrategyType }[];
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
  loadingSnapshotId: null,
  deletingSnapshotId: null,
  renamingSnapshotId: null,
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

  const cfg: BacktestStartConfig = {
    ...base,
    variants: targets.map((v) => ({ frequency: v.frequency, strategy_type: v.strategy })),
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
        const cur = next[v.key];
        if (cur?.status === 'running' || cur?.status === 'pending') {
          next[v.key] = { status: 'error', message: topLevelError! };
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
  momentumStore.set({ loadingSnapshotId: snapshotId, error: null });
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
  } finally {
    momentumStore.set({ loadingSnapshotId: null });
  }
}

export async function deleteCurrentPicksSnapshot(snapshotId: number): Promise<void> {
  momentumStore.set({ deletingSnapshotId: snapshotId, error: null });
  try {
    const resp = await fetch(`${API_URL}/api/momentum/current-picks/${snapshotId}`, {
      method: 'DELETE',
    });
    if (!resp.ok) {
      momentumStore.set({ error: `Failed to delete snapshot ${snapshotId}` });
      return;
    }
    momentumStore.set((s) => ({
      currentPicksSnapshots: s.currentPicksSnapshots.filter((m) => m.snapshot_id !== snapshotId),
      // If the deleted snapshot was loaded, clear it.
      currentPortfolio: s.currentPortfolio?.snapshot_id === snapshotId ? null : s.currentPortfolio,
    }));
  } catch (e) {
    momentumStore.set({ error: e instanceof Error ? e.message : 'Delete failed' });
  } finally {
    momentumStore.set({ deletingSnapshotId: null });
  }
}

export async function renameCurrentPicksSnapshot(
  snapshotId: number,
  name: string | null,
): Promise<void> {
  momentumStore.set({ renamingSnapshotId: snapshotId, error: null });
  try {
    const resp = await fetch(`${API_URL}/api/momentum/current-picks/${snapshotId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    if (!resp.ok) {
      momentumStore.set({ error: `Failed to rename snapshot ${snapshotId}` });
      return;
    }
    const updated = await resp.json();
    momentumStore.set((s) => ({
      currentPicksSnapshots: s.currentPicksSnapshots.map((m) =>
        m.snapshot_id === snapshotId ? { ...m, name: updated.name ?? null } : m,
      ),
    }));
  } catch (e) {
    momentumStore.set({ error: e instanceof Error ? e.message : 'Rename failed' });
  } finally {
    momentumStore.set({ renamingSnapshotId: null });
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
