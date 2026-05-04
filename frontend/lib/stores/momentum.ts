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

export type MonthlyRecord = {
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
  monthly_records: MonthlyRecord[];
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
    if ((e as { name?: string })?.name === 'AbortError') {
      // user-initiated cancel
    } else {
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
