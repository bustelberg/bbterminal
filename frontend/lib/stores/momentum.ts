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
};

export type BacktestResult = {
  monthly_records: MonthlyRecord[];
  summary: Summary;
};

export type UniverseEntry = {
  company_id: number;
  ticker: string;
  exchange: string;
  company_name: string;
  sector: string;
};

export type ProgressEntry = { pct: number; message: string };
export type WarningEntry = { scope: string; message: string };
export type InfoEntry = { scope: string; message: string };

export type MomentumState = {
  running: boolean;
  progress: ProgressEntry[];
  result: BacktestResult | null;
  universe: UniverseEntry[];
  error: string | null;
  warnings: WarningEntry[];
  infos: InfoEntry[];
  loadedRunId: number | null;
};

export type BacktestStartConfig = {
  start_date: string;
  end_date: string;
  signal_weights: Record<string, number>;
  category_weights: Record<string, number>;
  top_n_sectors: number;
  top_n_per_sector: number;
  skip_price_fetch: boolean;
  max_companies: number;
  universe_label: string | null;
  index_universe: string | null;
};

export const momentumStore = createStore<MomentumState>({
  running: false,
  progress: [],
  result: null,
  universe: [],
  error: null,
  warnings: [],
  infos: [],
  loadedRunId: null,
});

let abortController: AbortController | null = null;

export async function startBacktest(cfg: BacktestStartConfig): Promise<void> {
  abortController?.abort();
  const controller = new AbortController();
  abortController = controller;

  momentumStore.set({
    running: true,
    progress: [],
    result: null,
    universe: [],
    error: null,
    warnings: [],
    infos: [],
    loadedRunId: null,
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
          data?: BacktestResult;
          universe?: UniverseEntry[];
        };
        lastEventTime = Date.now();
        if (data.type === 'progress') {
          momentumStore.set((s) => ({
            progress: [...s.progress, { pct: data.pct ?? 0, message: data.message ?? '' }],
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
          momentumStore.set({ result: data.data ?? null });
          if (data.universe) momentumStore.set({ universe: data.universe });
        } else if (data.type === 'done') {
          receivedDone = true;
          momentumStore.set({ running: false });
        } else if (data.type === 'error') {
          momentumStore.set({ error: data.message ?? 'Unknown error', running: false });
        }
      },
      controller.signal,
    );

    if (!receivedDone) {
      if (receivedResult) {
        momentumStore.set({ running: false });
      } else {
        const elapsed = Math.round((Date.now() - lastEventTime) / 1000);
        momentumStore.set({
          error: `Stream disconnected unexpectedly (last event ${elapsed}s ago). This can happen due to proxy timeouts — try again with "Skip data fetch" checked if prices are already loaded.`,
          running: false,
        });
      }
    }
  } catch (e) {
    if ((e as { name?: string })?.name === 'AbortError') {
      // user-initiated cancel
    } else {
      momentumStore.set({ error: e instanceof Error ? e.message : 'Unknown error' });
    }
    momentumStore.set({ running: false });
  } finally {
    if (abortController === controller) abortController = null;
  }
}

export function cancelBacktest(): void {
  abortController?.abort();
  abortController = null;
  momentumStore.set({ running: false });
}
