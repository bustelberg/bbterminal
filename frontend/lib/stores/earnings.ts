import { createStore, type Store } from '../store';
import { runSSE } from '../stream';

export type EarningsLog = { type: string; message: string };

export type EarningsRefreshState = {
  running: boolean;
  logs: EarningsLog[];
};

export type EarningsRefreshController = {
  store: Store<EarningsRefreshState>;
  start: (
    url: string,
    callbacks?: {
      onApiCalls?: (region: string, count: number) => void;
      onDone?: () => void;
    },
  ) => Promise<void>;
  clearLogs: () => void;
};

/** One self-contained SSE refresh slot (store + start + clearLogs) so the
 * /earnings page can drive A and B independently. Each controller owns
 * its own AbortController + module-scoped store instance — the two
 * companies never share state. */
function createEarningsRefreshController(): EarningsRefreshController {
  const store = createStore<EarningsRefreshState>({ running: false, logs: [] });
  let controller: AbortController | null = null;

  const start: EarningsRefreshController['start'] = async (url, callbacks) => {
    if (store.get().running) return;

    controller?.abort();
    const c = new AbortController();
    controller = c;

    store.set({ running: true, logs: [] });

    try {
      await runSSE(
        url,
        { method: 'POST' },
        (raw) => {
          const parsed = raw as { type: string; message?: string; region?: string; count?: number };
          if (parsed.type === 'api_calls' && callbacks?.onApiCalls) {
            callbacks.onApiCalls(parsed.region ?? '', parsed.count ?? 0);
          }
          store.set((s) => ({
            logs: [...s.logs, { type: parsed.type, message: parsed.message ?? '' }],
          }));
        },
        c.signal,
      );
    } catch (e) {
      if ((e as { name?: string })?.name !== 'AbortError') {
        store.set((s) => ({
          logs: [...s.logs, { type: 'error', message: e instanceof Error ? e.message : 'Refresh failed' }],
        }));
      }
    } finally {
      store.set({ running: false });
      if (controller === c) controller = null;
      callbacks?.onDone?.();
    }
  };

  const clearLogs = () => { store.set({ logs: [] }); };

  return { store, start, clearLogs };
}

// Two parallel controllers — A is the primary company, B is the
// comparison company. EarningsDashboard hooks each to its own
// useSSERefresh and LogPanel.
export const earningsRefreshA = createEarningsRefreshController();
export const earningsRefreshB = createEarningsRefreshController();

// Back-compat aliases: existing callers (and anything else importing the
// originals) keep pointing at the A controller's pieces.
export const earningsRefreshStore = earningsRefreshA.store;
export const startEarningsRefresh = earningsRefreshA.start;
export const clearEarningsLogs = earningsRefreshA.clearLogs;
