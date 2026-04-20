import { createStore } from '../store';
import { runSSE } from '../stream';

export type EarningsLog = { type: string; message: string };

export type EarningsRefreshState = {
  running: boolean;
  logs: EarningsLog[];
};

export const earningsRefreshStore = createStore<EarningsRefreshState>({
  running: false,
  logs: [],
});

let refreshController: AbortController | null = null;

export async function startEarningsRefresh(
  url: string,
  callbacks?: {
    onApiCalls?: (region: string, count: number) => void;
    onDone?: () => void;
  },
): Promise<void> {
  if (earningsRefreshStore.get().running) return;

  refreshController?.abort();
  const controller = new AbortController();
  refreshController = controller;

  earningsRefreshStore.set({ running: true, logs: [] });

  try {
    await runSSE(
      url,
      { method: 'POST' },
      (raw) => {
        const parsed = raw as { type: string; message?: string; region?: string; count?: number };
        if (parsed.type === 'api_calls' && callbacks?.onApiCalls) {
          callbacks.onApiCalls(parsed.region ?? '', parsed.count ?? 0);
        }
        earningsRefreshStore.set((s) => ({
          logs: [...s.logs, { type: parsed.type, message: parsed.message ?? '' }],
        }));
      },
      controller.signal,
    );
  } catch (e) {
    if ((e as { name?: string })?.name !== 'AbortError') {
      earningsRefreshStore.set((s) => ({
        logs: [...s.logs, { type: 'error', message: e instanceof Error ? e.message : 'Refresh failed' }],
      }));
    }
  } finally {
    earningsRefreshStore.set({ running: false });
    if (refreshController === controller) refreshController = null;
    callbacks?.onDone?.();
  }
}

export function clearEarningsLogs(): void {
  earningsRefreshStore.set({ logs: [] });
}
