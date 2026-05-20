import { createStore } from '../store';
import { runSSE } from '../stream';
import { API_URL } from '../apiUrl';

export type IngestEvent = {
  type: 'info' | 'progress' | 'done' | 'error' | string;
  message: string;
};

export type IngestState = {
  running: boolean;
  log: IngestEvent[];
  finished: boolean;
};

export const ingestStore = createStore<IngestState>({
  running: false,
  log: [],
  finished: false,
});

let abortController: AbortController | null = null;

export async function startIngest(onFinish?: () => void): Promise<void> {
  if (ingestStore.get().running) return;

  abortController?.abort();
  const controller = new AbortController();
  abortController = controller;

  ingestStore.set({ running: true, finished: false, log: [] });

  try {
    await runSSE(
      `${API_URL}/api/ingest/long-equity`,
      { method: 'POST' },
      (raw) => {
        const evt = raw as IngestEvent;
        ingestStore.set((s) => ({ log: [...s.log, evt] }));
        if (evt.type === 'done') {
          ingestStore.set({ finished: true });
        }
      },
      controller.signal,
    );
  } catch (e) {
    if ((e as { name?: string })?.name !== 'AbortError') {
      ingestStore.set((s) => ({
        log: [...s.log, { type: 'error', message: e instanceof Error ? e.message : String(e) }],
      }));
    }
  } finally {
    ingestStore.set({ running: false });
    if (abortController === controller) abortController = null;
    onFinish?.();
  }
}

export function cancelIngest(): void {
  abortController?.abort();
  abortController = null;
  ingestStore.set({ running: false });
}
