import { createStore } from '../store';
import { runSSE } from '../stream';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

// ─── import-sp500 ───────────────────────────────────────────────────────────

export type Sp500ImportState = {
  running: boolean;
  logs: string[];
};

export const sp500ImportStore = createStore<Sp500ImportState>({
  running: false,
  logs: [],
});

let importController: AbortController | null = null;

export async function startSp500Import(onDone?: () => void): Promise<void> {
  if (sp500ImportStore.get().running) return;

  importController?.abort();
  const controller = new AbortController();
  importController = controller;

  sp500ImportStore.set({ running: true, logs: [] });

  try {
    await runSSE(
      `${API_URL}/api/index-universe/import-sp500`,
      { method: 'POST' },
      (raw) => {
        const evt = raw as { type: string; message?: string };
        if (evt.type === 'progress') {
          sp500ImportStore.set((s) => ({ logs: [...s.logs, evt.message ?? ''] }));
        } else if (evt.type === 'done') {
          sp500ImportStore.set((s) => ({ logs: [...s.logs, evt.message ?? 'Done'] }));
          onDone?.();
        } else if (evt.type === 'error') {
          sp500ImportStore.set((s) => ({ logs: [...s.logs, `ERROR: ${evt.message ?? 'unknown'}`] }));
        }
      },
      controller.signal,
    );
  } catch (e) {
    if ((e as { name?: string })?.name !== 'AbortError') {
      sp500ImportStore.set((s) => ({
        logs: [...s.logs, `ERROR: ${e instanceof Error ? e.message : 'Import failed'}`],
      }));
    }
  } finally {
    sp500ImportStore.set({ running: false });
    if (importController === controller) importController = null;
  }
}

export function clearSp500ImportLogs(): void {
  sp500ImportStore.set({ logs: [] });
}

// ─── check-gurufocus ────────────────────────────────────────────────────────

export type GFResult = {
  available: string[];
  missing: string[];
  total: number;
  available_count: number;
  missing_count: number;
  coverage_pct: number;
};

export type Sp500GfCheckState = {
  checkingGF: boolean;
  gfLogs: string[];
  gfResult: GFResult | null;
};

export const sp500GfCheckStore = createStore<Sp500GfCheckState>({
  checkingGF: false,
  gfLogs: [],
  gfResult: null,
});

let gfController: AbortController | null = null;

export async function startSp500GfCheck(indexName: string): Promise<void> {
  if (sp500GfCheckStore.get().checkingGF) return;

  gfController?.abort();
  const controller = new AbortController();
  gfController = controller;

  sp500GfCheckStore.set({ checkingGF: true, gfLogs: [], gfResult: null });

  try {
    await runSSE(
      `${API_URL}/api/index-universe/check-gurufocus?index=${encodeURIComponent(indexName)}`,
      { method: 'POST' },
      (raw) => {
        const evt = raw as { type: string; message?: string; data?: GFResult };
        if (evt.type === 'progress') {
          sp500GfCheckStore.set((s) => ({ gfLogs: [...s.gfLogs, evt.message ?? ''] }));
        } else if (evt.type === 'done') {
          sp500GfCheckStore.set((s) => ({
            gfLogs: [...s.gfLogs, evt.message ?? 'Done'],
            gfResult: evt.data ?? s.gfResult,
          }));
        } else if (evt.type === 'error') {
          sp500GfCheckStore.set((s) => ({
            gfLogs: [...s.gfLogs, `ERROR: ${evt.message ?? 'unknown'}`],
          }));
        }
      },
      controller.signal,
    );
  } catch (e) {
    if ((e as { name?: string })?.name !== 'AbortError') {
      sp500GfCheckStore.set((s) => ({
        gfLogs: [...s.gfLogs, `ERROR: ${e instanceof Error ? e.message : 'Check failed'}`],
      }));
    }
  } finally {
    sp500GfCheckStore.set({ checkingGF: false });
    if (gfController === controller) gfController = null;
  }
}

export function clearSp500GfCheck(): void {
  sp500GfCheckStore.set({ gfLogs: [], gfResult: null });
}
