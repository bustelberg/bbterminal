import { createStore } from '../store';
import { runSSE } from '../stream';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

// ─── fetch-all-details (EventSource) ────────────────────────────────────────

export type AcwiFetchProgress = {
  message: string;
  fetched: number;
  total: number;
  pct: number;
  errors: number;
};

export type AcwiFetchErrorEntry = { title: string; href: string; error: string };

export type AcwiFetchSummary = {
  message: string;
  errors: number;
  errorList: AcwiFetchErrorEntry[];
};

export type AcwiFetchState = {
  fetching: boolean;
  progress: AcwiFetchProgress | null;
  summary: AcwiFetchSummary | null;
};

export const acwiFetchStore = createStore<AcwiFetchState>({
  fetching: false,
  progress: null,
  summary: null,
});

let fetchES: EventSource | null = null;

export function startAcwiFetchDetails(onDone?: () => void): void {
  if (acwiFetchStore.get().fetching) return;
  fetchES?.close();

  acwiFetchStore.set({
    fetching: true,
    summary: null,
    progress: { message: 'Starting...', fetched: 0, total: 0, pct: 0, errors: 0 },
  });

  const es = new EventSource(`${API_URL}/api/acwi/fetch-all-details`);
  fetchES = es;

  es.onmessage = (event) => {
    const data = JSON.parse(event.data) as {
      type: string;
      message?: string;
      fetched?: number;
      total?: number;
      pct?: number;
      errors?: number;
      error_list?: AcwiFetchErrorEntry[];
    };
    if (data.type === 'progress') {
      acwiFetchStore.set({
        progress: {
          message: data.message ?? '',
          fetched: data.fetched ?? 0,
          total: data.total ?? 0,
          pct: data.pct ?? 0,
          errors: data.errors ?? 0,
        },
      });
    } else if (data.type === 'done') {
      acwiFetchStore.set({
        fetching: false,
        progress: null,
        summary: {
          message: data.message ?? '',
          errors: data.errors ?? 0,
          errorList: data.error_list ?? [],
        },
      });
      es.close();
      if (fetchES === es) fetchES = null;
      onDone?.();
    } else if (data.type === 'error') {
      acwiFetchStore.set({
        fetching: false,
        progress: null,
        summary: { message: `Error: ${data.message ?? 'unknown'}`, errors: 1, errorList: [] },
      });
      es.close();
      if (fetchES === es) fetchES = null;
    }
  };
  es.onerror = () => {
    acwiFetchStore.set({
      fetching: false,
      progress: null,
      summary: {
        message: 'Connection lost — partial results may have been cached',
        errors: -1,
        errorList: [],
      },
    });
    es.close();
    if (fetchES === es) fetchES = null;
    onDone?.();
  };
}

// ─── save-universe (fetch SSE) ──────────────────────────────────────────────

export type AcwiSaveResult = { ok: boolean; message: string };

export type AcwiSaveState = {
  saving: boolean;
  progress: string[];
  result: AcwiSaveResult | null;
};

export const acwiSaveStore = createStore<AcwiSaveState>({
  saving: false,
  progress: [],
  result: null,
});

let saveController: AbortController | null = null;

export async function startAcwiSave(params: {
  name: string;
  start_date: string;
  end_date: string;
}): Promise<void> {
  if (acwiSaveStore.get().saving) return;

  saveController?.abort();
  const controller = new AbortController();
  saveController = controller;

  acwiSaveStore.set({
    saving: true,
    progress: [`Starting save as "${params.name}"...`],
    result: null,
  });

  try {
    await runSSE(
      `${API_URL}/api/acwi/save-universe`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(params),
      },
      (raw) => {
        const data = raw as { type: string; message?: string };
        if (data.type === 'progress') {
          acwiSaveStore.set((s) => ({ progress: [...s.progress, data.message ?? ''] }));
        } else if (data.type === 'done') {
          acwiSaveStore.set({ result: { ok: true, message: data.message ?? '' } });
        } else if (data.type === 'error') {
          acwiSaveStore.set({ result: { ok: false, message: data.message ?? '' } });
        }
      },
      controller.signal,
    );
  } catch (e) {
    if ((e as { name?: string })?.name !== 'AbortError') {
      acwiSaveStore.set({
        result: { ok: false, message: e instanceof Error ? e.message : 'Save failed' },
      });
    }
  } finally {
    acwiSaveStore.set({ saving: false });
    if (saveController === controller) saveController = null;
  }
}
