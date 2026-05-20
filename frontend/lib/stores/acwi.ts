import { createStore } from '../store';
import { API_URL } from '../apiUrl';

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

// (save-universe removed: the `/api/acwi/save-universe` endpoint is
// superseded by `POST /api/universe-templates/ACWI/refresh`, called
// directly from `AcwiCanonicalView.tsx`.)
