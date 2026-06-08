import { createStore } from '../store';
import { API_URL } from '../apiUrl';
import { runSSE } from '../stream';

export type StepStatus = 'idle' | 'in_progress' | 'done' | 'error';

export type ScanSteps = {
  login: { status: StepStatus; message: string };
  navigate: { status: StepStatus; message: string };
  scrape: { status: StepStatus; message: string };
  ytd: { status: StepStatus; message: string };
};

export const INITIAL_STEPS: ScanSteps = {
  login: { status: 'idle', message: 'Log in to broker' },
  navigate: { status: 'idle', message: 'Navigate to portfolios' },
  scrape: { status: 'idle', message: 'Read portfolio table' },
  ytd: { status: 'idle', message: 'Load YTD returns' },
};

export type Portfolio = {
  portefeuille: string;
  depotbank: string;
  client: string;
  naam: string;
};

// Discriminator on structured scan errors so the UI can render targeted
// guidance instead of a generic red banner. Today only `ip_forbidden`
// (egress IP not on AirSPMS's allowlist) is classified; extend the union
// when more failure modes get bespoke UX.
export type AirsScanErrorKind = 'ip_forbidden';

export type AirsScanState = {
  scanning: boolean;
  steps: ScanSteps | null;
  portfolios: Portfolio[] | null;
  error: string | null;
  errorKind: AirsScanErrorKind | null;
  errorDetail: string | null;
};

export const airsScanStore = createStore<AirsScanState>({
  scanning: false,
  steps: null,
  portfolios: null,
  error: null,
  errorKind: null,
  errorDetail: null,
});

// fetch-based SSE (via `runSSE`) rather than EventSource so the session
// JWT rides along in the Authorization header — the API auth gate requires
// it. An AbortController replaces EventSource.close() for cancellation.
let abort: AbortController | null = null;

export function startAirsScan(callbacks: {
  onPortfolios?: (portfolios: Portfolio[]) => void;
}): void {
  abort?.abort();

  airsScanStore.set({
    scanning: true,
    steps: { ...INITIAL_STEPS },
    error: null,
    errorKind: null,
    errorDetail: null,
  });

  const controller = new AbortController();
  abort = controller;

  const onEvent = (raw: unknown) => {
    const data = raw as {
      type: string;
      step?: keyof ScanSteps;
      status?: StepStatus;
      message?: string;
      data?: Portfolio[];
      kind?: string;
      detail?: string;
    };
    if (data.type === 'progress' && data.step && data.step in INITIAL_STEPS) {
      airsScanStore.set((s) => ({
        steps: s.steps
          ? { ...s.steps, [data.step!]: { status: data.status ?? 'idle', message: data.message ?? '' } }
          : s.steps,
      }));
    } else if (data.type === 'portfolios') {
      airsScanStore.set({ portfolios: data.data ?? [] });
      callbacks.onPortfolios?.(data.data ?? []);
    } else if (data.type === 'done') {
      airsScanStore.set({ scanning: false });
      controller.abort();
    } else if (data.type === 'error') {
      airsScanStore.set({
        error: data.message ?? 'Unknown error',
        errorKind: data.kind === 'ip_forbidden' ? 'ip_forbidden' : null,
        errorDetail: data.detail ?? null,
        scanning: false,
      });
      controller.abort();
    }
  };

  runSSE(`${API_URL}/api/airs/scan`, {}, onEvent, controller.signal)
    .catch(() => {
      // Aborted (done/error/cancel) is expected; surface only real failures.
      if (controller.signal.aborted) return;
      airsScanStore.set({ error: 'Connection lost', errorKind: null, errorDetail: null, scanning: false });
    })
    .finally(() => { if (abort === controller) abort = null; });
}

export function cancelAirsScan(): void {
  abort?.abort();
  abort = null;
  airsScanStore.set({ scanning: false });
}
