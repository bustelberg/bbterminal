import { createStore } from '../store';
import { API_URL } from '../apiUrl';

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

export type AirsScanState = {
  scanning: boolean;
  steps: ScanSteps | null;
  portfolios: Portfolio[] | null;
  error: string | null;
};

export const airsScanStore = createStore<AirsScanState>({
  scanning: false,
  steps: null,
  portfolios: null,
  error: null,
});

let eventSource: EventSource | null = null;

export function startAirsScan(callbacks: {
  onPortfolios?: (portfolios: Portfolio[]) => void;
}): void {
  eventSource?.close();

  airsScanStore.set({
    scanning: true,
    steps: { ...INITIAL_STEPS },
    error: null,
  });

  const es = new EventSource(`${API_URL}/api/airs/scan`);
  eventSource = es;

  es.onmessage = (event) => {
    const data = JSON.parse(event.data) as {
      type: string;
      step?: keyof ScanSteps;
      status?: StepStatus;
      message?: string;
      data?: Portfolio[];
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
      es.close();
      if (eventSource === es) eventSource = null;
    } else if (data.type === 'error') {
      airsScanStore.set({ error: data.message ?? 'Unknown error', scanning: false });
      es.close();
      if (eventSource === es) eventSource = null;
    }
  };

  es.onerror = () => {
    airsScanStore.set({ error: 'Connection lost', scanning: false });
    es.close();
    if (eventSource === es) eventSource = null;
  };
}

export function cancelAirsScan(): void {
  eventSource?.close();
  eventSource = null;
  airsScanStore.set({ scanning: false });
}
