// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { apiFetch } from '../../../lib/apiFetch';
import { RiskBundleContext } from './riskBundleContext';
import WeeklyReturnsPanel from './WeeklyReturnsPanel';
import { startJob, type JobProgress, type JobToast } from '../../../lib/stores/jobs';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../../lib/stores/jobs', () => ({ startJob: vi.fn() }));

it('shows correlation for weekly histories recorded on different weekdays', async () => {
  const options = [
    { id: 'FUND', name: 'High Income Quality fund', isin: 'FUND', currency: 'EUR' },
    { id: 'STOCK', name: 'Adyen', isin: 'STOCK', currency: 'EUR' },
  ];
  const dates = [
    ['2025-06-15', '2025-06-22', '2025-06-29', '2025-07-06'],
    ['2025-06-11', '2025-06-18', '2025-06-25', '2025-07-02'],
  ];
  const instruments = Object.fromEntries(options.map((option, instrument) => [option.id, {
    selected: { ...option, held: true, weeks: [1, -2, 3].map(value => ({
      status: 'priced', from_price: 100, price: 100 + value * (instrument + 1), return_pct: value * (instrument + 1),
    })) },
    periods: dates[instrument].slice(1).map((date, index) => ({
      target_date: date, week_end: date, as_of_date: date, from_date: dates[instrument][index],
      partial: false, available: true,
    })), missing_observations: 0,
  }]));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<RiskBundleContext.Provider value={{ error: null, views: {
      'weekly-returns': { portefeuille: 'BOOK', start_date: '2025-01-01', through: '2025-07-06', options, instruments },
    } }}><WeeklyReturnsPanel portefeuille="BOOK" /></RiskBundleContext.Provider>));
    for (const [index, input] of Array.from(host.querySelectorAll<HTMLInputElement>('[role="combobox"]')).entries()) {
      await act(async () => input.focus());
      await act(async () => host.querySelectorAll<HTMLButtonElement>('[role="option"]')[index].click());
    }
    const correlation = Array.from(host.querySelectorAll('span')).find(span => /^(Correlation|Correlatie)$/.test(span.textContent ?? ''))?.parentElement;
    expect(correlation?.textContent).toContain('1.00');
    expect(correlation?.textContent).toMatch(/3 (weeks|weken)/);
  } finally {
    await act(async () => root.unmount());
    host.remove();
    vi.clearAllMocks();
  }
});

it('selects either weekly instrument from the Risk bundle without another request', async () => {
  const options = [
    { id: 'A', name: 'First instrument', isin: 'A', currency: 'EUR' },
    { id: 'B', name: 'Second instrument', isin: 'B', currency: 'EUR' },
  ];
  const periods = [{ week_end: '2026-01-02', target_date: '2026-01-02', as_of_date: '2026-01-02',
    from_date: '2025-12-31', partial: false, available: true }];
  const instruments = Object.fromEntries(options.map((option, index) => [option.id, {
    selected: { ...option, held: true, weeks: [{ status: 'priced', from_price: 100, price: 101 + index, return_pct: 1 + index }] },
    periods, missing_observations: 0,
    start_date: '2024-01-01', ytd_return: { value: 8 + index, from: '2025-12-31', to: '2026-01-02' },
  }]));
  const bundle = { error: null, views: { 'weekly-returns': {
    portefeuille: 'BOOK', start_date: '2026-01-01', through: '2026-01-02', options, instruments,
  } } };
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<RiskBundleContext.Provider value={bundle}>
      <WeeklyReturnsPanel portefeuille="BOOK" />
    </RiskBundleContext.Provider>));
    const inputs = host.querySelectorAll<HTMLInputElement>('[role="combobox"]');
    for (const [index, input] of Array.from(inputs).entries()) {
      await act(async () => input.focus());
      const choices = host.querySelectorAll<HTMLButtonElement>('[role="option"]');
      await act(async () => choices[index].click());
    }
    expect(host.textContent).toContain('+1.00%');
    expect(host.textContent).toContain('+2.00%');
    expect(host.textContent).toContain('+8.00%');
    expect(host.textContent).toContain('+9.00%');
    expect(host.textContent).toMatch(/Full history|Volledige historie/);
    expect(host.querySelector('svg[aria-label*="2025"]')).not.toBeNull(); // first actual quote, not the empty 2024 prefix
    expect(apiFetch).not.toHaveBeenCalled();
  } finally {
    await act(async () => root.unmount());
    host.remove();
  }
});

it('reloads all saved instrument series once after the history job finishes', async () => {
  const option = { id: 'A', name: 'Amazon.com', isin: 'A', currency: 'USD' };
  const periods = [{ week_end: '2026-01-02', target_date: '2026-01-02', as_of_date: '2026-01-02',
    from_date: '2025-12-31', partial: false, available: true }];
  const base = { portefeuille: 'BOOK', start_date: '2026-01-01', through: '2026-01-02', options: [option] };
  const series = { selected: { ...option, held: true,
    weeks: [{ status: 'missing_price', price: null, from_price: 100, return_pct: null }] },
    periods, missing_observations: 1 };
  let progress: ((event: JobProgress) => void) | undefined;
  let finish!: (job: JobToast) => void;
  const done = new Promise<JobToast>((resolve) => { finish = resolve; });
  vi.mocked(startJob).mockImplementation(async (_url, _title, _init, onProgress) => {
    progress = onProgress;
    return { id: 'history', done, body: {} };
  });
  vi.mocked(apiFetch).mockResolvedValue({ ok: true, json: async () => ({ ...base, ...series,
    instruments: { A: { ...series, selected: { ...series.selected, weeks: [{ status: 'priced', price: 105, from_price: 100, return_pct: 5 }] } } },
  }) } as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<RiskBundleContext.Provider value={{ error: null, views: {
      'weekly-returns': { ...base, instruments: { A: series } },
    } }}><WeeklyReturnsPanel portefeuille="BOOK" /></RiskBundleContext.Provider>));
    await act(async () => host.querySelector<HTMLInputElement>('[role="combobox"]')!.focus());
    await act(async () => host.querySelector<HTMLButtonElement>('[role="option"]')!.click());
    const load = Array.from(host.querySelectorAll('button')).find((button) => /Load full history|Volledige geschiedenis laden/.test(button.textContent ?? ''))!;
    await act(async () => load.click());
    expect(host.querySelector('rect.text-pos-300')).toBeNull();
    await act(async () => { progress?.({ done: 1, total: 3 }); });
    expect(load.disabled).toBe(true);
    expect(host.querySelector('rect.text-pos-300')).toBeNull();
    await act(async () => { finish({ status: 'done' } as JobToast); });
    expect(host.querySelector('rect.text-pos-300')).not.toBeNull();
    expect(host.textContent).toContain('+5.00%');
    expect(apiFetch).toHaveBeenCalledTimes(1);
    expect(apiFetch).toHaveBeenLastCalledWith(expect.stringContaining('weekly-returns/all'),
      expect.objectContaining({ cache: 'no-store', noReadCache: true }));
  } finally {
    await act(async () => root.unmount());
    host.remove();
    vi.clearAllMocks();
  }
});


it('fetches one saved portfolio bundle for both instrument selectors', async () => {
  vi.mocked(apiFetch).mockReset();
  const options = [
    { id: 'A', name: 'Amazon.com', isin: 'A', currency: 'USD' },
    { id: 'B', name: 'Microsoft', isin: 'B', currency: 'USD' },
  ];
  const instruments = Object.fromEntries(options.map(option => [option.id, {
    selected: { ...option, held: true, weeks: [] }, periods: [], missing_observations: 0,
  }]));
  vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify({ ready: true, portefeuille: 'BOOK',
    start_date: '2025-02-01', through: '2026-11-13', options, instruments })));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WeeklyReturnsPanel portefeuille="BOOK" />));
    expect(apiFetch).toHaveBeenCalledTimes(1);
    expect(apiFetch).toHaveBeenCalledWith(expect.stringContaining('/BOOK/weekly-returns/all'), expect.anything());
    const pickers = host.querySelectorAll<HTMLInputElement>('[role="combobox"]');
    await act(async () => pickers[0].focus());
    await act(async () => host.querySelectorAll<HTMLButtonElement>('[role="option"]')[0].click());
    await act(async () => pickers[1].focus());
    await act(async () => host.querySelectorAll<HTMLButtonElement>('[role="option"]')[1].click());
    expect(apiFetch).toHaveBeenCalledTimes(1);
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});
