// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { startJob, type JobToast } from '../../../lib/stores/jobs';
import { RiskBundleContext } from './riskBundleContext';
import WeightsPanel from './WeightsPanel';

const { dataFetch } = vi.hoisted(() => ({ dataFetch: vi.fn() }));
vi.mock('../../../lib/apiFetch', () => ({ apiFetch: (url: string, ...args: unknown[]) => {
  if (url.endsWith('/weight-search/bounds')) return Promise.resolve(new Response(JSON.stringify({
    profiles: ['Offensief', 'Neutraal'], profile: 'Offensief', all_bounds: ['Offensief', 'Neutraal'].flatMap(variant =>
      ['Equity', 'Bonds', 'Alternatives', 'Cash'].map(bucket => ({ variant, bucket, min_pct: 0, default_pct: 25, max_pct: 100 }))),
  })));
  return dataFetch(url, ...args);
} }));
vi.mock('../../../lib/stores/jobs', () => ({ startJob: vi.fn() }));
vi.mock('../../../lib/readCache', () => ({ invalidateReadCache: vi.fn() }));

it('opens the selected stored return bars without fetching and closes independently', async () => {
  const options = ['A', 'B'].map(id => ({ id, name: `Instrument ${id}`, isin: id, currency: 'EUR' }));
  const series = { selected: { ...options[0], held: true, weeks: [
    { status: 'priced', price: 105, from_price: 100, return_pct: 5 },
  ] }, periods: [{ week_end: '2026-11-11', target_date: '2026-11-11', as_of_date: '2026-11-11',
    from_date: '2026-11-04', partial: false, available: true }], missing_observations: 0 };
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<RiskBundleContext.Provider value={{ error: null, views: {
      'weekly-returns': { portefeuille: 'BOOK', start_date: '2025-02-01', through: '2026-11-11', options, instruments: { A: series } },
    } }}><WeightsPanel portefeuille="BOOK" /></RiskBundleContext.Provider>));
    await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="Open Instrument A"]')!.click());
    expect(host.querySelector('[role="dialog"]')).not.toBeNull();
    expect(host.querySelector('svg[aria-label*="Instrument A"]')).not.toBeNull();
    expect(host.querySelector('rect.text-pos-300')).not.toBeNull();
    expect(host.querySelector('[aria-label*="+5.00%"]')).not.toBeNull();
    await act(async () => Array.from(host.querySelectorAll('button')).find(button => button.textContent === 'Close')!.click());
    expect(host.querySelector('[role="dialog"]')).toBeNull();
    await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="Open Instrument B"]')!.click());
    expect(host.textContent).toContain('No weekly returns stored yet');
    expect(dataFetch).not.toHaveBeenCalled();
    expect(startJob).not.toHaveBeenCalled();
  } finally {
    await act(async () => root.unmount());
    host.remove();
    vi.clearAllMocks();
  }
});

it('fetches all instruments with one shared snapshot job and one saved bundle read', async () => {
  const options = ['A', 'B'].map(id => ({ id, name: id, isin: id, currency: 'EUR' }));
  const base = { portefeuille: 'BOOK', start_date: '2025-02-01', through: '2026-11-11', options };
  let finish!: (job: JobToast) => void;
  vi.mocked(startJob).mockResolvedValue({ id: 'all', body: {}, done: new Promise<JobToast>(resolve => { finish = resolve; }) });
  dataFetch.mockResolvedValue({ ok: true, json: async () => ({ ...base,
    instruments: Object.fromEntries(options.map(option => [option.id, { selected: { ...option, held: true, weeks: [] }, periods: [], missing_observations: 0 }])),
  }) } as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  const root = createRoot(host);
  try {
    await act(async () => root.render(<RiskBundleContext.Provider value={{ error: null, views: {
      'weekly-returns': { ...base, instruments: {} },
    } }}><WeightsPanel portefeuille="BOOK" /></RiskBundleContext.Provider>));
    const all = Array.from(host.querySelectorAll('button')).find(button => button.textContent === 'Fetch all')!;
    await act(async () => all.click());
    expect(startJob).toHaveBeenCalledTimes(1);
    expect(vi.mocked(startJob).mock.calls[0][0]).toContain('/weekly-returns/all/job?start_date=2025-02-01');
    expect(all.disabled).toBe(true);
    expect(host.querySelector<HTMLButtonElement>('[aria-label="Fetch A"]')!.disabled).toBe(true);
    expect(host.querySelector<HTMLButtonElement>('[aria-label="Open A"]')!.disabled).toBe(false);
    await act(async () => finish({ status: 'done' } as JobToast));
    expect(dataFetch).toHaveBeenCalledTimes(1);
    expect(dataFetch.mock.calls[0][0]).toContain('/weekly-returns/all');
    expect(all.disabled).toBe(false);
    expect(host.textContent).toContain('All instrument histories updated');
  } finally {
    await act(async () => root.unmount());
    vi.clearAllMocks();
  }
});

it('lists the AIRS catalogue and queues independent row fetches with the configured hard date', async () => {
  const options = ['A', 'B'].map(id => ({ id, name: `Instrument ${id}`, isin: id, currency: 'EUR' }));
  const base = { portefeuille: 'BOOK', start_date: '2025-02-01', through: '2026-11-11', options };
  const finishes: Array<(job: JobToast) => void> = [];
  vi.mocked(startJob).mockImplementation(async () => ({ id: 'job', body: {},
    done: new Promise<JobToast>(resolve => finishes.push(resolve)),
  }));
  dataFetch.mockResolvedValue({ ok: true, json: async () => ({ ...base, periods: [], selected: null }) } as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<RiskBundleContext.Provider value={{ error: null, views: {
      'weekly-returns': { ...base, instruments: {} },
    } }}><WeightsPanel portefeuille="BOOK" /></RiskBundleContext.Provider>));
    expect(dataFetch).not.toHaveBeenCalled();
    expect(host.querySelector<HTMLInputElement>('[aria-label="Earliest date"]')!.value).toBe('2025-02-01');
    const a = host.querySelector<HTMLButtonElement>('[aria-label="Fetch Instrument A"]')!;
    const b = host.querySelector<HTMLButtonElement>('[aria-label="Fetch Instrument B"]')!;
    await act(async () => a.click());
    expect(a.disabled).toBe(true);
    expect(b.disabled).toBe(false);
    await act(async () => b.click());
    expect(b.textContent).toBe('Queued');
    expect(startJob).toHaveBeenCalledTimes(1);
    expect(vi.mocked(startJob).mock.calls[0][0]).toContain('instrument_id=A&start_date=2025-02-01');
    await act(async () => finishes[0]({ status: 'done' } as JobToast));
    expect(startJob).toHaveBeenCalledTimes(2);
    expect(a.disabled).toBe(false);
    await act(async () => finishes[1]({ status: 'done' } as JobToast));
    expect(host.textContent).toContain('Updated');
    expect(b.disabled).toBe(false);
    const input = host.querySelector<HTMLInputElement>('[aria-label="Earliest date"]')!;
    await act(async () => {
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, '2025-03-01');
      input.dispatchEvent(new Event('change', { bubbles: true }));
      input.dispatchEvent(new Event('input', { bubbles: true }));
    });
    await act(async () => a.click());
    expect(vi.mocked(startJob).mock.calls[2][0]).toContain('start_date=2025-03-01');
    await act(async () => finishes[2]({ status: 'done' } as JobToast));
  } finally {
    await act(async () => root.unmount());
    host.remove();
    vi.clearAllMocks();
  }
});
