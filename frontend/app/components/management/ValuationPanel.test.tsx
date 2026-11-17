// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import ValuationPanel from './ValuationPanel';
import { apiFetch } from '../../../lib/apiFetch';
import { jobsStore } from '../../../lib/stores/jobs';
import type { Workbook } from './valuationWorkbook';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
const response = (body: unknown) => new Response(JSON.stringify(body));
const roster = [{ id: 1, name: 'One' }, { id: 2, name: 'Two' }];
const workbook = (): Workbook => ({ portfolio_id: 1, as_of: '2026-11-16', parameters: {
  years: 3, safety_margin: .2, bear_growth_delta: -.05, bear_multiple_delta: -.25, bull_growth_delta: .05, bull_multiple_delta: .25 },
  rows: [{ isin: 'EXACT', ticker: 'ADBE', selections: ['BB'], seed_source: null, assumptions: { growth: .1, fair_multiple: 20 },
    quote: { price: 60, currency: 'EUR', price_date: '2026-11-13' }, forecast: { fy1: 3, fy2: 4, fy1_date: '2026-12-31', fy2_date: '2027-12-31', currency: 'EUR' } }],
  membership_legend: [{ code: 'BB', name: 'Bustelberg', source: 'AIRS', scope: 'All profiles', status: 'complete', portfolios: [] }] });
const payload = (name: string) => ({ portfolio_id: 1, ready: true, stale: false, prepared_at: '2026-11-16T12:00:00Z', method: {}, evidence: {}, workbook: workbook(),
  rows: [{ isin: 'EXACT', name, weight_pct: 5, status: 'ok', price: 60, currency: 'EUR', estimated_value: 80, discount_pct: 25 }] });
const frame = (event: unknown) => `data: ${JSON.stringify(event)}\n\n`;
async function select(host: HTMLElement, id = '1') {
  const picker = host.querySelector('select')!;
  await act(async () => { picker.value = id; picker.dispatchEvent(new Event('change', { bubbles: true })); });
}
function mount() {
  const host = document.createElement('div'); document.body.appendChild(host);
  return { host, root: createRoot(host) };
}

it('shows only Offensief profiles while keeping standalone portfolios and Bustelberg 2.0', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockResolvedValue(response([
    { id: 1, name: 'Bustelberg Defensief' }, { id: 2, name: 'Bustelberg Neutraal' },
    { id: 3, name: 'Bustelberg Beperkt Offensief' }, { id: 4, name: 'Bustelberg Offensief' },
    { id: 5, name: 'Bustelberg 2.0 Offensief' }, { id: 6, name: 'Toppenberg Neutraal' },
    { id: 7, name: 'Toppenberg Offensief' }, { id: 8, name: 'AITopSelectie' }, { id: 9, name: 'Imagine Fund' },
  ]));
  const { host, root } = mount();
  try {
    await act(async () => root.render(<ValuationPanel />));
    expect([...host.querySelectorAll('select option')].slice(1).map(option => option.textContent)).toEqual([
      'Bustelberg Offensief', 'Bustelberg 2.0 Offensief', 'Toppenberg Offensief', 'AITopSelectie', 'Imagine Fund',
    ]);
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('shows the workbook directly and loads its panels without refetching', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockImplementation(async url => response(String(url).endsWith('/portfolios') ? roster : payload('Company One')));
  const { host, root } = mount();
  try {
    await act(async () => root.render(<ValuationPanel />));
    await select(host);
    expect(apiFetch).toHaveBeenCalledTimes(2); // Roster + one selected-portfolio payload.
    expect(vi.mocked(apiFetch).mock.calls[1][0]).toContain('/portfolios/1/dashboard');
    for (const label of ['Koers & winst', 'Waardering & koopzone', 'Scenario 2029', 'Bron & controle', 'Afkortingen', 'Parameters', 'Onderneming']) {
      await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav[aria-label="Valuation panels"] button')].find(b => b.textContent === label)!.click());
    }
    expect(host.textContent).toContain('Company One');
    expect(host.textContent).toContain('ADBE');
    expect(apiFetch).toHaveBeenCalledTimes(2);
    expect(host.querySelector('nav[aria-label="Valuation model"]')).toBeNull();
    expect(host.textContent).not.toContain('Historical reference');
    expect(host.textContent).not.toContain('Workbook model');
    expect(host.textContent).not.toContain('Loading valuation inputs');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('does not launch another preparation request for stale or pending cached data', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockImplementation(async url => response(String(url).endsWith('/portfolios') ? roster : { ...payload('Pending Company'), ready: false, stale: true }));
  const { host, root } = mount();
  try {
    await act(async () => root.render(<ValuationPanel />)); await select(host);
    expect(apiFetch).toHaveBeenCalledTimes(2);
    expect(host.textContent).toContain('Pending Company');
    expect(vi.mocked(apiFetch).mock.calls.every(([, init]) => !init?.method)).toBe(true);
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('shows the full workbook and tabs for an unprepared portfolio and leaves Refresh available', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockImplementation(async url => response(String(url).endsWith('/portfolios') ? roster : {
    ...payload('Unused'), ready: false, rows: [], detail: 'Portfolio data has not been prepared yet. Press Refresh to fetch it.',
    workbook: { ...workbook(), rows: [] },
  }));
  const { host, root } = mount();
  try {
    await act(async () => root.render(<ValuationPanel />)); await select(host);
    expect(host.querySelector('[role="alert"]')).toBeNull();
    expect(host.textContent).toContain('Press Refresh to load companies.');
    expect(host.textContent).not.toContain('No matching companies.');
    expect(host.querySelector('table')).not.toBeNull();
    expect(host.querySelectorAll('nav[aria-label="Valuation panels"] button')).toHaveLength(7);
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav[aria-label="Valuation panels"] button')].find(button => button.textContent === 'Parameters')!.click());
    expect(host.querySelector('nav[aria-label="Valuation panels"] button[aria-pressed="true"]')?.textContent).toBe('Parameters');
    expect([...host.querySelectorAll('button')].find(button => button.textContent === 'Refresh')?.disabled).toBe(false);
    expect(apiFetch).toHaveBeenCalledTimes(2);
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('can prepare an empty database and then select a portfolio with one bundled read', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockImplementation(async (url, init) => {
    if (String(url).endsWith('/portfolios')) return response([]);
    if (init?.method === 'POST') return new Response(frame({ type: 'complete', portfolios: roster, failures: [] }));
    return response(payload('Prepared Company'));
  });
  const { host, root } = mount();
  try {
    await act(async () => root.render(<ValuationPanel />));
    const refresh = [...host.querySelectorAll('button')].find(button => button.textContent === 'Refresh')!;
    expect(refresh.disabled).toBe(false);
    await act(async () => refresh.click());
    expect(vi.mocked(apiFetch).mock.calls[1][0]).toContain('/valuation/prepare/refresh');
    expect(jobsStore.get().jobs.at(-1)?.status).toBe('done');
    await select(host);
    expect(apiFetch).toHaveBeenCalledTimes(3);
    expect(vi.mocked(apiFetch).mock.calls[2][0]).toContain('/portfolios/1/dashboard');
    expect(host.textContent).toContain('Prepared Company');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it.each([null, undefined])('keeps selection to one bundled request when workbook data is %s', async missingWorkbook => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockImplementation(async url => response(String(url).endsWith('/portfolios')
    ? roster : { ...payload('Company One'), workbook: missingWorkbook }));
  const { host, root } = mount();
  try {
    await act(async () => root.render(<ValuationPanel />));
    await select(host);
    expect(vi.mocked(apiFetch).mock.calls.map(([url]) => String(url).split('/api/')[1])).toEqual([
      'airs/valuation/portfolios', 'airs/valuation/portfolios/1/dashboard',
    ]);
    expect(host.querySelector('[role="alert"]')?.textContent).toContain('missing workbook data');
    expect(host.textContent).not.toContain('Loading valuation inputs');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('ignores a late response for the previous portfolio', async () => {
  vi.mocked(apiFetch).mockReset();
  let finish!: (response: Response) => void;
  vi.mocked(apiFetch).mockResolvedValueOnce(response(roster))
    .mockReturnValueOnce(new Promise(resolve => { finish = resolve; }))
    .mockResolvedValueOnce(response(payload('Company Two')));
  const { host, root } = mount();
  try {
    await act(async () => root.render(<ValuationPanel />)); await select(host); await select(host, '2');
    await act(async () => finish(response(payload('Company One'))));
    expect(host.textContent).toContain('Company Two');
    expect(host.textContent).not.toContain('Company One');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('Refresh uses one POST stream for row progress and the complete data without polling', async () => {
  vi.mocked(apiFetch).mockReset();
  let stream!: ReadableStreamDefaultController<Uint8Array>;
  const encoder = new TextEncoder();
  vi.mocked(apiFetch).mockImplementation(async (url, init) => {
    if (String(url).endsWith('/portfolios')) return response(roster);
    if (init?.method === 'POST') return new Response(new ReadableStream({ start(controller) { stream = controller; } }), { headers: { 'Content-Type': 'text/event-stream' } });
    return response(payload('Company One'));
  });
  const { host, root } = mount();
  try {
    await act(async () => root.render(<ValuationPanel />)); await select(host);
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav[aria-label="Valuation panels"] button')].find(b => b.textContent === 'Koers & winst')!.click());
    await act(async () => [...host.querySelectorAll('button')].find(b => b.textContent === 'Refresh')!.click());
    expect(apiFetch).toHaveBeenCalledTimes(3);
    expect(vi.mocked(apiFetch).mock.calls[2][0]).toContain('/portfolios/1/dashboard/refresh');
    await act(async () => stream.enqueue(encoder.encode(frame({ type: 'progress', done: 0, total: 1, isin: 'EXACT', message: '1/1 · Company One' }))));
    expect(host.querySelector('tr[aria-busy="true"]')).not.toBeNull();
    expect(jobsStore.get().jobs.at(-1)?.message).toContain('Company One');
    const final = payload('Updated Company');
    final.workbook.rows[0].quote!.price = 72;
    final.workbook.rows[0].forecast.fy1 = 9;
    await act(async () => stream.enqueue(encoder.encode(frame({ type: 'row', row: final.rows[0], workbook: final.workbook }))));
    expect(host.querySelector('tbody [data-column="raw_price"]')?.textContent).toContain('72.00');
    expect(host.textContent).toContain('Updated Company');
    expect(jobsStore.get().jobs.at(-1)?.status).toBe('running');
    expect(apiFetch).toHaveBeenCalledTimes(3);
    await act(async () => {
      stream.enqueue(encoder.encode(frame({ type: 'row', row: final.rows[0] }) + frame({ type: 'progress', done: 1, total: 1, message: 'Updated Company' }) + frame({ type: 'complete', data: final, failures: [] })));
      stream.close();
    });
    expect(host.textContent).toContain('Updated Company');
    expect(host.querySelector('tr[aria-busy="true"]')).toBeNull();
    expect(host.querySelector('nav[aria-label="Valuation panels"] button[aria-pressed="true"]')?.textContent).toBe('Koers & winst');
    expect(jobsStore.get().jobs.at(-1)?.status).toBe('done');
    expect(apiFetch).toHaveBeenCalledTimes(3);
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('keeps source failures visible in the shared popup while accepting the final partial payload', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockImplementation(async (url, init) => {
    if (String(url).endsWith('/portfolios')) return response(roster);
    if (init?.method === 'POST') return new Response(frame({ type: 'row_error', message: 'Company One: GuruFocus returned empty statements.' })
      + frame({ type: 'complete', data: payload('Company One'), counts: { succeeded: 19, failed: 1, total: 20 },
        failures: ['Company One: GuruFocus returned empty statements.'] }));
    return response(payload('Company One'));
  });
  const { host, root } = mount();
  try {
    await act(async () => root.render(<ValuationPanel />)); await select(host);
    await act(async () => [...host.querySelectorAll('button')].find(b => b.textContent === 'Refresh')!.click());
    expect(apiFetch).toHaveBeenCalledTimes(3);
    expect(jobsStore.get().jobs.at(-1)?.status).toBe('failed');
    expect(jobsStore.get().jobs.at(-1)?.summary).toBe('19/20 companies refreshed; 1 failed. Successful updates saved.');
    expect(jobsStore.get().jobs.at(-1)?.details?.join('\n')).toContain('GuruFocus returned empty statements');
    expect(host.textContent).not.toContain('19/20 companies refreshed; 1 failed');
    expect(host.textContent).not.toContain('GuruFocus returned empty statements');
    expect(host.querySelector('[role="alert"]')).toBeNull();
  } finally { await act(async () => root.unmount()); host.remove(); }
});
