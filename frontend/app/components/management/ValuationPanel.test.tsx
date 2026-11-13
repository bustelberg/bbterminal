// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import ValuationPanel from './ValuationPanel';
import { apiFetch } from '../../../lib/apiFetch';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));

const response = (body: unknown) => new Response(JSON.stringify(body));
const roster = [{ id: 1, name: 'One' }, { id: 2, name: 'Two' }];
const payload = (name: string) => ({ portfolio_id: 1, ready: true, stale: false, calculation_date: '2026-11-13',
  prepared_at: '2026-11-13T23:15:00Z', method: {}, evidence: { composition_date: '2026-11-12' }, rows: [
    { name, isin: 'EXACT', weight_pct: 5, status: 'ok', price: 60, currency: 'EUR', price_date: '2026-11-13',
      median_pe: 20, current_pe: 15, estimated_value: 80, discount_pct: 25, observations: 1200 },
    { name: 'Missing', isin: 'OTHER', weight_pct: 2, status: 'unavailable', reason: 'Fewer than four consecutive reported EPS quarters.' },
  ] });

it('shows a compact stock table with diagnostic details available on hover', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockResolvedValueOnce(response(roster)).mockResolvedValueOnce(response(payload('Company One')));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<ValuationPanel />));
    expect(apiFetch).toHaveBeenCalledTimes(1);
    const select = host.querySelector('select')!;
    await act(async () => { select.value = '1'; select.dispatchEvent(new Event('change', { bubbles: true })); });
    expect(apiFetch).toHaveBeenCalledTimes(2);
    expect(vi.mocked(apiFetch).mock.calls[1][0]).toContain('/valuation/portfolios/1');
    expect(host.textContent).toContain('Company One');
    expect(host.textContent).toContain('80.00 EUR');
    expect(host.textContent).toContain('25.00%');
    expect(host.textContent).toContain('Insufficient data');
    expect(host.querySelector('[title*="four consecutive reported EPS quarters"]')).not.toBeNull();
    expect(host.querySelectorAll('thead th')).toHaveLength(6);
    expect(host.querySelector('details')).toBeNull();
    expect(host.textContent).not.toContain('Method and limitations');
    expect(host.textContent).not.toContain('Calculated');
    expect(host.textContent).toContain('Refresh');
    expect(vi.mocked(apiFetch).mock.calls.every(([, init]) => !init?.method || init.method === 'GET')).toBe(true);
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('does not show results from a previously selected portfolio or a late response', async () => {
  vi.mocked(apiFetch).mockReset();
  let finish!: (response: Response) => void;
  vi.mocked(apiFetch).mockResolvedValueOnce(response(roster))
    .mockReturnValueOnce(new Promise(resolve => { finish = resolve; }))
    .mockResolvedValueOnce(response(payload('Company Two')));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<ValuationPanel />));
    const select = host.querySelector('select')!;
    await act(async () => { select.value = '1'; select.dispatchEvent(new Event('change', { bubbles: true })); });
    await act(async () => { select.value = '2'; select.dispatchEvent(new Event('change', { bubbles: true })); });
    expect(host.textContent).toContain('Company Two');
    await act(async () => finish(response(payload('Company One'))));
    expect(host.textContent).not.toContain('Company One');
    expect(host.textContent).toContain('Company Two');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('shows saved stocks even before valuations have been prepared', async () => {
  vi.mocked(apiFetch).mockReset();
  let finish!: (response: Response) => void;
  vi.mocked(apiFetch).mockResolvedValueOnce(response(roster)).mockResolvedValueOnce(response({ ready: false,
    portfolio_id: 1, method: {}, rows: [{ name: 'Apple', isin: 'US0378331005', weight_pct: 5,
      status: 'pending', reason: 'Valuation has not been prepared yet.' }] }))
    .mockReturnValueOnce(new Promise(resolve => { finish = resolve; }));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<ValuationPanel />));
    const select = host.querySelector('select')!;
    await act(async () => { select.value = '1'; select.dispatchEvent(new Event('change', { bubbles: true })); });
    expect(host.textContent).toContain('Apple');
    expect(host.textContent).toContain('Pending');
    expect(host.textContent).not.toContain('portfolio_valuation_daily');
    expect(host.querySelector('table')).not.toBeNull();
    expect(apiFetch).toHaveBeenCalledTimes(3);
    expect(vi.mocked(apiFetch).mock.calls[2]).toEqual([expect.stringContaining('/portfolios/1/refresh'), { method: 'POST' }]);
    await act(async () => finish(response(payload('Apple'))));
    expect(host.textContent).toContain('80.00 EUR');
    expect(host.textContent).not.toContain('Pending');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('Refresh recalculates the selected portfolio instead of rereading an unchanged bundle', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockImplementation(async (url, init) => {
    if (String(url).endsWith('/portfolios')) return response(roster);
    return response(payload(init?.method === 'POST' ? 'Recalculated' : 'Original'));
  });
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<ValuationPanel />));
    const select = host.querySelector('select')!;
    await act(async () => { select.value = '1'; select.dispatchEvent(new Event('change', { bubbles: true })); });
    expect(host.textContent).toContain('Original');
    await act(async () => [...host.querySelectorAll('button')].find(button => button.textContent === 'Refresh')!.click());
    expect(vi.mocked(apiFetch).mock.calls).toContainEqual([expect.stringContaining('/portfolios/1/refresh?force=true'), { method: 'POST' }]);
    expect(host.textContent).toContain('Recalculated');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('displays dashboard account names and requests their linked Fixed model', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockResolvedValueOnce(response([
    { id: 7, name: 'Bustelberg Offensief', account: 'BUS_Offensief_Dyn' },
    { id: 7, name: 'Another dashboard account', account: 'SECOND_DYN' },
    { id: null, name: 'Unlinked account', account: 'UNLINKED', unavailable_reason: 'Connect model' },
  ])).mockResolvedValueOnce(response(payload('Company')));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<ValuationPanel />));
    const select = host.querySelector('select')!;
    expect(select.options[1].textContent).toBe('Bustelberg Offensief');
    expect(select.options[3].disabled).toBe(true);
    expect(select.options[3].textContent).toContain('no linked AIRS model');
    await act(async () => { select.value = 'SECOND_DYN'; select.dispatchEvent(new Event('change', { bubbles: true })); });
    expect(select.value).toBe('SECOND_DYN');
    expect(vi.mocked(apiFetch).mock.calls[1][0]).toContain('/valuation/portfolios/7');
    expect(host.textContent).toContain('Company');
  } finally { await act(async () => root.unmount()); host.remove(); }
});
