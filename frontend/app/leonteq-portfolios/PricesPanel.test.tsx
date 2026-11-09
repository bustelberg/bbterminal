// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import PricesPanel, { type PriceResult } from './PricesPanel';
import { refreshRequest } from './refreshProgress';

vi.mock('./refreshProgress', () => ({ refreshRequest: vi.fn() }));

const result: PriceResult = {
  success: true, checked_at: '2026-11-09T10:00:00Z', threshold_pct: 5,
  summary: { portfolios: 1, yahoo_symbols: 1, checked: 1, alerts: 1, unavailable: 1, cash: 0 }, errors: [],
  portfolios: [{ leonteq_name: 'Fund', airs_name: 'Dynamic', airs_portfolio_id: 1, rows: [
    { source: 'Leonteq', name: 'Example', isin: 'ONE', symbol: 'EXAMPLE', source_price: 106,
      source_currency: 'USD', source_date: '2026-11-06', yahoo_price: 100, yahoo_currency: 'USD',
      yahoo_date: '2026-11-06', difference_pct: 6, alert: true, status: 'alert', reason: 'Difference exceeds 5%.' },
    { source: 'AIRS', name: 'Unmapped', isin: 'TWO', symbol: null, source_price: 10,
      source_currency: 'EUR', source_date: '2026-11-06', yahoo_price: null, yahoo_currency: null,
      yahoo_date: null, difference_pct: null, alert: false, status: 'unavailable', reason: 'No verified Yahoo symbol.' },
  ] }],
};

it('checks every linked portfolio with one button and retains results after a failed retry', async () => {
  let finish!: (response: Response) => void;
  vi.mocked(refreshRequest).mockReturnValueOnce(new Promise(resolve => { finish = resolve; }));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PricesPanel />));
    expect(refreshRequest).not.toHaveBeenCalled();
    const button = host.querySelector('button')!;
    await act(async () => button.click());
    expect(button.disabled).toBe(true);
    expect(refreshRequest).toHaveBeenCalledOnce();
    expect(refreshRequest).toHaveBeenCalledWith(expect.stringContaining('/api/leonteq/price-checks'), undefined, 'Prices · Check all linked portfolios');
    await act(async () => finish(new Response(JSON.stringify(result))));
    expect(button.disabled).toBe(false);
    expect(host.textContent).toContain('1 price differences over 5%');
    expect(host.textContent).toContain('+6.00%');
    expect(host.textContent).toContain('6 nov 2026');
    expect(host.textContent).toContain('No verified Yahoo symbol.');
    expect(host.querySelector('[aria-label="All price checks passed"]')).toBeNull();
    vi.mocked(refreshRequest).mockRejectedValueOnce(new Error('Source unavailable'));
    await act(async () => button.click());
    expect(host.textContent).toContain('Source unavailable');
    expect(host.textContent).toContain('+6.00%');
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('marks a passing portfolio green and hides its empty table in issues-only mode', async () => {
  const passed = structuredClone(result);
  passed.errors = ['Leonteq holdings could not be refreshed for Another portfolio.'];
  passed.portfolios[0].rows = passed.portfolios[0].rows.map(row => ({ ...row, status: 'ok', alert: false, difference_pct: 0, reason: 'Within 5%.' }));
  vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify(passed)));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PricesPanel />));
    await act(async () => host.querySelector('button')!.click());
    expect(host.querySelector('[aria-label="All price checks passed"]')).not.toBeNull();
    expect(host.querySelector('h3')?.textContent).toContain('LeonteqFund');
    expect(host.querySelector('h3')?.textContent).toContain('AIRSDynamic');
    expect(host.querySelector('table')).not.toBeNull();
    await act(async () => host.querySelector<HTMLInputElement>('input[type="checkbox"]')!.click());
    expect(host.querySelector('table')).toBeNull();
    expect(host.querySelector('[aria-label="All price checks passed"]')).not.toBeNull();
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('rechecks one holding and updates the row and counts without replacing other rows', async () => {
  vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify(result)));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PricesPanel />));
    await act(async () => host.querySelector('button')!.click());
    const updated = structuredClone(result);
    updated.portfolios[0].rows = [{ ...result.portfolios[0].rows[1], symbol: 'FOUND', yahoo_price: 10,
      yahoo_currency: 'EUR', yahoo_date: '2026-11-06', difference_pct: 0, status: 'ok', reason: 'Within 5%.' }];
    vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify(updated)));
    const button = host.querySelector<HTMLButtonElement>('[aria-label="Recheck AIRS Unmapped"]')!;
    await act(async () => button.click());
    const request = vi.mocked(refreshRequest).mock.calls[1];
    expect(JSON.parse(String(request[1]?.body))).toEqual({ name: 'Fund', airs_portfolio_id: 1,
      source: 'AIRS', isin: 'TWO', holding_name: 'Unmapped' });
    expect(host.querySelector('a[href="https://finance.yahoo.com/quote/FOUND/"]')?.textContent).toBe('Unmapped');
    expect(host.textContent).toContain('+6.00%');
    expect(host.textContent).toContain('0 uncheckable');
    expect(host.querySelector('a[href="https://finance.yahoo.com/quote/FOUND/"]')).not.toBeNull();
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('collapses and deduplicates refresh notices without covering the portfolio tables', async () => {
  vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify({ ...result,
    errors: ['Source unavailable', 'Source unavailable'], warnings: ['Mappings could not be saved'] })));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PricesPanel />));
    await act(async () => host.querySelector('button')!.click());
    expect(host.querySelector('details')?.open).toBe(false);
    expect(host.querySelector('summary')?.textContent).toContain('2 refresh notices');
    expect(host.querySelectorAll('details li')).toHaveLength(2);
    expect(host.querySelector('table')).not.toBeNull();
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('puts matching ISINs on one row with three price columns and dated Yahoo references', async () => {
  const paired = structuredClone(result);
  paired.portfolios[0].rows.push({ ...paired.portfolios[0].rows[0], source: 'AIRS', name: 'Different source name',
    source_price: 103, source_date: '2026-11-05', yahoo_price: 102, yahoo_date: '2026-11-05',
    difference_pct: 0.98, alert: false, status: 'ok', reason: 'Within 5%.' });
  vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify(paired)));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PricesPanel />));
    await act(async () => host.querySelector('button')!.click());
    expect([...host.querySelectorAll('thead th')].map(th => th.textContent)).toEqual([
      'Instrument / ISIN', 'Leonteq', 'AIRS', 'Yahoo Finance',
      'Difference vs Yahoo', 'Result / Recheck']);
    expect(host.querySelectorAll('tbody tr')).toHaveLength(2);
    const cells = host.querySelector('tbody tr')!.querySelectorAll('td');
    expect(cells[0].textContent).toContain('106.00 USD');
    expect(cells[1].textContent).toContain('103.00 USD');
    expect(cells[2].textContent).toContain('For Leonteq');
    expect(cells[2].textContent).toContain('For AIRS');
    expect(cells[2].textContent).toContain('5 nov 2026');
    const filter = host.querySelector<HTMLInputElement>('input[type="checkbox"]')!;
    await act(async () => filter.click());
    expect(host.querySelector('tbody tr')!.textContent).toContain('103.00 USD');
    expect(host.querySelectorAll('tbody tr')[1].textContent).toContain('Not reported');
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});
