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
      yahoo_name: 'Example Corporation', yahoo_date: '2026-11-06', difference_pct: 6, alert: true, status: 'alert', reason: 'Difference exceeds 5%.' },
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
    expect(host.textContent).not.toContain('prices checked');
    expect(host.textContent).not.toContain('cash rows excluded');
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

it('keeps one table and shows an empty state when the filter excludes all rows', async () => {
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
    expect(host.querySelectorAll('table')).toHaveLength(1);
    expect(host.querySelector<HTMLInputElement>('input[type="checkbox"]')!.checked).toBe(true);
    expect(host.textContent).toContain('No alerts or uncheckable prices.');
    await act(async () => host.querySelector<HTMLInputElement>('input[type="checkbox"]')!.click());
    expect(host.querySelector('tbody tr > :nth-child(4)')?.textContent).toContain('LeonteqFund');
    expect(host.querySelector('tbody tr > :nth-child(4)')?.textContent).toContain('AIRSDynamic');
    expect(host.querySelector('table')).not.toBeNull();
    await act(async () => host.querySelector<HTMLInputElement>('input[type="checkbox"]')!.click());
    expect(host.textContent).toContain('No alerts or uncheckable prices.');
    expect(host.querySelectorAll('table')).toHaveLength(1);
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('filters one combined table across portfolios and restores passing rows without fetching', async () => {
  const combined = structuredClone(result);
  combined.portfolios.push({ leonteq_name: 'Second fund', airs_name: 'Second Dynamic', airs_portfolio_id: 2,
    rows: [{ ...combined.portfolios[0].rows[0], alert: false, status: 'ok', difference_pct: 0, reason: 'Within 5%.' }] });
  vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify(combined)));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PricesPanel />));
    await act(async () => host.querySelector('button')!.click());
    expect(host.querySelectorAll('table')).toHaveLength(1);
    expect(host.querySelectorAll('tbody tr')).toHaveLength(2);
    const unique = host.querySelectorAll<HTMLInputElement>('input[type="checkbox"]')[1];
    await act(async () => host.querySelector<HTMLInputElement>('input[type="checkbox"]')!.click());
    expect(unique.checked).toBe(true);
    expect(host.textContent).toContain('+1 portfolios');
    await act(async () => unique.click());
    expect(host.querySelectorAll('tbody tr')).toHaveLength(3);
    expect(host.querySelectorAll('tbody tr')[2].textContent).toContain('Second Dynamic');
    const filter = host.querySelector<HTMLInputElement>('input[type="checkbox"]')!;
    await act(async () => filter.click());
    expect(host.querySelectorAll('tbody tr')).toHaveLength(2);
    expect(host.querySelector('tbody')?.textContent).not.toContain('Second Dynamic');
    await act(async () => filter.click());
    expect(host.querySelectorAll('tbody tr')).toHaveLength(3);
    expect(refreshRequest).toHaveBeenCalledOnce();
    await act(async () => unique.click());
    expect(host.querySelectorAll('tbody tr')).toHaveLength(2);
    vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify({ ...combined,
      portfolios: [{ ...combined.portfolios[0], rows: [combined.portfolios[0].rows[0]] }] })));
    vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify({ ...combined,
      portfolios: [combined.portfolios[1]] })));
    await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="Recheck Example in Fund"]')!.click());
    expect(vi.mocked(refreshRequest).mock.calls.slice(1).map(call => JSON.parse(String(call[1]?.body)).airs_portfolio_id)).toEqual([1, 2]);
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
    await act(async () => host.querySelector<HTMLInputElement>('input[type="checkbox"]')!.click());
    const updated = structuredClone(result);
    updated.portfolios[0].rows = [{ ...result.portfolios[0].rows[1], symbol: 'FOUND', yahoo_price: 10,
      yahoo_name: 'Resolved Yahoo name', yahoo_currency: 'EUR', yahoo_date: '2026-11-06', difference_pct: 0, status: 'ok', reason: 'Within 5%.' }];
    vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify(updated)));
    const button = host.querySelector<HTMLButtonElement>('[aria-label="Recheck Unmapped in Fund"]')!;
    await act(async () => button.click());
    const request = vi.mocked(refreshRequest).mock.calls[1];
    expect(JSON.parse(String(request[1]?.body))).toEqual({ name: 'Fund', airs_portfolio_id: 1,
      source: 'AIRS', isin: 'TWO', holding_name: 'Unmapped' });
    expect(host.querySelector('a[href="https://finance.yahoo.com/quote/FOUND/"]')?.textContent).toBe('Resolved Yahoo name');
    expect(host.textContent).toContain('+6.00%');
    expect(host.textContent).not.toContain('uncheckable comparisons');
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
    expect(host.querySelector('details')?.querySelectorAll('li')).toHaveLength(2);
    expect(host.querySelector('table')).not.toBeNull();
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('shows grouped uncheckable causes with full instrument and dated evidence', async () => {
  const incomplete = structuredClone(result);
  incomplete.portfolios[0].rows[1] = { ...incomplete.portfolios[0].rows[1],
    reason_code: 'listing_unresolved', reason: 'No verified Yahoo listing: OpenFIGI returned no security identity.' };
  incomplete.portfolios[0].rows.push({ ...incomplete.portfolios[0].rows[1], source: 'Leonteq' });
  incomplete.summary.unavailable = 2;
  vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify(incomplete)));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PricesPanel />));
    await act(async () => host.querySelector('button')!.click());
    const causes = host.querySelector('details')!;
    expect(causes.open).toBe(false);
    expect(causes.querySelector('summary')?.textContent).toContain('2 uncheckable comparisons');
    expect(causes.textContent).toContain('2 · listing unresolved');
    expect(causes.querySelectorAll('li')).toHaveLength(1);
    expect(causes.querySelector('li')?.textContent).toBe('Unmapped · No verified Yahoo listing · 2 comparisons');
    const evidence = causes.querySelector('li')?.title;
    expect(evidence).toContain('Unmapped · TWO · No Yahoo listing');
    expect(evidence).toContain('AIRS · Fund → Dynamic');
    expect(evidence).toContain('Leonteq · Fund → Dynamic');
    expect(evidence).toContain('Source: 10.00 EUR · 6 nov 2026');
    expect(evidence).toContain('OpenFIGI returned no security identity');
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('keeps bond source comparisons separate from Yahoo and counts the pair once after recheck', async () => {
  const mixed = structuredClone(result);
  const bond = { leonteq_name: 'ObligatieTopSelectie', airs_name: 'OBTopSelectie_DYN', airs_portfolio_id: 7,
    rows: ['Leonteq', 'AIRS'].map(source => ({ ...mixed.portfolios[0].rows[0], source,
      source_price: source === 'Leonteq' ? 106 : 100, yahoo_name: null, symbol: null,
      yahoo_price: null, yahoo_currency: null, yahoo_date: null,
      comparison_basis: 'leonteq_airs' as const, comparison_counted: source === 'Leonteq',
      reason: 'Leonteq vs AIRS: difference exceeds 5%.' })) };
  mixed.portfolios.push(bond);
  vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify(mixed)));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PricesPanel />));
    await act(async () => host.querySelector('button')!.click());
    expect(host.querySelectorAll('tbody tr')).toHaveLength(3);
    const row = host.querySelectorAll('tbody tr')[2];
    expect(row.children[2].textContent).toBe('Not used');
    expect(row.children[6].textContent).toBe('Not used');
    expect(row.children[7].textContent).toBe('Leonteq vs AIRS+6.00%');
    for (const source of bond.rows) {
      vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify({ ...mixed,
        portfolios: [{ ...bond, rows: [source] }] })));
    }
    await act(async () => row.querySelector<HTMLButtonElement>('button')!.click());
    expect(host.querySelectorAll('tbody tr')).toHaveLength(3);
    expect(vi.mocked(refreshRequest).mock.calls.slice(1).map(call => JSON.parse(String(call[1]?.body)).source)).toEqual(['Leonteq', 'AIRS']);
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
      'Leonteq name', 'AIRS name', 'Yahoo Finance name', 'Portfolio', 'Leonteq', 'AIRS', 'Yahoo Finance',
      'Difference', 'Result / Recheck']);
    expect(host.querySelectorAll('tbody tr')).toHaveLength(2);
    expect([...host.querySelector('tbody tr')!.children].slice(0, 3).map(cell => cell.textContent)).toEqual(['ExampleONE', 'Different source nameONE', 'Example CorporationEXAMPLE']);
    const cells = [...host.querySelector('tbody tr')!.querySelectorAll('td')].slice(3);
    expect(cells[0].textContent).toContain('106.00 USD');
    expect(cells[1].textContent).toContain('103.00 USD');
    expect(cells[2].textContent).toContain('For Leonteq');
    expect(cells[2].textContent).toContain('For AIRS');
    expect(cells[2].textContent).toContain('5 nov 2026');
    const filter = host.querySelector<HTMLInputElement>('input[type="checkbox"]')!;
    await act(async () => filter.click());
    expect(host.querySelector('tbody tr')!.textContent).toContain('103.00 USD');
    expect(host.querySelectorAll('tbody tr')[1].textContent).toContain('Not reported');
    expect(host.querySelectorAll('tbody tr')[0].querySelectorAll('button')).toHaveLength(1);
    vi.mocked(refreshRequest).mockRejectedValueOnce(new Error('Leonteq temporarily unavailable'));
    vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify({ ...paired,
      portfolios: [{ ...paired.portfolios[0], rows: [paired.portfolios[0].rows[2]] }] })));
    await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="Recheck Example in Fund"]')!.click());
    expect(vi.mocked(refreshRequest).mock.calls.slice(1).map(call => JSON.parse(String(call[1]?.body)).source)).toEqual(['Leonteq', 'AIRS']);
    expect(host.textContent).toContain('Leonteq temporarily unavailable');
    expect(host.querySelector<HTMLButtonElement>('[aria-label="Recheck Example in Fund"]')!.disabled).toBe(false);
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('shows the Leonteq HOYA price alongside AIRS despite their different ISINs', async () => {
  const hoya = structuredClone(result);
  hoya.portfolios[0].rows = [
    { ...result.portfolios[0].rows[0], source: 'Leonteq', name: 'HOYA CORP', isin: 'JP3837800006',
      symbol: '7741.T', source_price: 23860, source_currency: 'JPY', yahoo_price: 23860,
      yahoo_currency: 'JPY', yahoo_name: 'HOYA Corporation', difference_pct: 0, alert: false, status: 'ok' },
    { ...result.portfolios[0].rows[0], source: 'AIRS', name: 'HOYA Corporation', isin: 'JP3786800000',
      symbol: '7741.T', source_price: 27000, source_currency: 'JPY', yahoo_price: 23860,
      yahoo_currency: 'JPY', yahoo_name: 'HOYA Corporation', difference_pct: 13.16 },
  ];
  vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify(hoya)));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PricesPanel />));
    await act(async () => host.querySelector('button')!.click());
    expect(host.querySelectorAll('tbody tr')).toHaveLength(1);
    const row = host.querySelector('tbody tr')!;
    expect(row.textContent).not.toContain('Not reported');
    expect(row.children[0].textContent).toBe('HOYA CORPJP3837800006');
    expect(row.children[1].textContent).toBe('HOYA CorporationJP3786800000');
    expect(row.children[4].textContent).toContain('23,860.00 JPY');
    expect(row.children[5].textContent).toContain('27,000.00 JPY');
    expect(row.children[7].textContent).toContain('Leonteq vs Yahoo0.00%');
    expect(row.children[7].textContent).toContain('AIRS vs Yahoo+13.16%');
    for (const source of hoya.portfolios[0].rows) {
      vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify({ ...hoya,
        portfolios: [{ ...hoya.portfolios[0], rows: [source] }] })));
    }
    await act(async () => row.querySelector<HTMLButtonElement>('button')!.click());
    expect(vi.mocked(refreshRequest).mock.calls.slice(1).map(call => JSON.parse(String(call[1]?.body)).isin))
      .toEqual(['JP3837800006', 'JP3786800000']);
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});
