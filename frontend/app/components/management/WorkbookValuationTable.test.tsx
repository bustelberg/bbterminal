// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import WorkbookValuationTable from './WorkbookValuationTable';
import { apiFetch } from '../../../lib/apiFetch';
import type { Workbook } from './valuationWorkbook';
import { columnsForPanel } from './valuationWorkbookColumns';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const workbook: Workbook = { portfolio_id: 7, as_of: '2026-11-16',
  membership_legend: [
    { code: 'BB', name: 'Bustelberg', scope: 'All standard profiles', source: 'AIRS', status: 'complete',
      portfolios: [{ name: 'Bustelberg Defensief', as_of: '2026-11-13', available: true },
        { name: 'Bustelberg Offensief', as_of: '2026-11-13', available: true }] },
    { code: 'BB2', name: 'Bustelberg 2.0 Offensief', scope: 'Offensief only', source: 'AIRS', status: 'complete',
      portfolios: [{ name: 'Bustelberg 2.0 Offensief', as_of: '2026-11-13', available: true }] },
    { code: 'FTS', name: 'FamilieTopSelectie', scope: 'Leonteq holdings', source: 'Leonteq', status: 'complete',
      portfolios: [{ name: 'FamilieTopSelectie', as_of: '2026-11-13', available: true }] },
  ],
  parameters: { years: 3, safety_margin: .2, bear_growth_delta: -.05,
    bear_multiple_delta: -.25, bull_growth_delta: .05, bull_multiple_delta: .25 },
  rows: [{ isin: 'EXACT', ticker: 'ABC', selections: ['BB', 'FTS'], seed_source: 'test.xlsx',
    membership_profiles: { BB: ['Bustelberg Defensief'], FTS: ['FamilieTopSelectie'] }, membership_complete: true,
    quote: { price: 10000, currency: 'GBp', price_date: '2026-11-13', source: 'Yahoo Finance' },
    assumptions: { growth: .1, fair_multiple: 20, exit_multiple: 18, review_required: false },
    forecast: { fy1: 10, fy2: 12, fy1_date: '2026-12-31', fy2_date: '2027-12-31', currency: 'GBP' } }],
};

it('separates blue source values from yellow conversions and preserves green analyst edits', async () => {
  vi.mocked(apiFetch).mockImplementation(async (_url, init) => {
    const saved = init?.method === 'PUT' ? { portfolio_id: 7, ...JSON.parse(String(init.body)), updated_at: '2026-11-16T12:00:00Z' } : null;
    return new Response(JSON.stringify(saved ?? workbook));
  });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} portfolioName="Portfolio"
      rows={[{ isin: 'EXACT', name: 'Company', price: 100, currency: 'USD', price_date: '2026-11-13' }]} />));
    expect([...host.querySelectorAll('nav button')].map(button => button.textContent)).toEqual([
      'Onderneming', 'Koers & winst', 'Waardering & koopzone', 'Scenario 2029', 'Bron & controle', 'Afkortingen', 'Parameters',
    ]);
    expect(host.querySelectorAll('thead th')).toHaveLength(4);
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'Koers & winst')!.click());
    expect(host.querySelector('tbody [data-column="price"]')?.textContent).toBe('100.00');
    expect(host.querySelector('tbody [data-column="price"]')?.getAttribute('data-kind')).toBe('calculation');
    expect(host.querySelector('tbody [data-column="price"]')?.className).toContain('bg-yellow-500/10');
    expect(host.querySelector('tbody [data-column="raw_price"]')?.textContent).toBe('10000.00');
    expect(host.querySelector('tbody [data-column="raw_price"]')?.getAttribute('data-kind')).toBe('data');
    expect(host.querySelector('tbody [data-column="raw_price"]')?.className).toContain('bg-blue-500/10');
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'Waardering & koopzone')!.click());
    expect(host.querySelector('thead [data-column="fair"]')?.getAttribute('title')).toContain('Click to see the calculation');
    expect(host.textContent).toContain('Buy signal');
    expect(host.querySelector('tbody [data-column="fair_multiple"]')?.className).toContain('bg-green-500/10');
    await act(async () => [...host.querySelectorAll('button')].find(button => button.textContent === 'Company')!.click());
    const form = host.querySelector<HTMLFormElement>('[role="dialog"] form')!;
    form.querySelector<HTMLInputElement>('input[name="growth"]')!.value = '0';
    form.querySelector<HTMLInputElement>('input[name="fair_multiple"]')!.value = '14';
    await act(async () => { form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })); });
    const [url, options] = vi.mocked(apiFetch).mock.calls.at(-1)!;
    expect(String(url)).toContain('/portfolios/7/workbook');
    expect(options?.method).toBe('PUT');
    const payload = JSON.parse(String(options?.body));
    expect(payload.companies.EXACT.growth).toBe(0);
    expect(payload.companies.EXACT.fair_multiple).toBe(14);
    expect(payload.companies.EXACT.price).toBeUndefined();
    expect(host.querySelector('[role="dialog"]')).toBeNull();
    expect(host.textContent).toContain('14.00×');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('preserves drag-selected tab text while allowing ordinary clicks and keyboard activation', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify(workbook)));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  const selection = document.getSelection()!;
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} portfolioName="Portfolio" rows={[]} />));
    const tab = [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'Koers & winst')!;
    const range = document.createRange(); range.selectNodeContents(tab);
    selection.removeAllRanges(); selection.addRange(range);
    await act(async () => { tab.dispatchEvent(new MouseEvent('click', { bubbles: true, detail: 1 })); });
    expect(host.querySelector('nav button[aria-pressed="true"]')?.textContent).toBe('Onderneming');
    expect(selection.toString()).toBe('Koers & winst');
    await act(async () => { tab.dispatchEvent(new MouseEvent('click', { bubbles: true, detail: 0 })); });
    expect(host.querySelector('nav button[aria-pressed="true"]')?.textContent).toBe('Koers & winst');
    selection.removeAllRanges();
    const other = [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'Onderneming')!;
    await act(async () => { other.dispatchEvent(new MouseEvent('click', { bubbles: true, detail: 1 })); });
    expect(other.getAttribute('aria-pressed')).toBe('true');
  } finally { selection.removeAllRanges(); await act(async () => root.unmount()); host.remove(); }
});

it('explains current portfolio shorthand in Afkortingen and shows full membership names on hover', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify(workbook)));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} portfolioName="Portfolio" rows={[{ isin: 'EXACT', name: 'Company' }]} />));
    expect(host.querySelector('td[data-column="selections"]')?.textContent).toBe('BBFTS');
    expect(host.querySelector('td[data-column="selections"] [title^="BB = Bustelberg"]')?.getAttribute('title')).toContain('Bustelberg Defensief');
    expect(host.textContent).not.toContain('Selections (Excel)');
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'Afkortingen')!.click());
    const legend = host.querySelector('[aria-label="Portfolio abbreviations"]')!;
    expect(legend.textContent).toContain('Bustelberg 2.0 Offensief');
    expect(legend.textContent).toContain('FamilieTopSelectie');
    expect(legend.textContent).toContain('Leonteq');
    expect(legend.textContent).toContain('2026-11-13');
    expect(legend.textContent).toContain('at least one');
    expect(apiFetch).toHaveBeenCalledTimes(2);
    expect(vi.mocked(apiFetch).mock.calls.filter(([url]) => String(url).endsWith('/workbook'))).toHaveLength(1);
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('shows source coverage once instead of labelling every confirmed membership partial', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify({ ...workbook,
    membership_legend: workbook.membership_legend!.map(item => item.code === 'FTS' ? { ...item, status: 'partial' } : item),
    rows: [{ ...workbook.rows[0], membership_complete: false, membership_unknown_codes: [] },
      { ...workbook.rows[0], isin: 'OTHER', selections: [], membership_complete: false, membership_unknown_codes: ['FTS'] }],
  })));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} portfolioName="Portfolio"
      rows={[{ isin: 'EXACT', name: 'Confirmed company' }, { isin: 'OTHER', name: 'Unknown company' }]} />));
    const cells = [...host.querySelectorAll('td[data-column="selections"]')];
    expect(cells[0].textContent).toBe('BBFTS');
    expect(cells[1].textContent).toBe('Not confirmed');
    expect(host.textContent).not.toContain('Partial coverage');
    const links = [...host.querySelectorAll<HTMLButtonElement>('button')].filter(button => button.textContent === 'Coverage details');
    expect(links).toHaveLength(1);
    expect(links[0].title).toContain('FTS (FamilieTopSelectie)');
    await act(async () => links[0].click());
    expect(host.querySelector('[aria-label="Portfolio abbreviations"] [role="status"]')?.textContent).toContain('Displayed codes are confirmed');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('switches panels without refetching, keeps search/sort, and links formulas to blue/green inputs', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify(workbook)));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} portfolioName="Portfolio"
      rows={[{ isin: 'EXACT', name: 'Company' }, { isin: 'OTHER', name: 'Other' }]} />));
    const search = host.querySelector<HTMLInputElement>('input[type="search"]')!;
    const navigation = host.querySelector('nav[aria-label="Valuation panels"]')!;
    const viewport = host.querySelector('[data-valuation-viewport]')!;
    await act(async () => {
      const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!;
      setter.call(search, 'Company'); search.dispatchEvent(new Event('input', { bubbles: true }));
      const sort = host.querySelector<HTMLSelectElement>('select')!;
      sort.value = 'cagr'; sort.dispatchEvent(new Event('change', { bubbles: true }));
    });
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'Waardering & koopzone')!.click());
    await act(async () => host.querySelector<HTMLButtonElement>('th[data-column="fair"] button')!.click());
    const formula = host.querySelector('[aria-label="Formula details"]')!;
    await act(async () => formula.querySelector<HTMLButtonElement>('button[data-input="fy1"]')!.dispatchEvent(new MouseEvent('click', { bubbles: true })));
    expect(host.querySelector('nav button[aria-pressed="true"]')?.textContent).toBe('Koers & winst');
    expect(host.querySelector('th[data-column="fy1"]')?.className).toContain('ring-2');
    expect(host.querySelector('td[data-column="fy1"]')?.getAttribute('data-kind')).toBe('data');
    expect(host.querySelector<HTMLInputElement>('input[type="search"]')!.value).toBe('Company');
    expect(host.querySelector<HTMLSelectElement>('select')!.value).toBe('cagr');
    expect(host.querySelectorAll('tbody tr')).toHaveLength(1);
    for (const [label, panel] of [['Onderneming', 'company'], ['Waardering & koopzone', 'valuation'], ['Scenario 2029', 'scenario'], ['Bron & controle', 'sources'], ['Afkortingen', 'memberships'], ['Parameters', 'parameters']] as const) {
      await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === label)!.click());
      expect(host.querySelector('nav[aria-label="Valuation panels"]')).toBe(navigation);
      expect(navigation.nextElementSibling).toBe(viewport);
      expect(host.querySelector('[data-valuation-viewport]')).toBe(viewport);
      expect(host.querySelector('input[type="search"]')).toBe(search);
      expect(search.disabled).toBe(panel === 'memberships' || panel === 'parameters');
      if (panel !== 'memberships' && panel !== 'parameters') expect(host.querySelectorAll('thead th')).toHaveLength(columnsForPanel(panel).length);
    }
    expect(apiFetch).toHaveBeenCalledTimes(2);
    expect(vi.mocked(apiFetch).mock.calls.filter(([url]) => String(url).endsWith('/workbook'))).toHaveLength(1);
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('edits all shared parameters in one tab and updates the scenario year without losing other inputs', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockImplementation(async (_url, init) => new Response(JSON.stringify(init?.method === 'PUT'
    ? { portfolio_id: 7, ...JSON.parse(String(init.body)), updated_at: '2026-11-16T12:00:00Z' } : workbook)));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} portfolioName="Portfolio" rows={[{ isin: 'EXACT', name: 'Company' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'Waardering & koopzone')!.click());
    expect(host.querySelector('input[name="safety_margin"]')).toBeNull();
    await act(async () => host.querySelector<HTMLButtonElement>('th[data-column="buyPrice"] button')!.click());
    await act(async () => host.querySelector<HTMLButtonElement>('[data-input="safety_margin"]')!.click());
    expect(host.querySelector('nav button[aria-pressed="true"]')?.textContent).toBe('Parameters');
    expect(host.querySelectorAll('details input')).toHaveLength(6);
    host.querySelector<HTMLInputElement>('input[name="safety_margin"]')!.value = '25';
    await act(async () => { host.querySelector('details form')!.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })); });
    let saved = JSON.parse(String(vi.mocked(apiFetch).mock.calls.at(-1)![1]?.body));
    expect(saved.parameters).toEqual({ ...workbook.parameters, safety_margin: .25 });
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'Scenario 2029')!.click());
    expect(host.querySelector('input[name="years"]')).toBeNull();
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'Parameters')!.click());
    host.querySelector<HTMLInputElement>('input[name="years"]')!.value = '5';
    await act(async () => { host.querySelector('details form')!.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })); });
    saved = JSON.parse(String(vi.mocked(apiFetch).mock.calls.at(-1)![1]?.body));
    expect(saved.parameters).toEqual({ ...workbook.parameters, safety_margin: .25, years: 5 });
    expect([...host.querySelectorAll('nav button')].some(button => button.textContent === 'Scenario 2031')).toBe(true);
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('patches refreshed source cells without restarting the workbook read or changing tabs', async () => {
  vi.mocked(apiFetch).mockImplementation(async () => new Response(JSON.stringify(workbook)));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  const props = { portfolioId: 7, nl: false, portfolioName: 'Portfolio', rows: [{ name: 'Company', isin: 'EXACT' }] };
  try {
    await act(async () => root.render(<WorkbookValuationTable {...props} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'Koers & winst')!.click());
    const reads = vi.mocked(apiFetch).mock.calls.length;
    const update = { ...workbook, rows: [{ ...workbook.rows[0], selections: [], quote: {
      price: 23456, currency: 'GBp', price_date: '2026-11-16', source: 'Yahoo Finance' } }] };
    await act(async () => root.render(<WorkbookValuationTable {...props} workbookUpdate={update} />));
    expect(apiFetch).toHaveBeenCalledTimes(reads);
    expect(host.querySelector('nav[aria-label="Valuation panels"] button[aria-pressed="true"]')?.textContent).toBe('Koers & winst');
    expect(host.textContent).toContain('23456.00');
    expect(host.textContent).not.toContain('Loading valuation inputs');
  } finally { await act(async () => root.unmount()); host.remove(); }
});


it('replaces a stalled initial read with a retry control', async () => {
  vi.useFakeTimers();
  vi.mocked(apiFetch).mockImplementation(() => new Promise(() => {}));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} portfolioName="Portfolio" rows={[{ name: 'Company', isin: 'EXACT' }]} />));
    expect(host.textContent).toContain('Loading valuation inputs');
    await act(async () => vi.advanceTimersByTime(30001));
    expect(host.textContent).not.toContain('Loading valuation inputs');
    expect(host.textContent).toContain('Retry valuation inputs');
    expect(host.textContent).toContain('took too long');
  } finally { await act(async () => root.unmount()); host.remove(); vi.useRealTimers(); }
});

it('links stock prices to their actual Yahoo listing in a new tab', async () => {
  vi.mocked(apiFetch).mockImplementation(async () => new Response(JSON.stringify({ ...workbook,
    rows: [{ ...workbook.rows[0], quote: { ...workbook.rows[0].quote, symbol: 'ABC.L' } }] })));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} portfolioName="Portfolio" rows={[{ name: 'Company', isin: 'EXACT' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'Koers & winst')!.click());
    const link = host.querySelector<HTMLAnchorElement>('td[data-column="raw_price"] a')!;
    expect(link.href).toBe('https://finance.yahoo.com/quote/ABC.L/');
    expect(link.target).toBe('_blank');
    expect(link.rel).toContain('noopener');
    expect(host.querySelector('td[data-column="price"] a')?.textContent).toBe('100.00');
  } finally { await act(async () => root.unmount()); host.remove(); }
});


it('explains fair value using company inputs while retaining the expanded formula', async () => {
  vi.mocked(apiFetch).mockImplementation(async () => new Response(JSON.stringify(workbook)));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} portfolioName="Portfolio" rows={[{ name: 'Company', isin: 'EXACT' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'Waardering & koopzone')!.click());
    await act(async () => host.querySelector<HTMLButtonElement>('th[data-column="fair"] button')!.click());
    const formula = host.querySelector('[aria-label="Formula details"]')!;
    expect(formula.textContent).toContain('Multiply the selected per-share valuation basis by your fair multiple');
    expect(formula.querySelector('[data-input="fair_multiple"]')?.textContent).toContain('20×');
    expect(formula.querySelector('[data-input="fy1"]')?.textContent).toContain('10');
    expect(formula.querySelector('[data-input="manual_basis"]')).toBeNull();
    expect(formula.textContent).toContain('GBP');
    expect(formula.querySelector('details')?.open).toBe(false);
    expect(formula.querySelector('details')?.textContent).toContain('fair_multiple');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('explains a manual basis without displaying unused forecast inputs', async () => {
  vi.mocked(apiFetch).mockImplementation(async () => new Response(JSON.stringify({ ...workbook,
    rows: [{ ...workbook.rows[0], assumptions: { ...workbook.rows[0].assumptions,
      basis_kind: 'manual', manual_basis: 5, basis_currency: 'GBP' } }] })));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} portfolioName="Portfolio" rows={[{ name: 'Company', isin: 'EXACT' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'Waardering & koopzone')!.click());
    await act(async () => host.querySelector<HTMLButtonElement>('td[data-column="fair"] button')!.click());
    const formula = host.querySelector('[aria-label="Formula details"]')!;
    expect(formula.textContent).toContain('100 GBP');
    expect(formula.textContent).toContain('Forecast EPS and month weights are not used');
    expect(formula.querySelector('[data-input="manual_basis"]')?.textContent).toContain('5');
    expect(formula.querySelector('[data-input="fy1"]')).toBeNull();
    expect(formula.querySelector('[data-input="basis_fx"]')).toBeNull();
    expect(formula.querySelector('select')?.value).toBe('EXACT');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('separates source holdings dates from retrieval timestamps and labels missing dates', async () => {
  const retrieved = '2026-11-16T15:56:28+00:00';
  const data: Workbook = { ...workbook, membership_legend: [{ code: 'FTS', name: 'FamilieTopSelectie',
    scope: 'Leonteq holdings', source: 'Leonteq', status: 'complete', portfolios: [{
      name: 'FamilieTopSelectie', as_of: null, retrieved_at: retrieved, available: true }] }] };
  vi.mocked(apiFetch).mockImplementation(async () => new Response(JSON.stringify(data)));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} portfolioName="Portfolio" rows={[{ name: 'Company', isin: 'EXACT' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'Afkortingen')!.click());
    const legend = host.querySelector('[aria-label="Portfolio abbreviations"]')!;
    expect(legend.textContent).toContain('Last retrieved');
    const cells = legend.querySelectorAll('tbody tr td');
    expect(cells[4].textContent).toBe('Date unavailable');
    expect(cells[5].querySelector('time')?.getAttribute('datetime')).toBe(retrieved);
    expect(cells[6].textContent).toBe('Complete');
    expect(legend.textContent).toContain('does not confirm freshness');
  } finally { await act(async () => root.unmount()); host.remove(); }
});


