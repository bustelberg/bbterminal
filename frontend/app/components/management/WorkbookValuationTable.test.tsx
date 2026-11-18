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

it('saves company assumptions directly from cells and recalculates without opening a dialog', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockImplementation(async (_url, init) => new Response(JSON.stringify(init?.method === 'PUT'
    ? { ...JSON.parse(String(init.body)), updated_at: 'now' } : workbook)));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false}
      workbookData={workbook} rows={[{ isin: 'EXACT', name: 'Company' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'Waardering & koopzone')!.click());
    const growth = host.querySelector<HTMLInputElement>('td[data-column="growth"] input')!;
    expect(growth.value).toBe('10.0');
    await act(async () => { growth.focus(); growth.value = '0'; growth.blur(); });
    const saved = JSON.parse(String(vi.mocked(apiFetch).mock.calls.at(-1)![1]?.body));
    expect(saved.companies.EXACT.growth).toBe(0);
    expect(saved.companies.EXACT.exit_multiple).toBe(18);
    expect(saved.companies.EXACT.price).toBeUndefined();
    expect(host.querySelector('[role="dialog"]')).toBeNull();
    const previousFair = host.querySelector('td[data-column="fair"]')?.textContent;
    const multiple = host.querySelector<HTMLInputElement>('td[data-column="fair_multiple"] input')!;
    await act(async () => {
      multiple.focus(); multiple.value = '14';
      multiple.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
    });
    expect(JSON.parse(String(vi.mocked(apiFetch).mock.calls.at(-1)![1]?.body)).companies.EXACT.fair_multiple).toBe(14);
    expect(host.querySelector('td[data-column="fair"]')?.textContent).not.toBe(previousFair);
    const input = host.querySelector<HTMLInputElement>('td[data-column="fair_multiple"] input')!;
    const calls = vi.mocked(apiFetch).mock.calls.length;
    await act(async () => {
      input.focus(); input.value = '99';
      input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    });
    expect(input.value).toBe('14.0');
    expect(apiFetch).toHaveBeenCalledTimes(calls);
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('retains an inline draft when saving fails', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockResolvedValue(new Response('{}', { status: 500 }));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false}
      workbookData={workbook} rows={[{ isin: 'EXACT', name: 'Company' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'Waardering & koopzone')!.click());
    const input = host.querySelector<HTMLInputElement>('td[data-column="fair_multiple"] input')!;
    await act(async () => { input.focus(); input.value = '14'; input.blur(); });
    expect(input.value).toBe('14');
    expect(input.disabled).toBe(false);
    expect(host.querySelector('[role="alert"]')?.textContent).toContain('could not be saved');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it.each([
  [50, false, 'BUY', 'text-pos-400'],
  [90, false, 'CONSIDER', 'text-warn-400'],
  [110, false, 'DO NOT BUY', 'text-neg-400'],
  [50, true, 'REVIEW', 'text-fg-muted'],
])('colors the %s / review=%s buy signal while retaining its explanation', async (price, review, label, tone) => {
  const data: Workbook = { ...workbook, rows: [{ ...workbook.rows[0],
    quote: { price, currency: 'GBP', price_date: '2026-11-13' },
    assumptions: { basis_kind: 'manual', manual_basis: 5, basis_currency: 'GBP', fair_multiple: 20, review_required: review } }] };
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false}
      workbookData={data} rows={[{ isin: 'EXACT', name: 'Company' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'Waardering & koopzone')!.click());
    const signal = host.querySelector<HTMLButtonElement>('td[data-column="signal"] button')!;
    expect(signal.textContent).toBe(label);
    expect(signal.className).toContain(tone);
    await act(async () => signal.click());
    expect(host.querySelector('[aria-label="Formula details"]')).not.toBeNull();
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('anchors formula popups to the clicked cell within a scrolled table', async () => {
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false}
      workbookData={workbook} rows={[{ isin: 'EXACT', name: 'Company' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'Waardering & koopzone')!.click());
    const viewport = host.querySelector<HTMLElement>('[data-valuation-viewport]')!;
    Object.defineProperties(viewport, { clientWidth: { value: 1000 }, clientHeight: { value: 500 } });
    vi.spyOn(viewport, 'getBoundingClientRect').mockReturnValue(new DOMRect(100, 80, 1000, 500));
    viewport.scrollLeft = 900;
    viewport.scrollTop = 200;
    {
      const anchor = host.querySelector<HTMLButtonElement>('td[data-column="fair"] button')!;
      vi.spyOn(anchor, 'getBoundingClientRect').mockReturnValue(new DOMRect(780, 180, 50, 20));
      await act(async () => anchor.click());
      const popup = host.querySelector<HTMLElement>('[aria-label="Formula details"]')!;
      expect(popup.style.left).toBe('912px');
      expect(popup.style.top).toBe('212px');
      expect(popup.style.width).toBe('976px');
      expect(popup.style.maxHeight).toBe('476px');
      expect(viewport.scrollLeft).toBe(900);
      expect(viewport.scrollTop).toBe(200);
    }
    const header = host.querySelector<HTMLButtonElement>('th[data-column="fair"] button')!;
    await act(async () => header.click());
    expect(host.querySelector('[aria-label="Formula details"]')).toBeNull();
    const cell = host.querySelector<HTMLButtonElement>('td[data-column="fair"] button')!;
    await act(async () => cell.click());
    expect(host.querySelector('[aria-label="Formula details"]')).not.toBeNull();
    await act(async () => cell.click());
    expect(host.querySelector('[aria-label="Formula details"]')).toBeNull();
    await act(async () => cell.click());
    const other = host.querySelector<HTMLButtonElement>('td[data-column="buyPrice"] button')!;
    await act(async () => other.click());
    expect(host.querySelector('[aria-label="Formula details"] strong')?.textContent).toBe('Buy price');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('separates blue source values from yellow conversions and preserves green analyst edits', async () => {
  vi.mocked(apiFetch).mockImplementation(async (_url, init) => {
    const saved = init?.method === 'PUT' ? { portfolio_id: 7, ...JSON.parse(String(init.body)), updated_at: '2026-11-16T12:00:00Z' } : null;
    return new Response(JSON.stringify(saved ?? workbook));
  });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false}
      rows={[{ isin: 'EXACT', name: 'Company', price: 100, currency: 'USD', price_date: '2026-11-13' }]} />));
    expect([...host.querySelectorAll('nav button')].map(button => button.textContent)).toEqual([
      'Onderneming', 'Waardering & koopzone', 'Scenario 2029', 'EPS berekening', 'Bron & controle', 'Afkortingen', 'Parameters',
    ]);
    expect(host.querySelectorAll('thead th')).toHaveLength(4);
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'EPS berekening')!.click());
    expect(host.querySelector('tbody [data-column="price"]')?.textContent).toBe('100.00');
    expect(host.querySelector('tbody [data-column="price"]')?.getAttribute('data-kind')).toBe('calculation');
    expect(host.querySelector('tbody [data-column="price"]')?.className).toContain('bg-card');
    expect(host.querySelector('tbody [data-column="raw_price"]')?.textContent).toBe('10000.00');
    expect(host.querySelector('tbody [data-column="raw_price"]')?.getAttribute('data-kind')).toBe('data');
    expect(host.querySelector('tbody [data-column="raw_price"]')?.className).toContain('bg-card');
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'Waardering & koopzone')!.click());
    expect(host.querySelector('thead [data-column="fair"] button')?.textContent).toBe('Fair price');
    expect(host.textContent).toContain('Buy signal');
    expect(host.querySelector('tbody [data-column="fair_multiple"]')?.className).toContain('bg-card');
    expect(host.querySelector('td[data-column="company"] button')).toBeNull();
    const growth = host.querySelector<HTMLInputElement>('td[data-column="growth"] input')!;
    await act(async () => { growth.focus(); growth.value = '0'; growth.blur(); });
    const multiple = host.querySelector<HTMLInputElement>('td[data-column="fair_multiple"] input')!;
    await act(async () => { multiple.focus(); multiple.value = '14'; multiple.blur(); });
    const [url, options] = vi.mocked(apiFetch).mock.calls.at(-1)!;
    expect(String(url)).toContain('/portfolios/7/workbook');
    expect(options?.method).toBe('PUT');
    const payload = JSON.parse(String(options?.body));
    expect(payload.companies.EXACT.growth).toBe(0);
    expect(payload.companies.EXACT.fair_multiple).toBe(14);
    expect(payload.companies.EXACT.price).toBeUndefined();
    expect(host.querySelector('[role="dialog"]')).toBeNull();
    expect(host.querySelector<HTMLInputElement>('td[data-column="fair_multiple"] input')?.value).toBe('14.0');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('preserves drag-selected tab text while allowing ordinary clicks and keyboard activation', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify(workbook)));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  const selection = document.getSelection()!;
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} rows={[]} />));
    const tab = [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'EPS berekening')!;
    const range = document.createRange(); range.selectNodeContents(tab);
    selection.removeAllRanges(); selection.addRange(range);
    await act(async () => { tab.dispatchEvent(new MouseEvent('click', { bubbles: true, detail: 1 })); });
    expect(host.querySelector('nav button[aria-pressed="true"]')?.textContent).toBe('Onderneming');
    expect(selection.toString()).toBe('EPS berekening');
    await act(async () => { tab.dispatchEvent(new MouseEvent('click', { bubbles: true, detail: 0 })); });
    expect(host.querySelector('nav button[aria-pressed="true"]')?.textContent).toBe('EPS berekening');
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
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} rows={[{ isin: 'EXACT', name: 'Company' }]} />));
    expect(host.querySelector('td[data-column="selections"]')?.textContent).toBe('BBFTS');
    expect(host.querySelector('td[data-column="selections"] [title^="BB = Bustelberg"]')?.getAttribute('title')).toContain('Bustelberg Defensief');
    expect(host.textContent).not.toContain('Selections (Excel)');
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'Afkortingen')!.click());
    const legend = host.querySelector('[aria-label="Portfolio abbreviations"]')!;
    expect(legend.textContent).toContain('Bustelberg 2.0 Offensief');
    expect(legend.textContent).toContain('FamilieTopSelectie');
    expect(legend.textContent).toContain('Leonteq');
    expect(legend.textContent).toContain('2026-11-13');
    expect(legend.querySelectorAll('p')).toHaveLength(1);
    expect(legend.querySelector('p')?.textContent).toBe('Codes show which portfolios hold each company, using saved AIRS and Leonteq holdings.');
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
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false}
      rows={[{ isin: 'EXACT', name: 'Confirmed company' }, { isin: 'OTHER', name: 'Unknown company' }]} />));
    const cells = [...host.querySelectorAll('td[data-column="selections"]')];
    expect(cells[0].textContent).toBe('BBFTS');
    expect(cells[1].textContent).toBe('Not confirmed');
    expect(host.textContent).not.toContain('Partial coverage');
    const links = [...host.querySelectorAll<HTMLButtonElement>('button')].filter(button => button.textContent === 'Coverage details');
    expect(links).toHaveLength(1);
    expect(links[0].title).toContain('FTS (FamilieTopSelectie)');
    await act(async () => links[0].click());
    expect(host.querySelector('[aria-label="Portfolio abbreviations"] table')?.textContent).toContain('Partial');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('switches panels without refetching, keeps search/sort, and links formulas to blue/green inputs', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify(workbook)));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false}
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
    await act(async () => host.querySelector<HTMLButtonElement>('td[data-column="fair"] button')!.click());
    const formula = host.querySelector('[aria-label="Formula details"]')!;
    await act(async () => formula.querySelector<HTMLButtonElement>('button[data-input="fy1"]')!.dispatchEvent(new MouseEvent('click', { bubbles: true })));
    expect(host.querySelector('nav button[aria-pressed="true"]')?.textContent).toBe('EPS berekening');
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
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} rows={[{ isin: 'EXACT', name: 'Company' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(button => button.textContent === 'Waardering & koopzone')!.click());
    expect(host.querySelector('input[name="safety_margin"]')).toBeNull();
    await act(async () => host.querySelector<HTMLButtonElement>('td[data-column="buyPrice"] button')!.click());
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
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'EPS berekening')!.click());
    const reads = vi.mocked(apiFetch).mock.calls.length;
    const update = { ...workbook, rows: [{ ...workbook.rows[0], selections: [], quote: {
      price: 23456, currency: 'GBp', price_date: '2026-11-16', source: 'Yahoo Finance' } }] };
    await act(async () => root.render(<WorkbookValuationTable {...props} workbookUpdate={update} />));
    expect(apiFetch).toHaveBeenCalledTimes(reads);
    expect(host.querySelector('nav[aria-label="Valuation panels"] button[aria-pressed="true"]')?.textContent).toBe('EPS berekening');
    expect(host.textContent).toContain('23456.00');
    expect(host.textContent).not.toContain('Loading valuation inputs');
  } finally { await act(async () => root.unmount()); host.remove(); }
});


it('replaces a stalled initial read with a retry control', async () => {
  vi.useFakeTimers();
  vi.mocked(apiFetch).mockImplementation(() => new Promise(() => {}));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} rows={[{ name: 'Company', isin: 'EXACT' }]} />));
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
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} rows={[{ name: 'Company', isin: 'EXACT' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'EPS berekening')!.click());
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
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} rows={[{ name: 'Company', isin: 'EXACT' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'Waardering & koopzone')!.click());
    await act(async () => host.querySelector<HTMLButtonElement>('td[data-column="fair"] button')!.click());
    const formula = host.querySelector('[aria-label="Formula details"]')!;
    expect(formula.querySelectorAll('.katex-display')).toHaveLength(2);
    expect(formula.querySelector('[aria-label="Variables"]')?.textContent).toContain('Fair valuation multiple.');
    expect(formula.querySelectorAll('[aria-label="Variables"] dt')).toHaveLength(6);
    expect(formula.querySelector('[data-input="fair_multiple"]')?.getAttribute('title')).toBe('20');
    expect(formula.querySelector('[data-input="fy1"]')?.getAttribute('title')).toBe('10');
    expect(formula.querySelector('[data-input="manual_basis"]')).toBeNull();
    expect(formula.textContent).toContain('GBP');
    expect(formula.querySelector('details')?.open).toBe(false);
    expect(formula.querySelector('details')?.textContent).toContain('Edit inputs');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('explains a manual basis without displaying unused forecast inputs', async () => {
  vi.mocked(apiFetch).mockImplementation(async () => new Response(JSON.stringify({ ...workbook,
    rows: [{ ...workbook.rows[0], assumptions: { ...workbook.rows[0].assumptions,
      basis_kind: 'manual', manual_basis: 5, basis_currency: 'GBP' } }] })));
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} rows={[{ name: 'Company', isin: 'EXACT' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'Waardering & koopzone')!.click());
    await act(async () => host.querySelector<HTMLButtonElement>('td[data-column="fair"] button')!.click());
    const formula = host.querySelector('[aria-label="Formula details"]')!;
    expect(formula.querySelector('[aria-label="With company data"] annotation')?.textContent).toContain('100.00\\;\\text{GBP}');
    expect(formula.querySelector('[aria-label="Formula"] annotation')?.textContent).not.toContain('EPS_1');
    expect(formula.querySelector('[data-input="manual_basis"]')?.getAttribute('title')).toBe('5');
    expect(formula.querySelector('[data-input="fy1"]')).toBeNull();
    expect(formula.querySelector('[data-input="basis_fx"]')).toBeNull();
    expect(formula.querySelector('select')).toBeNull();
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
    await act(async () => root.render(<WorkbookValuationTable portfolioId={7} nl={false} rows={[{ name: 'Company', isin: 'EXACT' }]} />));
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('nav button')].find(b => b.textContent === 'Afkortingen')!.click());
    const legend = host.querySelector('[aria-label="Portfolio abbreviations"]')!;
    expect(legend.textContent).toContain('Last retrieved');
    const cells = legend.querySelectorAll('tbody tr td');
    expect(cells[4].textContent).toBe('Date unavailable');
    expect(cells[5].querySelector('time')?.getAttribute('datetime')).toBe(retrieved);
    expect(cells[6].textContent).toBe('Complete');
    expect(legend.querySelectorAll('p')).toHaveLength(1);
  } finally { await act(async () => root.unmount()); host.remove(); }
});


