// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import WeightSearch from './WeightSearch';
import { exportToXlsx } from '../../../lib/tableExport';

vi.mock('../../../lib/tableExport', () => ({ exportToXlsx: vi.fn() }));

const { searchFetch } = vi.hoisted(() => ({ searchFetch: vi.fn() }));
vi.mock('../../../lib/apiFetch', () => ({ apiFetch: (url: string, ...args: unknown[]) => {
  if (url.endsWith('/weight-search/bounds')) return Promise.resolve(new Response(JSON.stringify({
    profiles: ['Offensief', 'Neutraal'], profile: 'Offensief', all_bounds: ['Offensief', 'Neutraal'].flatMap(variant =>
      ['Equity', 'Bonds', 'Alternatives', 'Cash'].map(bucket => ({ variant, bucket, min_pct: variant === 'Neutraal' && bucket === 'Equity' ? 40 : 0, default_pct: 25, max_pct: 100 }))),
  })));
  return searchFetch(url, ...args);
} }));

it('runs one search on click and displays allocation and held-out results', async () => {
  let finish!: (response: Response) => void;
  searchFetch.mockImplementation(() => new Promise<Response>(resolve => { finish = resolve; }));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  const root = createRoot(host);
  const metric = { sharpe: 1.23, sortino: 1.23, annual_return_pct: 8.5, max_drawdown_pct: -12, weeks: 60 };
  const candidate = { weights: { Equity: 50, Bonds: 30, Alternatives: 15, Cash: 5 }, training: metric, validation: { ...metric, sharpe: .75, sortino: .75 }, full_history: metric };
  try {
    await act(async () => root.render(<WeightSearch portefeuille="BOOK" startDate="2025-02-01" disabled={false} />));
    expect(searchFetch).not.toHaveBeenCalled();
    const button = host.querySelector('button')!;
    await act(async () => button.click());
    expect(button.disabled).toBe(true);
    expect(button.textContent).toBe('Searching…');
    expect(searchFetch.mock.calls[0][0]).toContain('/BOOK/weight-search?start_date=2025-02-01&annual_target=2&annual_cash=0');
    expect(searchFetch.mock.calls[0][0]).toContain('available_history=true');
    expect(searchFetch.mock.calls[0][0]).toContain('objective=sharpe');
    await act(async () => finish(new Response(JSON.stringify({ available: true, candidates: [candidate], current: candidate,
      candidates_tested: 1771, training_weeks: 60, validation_weeks: 20, annual_target_pct: 2, annual_cash_pct: 0,
      proxy_history: true, coverage: [{ bucket: 'Equity', coverage_pct: 50, included: ['Adobe'], excluded: ['Apple'] }],
      statistical_check: { supported: true, reason: 'Held-out improvement supported.' },
      training_period: { start: '2025-02-01', end: '2026-05-01' }, test_period: { start: '2026-05-01', end: '2026-11-01' },
      instrument_breakdown: [{ bucket: 'Equity', average_sharpe: 1.1, period: { start: '2025-02-01', end: '2026-11-01' }, training: metric, validation: { ...metric, sharpe: .75 }, instruments: [
        { name: 'Adobe', overall: { ...metric, sharpe: 1.1, start: '2025-02-01', end: '2026-11-01' }, training_weight_pct: 100, test_weight_pct: 100, training: metric, validation: { ...metric, sharpe: .75 } },
      ] }],
    }))));
    expect(searchFetch).toHaveBeenCalledTimes(1);
    expect(host.textContent).toContain('Train Sharpe');
    expect(host.textContent).toContain('Train: 1 feb 2025 – 1 may 2026');
    expect(host.textContent).toContain('Test: 1 may 2026 – 1 nov 2026');
    expect(host.textContent).toContain('Test Sharpe');
    expect(host.textContent).toContain('50.00%');
    expect(host.textContent).toContain('0.75');
    expect(host.textContent).not.toContain('1771 allocations tested');
    expect(host.textContent).toContain('Uses proxy baskets');
    expect(host.textContent).not.toContain('represented');
    expect(host.textContent).not.toContain('Excluded throughout: Apple');
    expect(host.textContent).toContain('Average instrument Sharpe');
    expect(host.textContent).not.toContain('Train weight');
    const details = Array.from(host.querySelectorAll('details')).find(element => element.querySelector('summary')?.textContent === 'Details')!;
    expect(Array.from(details.querySelectorAll('th[scope="col"], thead th')).map(cell => cell.textContent)).toEqual(['Instrument', 'Sharpe', 'Start', 'End']);
    expect(details.textContent).toContain('1.10');
    expect(details.textContent).toContain('1 feb 2025');
    expect(details.textContent).toContain('1 nov 2026');
    expect(details.querySelector('p')).toBeNull();
    expect(host.querySelector('caption')?.textContent).toBe('Allocation training and test comparison');
    expect(Array.from(host.querySelectorAll('caption')).some(caption => caption.textContent === 'Equity instrument full-period Sharpe')).toBe(true);
    expect(Array.from(host.querySelectorAll('details')).every(details => !details.open)).toBe(true);
    await act(async () => Array.from(host.querySelectorAll('button')).find(button => button.textContent === 'Download Excel')!.click());
    expect(exportToXlsx).toHaveBeenCalledTimes(1);
    const [exported, columns, filename] = vi.mocked(exportToXlsx).mock.calls[0];
    expect(exported).toEqual([
      { category: 'Stocks', instrument: 'Adobe', sharpe: 1.1, start: '1 feb 2025', end: '1 nov 2026' },
      { category: 'Stocks', instrument: 'Average instrument Sharpe', sharpe: 1.1, start: '1 feb 2025', end: '1 nov 2026' },
    ]);
    expect(columns.map(column => column.header)).toEqual(['Category', 'Instrument', 'Sharpe', 'Start', 'End']);
    expect(filename).toBe('Sharpe BOOK Offensief');
    expect(button.disabled).toBe(false);
    await act(async () => root.render(<WeightSearch portefeuille="BOOK" startDate="2025-03-01" disabled={false} />));
    expect(host.textContent).toContain('Inputs changed');
  } finally {
    await act(async () => root.unmount());
    vi.clearAllMocks();
  }
});

it('shows profile limits before searching and sends the selected profile', async () => {
  searchFetch.mockResolvedValue(new Response(JSON.stringify({ available: false, reason: 'Insufficient history' })));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  const root = createRoot(host);
  try {
    await act(async () => root.render(<WeightSearch portefeuille="BOOK" startDate="2025-02-01" disabled={false} />));
    const select = host.querySelector<HTMLSelectElement>('[aria-label="Risk profile"]')!;
    expect(select.value).toBe('Offensief');
    await act(async () => { select.value = 'Neutraal'; select.dispatchEvent(new Event('change', { bubbles: true })); });
    expect(host.textContent).toContain('Stocks 40.00%–100.00%');
    const objective = host.querySelector<HTMLSelectElement>('[aria-label="Search objective"]')!;
    await act(async () => { objective.value = 'sortino'; objective.dispatchEvent(new Event('change', { bubbles: true })); });
    await act(async () => host.querySelector('button')!.click());
    expect(searchFetch.mock.calls[0][0]).toContain('profile=Neutraal');
    expect(searchFetch.mock.calls[0][0]).toContain('objective=sortino');
  } finally {
    await act(async () => root.unmount());
    vi.clearAllMocks();
  }
});

it('shows exploratory candidate weights even when statistical support is absent', async () => {
  searchFetch.mockResolvedValue(new Response(JSON.stringify({ available: true,
    statistical_check: { supported: false, reason: 'Not enough evidence yet: 13 validation weeks; at least 26 needed.' },
    candidates: [{ weights: { Equity: 95, Bonds: 5, Alternatives: 0, Cash: 0 } }],
  })));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  const root = createRoot(host);
  try {
    await act(async () => root.render(<WeightSearch portefeuille="BOOK" startDate="2025-02-01" disabled={false} />));
    await act(async () => host.querySelector('button')!.click());
    expect(host.textContent).toContain('95.00%');
    expect(host.querySelector('table')).not.toBeNull();
    expect(host.textContent).toContain('Statistical improvement not established');
  } finally {
    await act(async () => root.unmount());
    vi.clearAllMocks();
  }
});

it('shows that a short-history calculation completed rather than reporting unusable data', async () => {
  searchFetch.mockResolvedValue(new Response(JSON.stringify({ available: true,
    training_weeks: 6, validation_weeks: 3, candidates_tested: 50,
    statistical_check: { supported: false, reason: 'Calculated from 9 usable weeks. Not enough evidence yet: 3 validation weeks; at least 26 needed.' },
  })));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  const root = createRoot(host);
  try {
    await act(async () => root.render(<WeightSearch portefeuille="BOOK" startDate="2025-02-01" disabled={false} />));
    await act(async () => host.querySelector('button')!.click());
    expect(host.querySelector('[role="status"]')?.textContent).toContain('6 training / 3 test weeks');
    expect(host.textContent).not.toContain('Not enough usable data');
  } finally {
    await act(async () => root.unmount());
    vi.clearAllMocks();
  }
});

it('shows data gaps rather than inventing optimal weights', async () => {
  searchFetch.mockResolvedValue(new Response(JSON.stringify({ available: false,
    reason: 'Only 12 consecutive common weeks are available; at least 52 are required.', issues: ['Bond: no saved history.'],
  })));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  const root = createRoot(host);
  try {
    await act(async () => root.render(<WeightSearch portefeuille="BOOK" startDate="2025-02-01" disabled={false} />));
    await act(async () => host.querySelector('button')!.click());
    expect(host.textContent).toContain('at least 52 are required');
    expect(host.querySelector('table')).toBeNull();
  } finally {
    await act(async () => root.unmount());
    vi.clearAllMocks();
  }
});
