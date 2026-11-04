// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { apiFetch } from '../../../lib/apiFetch';
import { startJob } from '../../../lib/stores/jobs';
import { invalidateReadCache } from '../../../lib/readCache';
import MonthlyReportPanel from './MonthlyReportPanel';
import type { PerformanceBundle } from './monthlyReport';

vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));
vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../../lib/stores/jobs', () => ({ startJob: vi.fn() }));

const november = '2026-11';
const october = '2026-10';
const december = '2025-12';
const summary = { return_pct: 5, benchmark_pct: 2, excess_pp: 3, start: '2026-01-01', end: '2026-11-04', complete: true };
const missing = { snapshot: null, needs_refresh: true, benchmark_needs_refresh: true };
const fixture: PerformanceBundle = {
  portefeuille: 'BOOK', current_month: november, available_years: [2026, 2025],
  years: {
    '2026': {
      months: [october, november], warning: null, summary,
      rows: { [november]: { periode: '2026-11-04', rendement: 0.41 } },
      weighted_benchmark: { [november]: { return_pct: 0.02, start_date: '2026-10-31', end_date: '2026-11-04', source: 'ETF' } },
      details: {
        [october]: missing,
        [november]: { ...missing, benchmark_needs_refresh: false, snapshot: {
          opening_weight_date: '2026-10-31',
          positions: [{ name: 'Sample stock', isin: null, status: 'held', return_pct: 5, contribution_pp: 1,
            start_date: '2026-10-31', end_date: '2026-11-04' }],
          lookthrough_positions: [{ name: 'Constituent', certificate: 'Sample stock', isin: 'TEST',
            via: [{ label: 'Sample stock', weight_pct: 1.44 }, { label: 'Another selection', weight_pct: 0.1 }],
            status: 'held', return_pct: 2, opening_weight_pct: 3, start_date: '2026-10-31', end_date: '2026-11-04' }],
          reconciliation: {
            status: 'reconciled', monthly_return_pct: 0.41, contribution_sum_pp: 0.41, difference_pp: 0,
            issues: [], adjustments: [{ kind: 'opbrengsten', contribution_pp: 0.1 }],
          },
        } },
      },
    },
    '2025': {
      months: [december], warning: null, summary: { ...summary, start: '2025-01-01', end: '2025-12-31' },
      rows: { [december]: { periode: '2025-12-31', rendement: -2 } },
      weighted_benchmark: {}, details: { [december]: missing },
    },
  },
};

let host: HTMLDivElement;
let root: Root;

beforeEach(() => {
  vi.resetAllMocks();
  invalidateReadCache('performance panel test');
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  host = document.createElement('div');
  document.body.appendChild(host);
  root = createRoot(host);
  vi.mocked(apiFetch).mockImplementation(async () => new Response(JSON.stringify(fixture)));
});

it('switches to bundled certificate constituents without fetching again', async () => {
  await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
  await act(async () => host.querySelectorAll<HTMLButtonElement>('nav button')[1].click());
  const details = host.querySelector('#monthly-report-details')!;
  expect(details.textContent).not.toContain('Constituent');
  const toggle = details.querySelector<HTMLInputElement>('input[type="checkbox"]')!;
  await act(async () => toggle.click());
  expect(details.textContent).toContain('Constituent');
  expect(details.textContent).toContain('Sample stock');
  expect(details.querySelector('th:nth-child(2)')?.textContent).toBe('Via');
  expect(details.textContent).toContain('Sample stock1.44%');
  expect(details.textContent).toContain('Another selection0.10%');
  expect(details.textContent).toContain('Income · AIRS');
  expect(details.querySelector('tfoot')?.textContent).toContain('Known contributions');
  expect(details.querySelector('tfoot')?.textContent).toContain('Unexplained difference');
  expect(details.querySelector('tfoot th')?.getAttribute('colspan')).toBe('4');
  expect(details.textContent).toContain('+2.00%');
  await act(async () => toggle.click());
  expect(details.textContent).not.toContain('Constituent');
  expect(apiFetch).toHaveBeenCalledOnce();
});

afterEach(async () => {
  await act(async () => root.unmount());
  host.remove();
});

it('opens and closes month details from the same data used for the tile', async () => {
  await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
  const tile = host.querySelectorAll<HTMLButtonElement>('nav button')[1];
  const contents = tile.textContent;
  expect(contents).toContain('+0.41%');
  expect(contents).toContain('+0.02%');

  await act(async () => tile.click());
  const details = host.querySelector('#monthly-report-details');
  expect(details?.textContent).toContain('Sample stock');
  expect(details?.textContent).toContain('+1.00pp');
  expect(details?.textContent).toContain('+0.41%');
  expect(details?.textContent).toContain('+0.02%');
  expect(tile.textContent).toBe(contents);

  await act(async () => tile.click());
  expect(host.querySelector('#monthly-report-details')).toBeNull();
  expect(tile.textContent).toBe(contents);
  expect(apiFetch).toHaveBeenCalledOnce();
  expect(apiFetch).toHaveBeenCalledWith(expect.stringContaining('/accounts/BOOK/performance'), expect.anything());
  expect(startJob).not.toHaveBeenCalled();
});

it('uses the bundle for prior years and keeps missing data visible without starting jobs', async () => {
  await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
  const year = host.querySelector('select')!;
  await act(async () => {
    year.value = '2025';
    year.dispatchEvent(new Event('change', { bubbles: true }));
  });
  const tile = host.querySelector<HTMLButtonElement>('nav button')!;
  expect(tile.textContent).toContain('-2.00%');
  await act(async () => tile.click());
  const details = host.querySelector('#monthly-report-details');
  expect(details?.textContent).toContain('Position results are not available yet.');
  expect(details?.textContent).toContain('Benchmark is not available yet.');
  expect(apiFetch).toHaveBeenCalledOnce();
  expect(startJob).not.toHaveBeenCalled();
});

it('replaces the bundle only after an explicit refresh', async () => {
  vi.mocked(startJob).mockResolvedValue({
    id: 'job', body: {}, done: Promise.resolve({ status: 'done' } as Awaited<Awaited<ReturnType<typeof startJob>>['done']>),
  });
  await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
  await act(async () => host.querySelector<HTMLButtonElement>('nav button')!.click());
  const refresh = [...host.querySelectorAll('button')].find((button) => button.textContent === 'Load month from AIRS')!;
  await act(async () => refresh.click());
  expect(startJob).toHaveBeenCalledWith(expect.stringContaining('/monthly-report/job?month=2026-10'), 'Client portfolio');
  expect(apiFetch).toHaveBeenCalledTimes(2);
  expect(host.querySelector('#monthly-report-details')).not.toBeNull();
});
