// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import FactsheetPanel from './FactsheetPanel';
import { apiFetch } from '../../../lib/apiFetch';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));

const response = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });
const stats = { start: '2011-11-11', end: '2026-11-12', years: 15, observations: 3780,
  total_return_pct: 100, cagr_pct: 4.73, volatility_pct: 15 };
const saved = { ready: true, stale: false, currency: 'EUR', prepared_at: '2026-11-13T23:45:00Z', rows: [
  { ticker: 'ACWI', name: 'iShares MSCI ACWI ETF', requested_start: '2011-11-12', end: stats.end, period: stats },
  { ticker: 'AGG', name: 'iShares Core U.S. Aggregate Bond ETF', requested_start: '2011-11-12', end: stats.end,
    period: null, reason: 'Fifteen years of fund history are not available.',
    available_history: { ...stats, start: '2017-11-22', years: 8.98, total_return_pct: 14, cagr_pct: 1.5, volatility_pct: 5 } },
  { ticker: 'QAI', name: 'NYLIM Hedge Multi-Strategy Tracker ETF · Liquid alternatives proxy', requested_start: '2011-11-12', end: stats.end, period: stats },
] };

it('shows a full fifteen-year AGG row and links to its own Yahoo quote', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockResolvedValue(response({ ...saved, rows: saved.rows.map(row => ({
    ...row, period: stats, available_history: null, reason: null,
  })) }));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<FactsheetPanel />));
    expect(host.querySelectorAll('tbody tr')).toHaveLength(3);
    const bond = host.querySelectorAll('tbody tr')[1];
    expect(bond.textContent).toContain('iShares Core U.S. Aggregate Bond ETF');
    expect(bond.textContent).toContain('15 years');
    expect(bond.textContent).not.toContain('Available history');
    expect(bond.querySelector('a')?.getAttribute('href')).toBe('https://finance.yahoo.com/quote/AGG/');
    expect(host.textContent).toContain('USD/EUR currency movements');
    const alternatives = host.querySelectorAll('tbody tr')[2];
    expect(alternatives.textContent).toContain('Liquid alternatives proxy');
    expect(alternatives.textContent).toContain('15 years');
    expect(alternatives.querySelector('a')?.getAttribute('href')).toBe('https://finance.yahoo.com/quote/QAI/');
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('separates true fifteen-year statistics from the bond funds shorter available history', async () => {
  vi.mocked(apiFetch).mockReset(); vi.mocked(apiFetch).mockResolvedValue(response(saved));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<FactsheetPanel />));
    const rows = host.querySelectorAll('tbody tr');
    expect(rows).toHaveLength(4);
    expect(rows[0].textContent).toContain('4.73%');
    expect(rows[0].textContent).toContain('15.00%');
    expect(rows[1].textContent).toContain('Fifteen years of fund history are not available');
    expect(rows[1].textContent).not.toContain('1.50%');
    expect(rows[2].textContent).toContain('Available history');
    expect(rows[2].textContent).toContain('2017-11-22');
    expect(rows[2].textContent).toContain('1.50%');
    expect(rows[2].textContent).toContain('5.00%');
    expect(apiFetch).toHaveBeenCalledTimes(1);
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('prepares the first factsheet automatically and Refresh forces an update', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockImplementation(async (_url, init) => response(init?.method === 'POST' ? saved : { ready: false, rows: [] }));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<FactsheetPanel />));
    expect(host.textContent).toContain('4.73%');
    expect(vi.mocked(apiFetch).mock.calls[1]).toEqual([expect.stringContaining('/factsheet/refresh'), { method: 'POST' }]);
    await act(async () => host.querySelector<HTMLButtonElement>('button')!.click());
    expect(vi.mocked(apiFetch).mock.calls).toContainEqual([expect.stringContaining('/factsheet/refresh?force=true'), { method: 'POST' }]);
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it('retains the dated figures and shows a source failure when refresh fails', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockImplementation(async (_url, init) => init?.method === 'POST'
    ? response({ detail: 'ACWI: missing historical USD/EUR FX.' }, 503) : response(saved));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<FactsheetPanel />));
    await act(async () => host.querySelector<HTMLButtonElement>('button')!.click());
    expect(host.querySelector('[role=alert]')?.textContent).toContain('missing historical USD/EUR FX');
    expect(host.textContent).toContain('4.73%');
  } finally { await act(async () => root.unmount()); host.remove(); }
});
