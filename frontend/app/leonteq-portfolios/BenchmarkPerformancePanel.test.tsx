// @vitest-environment happy-dom
import React, { act, type ReactNode } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { apiFetch } from '../../lib/apiFetch';
import { refreshRequest } from './refreshProgress';
import BenchmarkPerformancePanel from './BenchmarkPerformancePanel';

vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('./refreshProgress', () => ({ refreshRequest: vi.fn() }));
vi.mock('../../lib/stores/jobs', () => ({
  ExpectedJobFailure: class extends Error {},
  startLocalJob: (_title: string, _kind: string, run: (signal: AbortSignal, report: () => void) => Promise<unknown>) => {
    void run(new AbortController().signal, () => {}).catch(() => {}); return 'job';
  },
}));
vi.mock('recharts', () => ({
  ResponsiveContainer: ({ children }: { children: ReactNode }) => <>{children}</>,
  BarChart: ({ data, children }: { data: unknown; children: ReactNode }) => <div data-chart={JSON.stringify(data)}>{children}</div>,
  Bar: () => null, CartesianGrid: () => null, Legend: () => null, ReferenceLine: () => null,
  Tooltip: () => null, XAxis: () => null, YAxis: () => null,
}));

it.each(['complete', 'missing'] as const)('selects portfolios, years and benchmarks from saved %s data', async mode => {
  vi.mocked(apiFetch).mockReset(); vi.mocked(refreshRequest).mockReset();
  vi.mocked(apiFetch).mockImplementation(async url => {
    if (String(url).includes('/proxy/')) return new Response(JSON.stringify({ refreshed: true }));
    if (String(url).includes('portfolio-links')) return new Response(JSON.stringify({ links: [
      { leonteq_name: 'Fund A', airs_portfolio_id: 1 }, { leonteq_name: 'Fund B', airs_portfolio_id: 2 }] }));
    const query = new URL(String(url), 'http://localhost').searchParams;
    return new Response(JSON.stringify({ portfolio: query.get('name'), benchmark: query.get('benchmark'),
      year: Number(query.get('year')), years: [2026, 2025], saved_returns: mode === 'complete', benchmark_available: mode === 'complete', benchmark_ticker: 'ACWI',
      retrieved_at: '2026-11-05T10:00:00Z', benchmark_fetched_at: '2026-11-11T10:00:00Z', months: [
        { month: `${query.get('year')}-01`, leonteq_pct: mode === 'complete' ? -3 : null, benchmark_pct: mode === 'complete' ? 2 : null, difference_pp: mode === 'complete' ? -5 : null },
        { month: `${query.get('year')}-02`, leonteq_pct: null, benchmark_pct: null, difference_pp: null },
        { month: `${query.get('year')}-11`, leonteq_pct: 3.5, benchmark_pct: 2, difference_pp: 1.5, mtd: true }] }));
  });
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  const select = async (index: number, value: string) => act(async () => {
    const control = host.querySelectorAll('select')[index]; control.value = value;
    control.dispatchEvent(new Event('change', { bubbles: true }));
  });
  try {
    await act(async () => root.render(<BenchmarkPerformancePanel />));
    expect(host.querySelectorAll('select')[1].value).toBe('ACWI');
    expect(refreshRequest).not.toHaveBeenCalled();
    expect(host.textContent).toContain('ACWI fetched 11 nov 2026');
    expect(host.textContent).toContain('Leonteq fetched 5 nov 2026');
    expect(host.textContent).not.toContain('Completed months only');
    expect(host.querySelector('table')?.textContent).toContain('November (MTD)');
    if (mode === 'complete') {
      expect(host.querySelector('table')?.textContent).toContain('-3.00%');
      expect(host.querySelector('table')?.textContent).toContain('+2.00%');
      expect(host.querySelector('table')?.textContent).toContain('-5.00pp');
      const chart = JSON.parse(host.querySelector('[data-chart]')!.getAttribute('data-chart')!);
      expect(chart[1].leonteq_pct).toBeNull();
    } else {
      expect(host.textContent).toContain('No saved Leonteq returns yet');
      expect(host.textContent).toContain('Saved benchmark prices are unavailable');
      expect(host.querySelector('table')?.textContent).toContain('—');
    }
    await select(2, '2025');
    expect(String(vi.mocked(apiFetch).mock.lastCall![0])).toContain('year=2025');
    await select(1, 'SP500');
    expect(String(vi.mocked(apiFetch).mock.lastCall![0])).toContain('benchmark=SP500');
    await select(0, 'Fund B');
    expect(String(vi.mocked(apiFetch).mock.lastCall![0])).toContain('name=Fund+B');
    expect(host.querySelector('h3')?.textContent).toContain('Fund B');
    vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify({ success: true })));
    await act(async () => host.querySelector('button')!.click());
    expect(apiFetch).toHaveBeenCalledWith(expect.stringContaining('/api/benchmarks/proxy/SP500/refresh'), { method: 'POST' });
    expect(JSON.parse(String(vi.mocked(refreshRequest).mock.lastCall![1]?.body))).toEqual({ name: 'Fund B', airs_portfolio_id: 2, refresh: true });
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('shows an actionable read failure without displaying a previous chart', async () => {
  vi.mocked(apiFetch).mockReset();
  vi.mocked(apiFetch).mockResolvedValueOnce(new Response(JSON.stringify({ links: [{ leonteq_name: 'Fund', airs_portfolio_id: 1 }] })))
    .mockResolvedValueOnce(new Response(JSON.stringify({ detail: 'Benchmark is unavailable. Please retry.' }), { status: 503 }));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<BenchmarkPerformancePanel />));
    expect(host.querySelector('[role=alert]')?.textContent).toContain('Benchmark is unavailable');
    expect(host.querySelector('[data-chart]')).toBeNull();
  } finally { await act(async () => root.unmount()); host.remove(); }
});
