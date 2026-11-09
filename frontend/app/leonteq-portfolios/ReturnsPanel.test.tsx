// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { apiFetch } from '../../lib/apiFetch';
import ReturnsPanel, { compareReturns } from './ReturnsPanel';
import { jobsStore } from '../../lib/stores/jobs';
vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('./refreshProgress', async (importOriginal) => ({
  ...await importOriginal<typeof import('./refreshProgress')>(),
  refreshRequest: (url: string, init: Parameters<typeof apiFetch>[1]) => apiFetch(url, init),
}));
const link = { leonteq_name: 'Momentum', airs_name: 'Mo_DYN', airs_portfolio_id: 2 };
it('compares exact linked portfolios, preserves zero and handles missing or ambiguous data', () => {
  const source = { success: true, portfolios: [{ name: 'Momentum', ytd: '3,25%' }] };
  expect(compareReturns([link], source, [{ dynamic_portefeuille: 'Mo_DYN', ytd_pct: 0 }])[0]).toMatchObject({ leonteq: 3.25, airs: 0, difference: 3.25 });
  expect(compareReturns([link], source, [{ dynamic_portefeuille: 'Mo_DYN_other', ytd_pct: 5 }])[0].difference).toBeNull();
  expect(compareReturns([link], { success: false }, [{ dynamic_portefeuille: 'Mo_DYN', ytd_pct: 1 }])[0].leonteq).toBeNull();
  expect(compareReturns([link], source, [{ dynamic_portefeuille: 'Mo_DYN', ytd_pct: 1 }, { dynamic_portefeuille: 'Mo_DYN', ytd_pct: 2 }])[0].airs).toBeNull();
});
it('loads saved dynamic comparisons without starting a scrape or write', async () => {
  vi.mocked(apiFetch).mockImplementation(async (url) => ({ ok: true, json: async () => url.endsWith('/monthly-performance') ? { success: true, months: [{ month: '2026-11', leonteq_pct: 3.25, airs_pct: 1.25, difference_pp: 2 }], ytd: { '2026': { leonteq_pct: 3.25, airs_pct: 1.25, difference_pp: 2 } } } : url.includes('/portfolio-links?') ? { links: [link] }
    : url.endsWith('/snapshot') ? { success: true, retrieved_at: '2026-11-05T12:00:00Z', portfolios: [{ name: 'Momentum', ytd: '3.25%' }] }
      : [{ dynamic_portefeuille: 'Mo_DYN', ytd_pct: 1.25, periode: '2026-11-04' }],
  }) as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<ReturnsPanel />));
    expect(host.querySelector('tbody')!.textContent).toContain('+2.00 pp');
    expect(host.querySelector('tbody')!.textContent).toContain('+1.25%');
    expect(apiFetch).toHaveBeenCalledTimes(2);
    expect(vi.mocked(apiFetch).mock.calls.every(([url, options]) => !options?.method || options.method === 'GET' || url.endsWith('/monthly-performance'))).toBe(true);
    expect(host.querySelectorAll('button')).toHaveLength(2);
    await act(async () => host.querySelector('button')!.click());
    const requests = vi.mocked(apiFetch).mock.calls.filter(([url]) => url.endsWith('/monthly-performance'));
    expect(JSON.parse(requests[requests.length - 1][1]!.body as string).refresh).toBe(true);
    expect(jobsStore.get().jobs.at(-1)?.summary).toBe('All returns refreshed');
    expect(host.textContent).not.toContain('All returns refreshed');
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('highlights one connection at a time and waits before refreshing the next', async () => {
  const links = [link, { ...link, leonteq_name: 'Second', airs_name: 'Second_DYN', airs_portfolio_id: 3 }];
  const waiting: ((response: Response) => void)[] = [];
  const response = () => ({ ok: true, json: async () => ({ success: true, months: [] }) }) as Response;
  vi.mocked(apiFetch).mockImplementation(async (url, options) => {
    if (url.includes('/portfolio-links?')) return { ok: true, json: async () => ({ links }) } as Response;
    if (JSON.parse(options!.body as string).refresh) return await new Promise<Response>((resolve) => waiting.push(resolve));
    return response();
  });
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<ReturnsPanel />));
    await act(async () => host.querySelector('button')!.click());
    expect(waiting).toHaveLength(1);
    const cards = () => host.querySelectorAll('section[aria-label="Monthly returns"]');
    expect(cards()[0].textContent).toContain('Refreshing');
    expect(cards()[1].textContent).toContain('Queued');
    expect(jobsStore.get().jobs.at(-1)?.message).toContain('1/2 · Refreshing Momentum → Mo_DYN');
    await act(async () => waiting[0](response()));
    expect(waiting).toHaveLength(2);
    expect(cards()[0].textContent).toContain('Updated');
    expect(cards()[1].textContent).toContain('Refreshing');
    await act(async () => waiting[1](response()));
    expect(cards()[1].textContent).toContain('Updated');
    expect(jobsStore.get().jobs.at(-1)?.summary).toBe('All returns refreshed');
    expect(host.textContent).not.toContain('All returns refreshed');
    const before = vi.mocked(apiFetch).mock.calls.length;
    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="Refresh returns for Momentum"]')!.click());
    expect(waiting).toHaveLength(3);
    expect(cards()[1].textContent).toContain('Updated');
    await act(async () => waiting[2](response()));
    const newCalls = vi.mocked(apiFetch).mock.calls.slice(before);
    expect(newCalls).toHaveLength(1);
    expect(JSON.parse(newCalls[0][1]!.body as string)).toMatchObject({ name: 'Momentum', refresh: true });
    expect(jobsStore.get().jobs.at(-1)?.summary).toBe('Momentum refreshed');
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('reports expected refresh failure in the popup without a console error overlay', async () => {
  vi.mocked(apiFetch).mockImplementation(async (url, options) => ({ ok: true, json: async () => {
    if (url.includes('/portfolio-links?')) return { links: [link] };
    return JSON.parse(options!.body as string).refresh ? { success: false, status: 'browser_error' } : { success: true, months: [] };
  } }) as Response);
  const error = vi.spyOn(console, 'error').mockImplementation(() => {});
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<ReturnsPanel />));
    await act(async () => host.querySelector('button')!.click());
    expect(jobsStore.get().jobs.at(-1)?.status).toBe('failed');
    expect(jobsStore.get().jobs.at(-1)?.summary).toBe('Refresh incomplete · 1 failed');
    expect(jobsStore.get().jobs.at(-1)?.message).toContain('Copy .json');
    expect(error).not.toHaveBeenCalled();
    expect(host.textContent).toContain('Copy .json');
  } finally { await act(async () => root.unmount()); host.remove(); error.mockRestore(); vi.clearAllMocks(); }
});
