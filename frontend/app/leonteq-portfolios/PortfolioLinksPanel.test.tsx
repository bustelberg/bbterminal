// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import PortfolioLinksPanel from './PortfolioLinksPanel';
import { apiFetch } from '../../lib/apiFetch';

vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('./refreshProgress', async (importOriginal) => ({
  ...await importOriginal<typeof import('./refreshProgress')>(),
  refreshRequest: (url: string, init: Parameters<typeof apiFetch>[1]) => apiFetch(url, init),
}));
vi.mock('../../lib/i18n', () => ({ useLang: () => ['en'] }));

it('switches to dynamic connection options', async () => {
  vi.mocked(apiFetch).mockResolvedValue({ ok: true, json: async () => ({
    leonteq_portfolios: [], airs_portfolios: [], links: [],
  }) } as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PortfolioLinksPanel />));
    await act(async () => Array.from(host.querySelectorAll('button')).find((button) => button.textContent === 'Leonteq to AIRS Dynamic')!.click());
    expect(apiFetch).toHaveBeenLastCalledWith(expect.stringContaining('/portfolio-links?kind=dynamic'), { cache: 'no-store', noReadCache: true });
    expect(host.textContent).toContain('Dynamic AIRS portfolio');
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('saves and removes a selected connection and disables duplicate links', async () => {
  const saved = { leonteq_name: 'AI-TopSelectie', airs_portfolio_id: 1, airs_name: 'Fixed model', updated_at: '2026-11-05T12:00:00Z' };
  vi.mocked(apiFetch).mockResolvedValueOnce({ ok: true, json: async () => ({
    leonteq_portfolios: [{ name: 'AI-TopSelectie', sponsor: 'Bustelberg B.V.' }],
    airs_portfolios: [{ id: 1, name: 'Fixed model', omschrijving: 'Fixed portfolio', portfolio_type: 'fixed (13.7)' }], links: [],
  }) } as Response).mockImplementation(async (_url, init) => {
    if (init?.method === 'PUT') return { ok: true, json: async () => saved } as Response;
    if (init?.method === 'DELETE') return { ok: true, json: async () => ({ deleted: true }) } as Response;
    return { ok: false } as Response;
  });
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PortfolioLinksPanel />));
    const inputs = host.querySelectorAll<HTMLInputElement>('[role=combobox]');
    await act(async () => inputs[0].focus());
    await act(async () => {
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(inputs[0], 'ai-top');
      inputs[0].dispatchEvent(new Event('input', { bubbles: true }));
    });
    expect(host.querySelectorAll('[role=option]')).toHaveLength(1);
    await act(async () => inputs[0].dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })));
    expect(inputs[0].value).toBe('AI-TopSelectie');
    await act(async () => inputs[1].focus());
    await act(async () => {
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(inputs[1], 'portfolio');
      inputs[1].dispatchEvent(new Event('input', { bubbles: true }));
    });
    expect(host.querySelectorAll('[role=option]')).toHaveLength(1);
    await act(async () => host.querySelector<HTMLButtonElement>('[role=option]')!.click());
    expect(inputs[1].value).toBe('Fixed model');
    const save = host.querySelector<HTMLButtonElement>('button[type=submit]')!;
    expect(save.disabled).toBe(false);
    await act(async () => host.querySelector('form')!.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })));
    expect(apiFetch).toHaveBeenCalledWith(expect.stringContaining('/api/leonteq/portfolio-links'), {
      method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ leonteq_name: 'AI-TopSelectie', airs_portfolio_id: 1 }),
    });
    expect(host.querySelector('table')?.textContent).toContain('Fixed model');
    expect(host.querySelector('thead')?.textContent).toContain('Holdings connected');
    expect(host.querySelector('table')?.textContent).toContain('Not verified');
    expect(host.textContent).toContain('Connection saved.');
    expect(save.disabled).toBe(true);
    expect(inputs[0].value).toBe('');
    expect(inputs[1].value).toBe('');
    await act(async () => inputs[0].focus());
    expect(host.querySelectorAll('[role=option]')).toHaveLength(0);
    await act(async () => inputs[1].focus());
    expect(host.querySelectorAll('[role=option]')).toHaveLength(0);
    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label^="Remove connection"]')!.click());
    expect(apiFetch).toHaveBeenLastCalledWith(expect.stringContaining('/api/leonteq/portfolio-links'), {
      method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ leonteq_name: 'AI-TopSelectie', airs_portfolio_id: 1 }),
    });
    expect(host.textContent).toContain('Connection removed.');
    expect(host.querySelector('table')?.textContent).not.toContain('Fixed model');
    expect(save.disabled).toBe(true);
    await act(async () => inputs[1].focus());
    expect(host.querySelectorAll('[role=option]')).toHaveLength(1);
    await act(async () => {
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(inputs[1], 'unknown portfolio');
      inputs[1].dispatchEvent(new Event('input', { bubbles: true }));
    });
    expect(host.textContent).toContain('No portfolios found.');
    expect(save.disabled).toBe(true);
  } finally {
    await act(async () => root.unmount()); host.remove(); vi.clearAllMocks();
  }
});

it.each([['fixed', true], ['fixed', false], ['dynamic', true], ['dynamic', false]] as const)('opens %s connected holdings independently: leonteq=%s', async (kind, success) => {
  const link = { leonteq_name: 'MomentumTopSelectie Index', airs_portfolio_id: 7, airs_name: 'MoTopSelectie_FX', updated_at: '2026-11-05T12:00:00Z' };
  let finishLeonteq!: (response: Response) => void;
  const leonteqResponse = new Promise<Response>((resolve) => { finishLeonteq = resolve; });
  vi.mocked(apiFetch).mockImplementation(async (url) => {
    if (url.includes('/portfolio-links/holdings?')) return { ok: true, json: async () => ({ rules: [] }) } as Response;
    if (url.endsWith('/portfolio-links') || url.endsWith('/portfolio-links?kind=dynamic')) return { ok: true, json: async () => ({ leonteq_portfolios: [], airs_portfolios: [], links: [link] }) } as Response;
    if (url.endsWith('/portfolios/composition')) return leonteqResponse;
    return { ok: true, json: async () => ({ portfolio: 'MoTopSelectie_FX', portfolio_id: 7, datum: '2026-07-05', dates: [],
      cached_at: '2026-11-05T12:00:00Z', rows: [{ Fonds: 'Apple', Percentage: 4.95, ISINCode: 'US0378331005' },
        { Fonds: 'Microsoft', Percentage: 3.5, ISINCode: 'US5949181045' }],
    }) } as Response;
  });
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PortfolioLinksPanel />));
    if (kind === 'dynamic') await act(async () => Array.from(host.querySelectorAll('button')).find((button) => button.textContent === 'Leonteq to AIRS Dynamic')!.click());
    await act(async () => {
      const button = host.querySelector<HTMLButtonElement>('button[aria-label^="View holdings for"]')!;
      if (kind === 'dynamic') button.closest('tr')!.click();
      else button.click();
    });
    expect(apiFetch).toHaveBeenCalledWith(expect.stringContaining('/model-portfolios/7/composition'), { cache: 'no-store', noReadCache: true });
    expect(apiFetch).toHaveBeenCalledWith(expect.stringContaining('/portfolios/composition'), {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: 'MomentumTopSelectie Index', refresh: false }),
    });
    const airs = host.querySelector('section[aria-label="AIRS · MoTopSelectie_FX"]')!;
    expect(Array.from(airs.querySelectorAll('thead th'), (cell) => cell.textContent)).toEqual(['Fonds', 'Percentage', 'ISIN-code']);
    expect(airs.textContent).toContain('Apple');
    expect(airs.textContent).toContain('4.95%');
    expect(airs.textContent).toContain('5 jul 2026');
    await act(async () => { finishLeonteq({ ok: success, json: async () => ({ success: true, status: 'success', cached: true,
      retrieved_at: '2026-11-05T12:00:00Z', groups: [{ name: 'Cash', positions: [{ isin: null, name: 'EURO', weight: '2%' }] },
        { name: 'Share', positions: [{ isin: 'US0378331005', name: 'APPLE INC', weight: '4.9%' }] }],
    }) } as Response); });
    if (success) {
      const matched = host.querySelector('section[aria-labelledby="matched-holdings-heading"]')!;
      expect(matched.textContent).toContain('US0378331005');
      expect(matched.textContent).toContain('APPLE INC');
      expect(matched.textContent).toContain('4.90%');
      expect(matched.textContent).toContain('4.95%');
      expect(matched.querySelectorAll('tbody tr')).toHaveLength(1);
      const left = host.querySelector('section[aria-label="Leonteq · Unmatched holdings"]')!;
      const right = host.querySelector('section[aria-label="AIRS · Unmatched holdings"]')!;
      expect(left.textContent).toContain('EURO');
      expect(left.textContent).not.toContain('APPLE INC');
      expect(right.textContent).toContain('Microsoft');
      expect(right.textContent).not.toContain('Apple');
      const calls = vi.mocked(apiFetch).mock.calls.length;
      await act(async () => Array.from(host.querySelectorAll('button')).find((button) => button.textContent?.includes('All connections'))!.click());
      await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label^="View holdings for"]')!.click());
      expect(apiFetch).toHaveBeenCalledTimes(calls);
      expect(host.querySelectorAll('table')).toHaveLength(3);
    } else {
      expect(host.querySelector('[role=alert]')?.textContent).toContain('could not be refreshed');
      expect(airs.querySelector('table')?.textContent).toContain('Apple');
    }
  } finally {
    await act(async () => root.unmount()); host.remove(); vi.clearAllMocks();
  }
});
