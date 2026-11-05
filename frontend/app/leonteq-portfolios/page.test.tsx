// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { apiFetch } from '../../lib/apiFetch';
import Page from './LeonteqPanel';

vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../lib/i18n', () => ({ useLang: () => ['en'] }));

it.each([
  [{ success: true, status: 'success', portfolios: [{ name: 'AI-TopSelectie', sponsor: 'Bustelberg B.V.',
    start_date: '19 May 2026', value: '982.09', change: '-0.34 (-0.35%)', aum: '834’611',
    itd: '-1.79%', ytd: '-1.79%', last_7_days_path: 'M 0 10 L 120 20' }] }, 'Login successful'],
  [{ success: false, status: 'login_rejected' }, 'LYNQS rejected the login'],
  [{ success: false, status: 'verification_required' }, 'additional verification step'],
])('shows the backend login result without submitting credentials: %j', async (result, text) => {
  let finish!: (response: Response) => void;
  vi.mocked(apiFetch).mockReturnValue(new Promise((resolve) => { finish = resolve; }));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<Page />));
    const button = host.querySelector('button')!;
    await act(async () => button.click());
    expect(button.disabled).toBe(true);
    expect(host.textContent).toContain('Logging in…');
    expect(apiFetch).toHaveBeenLastCalledWith(expect.stringContaining('/api/leonteq/portfolios/login'), {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ refresh: false }),
    });
    await act(async () => { finish({ ok: true, json: async () => result } as Response); });
    expect(host.querySelector('[role=status]')?.textContent).toContain(text);
    expect(button.disabled).toBe(false);
    if (result.success) {
      const table = host.querySelector('table')!;
      expect(table.textContent).toContain('AI-TopSelectie');
      expect(table.textContent).toContain('834’611');
      expect(table.textContent).toContain('Since inception');
      expect(table.querySelector('svg path')?.getAttribute('d')).toBe('M 0 10 L 120 20');
      const portfolio = host.querySelector<HTMLButtonElement>('button[aria-label="AI-TopSelectie · View composition"]')!;
      vi.mocked(apiFetch).mockReturnValue(new Promise((resolve) => { finish = resolve; }));
      await act(async () => portfolio.click());
      expect(host.textContent).toContain('Loading current positions');
      expect(apiFetch).toHaveBeenLastCalledWith(expect.stringContaining('/api/leonteq/portfolios/composition'), {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: 'AI-TopSelectie', refresh: false }),
      });
      await act(async () => { finish({ ok: true, json: async () => ({ success: true, status: 'success',
        portfolio_name: 'AI-TopSelectie', total_value: '834’610.77 EUR', total_weight: '100%',
        groups: [{ name: 'Cash', weight: '27.25%', positions: [{ name: 'EURO', isin: null,
          total_value: '227’468.03 EUR', units: '227’468.03', price: '1', currency: 'EUR', performance: '-', weight: '27.25%' }] },
        { name: 'Share', weight: '72.75%', positions: [{ name: 'ADVANCED MICRO DEVICES', isin: 'US0079031078',
          total_value: '45’702.75 EUR', units: '105', price: '488.78', currency: 'USD', performance: '0.08%', weight: '5.48%' }] }],
      }) } as Response); });
      const composition = host.querySelector('table')!;
      expect(composition.textContent).toContain('EURO');
      expect(composition.textContent).toContain('US0079031078');
      expect(composition.textContent).toContain('72.75%');
      expect(composition.querySelector('tfoot')?.textContent).toContain('834’610.77 EUR');
      expect(composition.querySelector('tfoot')?.textContent).toContain('100%');
      const back = Array.from(host.querySelectorAll('button')).find((item) => item.textContent?.includes('All portfolios'))!;
      await act(async () => back.click());
      expect(host.querySelector('table')?.textContent).toContain('Since inception');
      const callsBeforeReopen = vi.mocked(apiFetch).mock.calls.length;
      await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="AI-TopSelectie · View composition"]')!.click());
      expect(apiFetch).toHaveBeenCalledTimes(callsBeforeReopen);
      expect(host.querySelector('table')?.textContent).toContain('ADVANCED MICRO DEVICES');
      expect(host.textContent).toContain('Saved composition');
      const refresh = Array.from(host.querySelectorAll('button')).find((item) => item.textContent === 'Refresh')!;
      vi.mocked(apiFetch).mockReturnValue(new Promise((resolve) => { finish = resolve; }));
      await act(async () => refresh.click());
      expect(apiFetch).toHaveBeenLastCalledWith(expect.stringContaining('/api/leonteq/portfolios/composition'), {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: 'AI-TopSelectie', refresh: true }),
      });
      expect(host.querySelector('table')?.textContent).toContain('ADVANCED MICRO DEVICES');
      await act(async () => { finish({ ok: true, json: async () => ({ success: false, status: 'timeout' }) } as Response); });
      expect(host.querySelector('table')?.textContent).toContain('ADVANCED MICRO DEVICES');
      expect(host.textContent).toContain('could not be confirmed in time');
    } else {
      expect(host.querySelector('table')).toBeNull();
    }
  } finally {
    await act(async () => root.unmount());
    host.remove();
    vi.clearAllMocks();
  }
});

it('opens the saved list without login and retains it during an unsuccessful refresh', async () => {
  const saved = { success: true, status: 'success', cached: true,
    retrieved_at: '2026-11-05T10:00:00+00:00', portfolios: [{ name: 'AI-TopSelectie', sponsor: 'Bustelberg B.V.' }] };
  let finish!: (response: Response) => void;
  vi.mocked(apiFetch).mockResolvedValue({ ok: true, json: async () => saved } as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<Page />));
    expect(apiFetch).toHaveBeenCalledTimes(1);
    expect(apiFetch).toHaveBeenCalledWith(expect.stringContaining('/api/leonteq/portfolios/snapshot'), { cache: 'no-store' });
    expect(host.textContent).toContain('Saved portfolios');
    expect(host.querySelector('table')?.textContent).toContain('AI-TopSelectie');
    vi.mocked(apiFetch).mockReturnValue(new Promise((resolve) => { finish = resolve; }));
    await act(async () => host.querySelector('button')!.click());
    expect(apiFetch).toHaveBeenLastCalledWith(expect.stringContaining('/api/leonteq/portfolios/login'), {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ refresh: true }),
    });
    expect(host.querySelector('table')?.textContent).toContain('AI-TopSelectie');
    await act(async () => { finish({ ok: true, json: async () => ({ success: false, status: 'timeout' }) } as Response); });
    expect(host.querySelector('table')?.textContent).toContain('AI-TopSelectie');
    expect(host.querySelector('[role=alert]')?.textContent).toContain('could not be confirmed in time');
  } finally {
    await act(async () => root.unmount());
    host.remove();
    vi.clearAllMocks();
  }
});
