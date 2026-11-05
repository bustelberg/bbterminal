// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import ConnectionHoldingsView, { type ConnectionHoldingsCache } from './ConnectionHoldingsView';
import type { HoldingRule } from './matchHoldings';
import { apiFetch } from '../../lib/apiFetch';

vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../lib/i18n', () => ({ useLang: () => ['en'] }));

it('disconnects a match into leftovers, reconnects it and keeps the saved rules on reopen', async () => {
  const link = { leonteq_name: 'AI', airs_portfolio_id: 1, airs_name: 'Fixed model' };
  const cache: ConnectionHoldingsCache = new Map([
    ['leonteq:AI', { rows: [{ isin: 'US0378331005', name: 'APPLE INC', weight: '4.9%' }] }],
    ['airs:1', { rows: [{ isin: 'US0378331005', name: 'Apple', weight: '4.95%' }] }],
  ]);
  const rulesCache = new Map<string, HoldingRule[]>([[JSON.stringify(['AI', 1]), []]]);
  vi.mocked(apiFetch).mockResolvedValueOnce({ ok: true, json: async () => ({ rules: [{ leonteq_key: 'isin:US0378331005', airs_key: null }] }) } as Response)
    .mockResolvedValueOnce({ ok: true, json: async () => ({ rules: [{ leonteq_key: 'isin:US0378331005', airs_key: 'isin:US0378331005' }] }) } as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  const view = <ConnectionHoldingsView link={link} cache={cache} rulesCache={rulesCache} onBack={() => {}} />;
  try {
    await act(async () => root.render(view));
    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="Disconnect holdings for APPLE INC"]')!.click());
    expect(host.querySelector('section[aria-labelledby=matched-holdings-heading]')?.textContent).toContain('No connected holdings');
    expect(host.querySelector('section[aria-label="Leonteq · Unmatched holdings"]')?.textContent).toContain('APPLE INC');
    const inputs = host.querySelectorAll<HTMLInputElement>('[role=combobox]');
    for (const input of inputs) {
      await act(async () => input.focus());
      await act(async () => input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })));
    }
    await act(async () => host.querySelector('form')!.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })));
    expect(apiFetch).toHaveBeenLastCalledWith(expect.stringContaining('/portfolio-links/holdings'), {
      method: 'PUT', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leonteq_name: 'AI', airs_portfolio_id: 1, leonteq_key: 'isin:US0378331005', airs_key: 'isin:US0378331005' }),
    });
    expect(host.querySelector('section[aria-labelledby=matched-holdings-heading]')?.textContent).toContain('Manually connected');
    await act(async () => root.render(null));
    await act(async () => root.render(view));
    expect(apiFetch).toHaveBeenCalledTimes(2);
    expect(host.querySelector('section[aria-labelledby=matched-holdings-heading]')?.textContent).toContain('Manually connected');
  } finally {
    await act(async () => root.unmount()); host.remove(); vi.clearAllMocks();
  }
});
