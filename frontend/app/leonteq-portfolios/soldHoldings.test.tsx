// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import ConnectionHoldingsView, { type ConnectionHoldingsCache } from './ConnectionHoldingsView';
import { apiFetch } from '../../lib/apiFetch';
import type { HoldingRule } from './matchHoldings';
vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../lib/i18n', () => ({ useLang: () => ['en'] }));
it('saves and undoes a sold marking without writing a percentage to AIRS', async () => {
  const link = { leonteq_name: 'Source', airs_portfolio_id: 1, airs_name: 'Fixed' };
  const cache: ConnectionHoldingsCache = new Map([
    ['leonteq:Source', { rows: [{ name: 'Apple', isin: 'US0378331005', weight: '5%' }] }],
    ['airs:security-v2:1', { rows: [{ name: 'Apple', isin: 'US0378331005', weight: '5%' }, { name: 'Inditex', isin: 'ES0148396007', weight: '1.35%' }] }],
  ]);
  const rulesCache = new Map<string, HoldingRule[]>([[JSON.stringify(['Source', 1]), []]]);
  vi.mocked(apiFetch).mockImplementation(async (_url, options) => {
    const body = JSON.parse(options!.body as string);
    return { ok: true, json: async () => ({ rules: [{ leonteq_key: body.leonteq_key, airs_key: body.airs_key }] }) } as Response;
  });
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<ConnectionHoldingsView link={link} cache={cache} rulesCache={rulesCache} onBack={() => {}} />));
    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="Mark Inditex sold in Leonteq"]')!.click());
    expect(JSON.parse(vi.mocked(apiFetch).mock.calls[0][1]!.body as string)).toMatchObject({ leonteq_key: 'sold:isin:ES0148396007', airs_key: 'isin:ES0148396007' });
    const sold = host.querySelector('section[aria-label="Sold in Leonteq"]')!;
    expect(sold.textContent).toContain('Inditex');
    expect(sold.textContent).toContain('0%');
    await act(async () => sold.querySelector('button')!.click());
    expect(host.querySelector('section[aria-label="Sold in Leonteq"]')).toBeNull();
    expect(host.querySelector('button[aria-label="Mark Inditex sold in Leonteq"]')).not.toBeNull();
    expect(vi.mocked(apiFetch).mock.calls.every(([url, options]) => url.endsWith('/portfolio-links/holdings') && options?.method === 'PUT')).toBe(true);
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});
