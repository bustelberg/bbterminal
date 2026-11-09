// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it } from 'vitest';
import HoldingsConnectionStatus from './HoldingsConnectionStatus';
import type { ConnectionHoldingsCache } from './ConnectionHoldingsView';
import type { HoldingRule } from './matchHoldings';

it.each(['matched', 'unmatched', 'manual', 'incomplete'] as const)('shows %s holding connection status', async mode => {
  const link = { leonteq_name: 'Fund', airs_portfolio_id: 1, airs_name: 'Dynamic' };
  const cache: ConnectionHoldingsCache = new Map([
    ['leonteq:Fund', { rows: [{ isin: 'US0378331005', name: 'Apple', weight: null }], warning: mode === 'incomplete' }],
    ['airs:security-v2:1', { rows: [{ isin: mode === 'matched' ? 'US0378331005' : 'US5949181045', name: 'Other', weight: null }] }],
  ]);
  const rules: HoldingRule[] = mode === 'manual' ? [{ leonteq_key: 'isin:US0378331005', airs_key: 'isin:US5949181045' }] : [];
  const rulesCache = new Map([[JSON.stringify(['Fund', 1]), rules]]);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<HoldingsConnectionStatus link={link} nl={false} cache={cache} rulesCache={rulesCache} />));
    const all = mode === 'matched' || mode === 'manual';
    expect(host.textContent).toBe(all ? 'All connected' : mode === 'incomplete' ? 'Not verified' : 'Not all connected');
    expect(host.querySelector(all ? '.bg-green-600' : '.bg-red-600')).not.toBeNull();
  } finally { await act(async () => root.unmount()); host.remove(); }
});
