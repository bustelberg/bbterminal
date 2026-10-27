// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import CorrelationView from './CorrelationView';

const state = vi.hoisted(() => ({ body: '', url: '' }));
vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));
vi.mock('./useRiskResult', () => ({
  riskRequestKey: (url: string, body: string) => { state.url = url; state.body = body; return body; },
  useRiskResult: () => ({ data: null, error: null }),
}));

it('always includes only individual stocks without a checkbox', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  document.body.append(host);
  const root = createRoot(host);
  const holdings = [
    { name: 'Microsoft', bucket: 'Equity', weight_pct: 30 },
    { name: 'World ETF', bucket: 'Equity', is_fund: true, weight_pct: 20 },
    { name: 'Corporate bond', bucket: 'Bonds', weight_pct: 20 },
    { name: 'Cash', bucket: 'Cash', weight_pct: 10 },
    { name: 'Selection Index', bucket: 'Equity', weight_pct: 10 },
    { name: 'Visa', bucket: 'Equity', weight_pct: 10 },
  ];
  try {
    await act(async () => root.render(<CorrelationView holdings={holdings} benchmark="ACWI" benchmarkWeights={{ Stocks: 60, Bonds: 40, EquityOnly: 1 }}
      portfolioName="Portfolio" portfolioSource="airs_volk" />));
    expect(host.querySelector('input[type="checkbox"]')).toBeNull();
    expect(state.url).toContain('frequency=weekly&start_date=2021-01-01&');
    expect(host.textContent).toContain('Since 1 January 2021 · weekly returns in EUR');
    expect(JSON.parse(state.body).holdings).toEqual([{ ...holdings[0], weight_pct: 75 }, { ...holdings[5], weight_pct: 25 }]);
    expect(JSON.parse(state.body).benchmark_weights).toEqual({ Stocks: 100, Bonds: 0 });
  } finally {
    await act(async () => root.unmount());
    host.remove();
  }
});
