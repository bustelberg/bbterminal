import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { expect, it, vi } from 'vitest';
import VolatilityView from './VolatilityView';

vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));
vi.mock('./useRiskResult', () => ({
  riskRequestKey: () => 'rolling-volatility-test',
  useRiskResult: () => ({ error: null, data: {
    available: true, benchmark: '60% ACWI', volatility_pct: 8.06, benchmark_volatility_pct: 7.15,
    rolling_volatility: [
      { date: '2026-08-31', portfolio_pct: 9, benchmark_pct: 8 },
      { date: '2026-09-30', portfolio_pct: 10.25, benchmark_pct: 7.45 },
    ],
  } }),
}));

it('shows the latest rolling values instead of full-history volatility and ratio', () => {
  const html = renderToStaticMarkup(<VolatilityView holdings={[]} benchmark="ACWI"
    portfolioName="Bustelberg Neutraal" portfolioSource="airs_volk" />);
  expect(html).toContain('10.25%');
  expect(html).toContain('7.45%');
  expect(html).toContain('2026-09-30');
  expect(html).toContain('Rolling 12-month volatility');
  expect(html).not.toContain('8.06%');
  expect(html).not.toContain('7.15%');
  expect(html).not.toContain('as volatile as');
});
