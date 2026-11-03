import { expect, it } from 'vitest';
import { riskBundleBody } from './riskBundleRequest';

it('uses the current direct-stock sleeve and policy for both warming and displaying Risk', () => {
  const body = JSON.parse(riskBundleBody([
    { isin: 'STOCK', name: 'Adobe', weight_pct: 10, weight_now_pct: 12, bucket: 'Equity' },
    { isin: 'FUND', name: 'Equity ETF', weight_pct: 20, bucket: 'Equity', is_fund: true },
    { isin: 'BOND', name: '6,5% Rabobank Certificaten', weight_pct: 30, bucket: 'Equity' },
  ], { Stocks: 80, Bonds: 20 }, 'BOOK'));
  expect(body.holdings.map((row: { isin: string; weight_pct: number }) => [row.isin, row.weight_pct])).toEqual([['STOCK', 12]]);
  expect(body.benchmark_weights).toEqual({ Stocks: 80, Bonds: 20, EquityOnly: 1 });
  expect(body.returns_account).toBe('BOOK');
});
