import { describe, expect, it } from 'vitest';
import { dcfRow } from './PortfolioFundamentalModal';

const metric = (metric_code: string, numeric_value: number | null, year = 2025) =>
  ({ metric_code, numeric_value, target_date: `${year}-12-31` });
const metrics = [
  metric('close_price', 100, 2026),
  metric('annuals__Income Statement__Shares Outstanding (Diluted Average)', 1000),
  metric('annuals__Cashflow Statement__Free Cash Flow', 8000),
  metric('annuals__Cashflow Statement__Stock Based Compensation', 500),
  metric('annuals__Cashflow Statement__Capital Expenditure', 2000),
  metric('annuals__Cashflow Statement__Cash Flow Depreciation, Depletion and Amortization', 1000),
  metric('annuals__Per Share Data__EPS without NRI', 8),
  metric('annuals__Per Share Data__Month End Stock Price', 80),
  metric('annual_per_share_eps_estimate', 10, 2026),
  metric('annual_per_share_eps_estimate', 12, 2027),
  metric('annuals__Cashflow Statement__Cash Flow from Operations', 10000),
  metric('annual_operating_cash_flow_estimate', 11000, 2026),
  metric('annual_operating_cash_flow_estimate', 12000, 2027),
];
const base = { company_id: 1, isin: 'TEST', name: 'Test', weight_pct: 10, currency: 'EUR',
  source_fetched_at: {}, metrics };
const calculate = (quoteCurrency: string, quote: number, observations = metrics) =>
  dcfRow({ ...base, stock_price_currency: quoteCurrency, stock_price: quote, metrics: observations },
    '2026-10-20', { growthRate: 0.1, exitPE: 20 });

describe('all portfolio Fundamental models share the normalized currency price', () => {
  it.each([['USD', 110], ['CAD', 150], ['GBp', 8000], ['HKD', 850]] as const)(
    '%s quotes do not leak into EUR model arithmetic', (currency, quote) => {
      const normalized = calculate('EUR', 100);
      const crossListed = calculate(currency, quote);
      expect(crossListed.src.price).toBe(100);
      expect(crossListed.growth).toEqual(normalized.growth);
      expect(crossListed.growth.some((cell) => cell.impliedGrowth != null)).toBe(true);
      expect(crossListed.egmResult).toEqual(normalized.egmResult);
      expect(crossListed.egmResult.upside).not.toBeNull();
      expect(crossListed.peByYear[2026]).toBe(10);
      expect(crossListed.pOcfByYear[2026]).toBeCloseTo(100 / 11);
      expect(crossListed.peByYear).toEqual(normalized.peByYear);
      expect(crossListed.pOcfByYear).toEqual(normalized.pOcfByYear);
    });

  it('never falls back to the unconverted displayed quote when FX is unavailable', () => {
    const missing = calculate('USD', 110, metrics.map((row) =>
      row.metric_code === 'close_price' ? { ...row, numeric_value: null } : row));
    expect(missing.src.price).toBeNull();
    expect(missing.egmResult.upside).toBeNull();
    expect(missing.peByYear[2026]).toBeNull();
    expect(missing.pOcfByYear[2026]).toBeNull();
  });
});
