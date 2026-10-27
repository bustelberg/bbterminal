import { describe, expect, it } from 'vitest';
import { airsRiskExposures } from './airsRiskExposures';

describe('AIRS-first current exposures', () => {
  it('merges a company across certificates without rebasing to the stock sleeve', () => {
    const result = airsRiskExposures([
      { name: 'Microsoft', isin: 'US1', company_id: 1, bucket: 'Equity', weight_now_pct: 20, via_names: ['A'], sources: [{ label: 'A', weight_now_pct: 20, value_eur: 20, start_value_eur: 20 }], sector: 'Technology', currency: 'USD' },
      { name: 'Microsoft ADR', isin: 'US2', company_id: 1, bucket: 'Equity', weight_now_pct: 10, via_names: ['B'], sources: [{ label: 'B', weight_now_pct: 10, value_eur: 10, start_value_eur: 10 }], sector: 'Technology', currency: 'USD' },
      { name: 'Cash', bucket: 'Cash', weight_now_pct: 40, currency: 'EUR' },
      { name: 'Bond ETF', bucket: 'Bonds', is_fund: true, weight_now_pct: 30, currency: 'EUR' },
    ]);
    expect(result.companies).toHaveLength(1);
    expect(result.companies[0].weight).toBe(30);
    expect(result.repeated).toHaveLength(1);
    expect(result.companyCoverage).toBe(30);
    expect(result.topFive).toBe(30);
    expect(result.total).toBe(100);
    expect(result.complete).toBe(true);
    expect(result.wrappers.map((r) => r.weight)).toEqual([20, 10]);
    expect(result.classes.find((r) => r.name === 'Cash')?.weight).toBe(40);
  });

  it('keeps opaque funds and unknown classifications visible, rather than calling them companies', () => {
    const result = airsRiskExposures([
      { name: 'Opaque equity fund', isin: 'FUND', bucket: 'Equity', is_fund: true, weight_now_pct: 60 },
      { name: 'Unknown', bucket: 'Unclassified', weight_now_pct: 40 },
    ]);
    expect(result.companyCoverage).toBe(0);
    expect(result.companies).toEqual([]);
    expect(result.currencies[0]).toMatchObject({ name: 'Unknown', weight: 100 });
    expect(result.classes).toHaveLength(2);
  });

  it('only tests recorded bounds on a complete supplied book', () => {
    const bands = [{ variant: 'Neutraal', bucket: 'Equity', max_pct: 60 }, { variant: 'Neutraal', bucket: 'Bonds', min_pct: 30 }];
    const result = airsRiskExposures([{ name: 'Stocks', bucket: 'Equity', weight_now_pct: 80 }, { name: 'Bonds', bucket: 'Bonds', weight_now_pct: 20 }], bands);
    expect(result.breaches.map((r) => r.direction)).toEqual(['above', 'below']);
    expect(airsRiskExposures([{ name: 'Partial', bucket: 'Equity', weight_now_pct: 20 }], bands).breaches).toEqual([]);
  });

  it('does not turn absent data into zero-risk claims', () => {
    const result = airsRiskExposures([]);
    expect(result.complete).toBe(false);
    expect(result.companies).toEqual([]);
  });
});
