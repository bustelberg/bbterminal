import { describe, expect, it } from 'vitest';

import {
  airsRiskHoldings, airsRiskWeightContext, applyCompanySectorOverride, collapseByCertificate,
  holdingsForCertificateScope, individualStocksBasket,
  syntheticAirsName, syntheticBasket,
} from './PortfolioAnalysisModal';
import { holdingsOnRiskBasis } from './ActiveSharePanel';

describe('collapseByCertificate', () => {
  it('keeps a folded TopSelectie in Stock ETFs when its first underlying row is cash', () => {
    const rows = [
      {
        name: 'Liquiditeiten', bucket: 'Cash', is_fund: false, weight_now_pct: 2,
        current_value_eur: 200, start_value_eur: 200, via_names: ['StarTopSelectie Offensief'],
        via_holding_names: ['Star Selection Index'],
        sources: [{ label: 'StarTopSelectie Offensief', book: 'StarTopSelectie OFF DYN' }],
      },
      {
        name: 'NVIDIA', isin: 'US-NVDA', bucket: 'Equity', is_fund: false, weight_now_pct: 98,
        current_value_eur: 9800, start_value_eur: 9000, via_names: ['StarTopSelectie Offensief'],
        via_holding_names: ['Star Selection Index'],
        sources: [{ label: 'StarTopSelectie Offensief', book: 'StarTopSelectie OFF DYN' }],
        mom_12_1_pct: 18, mom_state: 3, mom_pct_rank: 0.95, mom_rank_n: 1000,
        vol_5y_pct: 42, beta_5y: 1.4,
      },
    ];

    const foldedRows = collapseByCertificate(rows as never);
    expect(foldedRows).toHaveLength(1);
    const [folded] = foldedRows;

    expect(folded).toMatchObject({
      name: 'StarTopSelectie Offensief', bucket: 'Equity', is_fund: true,
      mom_12_1_pct: 18, mom_state: 3, mom_pct_rank: 0.95, mom_rank_n: 1000,
      vol_5y_pct: 42, beta_5y: 1.4,
    });
    expect(syntheticBasket(folded)).toEqual({
      label: 'StarTopSelectie Offensief',
      holdings: [{ isin: 'US-NVDA', weight: 98, name: 'NVIDIA' }],
    });
    expect(syntheticAirsName(folded)).toBe('StarTopSelectie OFF DYN');
  });

  it('keeps duplicate selected positions in the Fundamental basket coverage count', () => {
    const rows = [
      { name: 'Company A', isin: 'US-A', bucket: 'Equity', is_fund: false, weight_now_pct: 40,
        current_value_eur: 40, start_value_eur: 40, via_names: ['FamilieTopSelectie'] },
      { name: 'Company A', isin: 'US-A', bucket: 'Equity', is_fund: false, weight_now_pct: 60,
        current_value_eur: 60, start_value_eur: 60, via_names: ['FamilieTopSelectie'] },
    ];

    const [folded] = collapseByCertificate(rows as never);
    expect(syntheticBasket(folded)?.holdings).toHaveLength(2);
  });

  it('uses only the Individual stocks section for a folded TopSelectie basket', () => {
    const rows = [
      { name: 'Company A', isin: 'US-A', bucket: 'Equity', is_fund: false, weight_now_pct: 60,
        current_value_eur: 60, start_value_eur: 60, via_names: ['MerkenTopSelectie'],
        sources: [{ label: 'MerkenTopSelectie', model_id: 1917, fundamental_model_id: 1920 }] },
      { name: 'Equity ETF', isin: 'IE-ETF', bucket: 'Equity', is_fund: true, weight_now_pct: 40,
        current_value_eur: 40, start_value_eur: 40, via_names: ['MerkenTopSelectie'],
        sources: [{ label: 'MerkenTopSelectie', model_id: 1917, fundamental_model_id: 1920 }] },
    ];

    const [folded] = collapseByCertificate(rows as never);
    expect(syntheticBasket(folded)).toEqual({
      label: 'MerkenTopSelectie', sourcePortfolioId: 1920,
      holdings: [{ isin: 'US-A', weight: 60, name: 'Company A' }],
    });
  });
});

describe('individualStocksBasket', () => {
  it('reuses all 28 direct companies and excludes rows outside Individual stocks', () => {
    const companies = Array.from({ length: 28 }, (_, index) => ({
      name: `Company ${index + 1}`, isin: `US${String(index + 1).padStart(10, '0')}`,
      bucket: 'Equity', is_fund: false, weight_now_pct: index + 1,
    }));
    const direct = {
      book_holdings: [
        ...companies,
        { name: 'Stock ETF', isin: 'IE-ETF', bucket: 'Equity', is_fund: true,
          weight_now_pct: 5 },
        { name: 'Liquiditeiten', isin: null, bucket: 'Cash', is_fund: false,
          weight_now_pct: 2 },
      ],
    };

    const basket = individualStocksBasket(direct as never, 'FamilieTopSelectie');
    expect(basket.holdings).toHaveLength(28);
    expect(basket.holdings).toEqual(companies.map((holding) => ({
      isin: holding.isin, weight: holding.weight_now_pct, name: holding.name,
    })));
  });
});

describe('holdingsForCertificateScope', () => {
  const rows = [
    { name: 'Direct stock', isin: 'US-DIRECT', bucket: 'Equity', is_fund: false,
      weight_now_pct: 60, current_value_eur: 60, start_value_eur: 60 },
    { name: 'Underlying stock', isin: 'US-CHILD', bucket: 'Equity', is_fund: false,
      weight_now_pct: 40, current_value_eur: 40, start_value_eur: 40,
      via_names: ['Held certificate'] },
  ] as never;

  it('keeps certificate constituents out by default', () => {
    const scoped = holdingsForCertificateScope(rows, false);
    expect(scoped.map((row) => row.name)).toEqual(['Direct stock', 'Held certificate']);
    expect(scoped.find((row) => row.name === 'Held certificate')?.is_fund).toBe(true);
  });

  it('adds the underlying stocks only when look-through is selected', () => {
    expect(holdingsForCertificateScope(rows, true).map((row) => row.name))
      .toEqual(['Direct stock', 'Underlying stock']);
  });
});

describe('AIRS Risk weights', () => {
  const rows = [
    { name: 'A', isin: 'US-A', bucket: 'Equity', is_fund: false,
      current_value_eur: 600, start_value_eur: 200, weight_now_pct: 60 },
    { name: 'B', isin: 'US-B', bucket: 'Equity', is_fund: false,
      current_value_eur: 300, start_value_eur: 500, weight_now_pct: 30 },
    { name: 'Cash', isin: null, bucket: 'Cash', is_fund: false,
      current_value_eur: 100, start_value_eur: 300, weight_now_pct: 10 },
  ] as never;

  it('derives current and opening weights from their own complete AIRS euro totals', () => {
    const holdings = airsRiskHoldings(rows, true);
    expect(holdings.map((holding) => holding.weight_now_pct)).toEqual([60, 30, 10]);
    expect(holdings.map((holding) => holding.weight_start_pct)).toEqual([20, 50, 30]);
  });

  it('applies the selected basis to the one body shared by every Risk view', () => {
    const holdings = airsRiskHoldings(rows, true);
    expect(holdingsOnRiskBasis(holdings, 'now').map((holding) => holding.weight_pct))
      .toEqual([60, 30, 10]);
    expect(holdingsOnRiskBasis(holdings, 'start').map((holding) => holding.weight_pct))
      .toEqual([20, 50, 30]);
  });

  it('gives Risk info icons the same literal AIRS rows as the weight denominator', () => {
    const context = airsRiskWeightContext(rows, true);
    expect(context.now).toEqual([
      { name: 'A', value_eur: 600 },
      { name: 'B', value_eur: 300 },
      { name: 'Cash', value_eur: 100 },
    ]);
    expect(context.start).toEqual([
      { name: 'A', value_eur: 200 },
      { name: 'B', value_eur: 500 },
      { name: 'Cash', value_eur: 300 },
    ]);
    expect(context.nowTotal).toBe(1000);
    expect(context.startTotal).toBe(1000);
  });
});

describe('company sector override paint', () => {
  it('shows a confirmed sector immediately without losing the source sector', () => {
    const analysis = {
      book_holdings: [{ company_id: 42, name: 'Adyen', sector: 'Technology',
        sector_default: 'Technology', sector_overridden: false }],
    } as never;

    const updated = applyCompanySectorOverride(analysis, 42, 'Financials');

    expect(updated.book_holdings?.[0]).toMatchObject({
      sector: 'Financials', sector_default: 'Technology', sector_overridden: true,
    });
  });

  it('restores the source sector immediately when Automatic is selected', () => {
    const analysis = {
      book_holdings: [{ company_id: 42, name: 'Adyen', sector: 'Financials',
        sector_default: 'Technology', sector_overridden: true }],
    } as never;

    const updated = applyCompanySectorOverride(analysis, 42, null);

    expect(updated.book_holdings?.[0]).toMatchObject({
      sector: 'Technology', sector_default: 'Technology', sector_overridden: false,
    });
  });
});
