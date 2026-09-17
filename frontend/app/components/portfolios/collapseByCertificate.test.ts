import { describe, expect, it } from 'vitest';

import { collapseByCertificate, syntheticBasket } from './PortfolioAnalysisModal';

describe('collapseByCertificate', () => {
  it('keeps a folded TopSelectie in Stock ETFs when its first underlying row is cash', () => {
    const rows = [
      {
        name: 'Liquiditeiten', bucket: 'Cash', is_fund: false, weight_now_pct: 2,
        current_value_eur: 200, start_value_eur: 200, via_names: ['StarTopSelectie Offensief'],
      },
      {
        name: 'NVIDIA', isin: 'US-NVDA', bucket: 'Equity', is_fund: false, weight_now_pct: 98,
        current_value_eur: 9800, start_value_eur: 9000, via_names: ['StarTopSelectie Offensief'],
        mom_12_1_pct: 18, vol_5y_pct: 42, beta_5y: 1.4,
      },
    ];

    const foldedRows = collapseByCertificate(rows as never);
    expect(foldedRows).toHaveLength(1);
    const [folded] = foldedRows;

    expect(folded).toMatchObject({
      name: 'StarTopSelectie Offensief', bucket: 'Equity', is_fund: true,
      mom_12_1_pct: 18, vol_5y_pct: 42, beta_5y: 1.4,
    });
    expect(syntheticBasket(folded)).toEqual({
      label: 'StarTopSelectie Offensief',
      holdings: [{ isin: 'US-NVDA', weight: 98, name: 'NVIDIA' }],
    });
  });
});
