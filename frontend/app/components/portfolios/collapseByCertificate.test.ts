import { describe, expect, it } from 'vitest';

import { collapseByCertificate } from './PortfolioAnalysisModal';

describe('collapseByCertificate', () => {
  it('keeps a folded TopSelectie in Stock ETFs when its first underlying row is cash', () => {
    const rows = [
      {
        name: 'Liquiditeiten', bucket: 'Cash', is_fund: false, weight_now_pct: 2,
        current_value_eur: 200, start_value_eur: 200, via_names: ['StarTopSelectie Offensief'],
      },
      {
        name: 'NVIDIA', bucket: 'Equity', is_fund: false, weight_now_pct: 98,
        current_value_eur: 9800, start_value_eur: 9000, via_names: ['StarTopSelectie Offensief'],
      },
    ];

    const foldedRows = collapseByCertificate(rows as never);
    expect(foldedRows).toHaveLength(1);
    const [folded] = foldedRows;

    expect(folded).toMatchObject({
      name: 'StarTopSelectie Offensief', bucket: 'Equity', is_fund: true,
    });
  });
});
