import { describe, expect, it } from 'vitest';

import { riskRequestKey } from './useRiskResult';

describe('Risk result request identity', () => {
  it('separates current and opening bodies at the same endpoint', () => {
    const url = '/api/airs/portfolio/active-share?benchmark=ACWI';
    const current = JSON.stringify({ holdings: [{ isin: 'NL1', weight_pct: 4.1 }] });
    const opening = JSON.stringify({ holdings: [{ isin: 'NL1', weight_pct: 4.9 }] });

    expect(riskRequestKey(url, current)).not.toBe(riskRequestKey(url, opening));
  });

  it('separates views and benchmark dates even when the body is identical', () => {
    const body = JSON.stringify({ holdings: [{ isin: 'NL1', weight_pct: 4.9 }] });

    expect(riskRequestKey('/active?benchmark_start=2026-01-01', body))
      .not.toBe(riskRequestKey('/concentration?benchmark_start=2026-01-01', body));
  });
});
