import { describe, expect, it } from 'vitest';

import { CARDS } from './LongEquityTab';

describe('Long Equity per-share metric aliases', () => {
  it('reads Mastercard-style FCF/share history as well as its canonical LTM row', () => {
    const card = CARDS.find((candidate) => candidate.benchmarkMetric === 'fcf_ps');

    expect(card?.codes).toContain(
      'annuals__per_share_data_array__Free Cash Flow per Share',
    );
  });
});
