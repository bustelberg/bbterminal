import { describe, expect, it } from 'vitest';

import { computeNetStats } from './feeStats';
import type { Holding, PeriodRecord } from '../../../lib/stores/momentum';

function rec(swaps: number | undefined): PeriodRecord {
  const h: Holding = {
    company_id: 1, ticker: 'T1', company_name: 'C1', sector: 'Tech', score: 1,
    category_scores: {}, weight: 1, forward_return_pct: 10,
    entry_price_eur: 100, exit_price_eur: 110,
    entry_date: '2024-01-01', exit_date: '2024-12-31', side: 'long',
  };
  return {
    date: '2024-01', holdings: [h], portfolio_return_pct: 10, cumulative_return_pct: 10,
    daily_timing_swaps: swaps,
  };
}

const fees = new Map<string, number>([['NYSE', 100]]); // 1% one-way
const exch = new Map<number, string>([[1, 'NYSE']]);

describe('computeNetStats — daily-timing swap cost', () => {
  it('charges each full-book swap the held-book average fee, reducing net', () => {
    const noSwap = computeNetStats([rec(0)], fees, exch)!;
    const swapped = computeNetStats([rec(4)], fees, exch)!;
    expect(swapped.total_return_pct).toBeLessThan(noSwap.total_return_pct);
    // 4 swaps × 1% avg fee → an extra (0.99)^4 multiplier on the net factor.
    const ratio = (1 + swapped.total_return_pct / 100) / (1 + noSwap.total_return_pct / 100);
    expect(ratio).toBeCloseTo(Math.pow(0.99, 4), 4);
  });

  it('no swaps (0 or undefined) leaves net unchanged', () => {
    const zero = computeNetStats([rec(0)], fees, exch)!;
    const undef = computeNetStats([rec(undefined)], fees, exch)!;
    expect(zero.total_return_pct).toBeCloseTo(undef.total_return_pct, 10);
  });
});
