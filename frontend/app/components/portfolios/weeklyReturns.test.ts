import { describe, expect, it } from 'vitest';
import { priceSegments, type WeeklyCell } from './weeklyReturns';

const cell = (price: number | null, status = 'priced'): WeeklyCell => ({ price, status, return_pct: null });

describe('AIRS weekly price observations', () => {
  it('breaks lines at sales, missing weeks and missing quotes', () => {
    expect(priceSegments([cell(100), cell(110), cell(null, 'sold'), cell(120, 'reentered'), cell(null), cell(130)]))
      .toEqual([[{ index: 0, price: 100 }, { index: 1, price: 110 }], [{ index: 3, price: 120 }], [{ index: 5, price: 130 }]]);
  });
  it('keeps a single real observation without inventing earlier prices', () => {
    expect(priceSegments([cell(null), cell(100, 'added')])).toEqual([[{ index: 1, price: 100 }]]);
  });
});
