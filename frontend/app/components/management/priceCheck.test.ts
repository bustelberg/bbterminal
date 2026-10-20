import { describe, expect, it } from 'vitest';
import type { AirsAccountIsins } from '../../../lib/types/api';
import { newPriceDifferences, needsPriceReview, summarizePrices } from './priceCheck';

type Holding = NonNullable<AirsAccountIsins['rows']>[number];
const holding = (changes: Partial<Holding> = {}): Holding => ({
  holding_name: 'Stock', isin: 'TEST', bucket: 'Equity', verdict: 'ok',
  currency: 'USD', our_price_currency: 'USD',
  airs_price_local: 100, our_price_local: 100, our_price_date: '2026-10-19', ...changes,
});
const summarize = (...rows: Holding[]) => summarizePrices({ portefeuille: 'BOOK', as_of: '2026-10-19', rows });

describe('dashboard price checks', () => {
  it('adds timing context without hiding the original gap or using insufficient history', () => {
    const range = { low: 20, high: 25, start: '2026-10-12', end: '2026-10-16', count: 5 };
    const within = summarize(holding({ airs_price_local: 21.51, our_price_local: 24.14, recent_close_range: range }));
    expect(within.items[0].rangeAssessment).toBe('within');
    expect(needsPriceReview(within)).toBe(true);
    const outside = summarize(holding({ airs_price_local: 19, recent_close_range: range }));
    expect(outside.items[0].rangeAssessment).toBe('outside');
    for (const recent_close_range of [{ ...range, count: 4 }, { ...range, end: '2026-10-01' }, null]) {
      expect(summarize(holding({ recent_close_range })).items[0].rangeAssessment).toBe('insufficient');
    }
    expect(summarize(holding({ currency: 'AUD', recent_close_range: range })).items[0].rangeAssessment).toBe('insufficient');
  });
  it('compares native prices without FX and refuses different currencies or quote units', () => {
    const summary = summarize(holding({ airs_price_local: 21.51, our_price_local: 24.14,
      implied_price_eur: 999, our_price_eur: 1 }));
    expect(summary.items[0].difference).toBeCloseTo((21.51 / 24.14 - 1) * 100);
    for (const currency of ['AUD', 'GBp', null]) {
      const different = summarize(holding({ currency, our_price_currency: 'GBP', airs_price_local: 1000 }));
      expect(different.items[0].status).toBe('unavailable');
      expect(different.items[0].difference).toBeNull();
      expect(newPriceDifferences(null, different)).toBe(0);
    }
  });
  it('excludes High Income Quality fund by ISIN from rows, counts and alerts', () => {
    const excluded = holding({ isin: 'CH1369849273', holding_name: 'High Income Quality fund', airs_price_local: 1000 });
    const summary = summarize(holding(), excluded);
    expect(summary.items).toHaveLength(1);
    expect(summary.total).toBe(1);
    expect(summary.checked).toBe(1);
    expect(summary.differences).toBe(0);
    expect(needsPriceReview(summary)).toBe(false);
    expect(newPriceDifferences(null, summary)).toBe(0);
    expect(summarize({ ...excluded, isin: ' ch1369849273 ' }).total).toBe(0);
  });

  it('classifies comparable same-date prices at the two warning thresholds', () => {
    const summary = summarize(holding({ airs_price_local: 102 }),
      holding({ holding_name: 'Warning', airs_price_local: 103 }),
      holding({ holding_name: 'Difference', airs_price_local: 105 }));
    expect(summary.items.map((item) => item.status)).toEqual(['matched', 'warning', 'difference']);
    expect(summary.checked).toBe(3);
    expect(summary.warnings).toBe(1);
    expect(summary.differences).toBe(1);
    expect(summary.items[2].difference).toBeCloseTo(5);
  });

  it('never counts stale, absent or deliberately linked prices as a pass', () => {
    const summary = summarize(
      holding({ holding_name: 'Stale', our_price_date: '2026-10-16', airs_price_local: 120 }),
      holding({ holding_name: 'Missing', our_price_local: null }),
      holding({ holding_name: 'Linked', served_by: 'OTHER', airs_price_local: 500 }),
      holding({ holding_name: 'Unknown date', our_price_date: null }),
    );
    expect(summary.items.map((item) => item.status)).toEqual(['stale', 'unavailable', 'linked', 'unavailable']);
    expect(summary.checked).toBe(0);
    expect(summary.total).toBe(4);
    expect(summary.differences).toBe(0);
    expect(needsPriceReview(summary)).toBe(true); // The stale 20% gap is still worth investigating.
  });

  it('excludes cash but keeps unpriceable investments in the coverage denominator', () => {
    const summary = summarize(holding(), holding({ holding_name: 'Cash', bucket: 'Cash' }),
      holding({ holding_name: 'Certificate', our_price_local: null }));
    expect(summary.total).toBe(2);
    expect(summary.checked).toBe(1);
    expect(summary.unavailable).toBe(1);
    expect(needsPriceReview(summarize(holding()))).toBe(false);
    expect(summarize().checked).toBe(0);
  });

  it('handles invalid and zero prices as unavailable instead of infinite differences', () => {
    for (const value of [0, -1, NaN, Infinity]) {
      expect(summarize(holding({ our_price_local: value })).items[0].status).toBe('unavailable');
    }
  });
});

describe('new-discrepancy alerts', () => {
  it('alerts for new differences, suppresses repeats, and alerts again after recovery', () => {
    const good = summarize(holding());
    const bad = summarize(holding({ airs_price_local: 110 }));
    expect(newPriceDifferences(good, bad)).toBe(1);
    expect(newPriceDifferences(bad, bad)).toBe(0);
    expect(newPriceDifferences(bad, good)).toBe(0);
    expect(newPriceDifferences(good, bad)).toBe(1);
  });

  it('alerts when a warning escalates and ignores stale or missing prices', () => {
    const warning = summarize(holding({ airs_price_local: 103 }));
    const bad = summarize(holding({ airs_price_local: 110 }));
    expect(newPriceDifferences(warning, bad)).toBe(1);
    expect(newPriceDifferences(bad, warning)).toBe(0);
    expect(newPriceDifferences(null, summarize(holding({ our_price_date: null })))).toBe(0);
    expect(newPriceDifferences(null, warning)).toBe(0);
  });

  it('ignores small gaps and stale/missing noise but reports extreme stale gaps', () => {
    const quiet = summarize(holding({ airs_price_local: 103 }),
      holding({ holding_name: 'Stale', our_price_date: '2026-10-16', airs_price_local: 101 }),
      holding({ holding_name: 'Missing', our_price_local: null }));
    expect(needsPriceReview(quiet)).toBe(false);
    expect(newPriceDifferences(null, quiet)).toBe(0);
    const stale = summarize(holding({ our_price_date: '2026-10-16', airs_price_local: 90 }));
    expect(needsPriceReview(stale)).toBe(true);
    expect(newPriceDifferences(quiet, stale)).toBe(1);
    expect(newPriceDifferences(stale, stale)).toBe(0);
    expect(needsPriceReview(summarize(holding({ served_by: 'OTHER', airs_price_local: 200 })))).toBe(false);
  });
});
