import { describe, expect, it } from 'vitest';
import { equityParts, type Splittable } from './equityParts';

/**
 * Dividing the Stocks class into operating companies and funds.
 *
 * ⚠ WHAT IS WORTH PINNING IS **WHEN A DIVISION APPEARS**, NOT THE MARKUP. A sub-header over a list
 * that is all one kind of thing is furniture that has to be read before it can be ignored, and it
 * is the failure this helper is shaped to avoid — so most of these cases are about the absence.
 *
 * ⚠ AND THE DENOMINATOR. `weight_now_pct` is a share of the WHOLE BOOK; the share printed beside a
 * division inside Stocks has to be a share of Stocks, or the two halves do not add to 100 and the
 * reader is quietly given the answer to a different question.
 *
 * Pure — no DOM, no network.
 */

const EQUITY = 'Equity';
const co = (weight: number): Splittable => ({ is_fund: false, weight_now_pct: weight });
const etf = (weight: number): Splittable => ({ is_fund: true, weight_now_pct: weight });

describe('when the division is real', () => {
  const rows = [co(30), etf(10), co(20), etf(40)];

  it('splits into companies first, then funds', () => {
    const parts = equityParts(EQUITY, EQUITY, rows);
    expect(parts.map((p) => p.key)).toEqual(['stocks', 'funds']);
    expect(parts.map((p) => p.label)).toEqual(['Individual stocks', 'Stock ETFs']);
    expect(parts.map((p) => p.rows.length)).toEqual([2, 2]);
  });

  it('⚠ shares are OF THE CLASS, so the two halves add to 100', () => {
    // 50 of 100 either way here — the point is the denominator, not the arithmetic: these rows are
    // 50% of the BOOK each, and "50% of everything" is what a book-share would have printed.
    const parts = equityParts(EQUITY, EQUITY, rows);
    expect(parts[0].classPct).toBeCloseTo(50, 10);
    expect(parts[1].classPct).toBeCloseTo(50, 10);
  });

  it('divides by the class total even when the class is a slice of the book', () => {
    // A book that is 20% stocks: 15pp companies, 5pp ETFs → 75/25 OF STOCKS, not 15/5 of the book.
    const parts = equityParts(EQUITY, EQUITY, [co(15), etf(5)]);
    expect(parts[0].classPct).toBeCloseTo(75, 10);
    expect(parts[1].classPct).toBeCloseTo(25, 10);
  });

  it('keeps every row — a division moves rows, it never drops them', () => {
    const parts = equityParts(EQUITY, EQUITY, rows);
    expect(parts.flatMap((p) => p.rows)).toHaveLength(rows.length);
    expect(new Set(parts.flatMap((p) => p.rows))).toEqual(new Set(rows));
  });

  it('preserves the order it was handed within each half', () => {
    // The caller sorts before splitting, so the sort must survive — otherwise the table's own
    // sort control would silently stop applying inside Stocks.
    const a = co(1); const b = co(2); const c = co(3);
    expect(equityParts(EQUITY, EQUITY, [c, a, b, etf(9)])[0].rows).toEqual([c, a, b]);
  });
});

describe('⚠ when it is not', () => {
  it('draws no sub-header for a class with no funds', () => {
    const parts = equityParts(EQUITY, EQUITY, [co(30), co(20)]);
    expect(parts).toHaveLength(1);
    expect(parts[0]).toMatchObject({ key: 'all', label: null, classPct: null });
  });

  it('draws no sub-header for a class that is nothing but funds', () => {
    // A heading reading "Stock ETFs" over every row says a division exists where there is none.
    const parts = equityParts(EQUITY, EQUITY, [etf(30), etf(20)]);
    expect(parts).toHaveLength(1);
    expect(parts[0].label).toBeNull();
  });

  it('leaves every other class alone, funds or not', () => {
    // ⚠ Bonds is largely ETFs and Cash has no fund concept at all — the split answers nothing
    // there, and the CALLER names the equity bucket rather than this file guessing which is which.
    for (const bucket of ['Bonds', 'Alternatives', 'Cash', 'Unclassified']) {
      expect(equityParts(bucket, EQUITY, [co(10), etf(10)])).toHaveLength(1);
    }
  });

  it('an empty class is one empty part, not two', () => {
    expect(equityParts(EQUITY, EQUITY, [])).toEqual(
      [{ key: 'all', label: null, rows: [], classPct: null }]);
  });
});

describe('⚠ a weightless class states no share rather than 0%', () => {
  it('reports null when nothing in the class is priced', () => {
    // Every holding unpriced: there is no weight to apportion, and "0% of Stocks" beside a list of
    // real rows claims the book holds none of what it is looking at.
    const parts = equityParts(EQUITY, EQUITY,
      [{ is_fund: false, weight_now_pct: 0 }, { is_fund: true, weight_now_pct: null }]);
    expect(parts).toHaveLength(2);
    expect(parts.map((p) => p.classPct)).toEqual([null, null]);
  });

  it('treats a missing weight as zero for the sum, not as a dropped row', () => {
    const parts = equityParts(EQUITY, EQUITY,
      [co(30), { is_fund: true, weight_now_pct: null }, etf(10)]);
    expect(parts[1].rows).toHaveLength(2);          // the unpriced ETF is still listed
    expect(parts[0].classPct).toBeCloseTo(75, 10);  // 30 of 40
  });
});
