import { describe, it, expect } from 'vitest';
import {
  coverageByYear, dividendYieldByYear, dividendYieldOf, type DividendYieldRow,
} from './dividendYieldData';

const row = (name: string, weight_pct: number,
  div_ps: Record<string, number | null>, price_ps: Record<string, number | null>): DividendYieldRow =>
  ({ isin: name, name, weight_pct, currency: 'EUR', ticker: name, exchange: 'XPAR',
    status: 'ok', div_ps, price_ps });

describe('dividendYieldOf', () => {
  it('is DPS over the year-end price, as a percent', () => {
    expect(dividendYieldOf(2.5, 100)).toBeCloseTo(2.5);
  });

  it('a reported 0.00 is a REAL yield of zero — a non-payer, not a gap', () => {
    expect(dividendYieldOf(0, 100)).toBe(0);
  });

  it('⚠ an ABSENT dividend line is not a zero', () => {
    // Reading it as 0 would let un-ingested holdings deflate the book's yield with a number
    // nobody reported. Unknown stays unknown, and the average renormalises over the rest.
    expect(dividendYieldOf(null, 100)).toBeNull();
    expect(dividendYieldOf(undefined, 100)).toBeNull();
  });

  it('needs a positive price — the denominator is not optional', () => {
    expect(dividendYieldOf(2.5, null)).toBeNull();
    expect(dividendYieldOf(2.5, 0)).toBeNull();
    expect(dividendYieldOf(2.5, -1)).toBeNull();
  });
});

describe('dividendYieldByYear', () => {
  it('weight-averages the holdings, which for value weights IS the portfolio yield', () => {
    // 75% at 4%, 25% at 0% -> 3.0%. Verified against Σ(value·yield)/Σvalue on the same book.
    const rows = [row('A', 75, { 2024: 4 }, { 2024: 100 }), row('B', 25, { 2024: 0 }, { 2024: 50 })];
    expect(dividendYieldByYear(rows).get(2024)).toBeCloseTo(3.0);
  });

  it('a non-payer counts as 0 and pulls the book down — that is the point', () => {
    const payer = [row('A', 50, { 2024: 4 }, { 2024: 100 })];
    const both = [...payer, row('B', 50, { 2024: 0 }, { 2024: 50 })];
    expect(dividendYieldByYear(payer).get(2024)).toBeCloseTo(4.0);
    expect(dividendYieldByYear(both).get(2024)).toBeCloseTo(2.0);
  });

  it('renormalises over the holdings that reported, rather than counting the rest as zero', () => {
    // B has no dividend line at all: the year is 4%, over the 85% that reported — NOT 3.4%.
    // (85/15 rather than 50/50 so the year clears the shared coverage floor and the
    // renormalisation is observable at all — at 50% no point is drawn, which is the floor's job.)
    const rows = [row('A', 85, { 2024: 4 }, { 2024: 100 }), row('B', 15, {}, { 2024: 50 })];
    expect(dividendYieldByYear(rows).get(2024)).toBeCloseTo(4.0);
  });

  it('a company that starts paying mid-window still charts — no rebase, no drop', () => {
    // The bug this card replaces: a level series starting at 0.00 could not be rebased to an
    // index and the holding was dropped from the metric entirely.
    const rows = [row('A', 100, { 2015: 0, 2024: 3 }, { 2015: 60, 2024: 100 })];
    const y = dividendYieldByYear(rows);
    expect(y.get(2015)).toBe(0);
    expect(y.get(2024)).toBeCloseTo(3.0);
  });
});

describe('coverageByYear', () => {
  it('reports the share of the WHOLE book a year is computed over', () => {
    const rows = [row('A', 50, { 2024: 4 }, { 2024: 100 }), row('B', 30, {}, { 2024: 50 }),
      row('C', 20, { 2024: 1 }, {})];
    expect(coverageByYear(rows).get(2024)).toBeCloseTo(50);
  });
});
