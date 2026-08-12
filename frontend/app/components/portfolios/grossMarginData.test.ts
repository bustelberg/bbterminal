import { describe, it, expect } from 'vitest';
import { grossMarginByYear, grossMarginOf, type GrossMarginRow } from './grossMarginData';

const row = (over: Partial<GrossMarginRow> = {}): GrossMarginRow => ({
  isin: 'X', name: 'X', weight_pct: 100, currency: 'EUR', ticker: null, exchange: null,
  status: 'ok', gross_profit: {}, revenue: {}, ...over,
});

describe('grossMarginOf', () => {
  it('reproduces GuruFocus\'s own published figure', () => {
    // Verified against `annuals__Ratios__Gross Margin %` before this card was built — the reason
    // it is derived rather than read: identical number, plus two lines the drill-down can show.
    expect(grossMarginOf(17258, 32667.3)).toBeCloseTo(52.83, 2);    // ASML 2025, published 52.83
    expect(grossMarginOf(195201, 416161)).toBeCloseTo(46.91, 2);    // Apple 2025, published 46.905
  });

  it('⚠ a missing gross profit is NULL, never 0 — a bank has no such line', () => {
    // GuruFocus's 'B' template has no cost of goods sold, so the key is absent for JPMorgan. The
    // concept does not apply; a 0 would draw a company selling at cost, which is a claim.
    expect(grossMarginOf(null, 100000)).toBeNull();
    expect(grossMarginOf(undefined, 100000)).toBeNull();
  });

  it('⚠ KEEPS a negative gross profit — selling below cost is a real observation', () => {
    expect(grossMarginOf(-20, 100)).toBeCloseTo(-20);
  });

  it('needs a positive denominator', () => {
    expect(grossMarginOf(50, 0)).toBeNull();
    expect(grossMarginOf(50, -100)).toBeNull();
    expect(grossMarginOf(50, null)).toBeNull();
  });
});

describe('grossMarginByYear', () => {
  it('weights each company\'s RATIO, never sums the amounts', () => {
    // ⚠ The amounts are in each company's own reporting currency; adding them would be adding
    // euros to yen. A ratio is currency-free, so the average is safe.
    const rows = [
      row({ weight_pct: 75, gross_profit: { 2025: 50 }, revenue: { 2025: 100 } }),   // 50%
      row({ weight_pct: 25, gross_profit: { 2025: 10 }, revenue: { 2025: 100 } }),   // 10%
    ];
    expect(grossMarginByYear(rows).get(2025)).toBeCloseTo(0.75 * 50 + 0.25 * 10, 6);
  });

  it('excludes a company with no margin rather than counting it as 0', () => {
    // A bank in the book must not drag the average toward zero — it has no gross margin at all,
    // so it leaves the average rather than entering it as a nil.
    const rows = [
      row({ weight_pct: 90, gross_profit: { 2025: 40 }, revenue: { 2025: 100 } }),
      row({ weight_pct: 10, gross_profit: {}, revenue: { 2025: 100 } }),             // bank
    ];
    expect(grossMarginByYear(rows).get(2025)).toBeCloseTo(40, 6);   // NOT 0.9 * 40 = 36
  });

  it('⚠ refuses the year entirely once too much of the book has no gross margin', () => {
    // `MIN_YEAR_COVERAGE_PCT` is the shared floor every card on this tab honours. A book that is
    // mostly banks would otherwise print a confident "gross margin" describing the rest — the same
    // renormalise-over-what-we-can-price fabrication the AIRS coverage floor guards.
    // ⚠ 40/60, NOT 50/50: the floor moved to 50 (2026-08-12) and an even split now clears it by
    // design, so the refusal has to be tested below half.
    const rows = [
      row({ weight_pct: 40, gross_profit: { 2025: 40 }, revenue: { 2025: 100 } }),
      row({ weight_pct: 60, gross_profit: {}, revenue: { 2025: 100 } }),             // banks
    ];
    expect(grossMarginByYear(rows).has(2025)).toBe(false);
  });

  it('is empty when nothing is computable', () => {
    expect(grossMarginByYear([row({ revenue: { 2025: 0 }, gross_profit: { 2025: 5 } })]).size).toBe(0);
  });
});
