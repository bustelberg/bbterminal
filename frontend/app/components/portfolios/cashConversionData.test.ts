import { describe, it, expect } from 'vitest';
import {
  cashConversionByYear, cashConversionOf, type CashConversionRow,
} from './cashConversionData';

const row = (over: Partial<CashConversionRow> = {}): CashConversionRow => ({
  isin: 'X', name: 'X', weight_pct: 100, currency: 'EUR', ticker: null, exchange: null,
  status: 'ok', fcf: {}, sbc: {}, net_income: {}, ...over,
});

describe('cashConversionOf', () => {
  it('reproduces the measured figures', () => {
    expect(cashConversionOf(11027.3, 9609.4)).toBeCloseTo(114.76, 2);   // ASML 2025
    expect(cashConversionOf(98767, 112010)).toBeCloseTo(88.18, 2);      // Apple 2025
  });

  it('⚠ does NOT clamp above 100% — that is the healthy case, not an error', () => {
    // Depreciation running ahead of capex converts more cash than the accounts book as profit.
    expect(cashConversionOf(150, 100)).toBeCloseTo(150);
  });

  it('⚠ KEEPS a negative FCF against positive earnings', () => {
    // Profit with no cash behind it is the entire reason this ratio exists; it belongs below zero.
    expect(cashConversionOf(-40, 100)).toBeCloseTo(-40);
  });

  it('⚠ refuses a non-positive denominator', () => {
    // A loss-maker with POSITIVE cash flow would print a negative conversion, reading as "burning
    // cash" when the opposite is happening — and two companies could show −80% for opposite
    // reasons. The ratio does not apply to a loss.
    expect(cashConversionOf(100, -50)).toBeNull();
    expect(cashConversionOf(100, 0)).toBeNull();
    expect(cashConversionOf(-100, -50)).toBeNull();
  });

  it('is null on a missing input rather than assuming zero', () => {
    expect(cashConversionOf(null, 100)).toBeNull();
    expect(cashConversionOf(100, null)).toBeNull();
  });
});

describe('cashConversionByYear', () => {
  it('weights each company\'s RATIO, never sums the amounts', () => {
    const rows = [
      row({ weight_pct: 60, fcf: { 2025: 120 }, net_income: { 2025: 100 } }),   // 120%
      row({ weight_pct: 40, fcf: { 2025: 70 }, net_income: { 2025: 100 } }),    // 70%
    ];
    expect(cashConversionByYear(rows).get(2025)).toBeCloseTo(0.6 * 120 + 0.4 * 70, 6);
  });

  it('excludes a loss-making holding instead of scoring it', () => {
    const rows = [
      row({ weight_pct: 90, fcf: { 2025: 110 }, net_income: { 2025: 100 } }),
      row({ weight_pct: 10, fcf: { 2025: 50 }, net_income: { 2025: -20 } }),    // loss
    ];
    expect(cashConversionByYear(rows).get(2025)).toBeCloseTo(110, 6);
  });

  it('⚠ drops the year once too much of the book has no ratio', () => {
    // The shared `MIN_YEAR_COVERAGE_PCT` floor — a book mostly in losses must not publish a "cash
    // conversion" that silently describes only the profitable part.
    // ⚠ 40/60, NOT 50/50: the floor moved to 50 (2026-08-12) and `<` is the comparison, so an even
    // split now clears it BY DESIGN. The case being pinned is "the computable part is under the
    // floor", which needs it under half.
    const rows = [
      row({ weight_pct: 40, fcf: { 2025: 110 }, net_income: { 2025: 100 } }),
      row({ weight_pct: 60, fcf: { 2025: 50 }, net_income: { 2025: -20 } }),
    ];
    expect(cashConversionByYear(rows).has(2025)).toBe(false);
  });
});
