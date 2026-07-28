import { describe, it, expect } from 'vitest';
import {
  MIN_YEAR_COVERAGE_PCT, coverageByYear, marginByYear, weightedByYear, type MarginRow,
} from './marginData';
import { dividendYieldByYear, type DividendYieldRow } from './dividendYieldData';

/**
 * The per-year coverage floor shared by every card on the Long Equity tab. It exists for the
 * NEWEST fiscal year: books close on different dates, so early in a year a couple of holdings have
 * filed and the rest have not — and an average that renormalises over whoever reported draws that
 * as a full-height point in the same ink as a complete year.
 */

const w = (weight_pct: number, years: Record<string, number | null>) => ({ weight_pct, years });
const rows = (...xs: { weight_pct: number; years: Record<string, number | null> }[]) => xs;
const YEARS = (r: { years: Record<string, number | null> }) => Object.keys(r.years);
const VALUE = (r: { years: Record<string, number | null> }, y: string) => r.years[y] ?? null;

describe('weightedByYear', () => {
  it('is the weighted average, renormalised over whoever reported', () => {
    // 60/20 reporting -> (60x10 + 20x30)/80 = 15, NOT (600+600)/100 = 12.
    const out = weightedByYear(rows(w(60, { 2024: 10 }), w(20, { 2024: 30 }), w(20, {})), YEARS, VALUE);
    expect(out.get(2024)).toBeCloseTo(15);
  });

  it('⚠ omits a year the charted set has mostly not reported yet', () => {
    // The 2026 case: two of five holdings have filed. 40% is not the book.
    const out = weightedByYear(
      rows(w(40, { 2025: 8, 2026: 20 }), w(60, { 2025: 8 })), YEARS, VALUE);
    expect([...out.keys()]).toEqual([2025]);
  });

  it('draws a year once the floor is cleared', () => {
    const out = weightedByYear(
      rows(w(85, { 2026: 20 }), w(15, {})), YEARS, VALUE);
    expect(out.get(2026)).toBeCloseTo(20);
  });

  it('⚠ measures coverage against the CHARTED SET, not the whole book', () => {
    // Weights are shares of the whole book, so they sum to 70 here (cash and bonds make up the
    // rest). Against 100 nothing could ever clear 80 and every chart would go blank.
    const out = weightedByYear(rows(w(40, { 2024: 10 }), w(30, { 2024: 10 })), YEARS, VALUE);
    expect(out.get(2024)).toBeCloseTo(10);
  });

  it('a single company is its own 100% — the floor never bites on a company view', () => {
    expect(weightedByYear(rows(w(100, { 2026: 7 })), YEARS, VALUE).get(2026)).toBeCloseTo(7);
  });

  it('the floor is the documented one, shared with the backend blend', () => {
    expect(MIN_YEAR_COVERAGE_PCT).toBe(80);
  });
});

describe('coverageByYear', () => {
  it('reports the share behind each year, including years no point was drawn for', () => {
    const out = coverageByYear(rows(w(40, { 2026: 20 }), w(60, {})), YEARS, VALUE);
    expect(out.get(2026)).toBeCloseTo(40);
  });
});

describe('the floor reaches every card', () => {
  const margin = (weight_pct: number, revenue: Record<string, number | null>,
    fcf: Record<string, number | null>): MarginRow => ({
    isin: 'X', name: 'X', weight_pct, currency: 'EUR', ticker: 'X', exchange: 'XPAR',
    status: 'ok', revenue, fcf, sbc: {},
  });
  const dy = (weight_pct: number, div_ps: Record<string, number | null>,
    price_ps: Record<string, number | null>): DividendYieldRow => ({
    isin: 'X', name: 'X', weight_pct, currency: 'EUR', ticker: 'X', exchange: 'XPAR',
    status: 'ok', div_ps, price_ps,
  });

  it('drops a thin newest year on the FCF-SBC margin', () => {
    const out = marginByYear([margin(30, { 2025: 100, 2026: 100 }, { 2025: 20, 2026: 30 }),
      margin(70, { 2025: 100 }, { 2025: 20 })]);
    expect([...out.keys()]).toEqual([2025]);
  });

  it('drops a thin newest year on the dividend yield', () => {
    const out = dividendYieldByYear([dy(30, { 2025: 2, 2026: 2 }, { 2025: 100, 2026: 50 }),
      dy(70, { 2025: 2 }, { 2025: 100 })]);
    expect([...out.keys()]).toEqual([2025]);
  });
});
