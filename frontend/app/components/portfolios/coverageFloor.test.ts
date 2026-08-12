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
    // Weights are shares of the WHOLE book, so they need not sum to 100 — cash, bonds and anything
    // unpriceable make up the rest. Both of these reported, so the charted set is 100% covered and
    // the year draws. ⚠ The weights sum to 40 on purpose: against a denominator of 100 this would
    // read 40% and be refused, so the case still separates the two bases now the floor is 50 (at
    // the old 80 any book under 80% invested made the point, which is why it read 70 before).
    const out = weightedByYear(rows(w(20, { 2024: 10 }), w(20, { 2024: 10 })), YEARS, VALUE);
    expect(out.get(2024)).toBeCloseTo(10);
  });

  it('a single company is its own 100% — the floor never bites on a company view', () => {
    expect(weightedByYear(rows(w(100, { 2026: 7 })), YEARS, VALUE).get(2026)).toBeCloseTo(7);
  });

  it('the floor is the documented one, shared with the backend blend', () => {
    // 60 → 80 (2026-07-28) → 50 (2026-08-12, on request: half the constituents should draw).
    // ⚠ Must equal `_fundamental_blend.MIN_BLEND_COVERAGE_PCT` — two floors that disagree put two
    // cards on one screen spanning different fractions of the same book.
    expect(MIN_YEAR_COVERAGE_PCT).toBe(50);
  });

  it('⚠ an EVEN SPLIT draws — that is what the floor was lowered for', () => {
    // `<` is the comparison, so exactly 50% covered clears. Half the book reporting is a data
    // point about half the book, labelled as such by `coverageByYear`, rather than a blank.
    const out = weightedByYear(
      rows(w(50, { 2024: 10 }), w(50, {})), YEARS, VALUE);
    expect(out.get(2024)).toBeCloseTo(10);
  });
});

/**
 * PER-PERIOD WEIGHTING — an index is weighted by the cap it HAD in that period, not by today's.
 *
 * Weighting 2018's margin by today's cap is look-ahead bias: measured on the S&P, NVIDIA is
 * carried at 7.46% of a year it was 0.63% of, and the FCF-SBC margin benchmark moves up to 3.00pp.
 * A portfolio has no cap history — a holding weight is not a market cap — so it keeps one basis
 * for every period, which is what the absence of `market_cap_by_period` means.
 */
describe('weightedByYear with per-period caps', () => {
  const c = (weight_pct: number, years: Record<string, number | null>,
    caps: Record<string, number>) => ({ weight_pct, years, market_cap_by_period: caps });

  it('weights by THAT period’s cap, not by the stable weight', () => {
    // Equal weight_pct, wildly unequal caps in 2024: 900/100, so the answer is 12 and NOT the 20
    // that today's-cap weighting gives. This is the whole change in one assertion.
    const out = weightedByYear(
      [c(50, { 2024: 10 }, { 2024: 900 }), c(50, { 2024: 30 }, { 2024: 100 })], YEARS, VALUE);
    expect(out.get(2024)).toBeCloseTo(12);
    expect(out.get(2024)).not.toBeCloseTo(20);
  });

  it('⚠ drops a row with no cap that period from the average entirely', () => {
    // C has a figure and no cap: it cannot be weighted on the same basis as the others, so it is
    // out of both numerator and denominator. Its 999 must not reach the average.
    const out = weightedByYear([
      c(45, { 2024: 10 }, { 2024: 100 }),
      c(45, { 2024: 10 }, { 2024: 100 }),
      c(10, { 2024: 999 }, {}),
    ], YEARS, VALUE);
    expect(out.get(2024)).toBeCloseTo(10);
  });

  it('⚠⚠ measures COVERAGE on the stable weight — the bug that silently disabled the floor', () => {
    // The per-period cap comes out of the same GuruFocus blob as the figure, so a company that has
    // not filed FY2026 has no FY2026 cap either. Measuring coverage with it divides the filers by
    // the filers and reads 100% — which is how FY2026 came to draw a full-height point built
    // almost entirely out of NVIDIA. On the stable weight this is 20% covered, and omitted.
    const out = weightedByYear([
      c(20, { 2026: 20 }, { 2026: 500 }),
      c(80, {}, {}),
    ], YEARS, VALUE);
    expect([...out.keys()]).toEqual([]);
  });

  it('a portfolio keeps ONE basis for every period', () => {
    // No `market_cap_by_period` at all ⇒ the holding weight applies throughout, unchanged.
    const out = weightedByYear(rows(w(50, { 2024: 10 }), w(50, { 2024: 30 })), YEARS, VALUE);
    expect(out.get(2024)).toBeCloseTo(20);
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
