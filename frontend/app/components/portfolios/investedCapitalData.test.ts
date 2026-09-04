import { describe, expect, it } from 'vitest';
import { investedCapitalIndexByYear } from './investedCapitalData';
import { type CashReturnRow } from './cashReturnData';

/**
 * The invested-capital blend, and the SPAC-shell row it has to survive.
 *
 * ⚠ THE FIXTURE IS REAL. These are Vertiv Holdings' stored figures (company 2852, `VRT`), which
 * listed via SPAC in Feb 2020 — so the pre-2020 fiscal years under that ticker belong to the
 * blank-cheque shell: $24–25k of founder capital, then the June 2018 IPO trust, then the actual
 * business in 2020. `base > 0` waved 0.024 through and put a single row into the S&P 500 line at an
 * index of 2,784,248 (2018) and 31,221,600 (2025) — measured, the drawn line read 33,849 in 2025
 * where the honest figure is ~561, and the whole 2017→2018 "skyrocket" was this one company.
 */
const VERTIV = (caps: Record<string, number> | undefined): CashReturnRow => ({
  isin: 'US92537N1081',
  name: 'Vertiv Holdings Co',
  weight_pct: 100,
  ...(caps ? { market_cap_by_period: caps } : {}),
  currency: 'USD',
  ticker: 'VRT',
  exchange: 'NYSE',
  status: 'ok',
  fcf: {},
  sbc: {},
  roic: {},
  noncurrent_liabilities: { '2016': 0, '2017': 0, '2018': 24.15, '2020': 2820.1 },
  total_equity: { '2016': 0.025, '2017': 0.024, '2018': 671.912, '2020': 512.1 },
});

/** Its real caps: nothing until the June 2018 IPO. A cap of 0 is not a cap. */
const CAPS = { '2016': 0, '2017': 0, '2018': 845.25, '2020': 6385.607 };

/**
 * ⚠⚠ THIS LINE IS NOW CHAINED FROM WEIGHTED GROWTH (2026-08-21) — it goes through `buildBlend`,
 * the same rule as every other level series on the tab, having previously averaged each member's
 * REBASED LEVEL. Measured on ACWI 2015→2025: the old construction read +18.14%/yr against the
 * chain's +10.81%/yr.
 *
 * ⚠ THE FIRST TWO CASES BELOW ARE UNCHANGED BY THAT, AND THAT IS THE POINT OF KEEPING THEM. A
 * zero-cap period is excluded from the average either way (`wAt` returns null), so the shell years
 * are still absent and the first DRAWN period is still the index's 100. What changed is what
 * happens to a member whose base is a rounding artefact — see the third case.
 */
describe('investedCapitalIndexByYear', () => {
  it('excludes a period the row cannot be weighted in, so the shell years are not the base', () => {
    const idx = investedCapitalIndexByYear([VERTIV(CAPS)]);
    // The shell years carry no cap, so they are not in any period's average — and therefore
    // cannot be the base of the index that average is taken over.
    expect(idx.get(2016)).toBeUndefined();
    expect(idx.get(2017)).toBeUndefined();
    expect(idx.get(2018)).toBe(100);
    // 2020: (2820.1 + 512.1) / (24.15 + 671.912) = 4.787…
    expect(idx.get(2020)).toBeCloseTo(478.72, 1);
  });

  it('does NOT let a $24k founder-capital base through just because it is positive', () => {
    // The old rule's answer, pinned so a regression is unmistakable rather than merely large.
    const idx = investedCapitalIndexByYear([VERTIV(CAPS)]);
    expect(idx.get(2018)).not.toBeCloseTo(2_784_248, -2);
  });

  it('⚠⚠ a PORTFOLIO row is NO LONGER protected — the step guards were removed on request', () => {
    // ⚠⚠⚠ THIS FIXTURE NOW PINS WHAT WAS GIVEN UP, AND IT IS THE CLEAREST STATEMENT OF IT ANYWHERE.
    // Both magnitude heuristics were removed on 2026-09-04, on request — `MIN_STEP_BASE_FRACTION`
    // (a member's anchor under 10% of its own median) and `MAX_STEP_GROWTH` (a step over 100x). A
    // portfolio row has no per-period cap, so nothing excludes Vertiv's SPAC-shell years, and the
    // 0.024 → 696.062 step (~29,000x) is now taken.
    //
    // The result is EXACTLY the number the guards were built to stop: an index of 2,784,248 at
    // 2018 off $24k of founder capital — three different legal entities in one ticker's column.
    //
    // ⚠ IT IS NOT A REGRESSION, IT IS THE AGREED TRADE. The rules that caught this also refused 44
    // steps that were flat, falling or under 2x across ACWI's five lines, and cost 6.72pp/yr on
    // FCF/share; a threshold on the answer cannot tell a shell year from a trough year. Catching
    // this case belongs in a STRUCTURAL test — an entity discontinuity at the listing date, which
    // is what a SPAC actually is — not in a magnitude rule. Until that exists, a portfolio holding
    // a post-SPAC ticker draws a line nobody can read, and that is visible rather than silent.
    const idx = investedCapitalIndexByYear([VERTIV(undefined)]);
    expect(idx.get(2016)).toBe(100);
    expect(idx.get(2017)).toBeCloseTo(96, 6);            // 0.024 / 0.025 — the shell's own year
    expect(idx.get(2018)).toBeCloseTo(2_784_248, -2);    // the 29,000x step, now taken
    expect(idx.get(2020)).toBeCloseTo(13_328_800, -2);   // and compounding from there
  });

  it('chains from weighted growth, so the base cancels out of the answer', () => {
    // ⚠ THE SAME ROW AT TEN TIMES THE SCALE MUST DRAW THE IDENTICAL INDEX. That is the property
    // that makes a base irrelevant, and it is what the old average-of-rebased-levels rule could
    // not offer — there the base decided where a member sat in the average.
    const scaled = (k: number): CashReturnRow => ({
      ...VERTIV(CAPS),
      noncurrent_liabilities: { '2018': 24.15 * k, '2020': 2820.1 * k },
      total_equity: { '2018': 671.912 * k, '2020': 512.1 * k },
    });
    const a = investedCapitalIndexByYear([scaled(1)]);
    const b = investedCapitalIndexByYear([scaled(10)]);
    expect(a.get(2020)).toBeCloseTo(b.get(2020) as number, 6);
  });
});
