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

  it('⚠ a PORTFOLIO row is now protected too, by the step guards rather than by the base', () => {
    // ⚠⚠ THIS IS THE CASE THAT CHANGED, AND IT CHANGED FOR THE BETTER. The old base rule was
    // explicitly INERT here: a holding weight has no history, so "first weightable period" is
    // "first period with a figure", and a portfolio holding Vertiv was shown the shell's 0.024
    // base — an index running to 13,328,800 by 2020, documented as an accepted limit.
    //
    // The chain has no base to get wrong (`g = at(y)/at(anchor) − 1` divides it out) and refuses
    // the STEP instead, twice over: 0.024 → 696.062 is ~29,000x (past `_MAX_STEP_GROWTH`, 100x) and
    // 0.024 is 0.00002 of this member's own median (under `_MIN_STEP_BASE_FRACTION`, 0.10). So the
    // line starts and stops rather than drawing a number nobody can read — which is the honest
    // answer for a ticker whose reported history is three different entities.
    const idx = investedCapitalIndexByYear([VERTIV(undefined)]);
    expect(idx.get(2016)).toBe(100);
    expect(idx.get(2017)).toBeUndefined();
    // And above all: never the seven-figure index the old rule drew here.
    expect([...idx.values()].every((v) => v < 1e4)).toBe(true);
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
