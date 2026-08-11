import { describe, it, expect } from 'vitest';
import {
  defaultDiscountRate, FALLBACK_DISCOUNT_RATE, FORECAST_YEARS, impliedGrowth, marketCapOf,
  modelValue, PERPETUITY_GROWTH, solveGrowth,
  type ReverseDcfAssumptions, type ReverseDcfInputs,
} from './reverseDcf';

// ⚠ THE BASELINE LIVES HERE NOW. It was `REVERSE_DCF_DEFAULTS`, exported from the module — but
// nothing in production ever read it, so it was a test fixture wearing a module export's clothes
// and the dead-code sweep (2026-08-03) removed it. Assembled from the module's own constants
// rather than hard-coded, so a change to the forecast horizon or terminal growth still reaches
// these tests instead of silently diverging from them.
const A: ReverseDcfAssumptions = {
  years: FORECAST_YEARS,
  perpetuityGrowth: PERPETUITY_GROWTH,
  discountRates: [FALLBACK_DISCOUNT_RATE],
};   // 10 years, 3% terminal, discounted at 10%

const INPUTS: ReverseDcfInputs = {
  price: 100, sharesOutstanding: 1000, fcf: 9500,     // 100,000M market cap, ~10.5x FCF
};

describe('modelValue', () => {
  it('⚠ handles g = r, where the closed form is 0/0', () => {
    // Every discounted term is fcf/(1+r), so the explicit leg is n·fcf/(1+r). Without the limit the
    // solver hits ±Infinity mid-bracket and the sign test decides on a non-number.
    const atLimit = modelValue(1000, 0.10, 0.10, 10, 0.03) as number;
    const explicit = 1000 * 10 / 1.10;
    const terminal = (1000 * Math.pow(1.10, 9) * 1.03 / (0.10 - 0.03)) / Math.pow(1.10, 10);
    expect(atLimit).toBeCloseTo(explicit + terminal, 6);
  });

  it('is continuous either side of that band', () => {
    // ⚠ RELATIVE, not absolute: these are values in the tens of thousands, and the closed form
    // genuinely moves ~1.5 per 1e-5 of g. An absolute tolerance tests the units, not the continuity.
    const at = modelValue(1000, 0.10, 0.10, 10, 0.03) as number;
    for (const d of [-1e-5, 1e-5]) {
      const v = modelValue(1000, 0.10 + d, 0.10, 10, 0.03) as number;
      expect(Math.abs(v / at - 1)).toBeLessThan(1e-3);
    }
  });

  it('refuses a terminal growth at or above the discount rate', () => {
    // Not a large value — an invalid one.
    expect(modelValue(1000, 0.05, 0.03, 10, 0.03)).toBeNull();
    expect(modelValue(1000, 0.05, 0.02, 10, 0.03)).toBeNull();
  });

  it('rises with growth and falls with the discount rate', () => {
    const base = modelValue(1000, 0.08, 0.10, 10, 0.03) as number;
    expect(modelValue(1000, 0.09, 0.10, 10, 0.03) as number).toBeGreaterThan(base);
    expect(modelValue(1000, 0.08, 0.11, 10, 0.03) as number).toBeLessThan(base);
  });
});

describe('solveGrowth — the round trip', () => {
  // The strongest test there is: price the model at a known g, then ask the solver to find it.
  const cases: [number, number][] = [
    [0.08, 0.10], [0.00, 0.10], [-0.10, 0.09], [0.05, 0.07],
    [0.15, 0.18], [0.03, 0.12], [0.115, 0.20], [-0.25, 0.14],
  ];
  for (const [g, r] of cases) {
    it(`recovers g = ${(g * 100).toFixed(1)}% at r = ${(r * 100).toFixed(0)}%`, () => {
      const target = modelValue(9500, g, r, 10, 0.03) as number;
      expect(solveGrowth(9500, target, r, 10, 0.03)).toBeCloseTo(g, 6);
    });
  }

  it('recovers a growth rate pressed right up against the discount rate', () => {
    const r = 0.10;
    for (const g of [r - 1e-4, r - 1e-7, r, r + 1e-7, r + 1e-4]) {
      const target = modelValue(9500, g, r, 10, 0.03) as number;
      expect(solveGrowth(9500, target, r, 10, 0.03)).toBeCloseTo(g, 5);
    }
  });

  it('⚠ SOLVES ABOVE THE DISCOUNT RATE — a 10-year annuity is not a perpetuity', () => {
    // The bracket used to stop at r, on the perpetuity rule that g < r. Over a FINITE horizon the
    // sum converges for any g: at g > r the closed form's numerator and denominator both go
    // negative and it stays correct. Capping there made every richly-priced company read "out of
    // range" — measured, a 55x-FCF market cap that in fact implies ~24% growth.
    for (const g of [0.15, 0.23, 0.40, 0.75]) {
      const target = modelValue(9500, g, 0.10, 10, 0.03) as number;
      expect(solveGrowth(9500, target, 0.10, 10, 0.03)).toBeCloseTo(g, 6);
    }
  });

  it('a 55x-FCF market cap resolves to a plausible growth rate, not a refusal', () => {
    // The measured case: EUR 613,606M against EUR 11,160M of free cash flow at 10%.
    const g = solveGrowth(11160, 613606, 0.10, 10, 0.03);
    expect(g).not.toBeNull();
    expect(g as number).toBeGreaterThan(0.20);
    expect(g as number).toBeLessThan(0.26);
  });
});

describe('solveGrowth — when there is no answer', () => {
  it('⚠ returns null rather than the bracket end', () => {
    // A price beyond even 1000%/yr for a decade. Clamping to the ceiling would render as a real
    // figure — the one wrong answer worse than "out of range".
    const absurd = (modelValue(9500, 10.0, 0.10, 10, 0.03) as number) * 10;
    expect(solveGrowth(9500, absurd, 0.10, 10, 0.03)).toBeNull();
  });

  it('returns null below one year of discounted cash flow — the model floor', () => {
    // At g = −99% every year past the first is ~0, leaving fcf/(1+r). A price under that has no
    // solution because of what a DCF is, not because of a bound anyone chose.
    const tiny = (modelValue(9500, -0.99, 0.10, 10, 0.03) as number) / 100;
    expect(solveGrowth(9500, tiny, 0.10, 10, 0.03)).toBeNull();
  });

  it('cannot solve on a non-positive cash flow', () => {
    expect(solveGrowth(0, 100000, 0.10, 10, 0.03)).toBeNull();
    expect(solveGrowth(-500, 100000, 0.10, 10, 0.03)).toBeNull();
  });

  it('refuses a discount rate at or below the perpetuity growth', () => {
    expect(solveGrowth(9500, 100000, 0.03, 10, 0.03)).toBeNull();
    expect(solveGrowth(9500, 100000, 0.02, 10, 0.03)).toBeNull();
  });
});

describe('impliedGrowth', () => {
  it('solves against the market cap the inputs imply', () => {
    // ⚠ RELATIVE: the solver converges to 1e-8 on g, which on a six-figure valuation is ~0.0015
    // absolute. An absolute tolerance here would be testing the units, not the solve.
    const g = impliedGrowth(INPUTS, A)[0].impliedGrowth as number;
    const v = modelValue(9500, g, 0.10, 10, 0.03) as number;
    expect(Math.abs(v / 100_000 - 1)).toBeLessThan(1e-6);
  });

  it('rises with the discount rate — more required return means believing more growth', () => {
    const rows = impliedGrowth(INPUTS, { ...A, discountRates: [0.07, 0.10, 0.14] });
    const gs = rows.map((r) => r.impliedGrowth as number);
    expect(gs.every((g) => g != null)).toBe(true);
    for (let i = 1; i < gs.length; i++) expect(gs[i]).toBeGreaterThan(gs[i - 1]);
  });

  it('⚠ a negative implied growth is a real answer and is never clamped', () => {
    // The market pricing in decline. Clamping at zero would hide exactly the case worth seeing.
    expect(impliedGrowth({ ...INPUTS, price: 20 }, A)[0].impliedGrowth as number).toBeLessThan(0);
  });

  it('honours the override in place of the reported figure', () => {
    const base = impliedGrowth(INPUTS, A)[0].impliedGrowth as number;
    const richer = impliedGrowth(INPUTS, { ...A, fcfOverride: 19000 })[0].impliedGrowth as number;
    expect(richer).toBeLessThan(base);        // twice the cash flow needs less growth
  });

  it('is null when an input is missing, and never throws', () => {
    expect(impliedGrowth({ ...INPUTS, fcf: null }, A)[0].impliedGrowth).toBeNull();
    expect(impliedGrowth({ ...INPUTS, price: null }, A)[0].impliedGrowth).toBeNull();
    expect(() => impliedGrowth(
      { price: null, sharesOutstanding: null, fcf: null }, A)).not.toThrow();
  });
});

describe('marketCapOf', () => {
  it('is price times shares', () => {
    expect(marketCapOf(INPUTS)).toBeCloseTo(100_000, 6);
    expect(marketCapOf({ ...INPUTS, sharesOutstanding: null })).toBeNull();
  });
});

describe('defaultDiscountRate', () => {
  it("uses the company's own WACC when it is usable", () => {
    expect(defaultDiscountRate(0.082)).toBeCloseTo(0.082, 9);
  });

  it('⚠ falls back when the WACC is at or near the perpetuity growth', () => {
    // The Gordon leg divides by (r − gp): a 2.5% WACC against 3% terminal growth is a NEGATIVE
    // denominator and a negative valuation, and the panel would read "no solution" for a company
    // whose only sin is a low cost of capital.
    for (const wacc of [0.03, 0.025, 0.035, 0.01, -0.05]) {
      expect(defaultDiscountRate(wacc)).toBeCloseTo(FALLBACK_DISCOUNT_RATE, 9);
    }
    expect(defaultDiscountRate(0.041)).toBeCloseTo(0.041, 9);   // clears gp + 1pp
  });

  it('falls back on an absent or implausible reading', () => {
    expect(defaultDiscountRate(null)).toBeCloseTo(FALLBACK_DISCOUNT_RATE, 9);
    expect(defaultDiscountRate(Number.NaN)).toBeCloseTo(FALLBACK_DISCOUNT_RATE, 9);
    expect(defaultDiscountRate(0.85)).toBeCloseTo(FALLBACK_DISCOUNT_RATE, 9);
  });

  it('honours a non-default perpetuity growth', () => {
    expect(defaultDiscountRate(0.055, 0.05)).toBeCloseTo(FALLBACK_DISCOUNT_RATE, 9);
    expect(defaultDiscountRate(0.075, 0.05)).toBeCloseTo(0.075, 9);
  });
});

describe('the target market cap can be overridden', () => {
  it('solves against a hypothetical valuation instead of today\'s', () => {
    const atMarket = impliedGrowth(INPUTS, A)[0].impliedGrowth as number;
    const atHalf = impliedGrowth(INPUTS, { ...A, targetOverride: 50_000 })[0].impliedGrowth as number;
    expect(atHalf).toBeLessThan(atMarket);      // half the price needs less growth
  });

  it('falls back to the actual market cap when not set', () => {
    expect(impliedGrowth(INPUTS, { ...A, targetOverride: null })[0].impliedGrowth)
      .toBeCloseTo(impliedGrowth(INPUTS, A)[0].impliedGrowth as number, 9);
  });
});

describe('the ceiling does not refuse answerable questions', () => {
  it('⚠ solves a market cap that the old 100%/yr cap turned into "no solution"', () => {
    // Measured: ~3,643M of free cash flow behind a multi-trillion market cap. At the old bound the
    // panel said "no answer"; the honest reply is a figure so large it settles the question.
    const g = solveGrowth(3643, 15_000_000, 0.10, 10, 0.03);
    expect(g).not.toBeNull();
    expect(g as number).toBeGreaterThan(1.0);       // above the old ceiling
    expect(g as number).toBeLessThan(10.0);
  });

  it('round-trips above the old ceiling', () => {
    for (const g of [1.2, 2.5, 6.0]) {
      const target = modelValue(9500, g, 0.10, 10, 0.03) as number;
      expect(solveGrowth(9500, target, 0.10, 10, 0.03)).toBeCloseTo(g, 5);
    }
  });
});
