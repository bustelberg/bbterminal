import { describe, it, expect } from 'vitest';
import { calculateEGM, EGM_DEFAULTS, type EgmInputs } from './egm';

const CASE: EgmInputs = { price: 281.365, forwardPE: 25, epsNextFY: 11.46 };
// The known-good case's 0.3% yield is now an ASSUMPTION, so it rides here rather than in CASE.
const A = { ...EGM_DEFAULTS, dividendYield: 0.003 };

describe('calculateEGM — the known-good case', () => {
  const r = calculateEGM(CASE, A);

  it('maxPE is the highest multiple that still clears the hurdle', () => {
    expect(r.maxPE).toBeCloseTo(20.608, 3);
  });

  it('fairValue is next-year EPS at that multiple', () => {
    expect(r.fairValue).toBeCloseTo(236.17, 2);
  });

  it('upside compares it with the price paid', () => {
    expect(r.upside).toBeCloseTo(-0.1606, 4);
  });

  it('peRatio is how much room there is to buy', () => {
    expect(r.peRatio).toBeCloseTo(20.608 / 25, 4);
  });

  it('expectedReturn at forwardPE 25 is 7.90%, NOT the sheet\'s 8.09%', () => {
    // ⚠ THE ONLY FIGURE IN THE SPEC THAT DOES NOT RECONCILE, AND IT IS THE INPUT'S FAULT, NOT THE
    // FORMULA'S. `forwardPE = 25` is a back-solve (the sheet's P/E cells read #LOGIN!), while the
    // 8.09% came off the sheet with the real multiple. maxPE / fairValue / upside are unaffected —
    // none of them touch forwardPE — which is why those three match to the digit and this one does
    // not. Pinned at the value the stated formula actually produces, so the day the real P/E lands
    // this test tells the truth about which number moved.
    //   1.1033 × (20/25)^0.1 − 1 = 0.078953
    expect(r.expectedReturn).toBeCloseTo(0.078953, 6);
  });

  it('reproduces the sheet\'s 8.09% at the forward P/E that implies it', () => {
    // Back-solved from the sheet: 1.1033 × (20/x)^0.1 − 1 = 0.0809  →  x ≈ 24.554.
    const r2 = calculateEGM({ ...CASE, forwardPE: 24.554 }, A);
    expect(r2.expectedReturn).toBeCloseTo(0.0809, 4);
  });
});

describe('calculateEGM — a missing input blanks only what it touches', () => {
  it('⚠ a loss-making company keeps its fair value', () => {
    // expectedReturn and peRatio divide by the forward P/E; the other three never touch it. A
    // panel that blanked entirely here would hide three answers it still has.
    for (const forwardPE of [null, 0, -25]) {
      const r = calculateEGM({ ...CASE, forwardPE }, A);
      expect(r.expectedReturn).toBeNull();
      expect(r.peRatio).toBeNull();
      expect(r.maxPE).toBeCloseTo(20.608, 3);
      expect(r.fairValue).toBeCloseTo(236.17, 2);
      expect(r.upside).toBeCloseTo(-0.1606, 4);
    }
  });

  it('a negative forward P/E returns null, never NaN', () => {
    // (20 / −25)^0.1 is the tenth root of a negative number — JavaScript hands back NaN rather
    // than raising, and NaN reaches the screen as "NaN%" unless it is caught here.
    const r = calculateEGM({ ...CASE, forwardPE: -25 }, A);
    expect(Number.isNaN(r.expectedReturn as number)).toBe(false);
    expect(r.expectedReturn).toBeNull();
  });

  it('a non-positive EPS estimate has no fair value, and therefore no upside', () => {
    for (const epsNextFY of [0, -1.5, null]) {
      const r = calculateEGM({ ...CASE, epsNextFY }, A);
      expect(r.fairValue).toBeNull();
      expect(r.upside).toBeNull();
      expect(r.maxPE).toBeCloseTo(20.608, 3);          // the multiple is still knowable
      expect(r.expectedReturn).toBeCloseTo(0.078953, 6);
    }
  });

  it('a null dividend yield is a non-payer — a real 0, not an unknown', () => {
    const r = calculateEGM(CASE, { ...A, dividendYield: null });
    const zero = calculateEGM(CASE, { ...A, dividendYield: 0 });
    expect(r.maxPE).toBeCloseTo(zero.maxPE as number, 10);
    expect(r.maxPE).toBeCloseTo(20, 10);               // compounder == discount → maxPE == exitPE
  });

  it('no price means no upside, but the fair value still stands', () => {
    for (const price of [null, 0, -5]) {
      const r = calculateEGM({ ...CASE, price }, A);
      expect(r.upside).toBeNull();
      expect(r.fairValue).toBeCloseTo(236.17, 2);
    }
  });
});

describe('calculateEGM — assumptions that cannot produce a valuation', () => {
  it('returns all nulls rather than a plausible-looking number', () => {
    // ⚠ A NEGATIVE COMPOUNDER RAISED TO AN EVEN NUMBER OF YEARS COMES BACK POSITIVE. Left
    // unguarded, growthRate = −2 renders an entirely ordinary maxPE built on arithmetic that
    // stopped meaning anything ten years ago.
    const bad = [
      { ...A, growthRate: -2 },
      { ...A, hurdleRate: -1.5 },
      { ...A, years: 0 },
      { ...A, exitPE: 0 },
      { ...A, exitPE: -20 },
      { ...A, years: Number.NaN },
    ];
    for (const a of bad) {
      const r = calculateEGM(CASE, a);
      // ⚠ AN EXHAUSTIVE `toEqual`, DELIBERATELY — it fails when a new output is added and not
      // given a null here, which is how `bridge` was caught the day it landed. A per-key check
      // would have let a new field arrive already broken in every refusal case.
      expect(r).toEqual({
        maxPE: null, fairValue: null, upside: null, expectedReturn: null, peRatio: null,
        bridge: null, impliedPrice: null, priceReturn: null, priceCagr: null, totalReturn: null,
      });
    }
  });

  it('never throws on any combination of nulls', () => {
    expect(() => calculateEGM(
      { price: null, forwardPE: null, epsNextFY: null }, { ...A, dividendYield: null })).not.toThrow();
  });
});

describe('calculateEGM — the reading of it', () => {
  it('growth plus dividends beating the hurdle earns the right to pay above the exit multiple', () => {
    const r = calculateEGM(CASE, { ...A, growthRate: 0.15 });
    expect(r.maxPE as number).toBeGreaterThan(A.exitPE);
    expect(r.peRatio as number).toBeGreaterThan(1);     // room to buy
  });

  it('paying above the exit multiple drags the return below growth plus dividends', () => {
    const base = (1 + A.growthRate) * (1 + 0.003) - 1;  // 10.33% before any rerating
    const rich = calculateEGM({ ...CASE, forwardPE: 30 }, A);
    const cheap = calculateEGM({ ...CASE, forwardPE: 15 }, A);
    expect(rich.expectedReturn as number).toBeLessThan(base);
    expect(cheap.expectedReturn as number).toBeGreaterThan(base);
  });

  it('at exactly the exit multiple the rerating term vanishes', () => {
    const r = calculateEGM({ ...CASE, forwardPE: A.exitPE }, A);
    expect(r.expectedReturn).toBeCloseTo((1 + A.growthRate) * 1.003 - 1, 10);
  });
});

/**
 * ⚠⚠ THE BRIDGE EXISTS BECAUSE THE PANEL'S OWN SUBTITLE IS AN ADDITION AND THE MODEL IS A PRODUCT.
 * "earnings growth + dividend yield + change in the multiple" is the right intuition and the wrong
 * arithmetic: a reader who takes it literally and adds the three drivers gets a number that is not
 * the one in the tile beside them, and both look entirely reasonable.
 *
 * These tests pin the two halves of the fix — that the FACTORS tie to the total exactly, and that
 * the RATES deliberately do not. The second is the one at risk: the natural instinct on seeing a
 * breakdown whose column does not add up is to "fix" it, and the fix is wrong.
 */
describe('calculateEGM — the return bridge', () => {
  const r = calculateEGM(CASE, A);
  const bridge = r.bridge!;

  it('⚠⚠ THE FACTORS MULTIPLY TO THE TOTAL EXACTLY — a breakdown must tie to what it breaks down', () => {
    const product = bridge.legs.reduce((p, l) => p * l.factor, 1);
    expect(product).toBeCloseTo(bridge.factor, 12);
    expect(bridge.factor - 1).toBeCloseTo(r.expectedReturn as number, 12);
    expect(bridge.rate).toBe(r.expectedReturn);
  });

  it('⚠⚠ THE RATES DO **NOT** ADD TO IT, AND THAT IS NOT A BUG TO FIX', () => {
    // Drivers compound. If this assertion ever starts passing as an equality, someone has changed
    // the model into one that adds — which is a different, wrong model.
    expect(bridge.sumOfRates).not.toBeCloseTo(bridge.rate, 4);
    expect(Math.abs(bridge.sumOfRates - bridge.rate)).toBeGreaterThan(0.001);
  });

  it('names the three drivers the subtitle promises, in that order', () => {
    expect(bridge.legs.map((l) => l.key)).toEqual(['growth', 'yield', 'multiple']);
  });

  it('each leg carries its rate and its multiplier as the same fact', () => {
    for (const l of bridge.legs) expect(l.factor - 1).toBeCloseTo(l.rate, 12);
  });

  it('the growth and yield legs are the assumptions, unannualised — they already are per year', () => {
    expect(bridge.legs[0].rate).toBe(A.growthRate);
    expect(bridge.legs[1].rate).toBe(A.dividendYield);
  });

  it('⚠ THE MULTIPLE LEG CARRIES ITS OWN ENDPOINTS, so a label cannot name a different pair', () => {
    const m = bridge.legs[2];
    expect(m.from).toBe(CASE.forwardPE);
    expect(m.to).toBe(A.exitPE);
    // The drag is the ratio of those two spread over the years — not the ratio itself.
    expect(m.factor).toBeCloseTo(Math.pow(A.exitPE / (CASE.forwardPE as number), 1 / A.years), 12);
  });

  it('a rerating DOWN is a drag and a rerating UP is a tailwind', () => {
    expect(calculateEGM({ ...CASE, forwardPE: 30 }, A).bridge!.legs[2].rate).toBeLessThan(0);
    expect(calculateEGM({ ...CASE, forwardPE: 15 }, A).bridge!.legs[2].rate).toBeGreaterThan(0);
  });

  it('at exactly the exit multiple the multiple leg is a no-op — factor 1, rate 0', () => {
    const m = calculateEGM({ ...CASE, forwardPE: A.exitPE }, A).bridge!.legs[2];
    expect(m.factor).toBeCloseTo(1, 12);
    expect(m.rate).toBeCloseTo(0, 12);
  });

  it('⚠ NULL IN EXACTLY THE CASES expectedReturn IS NULL — they are one computation', () => {
    for (const fwd of [null, 0, -25]) {
      const x = calculateEGM({ ...CASE, forwardPE: fwd }, A);
      expect(x.expectedReturn).toBeNull();
      expect(x.bridge).toBeNull();
    }
    // ...and present whenever it is, including where the fair value is not (no EPS estimate).
    const noEps = calculateEGM({ ...CASE, epsNextFY: null }, A);
    expect(noEps.fairValue).toBeNull();
    expect(noEps.bridge).not.toBeNull();
  });

  it('a non-payer contributes a leg of exactly zero rather than no leg at all', () => {
    // ⚠ THE ROW STAYS. Dropping it would make two companies' bridges different SHAPES, and the
    // reader comparing them has to notice a missing row rather than read a 0.0%.
    const b = calculateEGM(CASE, { ...A, dividendYield: null }).bridge!;
    expect(b.legs).toHaveLength(3);
    expect(b.legs[1].rate).toBe(0);
    expect(b.legs[1].factor).toBe(1);
  });
});

/**
 * The panel's conclusion: what you pay now, what the assumptions say you sell at, and the return
 * between the two.
 *
 * ⚠⚠ THE PRICE LEG AND THE TOTAL RETURN ARE DIFFERENT NUMBERS ON A DIVIDEND PAYER, and drawing
 * them as one is the trap here. `expectedReturn` includes the dividends — cash you were paid, not
 * price you can sell at — so an "implied share price" derived from it would be a figure no screen
 * will ever quote. They tie EXACTLY on a non-payer, which is most of the names this tab is opened
 * on, and that is precisely why a bug here would go unnoticed.
 */
describe('calculateEGM — implied price and whole-period return', () => {
  it('the implied price is today grown at earnings and rerated to the exit multiple', () => {
    const r = calculateEGM(CASE, A);
    const expected = (CASE.price as number)
      * Math.pow(1 + A.growthRate, A.years) * (A.exitPE / (CASE.forwardPE as number));
    expect(r.impliedPrice as number).toBeCloseTo(expected, 8);
    expect(r.priceReturn as number).toBeCloseTo(expected / (CASE.price as number) - 1, 10);
  });

  it('⚠⚠ ON A NON-PAYER THE TWO RETURNS ARE IDENTICAL — nothing separates them', () => {
    const r = calculateEGM(CASE, { ...A, dividendYield: 0 });
    expect(r.priceReturn as number).toBeCloseTo(r.totalReturn as number, 10);
    // ...and the implied price is then exactly the compounded expected return.
    expect(r.impliedPrice as number).toBeCloseTo(
      (CASE.price as number) * Math.pow(1 + (r.expectedReturn as number), A.years), 6);
  });

  it('⚠⚠ ON A PAYER THEY SEPARATE, AND THE TOTAL IS THE BIGGER ONE', () => {
    const r = calculateEGM(CASE, { ...A, dividendYield: 0.03 });
    expect(r.totalReturn as number).toBeGreaterThan(r.priceReturn as number);
    // The gap IS the dividend compounding, exactly.
    expect((1 + (r.totalReturn as number)) / (1 + (r.priceReturn as number)))
      .toBeCloseTo(Math.pow(1.03, A.years), 8);
  });

  it('the whole-period return is the annualised one compounded, not a second computation', () => {
    const r = calculateEGM(CASE, A);
    expect(r.totalReturn as number)
      .toBeCloseTo(Math.pow(1 + (r.expectedReturn as number), A.years) - 1, 10);
  });

  it('⚠ THE MEASURED PANEL CASE — a 78.5x name rerating to 20x loses a third of its price', () => {
    // The company the layout was designed against: forward P/E 78.5, no dividend, 10% growth.
    const r = calculateEGM({ price: 331.83, forwardPE: 78.5, epsNextFY: 4.23 },
      { ...A, dividendYield: 0 });
    expect(r.expectedReturn as number).toBeCloseTo(-0.0406, 4);
    expect(r.impliedPrice as number).toBeCloseTo(219.28, 1);
    expect(r.priceReturn as number).toBeCloseTo(-0.3392, 4);
  });

  it('no price means no implied price — but the RETURN still stands', () => {
    // ⚠ They are independently nullable: the return is a ratio of multiples and a growth rate,
    // none of which needs a price. Blanking it because one input is missing hides an answer we have.
    const r = calculateEGM({ ...CASE, price: null }, A);
    expect(r.impliedPrice).toBeNull();
    expect(r.priceReturn).not.toBeNull();
    expect(r.totalReturn).not.toBeNull();
  });

  it('a loss-maker has neither — there is no multiple to rerate from', () => {
    const r = calculateEGM({ ...CASE, forwardPE: -25 }, A);
    expect(r.impliedPrice).toBeNull();
    expect(r.priceReturn).toBeNull();
    expect(r.totalReturn).toBeNull();
  });
});

describe('calculateEGM — the price CAGR', () => {
  it('is the whole-period price return, annualised', () => {
    const r = calculateEGM(CASE, A);
    expect(Math.pow(1 + (r.priceCagr as number), A.years) - 1)
      .toBeCloseTo(r.priceReturn as number, 10);
  });

  it('⚠⚠ IS NOT `expectedReturn` ON A PAYER — that one is the TOTAL per year', () => {
    // The two are both honestly called "the CAGR"; which is right depends on the row it sits in.
    // Beside two PRICES, only the price leg can be derived from what is on screen.
    const r = calculateEGM(CASE, { ...A, dividendYield: 0.03 });
    expect(r.priceCagr as number).toBeLessThan(r.expectedReturn as number);
    // The gap is exactly the yield, by construction.
    expect((1 + (r.expectedReturn as number)) / (1 + (r.priceCagr as number)))
      .toBeCloseTo(1.03, 10);
  });

  it('...and IS `expectedReturn` on a non-payer — which is why the difference hides', () => {
    const r = calculateEGM(CASE, { ...A, dividendYield: 0 });
    expect(r.priceCagr as number).toBeCloseTo(r.expectedReturn as number, 12);
  });

  it('⚠ THE MEASURED PANEL CASE — 78.5x → 20x at 10% growth compounds the price at −4.1%/yr', () => {
    const r = calculateEGM({ price: 331.83, forwardPE: 78.5, epsNextFY: 4.23 },
      { ...A, dividendYield: 0 });
    expect(r.priceCagr as number).toBeCloseTo(-0.0406, 4);
  });

  it('exists wherever the whole-period figure does, and is null wherever it is not', () => {
    // ⚠ It is derived from the same factor, so "one present and the other absent" is a state that
    // must not be reachable — the panel prints them in one row.
    for (const fwd of [null, 0, -25]) {
      const x = calculateEGM({ ...CASE, forwardPE: fwd }, A);
      expect(x.priceCagr).toBeNull();
      expect(x.priceReturn).toBeNull();
    }
    // No price is fine: the RATE never needed one.
    const noPrice = calculateEGM({ ...CASE, price: null }, A);
    expect(noPrice.impliedPrice).toBeNull();
    expect(noPrice.priceCagr).not.toBeNull();
  });
});
