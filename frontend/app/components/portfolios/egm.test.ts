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
      expect(r).toEqual({
        maxPE: null, fairValue: null, upside: null, expectedReturn: null, peRatio: null,
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
