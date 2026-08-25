/**
 * ⚠⚠ EVERY TEST HERE REDOES THE PRINTED ARITHMETIC AND CHECKS IT LANDS ON THE PRINTED ANSWER.
 * That is the only claim these strings make and the only one worth asserting: a reader with a
 * calculator must get the number the tile shows. Pinning the exact wording instead would lock the
 * copy and prove nothing.
 *
 * This module now feeds roughly twenty tooltips across the Analyse modal, the Long Equity cards,
 * Tables and Quick Valuation, so a rounding regression here is not one wrong tooltip — it is every
 * worked formula in the app disagreeing with the figure above it at once.
 */
import { describe, expect, it } from 'vitest';
import {
  subDigits, subNum, subPct, withWorked, workedBand, workedCagr, workedMean, workedRatio,
} from './workedFormula';
import { oneSigmaBand } from './activeBand';

describe('withWorked', () => {
  it('separates the halves with a blank line', () => {
    expect(withWorked('a ÷ b', '1 ÷ 2 = 50%')).toBe('a ÷ b\n\n1 ÷ 2 = 50%');
    expect(withWorked('a ÷ b', '1 ÷ 2 = 50%', 'note')).toBe('a ÷ b\n\n1 ÷ 2 = 50%\n\nnote');
  });

  it('collapses rather than leaving an empty paragraph', () => {
    // ⚠ THE COMMON PATH ON A THIN SERIES, not an edge case — every builder returns '' when an
    // operand is missing. A blank gap mid-tooltip reads as a rendering bug.
    expect(withWorked('a ÷ b', '')).toBe('a ÷ b');
    expect(withWorked('a ÷ b', '', 'note')).toBe('a ÷ b\n\nnote');
    expect(withWorked('a ÷ b', '1 ÷ 2 = 50%', '')).toBe('a ÷ b\n\n1 ÷ 2 = 50%');
  });
});

describe('subDigits', () => {
  it('gives an operand the digits it needs to be divided into', () => {
    // ⚠ THE REGRESSION THIS EXISTS FOR: at a flat one decimal the interest-coverage tooltip read
    // `100 ÷ 1.2% = 84.7×`, and 100 ÷ 1.2 is 83.3.
    expect(100 / Number(subNum(1.18))).toBeCloseTo(84.7, 1);
    // ⚠ TWO IS THE FLOOR (2026-08-22), because every figure on the risk views now prints two
    // decimals — a worked line rendering the same quantity as `55.4` beside a tile reading `55.40`
    // invites the reader to check whether they are the same number. More precision on an operand
    // is always the safe direction.
    expect(subDigits(55.4)).toBe(2);
    expect(subDigits(1.18)).toBe(2);
    // ⚠ EXCEPT AT OR ABOVE 1000, where the value already reads as an integer and `1234567.00` is
    // noise that costs the digits that matter.
    expect(subDigits(1200)).toBe(0);
    // ⚠ AND MORE THAN TWO BELOW 1 — the rule that predates this and still governs: a number under
    // one is almost always about to be a denominator.
    expect(subDigits(0.108)).toBe(3);
  });

  it('reads a negative by its magnitude', () => {
    expect(subDigits(-0.5)).toBe(subDigits(0.5));
    expect(subNum(-1.234)).toBe('-1.23');
  });

  it('lets a caller force one precision across a list', () => {
    expect(subNum(1.18, 1)).toBe('1.2');
    expect(subNum(55.4, 3)).toBe('55.400');
  });
});

describe('subPct', () => {
  it('always carries a sign, so a positive cannot read as an absolute level', () => {
    expect(subPct(19.74)).toBe('+19.7%');
    expect(subPct(-3.21)).toBe('-3.2%');
    expect(subPct(0)).toBe('+0.0%');
  });
});

describe('workedMean', () => {
  it('prints addends that average to the printed mean', () => {
    const vals = [55.4, 54.1, 53.3, 56.6, 57.5];
    const m = /^\(([^)]*)\) ÷ (\d+) = (-?[\d.]+)%$/.exec(workedMean(vals))!;
    expect(m).not.toBeNull();
    const addends = m[1].split(' + ').map(Number);
    expect(addends).toEqual(vals);
    expect(Number(m[2])).toBe(vals.length);
    // ⚠ THE READER REDOES IT AT WHATEVER PRECISION IS PRINTED, derived rather than hard-coded —
    // the point is that the arithmetic on screen reconciles, not that it uses any given number of
    // decimals. Hard-coding one is what made this test fail when the floor moved to two.
    const dp = (m[3].split('.')[1] ?? '').length;
    const reader = addends.reduce((a, b) => a + b, 0) / addends.length;
    expect(Number(reader.toFixed(dp))).toBe(Number(m[3]));
  });

  it('uses one precision for the whole list', () => {
    // ⚠ REGRESSION: `vals.map(subNum)` hands `Array.map`'s INDEX to the digits parameter, so the
    // first addend prints at 0 decimals, the second at 1, and the list stops adding up.
    const texts = /^\(([^)]*)\)/.exec(workedMean([1.11, 2.22, 3.33, 4.44]))![1].split(' + ');
    expect(new Set(texts.map((t) => t.split('.')[1]?.length ?? 0)).size).toBe(1);
  });

  it('takes its precision from the mean, so a near-zero list survives', () => {
    const out = workedMean([0.11, 0.09, 0.14, 0.08, 0.12]);
    expect(out).toContain('0.110');
    expect(out.endsWith('= 0.108%')).toBe(true);
  });

  it('accepts a different unit, and none at all', () => {
    expect(workedMean([2, 4], '×')).toBe('(2.00 + 4.00) ÷ 2 = 3.00×');
    expect(workedMean([2, 4], '')).toBe('(2.00 + 4.00) ÷ 2 = 3.00');
  });

  it('refuses an empty list rather than printing a division by zero', () => {
    expect(workedMean([])).toBe('');
  });
});

describe('workedCagr', () => {
  const rate = (fromValue: number, toValue: number, years: number) =>
    ({ pct: ((toValue / fromValue) ** (1 / years) - 1) * 100,
      from: '2015', to: '2025', years, fromValue, toValue });

  it('prints endpoints that compound to the printed rate', () => {
    const got = rate(100, 606.34, 10);
    const m = /^\((-?[\d.]+) \[(\w+)\] ÷ (-?[\d.]+) \[(\w+)\]\) \^ \(1 ÷ (\d+)\) − 1 = ([+-][\d.]+)%$/
      .exec(workedCagr(got))!;
    expect(m).not.toBeNull();
    // ⚠ THE LATER PERIOD IS THE NUMERATOR. Read upside down a CAGR is a plausible, wrong,
    // opposite-signed answer — the bracketed labels are what make that checkable.
    expect([m[2], m[4]]).toEqual(['2025', '2015']);
    const reader = ((Number(m[1]) / Number(m[3])) ** (1 / Number(m[5])) - 1) * 100;
    expect(Math.abs(reader - Number(m[6]))).toBeLessThan(0.1);
  });

  it('keeps enough digits on a small base to reconcile', () => {
    const got = { ...rate(2.4913, 8.137, 5), from: '2020' };
    const nums = [...workedCagr(got).matchAll(/(-?[\d.]+) \[/g)].map((x) => Number(x[1]));
    expect(Math.abs(((nums[0] / nums[1]) ** (1 / 5) - 1) * 100 - got.pct)).toBeLessThan(0.1);
  });

  it('refuses what it cannot honestly show', () => {
    expect(workedCagr({ pct: null, reason: 'no line' })).toBe('');
    // A non-positive base: every producer refuses one, but `(606 ÷ -3) ^ …` printed beside a
    // positive rate would be a worked example of something impossible.
    expect(workedCagr({ pct: 10, from: '2015', to: '2025', years: 10,
      fromValue: -3, toValue: 606 })).toBe('');
    expect(workedCagr({ pct: 10, from: '2015', to: '2025', years: 10,
      fromValue: 0, toValue: 606 })).toBe('');
  });
});

describe('workedRatio', () => {
  it('writes one division out with the callers own formatting of the answer', () => {
    expect(workedRatio(12.34, 5.6, '+220.4%')).toBe('12.34 ÷ 5.60 = +220.4%');
    expect(workedRatio(12.34, 5.6, '€220', '', '%')).toBe('12.34 ÷ 5.60% = €220');
  });

  it('gives both operands enough digits to reproduce the answer', () => {
    // ⚠ THE REGRESSION: at plain `subDigits` this printed `12.3 ÷ 5.60`, and a reader dividing
    // those gets 219.6 against a price target of €220 — near enough to look right and to be
    // wrong. Both sides of a lone division are about to be divided, so both get two decimals.
    expect(workedRatio(12.34, 220.5, '5.6%')).toBe('12.34 ÷ 220.50 = 5.6%');
    // ⚠ EXCEPT ABOVE 1000, where two decimals print `1234567.00` and buy nothing.
    expect(workedRatio(1234567, 1000, 'x')).toBe('1234567 ÷ 1000 = x');
  });

  it('does not recompute the answer', () => {
    // ⚠ THE RESULT IS PASSED IN so it can never disagree with the tile it explains. Asserted
    // because the tempting "improvement" is to compute a / b here, which is a second
    // implementation whose only job is to match the first.
    expect(workedRatio(1, 2, 'whatever the tile says')).toBe('1.00 ÷ 2.00 = whatever the tile says');
  });

  it('refuses a missing or zero denominator', () => {
    expect(workedRatio(null, 5, 'x')).toBe('');
    expect(workedRatio(5, null, 'x')).toBe('');
    expect(workedRatio(5, 0, 'x')).toBe('');
    expect(workedRatio(undefined, undefined, 'x')).toBe('');
  });
});

describe('workedBand', () => {
  const band = oneSigmaBand(0.06, 52, 12.41)!;

  it('prints the interval it claims — the ends really are centre ∓ TE', () => {
    // ⚠⚠ THE WHOLE VALUE OF THE LINE IS THAT A READER CAN REDO IT. +3.12 − 12.41 = −9.29 and
    // +3.12 + 12.41 = +15.53; a rounding convention that made either end miss by 0.01 would hand
    // the reader a second reason to distrust the tile, which is the failure this file exists for.
    const tex = workedBand(band);
    expect(tex).toContain('+3.12');
    expect(tex).toContain('12.41');
    expect(tex).toContain('-9.29');
    expect(tex).toContain('+15.53');
  });

  it('signs both ends but never the tracking error itself', () => {
    // ⚠ A `± +12.41%` reads as two signs on a standard deviation, which cannot be negative — and
    // the tile above prints it unsigned. The interval's ends DO carry theirs: the band is not
    // symmetric about zero, and that is a claim about signs.
    expect(workedBand(band)).toContain(String.raw`\pm 12.41\%`);
    expect(workedBand(band)).not.toContain(String.raw`\pm +12.41`);
  });

  it('keeps the centre out in front, because it is what the reader is missing', () => {
    // ⚠ ā·f COMES FIRST rather than being left to be inferred from the interval. The point of the
    // line is that the band is NOT centred on the benchmark; burying the centre inside two
    // endpoints would leave the reader to subtract it back out.
    expect(workedBand(band)).toMatch(/^\bar\{a\}/);
  });

  it('refuses rather than printing half a band', () => {
    expect(workedBand(null)).toBe('');
    expect(workedBand(undefined)).toBe('');
    expect(workedBand(oneSigmaBand(0.06, 52, 0))).toBe('');
  });
});
