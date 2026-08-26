/**
 * Every Deep Valuation expression must PARSE AS LaTeX — in strict mode — and keep its tail.
 *
 * ⚠⚠ THE FAILURE THIS EXISTS FOR IS INVISIBLE ON SCREEN, which is why a tooltip cannot be the
 * place these live. An unescaped `%` starts a LaTeX COMMENT, so `… = +5.8%` renders as the
 * expression up to the first figure and STOPS — a shorter formula that looks finished. The app
 * renders with `throwOnError: false` and default strictness (right at runtime: one bad tooltip
 * degrades instead of blanking the panel), so nothing tells anybody. This is the thing that tells
 * somebody. Same shape as `workedFormula.latex.test.ts`, which guards the shared builders.
 *
 * ⚠ THE OPERANDS BELOW ARE THE SHAPE OF THE LIVE ONES: a real bridge with a rerating DOWN (the
 * common case — the house default exit multiple is 20x), a negatively-filed capex, and a
 * perpetuity growth low enough for the terminal leg to be finite.
 *
 * Pure — no DOM, no network.
 */
import { describe, expect, it } from 'vitest';
import katex from 'katex';

import { type EgmBridge } from './egm';
import {
  bridgeParts, workedCashFlowValued, workedEgmReturn, workedForwardFcf, workedGrowthCapex,
  workedImpliedGrowth, workedImpliedPrice, workedMarketCap, workedPriceMove,
} from './valuationFormulas';

/** Render as `lib/formula` does, but refusing anything KaTeX would only WARN about. */
const render = (tex: string) =>
  katex.renderToString(tex, { displayMode: true, throwOnError: true, strict: 'error' });

/** Render with the APP's options, so a silent truncation can be demonstrated rather than thrown. */
const renderLikeApp = (tex: string) =>
  katex.renderToString(tex, { displayMode: true, throwOnError: false, trust: false });

/**
 * The text a READER would see — the `katex-html` half only.
 *
 * ⚠ STRIPPING TAGS FROM THE WHOLE OUTPUT DOES NOT WORK: KaTeX also emits a `katex-mathml` tree
 * whose `<annotation>` holds the ORIGINAL TeX verbatim, so a naive strip finds the truncated tail
 * there and concludes nothing was lost. Same trap `workedFormula.latex.test.ts` documents.
 */
const shown = (tex: string) => {
  const html = renderLikeApp(tex);
  return html.slice(html.indexOf('katex-html')).replace(/<[^>]*>/g, '');
};

/** 10% growth, a 0.30% yield, rerating 30.50x → 20.00x. `factor`/`rate` as the model computes. */
const BRIDGE: EgmBridge = {
  legs: [
    { key: 'growth', rate: 0.10, factor: 1.10 },
    { key: 'yield', rate: 0.003, factor: 1.003 },
    { key: 'multiple', rate: -0.0409, factor: 0.9591, from: 30.5, to: 20 },
  ],
  factor: 1.0577,
  rate: 0.0577,
  sumOfRates: 0.0621,
};

describe('every Deep Valuation expression parses in strict mode', () => {
  it('the EGM return, implied price and price move', () => {
    expect(() => render(workedEgmReturn(BRIDGE, 10, '+5.8%'))).not.toThrow();
    expect(() => render(workedImpliedPrice(220.5, BRIDGE, 10, 375.42))).not.toThrow();
    expect(() => render(workedPriceMove(375.42, 220.5, '+70.3%'))).not.toThrow();
  });

  it('the Reverse DCF base, its two corrections and the market cap', () => {
    expect(() => render(workedForwardFcf(17000, -1631.2, 15368.8))).not.toThrow();
    expect(() => render(workedGrowthCapex(-1631.2, 1025.9, 605.3))).not.toThrow();
    expect(() => render(workedCashFlowValued(11027.3, 202.3, 605.3, 11430.3))).not.toThrow();
    expect(() => render(workedMarketCap(220.5, 1000, 220500))).not.toThrow();
  });

  it('the reverse DCF equation itself', () => {
    expect(() => render(workedImpliedGrowth({
      fcf: 11430.3, rate: 0.098, perpetuityGrowth: 0.03, years: 10,
      target: 220500, growth: 0.243,
    }))).not.toThrow();
  });

  it('⚠⚠ and it states the exponent `modelValue` actually uses, not the naive one', () => {
    // `modelValue` pays the BASE cash flow in year 1 and grows from year 2: its closed form
    // `F(1 − x^n)/(r − g)` expands to `Σ F(1+g)^{t-1}/(1+r)^t`, and its `g = r` limit case
    // (`F·n/(1+r)`, every term equal) only comes out right on that reading. Written with `t` the
    // formula overstates the explicit leg by a whole year of growth — a tooltip that cannot be
    // reconciled with the number beside it, in the one place whose job is to let a reader redo
    // the arithmetic. Shipped that way 2026-08-26 and caught by reading `modelValue`, not by
    // anything that runs; this is the thing that would have run.
    const tex = workedImpliedGrowth({
      fcf: 11430.3, rate: 0.098, perpetuityGrowth: 0.03, years: 10,
      target: 220500, growth: 0.243,
    });
    expect(tex).toContain('(1+g)^{t-1}');
    expect(tex).toContain('(1+g)^{n-1}');
    expect(tex).not.toContain('(1+g)^{t}');
  });
});

describe('the percent signs survive', () => {
  /**
   * ⚠⚠ THE TAIL IS THE ASSERTION, NOT THE PARSE. Every one of these lines ends in the answer, so a
   * `%` eaten as a comment takes exactly the figure the reader opened the tooltip for.
   */
  it('the EGM return keeps its answer', () => {
    expect(shown(workedEgmReturn(BRIDGE, 10, '+5.8%'))).toContain('5.8');
  });

  it('the implied growth keeps every operand AND the solved rate', () => {
    // ⚠ FOUR PERCENTAGES ON ONE LINE — the discount rate, the perpetuity rate and the answer, with
    // the answer LAST. This is the most exposed expression of the set.
    const seen = shown(workedImpliedGrowth({
      fcf: 11430.3, rate: 0.098, perpetuityGrowth: 0.03, years: 10,
      target: 220500, growth: 0.243,
    }));
    expect(seen).toContain('9.8');     // r
    expect(seen).toContain('3.0');     // g∞
    expect(seen).toContain('24.3');    // the solved g — the whole point of the panel
  });

  it('no builder leaves a bare percent', () => {
    for (const tex of [
      workedEgmReturn(BRIDGE, 10, '+5.8%'),
      workedPriceMove(375.42, 220.5, '+70.3%'),
      workedImpliedGrowth({
        fcf: 11430.3, rate: 0.098, perpetuityGrowth: 0.03, years: 10,
        target: 220500, growth: 0.243,
      }),
    ]) {
      // `\%` contains `%`, so a `toContain` check alone would pass on `\% … %`.
      expect(tex).not.toMatch(/(^|[^\\])%/);
    }
  });
});

describe('a missing operand collapses the line rather than guessing', () => {
  /**
   * ⚠ `AspectCard` DROPS AN EMPTY `worked`, so '' is the shape that leaves the card looking as it
   * did before any of this existed. A builder that returned a half-formula instead would put a
   * `n/a` or an `undefined` inside a typeset expression, which reads as a rendering fault.
   */
  it('returns empty everywhere an input is absent', () => {
    expect(workedForwardFcf(null, -1631.2, 15368.8)).toBe('');
    expect(workedForwardFcf(17000, null, 15368.8)).toBe('');
    expect(workedGrowthCapex(-1631.2, null, 605.3)).toBe('');
    expect(workedCashFlowValued(null, 202.3, 605.3, 11430.3)).toBe('');
    // ⚠ AND WITH NEITHER CORRECTION AVAILABLE — `F = F` is not arithmetic worth typesetting.
    expect(workedCashFlowValued(11027.3, null, null, 11027.3)).toBe('');
    // …but ONE of them is enough, and the symbolic half then carries only that term.
    expect(workedCashFlowValued(11027.3, 202.3, null, 10825)).toContain('F - S');
    expect(workedCashFlowValued(11027.3, 202.3, null, 10825)).not.toContain('G');
    expect(workedMarketCap(220.5, null, null)).toBe('');
    expect(workedPriceMove(375.42, null, '+70.3%')).toBe('');
    expect(workedImpliedPrice(null, BRIDGE, 10, 375.42)).toBe('');
  });

  it('⚠ and refuses a non-positive cash flow — there is no growth rate that works', () => {
    expect(workedImpliedGrowth({
      fcf: -50, rate: 0.098, perpetuityGrowth: 0.03, years: 10, target: 220500, growth: 0.243,
    })).toBe('');
  });

  it('⚠ and a bridge with no usable forward P/E, which is the live absence', () => {
    // A company GuruFocus publishes no forward multiple for: the rerating leg has nothing to start
    // from, so the return and the implied price both have no worked line — not a zero rerating.
    const noPE: EgmBridge = {
      ...BRIDGE,
      legs: [BRIDGE.legs[0], BRIDGE.legs[1], { key: 'multiple', rate: 0, factor: 1 }],
    };
    expect(workedEgmReturn(noPE, 10, '+5.8%')).toBe('');
    expect(workedImpliedPrice(220.5, noPE, 10, 375.42)).toBe('');
  });
});

describe('the operands are the bridge\'s own', () => {
  /**
   * ⚠⚠ THE ONE THING A WORKED EXAMPLE MUST NOT DO IS QUOTE DIFFERENT NUMBERS FROM THE ARITHMETIC
   * IT PROVES. The panel holds `yieldUsed` as `number | null` and the model reads a null as 0, so
   * rebuilding the operands from the inputs would print a blank where the model multiplied by 1.
   */
  it('reads growth, yield and both multiples off the legs', () => {
    expect(bridgeParts(BRIDGE)).toEqual({ g: 0.10, y: 0.003, from: 30.5, to: 20 });
  });

  it('and the substituted line contains them, not the tile\'s own formatting', () => {
    const tex = workedEgmReturn(BRIDGE, 10, '+5.8%');
    expect(tex).toContain('30.50');
    expect(tex).toContain('20.00');
    expect(tex).toContain('0.1000');
    expect(tex).toContain('0.0030');
  });
});
