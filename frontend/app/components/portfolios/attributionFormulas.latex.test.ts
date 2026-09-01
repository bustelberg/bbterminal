import { describe, expect, it } from 'vitest';
import katex from 'katex';

import {
  pctTex, workedAllocation, workedContribution, workedInteraction, workedReturn,
  workedSelection, workedTotal, workedWeight,
} from './attributionFormulas';

/**
 * Every attribution expression must PARSE AS LaTeX — in strict mode — and keep its tail.
 *
 * ⚠⚠ THE FAILURE THIS EXISTS FOR IS INVISIBLE ON SCREEN. An unescaped `%` starts a LaTeX COMMENT,
 * so `= +0.45%` renders as everything up to the first figure and STOPS — a shorter formula that
 * looks finished. The app renders with `throwOnError: false`, so nothing tells anybody. Every
 * figure on this panel is a percentage or a percentage point, which makes it the densest place in
 * the app for that hazard. Same shape as `valuationFormulas.latex.test.ts`.
 *
 * Pure — no DOM, no network.
 */

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
 * there and concludes nothing was lost.
 */
const shown = (tex: string) => {
  const html = renderLikeApp(tex);
  return html.slice(html.indexOf('katex-html')).replace(/<[^>]*>/g, '');
};

describe('pctTex makes a formatted figure safe for maths mode', () => {
  it('escapes a percent', () => {
    expect(pctTex('12.3%')).toBe('12.3\\%');
  });

  it('sets pp as text, not as two italic variables', () => {
    /** ⚠ `pp` IN MATHS MODE IS `p × p`. It renders as a product of two italics beside a number,
     *  which looks like algebra in a cell that is reporting percentage points. */
    expect(pctTex('+0.45pp')).toBe('+0.45\\,\\text{pp}');
  });

  it('is empty for an absent operand, so the builder can fall back to the rule alone', () => {
    expect(pctTex(null)).toBe('');
    expect(pctTex('')).toBe('');
  });
});

describe('every expression parses in strict mode', () => {
  it('the two column rules', () => {
    expect(() => render(workedWeight('18.4%'))).not.toThrow();
    expect(() => render(workedReturn('12.7%'))).not.toThrow();
  });

  it('a holding contribution', () => {
    expect(() => render(workedContribution('4.1%', '22.6%', '+0.93pp'))).not.toThrow();
  });

  it('the three Brinson effects and their total', () => {
    expect(() => render(workedAllocation('18.4%', '11.2%', '9.8%', '14.7%', '-0.36pp')))
      .not.toThrow();
    expect(() => render(workedSelection('11.2%', '15.3%', '9.8%', '+0.62pp'))).not.toThrow();
    expect(() => render(workedInteraction('18.4%', '11.2%', '15.3%', '9.8%', '+0.40pp')))
      .not.toThrow();
    expect(() => render(workedTotal('-0.36pp', '+0.62pp', '+0.40pp', '+0.66pp'))).not.toThrow();
  });
});

describe('the figures survive to the screen', () => {
  /**
   * ⚠⚠ THIS IS THE WHOLE POINT. Written as `= +0.45%` the line renders up to `+0.45` and the `%`
   * swallows everything after it. Asserting on the VISIBLE half is what catches that; a strict
   * parse does not, because a comment is legal LaTeX.
   */
  it('an allocation line keeps every operand and its result', () => {
    const seen = shown(workedAllocation('18.4%', '11.2%', '9.8%', '14.7%', '-0.36pp'));
    for (const part of ['18.4', '11.2', '9.8', '14.7', '0.36', 'pp']) {
      expect(seen, `${part} was swallowed`).toContain(part);
    }
    expect(seen).toContain('%');
  });

  it('and an unescaped percent really would truncate it — the failure, demonstrated', () => {
    expect(shown(String.raw`x = 18.4\% - 11.2\%`)).toContain('11.2');
    expect(shown(String.raw`x = 18.4% - 11.2%`)).not.toContain('11.2');
  });
});

describe('a missing operand yields the rule alone, never half a substitution', () => {
  it.each([
    ['weight', workedWeight(null)],
    ['return', workedReturn(null)],
    ['contribution', workedContribution('4.1%', null, '+0.9pp')],
    ['allocation', workedAllocation('18.4%', '11.2%', null, '14.7%', '-0.36pp')],
    ['selection', workedSelection(null, '15.3%', '9.8%', '+0.62pp')],
    ['interaction', workedInteraction('18.4%', '11.2%', '15.3%', null, '+0.40pp')],
    ['total', workedTotal('-0.36pp', '+0.62pp', '+0.40pp', null)],
  ])('%s still parses and states its rule', (_name, tex) => {
    expect(tex).not.toBe('');
    expect(() => render(tex)).not.toThrow();
    /** ⚠ NO `=` MEANS NO HALF-SUBSTITUTION — the rule is shown and no numbers are, rather than
     *  some numbers and a dangling operator. */
    expect(tex).not.toContain('undefined');
  });
});
