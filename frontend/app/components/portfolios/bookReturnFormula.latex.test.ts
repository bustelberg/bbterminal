/**
 * The book-return chart's ⓘ expressions must PARSE AS LaTeX — in strict mode — and keep their tail.
 *
 * ⚠⚠ THE FAILURE THIS EXISTS FOR IS INVISIBLE ON SCREEN, which is why these live in a module and
 * not in the tooltip. An unescaped `%` starts a LaTeX COMMENT, so `… = +35.36%` renders as
 * everything up to the first figure and STOPS — a shorter formula that looks finished. The app
 * renders with `throwOnError: false`, so nothing tells anybody. This is the thing that tells
 * somebody. Same shape as `valuationFormulas.latex.test.ts` and `tablesCopy.latex.test.ts`.
 *
 * ⚠ EVERY FIGURE ON THIS LINE IS A PERCENTAGE OR A EUR SUM, so the visible-tail check is the half
 * that matters: a strict parse alone passes on a line that was truncated at the `%`.
 *
 * Pure — no DOM, no network.
 */
import { describe, expect, it } from 'vitest';
import katex from 'katex';

import { CHAIN_TEX, workedReturn } from './bookReturnFormula';

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

/** A real point: AITopSelectie OFF DYN on 2026-08-26, measured. */
const POINT = { date: '2026-08-26', cum_pct: 35.3619, value_eur: 1_353_619.25 };

describe('the book-return expressions parse in strict mode', () => {
  it('the chain and its worked line', () => {
    expect(() => render(CHAIN_TEX)).not.toThrow();
    expect(() => render(workedReturn(POINT, '2026-01-01'))).not.toThrow();
  });

  it('a negative return, which carries a minus as well as a percent', () => {
    const tex = workedReturn({ date: '2026-08-26', cum_pct: -3.5, value_eur: 971_787.43 },
      '2026-01-01');
    expect(() => render(tex)).not.toThrow();
    // ⚠ THE DIGITS, NOT THE SIGN. KaTeX sets a maths minus as U+2212, not the ASCII hyphen this
    // file typed — asserting on `-3.50` would fail on a line that rendered perfectly.
    expect(shown(tex)).toContain('3.50');
    expect(shown(tex)).toContain('%');
  });

  it('a point we hold no value for', () => {
    const tex = workedReturn({ date: '2026-05-31', cum_pct: 12.5, value_eur: null }, '2026-01-01');
    expect(() => render(tex)).not.toThrow();
    expect(shown(tex)).not.toContain('EUR');
  });
});

describe('the percentage survives to the screen', () => {
  /**
   * ⚠⚠ THIS IS THE WHOLE POINT. Written as `= +35.36%` the line renders up to `+35.36` and the
   * `%` swallows the rest — including the EUR figure after it. Asserting on the VISIBLE half is
   * what catches that; a strict parse does not, because a comment is legal LaTeX.
   */
  it('the percent sign and everything after it are still there', () => {
    const seen = shown(workedReturn(POINT, '2026-01-01'));
    expect(seen).toContain('35.36');
    expect(seen).toContain('%');
    expect(seen).toContain('EUR');
    expect(seen).toContain('1,353,619');
  });

  it('and an unescaped percent really would truncate it — the failure, demonstrated', () => {
    expect(shown(String.raw`x = +35.36\% \quad \text{EUR}`)).toContain('EUR');
    expect(shown(String.raw`x = +35.36% \quad \text{EUR}`)).not.toContain('EUR');
  });
});

describe('it declines to write a formula it has no operands for', () => {
  /** ⚠ `''`, which `withWorked` collapses to the formula alone — the rule every worked line in
   *  this app follows. A half-substituted equation is worse than none. */
  it('no anchor, no point, or no return', () => {
    expect(workedReturn(POINT, null)).toBe('');
    expect(workedReturn(null, '2026-01-01')).toBe('');
    expect(workedReturn({ date: '2026-08-26', cum_pct: null }, '2026-01-01')).toBe('');
  });
});
