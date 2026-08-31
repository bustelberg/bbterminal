/**
 * Every expression the Tables tab typesets must PARSE AS LaTeX — in strict mode.
 *
 * ⚠⚠ THIS FILE EXISTS BECAUSE THE TAB SHIPPED THE OPPOSITE FAILURE. `workedCagr` and `workedMean`
 * became LaTeX on 2026-08-22, and Tables kept handing their output to `InfoTip text=`, which
 * renders a string as prose — so the tooltips printed `\left(\dfrac{606.30_{\,2025}}…` at the
 * reader, backslashes and braces, for nine days. Reported 2026-08-31 as "the latex in the info
 * icons in Tables is not rendering properly". The ⓘ now typesets it, and the moment it does, every
 * one of these strings has to be valid.
 *
 * ⚠⚠ AND PARSING IS NOT ENOUGH — THE WORST FAILURE IS VALID LaTeX. An unescaped `%` starts a
 * COMMENT: the rest of the line vanishes and KaTeX paints a shorter formula that looks finished.
 * The app renders with `throwOnError: false`, so nothing reports it. Hence STRICT mode here, and
 * the visible-text assertion below — the same protection `workedFormula.latex.test.ts` gives the
 * builders these strings are joined to.
 */
import { describe, expect, it } from 'vitest';
import katex from 'katex';

import { LANGS, type Lang } from '../../../lib/i18n';
import { COPY, MEASURE_KEYS } from './tablesCopy';
import { meanSub, rateSub } from './tablesSubstitution';
import { withWorked } from './workedFormula';

/** Exactly what `lib/formula` renders, but refusing anything KaTeX would only warn about. */
const render = (tex: string) =>
  katex.renderToString(tex, { displayMode: true, throwOnError: true, strict: 'error' });

/** The text a READER sees — the `katex-html` half only.
 *  ⚠ NOT the whole output: `katex-mathml` carries the ORIGINAL source verbatim, so stripping tags
 *  from all of it finds a truncated tail in the annotation and concludes nothing was lost. */
const shown = (tex: string) => {
  const html = katex.renderToString(tex, { displayMode: true, throwOnError: false });
  return html.slice(html.indexOf('katex-html')).replace(/<[^>]*>/g, '');
};

const CAGR = { pct: 19.7, from: '2015', to: '2025', years: 10, fromValue: 100, toValue: 606.34 };
const SERIES = new Map([[2021, 55.4], [2022, 54.1], [2023, 53.3], [2024, 56.6], [2025, 57.5]]);

describe('every row formula renders', () => {
  it.each(LANGS)('%s', (lang: Lang) => {
    const c = COPY[lang];
    for (const k of MEASURE_KEYS) {
      for (const sbc of [false, true]) {
        expect(() => render(c.rowFormula[k](sbc)), `rowFormula.${k}(${sbc})`).not.toThrow();
      }
    }
  });
});

describe('the worked line renders, joined to the formula the way the card joins it', () => {
  it.each(LANGS)('%s rate rows', (lang: Lang) => {
    const c = COPY[lang];
    const tex = withWorked(c.rowFormula.revCagr(false), rateSub(CAGR));
    expect(() => render(tex)).not.toThrow();
    // ⚠ FORMULA THEN NUMBERS, AND NOTHING BETWEEN THEM (2026-08-31). The book's name used to head
    // the worked line and landed in the middle of one display expression, reading as a term in it.
    expect(shown(tex)).toContain('606.34');
    expect(shown(tex)).not.toContain('Offensief');
  });

  it.each(LANGS)('%s mean rows, including the inversion line', (lang: Lang) => {
    const c = COPY[lang];
    const cover = (m: number) => (m > 0 ? 100 / m : null);
    const tex = withWorked(c.rowFormula.intCover(false),
      meanSub(SERIES, 2025, 5, cover));
    expect(() => render(tex)).not.toThrow();
    // ⚠⚠ THE TAIL IS THE ASSERTION, not the parse. Every one of these lines carries a `%`, and an
    // unescaped one would comment out everything after it while still rendering — so the check is
    // that the LAST thing in the expression is still on screen.
    expect(shown(tex)).toContain('55.38');           // the mean of the five printed years
    expect(shown(tex)).toContain('1.8');             // …and 100 over it, the line AFTER the `%`
  });

  it('a percentage inside the line does not comment out the rest of it', () => {
    // ⚠⚠ THE SILENT FAILURE. An unescaped `%` starts a LaTeX comment: the rest of the line
    // vanishes and KaTeX paints a shorter formula that looks finished.
    const tex = withWorked(COPY.en.rowFormula.fcfMargin(true), meanSub(SERIES, 2025, 5));
    expect(() => render(tex)).not.toThrow();
    // The mean sits AFTER the `%`-carrying addends, so its presence proves nothing was commented
    // out — every figure in these lines is a percentage, which is why this is the common path.
    expect(shown(tex)).toContain('55.38');
  });
});
