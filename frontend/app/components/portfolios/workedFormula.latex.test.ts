/**
 * Every expression the builders emit must PARSE AS LaTeX — in strict mode.
 *
 * ⚠⚠ THE APP DELIBERATELY DOES NOT USE STRICT MODE, WHICH IS WHY THIS FILE EXISTS. `lib/formula`
 * renders with `throwOnError: false` so a bad expression degrades to red text in one tooltip
 * instead of blanking the panel around it. That is right at runtime and useless as a check: nothing
 * tells anybody. This is the thing that tells somebody.
 *
 * ⚠⚠ AND PARSING IS NOT ENOUGH, BECAUSE THE WORST FAILURE IS VALID LaTeX. An unescaped `%` starts a
 * COMMENT, so `overlap 20.54% + active 79.46% = 100%` renders as `overlap 20.54` — measured, not
 * supposed. KaTeX logs a `commentAtEnd` warning to the console under its default strictness and
 * paints nothing to say the rest is gone: on screen it is a shorter formula that looks finished.
 * Every figure in these expressions carries a percent sign, so this is the common path rather than
 * an edge case. Hence the tail assertions below.
 */
import { describe, expect, it } from 'vitest';
import katex from 'katex';
import {
  subPct, withWorked, workedBand, workedCagr, workedMean, workedRatio,
} from './workedFormula';
import { oneSigmaBand } from './activeBand';

/** Render exactly as `lib/formula` does, but refusing anything KaTeX would only warn about. */
function render(tex: string): string {
  return katex.renderToString(tex, { displayMode: true, throwOnError: true, strict: 'error' });
}

/**
 * Render with the APP's OWN OPTIONS — `throwOnError: false` and default (warn) strictness.
 *
 * ⚠ THE DIFFERENCE BETWEEN THIS AND `render` IS THE WHOLE POINT OF THE NEGATIVE TEST BELOW. Strict
 * mode throws on a bare `%`; the app does not, and instead drops the rest of the line. So the
 * silent-truncation failure can only be demonstrated through this one.
 */
const renderLikeApp = (tex: string) =>
  katex.renderToString(tex, { displayMode: true, throwOnError: false, trust: false });

/**
 * The text a READER would see — the `katex-html` half only.
 *
 * ⚠⚠ STRIPPING TAGS FROM THE WHOLE OUTPUT DOES NOT WORK, and getting that wrong made this test
 * pass on the broken input. KaTeX emits TWO trees: `katex-mathml` (for screen readers, whose
 * `<annotation>` contains the ORIGINAL TeX source verbatim) and `katex-html` (what is painted). So
 * a naive strip finds the truncated tail in the annotation and concludes nothing was lost.
 */
const shown = (tex: string) => {
  const html = renderLikeApp(tex);
  const visible = html.slice(html.indexOf('katex-html'));
  return visible.replace(/<[^>]*>/g, '');
};

const CAGR = { pct: 19.7, from: '2015', to: '2025', years: 10,
  fromValue: 100, toValue: 606.34 } as const;

/** A weekly book ~3pp/yr ahead of its index at a 12.41% TE — the shape of the live figures. */
const BAND = oneSigmaBand(0.06, 52, 12.41)!;

describe('every builder emits parseable LaTeX', () => {
  it('workedMean', () => {
    expect(() => render(workedMean([55.4, 54.1, 53.3, 56.6, 57.5]))).not.toThrow();
    expect(() => render(workedMean([2, 4], '×'))).not.toThrow();
    expect(() => render(workedMean([0.11, 0.09, 0.14]))).not.toThrow();
  });

  it('workedCagr', () => {
    expect(() => render(workedCagr(CAGR))).not.toThrow();
  });

  it('workedRatio, including formatted money', () => {
    expect(() => render(workedRatio(12.34, 220.5, '5.6%'))).not.toThrow();
    // ⚠ `€` IS NOT A CHARACTER KaTeX KNOWS — not in maths mode and not inside `\text{}` either
    // (verified against 0.18.4). The price-target tile hands exactly this in, so `tex` maps it to
    // an upright ISO code. Without that it renders as a fallback glyph under the app's default
    // strictness, which looks deliberate.
    expect(() => render(workedRatio(12.34, 5.6, '€220', '', '%'))).not.toThrow();
    expect(() => render(workedRatio(12.34, 5.6, '£220', '', '%'))).not.toThrow();
  });

  it('workedBand, including the negative lower end', () => {
    expect(() => render(workedBand(BAND))).not.toThrow();
    // ⚠ A NEGATIVE LOWER END IS THE COMMON CASE, and its minus sits beside a `\pm` and inside a
    // `\left[ \right]` pair — the one place a stray sign breaks the delimiters rather than the
    // spacing, which is a red block rather than a slightly-off one.
    expect(() => render(workedBand(oneSigmaBand(-0.05, 52, 8)))).not.toThrow();
    expect(workedBand(null)).toBe('');
  });

  it('withWorked joins two halves into one display', () => {
    const tex = withWorked(String.raw`\tfrac{1}{2}\sum_i \left| w_i^{\,p} - w_i^{\,b} \right|`,
      String.raw`\text{overlap } 20.54\% + \text{active } 79.46\% = 100\%`);
    expect(() => render(tex)).not.toThrow();
  });
});

describe('an unescaped percent would truncate the expression', () => {
  it('⚠ and the band keeps BOTH ends of its interval', () => {
    // ⚠⚠ FOUR PERCENT SIGNS ON ONE LINE MAKES THIS THE MOST EXPOSED BUILDER OF THE SET, and the
    // end that would vanish is the LOWER one — the end a reader actually needs. `ā f ± TE = +3.12`
    // with the interval silently gone still reads as a finished formula.
    const seen = shown(workedBand(BAND));
    expect(seen).toContain('9.29');
    expect(seen).toContain('15.53');
  });

  it('the escaped form keeps its tail', () => {
    // The real string the Active share card builds.
    const good = String.raw`\text{overlap } 20.54\% + \text{active } 79.46\% = 100\%`;
    expect(shown(good)).toContain('100');
  });

  it('⚠ and the UNESCAPED form parses fine while losing everything after the first %', () => {
    // ⚠⚠ THIS IS THE NEGATIVE TEST, and it is the reason the file exists. Under the APP's options
    // it does NOT throw — it silently drops the rest of the line. A suite that only checked "does
    // it parse" would pass on a card showing `overlap 20.54` and call it green.
    //
    // ⚠ IT DOES throw under `strict: 'error'`, which is precisely why the positive tests above use
    // that mode: strictness is the thing that turns this failure from invisible into loud.
    const bad = String.raw`\text{overlap } 20.54% + \text{active } 79.46\% = 100\%`;
    expect(() => renderLikeApp(bad)).not.toThrow();
    expect(shown(bad)).not.toContain('100');
    expect(() => render(bad)).toThrow();
  });

  it('every builder escapes it', () => {
    expect(workedMean([55.4, 54.1])).toContain(String.raw`\%`);
    expect(workedCagr(CAGR)).toContain(String.raw`\%`);
    expect(workedRatio(1, 2, subPct(5), '', '%')).toContain(String.raw`\%`);
    // ⚠ AND NONE OF THEM LEAVES A BARE ONE. `\%` contains `%`, so a `toContain` check alone would
    // pass on `\% ... %`; this asserts there is no percent that is not preceded by a backslash.
    expect(workedBand(BAND)).toContain(String.raw`\%`);
    for (const tex of [workedMean([55.4, 54.1]), workedCagr(CAGR), workedBand(BAND),
      workedRatio(1, 2, subPct(5), '', '%')]) {
      expect(tex).not.toMatch(/(^|[^\\])%/);
    }
  });
});
