/**
 * THE BOOK-RETURN CHART'S ⓘ FORMULA, AND ITS WORKED LINE — see `BookReturnChart`.
 *
 * ⚠⚠ IN A PURE MODULE RATHER THAN IN THE JSX, BECAUSE A LaTeX STRING IS TESTABLE AND A TOOLTIP IS
 * NOT — and the failure this guards against is INVISIBLE ON SCREEN. An unescaped `%` starts a
 * LaTeX COMMENT, so `= +35.36%` renders as everything up to the first figure and STOPS: a shorter
 * formula that looks finished. The app renders with `throwOnError: false`, so nothing reports it.
 * Every percentage here goes through `\%`, and `bookReturnFormula.latex.test.ts` renders these in
 * STRICT mode and checks the visible tail — which is the only thing that can tell anybody.
 *
 * ⚠ THE WORKED LINE IS THE FORMULA AND NOTHING ELSE. No book name, no window prose: once
 * `withWorked` joins the two halves into one display, any word in it is a TERM IN AN EQUATION. The
 * dates are subscripted operands, which is what they are.
 */
import { texEscape } from './workedFormula';

/**
 * The chain, typeset.
 *
 * ⚠ WORDS INSIDE `\text{}`, NOT SYMBOLS. `R_d = Π(1+rₚ) − 1` needs a legend to say what R and r
 * are; spelled out it needs none, and this card is meant to be read at a glance.
 *
 * ⚠ IT IS A COMPOUNDING, NOT A SUM, and that is the one thing worth typesetting: AIRS chains each
 * period's return rather than adding them, so the year is not the months added up.
 */
export const CHAIN_TEX = String.raw`\text{return}(d) = \prod_{\text{periods} \le d}`
  + String.raw`\bigl(1 + \text{period return}\bigr) - 1`;

/**
 * The same statement with this book's newest point in it.
 *
 * ⚠⚠ THE THOUSANDS SEPARATORS ARE `{,}`. A bare comma in maths mode is PUNCTUATION and KaTeX sets
 * a space after it, so `1,353,619` renders as three numbers in a list.
 *
 * ⚠ THE VALUE ONLY WHERE WE HOLD ONE. AIRS publishes a return for dates we have no snapshot for,
 * and `EUR —` in an equation is not a term.
 *
 * ⚠ '' WHEN THERE IS NOTHING TO SUBSTITUTE, which `withWorked` collapses to the formula alone —
 * the rule every worked line in this app follows.
 */
export function workedReturn(
  point: { date: string; cum_pct: number | null; value_eur?: number | null } | null | undefined,
  from: string | null | undefined,
): string {
  if (!point || point.cum_pct == null || !from) return '';
  const sign = point.cum_pct >= 0 ? '+' : '';
  const worth = point.value_eur == null ? ''
    : String.raw` \quad (\text{EUR}\,${Math.round(point.value_eur).toLocaleString('en-US')
      .replace(/,/g, '{,}')})`;
  return String.raw`\text{return}(\text{${texEscape(from)}} \rightarrow `
    + String.raw`\text{${texEscape(point.date)}}) = ${sign}${point.cum_pct.toFixed(2)}\%`
    + worth;
}
