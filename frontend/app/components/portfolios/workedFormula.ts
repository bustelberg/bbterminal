/**
 * ONE VOCABULARY FOR "THE SAME FORMULA, WITH THIS BOOK'S NUMBERS IN IT".
 *
 * Every ⓘ that states a formula owes the reader a second line: the same expression with the actual
 * operands substituted, so the arithmetic can be redone rather than believed. This module holds the
 * primitives — how many decimals an operand needs, and how a division / a mean / a compound rate is
 * written out — so that every surface writes them the same way.
 *
 * ⚠⚠ IT EXISTS BECAUSE THE ALTERNATIVE IS FORTY SLIGHTLY DIFFERENT CONVENTIONS. The Analyse modal,
 * the Long Equity cards, Tables, Quick Valuation and Attribution all print worked formulas; written
 * one at a time they drift on rounding (one place decides 1.2, another 1.18), on operand order, and
 * on whether the answer is repeated at the end. A reader who has learned to check one of them
 * should not have to relearn the next.
 *
 * ⚠ AND THE ROUNDING IS THE PART THAT MUST NOT BE LOCAL. An operand needs more digits than a
 * result: `100 ÷ 1.2% = 84.7×` is what a fixed one-decimal rule produced, and 100 ÷ 1.2 is 83.3, so
 * the worked line contradicted the very figure it existed to justify. See {@link subDigits}.
 *
 * ⚠ NOTHING HERE GUESSES. Every builder returns '' when an operand is missing or unusable, and the
 * caller falls back to the symbolic formula alone — which is what these tooltips said before any of
 * this existed. A worked example is worth having only while every number in it is real.
 */
import { type Cagr } from './lineCagr';
import { type ActiveBand } from './activeBand';

/**
 * A literal that must survive being read as LaTeX — `%` starts a comment, `_` and `^` are scripts.
 *
 * ⚠⚠ `%` IS THE ONE THAT BITES, AND IT FAILS SILENTLY. An unescaped `20.54%` makes KaTeX treat
 * the rest of the LINE as a comment, so the expression renders truncated at the first percentage
 * with no error anywhere — `overlap 20.54` and nothing after it. Everything numeric here carries a
 * percent sign, so this is not an edge case, it is the common path.
 */
const tex = (s: string) => s
  // ⚠⚠ ESCAPE FIRST, SUBSTITUTE SECOND — AND THE ORDER IS THE WHOLE OF IT (fixed 2026-08-25).
  // This pass used to run LAST, so it escaped the braces of the `\text{EUR}` the euro rule below
  // had just inserted: `\text\{EUR\}\,220`. That PARSES — no throw, even in strict mode, which is
  // why the LaTeX suite was green — and renders as a literal `{EUR}` with the letters italicised
  // as a product of three variables. Wrong on screen, silent everywhere else.
  //
  // ⚠⚠ `%` STARTS A COMMENT, and that is the escape that matters most because the failure is
  // INVISIBLE ON SCREEN. `\text{overlap } 20.54% + …` renders as `overlap 20.54` and stops:
  // measured, not supposed. KaTeX logs a `commentAtEnd` warning under its default strictness and
  // paints nothing to say the rest is gone, so the reader gets a shorter formula that looks
  // finished. Everything numeric here carries a percent sign, so this is the common path. Pinned
  // by `workedFormula.latex.test.ts`, which renders in STRICT mode so it throws instead.
  //
  // ⚠ IT ESCAPES THE CALLER'S SPECIALS, NOT OURS. Anything this function ADDS below is LaTeX we
  // wrote deliberately and must survive intact.
  .replace(/([%$&#_{}])/g, '\\$1')
  // ⚠⚠ `€` IS NOT A CHARACTER KaTeX KNOWS, in maths mode OR in `\text{}` — verified against
  // 0.18.4 with `strict: 'error'`. Callers hand formatted money in (`€220` from the price-target
  // tile), so this is not hypothetical. Under the app's default strictness it would render as a
  // warning and a fallback glyph rather than throwing, which is the worst outcome: it looks
  // deliberate. The ISO code set upright is unambiguous and typesets cleanly.
  .replace(/€/g, String.raw`\text{EUR}\,`)
  .replace(/£/g, String.raw`\pounds `);

/** Prose inside an expression — upright, spaced, and not italicised as a product of variables. */
const words = (s: string) => `\\text{${tex(s)}}`;

/**
 * How many decimals an OPERAND needs to survive being divided into.
 *
 * ⚠⚠ AN OPERAND NEEDS MORE DIGITS THAN A RESULT, AND GETTING THAT WRONG DEFEATS THE WHOLE FEATURE.
 * The first version printed everything at one decimal, which read `100 ÷ 1.2% = 84.7×` — a reader
 * who does that division gets 83.3 and now has a *second* reason to distrust the cell. The mean
 * really is 1.18%, and 100 ÷ 1.18 is 84.7; one decimal threw away the digit the arithmetic needed.
 *
 * So precision scales with magnitude rather than being fixed: 55.4% is exact enough at one decimal
 * because nothing downstream divides by it, while a number under 1 is almost always about to be a
 * denominator. Cheap to widen, and the cost of being wrong is a tooltip that contradicts itself.
 */
export const subDigits = (v: number): number => {
  const a = Math.abs(v);
  // ⚠⚠ TWO IS THE FLOOR, NOT THE CEILING (2026-08-22). Every figure on the risk views now prints
  // two decimals, and a worked line that renders the same quantity as `55.4` beside a tile reading
  // `55.40` invites the reader to check whether they are the same number. More precision on an
  // operand is always safe — it is the direction that keeps the arithmetic reconciling.
  //
  // ⚠ EXCEPT AT OR ABOVE 1000, where the figure already reads as an integer and `1234567.00` is
  // noise that costs the digits that matter.
  if (a >= 1000) return 0;
  return a >= 1 ? 2 : 3;
};

/** A number as it goes into a worked formula. `digits` forces a whole list to one precision. */
export const subNum = (v: number, digits?: number) => v.toFixed(digits ?? subDigits(v));

/** ⚠ ESCAPE ANY LITERAL THAT GOES INTO A FORMULA. See `tex` — an unescaped `%` truncates the
 *  expression at that point, silently. Exported because several call sites interpolate their own
 *  numbers and labels rather than going through a builder here. */
export const texEscape = tex;
/** Prose inside an expression, upright rather than italic. */
export const texWords = words;

/** A signed percentage, as a RESULT — one decimal, because nothing divides by it. */
export const subPct = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`;

/**
 * The same thing at TWO decimals — for a figure that appears in a card's PROSE and in its worked
 * line at once.
 *
 * ⚠ NOT A SECOND OPINION ABOUT PRECISION, A SECOND SITUATION. `subPct` is a terminal result nobody
 * checks against anything; this is for a number the reader is invited to find twice in one tooltip,
 * and the risk views all print two decimals (see `subDigits`' own ⚠⚠). Rounding the sentence at one
 * decimal and the formula at two puts `+3.1%` and `+3.12%` four lines apart in the same card, which
 * is precisely the "are these the same number?" doubt the worked lines exist to remove.
 */
export const subPct2 = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;

/**
 * THE SHAPE ITSELF: symbols, a blank line, the same expression with numbers, then any caveats.
 *
 * ⚠⚠ THE BLANK LINE IS THE WHOLE POINT, AND SO IS WHAT HAPPENS WHEN THERE ARE NO NUMBERS. The two
 * halves answer different doubts — "what was computed?" and "does that arithmetic give this?" — and
 * prose answers only the first, which is why a reader who disbelieves a figure can read a perfectly
 * clear definition and still disbelieve it.
 *
 * ⚠ AN EMPTY `worked` COLLAPSES RATHER THAN LEAVING A GAP. Every builder here returns '' when an
 * operand is missing, so this is the common path on a thin series — and a tooltip with a blank
 * paragraph in the middle of it reads as a rendering bug, which is the opposite of reassuring.
 * Same for an empty `tail`.
 */
export function withWorked(formula: string, worked: string): string {
  // ⚠ `\\\\` IS A LINE BREAK IN LaTeX, not a paragraph in text. The old shape joined on `\n\n`
  // and the card split it back apart — which stopped working the moment either half became LaTeX,
  // because a backslash means something in both languages and neither knows about the other.
  //
  // ⚠ AND `tail` IS GONE FROM THE SIGNATURE TOO (2026-09-01). It was prose, and prose does not
  // belong inside a typeset expression, so the two callers that used it moved to the card's `how`
  // — but the PARAMETER stayed, ending in `+ (tail ? '' : '')`, which appends '' either way. A
  // parameter that is accepted, documented and discarded is worse than none: the next caller
  // passes something and watches it vanish. `tsc` confirms nobody was passing one.
  const lines = [formula, worked].filter(Boolean);
  if (lines.length < 2) return lines.join('');
  // ⚠⚠ `\\begin{gathered}` BECAUSE A BARE `\\\\` DOES NOTHING IN DISPLAY MODE. That is LaTeX's own
  // rule, and KaTeX says so on every render: "In LaTeX, \\\\ or \\newline does nothing in display
  // mode [newLineInDisplayMode]". It reached the browser console twice per card and had been there
  // since this helper was written.
  //
  // ⚠⚠ AND `strict: 'error'` DOES NOT CATCH IT, WHICH IS WHY THE LaTeX TESTS WERE GREEN THROUGHOUT.
  // Every `*.latex.test.ts` here renders with `throwOnError: true, strict: 'error'` and asserts it
  // does not throw — and this is a WARNING, not a strict violation, so all of them passed while
  // every worked line in the app warned. `workedFormula.latex.test.ts` now captures `console.warn`
  // instead of only catching throws.
  //
  // ⚠ `gathered`, NOT `aligned`. `aligned` wants an `&` anchor per line and none of the builders
  // emit one, so it would set both halves flush-left against an invisible column; `gathered`
  // centres each line, which is what a formula above its own substitution should do.
  return `\\begin{gathered} ${lines.join(' \\\\[4pt] ')} \\end{gathered}`;
}

/**
 * `(606.3 [2025] ÷ 100.0 [2015]) ^ (1 ÷ 10) − 1 = +19.7%`
 *
 * ⚠ THE ENDPOINTS CARRY THEIR PERIODS IN BRACKETS, which is what lets this line stand on its own.
 * Without them a reader has two bare numbers and has to trust that the later one is the numerator —
 * and a CAGR read upside down is a plausible, wrong, opposite-signed answer.
 *
 * ⚠ IT TAKES THE `Cagr` AND NOTHING ELSE. The operands ride on the result (see `Cagr.fromValue`),
 * so there is no way to hand this the right rate and the wrong pair of numbers.
 */
export function workedCagr(got: Cagr): string {
  if (got.pct === null) return '';
  // ⚠ A NON-POSITIVE BASE CANNOT REACH HERE — every producer refuses one — but printing
  // `(606 ÷ -3) ^ …` beside a positive rate would be a worked example of something impossible,
  // so the guard stays rather than resting on an invariant three functions away.
  if (!(got.fromValue > 0)) return '';
  // ⚠ THE PERIODS RIDE AS SUBSCRIPTS on the values they belong to, which is what the bracketed
  // `[2025]` was standing in for. A subscript cannot be mistaken for another operand.
  return `\\left(\\dfrac{${subNum(got.toValue)}_{\\,${tex(got.to)}}}`
    + `{${subNum(got.fromValue)}_{\\,${tex(got.from)}}}\\right)^{1/${got.years}} - 1`
    + ` = ${tex(subPct(got.pct))}`;
}

/**
 * `(55.4 + 54.1 + 53.3) ÷ 3 = 54.3%`
 *
 * ⚠ ONE PRECISION FOR THE WHOLE LIST, TAKEN FROM THE MEAN — a column of addends at mixed decimals
 * reads as noise, and it is the mean's magnitude that decides whether a digit matters (it is the
 * number that will get divided into). ⚠ Written as an arrow function rather than `vals.map(subNum)`,
 * which would hand `Array.map`'s INDEX to the `digits` parameter and print the first addend at zero
 * decimals — silently, and only visibly wrong to a reader who added them up.
 */
export function workedMean(vals: readonly number[], unit = '%'): string {
  if (!vals.length) return '';
  const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
  const d = subDigits(mean);
  // ⚠ `\dfrac`, NOT `a ÷ b`. A displayed fraction is the whole reason to typeset this at all: the
  // addends sit over their own count instead of trailing off to the right of a division sign.
  return `\\dfrac{${vals.map((v) => subNum(v, d)).join(' + ')}}{${vals.length}}`
    + ` = ${subNum(mean, d)}${tex(unit)}`;
}

/**
 * `12.34 ÷ 220.50 = 5.6%` — one division, written out.
 *
 * ⚠ THE RESULT IS PASSED IN RATHER THAN COMPUTED, so this can never disagree with the tile it
 * explains. Recomputing `a / b` here would be a second implementation whose only job is to match
 * the first, which is exactly the thing that silently stops matching.
 *
 * ⚠⚠ ITS OPERANDS CARRY MORE DIGITS THAN {@link subDigits} ALONE WOULD GIVE THEM, and that is the
 * same rule one step further on. Both sides of a lone division are about to be divided, and both
 * feed a result the tile has already printed: at plain `subDigits`, a per-share figure of 12.34
 * rendered as `12.3` and a demanded yield of 5.6% as `5.60`, so the reader's own division came out
 * at 219.6 against a price target of €220. Not wrong enough to look wrong — which is worse.
 *
 * At least two decimals below 1000 (money and per-share amounts are read that way anyway), and
 * `subDigits` alone at or above it, where two would print `1234567.00` and buy nothing.
 */
const ratioDigits = (v: number): number => {
  const d = subDigits(v);
  return d === 0 ? 0 : Math.max(2, d);
};

export function workedRatio(a: number | null | undefined, b: number | null | undefined,
  result: string, aUnit = '', bUnit = ''): string {
  if (a == null || b == null || b === 0) return '';
  return `\\dfrac{${subNum(a, ratioDigits(a))}${tex(aUnit)}}`
    + `{${subNum(b, ratioDigits(b))}${tex(bUnit)}} = ${tex(result)}`;
}

/**
 * `ā f ± TE = +3.12% ± 12.41% ⟹ [−9.29%, +15.53%]`
 *
 * ⚠⚠ THE LINE EXISTS BECAUSE THE TILE ABOVE IT NAMES A SPREAD AND NOT ITS CENTRE, and every reader
 * supplies the missing centre themselves — as zero. It is ā (see `activeBand`), so the interval is
 * asymmetric about the benchmark, and writing it out is the only way that fact reaches anybody.
 *
 * ⚠ THE INTERVAL IS PRINTED AS WELL AS THE `centre ± TE` FORM, redundantly and on purpose. `+3.12%
 * ± 12.41%` still asks the reader to do two signed additions, and the one they get wrong is the
 * lower end — which is the end that matters.
 *
 * ⚠ IT TAKES THE `ActiveBand` AND NOTHING ELSE, the same rule {@link workedCagr} follows: the ends
 * ride on the object that computed them, so there is no way to hand this the right TE and a centre
 * from another cadence.
 */
export function workedBand(band: ActiveBand | null | undefined): string {
  if (!band) return '';
  // ⚠ THROUGH `tex` — every one of these carries a `%`, which starts a LaTeX comment and would
  // truncate the line at the first figure, silently. See the ⚠⚠ on `tex` itself.
  // ⚠ THE TE ITSELF CARRIES NO SIGN. It is a standard deviation — a `+12.41%` after a `±` reads as
  // a second sign on a quantity that cannot be negative, and the tile above prints it unsigned too.
  return String.raw`\bar{a}\,f \pm TE = ${tex(subPct2(band.centre))} \pm ${subNum(band.te, 2)}\%`
    + String.raw` \;\Rightarrow\; \left[\,${tex(subPct2(band.lo))},\;${tex(subPct2(band.hi))}\,\right]`;
}
