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
  if (a >= 1000) return 0;
  if (a >= 10) return 1;
  if (a >= 1) return 2;
  return 3;
};

/** A number as it goes into a worked formula. `digits` forces a whole list to one precision. */
export const subNum = (v: number, digits?: number) => v.toFixed(digits ?? subDigits(v));

/** A signed percentage, as a RESULT — one decimal, because nothing divides by it. */
export const subPct = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`;

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
export function withWorked(formula: string, worked: string, tail = ''): string {
  return [formula, worked, tail].filter(Boolean).join('\n\n');
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
  return `(${subNum(got.toValue)} [${got.to}] ÷ ${subNum(got.fromValue)} [${got.from}])`
    + ` ^ (1 ÷ ${got.years}) − 1 = ${subPct(got.pct)}`;
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
  return `(${vals.map((v) => subNum(v, d)).join(' + ')}) ÷ ${vals.length}`
    + ` = ${subNum(mean, d)}${unit}`;
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
  return `${subNum(a, ratioDigits(a))}${aUnit} ÷ ${subNum(b, ratioDigits(b))}${bUnit} = ${result}`;
}
