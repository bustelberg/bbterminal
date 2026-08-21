/**
 * THE SECOND HALF OF A TABLES ⓘ — a row's formula with this book's own numbers substituted in.
 *
 * ⚠⚠ SEPARATE FROM `TablesTab` SO IT CAN BE TESTED, and it is exactly the kind of thing that needs
 * to be: every string here is an ASSERTION ABOUT ARITHMETIC ("these ten numbers average to 1.18,
 * and 100 over that is 84.7"). A rendering bug elsewhere makes a page look wrong; a bug here makes
 * a page look right and lie about why. `tablesSubstitution.test.ts` re-does each printed expression
 * the way a reader would and checks it lands on the printed answer.
 *
 * ⚠ EVERY BUILDER RETURNS '' RATHER THAN GUESSING — no line, no positive base, nothing in the
 * window. The ⓘ then falls back to formula + prose, which is what it showed before any of this
 * existed. A worked example is worth having only while every number in it is real.
 *
 * ⚠ THE ARITHMETIC ITSELF LIVES IN `workedFormula`, NOT HERE. Tables is one of six surfaces that
 * print worked formulas, and the rounding rule in particular must not be local — two tooltips that
 * round the same operand differently are two tooltips a reader cannot check against each other.
 * What is left in this file is the part that IS Tables-specific: which window to work, which side
 * to work it for, and how to name the span.
 */
import { type Cagr } from './lineCagr';
import { xToPeriod } from './marginData';
import { windowMean } from './windowStats';
import { subDigits, subNum, workedCagr, workedMean } from './workedFormula';

/**
 * ⚠⚠ THE MEAN IS TAKEN FIRST AND TRANSFORMED SECOND, WHICH IS THE WHOLE REASON THIS EXISTS.
 * Interest coverage is one over the interest burden, and the burden is the quantity that averages
 * — so the cells carry burdens, `windowMean` averages those, and only the displayed number is
 * inverted. Doing it the other way round drops every debt-free year (a burden of 0 has no
 * reciprocal) and lets one high-coverage year dominate: measured on ASML, 84.2× over 9 of 10 years
 * against 84.8× over all 10, and 87.4× against 79.3× on the five-year window.
 *
 * ⚠ IT MAY RETURN NULL, and that is a real answer rather than an error: an average burden of zero
 * means nothing in the window paid any interest, so there is no coverage to state.
 */
export type MeanTransform = (mean: number) => number | null;

/**
 * THE SECOND HALF OF EVERY ⓘ: the row's formula with this book's actual numbers in it.
 *
 * ⚠⚠ THE BOOK'S LONGEST SHOWN WINDOW, NOT ALL OF THEM. Substituting every window would put four
 * expressions in one tooltip and make the reader work out which line belongs to which cell — so it
 * works one, and NAMES the periods inside the expression (`[2025]`, `2016–2025`) rather than
 * relying on the reader to infer them. Naming them is also what keeps this string language-neutral:
 * it is arithmetic and period labels, so there is no Dutch copy to keep in step with the English.
 *
 * ⚠ IT IS DERIVED FROM THE SAME `windowMean`/`lineCagr` CALL THE CELL MAKES, never re-derived
 * "the same way". A verification line that computes independently is a second implementation whose
 * whole purpose is to agree with the first — which is the one thing it can silently stop doing.
 *
 * ⚠ AND IT RETURNS '' RATHER THAN GUESSING. No line, no positive base, nothing in the window: the
 * ⓘ falls back to formula + prose, which is what it said before this existed. A worked example is
 * worth having only while every number in it is real.
 */
// ⚠ THE `Cagr` CARRIES ITS OWN OPERANDS — this used to take the `Blend` too and look the endpoints
// up again by period. It cannot any more, which removes the failure that lookup allowed: printing
// the right rate beside the wrong pair of numbers. See `Cagr.fromValue`.
export function rateSub(name: string, got: Cagr | null): string {
  if (!got || got.pct == null) return '';
  const expr = workedCagr(got);
  // ⚠ THE BOOK'S NAME IS THE CAPTION, and it is what makes the line readable without one. The
  // ⓘ belongs to a row that has a book column and an index column; an unlabelled expression
  // under it is a worked example of an unstated one of the two.
  return expr ? `${name}\n${expr}` : '';
}

/**
 * The same, for a row that averages a per-year series — see {@link rateSub}.
 *
 * ⚠⚠ THE LISTED NUMBERS ARE THE PER-YEAR WEIGHTED FIGURES, WHICH IS ONE HALF OF THE FORMULA AND
 * SAYS SO. `Σ(w × ratio) ÷ Σw` happens per year across twenty-odd holdings and cannot be written
 * out in a tooltip; the mean OF those years can, and it is the step a reader actually doubts
 * ("is 84.8× really the average of these ten?"). The per-holding half is what the drill-down behind
 * the row label exists for, and the two are the same computation seen at two depths.
 *
 * ⚠ THE TRANSFORM IS SHOWN AS ITS OWN LINE, because that is where its whole argument lives: the
 * reader sees the burdens averaged and only THEN inverted, which is exactly the ordering the
 * `(9/10)` badge used to hide. See `coverageFromBurden`.
 */
export function meanSub(
  name: string, m: Map<number, number | null>, endX: number | null, years: number,
  transform?: MeanTransform,
): string {
  if (endX == null) return '';
  const got = windowMean(m, endX, years);
  if (got.mean == null) return '';
  const vals: number[] = [];
  // ⚠ `fromX`/`toX` ARE THE FIRST AND LAST YEARS THAT HAVE A VALUE, not the window's edges — so
  // this walk collects exactly the `got.n` figures the mean was taken over, and a short window
  // lists what it actually had rather than padding it out.
  for (let x = got.fromX; x <= got.toX; x += 1) {
    const v = m.get(x);
    if (v != null) vals.push(v);
  }
  if (!vals.length) return '';
  const mean = workedMean(vals);
  const head = `${name}, ${xToPeriod(got.fromX)}–${xToPeriod(got.toX)}`;
  if (!transform) return `${head}\n${mean}`;
  const t = transform(got.mean);
  // ⚠ THE INVERSION IS ITS OWN LINE, because that is where its whole argument lives: the reader
  // sees the burdens averaged and only THEN inverted, which is exactly the ordering the `(9/10)`
  // badge used to hide. ⚠ And it re-prints the mean at the SAME precision `workedMean` chose, so
  // the operand on this line is visibly the answer on the line above it.
  return t == null ? `${head}\n${mean}`
    : `${head}\n${mean}\n100 ÷ ${subNum(got.mean, subDigits(got.mean))}% = ${t.toFixed(1)}×`;
}
