/**
 * ONE MEMBER'S GROWTH OVER ONE INTERVAL — the client twin of
 * `backend/routers/_fundamental_blend.step_growth`.
 *
 * ⚠ THE DRILL-DOWN'S "Weighted (= the line)" ROW IS SUPPOSED TO **BE** THE PLOTTED LINE. It is
 * recomputed here rather than read back from the server, so every rule the server's chain applies
 * has to exist here too — a table that explains a number the chart does not show is worse than no
 * table, because it is checked once and believed thereafter.
 *
 * ⚠ A MODULE, NOT A CLOSURE INSIDE THE MODAL, so the rule can be unit-tested against the same
 * measured cases the backend test pins. See `stepGrowth.test.ts`.
 *
 * ⚠⚠⚠ THE TWO MAGNITUDE HEURISTICS WERE REMOVED ON 2026-09-04, ON REQUEST — `MIN_STEP_BASE_FRACTION`
 * (refuse a step whose anchor was under 10% of that member's own median) and `MAX_STEP_GROWTH`
 * (refuse a step over 100x). With them went `memberScale` and `baseBarScale`, which existed only to
 * feed the first. What is left is arithmetic: a ratio needs a positive divisor, and an index cannot
 * carry a term below −1. The measurements behind both constants are preserved in the backend twin's
 * comment rather than deleted — if a corrupt figure ever has to be caught again, start there.
 *
 * ⚠⚠ AND REMOVING THEM ENDED A CLASS OF TWIN DIVERGENCE, WHICH IS HALF THE REASON IT WAS WORTH
 * DOING. The bar was a function of a member's OWN median over the periods it contributes, and the
 * two sides never had the same period set: the server's `at` carries a carried `2026` and an `LTM`
 * that `portfolio-revenue-matrix` does not ship. So `Graphs` and `Tables` disagreed on precisely
 * the members sitting near their own bar — ACWI FCF/share read 18.85% against 18.90%, traced to one
 * member (Industrivärden, whose 1.087 → 16.18 recovery one side counted and the other refused), and
 * ACWI EPS 16.81% against 16.82% before that. With no bar there is no scale, and nothing left to
 * disagree about.
 */

/**
 * This member's growth from `prev` to `now`, or null when it has none to give.
 *
 * Two refusals and a floor — see the backend twin for the reasoning behind each:
 *  * no anchor / no value → it sits out THIS step and joins at the next;
 *  * a non-positive anchor → there is no ratio to a zero or a negative;
 *  * floored at −100%, because below zero there is no scale. An index is a product of (1 + g), so
 *    a term under −1 does not make it small, it makes it NEGATIVE — and a negative index is not a
 *    low reading, it is not an index. That floor is what guarantees the line cannot cross zero,
 *    which is the only reason a log axis can be trusted to be showing all of it.
 */
export function stepGrowth(
  prev: number | null | undefined, now: number | null | undefined,
): number | null {
  if (prev == null || now == null || !(prev > 0)) return null;
  return Math.max(now / prev - 1, -1);
}
