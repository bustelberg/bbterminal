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
 */

/**
 * ⚠⚠ HOW BIG A MEMBER'S STARTING FIGURE MUST BE, RELATIVE TO ITS OWN TYPICAL SIZE, FOR THE RATIO
 * OFF IT TO BE A GROWTH RATE AT ALL. Keep in lock-step with the backend's
 * `_MIN_STEP_BASE_FRACTION`, whose comment carries the measurements.
 *
 * The short version: the chain's only guard used to be `prev > 0`, which catches zero and misses
 * the case that bites — a base that is positive and NEAR zero. Prosus's first positive FCF/share is
 * 0.0090 a share against a 0.1485 median, so its next (negative) figure divides out to −2,700%
 * growth at a 26% index weight, the AEX FCF/share index went to −1,456, and a LOG axis then dropped
 * every point after the crossing without a word (6 of 10 drawn annually, 26 of 32 quarterly).
 *
 * 0.10 is read off the distribution, not picked: base ÷ median|value| puts the two pathological
 * anchors at 0.0078 (AMD) and 0.0606 (Prosus), the next-lowest legitimate ones at 0.150 (Adyen,
 * a real 6.7x growth story that must survive) and 0.184, and the bulk at 0.21–1.0.
 */
export const MIN_STEP_BASE_FRACTION = 0.10;

/**
 * A member's own typical magnitude — the median |value| over the periods it contributes.
 *
 * ⚠ MEDIAN, NOT MEAN: the thing being measured against is an outlier, and a mean is moved by the
 * very outlier it exists to identify.
 *
 * ⚠ SAFE ON THE REBASED VALUES. `_prepare` scales a level member by a per-member constant, which
 * divides out of `prev ÷ scale` — so this needs neither the raw series nor a currency.
 */
export function memberScale(values: number[]): number {
  const xs = values.filter((v) => Number.isFinite(v)).map(Math.abs).sort((a, b) => a - b);
  if (!xs.length) return 0;
  const mid = xs.length >> 1;
  return xs.length % 2 ? xs[mid] : (xs[mid - 1] + xs[mid]) / 2;
}

/**
 * This member's growth from `prev` to `now`, or null when it has none to give.
 *
 * Three refusals and a floor — see the backend twin for the reasoning behind each:
 *  * no anchor / no value → it sits out THIS step and joins at the next;
 *  * a non-positive anchor → there is no ratio to a zero or a negative;
 *  * an immaterial anchor → see `MIN_STEP_BASE_FRACTION`;
 *  * floored at −100%, because below zero there is no scale. An index is a product of (1 + g), so
 *    a term under −1 does not make it small, it makes it NEGATIVE — and a negative index is not a
 *    low reading, it is not an index. That floor is what guarantees the line cannot cross zero,
 *    which is the only reason a log axis can be trusted to be showing all of it.
 */
export function stepGrowth(
  prev: number | null | undefined, now: number | null | undefined, scale: number,
): number | null {
  if (prev == null || now == null || !(prev > 0)) return null;
  if (prev < MIN_STEP_BASE_FRACTION * scale) return null;
  return Math.max(now / prev - 1, -1);
}
