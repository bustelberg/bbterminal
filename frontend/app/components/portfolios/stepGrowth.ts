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
 * ⚠⚠ THE OTHER END OF THE SAME QUESTION, AND IT WAS NEVER ASKED. `MIN_STEP_BASE_FRACTION` refuses a
 * DIVISOR too small to divide by; nothing refused a RESULT too large to believe. A vendor scale
 * error — a per-share figure delivered in the wrong unit — went through as growth, and the chain
 * multiplies it by the member's weight with no bound. Keep in lock-step with the backend's
 * `_MAX_STEP_GROWTH`, whose comment carries the measurements. The short version, on ACWI's annual
 * FCF/share (26,160 accepted steps, 1,712 constituents):
 *
 *     MITSUBISHI HEAVY  2024→2025      50.78 →  86,214.52   +169,684%   moves the index +116.12pp
 *     DENSO CORP        2024→2025     172.97 → 108,415.57    +62,580%   moves the index  +17.97pp
 *
 * One corrupt cell in a 0.07%-weight constituent more than doubled a line indexed to 100. 100x in a
 * year is read off the distribution: p99.99 is +6,889% and the largest unambiguously REAL step is
 * Bank of America's +3,818%, while the corrupt band starts at +10,097%.
 */
export const MAX_STEP_GROWTH = 100;

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
 * The materiality bar for ONE member — `memberScale`, or **0 (no bar) on a ONE-MEMBER line**, where
 * the member IS the line. The client twin of `_fundamental_blend.base_bar_scale`.
 *
 * ⚠⚠ THE BAR IS A RULE ABOUT ONE MEMBER INSIDE AN AVERAGE OF MANY, AND ON A LINE OF ONE IT HAS
 * NOTHING TO PROTECT. `MIN_STEP_BASE_FRACTION` refuses a member's step and lets the others carry
 * the interval — the refusal is an ABSTENTION. With a single contributor there is nobody to
 * abstain in favour of: `den` comes out 0, `buildBlend` hits its "nothing spans this interval"
 * `continue`, and — because that path deliberately does NOT advance `anchor` — the SAME base is
 * offered at every later period and refused every time. One refusal deletes the whole line.
 *
 * ⚠⚠ AND THE FIRST PERIOD OF A HYPERGROWER ALWAYS TRIPS IT. Every member is rebased to 100 at its
 * own first positive period, so the bar reads `100 < 0.10 × median|rebased|` — it fires on ANY
 * member that grew more than ~10x from its first period to its median one, which is growth and not
 * a corrupt divisor. Measured 2026-09-03 on NVIDIA through `portfolio-revenue-matrix` as a
 * one-holding book: `price_ps` 13 periods, median rebased 2,706, bar 271 → **1 period drawn**;
 * `eps_nri` 18 periods, bar 227 → **1**; `fcf_ps` 13 periods, bar 49 → **13**. A one-point line has
 * no window, so the `Tables` tab's Share price and EPS rows read `—` while the Graphs tab, which
 * for one company plots the filed figures with no chain at all, drew all thirteen.
 *
 * ⚠ IT DOES NOT LOOSEN THE BAR FOR AN INDEX OR A BOOK. `members > 1` is the whole condition, so
 * every case the constant was read off (Prosus at a 26% AEX weight, AMD, Mitsubishi Heavy) is
 * untouched — those are the lines where an abstention has somewhere to fall back to.
 *
 * ⚠ 0 IS ALREADY THE "NO BAR" VALUE (`memberScale([])` returns it), so this adds a reason, not a
 * mechanism. ⚠ Read it with a `??` default, never `||` — `0 || memberScale(v)` puts the bar back.
 */
export function baseBarScale(values: number[], members: number): number {
  return members > 1 ? memberScale(values) : 0;
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
  const growth = now / prev - 1;
  // ⚠ AN IMPLAUSIBLE RESULT — see `MAX_STEP_GROWTH`. Refused, never capped: capping would invent a
  // growth rate nobody reported, where refusing simply means the member sits out this one interval
  // and rejoins at the next, exactly as the refusals above it behave.
  if (growth > MAX_STEP_GROWTH) return null;
  return Math.max(growth, -1);
}
