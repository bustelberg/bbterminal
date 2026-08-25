/**
 * GROWTH FOR A SERIES THAT CROSSES ZERO — the skip rule.
 *
 * A ratio needs a positive base. Free cash flow, and every per-share line built on it, crosses zero
 * routinely: Eli Lilly ran `5.085 → −3.489 → 0.458 → 6.632`. Measured one period at a time against
 * whatever sits behind it, that series produces a −100%, a refusal, and then a +1,348% — and the
 * company's real three-year growth of +30.4% appears nowhere.
 *
 * ⚠⚠ THE RULE IS: WALK BACK TO THE LAST USABLE BASE, THEN ANNUALISE OVER THE SPAN. Skipping the
 * unusable periods and keeping the CLOCK is what makes it honest — the growth really did take three
 * years, and reporting it as a one-year figure would be the same overstatement in a new place. For
 * Lilly the answer becomes `5.085 → 6.632` over 3 years, **+9.2%/yr**, which is the truth.
 *
 * ⚠⚠ "USABLE" IS NOT "POSITIVE". Two rules, and the second is the one that catches the real cases.
 * A base must be positive AND MATERIAL against the series' own typical size. Lilly's 0.458 is
 * positive and still poison — it is 9% of its own median, and dividing by it turns a recovery into
 * +1,348%. Japan Post Bank's 4.998 against a median of 1,623 is worse. Skipping only negatives
 * fixes neither. The floor is a tenth of the median |value|, the same constant and the same
 * reasoning as `_MIN_STEP_BASE_FRACTION` on the server.
 *
 * ⚠ IT TELESCOPES, WHICH IS WHY IT CAN BE TRUSTED IN A COLUMN. Chaining the retained steps equals
 * the endpoint ratio exactly, because each step's base is the previous step's end. Nothing is
 * double counted and nothing is lost — only the unusable bases are stepped over.
 *
 * ⚠ AND IT IS DELIBERATELY NOT `asinh`. A signed-log transform also handles zero crossings, but it
 * needs a scale parameter θ that swings the answer three-fold on its own recommended range, it is
 * not scale-invariant across companies, and its output is not a rate: `V₀(1+r)ⁿ ≠ Vₙ`. This rule
 * keeps a real ratio, needs no parameter, and survives a change of units.
 *
 * ⚠ THIS IS A PER-COMPANY RULE AND MUST NOT BE LIFTED TO AN INDEX. An index averages rates weighted
 * by cap, so a company holding 1% of the cash flow with a +1,348% rate moves the average 13.5
 * points regardless of how few euros are behind it — measured at ~40x too much on Lilly alone. The
 * fix there is to sum euros, not to mend the ratio: `scripts/acwi_fcf_growth.py` does, and gets
 * ACWI FCF at +7.56%/yr against the growth chain's +19.1%. ⚠ THE INDEX DEFECT IS NOT ABOUT
 * NEGATIVES AT ALL, which is why no per-company rule can reach it — REVENUE is never negative, so
 * this module is a no-op on it, and cap-weighted rate-averaging still reads ~9.95%/yr against the
 * euro sum's +4.60%. The whole gap is the weight.
 */

/** The series' own typical size. ⚠ MEDIAN, NOT MEAN — the thing being measured against is an
 *  outlier, and a mean is moved by the very outlier it exists to identify. Mirrors
 *  `_fundamental_blend.member_scale`. */
export function medianAbs(values: readonly (number | null | undefined)[]): number {
  const abs = values.filter((v): v is number => v != null && Number.isFinite(v))
    .map((v) => Math.abs(v)).sort((a, b) => a - b);
  if (!abs.length) return 0;
  const mid = abs.length >> 1;
  return abs.length % 2 ? abs[mid] : (abs[mid - 1] + abs[mid]) / 2;
}

/** A base below this share of the series' median |value| is refused. See the ⚠⚠ above. */
export const MIN_BASE_FRACTION = 0.10;

export type Step = {
  /** The period the base was taken from. */
  from: string;
  base: number;
  /** How many REPORTED periods the step spans. 1 is an ordinary year-on-year. */
  span: number;
  /** Total growth over the span, as a fraction. `0.304` = +30.4%. */
  growth: number;
  /**
   * `(1 + growth)^(1/span) − 1` — what the column should print, so a three-year step never renders
   * in the same ink as everyone else's one-year one.
   *
   * ⚠⚠ NULL WHEN THERE IS NO SUCH RATE, and that is a fact about arithmetic rather than a gap in
   * the data. A rate asserts `V₀(1+r)ⁿ = Vₙ`; with `Vₙ < 0` and `n > 1` no real `r` satisfies it —
   * an even root of a negative is not real and an odd one solves the wrong equation. This is the
   * company that turned FCF-negative and never came back: the skip rule can bridge a dip BETWEEN
   * two positive years, and there is nothing on the far side of a trailing negative run to bridge
   * to. ⚠ Over ONE period it is defined and is not null — `10 → −5` is exactly −150%, no root
   * taken — so the refusal is narrow and only ever covers the case that has no answer.
   */
  annualised: number | null;
  /** How many periods were stepped over to find a usable base. `0` on the ordinary path. */
  skipped: number;
};

/**
 * The growth to show at `periods[i]`, or null when there is no usable base behind it.
 *
 * `periods` must be the row's own REPORTED periods in order — not the table's columns. A company
 * that skipped a year would otherwise be measured against a period it never filed.
 *
 * ⚠ NULL MEANS "NO USABLE BASE", AND THE CALLER MUST SAY SO. Rendered as an empty cell it is
 * indistinguishable from "no data", which is how a refusal became invisible in the first place.
 */
export function usableStep(
  valueAt: (period: string) => number | null | undefined,
  periods: readonly string[],
  i: number,
  scale: number,
): Step | null {
  if (i <= 0 || i >= periods.length) return null;
  const now = valueAt(periods[i]);
  if (now == null || !Number.isFinite(now)) return null;

  const floor = MIN_BASE_FRACTION * scale;
  for (let j = i - 1; j >= 0; j -= 1) {
    const base = valueAt(periods[j]);
    // ⚠ BOTH TESTS, IN THIS ORDER. `> 0` alone lets 0.458 through; the floor alone would divide by
    // a large negative. A series whose scale is 0 (every value zero) has no usable base at all,
    // and `base > 0` refuses it before the floor can be trivially satisfied.
    if (base == null || !Number.isFinite(base) || base <= 0 || base < floor) continue;
    const span = i - j;
    const growth = now / base - 1;
    return {
      from: periods[j],
      base,
      span,
      growth,
      // ⚠ SEE THE `annualised` NOTE ON `Step`. One period needs no root, so it is the growth
      // itself; a multi-period step ending at or below zero has no real rate and says so.
      annualised: span === 1 ? growth
        : 1 + growth <= 0 ? null
          : (1 + growth) ** (1 / span) - 1,
      skipped: span - 1,
    };
  }
  return null;
}

/**
 * The first period of the unbroken non-positive run ending at `periods[i]`, or null if the value
 * there is positive.
 *
 * ⚠ THE CELL THAT HAS NO RATE MUST STILL SAY SOMETHING, and "when did this turn" is the fact a
 * reader actually wants. A company that went FCF-negative in 2023 and has not recovered produces
 * no growth figure at any later period (see `Step.annualised`), and a column of blanks reads as
 * missing data — the one reading that is certainly wrong, because the data is present and it is
 * the arithmetic that refuses.
 */
export function negativeRunStart(
  valueAt: (period: string) => number | null | undefined,
  periods: readonly string[],
  i: number,
): string | null {
  const now = valueAt(periods[i]);
  if (now == null || !Number.isFinite(now) || now > 0) return null;
  let j = i;
  while (j > 0) {
    const prev = valueAt(periods[j - 1]);
    // ⚠ A GAP ENDS THE RUN. An unreported period is not a negative one, and walking through it
    // would date the turn to before a year we have no figure for.
    if (prev == null || !Number.isFinite(prev) || prev > 0) break;
    j -= 1;
  }
  return periods[j];
}
