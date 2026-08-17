/**
 * Compound annual growth of a BLENDED LINE, over a fixed number of years.
 *
 * ⚠⚠ IT IS COMPUTED ON THE LINE, NOT ON THE UNDERLYING FIGURES, AND THAT IS WHAT MAKES IT DEFINED
 * AT ALL. `(end/start)^(1/n) − 1` needs a positive start, and FCF per share is negative for real
 * companies in real years — a growth rate out of a negative base is not a large number, it is a
 * meaningless one (and its SIGN flips, so it reads as a triumph). `buildBlend` rebases every member
 * to 100 at its own first positive period and drops the ones with a non-positive base
 * (`non_positive_base`), so `level` is positive by construction. Asking it for a CAGR is therefore
 * always answerable or honestly absent — never quietly wrong.
 *
 * ⚠ AND IT IS THE SAME SERIES THE CHART DRAWS AND THE `Rebased` FOOTER PRINTS. A second derivation
 * "the same way" from the raw cells is how a summary comes to disagree with the table under it.
 */
import { isEstimatePeriod, periodOrder } from './fundamentalBlend';

export type Cagr =
  | { pct: number; from: string; to: string; years: number }
  /** ⚠ A REASON, NOT A NULL. Every absence here has a different fix — fetch more history, wait for
   *  a filing, lower the coverage floor — and a bare dash sends the reader to guess which. */
  | { pct: null; reason: string };

/**
 * The fiscal YEAR a period label belongs to, or null when it is not a reported one.
 *
 * ⚠⚠ `LTM` AND `2026e` ARE BOTH REFUSED, FOR DIFFERENT REASONS, AND BOTH WOULD LOOK FINE.
 *
 *   * an ESTIMATE endpoint makes the answer a forecast wearing the clothes of a track record — "5y
 *     CAGR 8.4%" where a third of the span has not happened yet;
 *   * `LTM` is a real, current figure, but it ends at the newest QUARTERLY filing, so a span from
 *     FY2020 to LTM is five years and some months. Dividing by 5 there overstates the rate, by more
 *     the further into the year we are, and nothing on screen would show the span was not 5.0.
 *
 * The cost is up to a year of freshness at the endpoint, which is the right trade for a number
 * whose whole claim is "per annum".
 */
export const periodYear = (p: string): number | null => {
  if (p === 'LTM' || isEstimatePeriod(p)) return null;
  const m = /^(\d{4})/.exec(p);
  return m ? Number(m[1]) : null;
};

/** `2025-Q3` → `3`; an annual period → `''`. */
export const periodQuarter = (p: string): string => (/-Q(\d)$/.exec(p)?.[1] ?? '');

/**
 * `years`-year CAGR of `level`, ending at the latest reported period.
 *
 * ⚠ THE START MUST BE THE PERIOD EXACTLY `years` BACK — never "the earliest we have". Falling back
 * to whatever exists is the failure that matters here: it returns a 6-year growth rate in a column
 * headed 10y, which is not a missing number but a wrong one, and it is wrong in the flattering
 * direction for any line that has been rising.
 *
 * ⚠ AND ON A QUARTERLY AXIS IT IS THE SAME QUARTER, `years` EARLIER. Comparing Q3 against Q1 five
 * years back reads a seasonal swing as compound growth — for a retailer that is most of the answer.
 *
 * ⚠ THE SERIES IS ALREADY FILTERED TO PERIODS THE CHART DRAWS. `buildBlend` only writes `level[y]`
 * for a period over `MIN_YEAR_COVERAGE_PCT`, so a year where too few constituents had reported is
 * ABSENT rather than thin — and this reports it as absent instead of silently spanning across it.
 */
export function lineCagr(
  level: Record<string, { value: number }>, years: number, endPeriod?: string,
): Cagr {
  const reported = Object.keys(level).filter((p) => periodYear(p) !== null).sort(periodOrder);
  if (!reported.length) return { pct: null, reason: 'the line has no reported periods' };

  /**
   * ⚠⚠ `endPeriod` PINS BOTH SIDES TO THE SAME WINDOW, AND WITHOUT IT THE COMPARISON QUIETLY STOPS
   * BEING ONE. Each line ends at its own latest DRAWN period, and the two need not agree: the
   * coverage floor drops a period until enough constituents have filed, and a twenty-holding book
   * crosses that threshold weeks before a 1,900-name index does. So the book's line routinely ends
   * a year ahead of the benchmark's — and a 2020→2025 rate set beside a 2019→2024 one, in a column
   * headed "5y", is two different questions printed as a comparison.
   *
   * The caller passes the latest period BOTH lines have, so every row spans the same years. The
   * cost is that a book may be shown a year staler than it could manage alone; that is the right
   * trade for a table whose entire purpose is the row underneath.
   */
  const to = endPeriod && level[endPeriod] && periodYear(endPeriod) !== null
    ? endPeriod
    : reported[reported.length - 1];
  if (endPeriod && to !== endPeriod) {
    return { pct: null, reason: `this line has no ${endPeriod} point, so it cannot be measured over `
      + 'the same window as the other one' };
  }
  const endYear = periodYear(to) as number;
  const q = periodQuarter(to);
  const wantYear = endYear - years;
  const from = reported.find((p) => periodYear(p) === wantYear && periodQuarter(p) === q);
  if (!from) {
    return { pct: null,
      reason: `no ${q ? `Q${q} ` : ''}${wantYear} point on the line — either the history does not `
        + 'reach back that far, or too few constituents had reported that period for it to be drawn' };
  }

  const a = level[from].value;
  const b = level[to].value;
  if (!(a > 0) || !(b > 0)) {
    return { pct: null, reason: 'the line is not positive at both ends, so a growth RATE is undefined' };
  }
  return { pct: 100 * ((b / a) ** (1 / years) - 1), from, to, years };
}

/**
 * Portfolio minus benchmark, in PERCENTAGE POINTS.
 *
 * ⚠ pp, NOT `%`. The difference between two rates is not itself a rate, and writing "3.2%" for a
 * gap between 8.4% and 5.2% invites it being read as a relative one (which would be 62%).
 *
 * ⚠ AND IT REFUSES UNLESS BOTH SIDES SPAN THE SAME WINDOW. A portfolio measured 2019→2024 against
 * an index measured 2015→2025 is two different questions subtracted from each other; the gap would
 * be a number with no meaning that nothing on screen would contradict.
 */
/**
 * The latest reported period BOTH lines carry — the window a comparison can honestly use.
 *
 * ⚠ `null` WHEN THEY SHARE NONE, which is a real state (a book of 2020-onwards listings against an
 * index whose drawn periods stop in 2019) and not something to paper over with the newer of the two.
 */
export function commonEndPeriod(
  a: Record<string, { value: number }>, b: Record<string, { value: number }>,
): string | null {
  const inB = new Set(Object.keys(b));
  const shared = Object.keys(a)
    .filter((p) => inB.has(p) && periodYear(p) !== null)
    .sort(periodOrder);
  return shared.length ? shared[shared.length - 1] : null;
}

export function cagrExcess(a: Cagr, b: Cagr): { pp: number } | { pp: null; reason: string } {
  if (a.pct == null) return { pp: null, reason: a.reason };
  if (b.pct == null) return { pp: null, reason: b.reason };
  if (a.from !== b.from || a.to !== b.to) {
    return { pp: null,
      reason: `the two are measured over different windows (${a.from}→${a.to} against `
        + `${b.from}→${b.to}), so the difference would not mean anything` };
  }
  return { pp: a.pct - b.pct };
}
