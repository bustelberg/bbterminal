/**
 * Five- and ten-year reads of a per-year series — the arithmetic behind the `Tables` tab.
 *
 * ⚠⚠ TWO DIFFERENT QUESTIONS, AND THE ROW DECIDES WHICH. A per-share amount COMPOUNDS, so the
 * honest summary is a rate (`lineCagr`). A margin and a return on capital are RATIOS: they do not
 * compound, and "ROIC grew 6% a year" is a sentence about a percentage of a percentage that nobody
 * means. For those the five-year read is the five-year AVERAGE — the standard quality read, and the
 * one `MarginCard`'s own tiles already show.
 *
 * ⚠ THE SERIES ARE THE CARDS' OWN. `marginByYear` and `roicByYear` are what the Long Equity charts
 * draw, weight-weighted per year; this module only takes windows of them. A second aggregation
 * "the same way" is how a table comes to disagree with the chart two tabs from it.
 */
import { xToPeriod } from './marginData';

export type WindowMean =
  | { mean: number; n: number; of: number; fromX: number; toX: number }
  /** ⚠ A REASON, NOT A NULL — the absences here have different fixes and a bare dash hides which. */
  | { mean: null; reason: string };

/**
 * The latest x BOTH series carry a value at, or null.
 *
 * ⚠⚠ IT IS WHY THE TWO ROWS ARE COMPARABLE AT ALL. Each side ends at its own latest year, and a
 * twenty-holding book crosses into a new fiscal year long before a 1,900-name index does. A book
 * averaged over 2021-2025 beside an index averaged over 2020-2024, under one "5y" heading, is two
 * different questions printed as a comparison.
 */
export function latestCommonX(
  a: ReadonlyMap<number, number | null>, b: ReadonlyMap<number, number | null>,
): number | null {
  const xs = [...a.keys()].filter((x) => a.get(x) != null && b.get(x) != null);
  return xs.length ? Math.max(...xs) : null;
}

/** The latest x this one series has a value at. */
export function latestX(a: ReadonlyMap<number, number | null>): number | null {
  const xs = [...a.keys()].filter((x) => a.get(x) != null);
  return xs.length ? Math.max(...xs) : null;
}

/**
 * Mean of `series` over the `years` ending at `endX` (inclusive).
 *
 * ⚠ THE WINDOW IS HALF-OPEN AT THE START — `endX - years < x <= endX`. For annual data that is
 * exactly `years` points (2021..2025 for a 5-year window ending 2025); written `>=` it would be six.
 *
 * ⚠⚠ IT AVERAGES WHAT IS THERE AND SAYS HOW MANY, RATHER THAN REFUSING OR PRETENDING. A gap year in
 * the middle of a margin series is normal (a constituent restated, a period under the coverage
 * floor) and a mean over four of five years is still the number you want — but "12.3%" and
 * "12.3% (4 of 5)" are different claims, and the second one is the true one. `n < of` is surfaced
 * on the cell, not buried.
 *
 * ⚠ ZERO IS A REAL READING AND A NULL IS NOT. `marginByYear` yields null for a year it cannot
 * compute; counting those as 0 would drag every average toward zero by exactly the amount of the
 * missing data, which is the most flattering-looking way to be wrong about a bad year.
 */
export function windowMean(
  series: ReadonlyMap<number, number | null>, endX: number, years: number,
): WindowMean {
  const inWindow = [...series.entries()]
    .filter(([x, v]) => v != null && x > endX - years && x <= endX)
    .sort((p, q) => p[0] - q[0]);
  if (!inWindow.length) {
    return { mean: null,
      reason: `nothing on this line between ${xToPeriod(endX - years + 1)} and ${xToPeriod(endX)}` };
  }
  const sum = inWindow.reduce((acc, [, v]) => acc + (v as number), 0);
  return {
    mean: sum / inWindow.length,
    n: inWindow.length,
    // ⚠ THE DENOMINATOR IS THE WINDOW, NOT THE SAMPLE — `of` is how many years were ASKED for, so
    // `n of of` reads as coverage. Setting both from the sample would always say "5 of 5".
    of: years,
    fromX: inWindow[0][0],
    toX: inWindow[inWindow.length - 1][0],
  };
}

/**
 * Portfolio minus benchmark, in percentage POINTS.
 *
 * ⚠ pp, NOT `%` — both sides are already percentages, and the difference between two percentages is
 * not a percentage of anything.
 *
 * ⚠ AND IT REFUSES ACROSS DIFFERENT WINDOWS, for the same reason `cagrExcess` does: subtracting a
 * 2021-2025 average from a 2019-2023 one produces a number with no meaning and nothing on screen
 * would contradict it.
 */
export function meanExcess(a: WindowMean, b: WindowMean): { pp: number } | { pp: null; reason: string } {
  if (a.mean == null) return { pp: null, reason: a.reason };
  if (b.mean == null) return { pp: null, reason: b.reason };
  if (a.fromX !== b.fromX || a.toX !== b.toX) {
    return { pp: null,
      reason: `the two are averaged over different windows (${xToPeriod(a.fromX)}-${xToPeriod(a.toX)} `
        + `against ${xToPeriod(b.fromX)}-${xToPeriod(b.toX)}), so the difference would not mean anything` };
  }
  return { pp: a.mean - b.mean };
}
