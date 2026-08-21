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

/**
 * ─── THE STAT TILES' WINDOW ─────────────────────────────────────────────────────────────────────
 *
 * ⚠⚠ TWO TILES SIDE BY SIDE ARE A COMPARISON, AND A COMPARISON IS ONLY ONE IF BOTH SIDES SPAN THE
 * SAME YEARS. Every Long Equity card now prints its own figure and the benchmark's next to it. Left
 * to their own histories those are two different questions under two labels a reader has every
 * reason to subtract: a single company reaches back to 1998 while an index blend starts in 2015, so
 * "CAGR +14.2%" over 27 years would sit beside "CAGR · ACWI +8.1%" over 10, with nothing on screen
 * saying the spans differ. The same trap `latestCommonX`, `meanExcess` and `lineCagr`'s `endPeriod`
 * already exist to close one tab away, in the same modal.
 *
 * So when a benchmark line is drawn, BOTH sides are measured over the span the two lines share, and
 * the card states that span. ⚠ THE COST IS REAL AND IS THE ACCEPTED ONE: a book that has filed a
 * year the index has not is shown a year staler than it could manage alone. That is the trade
 * `windowMean`'s callers already make, and it is the right way round — a fresher number nobody can
 * compare is worth less here than a comparable one, because the second line IS why the tile exists.
 *
 * ⚠ WITH NO BENCHMARK NOTHING NARROWS. `span` is null, every helper below falls through to the
 * whole series, and the tiles read exactly as they did before there was a second line.
 */

/** An inclusive x range. ⚠ A RANGE, NOT A SET — a series may still have gaps inside it (a period
 *  under the coverage floor), and `tileStats` reports how many points it actually found. */
export type Span = { fromX: number; toX: number };

/**
 * The x range BOTH series carry a value in, or null when they overlap in nothing.
 *
 * ⚠ THE OVERLAP OF THE TWO, NOT EITHER ONE'S OWN EXTENT. Taking the book's range and hoping the
 * index covers it is the failure this exists to prevent: the index would then be averaged over
 * whatever part of that window it happens to have, which is a third window again.
 *
 * ⚠ NULL-VALUED KEYS ARE NOT COVERAGE. Both cards' series maps carry `null` for a period they could
 * not compute, and a key with no value is not a year the line has — counting it would let two lines
 * "share" a span in which one of them draws nothing.
 */
export function sharedSpan(
  a: ReadonlyMap<number, number | null>, b: ReadonlyMap<number, number | null>,
): Span | null {
  const xs = [...a.keys()].filter((x) => a.get(x) != null && b.get(x) != null);
  return xs.length ? { fromX: Math.min(...xs), toX: Math.max(...xs) } : null;
}

/** What one stat tile reads off one line. `n`/`fromX`/`toX` are what the card states about the
 *  window — see the ⚠⚠ above for why an unstated window is the bug. */
export type TileStats = {
  avg: number | null; latest: number | null; latestX: number | null;
  n: number; fromX: number | null; toX: number | null;
};

const EMPTY_TILE: TileStats =
  { avg: null, latest: null, latestX: null, n: 0, fromX: null, toX: null };

/**
 * The `Avg` and `Latest` a tile prints, over `span` (the whole series when null).
 *
 * ⚠ `latest` IS THE LAST POINT **IN THE WINDOW**, not the series' own newest. That is the entire
 * point of the window: with a benchmark on screen the two `Latest` tiles must name the same period,
 * and a card whose own line reaches one year further would otherwise print FY2025 beside the
 * index's FY2024 under one word.
 *
 * ⚠ A NULL IS SKIPPED, NEVER READ AS ZERO. Both `weightedByYear` and the growth blend leave a period
 * they could not compute out; averaging it in as 0 would drag the mean down by exactly the amount of
 * the missing data — the most flattering-looking way to be wrong about a bad year. Same rule as
 * `windowMean`.
 */
export function tileStats(
  series: ReadonlyMap<number, number | null>, span: Span | null,
): TileStats {
  const pts = [...series.entries()]
    .filter(([x, v]) => v != null && (!span || (x >= span.fromX && x <= span.toX)))
    .sort((p, q) => p[0] - q[0]) as [number, number][];
  if (!pts.length) return EMPTY_TILE;
  return {
    avg: pts.reduce((a, [, v]) => a + v, 0) / pts.length,
    latest: pts[pts.length - 1][1],
    latestX: pts[pts.length - 1][0],
    n: pts.length,
    fromX: pts[0][0],
    toX: pts[pts.length - 1][0],
  };
}

/** The same clip for the growth cards, whose series are point ARRAYS rather than maps — they feed
 *  `logLinearFit` and `endpointCagr`, which both take `{year, value}[]`. Order is preserved. */
export function clipPoints<T extends { year: number }>(pts: T[], span: Span | null): T[] {
  return span ? pts.filter((p) => p.year >= span.fromX && p.year <= span.toX) : pts;
}

/**
 * Did the span actually take anything off THIS series? Only then is there a window worth stating.
 *
 * ⚠ ASKED OF THE SERIES, NOT OF THE SPAN. When the two lines already cover the same years the span
 * is non-null and changes nothing, and a card that announced a window there would be explaining a
 * narrowing that did not happen — noise on the common case, which is how a real warning stops being
 * read.
 */
export function spanNarrows(
  series: ReadonlyMap<number, number | null>, span: Span | null,
): boolean {
  if (!span) return false;
  const full = tileStats(series, null);
  return full.fromX != null
    && (full.fromX < span.fromX || (full.toX as number) > span.toX);
}
