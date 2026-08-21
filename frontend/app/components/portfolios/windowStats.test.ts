import { describe, expect, it } from 'vitest';
import {
  clipPoints, latestCommonX, latestX, meanExcess, sharedSpan, spanNarrows, tileStats,
  windowMean,
} from './windowStats';

/**
 * The 5y/10y averages behind the `Tables` tab.
 *
 * ⚠ A MARGIN AND A ROIC DO NOT COMPOUND, which is why these are means and not rates. "ROIC grew 6%
 * a year" is a sentence about a percentage of a percentage that nobody means; the five-year read of
 * a ratio is its five-year average, which is also what `MarginCard`'s own tiles show.
 */

const series = (pairs: [number, number | null][]) => new Map(pairs);

describe('the window', () => {
  const s = series([[2019, 10], [2020, 12], [2021, 14], [2022, 16], [2023, 18],
                    [2024, 20], [2025, 22]]);

  it('a 5y window ending 2025 is 2021..2025 — five points, not six', () => {
    const got = windowMean(s, 2025, 5);
    expect(got).toMatchObject({ n: 5, of: 5, fromX: 2021, toX: 2025 });
    expect(got.mean).toBeCloseTo((14 + 16 + 18 + 20 + 22) / 5, 10);
  });

  it('⚠ the start is EXCLUSIVE — written `>=` it would be a six-year mean under a 5y heading', () => {
    expect(windowMean(s, 2025, 5)).toMatchObject({ fromX: 2021, n: 5 });
  });

  it('ignores anything after the endpoint', () => {
    // A book that has filed 2025 must still be averaged to 2024 when that is the shared endpoint.
    expect(windowMean(s, 2024, 5)).toMatchObject({ fromX: 2020, toX: 2024, n: 5 });
  });

  it('a 10y window over 7 years of history averages the 7 and says so', () => {
    const got = windowMean(s, 2025, 10);
    expect(got).toMatchObject({ n: 7, of: 10 });
  });
});

describe('gaps', () => {
  it('⚠ a null year is skipped, NEVER counted as zero', () => {
    // Counting it as 0 drags the average toward zero by exactly the missing data — the most
    // flattering-looking way to be wrong about a bad year.
    const s = series([[2021, 10], [2022, null], [2023, 20]]);
    const got = windowMean(s, 2023, 3);
    expect(got.mean).toBeCloseTo(15, 10);
    expect(got).toMatchObject({ n: 2, of: 3 });
  });

  it('a zero year IS counted — it is a reading, not an absence', () => {
    const s = series([[2022, 0], [2023, 20]]);
    const got = windowMean(s, 2023, 2);
    expect(got.mean).toBeCloseTo(10, 10);
    expect(got).toMatchObject({ n: 2 });
  });

  it('an empty window refuses and names the years it looked at', () => {
    const got = windowMean(series([[2010, 5]]), 2025, 5);
    expect(got.mean).toBeNull();
    expect((got as { reason: string }).reason).toMatch(/2021/);
  });

  it('negative values average normally — a loss-making year is a fact', () => {
    const s = series([[2022, -5], [2023, 15]]);
    expect(windowMean(s, 2023, 2).mean).toBeCloseTo(5, 10);
  });
});

describe('the shared endpoint', () => {
  it('is the latest year BOTH sides have a value at', () => {
    const book = series([[2023, 1], [2024, 2], [2025, 3]]);
    const index = series([[2023, 1], [2024, 2]]);
    expect(latestCommonX(book, index)).toBe(2024);
  });

  it('⚠ a null on one side is not a shared year', () => {
    const book = series([[2024, 2], [2025, 3]]);
    const index = series([[2024, 2], [2025, null]]);
    expect(latestCommonX(book, index)).toBe(2024);
  });

  it('is null when they overlap nowhere', () => {
    expect(latestCommonX(series([[2025, 1]]), series([[2019, 1]]))).toBeNull();
  });

  it('a single series reports its own latest', () => {
    expect(latestX(series([[2023, 1], [2024, null], [2025, 3]]))).toBe(2025);
    expect(latestX(series([[2025, null]]))).toBeNull();
  });
});

describe('the excess', () => {
  const s = series([[2021, 10], [2022, 10], [2023, 10], [2024, 10], [2025, 10]]);
  const t = series([[2021, 6], [2022, 6], [2023, 6], [2024, 6], [2025, 6]]);

  it('is a difference in percentage POINTS', () => {
    const got = meanExcess(windowMean(s, 2025, 5), windowMean(t, 2025, 5));
    expect((got as { pp: number }).pp).toBeCloseTo(4, 10);
  });

  it('⚠ refuses across different windows', () => {
    const got = meanExcess(windowMean(s, 2025, 5), windowMean(t, 2024, 5));
    expect(got.pp).toBeNull();
    expect((got as { reason: string }).reason).toMatch(/different windows/);
  });

  it('carries a missing side’s own reason through', () => {
    const empty = windowMean(series([[2010, 1]]), 2025, 5);
    expect((meanExcess(windowMean(s, 2025, 5), empty) as { reason: string }).reason)
      .toMatch(/nothing on this line/);
  });
});

/**
 * ─── THE STAT TILES' SHARED WINDOW ──────────────────────────────────────────────────────────────
 *
 * ⚠⚠ EVERY LONG EQUITY CARD NOW PRINTS ITS FIGURE AND THE BENCHMARK'S SIDE BY SIDE, and two tiles
 * side by side are a subtraction waiting to happen. Over different spans that subtraction means
 * nothing, and nothing on screen would contradict it — a single company reaching back to 1998
 * against an index blend starting in 2015 is a 27-year rate beside a 10-year one under one word.
 * These are the rules that stop it, and they are pure, so they are tested here rather than by
 * eyeballing fourteen charts.
 */
describe('the span two lines share', () => {
  const own = series([[2018, 1], [2019, 2], [2020, 3], [2021, 4], [2022, 5]]);
  const bench = series([[2020, 9], [2021, 9], [2022, 9], [2023, 9]]);

  it('is the overlap, not either line\u2019s own extent', () => {
    expect(sharedSpan(own, bench)).toEqual({ fromX: 2020, toX: 2022 });
  });

  it('\u26a0 a key with a NULL value is not a year the line has', () => {
    // Without this the two "share" 2023, in which the book draws nothing at all.
    const holed = series([[2020, 1], [2021, 2], [2022, 3], [2023, null]]);
    expect(sharedSpan(holed, bench)).toEqual({ fromX: 2020, toX: 2022 });
  });

  it('is null when they overlap in nothing, and then nothing is narrowed', () => {
    expect(sharedSpan(own, series([[2030, 1]]))).toBeNull();
    expect(tileStats(own, null).n).toBe(5);
    expect(spanNarrows(own, null)).toBe(false);
  });
});

describe('what one tile reads off one line', () => {
  const own = series([[2018, 10], [2019, 20], [2020, 30], [2021, 40], [2022, 50]]);
  const bench = series([[2020, 1], [2021, 2], [2022, 3], [2023, 4]]);

  it('over the whole series when there is no benchmark to pin it to', () => {
    expect(tileStats(own, null)).toMatchObject({ avg: 30, latest: 50, latestX: 2022, n: 5 });
  });

  it('\u26a0\u26a0 `latest` is the last point IN THE WINDOW, never the series\u2019 own newest', () => {
    // The whole point: with a benchmark on screen both `Latest` tiles must name the same period.
    const span = sharedSpan(own, bench) as { fromX: number; toX: number };
    expect(tileStats(own, span)).toMatchObject({ latest: 50, latestX: 2022 });
    // The index reaches a year further and is cut back to the shared end, not left at 2023.
    expect(tileStats(bench, span)).toMatchObject({ latest: 3, latestX: 2022, n: 3 });
  });

  it('averages only the shared years \u2014 the book\u2019s 2018/2019 are out', () => {
    const span = sharedSpan(own, bench) as { fromX: number; toX: number };
    expect(tileStats(own, span).avg).toBeCloseTo((30 + 40 + 50) / 3, 10);
    expect(tileStats(bench, span).avg).toBeCloseTo(2, 10);
  });

  it('\u26a0 skips a null rather than reading it as zero', () => {
    // Counting the gap as 0 would drag the mean down by exactly the missing data — the most
    // flattering-looking way to be wrong about a bad year. Same rule as `windowMean`.
    expect(tileStats(series([[2020, 10], [2021, null], [2022, 20]]), null))
      .toMatchObject({ avg: 15, latest: 20, n: 2 });
  });

  it('an empty line reads as dashes, not as NaN', () => {
    expect(tileStats(series([]), null)).toMatchObject({ avg: null, latest: null, n: 0 });
    expect(tileStats(series([[2019, null]]), null).avg).toBeNull();
  });
});

describe('saying so when the window moved', () => {
  const own = series([[2018, 1], [2019, 2], [2020, 3], [2021, 4], [2022, 5]]);

  it('\u26a0 announces a narrowing only when one actually happened', () => {
    // A number that changes because of an unrelated control, silently, reads as a bug.
    expect(spanNarrows(own, { fromX: 2020, toX: 2022 })).toBe(true);   // lost 2018-2019
    expect(spanNarrows(own, { fromX: 2018, toX: 2021 })).toBe(true);   // lost 2022
  });

  it('\u26a0 stays quiet on the common case \u2014 both lines already covering the same years', () => {
    // A warning that fires when nothing happened is how a real warning stops being read.
    expect(spanNarrows(own, { fromX: 2018, toX: 2022 })).toBe(false);
    expect(spanNarrows(own, { fromX: 2017, toX: 2023 })).toBe(false);  // wider than us: no loss
  });
});

describe('the growth cards\u2019 clip is the same window on a point array', () => {
  const pts = [2018, 2019, 2020, 2021, 2022].map((year) => ({ year, value: year - 2000 }));

  it('keeps the shared years, in order', () => {
    expect(clipPoints(pts, { fromX: 2020, toX: 2021 }).map((p) => p.year)).toEqual([2020, 2021]);
  });

  it('\u26a0 a null span is the whole series \u2014 no benchmark, nothing to pin to', () => {
    expect(clipPoints(pts, null)).toHaveLength(5);
  });

  it('agrees with `tileStats` about which years are in \u2014 one window, two shapes', () => {
    // The map form feeds the ratio cards and the array form the growth cards; if these two ever
    // disagreed, two cards on one screen would be measured over different years.
    const span = { fromX: 2019, toX: 2021 };
    const asMap = series(pts.map((p) => [p.year, p.value] as [number, number]));
    expect(clipPoints(pts, span).map((p) => p.year))
      .toEqual([...asMap.keys()].filter((x) => tileStats(asMap, span).n && x >= span.fromX && x <= span.toX));
  });
});
