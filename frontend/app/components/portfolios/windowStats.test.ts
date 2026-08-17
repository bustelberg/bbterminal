import { describe, expect, it } from 'vitest';
import { latestCommonX, latestX, meanExcess, windowMean } from './windowStats';

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
