import { describe, expect, it } from 'vitest';
import {
  cagrExcess, commonEndPeriod, forwardCagr, lineCagr, periodQuarter, periodYear,
} from './lineCagr';

/**
 * The CAGR behind the `Table` view.
 *
 * ⚠ EVERY CASE HERE IS AN ABSENCE THAT WOULD OTHERWISE HAVE BEEN A PLAUSIBLE NUMBER. A CAGR is one
 * division and a root; nothing about the arithmetic is hard. What is hard is refusing to answer
 * when the window is not the window the column claims — and every one of those failures reads as a
 * finding rather than as a bug.
 */

/** A level series: `{period: {value}}`, exactly what `buildBlend` returns as `level`. */
const lvl = (pairs: [string, number][]) =>
  Object.fromEntries(pairs.map(([p, v]) => [p, { value: v }]));

describe('the arithmetic', () => {
  it('doubling over 5 years is ~14.87%/yr', () => {
    const got = lineCagr(lvl([['2019', 100], ['2024', 200]]), 5);
    expect(got.pct).toBeCloseTo(14.87, 2);
    expect(got).toMatchObject({ from: '2019', to: '2024', years: 5 });
  });

  it('a flat line is exactly zero, not a rounding artefact', () => {
    expect(lineCagr(lvl([['2014', 137.5], ['2024', 137.5]]), 10).pct).toBe(0);
  });

  it('a falling line is negative', () => {
    const got = lineCagr(lvl([['2019', 200], ['2024', 100]]), 5);
    expect(got.pct).toBeLessThan(0);
    expect(got.pct).toBeCloseTo(-12.94, 2);
  });

  it('the index BASE is irrelevant — only the ratio is', () => {
    const a = lineCagr(lvl([['2019', 100], ['2024', 150]]), 5).pct;
    const b = lineCagr(lvl([['2019', 431.7], ['2024', 647.55]]), 5).pct;
    expect(a).toBeCloseTo(b as number, 10);
  });
});

describe('it measures the window the column CLAIMS', () => {
  it('⚠ it does NOT fall back to the earliest period it has', () => {
    // Six years of history, asked for ten. Answering 6 would be a wrong number in a 10y column,
    // and wrong in the flattering direction for anything that has been rising.
    const series = lvl([['2018', 100], ['2019', 110], ['2020', 121], ['2021', 133],
                        ['2022', 146], ['2023', 161], ['2024', 177]]);
    expect(lineCagr(series, 10).pct).toBeNull();
    expect(lineCagr(series, 5).pct).not.toBeNull();
  });

  it('a period the coverage floor dropped is an absence, not a span across it', () => {
    // 2019 is missing because too few constituents had reported it — `buildBlend` never wrote it.
    const series = lvl([['2018', 90], ['2020', 121], ['2024', 177]]);
    const got = lineCagr(series, 5);
    expect(got.pct).toBeNull();
    expect((got as { reason: string }).reason).toMatch(/2019/);
  });

  it('names the year it wanted, so the reason is actionable', () => {
    const got = lineCagr(lvl([['2022', 100], ['2024', 120]]), 10);
    expect((got as { reason: string }).reason).toContain('2014');
  });
});

describe('quarterly axes', () => {
  it('⚠ it compares the SAME quarter, n years back', () => {
    // Q3 against Q1 five years back would read a seasonal swing as compound growth.
    const series = lvl([['2019-Q1', 50], ['2019-Q3', 100], ['2024-Q1', 90], ['2024-Q3', 200]]);
    const got = lineCagr(series, 5);
    expect(got).toMatchObject({ from: '2019-Q3', to: '2024-Q3' });
    expect(got.pct).toBeCloseTo(14.87, 2);
  });

  it('refuses when that same quarter is missing, rather than taking a neighbour', () => {
    const series = lvl([['2019-Q1', 50], ['2024-Q3', 200]]);
    expect(lineCagr(series, 5).pct).toBeNull();
  });
});

describe('which periods may be an endpoint', () => {
  it('⚠ an ESTIMATE is never the end — that would be a forecast dressed as a track record', () => {
    const series = lvl([['2019', 100], ['2024', 200], ['2025e', 260]]);
    expect(lineCagr(series, 5)).toMatchObject({ to: '2024' });
  });

  it('⚠ LTM is never the end either — its span is five years AND SOME MONTHS', () => {
    // Real and current, but it ends at the newest quarterly filing, so dividing by 5 overstates
    // the rate and nothing on screen would show the span was not 5.0.
    const series = lvl([['2019', 100], ['2024', 200], ['LTM', 215]]);
    expect(lineCagr(series, 5)).toMatchObject({ to: '2024' });
  });

  it('a line of nothing but estimates has no CAGR at all', () => {
    expect(lineCagr(lvl([['2025e', 100], ['2026e', 120]]), 5).pct).toBeNull();
  });
});

describe('undefined rather than infinite', () => {
  it('a non-positive endpoint refuses', () => {
    expect(lineCagr(lvl([['2019', 0], ['2024', 100]]), 5).pct).toBeNull();
    expect(lineCagr(lvl([['2019', 100], ['2024', -20]]), 5).pct).toBeNull();
  });

  it('an empty series refuses', () => {
    expect(lineCagr({}, 5).pct).toBeNull();
  });
});

describe('period parsing', () => {
  it('reads the year off both cadences', () => {
    expect(periodYear('2024')).toBe(2024);
    expect(periodYear('2024-Q3')).toBe(2024);
  });

  it('refuses the two non-fiscal labels', () => {
    expect(periodYear('LTM')).toBeNull();
    expect(periodYear('2026e')).toBeNull();
  });

  it('reads the quarter, or empty for an annual period', () => {
    expect(periodQuarter('2024-Q3')).toBe('3');
    expect(periodQuarter('2024')).toBe('');
  });
});

describe('expected growth, from the actuals into the consensus', () => {
  // The shape your AEX actually has: actuals to 2025 (2026 is under the floor), 2026e-2030e.
  const s = lvl([['2023', 80], ['2024', 90], ['2025', 100],
                 ['2026e', 110], ['2027e', 121], ['2028e', 133.1], ['2029e', 146.41]]);

  it('a 3y expectation runs from the latest ACTUAL to the estimate three years out', () => {
    const got = forwardCagr(s, 3);
    expect(got).toMatchObject({ from: '2025', to: '2028e', years: 3 });
    expect(got.pct).toBeCloseTo(10, 6);            // 100 -> 133.1 over 3 years
  });

  it('⚠ the base is the ACTUAL, never the nearest estimate', () => {
    // 2026e -> 2029e would be the consensus's own internal slope: three forecasts compared with
    // each other, with no contact with anything that happened.
    expect(forwardCagr(s, 3)).toMatchObject({ from: '2025' });
  });

  it('⚠ the target is matched by FISCAL YEAR, not by position in the estimate list', () => {
    // Ragged estimate columns (AEX runs to 2030e, ACWI to 2031e) must not change what "3y" means.
    const ragged = lvl([['2025', 100], ['2027e', 121], ['2028e', 133.1], ['2031e', 200]]);
    expect(forwardCagr(ragged, 3)).toMatchObject({ from: '2025', to: '2028e' });
    // …and asking for 6 finds 2031e, not "the last one".
    expect(forwardCagr(ragged, 6)).toMatchObject({ to: '2031e' });
    // …while 4 has no 2029e and refuses rather than taking a neighbour.
    expect(forwardCagr(ragged, 4).pct).toBeNull();
  });

  it('names the consensus year it wanted when it is missing', () => {
    const got = forwardCagr(lvl([['2025', 100], ['2026e', 110]]), 3);
    expect(got.pct).toBeNull();
    expect((got as { reason: string }).reason).toContain('2028e');
  });

  it('⚠ `lineCagr` still refuses to end on an estimate — the two do not overlap', () => {
    // The whole reason these are separate functions: the historical rate must never reach into the
    // forecast by accident, and the forward one must ask for it by name.
    //
    // ⚠ TWO YEARS, NOT THREE, AND THE DIFFERENCE IS THE FIXTURE'S NOT THE RULE'S. `s` holds
    // actuals for 2023-2025 only, so a 3-year LOOKBACK wants 2022 and correctly refuses — which
    // would have proved nothing about estimates. The backward window has to fit inside the actuals
    // while the forward one reaches into the consensus; that asymmetry is the point being made.
    expect(lineCagr(s, 2)).toMatchObject({ from: '2023', to: '2025' });
    expect(forwardCagr(s, 3)).toMatchObject({ from: '2025', to: '2028e' });
  });

  it('a shared base pins both sides to the same expectation window', () => {
    const book = lvl([['2024', 90], ['2025', 100], ['2027e', 121], ['2028e', 133.1]]);
    const index = lvl([['2024', 90], ['2027e', 110], ['2028e', 116]]);   // no 2025 actual
    const base = commonEndPeriod(book, index) as string;
    expect(base).toBe('2024');
    expect(forwardCagr(book, 3, base)).toMatchObject({ from: '2024', to: '2027e' });
    expect(forwardCagr(index, 3, base)).toMatchObject({ from: '2024', to: '2027e' });
  });

  it('a line without the pinned base says so rather than using its own', () => {
    const got = forwardCagr(lvl([['2024', 90], ['2027e', 110]]), 3, '2025');
    expect(got.pct).toBeNull();
    expect((got as { reason: string }).reason).toMatch(/same base/);
  });

  it('a loss year refuses rather than inverting', () => {
    expect(forwardCagr(lvl([['2025', -5], ['2028e', 10]]), 3).pct).toBeNull();
  });

  it('a line with no actuals at all has nothing to grow FROM', () => {
    expect(forwardCagr(lvl([['2026e', 100], ['2029e', 133]]), 3).pct).toBeNull();
  });
});

describe('both rows measured over ONE window', () => {
  /**
   * ⚠⚠ THE CASE THIS EXISTS FOR IS THE NORMAL ONE, NOT AN EDGE. Each line ends at its own latest
   * DRAWN period, and the coverage floor holds a period back until enough constituents have filed —
   * so a twenty-holding book crosses into a new fiscal year weeks before a 1,900-name index does.
   * Left alone the book reads 2020→2025 and the index 2019→2024, printed side by side under one
   * "5y" heading.
   */
  const book = lvl([['2019', 100], ['2020', 110], ['2024', 190], ['2025', 200]]);
  const index = lvl([['2019', 100], ['2020', 104], ['2024', 150]]);   // no 2025 yet

  it('the common endpoint is the latest period BOTH carry', () => {
    expect(commonEndPeriod(book, index)).toBe('2024');
  });

  it('pinning it makes both rows span the same years', () => {
    const end = commonEndPeriod(book, index) as string;
    const a = lineCagr(book, 5, end);
    const b = lineCagr(index, 5, end);
    expect(a).toMatchObject({ from: '2019', to: '2024' });
    expect(b).toMatchObject({ from: '2019', to: '2024' });
    expect(cagrExcess(a, b).pp).not.toBeNull();
  });

  it('WITHOUT pinning, the two drift apart and the excess is refused', () => {
    const a = lineCagr(book, 5);          // ends 2025
    const b = lineCagr(index, 5);         // ends 2024
    expect(a).toMatchObject({ to: '2025' });
    expect(b).toMatchObject({ to: '2024' });
    expect(cagrExcess(a, b).pp).toBeNull();
  });

  it('a line missing the pinned period says so rather than silently using its own', () => {
    const got = lineCagr(index, 5, '2025');
    expect(got.pct).toBeNull();
    expect((got as { reason: string }).reason).toMatch(/same window/);
  });

  it('no shared period at all is null, not the newer of the two', () => {
    expect(commonEndPeriod(lvl([['2024', 1]]), lvl([['2019', 1]]))).toBeNull();
  });

  it('LTM and estimates are never the common endpoint either', () => {
    const a = lvl([['2023', 1], ['2024', 2], ['LTM', 3], ['2025e', 4]]);
    const b = lvl([['2023', 1], ['2024', 2], ['LTM', 3], ['2025e', 4]]);
    expect(commonEndPeriod(a, b)).toBe('2024');
  });
});

describe('the excess', () => {
  const p = lineCagr(lvl([['2019', 100], ['2024', 200]]), 5);      // 14.87%
  const b = lineCagr(lvl([['2019', 100], ['2024', 150]]), 5);      //  8.45%

  it('is a difference in percentage POINTS', () => {
    expect((cagrExcess(p, b) as { pp: number }).pp).toBeCloseTo(6.42, 2);
  });

  it('⚠ refuses when the two span different windows', () => {
    // A portfolio measured 2019→2024 against an index measured 2015→2020 is two different
    // questions subtracted from each other.
    const other = lineCagr(lvl([['2015', 100], ['2020', 150]]), 5);
    const got = cagrExcess(p, other);
    expect(got.pp).toBeNull();
    expect((got as { reason: string }).reason).toMatch(/different windows/);
  });

  it('carries the missing side’s own reason through', () => {
    const missing = lineCagr(lvl([['2022', 100], ['2024', 120]]), 10);
    expect((cagrExcess(p, missing) as { reason: string }).reason).toContain('2014');
    expect((cagrExcess(missing, p) as { reason: string }).reason).toContain('2014');
  });
});
