import { describe, expect, it } from 'vitest';
import { endpointCagr, lineCagr } from './lineCagr';

/**
 * ONE definition of CAGR, used by both surfaces that quote one.
 *
 * ⚠⚠ THERE WERE TWO. The Long Equity growth card reported the SLOPE OF A FITTED EXPONENTIAL
 * (`logLinearFit`) while the Tables tab measured endpoints, so the same book's FCF/share read
 * 29.7% on one screen and 30.1% two tabs away. Both were defensible; neither was checkable against
 * the other, and a reader has no way to tell a modelling difference from a data problem. The card
 * now calls `endpointCagr` and the table `lineCagr`, and the two share the arithmetic.
 *
 * ⚠ THE FIT DID NOT GO AWAY — R² and the drawn trend line are still it. "How steady" is a question
 * about a model; "what was the rate" is a question about two reported numbers. Keeping both, and
 * keeping them clearly separate, is the point.
 */

const pts = (...v: [number, number][]) => v.map(([year, value]) => ({ year, value }));

describe('endpointCagr', () => {
  it('is (end/start)^(1/years) − 1 over the first and last points', () => {
    // 100 → 200 over 10 years = 2^0.1 − 1
    const got = endpointCagr(pts([2015, 100], [2020, 141.4213], [2025, 200]));
    expect(got.pct).toBeCloseTo(7.1773, 3);
    expect(got.pct != null && got.years).toBe(10);
  });

  it('⚠ IGNORES EVERY POINT IN BETWEEN, which is the whole difference from a fit', () => {
    // The same endpoints with a wildly different path give the identical answer. That is the
    // definition, and it is why a single unrepresentative endpoint year moves this number.
    const a = endpointCagr(pts([2015, 100], [2020, 500], [2025, 200]));
    const b = endpointCagr(pts([2015, 100], [2020, 20], [2025, 200]));
    expect(a.pct).toBeCloseTo(b.pct as number, 12);
  });

  it('handles a FRACTIONAL span, so a quarterly axis is still per annum', () => {
    // x is the period as a number: 2015.25 is Q2. 100 → 200 over 2.5 years.
    const got = endpointCagr(pts([2015.0, 100], [2017.5, 200]));
    expect(got.pct != null && got.years).toBe(2.5);
    expect(got.pct).toBeCloseTo(100 * (2 ** (1 / 2.5) - 1), 6);
  });

  it('⚠ REFUSES a non-positive endpoint rather than trimming inward to a positive one', () => {
    // Trimming would answer over a window nobody chose and label it as the whole chart — the trap
    // `logLinearFit` fell into by DROPPING those points while the tile still said "over 9 years".
    const got = endpointCagr(pts([2015, -0.41], [2020, 0.3], [2025, 2.0]));
    expect(got.pct).toBeNull();
    expect(got.pct == null && got.reason).toMatch(/not positive at both ends/);
  });

  it('refuses a negative END too — a company that fell into losses has no growth RATE', () => {
    expect(endpointCagr(pts([2015, 5], [2025, -1])).pct).toBeNull();
  });

  it('refuses fewer than two points, and a zero-length span', () => {
    expect(endpointCagr(pts([2025, 100])).pct).toBeNull();
    expect(endpointCagr(pts([2025, 100], [2025, 200])).pct).toBeNull();
  });

  it('names the periods it used, so the tile can state its window', () => {
    const got = endpointCagr(pts([2015, 100], [2025, 200]));
    expect(got.pct != null && [got.from, got.to]).toEqual(['2015', '2025']);
  });

  it('honours a label formatter for a quarterly axis', () => {
    const got = endpointCagr(pts([2015.25, 100], [2025.25, 200]),
                             (x) => `${Math.floor(x)} Q${Math.round((x % 1) * 4) + 1}`);
    expect(got.pct != null && got.from).toBe('2015 Q2');
  });
});

describe('the two surfaces agree by construction', () => {
  /**
   * ⚠⚠ THE INVARIANT THE WHOLE CHANGE EXISTS FOR. Given the same series and the same window, the
   * card's `endpointCagr` and the table's `lineCagr` must return the same number — not close, the
   * same. They take different shapes (points vs a period-keyed level map) because their callers
   * hold different shapes; the arithmetic is one definition.
   */
  it('endpointCagr(points) == lineCagr(level, span) on the same series', () => {
    const years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025];
    const vals = [100, 126.34, 171.35, 223.06, 270.61, 338.46, 461.01, 454.35, 619.15,
                  987.81, 1167.75];   // Bustelberg Offensief's real FCF/share index
    const level = Object.fromEntries(years.map((y, i) => [String(y), { value: vals[i] }]));
    const a = endpointCagr(years.map((y, i) => ({ year: y, value: vals[i] })));
    const b = lineCagr(level, 10);
    expect(a.pct).toBeCloseTo(b.pct as number, 12);
    expect(a.pct).toBeCloseTo(27.86, 2);
  });

  it('⚠ AND THE WINDOWS MUST MATCH FOR THAT TO HOLD — the table pins 10 years, the card takes '
     + 'its whole drawn series, so a series reaching further back is a DIFFERENT window', () => {
    const years = [2013, 2014, 2015, 2020, 2025];
    const vals = [50, 60, 100, 300, 1167.75];
    const level = Object.fromEntries(years.map((y, i) => [String(y), { value: vals[i] }]));
    const a = endpointCagr(years.map((y, i) => ({ year: y, value: vals[i] })));   // 2013→2025
    const b = lineCagr(level, 10);                                                // 2015→2025
    expect(a.pct).not.toBeCloseTo(b.pct as number, 4);
    // …which is why both tiles state the window they used rather than just a percentage.
    expect(a.pct != null && a.years).toBe(12);
    expect(b.pct != null && b.years).toBe(10);
  });
});
