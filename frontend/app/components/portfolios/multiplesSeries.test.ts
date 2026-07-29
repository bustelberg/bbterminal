import { describe, it, expect } from 'vitest';
import {
  align, forwardSeries, medianSpacing, pick, reportedAt, since, thin, trailingMultiples, ttm,
  FORWARD_PE_CODE, QUARTERLY_FCF_CODES, REPORT_LAG_DAYS,
} from './multiplesSeries';
import { type MetricRow } from './quickValuation';

const m = (metric_code: string, target_date: string, numeric_value: number | null): MetricRow =>
  ({ metric_code, target_date, numeric_value });
const d = (iso: string) => new Date(`${iso}T00:00:00Z`).getTime();

describe('ttm — the rolling four quarters', () => {
  // ASML's real 2024 quarters. ⚠ Verified against the annual row BEFORE this was written: the
  // four sum to 23.08, which is exactly `annuals__…__Free Cash Flow per Share` for 2024. Had the
  // vendor been publishing year-to-date cumulatives, summing would have counted Q1 four times.
  const q2024 = [
    { date: '2024-03-31', value: -1.716 },
    { date: '2024-06-30', value: 0.979 },
    { date: '2024-09-30', value: 1.357 },
    { date: '2024-12-31', value: 22.458 },
  ];

  it('sums four consecutive quarters and stamps the window at its close', () => {
    const out = ttm(q2024);
    expect(out).toHaveLength(1);
    expect(out[0].date).toBe('2024-12-31');
    expect(out[0].value).toBeCloseTo(23.078, 3);       // == the published annual figure
  });

  it('carries a negative quarter rather than dropping it', () => {
    // ASML's Q1 is routinely negative (−1.72, −1.21, −6.76). Dropping it would overstate the year.
    expect(ttm(q2024)[0].value).toBeLessThan(q2024.reduce((s, q) => s + Math.max(q.value, 0), 0));
  });

  it('⚠ refuses to sum across a missing quarter', () => {
    // Four consecutive ROWS are not four consecutive QUARTERS. Spanning the hole would report
    // nine months as a year — low, and indistinguishable from a bad year.
    const gapped = [
      { date: '2023-03-31', value: 1 },
      { date: '2023-06-30', value: 1 },
      { date: '2024-09-30', value: 1 },     // a year missing
      { date: '2024-12-31', value: 1 },
    ];
    expect(ttm(gapped)).toEqual([]);
  });

  it('needs four quarters before it reports anything', () => {
    expect(ttm(q2024.slice(0, 3))).toEqual([]);
  });

  it('slides one quarter at a time', () => {
    const five = [...q2024, { date: '2025-03-31', value: -1.21 }];
    expect(ttm(five).map((r) => r.date)).toEqual(['2024-12-31', '2025-03-31']);
  });
});

describe('reportedAt — no look-ahead', () => {
  const fy = [{ date: '2015-12-31', value: 10 }, { date: '2016-12-31', value: 12 }];

  it('⚠ a fiscal year is NOT usable on the day it ends', () => {
    // GuruFocus stamps the row 2015-12-31; ASML published it in late January. Priced on 5 Jan the
    // multiple would use a number the market did not have, and the whole series would look
    // cleverer than anything anyone could have traded.
    expect(reportedAt(fy, '2016-01-05')).toBeNull();
  });

  it('becomes usable once the lag has passed', () => {
    expect(reportedAt(fy, '2016-04-01')).toBe(10);
  });

  it('rolls to the newer year only after ITS lag', () => {
    expect(reportedAt(fy, '2017-01-15')).toBe(10);
    expect(reportedAt(fy, '2017-04-01')).toBe(12);
  });

  it('is null before anything has been published', () => {
    expect(reportedAt(fy, '2014-06-01')).toBeNull();
  });

  it('has a generous default so a late filer reads stale, not clairvoyant', () => {
    expect(REPORT_LAG_DAYS).toBeGreaterThanOrEqual(60);
  });
});

describe('trailingMultiples', () => {
  const closes = [
    { date: '2016-04-01', value: 100 },
    { date: '2016-04-02', value: 120 },
  ];
  const fy = [{ date: '2015-12-31', value: 10 }];

  it('is price over the figure in force at that date', () => {
    const out = trailingMultiples(closes, fy);
    expect(out.map((p) => p.value)).toEqual([10, 12]);
  });

  it('⚠ a non-positive denominator produces NO POINT, never a negative multiple', () => {
    // −20x sorts below every cheap year on any axis and reads as the bargain of the decade.
    expect(trailingMultiples(closes, [{ date: '2015-12-31', value: -4 }])).toEqual([]);
    expect(trailingMultiples(closes, [{ date: '2015-12-31', value: 0 }])).toEqual([]);
  });

  it('emits nothing before the first report — a gap, not a guess', () => {
    expect(trailingMultiples([{ date: '2015-06-01', value: 100 }], fy)).toEqual([]);
  });
});

describe('forwardSeries — published, not computed', () => {
  it('reads the vendor indicator straight through', () => {
    const rows = [m(FORWARD_PE_CODE, '2015-11-30', 23.6), m(FORWARD_PE_CODE, '2016-06-03', 22.2)];
    expect(forwardSeries(rows).map((p) => p.value)).toEqual([23.6, 22.2]);
  });

  it('drops a non-positive reading', () => {
    expect(forwardSeries([m(FORWARD_PE_CODE, '2020-01-01', 0)])).toEqual([]);
  });
});

describe('pick — a priority list, not a union', () => {
  it('takes the first spelling that answers and stops', () => {
    const rows = [
      m(QUARTERLY_FCF_CODES[0], '2024-03-31', 1),
      m(QUARTERLY_FCF_CODES[1], '2024-06-30', 99),
    ];
    expect(pick(rows, QUARTERLY_FCF_CODES)).toEqual([{ date: '2024-03-31', value: 1 }]);
  });

  it('falls through when the first is empty', () => {
    expect(pick([m(QUARTERLY_FCF_CODES[1], '2024-06-30', 5)], QUARTERLY_FCF_CODES))
      .toEqual([{ date: '2024-06-30', value: 5 }]);
  });
});

describe('thin', () => {
  const daily = Array.from({ length: 40 }, (_, i) => ({
    t: d('2024-01-01') + i * 86_400_000, value: 10 + i,
  }));

  it('keeps roughly one point a week out of a daily series', () => {
    const out = thin(daily, 7);
    expect(out.length).toBeLessThan(daily.length / 4);
  });

  it('⚠ always keeps the LAST point', () => {
    // Ending the line days short of today reads, on a valuation chart, as the multiple having
    // stopped moving.
    expect(thin(daily, 7).at(-1)).toEqual(daily.at(-1));
  });

  it('is a no-op on a series too short to thin', () => {
    expect(thin(daily.slice(0, 1))).toHaveLength(1);
  });
});

describe('since', () => {
  it('keeps only the advertised window', () => {
    const pts = [{ t: d('2014-06-01'), value: 1 }, { t: d('2015-06-01'), value: 2 }];
    expect(since(pts, 2015)).toHaveLength(1);
  });
});

// ⚠ THE BUG THIS PREVENTS RENDERED AS "disconnected lines and dots". Two independently-sampled
// series merged by timestamp share almost no timestamps, so every row holds one value and a null
// for the other — and `connectNulls={false}`, which is correct for real holes, then joins nothing.
describe('align — two series, one timeline', () => {
  it('carries each series across the other series\' timestamps', () => {
    const rows = align({
      fwd: [{ t: d('2024-01-01'), value: 20 }, { t: d('2024-01-08'), value: 21 }],
      trail: [{ t: d('2024-01-04'), value: 30 }, { t: d('2024-01-11'), value: 31 }],
    });
    // Four distinct timestamps, and after each series has started NEITHER column is null.
    expect(rows.map((r) => r.t)).toEqual(
      [d('2024-01-01'), d('2024-01-04'), d('2024-01-08'), d('2024-01-11')]);
    expect(rows[1]).toMatchObject({ fwd: 20, trail: 30 });   // fwd carried onto trail's date
    expect(rows[2]).toMatchObject({ fwd: 21, trail: 30 });   // trail carried onto fwd's date
    expect(rows.filter((r) => r.fwd == null && r.t >= d('2024-01-01'))).toHaveLength(0);
  });

  it('leaves a series null BEFORE its first observation', () => {
    const rows = align({
      fwd: [{ t: d('2024-01-01'), value: 20 }],
      trail: [{ t: d('2023-01-01'), value: 30 }],
    });
    expect(rows[0].fwd).toBeNull();          // no back-fill — we did not know it yet
  });

  it('⚠ still breaks the line across a REAL gap', () => {
    // Weekly sampling, then a two-year hole: the carry must stop, or the chart draws a confident
    // straight line through a period with no observation at all.
    const weekly = Array.from({ length: 8 }, (_, i) => ({ t: d('2024-01-01') + i * 7 * 864e5, value: 20 }));
    const rows = align({
      fwd: [...weekly, { t: d('2026-06-01'), value: 40 }],
      trail: [{ t: d('2025-06-01'), value: 30 }],
    });
    const mid = rows.find((r) => r.t === d('2025-06-01'));
    expect(mid?.fwd).toBeNull();
  });

  it('adapts the tolerance to each series\' own sampling rate', () => {
    // ⚠ The same vendor feed is weekly for one company and quarterly for another. A hardcoded
    // threshold would turn the quarterly one back into dots.
    const quarterly = [0, 91, 182, 273].map((k) => ({ t: d('2024-01-01') + k * 864e5, value: 20 }));
    const rows = align({
      fwd: quarterly,
      trail: [{ t: d('2024-02-15'), value: 30 }],   // between two quarterly points
    });
    expect(rows.find((r) => r.t === d('2024-02-15'))?.fwd).toBe(20);
  });

  it('is empty when nothing has any points', () => {
    expect(align({ fwd: [], trail: [] })).toEqual([]);
  });

  it('handles a single series alone', () => {
    const rows = align({ trail: [{ t: d('2024-01-01'), value: 30 }] });
    expect(rows).toHaveLength(1);
    expect(rows[0].trail).toBe(30);
  });
});

describe('medianSpacing', () => {
  it('measures how often a series is actually sampled', () => {
    const weekly = Array.from({ length: 5 }, (_, i) => ({ t: d('2024-01-01') + i * 7 * 864e5, value: 1 }));
    expect(medianSpacing(weekly)).toBe(7 * 864e5);
  });

  it('is null for a series too short to have a gap', () => {
    expect(medianSpacing([{ t: 1, value: 1 }])).toBeNull();
  });
});
