import { describe, expect, it } from 'vitest';
import { perfByPeriod } from './perfStats';

// Build a daily index level series from a per-year constant daily return, so the
// expected stats are analytic. 252 days/year.
function series(perYear: { year: number; dailyRet: number; days?: number }[]): {
  dates: string[];
  level: number[];
} {
  const dates: string[] = [];
  const level: number[] = [];
  let lvl = 100;
  // seed a day-0 anchor (dropped by the return loop) dated the first year.
  dates.push(`${perYear[0].year}-01-01`);
  level.push(lvl);
  for (const { year, dailyRet, days = 252 } of perYear) {
    for (let d = 0; d < days; d++) {
      lvl *= 1 + dailyRet;
      // pad month/day so slice(0,4) yields the year; exact calendar date is irrelevant.
      dates.push(`${year}-06-${String((d % 27) + 1).padStart(2, '0')}`);
      level.push(lvl);
    }
  }
  return { dates, level };
}

describe('perfByPeriod', () => {
  it('groups by calendar year', () => {
    const { dates, level } = series([
      { year: 2020, dailyRet: 0.001 },
      { year: 2021, dailyRet: -0.0005 },
    ]);
    const rows = perfByPeriod(dates, level, 'year');
    expect(rows.map((r) => r.label)).toEqual(['2020', '2021']);
    // 2020 positive CAGR, 2021 negative.
    expect(rows[0].ret).toBeGreaterThan(0);
    expect(rows[1].ret).toBeLessThan(0);
  });

  it('CAGR ≈ full-year compounded return for a 252-day constant-return year', () => {
    const { dates, level } = series([{ year: 2020, dailyRet: 0.0004 }]);
    const [row] = perfByPeriod(dates, level, 'year');
    const expected = (1 + 0.0004) ** 252 - 1;
    expect(row.ret).toBeCloseTo(expected, 4);
  });

  it('a zero-vol constant-return bucket has ~0 vol and null ratios', () => {
    const { dates, level } = series([{ year: 2020, dailyRet: 0.0004 }]);
    const [row] = perfByPeriod(dates, level, 'year');
    expect(row.vol).toBeCloseTo(0, 6);
    expect(row.sharpe).toBeNull();   // zero vol → undefined ratio
    expect(row.sortino).toBeNull();  // no down days → undefined ratio
  });

  it('buckets into 5- and 10-year blocks aligned to multiples', () => {
    const { dates, level } = series([
      { year: 2013, dailyRet: 0.0002 },
      { year: 2016, dailyRet: 0.0002 },
      { year: 2022, dailyRet: 0.0002 },
    ]);
    expect(perfByPeriod(dates, level, '5y').map((r) => r.label)).toEqual([
      '2010–2014', '2015–2019', '2020–2024',
    ]);
    expect(perfByPeriod(dates, level, '10y').map((r) => r.label)).toEqual([
      '2010–2019', '2020–2029',
    ]);
  });

  it('positive Sharpe/Sortino for a rising, mildly volatile bucket', () => {
    // alternate +1.2% / -0.8% → net upward drift with real volatility.
    const dates: string[] = ['2020-01-01'];
    const level: number[] = [100];
    let lvl = 100;
    for (let d = 0; d < 252; d++) {
      lvl *= 1 + (d % 2 === 0 ? 0.012 : -0.008);
      dates.push(`2020-06-${String((d % 27) + 1).padStart(2, '0')}`);
      level.push(lvl);
    }
    const [row] = perfByPeriod(dates, level, 'year');
    expect(row.sharpe).not.toBeNull();
    expect(row.sharpe!).toBeGreaterThan(0);
    expect(row.sortino).not.toBeNull();
    // Sortino only penalizes downside, so it exceeds Sharpe here.
    expect(row.sortino!).toBeGreaterThan(row.sharpe!);
  });

  it('returns [] for a too-short series', () => {
    expect(perfByPeriod(['2020-01-01'], [100], 'year')).toEqual([]);
  });
});
