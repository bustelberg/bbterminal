import { describe, expect, it } from 'vitest';
import { instrumentLabel, searchInstruments, weeklyBars, visibleWeeklyReturns, periodReturn, fullHistoryReturn, weeklyCorrelation, type WeeklyCell, type WeeklyReturns } from './weeklyReturns';

describe('Instrument-only weekly returns', () => {
  const options = [
    { id: 'A|USD', name: 'Adobe', isin: 'US00724F1012', currency: 'USD' },
    { id: 'B|EUR', name: 'Sold fund', isin: 'IE000EXAMPLE', currency: 'EUR' },
  ];
  it('searches the full catalogue by name, ISIN and currency, including sold instruments', () => {
    expect(searchInstruments(options, 'adob')).toEqual([options[0]]);
    expect(searchInstruments(options, 'ie000')).toEqual([options[1]]);
    expect(searchInstruments(options, 'EUR')).toEqual([options[1]]);
    expect(searchInstruments(options, '')).toEqual(options);
    expect(instrumentLabel(options[0])).toContain('US00724F1012');
  });
  it('preserves missing bars, genuine zero and negative returns', () => {
    const cells = [null, 0, -2.5, 3.5, Infinity].map((return_pct) => ({ return_pct, price: 100, status: 'priced' } as WeeklyCell));
    expect(weeklyBars(cells)).toEqual([null, 0, -2.5, 3.5, null]);
  });
  const data = (): WeeklyReturns => ({
    portefeuille: 'BOOK', start_date: '2026-01-01', through: '2026-01-18', missing_observations: 0, options: [],
    periods: ['2026-01-04', '2026-01-11', '2026-01-18'].map((target_date, index) => ({
      target_date, week_end: target_date, as_of_date: target_date,
      from_date: ['2025-12-31', '2026-01-04', '2026-01-11'][index], partial: false, available: true,
    })),
    selected: { id: 'A', name: 'Adobe', isin: 'A', currency: 'USD', held: true,
      weeks: [
        { status: 'priced', from_price: 100, price: 110, return_pct: 10 },
        { status: 'priced', from_price: 110, price: 99, return_pct: -10 },
        { status: 'priced', from_price: 99, price: 99, return_pct: 0 },
      ] },
  });
  it('calculates YTD and selected windows from their boundary quotes rather than adding returns', () => {
    expect(periodReturn(data())?.value).toBeCloseTo(-1);
    expect(periodReturn(data(), ['2026-01-11', '2026-01-18'])).toEqual({ value: expect.closeTo(-10), from: '2026-01-04', to: '2026-01-18' });
    expect(periodReturn(data(), ['2026-01-18', '2026-01-18'])?.value).toBe(0);
  });
  it('matches selected calendar weeks but still requires valid boundary prices', () => {
    const sample = data();
    expect(periodReturn(sample, ['2026-01-03', '2026-01-18'])?.value).toBeCloseTo(-1);
    expect(periodReturn(sample, ['2025-12-21', '2026-01-18'])).toBeNull();
    sample.selected!.weeks[0].from_price = null;
    expect(periodReturn(sample)).toBeNull();
    expect(periodReturn(sample, ['2026-01-11', '2026-01-18'])?.value).toBeCloseTo(-10);
    sample.selected!.weeks[2].price = NaN;
    expect(periodReturn(sample, ['2026-01-11', '2026-01-18'])).toBeNull();
    expect(periodReturn(undefined)).toBeNull();
  });
  it('can compare valid boundary quotes even when an intermediate weekly quote is missing', () => {
    const sample = data();
    sample.selected!.weeks[1] = { status: 'missing_price', from_price: 110, price: null, return_pct: null };
    expect(periodReturn(sample)?.value).toBeCloseTo(-1);
  });
  it('full history uses actual first and last quotes while retaining unheld or missing weeks', () => {
    const sample = data();
    sample.selected!.weeks[0].from_price = null;
    sample.selected!.weeks[2].price = null;
    const result = fullHistoryReturn(sample);
    expect(result).toEqual({ value: expect.closeTo(-10), from: '2026-01-04', to: '2026-01-11' });
    expect(sample.selected!.weeks[2].price).toBeNull();
    expect(fullHistoryReturn(undefined)).toBeNull();
  });
  it('draws only actual non-zero returns, with dates and quote cells aligned', () => {
    const sample = data();
    sample.selected!.weeks[0] = { status: 'missing_price', return_pct: null, price: null };
    const visible = visibleWeeklyReturns(sample);
    expect(visible.periods.map(period => period.target_date)).toEqual(['2026-01-11']);
    expect(visible.selected!.weeks.map(cell => cell.return_pct)).toEqual([-10]);
    expect(sample.periods).toHaveLength(3);
  });
  it('calculates positive and negative correlation from paired weekly returns', () => {
    const a = data(), b = data();
    expect(weeklyCorrelation(a, b)).toEqual({ value: expect.closeTo(1), count: 3 });
    b.selected!.weeks.forEach((cell) => { cell.return_pct = -cell.return_pct!; });
    expect(weeklyCorrelation(a, b).value).toBeCloseTo(-1);
  });
  it('excludes missing, mismatched and out-of-window observations', () => {
    const a = data(), b = data();
    b.periods[0].as_of_date = '2026-01-03';
    expect(weeklyCorrelation(a, b)).toEqual({ value: null, count: 2 });
    b.selected!.weeks[1].return_pct = null;
    expect(weeklyCorrelation(a, b)).toEqual({ value: null, count: 1 });
    expect(weeklyCorrelation(a, a, ['2026-01-11', '2026-01-18'])).toEqual({ value: null, count: 2 });
    expect(weeklyCorrelation(null, a)).toEqual({ value: null, count: 0 });
  });
  it('does not invent a correlation for a constant series', () => {
    const a = data(), b = data();
    b.selected!.weeks.forEach((cell) => { cell.return_pct = 0; });
    expect(weeklyCorrelation(a, b)).toEqual({ value: null, count: 3 });
    b.selected!.weeks.forEach((cell) => { cell.return_pct = 3.3; });
    expect(weeklyCorrelation(a, b)).toEqual({ value: null, count: 3 });
  });

  function weekdaySeries(dates: string[], returns: number[]): WeeklyReturns {
    let price = 100;
    return { ...data(), start_date: dates[0], through: dates.at(-1)!,
      periods: returns.map((_, index) => ({ target_date: dates[index + 1], week_end: dates[index + 1],
        from_date: dates[index], as_of_date: dates[index + 1], partial: false, available: true })),
      selected: { ...data().selected!, weeks: returns.map(return_pct => {
        const from_price = price;
        price *= 1 + return_pct / 100;
        return { status: 'priced', from_price, price, return_pct };
      }) },
    };
  }

  it('correlates overlapping Sunday and Wednesday histories inside a longer selection', () => {
    const fund = weekdaySeries(['2025-06-15', '2025-06-22', '2025-06-29', '2025-07-06', '2025-07-13'], [1, 0, -3, 4]);
    const stock = weekdaySeries(['2025-06-11', '2025-06-18', '2025-06-25', '2025-07-02', '2025-07-09'], [2, 0, -6, 8]);
    expect(weeklyCorrelation(fund, stock, ['2025-02-16', '2025-12-28']))
      .toEqual({ value: expect.closeTo(1), count: 4 });
    expect(weeklyCorrelation(stock, fund, ['2025-02-16', '2025-12-28']))
      .toEqual({ value: expect.closeTo(1), count: 4 });
    expect(periodReturn(fund, ['2025-02-16', '2025-12-28'])).toBeNull();
    const selected = periodReturn(stock, ['2025-06-22', '2025-07-13']);
    expect(selected?.from).toBe('2025-06-11');
    expect(selected?.to).toBe('2025-07-09');
    expect(selected?.value).toBeCloseTo(((1.02 * 1 * .94 * 1.08) - 1) * 100);
  });

  it('does not pair a multiweek return with a single-week return or fill missing prices', () => {
    const a = weekdaySeries(['2025-06-15', '2025-06-22', '2025-06-29', '2025-07-06'], [1, 2, 3]);
    const b = weekdaySeries(['2025-06-11', '2025-06-18', '2025-06-25', '2025-07-02'], [2, 4, 6]);
    b.periods[1].from_date = '2025-06-04';
    expect(weeklyCorrelation(a, b)).toEqual({ value: null, count: 2 });
    b.selected!.weeks[2].return_pct = null;
    expect(weeklyCorrelation(a, b)).toEqual({ value: null, count: 1 });
  });
});
