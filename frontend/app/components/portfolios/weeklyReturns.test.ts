import { describe, expect, it } from 'vitest';
import { instrumentLabel, searchInstruments, weeklyBars, periodReturn, type WeeklyCell, type WeeklyReturns } from './weeklyReturns';

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
  it('requires exact window boundaries and valid opening and closing observations', () => {
    const sample = data();
    expect(periodReturn(sample, ['2026-01-03', '2026-01-18'])).toBeNull();
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
});
