import { describe, expect, it } from 'vitest';
import { instrumentLabel, searchInstruments, weeklyBars, type WeeklyCell } from './weeklyReturns';

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
});
