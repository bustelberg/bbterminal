import { describe, expect, it } from 'vitest';

import {
  compareInstruments, seriesPoints, sourceLabel, sparkPath, windowReturnPct, ytdStart,
  type SeriesBlock,
} from './correlationSeries';

const BLOCK: SeriesBlock = {
  dates: ['2026-01-02', '2026-01-05', '2026-01-06', '2026-01-07'],
  values: {
    'a:1': [10, null, 12, 14],      // a gap: this venue did not trade on the 5th
    'a:2': [100, 101, 102, 103],
    'p:7': [100, 100, 100, 100],    // a look-through index, flat
  },
};

describe('seriesPoints', () => {
  it('drops a gap rather than reading it as zero', () => {
    // ⚠ THE BUG THIS EXISTS TO PREVENT. `col[i] ?? 0` turns a Japanese holiday into a 100% crash
    // and back, on a chart nobody would think to distrust.
    const pts = seriesPoints(BLOCK, 'a:1');
    expect(pts.map((p) => p.value)).toEqual([10, 12, 14]);
    expect(pts.map((p) => p.date)).toEqual(['2026-01-02', '2026-01-06', '2026-01-07']);
  });

  it('slices from the window start, inclusive', () => {
    expect(seriesPoints(BLOCK, 'a:2', '2026-01-06').map((p) => p.date))
      .toEqual(['2026-01-06', '2026-01-07']);
  });

  it('is empty for an unpriced row rather than throwing', () => {
    // An unpriced instrument carries series_key === null; the table still renders its row.
    expect(seriesPoints(BLOCK, null)).toEqual([]);
    expect(seriesPoints(BLOCK, 'a:404')).toEqual([]);
  });
});

describe('ytdStart', () => {
  it('is 1 January of the as-of year', () => {
    expect(ytdStart('2026-08-10')).toBe('2026-01-01');
  });
});

describe('sparkPath', () => {
  it('spans the box and puts the high at the top', () => {
    const d = sparkPath([{ date: 'a', value: 1 }, { date: 'b', value: 3 }], 100, 20, 1);
    expect(d).toBe('M0.00,19.00 L100.00,1.00');   // y grows downward, so the high is the small y
  });

  it('draws a flat series on the centre line instead of dividing by zero', () => {
    const d = sparkPath([{ date: 'a', value: 5 }, { date: 'b', value: 5 }], 100, 20, 1);
    expect(d).toBe('M0.00,10.00 L100.00,10.00');
  });

  it('is empty for no points', () => {
    expect(sparkPath([], 100, 20)).toBe('');
  });

  it('scales per row, so two rows at different price levels look the same shape', () => {
    // The point of per-row scaling: a EUR 2 share and a EUR 2,000 share that both doubled
    // produce the SAME path. One shared scale would flatten the cheap one into a line.
    const cheap = sparkPath([{ date: 'a', value: 1 }, { date: 'b', value: 2 }], 50, 10);
    const dear = sparkPath([{ date: 'a', value: 1000 }, { date: 'b', value: 2000 }], 50, 10);
    expect(cheap).toBe(dear);
  });
});

describe('windowReturnPct', () => {
  it('is the point-to-point change', () => {
    expect(windowReturnPct([{ date: 'a', value: 100 }, { date: 'b', value: 110 }]))
      .toBeCloseTo(10, 9);
  });

  it('is null on a single observation, never 0', () => {
    // ⚠ A quiet 0% is how a thin listing passes for a stable one.
    expect(windowReturnPct([{ date: 'a', value: 100 }])).toBeNull();
    expect(windowReturnPct([])).toBeNull();
  });

  it('refuses a non-positive base rather than returning Infinity', () => {
    expect(windowReturnPct([{ date: 'a', value: 0 }, { date: 'b', value: 5 }])).toBeNull();
  });
});

describe('compareInstruments', () => {
  const rows = [
    { isin: 'A', name: 'Alpha', in_portfolios: 3, weight_pct_sum: 10, med_adv_eur: 5_000 },
    { isin: 'B', name: 'Beta', in_portfolios: 1, weight_pct_sum: 50, med_adv_eur: 9_000_000 },
    { isin: 'C', name: 'Gamma', in_portfolios: 2, weight_pct_sum: 20, med_adv_eur: null },
  ];
  const none = () => null;

  it('sorts by models holding it, descending', () => {
    const out = [...rows].sort((a, b) => compareInstruments(a, b, 'holdings', true, none));
    expect(out.map((r) => r.isin)).toEqual(['A', 'C', 'B']);
  });

  it('puts an absent liquidity at the bottom in BOTH directions', () => {
    // ⚠ THE ASSERTION THAT MATTERS. A missing ADV is not "the least liquid" — sorted ascending it
    // would take the top of the table and read as the thinnest listing in the book.
    const asc = [...rows].sort((a, b) => compareInstruments(a, b, 'liquidity', false, none));
    const desc = [...rows].sort((a, b) => compareInstruments(a, b, 'liquidity', true, none));
    expect(asc[asc.length - 1].isin).toBe('C');
    expect(desc[desc.length - 1].isin).toBe('C');
    expect(asc.map((r) => r.isin)).toEqual(['A', 'B', 'C']);
    expect(desc.map((r) => r.isin)).toEqual(['B', 'A', 'C']);
  });

  it('sorts unpriced rows (no return) to the bottom too', () => {
    const ret = (r: { isin: string }) => (r.isin === 'C' ? null : r.isin === 'A' ? 5 : -2);
    const desc = [...rows].sort((a, b) => compareInstruments(a, b, 'return', true, ret));
    expect(desc.map((r) => r.isin)).toEqual(['A', 'B', 'C']);
  });

  it('breaks ties by name so the order is stable, not arbitrary', () => {
    const tied = [
      { isin: 'Z', name: 'Zulu', in_portfolios: 2, weight_pct_sum: 1, med_adv_eur: 1 },
      { isin: 'M', name: 'Mike', in_portfolios: 2, weight_pct_sum: 1, med_adv_eur: 1 },
    ];
    const out = [...tied].sort((a, b) => compareInstruments(a, b, 'holdings', true, none));
    expect(out.map((r) => r.name)).toEqual(['Mike', 'Zulu']);
  });
});

describe('sourceLabel', () => {
  it('names BOTH vendors when a conversion happened', () => {
    // ⚠ A EUR level for a USD holding is a yfinance close TIMES an ECB rate. Reporting only the
    // price vendor answers "which source?" with half the truth.
    const { short, title } = sourceLabel({ price_source: 'yfinance', fx_source: 'ECB' });
    expect(short).toBe('yfinance + ECB');
    expect(title).toContain('yfinance');
    expect(title).toContain('ECB');
  });

  it('names one vendor for a EUR holding, without padding it out to look like the others', () => {
    // No conversion happens, so crediting a second vendor would be inventing a step.
    expect(sourceLabel({ price_source: 'yfinance', fx_source: null }).short).toBe('yfinance');
  });

  it('says basket for a look-through rather than picking one currency', () => {
    const { short, title } = sourceLabel({ price_source: 'yfinance', fx_source: 'per holding' });
    expect(short).toBe('yfinance · basket');
    expect(title).toContain('own rate');
  });

  it('is a dash for an unpriced row, not a vendor that supplied nothing', () => {
    expect(sourceLabel({ price_source: null, fx_source: null }).short).toBe('—');
  });

  it('carries the vendor through rather than hard-coding it', () => {
    // If this path ever DID read GuruFocus, the column must say so — the label is data, not a
    // constant string in the UI.
    expect(sourceLabel({ price_source: 'gurufocus', fx_source: 'ECB' }).short)
      .toBe('gurufocus + ECB');
  });
});
