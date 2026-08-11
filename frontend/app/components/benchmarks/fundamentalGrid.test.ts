import { describe, expect, it } from 'vitest';

import {
  aggregateRow, capOf, COL_CHAR_REM, COL_MIN_CHARS, COL_PAD_REM, fixedWidthsRem, fmtCell,
  fmtMillions, gridWidths, measureWidthRem, orderedIds, periodAxes, periodKey, periodTitle,
  weightPct,
} from './fundamentalGrid';
import type { FundamentalGridColumn, FundamentalGridRow } from '../../../lib/types/api';

const row = (name: string, v: Record<string, Record<string, number>>): FundamentalGridRow =>
  ({ company_id: 1, name, v, n: {}, fx: {} } as unknown as FundamentalGridRow);

const col = (key: string, agg: string, unit = 'millions'): FundamentalGridColumn =>
  ({ key, label: key, agg, unit } as FundamentalGridColumn);

describe('periodAxes', () => {
  it('splits the payload periods into a year axis and a per-year quarter axis', () => {
    const a = periodAxes(['2023-Q3', '2023-Q4', '2024-Q1', '2022-Q4']);
    expect(a.years).toEqual(['2022', '2023', '2024']);
    expect(a.quartersByYear['2023']).toEqual([3, 4]);
    expect(a.quartersByYear['2024']).toEqual([1]);
  });

  it('⚠ offers a quarter ONLY for the year that has it — the current year is partial', () => {
    // A TTM point needs four quarters behind it, and the live year has only what has been filed.
    // A global [Q1..Q4] axis would offer positions resolving to no data, which reads as "the index
    // has nothing here" rather than "that quarter has not happened".
    const a = periodAxes(['2025-Q1', '2025-Q2']);
    expect(a.quartersByYear['2025']).toEqual([1, 2]);
    expect(a.quartersByYear['2025']).not.toContain(4);
  });

  it('leaves annual periods with no quarter axis at all', () => {
    const a = periodAxes(['2023', '2024']);
    expect(a.years).toEqual(['2023', '2024']);
    expect(a.quartersByYear['2024']).toBeUndefined();
  });
});

describe('periodKey / periodTitle', () => {
  it('the two slider positions resolve to the payload key', () => {
    expect(periodKey('2024', null)).toBe('2024');
    expect(periodKey('2024', 3)).toBe('2024-Q3');
  });

  it('⚠ NAMES A QUARTER POSITION AS TTM, because the figure under it is twelve months', () => {
    // Labelled "Q3", a 12-month revenue reads as three months of it — ~4x what the label implies,
    // with nothing on screen to contradict it.
    expect(periodTitle('2024-Q3')).toBe('TTM → Q3 2024');
    expect(periodTitle('2024')).toBe('FY2024');
  });
});

describe('aggregateRow', () => {
  const columns = [col('revenue', 'sum'), col('roic', 'weighted_mean', 'percent')];
  const rows = [
    row('Big', { '2024': { market_cap: 900, revenue: 100, roic: 20 } }),
    row('Small', { '2024': { market_cap: 100, revenue: 50, roic: 10 } }),
  ];

  it('sums a flow into the index total', () => {
    expect(aggregateRow(rows, '2024', columns).revenue).toEqual({ value: 150, contributors: 2 });
  });

  it('⚠ CAP-WEIGHTS A RATE INSTEAD OF SUMMING IT', () => {
    // Summed, ROIC would read 30% here and ~5,000% across a real index — and the cell would print
    // it with a % sign, looking like a number rather than a category error.
    const a = aggregateRow(rows, '2024', columns).roic;
    expect(a.value).toBeCloseTo((20 * 900 + 10 * 100) / 1000, 10);   // 19, not 30
    expect(a.value).toBeCloseTo(19, 10);
  });

  it('⚠ a weighted mean renormalises over the rows that HAVE the metric, and counts them', () => {
    const partial = [
      row('Big', { '2024': { market_cap: 900, roic: 20 } }),
      row('NoRoic', { '2024': { market_cap: 100 } }),
    ];
    const a = aggregateRow(partial, '2024', columns).roic;
    expect(a.value).toBeCloseTo(20, 10);      // the 100-cap row is not a 0% drag
    expect(a.contributors).toBe(1);           // …and the count says so
  });

  it('a rate with no market cap behind it cannot be weighted, so it does not contribute', () => {
    const noCap = [row('X', { '2024': { roic: 50 } })];
    expect(aggregateRow(noCap, '2024', columns).roic).toBeUndefined();
    // The flow beside it is unaffected — one missing cap is not a reason to drop a sum.
    expect(aggregateRow([row('X', { '2024': { revenue: 5 } })], '2024', columns).revenue)
      .toEqual({ value: 5, contributors: 1 });
  });

  it('omits a column entirely when nothing in the period carries it', () => {
    expect(aggregateRow(rows, '2019', columns)).toEqual({});
  });

  it('⚠ REFUSES `none` — a share count and a per-share amount have no index-level total', () => {
    // "The S&P 500's share count" is not a quantity, and 500 companies' dividends-per-share summed
    // is a well-formed number with no referent. The per-company cells are true; the index cell is
    // a dash. ⚠ The refusal lives in aggregateRow, not the renderer, so a later caller that reads
    // only the values cannot total it by accident.
    const withNone = [
      ...columns,
      col('shares', 'none', 'shares'),
      col('div_ps', 'none', 'per_share'),
    ];
    const withCounts = [
      row('Big', { '2024': { market_cap: 900, shares: 1_000, div_ps: 2 } }),
      row('Small', { '2024': { market_cap: 100, shares: 40, div_ps: 1 } }),
    ];
    const a = aggregateRow(withCounts, '2024', withNone);
    expect(a.shares).toBeUndefined();
    expect(a.div_ps).toBeUndefined();
    // …and it is a per-column refusal, not a whole-row one.
    expect(a.market_cap).toBeUndefined();     // not in `columns` here
    expect(Object.keys(a)).toEqual([]);
    expect(aggregateRow(withCounts, '2024', [col('shares', 'sum', 'shares')]).shares)
      .toEqual({ value: 1_040, contributors: 2 });   // proves `none` is what refused it
  });
});

describe('orderedIds — the row order must not move when the sliders do', () => {
  // Deliberately chosen so ranking by 2018 and ranking by 2024 give OPPOSITE orders: if the
  // ordering ever reads the selected period, these tests fail loudly instead of subtly.
  const ids = (m: Map<number, FundamentalGridRow>) => m;
  const mk = (list: [number, string, Record<string, Record<string, number>>][]) => {
    const m = new Map<number, FundamentalGridRow>();
    for (const [id, name, v] of list) {
      const r = { company_id: id, name, ticker: name.slice(0, 3).toUpperCase(), v, n: {}, fx: {} };
      m.set(id, r as unknown as FundamentalGridRow);
    }
    return m;
  };
  const world = mk([
    [1, 'Riser', { 2018: { market_cap: 10 }, 2024: { market_cap: 900 } }],
    [2, 'Faller', { 2018: { market_cap: 800 }, 2024: { market_cap: 20 } }],
    [3, 'Steady', { 2018: { market_cap: 100 }, 2024: { market_cap: 100 } }],
  ]);

  it('⚠ THE ORDER IS THE SAME WHATEVER PERIOD IS ON SCREEN', () => {
    // The anchor is fixed; the period the reader is looking at is not an input at all. Ranking by
    // the visible period would give [1,3,2] on 2024 and [2,3,1] on 2018 — a table that reshuffles
    // under the cursor, which makes tracking one company across periods impossible.
    const opts = { anchor: '2024', sortKey: 'market_cap', dir: 'desc' as const };
    expect(orderedIds(ids(world), world, opts)).toEqual([1, 3, 2]);
    // …and nothing about scrubbing to 2018 can change it, because 2018 is nowhere in the call.
    expect(orderedIds(ids(world), world, opts)).toEqual([1, 3, 2]);
    // Proof the fixture would have exposed the bug: anchored on 2018 the order genuinely reverses.
    expect(orderedIds(ids(world), world, { ...opts, anchor: '2018' })).toEqual([2, 3, 1]);
  });

  it('⚠ KEEPS A ROW THE CURRENT BASIS CANNOT ANSWER FOR — identity and ranking are separate', () => {
    // A company with fewer than four quarters has annual lines and no TTM point. Rendering only
    // the current payload's rows would drop it out of the middle of the table on the way to Q3 and
    // put it back on the way home; here it holds its place and renders as dashes.
    const quarterlyOnly = mk([
      [1, 'Riser', { 2024: { market_cap: 900 } }],
      [3, 'Steady', { 2024: { market_cap: 100 } }],
    ]);
    const order = orderedIds(ids(world), quarterlyOnly, {
      anchor: '2024', sortKey: 'market_cap', dir: 'desc',
    });
    expect(order).toHaveLength(3);
    expect(order).toContain(2);          // present in the union, absent from this basis
    expect(order[order.length - 1]).toBe(2);   // …and unrankable sorts last, not first
  });

  it('sorts unrankable rows last in BOTH directions — absent is not a small number', () => {
    const partial = mk([[1, 'Riser', { 2024: { market_cap: 900 } }]]);
    for (const dir of ['desc', 'asc'] as const) {
      const o = orderedIds(ids(world), partial, { anchor: '2024', sortKey: 'market_cap', dir });
      expect(o[0]).toBe(1);
      expect(o.slice(1).sort()).toEqual([2, 3]);
    }
  });

  it('breaks ties on company_id, so equal values cannot swap between renders', () => {
    const tied = mk([
      [7, 'A', { 2024: { market_cap: 50 } }],
      [3, 'B', { 2024: { market_cap: 50 } }],
    ]);
    expect(orderedIds(ids(tied), tied, { anchor: '2024', sortKey: 'market_cap', dir: 'desc' }))
      .toEqual([3, 7]);
  });

  it('filters on identity without disturbing the ranking of what survives', () => {
    const o = orderedIds(ids(world), world,
      { anchor: '2024', sortKey: 'market_cap', dir: 'desc', needle: 'er' });
    expect(o).toEqual([1, 2]);   // Riser, Faller — Steady filtered out, order otherwise unchanged
  });
});

describe('gridWidths — the geometry must not depend on the data', () => {
  const labels = ['Revenue', 'Non-current liabilities', 'ROIC %'];

  it('⚠ TAKES ONLY THE HEADINGS AND THE ROLE — the period and the rows are not inputs', () => {
    // This is the whole fix. An auto-layout table sized its columns from cell CONTENT, so 2018
    // (fewer, shorter figures than 2025) produced narrower columns and headings that wrapped to
    // two lines — the header bar rebuilding as the slider moved. Nothing here can see a value.
    // The second argument is the admin-only Fetch column: shape may vary by ROLE, never by data.
    expect(gridWidths(labels, true)).toEqual(gridWidths(labels, true));
    expect(gridWidths.length).toBe(2);       // (labels, withFetch) — no rows, no period
  });

  it('gives every column at least the width of its own heading', () => {
    // 0.45rem/char is deliberately generous for text-xs: a column 20% too wide costs a little
    // scrolling, one 2% too narrow costs the wrap this exists to prevent.
    for (const label of labels) {
      expect(measureWidthRem(label)).toBeGreaterThan(label.length * COL_CHAR_REM);
    }
    expect(measureWidthRem('Non-current liabilities'))
      .toBeGreaterThan(measureWidthRem('Revenue'));
  });

  it('⚠ floors short headings at the widest FIGURE a cell can hold', () => {
    // "€181.2bn", "24,514M" and "133.0%" are all inside ten characters, so a heading shorter than
    // that must still be given ten — otherwise `Capex` gets a column its own numbers overflow.
    expect(measureWidthRem('Capex')).toBe(measureWidthRem('ROIC %'));
    expect(measureWidthRem('Capex')).toBe(COL_CHAR_REM * COL_MIN_CHARS + COL_PAD_REM);
  });

  it('⚠ EMITS ONE WIDTH PER RENDERED COLUMN, and the Fetch column is admin-only', () => {
    // A `<col>` list out of step with the columns does not fail loudly — every column after the
    // gap silently takes its neighbour's width, which looks like a styling bug rather than an
    // off-by-one. Admin: # · Company · Fetch · Exch · Ticker · Ccy · Cap · Weight. User: the same
    // without Fetch, because the ingest it fires 403s for them.
    //
    // ⚠ THESE COUNTS ARE 8/7 SINCE **Exch** WAS ADDED, and this test held 7/6 for a while after —
    // failing in the one direction that is only annoying rather than dangerous. Exchange is the
    // other half of the identifier (GuruFocus addresses a stock as `EXCHANGE:TICKER`), so it is a
    // real column and the source is what is right here.
    expect(gridWidths(labels, true).widths).toHaveLength(8 + labels.length);
    expect(gridWidths(labels, false).widths).toHaveLength(7 + labels.length);
    expect(fixedWidthsRem(true)).toHaveLength(8);
    expect(fixedWidthsRem(false)).toHaveLength(7);
  });

  it('⚠ Fetch is inserted AFTER Company, so the sticky pair keeps its widths', () => {
    // It sits between Company and Ticker. Inserting it before Company would move the `#`/Company
    // pair the sticky offsets are pinned to, and the name column would land on top of the row
    // numbers.
    const admin = fixedWidthsRem(true);
    const user = fixedWidthsRem(false);
    expect(admin.slice(0, 2)).toEqual(user.slice(0, 2));    // # and Company unchanged
    expect(admin.slice(3)).toEqual(user.slice(2));          // everything after Fetch unchanged
  });

  it('⚠ the `#` width IS the sticky offset the name column is pinned at', () => {
    // Both columns pin when the table scrolls sideways. `Company` carries `left-[3rem]`, so if
    // this width ever changes without that class changing with it, the name slides over the row
    // numbers and hides them — a silent overlap, not a broken layout.
    expect(fixedWidthsRem(true)[0]).toBe(3);
    expect(fixedWidthsRem(false)[0]).toBe(3);
  });

  it('totals to the sum, because a fixed-layout table needs a definite width', () => {
    // With `width: auto` a fixed table distributes the CONTAINER's width and ignores the colgroup
    // entirely — the bug would come straight back.
    for (const withFetch of [true, false]) {
      const { widths, total } = gridWidths(labels, withFetch);
      expect(total).toBeCloseTo(widths.reduce((a, b) => a + b, 0), 10);
      expect(total).toBeGreaterThan(0);
    }
  });
});

describe('weightPct', () => {
  it('is the period’s own cap over the period’s own total', () => {
    const r = row('A', { '2024': { market_cap: 250 } });
    expect(weightPct(r, '2024', 1000)).toBeCloseTo(25, 10);
  });

  it('refuses rather than dividing by a total that is missing or zero', () => {
    const r = row('A', { '2024': { market_cap: 250 } });
    expect(weightPct(r, '2024', 0)).toBeNull();
    expect(weightPct(r, '2024', null)).toBeNull();
    expect(weightPct(row('B', { '2024': {} }), '2024', 1000)).toBeNull();
  });

  it('⚠ the priced rows sum to 100% — the denominator is the AVAILABLE caps, not the index', () => {
    // The defining property of this column as specified: cap ÷ Σ available caps. A constituent we
    // cannot price does not dilute anyone, it inflates everybody else pro rata — which is exactly
    // why the header names it a share of the Total row rather than the index's weight.
    const priced = [
      row('A', { '2024': { market_cap: 600 } }),
      row('B', { '2024': { market_cap: 300 } }),
      row('C', { '2024': { market_cap: 100 } }),
    ];
    const unpriced = row('D', { '2024': {} });
    const total = 1000;
    const sum = [...priced, unpriced]
      .reduce((s, r) => s + (weightPct(r, '2024', total) ?? 0), 0);
    expect(sum).toBeCloseTo(100, 10);
    expect(weightPct(priced[0], '2024', total)).toBeCloseTo(60, 10);
    // ⚠ The unpriced row is NULL, never 0 — a 0% would read as a real but negligible holding.
    expect(weightPct(unpriced, '2024', total)).toBeNull();
  });

  it('⚠ reads the SELECTED period’s cap, never the latest one', () => {
    // Weighting an old cross-section by today's cap is the look-ahead bias the whole grid exists
    // to avoid: a company that tripled would retroactively get three times the index share.
    const r = row('A', { '2018': { market_cap: 100 }, '2024': { market_cap: 300 } });
    expect(capOf(r, '2018')).toBe(100);
    expect(weightPct(r, '2018', 1000)).toBeCloseTo(10, 10);
  });
});

describe('fmtMillions / fmtCell', () => {
  it('⚠ treats the input as MILLIONS — 181,171 is €181.2bn, not €181,171', () => {
    expect(fmtMillions(181_171)).toBe('€181.2bn');
    expect(fmtMillions(4_130_734)).toBe('€4.13tn');
    expect(fmtMillions(842)).toBe('€842M');
    expect(fmtMillions(-2_500)).toBe('−€2.5bn');
  });

  it('renders each unit in its own shape, from the DECLARED unit', () => {
    expect(fmtCell(132.98, 'percent')).toBe('133.0%');
    expect(fmtCell(24_514, 'shares')).toBe('24,514M');
    expect(fmtCell(1.235, 'per_share')).toBe('€1.24');
    expect(fmtCell(181_171, 'millions')).toBe('€181.2bn');
  });

  it('an absent value is a dash, never a zero', () => {
    // A 0 in a fundamentals cell is a claim about the company; a dash is a statement about us.
    expect(fmtCell(null, 'millions')).toBe('—');
    expect(fmtCell(undefined, 'percent')).toBe('—');
    expect(fmtMillions(Number.NaN)).toBe('—');
  });
});
