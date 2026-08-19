import { describe, expect, it } from 'vitest';
import {
  benchBody, benchKey, benchNote, isUniverseTarget, mergeSeries, rebaseSeries, seriesCrossesZero,
  spliceCaps, withBench, type BenchTarget,
} from './benchSeries';
import { weightAt } from './marginData';

/**
 * The pure halves of the Long Equity benchmark overlay.
 *
 * `rebaseSeries` is the one that can lie. The ratio cards need nothing (a margin is a % on both
 * sides); the LEVEL cards put a company's EUR millions beside an index and would otherwise be two
 * scales on one axis. Indexing BOTH to 100 at the first SHARED year is what makes that honest —
 * the anchor has to be shared, or the two lines start in different years and the gap between them
 * is an artefact of the anchor rather than of the businesses.
 */

const m = (o: Record<number, number | null>) => new Map(
  Object.entries(o).map(([k, v]) => [Number(k), v] as [number, number | null]));

describe('mergeSeries', () => {
  it('spans the UNION of periods, so the index keeps its own history', () => {
    const rows = mergeSeries(m({ 2021: 5 }), m({ 2019: 1, 2021: 3 }), 'margin');
    expect(rows.map((r) => r.year)).toEqual([2019, 2021]);
    expect(rows[0]).toEqual({ year: 2019, margin: null, bench: 1 });
    expect(rows[1]).toEqual({ year: 2021, margin: 5, bench: 3 });
  });

  it('emits no bench key at all when no benchmark is selected', () => {
    const rows = mergeSeries(m({ 2021: 5 }), null, 'ratio');
    expect(rows).toEqual([{ year: 2021, ratio: 5 }]);
    expect('bench' in rows[0]).toBe(false);
  });

  it('sorts ascending — recharts draws in array order, not by x', () => {
    const rows = mergeSeries(m({ 2022: 1, 2020: 2, 2021: 3 }), null);
    expect(rows.map((r) => r.year)).toEqual([2020, 2021, 2022]);
  });
});

describe('benchNote', () => {
  const aex: BenchTarget = { universe: 'AEX', label: 'AEX', cadence: 'annual' };
  // ⚠ TWO PERIODS, NOT ONE — A ONE-POINT SERIES IS NO LONGER "DREW". This fixture was
  // `m({ 2020: 5 })`, which `benchNote` now reports as "one period only", so the test below broke
  // on a source change that was correct: a single surviving point is not a line you can read a
  // trend off, and saying so is the whole job of this function. The one-period case gets its own
  // assertion under "keeps the absences apart", where it belongs.
  const drawn = m({ 2019: 4, 2020: 5 });

  it('says nothing when no benchmark is selected', () => {
    expect(benchNote(null, null, null, null)).toBeNull();
  });

  it('says nothing when the line drew', () => {
    expect(benchNote(aex, { rows: [] }, null, drawn)).toBeNull();
  });

  it('⚠ a ONE-period line is an absence too — it draws, and it says nothing readable', () => {
    // It renders as a single dot beside a full portfolio curve, which looks like a benchmark that
    // simply tracks nothing rather than one where every other period fell under the coverage floor.
    expect(benchNote(aex, { rows: [] }, null, m({ 2020: 5 })))
      .toBe('AEX: one period only — the rest fall under the 50% coverage floor');
  });

  it('keeps the three absences apart — they have different fixes', () => {
    // ⚠ This is the whole point. On screen all three are "no second line", and a reader who
    // cannot tell them apart will read a failed request as an index that tracks the book.
    expect(benchNote(aex, null, null, null)).toBe('AEX: loading…');
    expect(benchNote(aex, null, 'no holdings', null)).toBe('AEX: no holdings');
    expect(benchNote(aex, { rows: [] }, null, m({}))).toBe(
      'AEX: no period clears the 50% coverage floor');
  });

  it('calls out a single period, which otherwise reads as a rendering glitch', () => {
    // Measured on "Interest / op. profit": a bank reports no operating income at all, so the
    // financials' weight sits in the denominator uncounted and AEX clears the floor in exactly
    // one year of twelve. A lone dot with no explanation looks like a bug in the chart.
    expect(benchNote(aex, { rows: [] }, null, m({ 2025: 12.3 }))).toBe(
      'AEX: one period only — the rest fall under the 50% coverage floor');
  });

  it('reports the error even once a stale series is still in hand', () => {
    expect(benchNote(aex, { rows: [] }, 'HTTP 500', drawn)).toBe('AEX: HTTP 500');
  });
});

describe('withBench', () => {
  it('feeds the y-domain BOTH series — a benchmark drawn off-axis is worse than none', () => {
    expect(withBench([1, 2], m({ 2020: 40 })).sort((a, b) => a - b)).toEqual([1, 2, 40]);
  });

  it('drops nulls, because the domain helper takes numbers', () => {
    expect(withBench([1, null], m({ 2020: null, 2021: 9 }))).toEqual([1, 9]);
  });

  it('is just our own values when nothing is selected', () => {
    expect(withBench([3, 4], null)).toEqual([3, 4]);
  });
});

/**
 * `rebaseSeries` is what the four LEVEL cards now plot: both lines indexed to 100 on a shared
 * anchor, with the actual values moved to the hover. It replaced `rebaseOnto` (since removed, once
 * nothing called it), so it inherits the rule that one existed for — a shared anchor — plus one
 * that helper never had to face: it divides by BOTH series' base, not just the benchmark's, so a
 * zero or negative base is its own failure mode.
 */
describe('rebaseSeries', () => {
  it('indexes both lines to 100 at the first shared year', () => {
    const own = m({ 2020: 500, 2021: 600 });
    const bench = m({ 2020: 100, 2021: 110 });
    const out = rebaseSeries(own, bench)!;
    expect(out.anchor).toBe(2020);
    expect(out.own.get(2020)).toBe(100);
    expect(out.bench?.get(2020)).toBe(100);
    expect(out.own.get(2021)).toBe(120);          // +20%
    expect(out.bench?.get(2021)).toBeCloseTo(110, 12);
  });

  it('anchors on the first SHARED year, not on each series own first point', () => {
    // Ours starts in 2020. Anchoring the index on its own 2018 would compare a five-year path
    // against a two-year one and read the difference as performance.
    const own = m({ 2020: 500, 2021: 600 });
    const bench = m({ 2018: 50, 2019: 80, 2020: 100, 2021: 110 });
    const out = rebaseSeries(own, bench)!;
    expect(out.anchor).toBe(2020);
    expect(out.bench?.get(2018)).toBe(50);        // earlier history kept, on the 2020 base
    expect(out.bench?.get(2020)).toBe(100);
  });

  it('preserves growth exactly — indexing is a scale, never a reshape', () => {
    const out = rebaseSeries(m({ 2020: 7, 2021: 9.1, 2022: 11.83 }), null)!;
    expect((out.own.get(2021) as number) / (out.own.get(2020) as number)).toBeCloseTo(1.3, 12);
    expect((out.own.get(2022) as number) / (out.own.get(2021) as number)).toBeCloseTo(1.3, 12);
  });

  it('refuses a NEGATIVE base rather than flipping the series', () => {
    // FCF/share genuinely goes negative. Dividing by it inverts the line and the chart still
    // renders — a company recovering from negative FCF would appear to collapse.
    expect(rebaseSeries(m({ 2020: -5, 2021: 10 }), null)?.anchor).toBe(2021);
    expect(rebaseSeries(m({ 2020: -5 }), null)).toBeNull();
  });

  it('refuses a ZERO base — this is what cost the dividend-per-share card', () => {
    expect(rebaseSeries(m({ 2020: 0 }), null)).toBeNull();
  });

  it('refuses when the two share no year with both values positive', () => {
    expect(rebaseSeries(m({ 2020: 500 }), m({ 2010: 100 }))).toBeNull();
    expect(rebaseSeries(m({ 2020: 500 }), m({ 2020: 0, 2021: 100 }))).toBeNull();
  });

  it('indexes a lone series when no benchmark is selected', () => {
    const out = rebaseSeries(m({ 2019: 200, 2020: 250 }), null)!;
    expect(out.anchor).toBe(2019);
    expect(out.own.get(2020)).toBe(125);
    expect(out.bench).toBeNull();
  });
});

describe('seriesCrossesZero', () => {
  /**
   * ⚠⚠ THE BUG THIS CLOSES: "do not index it" and "do not put it on a log axis" were two separate
   * decisions, and only the first was made. `rebaseSeries` refused a sign-changing series, the card
   * fell back to absolute values and SAID SO in the legend — and then plotted them on a log axis,
   * which nulls everything ≤ 0. The fallback promised the real numbers and hid exactly the ones
   * that had triggered it: AMD's 2015-16 losses and Intel's 2024 were invisible either way, and the
   * line just appeared to start late.
   */
  it('is false for a series that never goes non-positive', () => {
    expect(seriesCrossesZero([3.21, 5.4, 24.71])).toBe(false);
  });

  it('is TRUE for a company with a loss year (AMD 2015-16, Intel 2024)', () => {
    expect(seriesCrossesZero([-0.741, -0.35, 0.3, 4.17])).toBe(true);
    expect(seriesCrossesZero([2.49, 1.2, -0.13, 0.42])).toBe(true);
  });

  it('treats an exact zero as a crossing — a log axis cannot draw it either', () => {
    expect(seriesCrossesZero([1, 0, 2])).toBe(true);
  });

  it('treats a hole as unplottable too, which is the same axis decision', () => {
    // Not a sign change, but equally impossible on a log axis — and `!(v > 0)` covers both without
    // a second rule to keep in step.
    expect(seriesCrossesZero([1, null, 2])).toBe(true);
    expect(seriesCrossesZero([1, undefined, 2])).toBe(true);
    expect(seriesCrossesZero([1, NaN, 2])).toBe(true);
  });

  it('an empty series does not claim to cross anything', () => {
    expect(seriesCrossesZero([])).toBe(false);
  });

  it('⚠ agrees with rebaseSeries: anything it flags cannot be indexed against itself', () => {
    // The two must not disagree — that disagreement IS the bug. A series with no positive year has
    // no anchor at all, so `rebaseSeries` refuses and the axis must go linear.
    const vals = [-2, -1, -0.5];
    expect(seriesCrossesZero(vals)).toBe(true);
    expect(rebaseSeries(m({ 2020: -2, 2021: -1, 2022: -0.5 }), null)).toBeNull();
  });
});

describe('spliceCaps', () => {
  /**
   * The index's per-period caps are fetched ONCE and put back on the rows here, because shipping
   * them on every row of all ten card responses was 29.9% of each ACWI payload — the same table
   * ten times. What can go wrong is not "the caps are missing" (visible) but "the caps are missing
   * for THIS row" (invisible): `weightAt` reads `{}` and `undefined` as two different answers, and
   * both draw a line.
   */
  const rows = {
    years: ['2019'],
    rows: [{ isin: 'A', weight_pct: 4 }, { isin: 'B', weight_pct: 6 }],
  };

  it('gives a row we hold no cap for an EMPTY table, not a missing one', () => {
    const out = spliceCaps(rows, { A: { 2019: 100 }, B: {} }) as typeof rows &
      { rows: { market_cap_by_period?: Record<string, number> }[] };
    expect(out.rows[0].market_cap_by_period).toEqual({ 2019: 100 });
    expect(out.rows[1].market_cap_by_period).toEqual({});
    // ⚠ THE POINT OF THE DISTINCTION, asserted through the consumer rather than the shape:
    // `{}` puts B out of that period's average; `weight_pct` would have kept it in at 6.
    expect(weightAt(out.rows[0] as never, '2019')).toBe(100);
    expect(weightAt(out.rows[1] as never, '2019')).toBe(null);
  });

  it('leaves rows untouched when the whole table is empty — the portfolio shape', () => {
    const out = spliceCaps(rows, {});
    expect(out).toBe(rows);
    // No key at all => `weightAt` falls back to the holding weight, flat across every period.
    expect(weightAt(rows.rows[0] as never, '2019')).toBe(4);
  });

  it('does not invent a row shape it was not given', () => {
    expect(spliceCaps({ metrics: [] }, { A: { 2019: 1 } })).toEqual({ metrics: [] });
  });
});

describe('the second line can be an index or a company', () => {
  const idx: BenchTarget = { universe: 'ACWI', label: 'ACWI', cadence: 'annual' };
  const co: BenchTarget = { isin: 'US67066G1040', label: 'NVIDIA Corporation', cadence: 'annual' };

  it('sends a company as a ONE-HOLDING BOOK, which is the shape the endpoints already serve', () => {
    // ⚠ This is the whole reason company-vs-company needed no backend work. Verified against the
    // real endpoints: `{holdings:[{isin, weight:1}]}` returns one row at weight_pct = 100.
    expect(JSON.parse(benchBody(co))).toEqual({
      holdings: [{ isin: 'US67066G1040', name: 'NVIDIA Corporation', weight: 1 }],
      cadence: 'annual',
    });
    // ⚠⚠ AND IT MUST NOT CARRY `universe`. A company body with a stray universe key is answered by
    // the INDEX branch server-side — a chart that draws ACWI under a company's name.
    expect(JSON.parse(benchBody(co))).not.toHaveProperty('universe');
  });

  it('sends an index as before, with no holdings', () => {
    expect(JSON.parse(benchBody(idx))).toEqual({ universe: 'ACWI', cadence: 'annual' });
    expect(JSON.parse(benchBody(idx))).not.toHaveProperty('holdings');
  });

  it('keys on the identifier, never on the label alone', () => {
    // ⚠ Two companies can share a name (dual listings, share classes). Keyed on the label, the
    // fetch effect would not re-run and the chart would keep the previous company's line under the
    // new name — the failure that looks most like a correct answer.
    const twin: BenchTarget = { isin: 'US67066G1041', label: 'NVIDIA Corporation', cadence: 'annual' };
    expect(benchKey(co)).not.toEqual(benchKey(twin));
    expect(benchKey(co)).not.toEqual(benchKey({ ...co, cadence: 'quarterly' }));
    expect(benchKey(null)).toBe('');
    // An index and a company that happen to share a label are still different targets.
    expect(benchKey(idx)).not.toEqual(benchKey({ isin: 'X', label: 'ACWI', cadence: 'annual' }));
  });

  it('discriminates the two, since the caps fetch and the label both depend on it', () => {
    expect(isUniverseTarget(idx)).toBe(true);
    expect(isUniverseTarget(co)).toBe(false);
  });

  it('labels a company by its NAME in the note, not by an identifier', () => {
    expect(benchNote(co, null, 'boom', null)).toBe('NVIDIA Corporation: boom');
  });
});
