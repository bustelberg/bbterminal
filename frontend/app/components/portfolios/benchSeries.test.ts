import { describe, expect, it } from 'vitest';
import { benchNote, mergeSeries, rebaseSeries, withBench, type BenchTarget } from './benchSeries';

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
  const aex: BenchTarget = { universe: 'AEX', cadence: 'annual' };
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
      .toBe('AEX: one period only — the rest fall under the 80% coverage floor');
  });

  it('keeps the three absences apart — they have different fixes', () => {
    // ⚠ This is the whole point. On screen all three are "no second line", and a reader who
    // cannot tell them apart will read a failed request as an index that tracks the book.
    expect(benchNote(aex, null, null, null)).toBe('AEX: loading…');
    expect(benchNote(aex, null, 'no holdings', null)).toBe('AEX: no holdings');
    expect(benchNote(aex, { rows: [] }, null, m({}))).toBe(
      'AEX: no period clears the 80% coverage floor');
  });

  it('calls out a single period, which otherwise reads as a rendering glitch', () => {
    // Measured on "Interest / op. profit": a bank reports no operating income at all, so the
    // financials' weight sits in the denominator uncounted and AEX clears the floor in exactly
    // one year of twelve. A lone dot with no explanation looks like a bug in the chart.
    expect(benchNote(aex, { rows: [] }, null, m({ 2025: 12.3 }))).toBe(
      'AEX: one period only — the rest fall under the 80% coverage floor');
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
