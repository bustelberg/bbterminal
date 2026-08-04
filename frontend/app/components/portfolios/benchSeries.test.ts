import { describe, expect, it } from 'vitest';
import { benchNote, mergeSeries, rebaseOnto, withBench, type BenchTarget } from './benchSeries';

/**
 * The two pure halves of the Long Equity benchmark overlay.
 *
 * `rebaseOnto` is the one that can lie. The ratio cards need nothing (a margin is a % on both
 * sides); the LEVEL cards put a company's EUR millions beside an index and would otherwise be two
 * scales on one axis. Scaling the index to meet the line at the first SHARED period is what makes
 * that honest — and the anchor has to be shared, or the two lines start in different years and the
 * gap between them is an artefact of the anchor rather than of the businesses.
 */

const m = (o: Record<number, number | null>) => new Map(
  Object.entries(o).map(([k, v]) => [Number(k), v] as [number, number | null]));

describe('rebaseOnto', () => {
  it('scales the benchmark to meet ours at the first shared period', () => {
    const own = m({ 2020: 500, 2021: 600 });
    const bench = m({ 2020: 100, 2021: 110 });
    const out = rebaseOnto(bench, own);
    expect(out?.get(2020)).toBe(500);          // meets there by construction
    expect(out?.get(2021)).toBe(550);          // +10% on the index, off the anchored base
  });

  it('anchors on the first SHARED period, not on each series own first point', () => {
    // The index reaches back further. Anchoring on its own 2018 would draw it from a base our
    // line never had, and the visible gap would be a fact about the anchor, not the companies.
    const own = m({ 2020: 500, 2021: 600 });
    const bench = m({ 2018: 50, 2019: 80, 2020: 100, 2021: 110 });
    const out = rebaseOnto(bench, own);
    expect(out?.get(2020)).toBe(500);
    expect(out?.get(2018)).toBe(250);          // 50 x (500/100) — earlier history is kept, scaled
  });

  it('keeps the benchmark growth rate exactly — a rebase is a scale, never a reshape', () => {
    const own = m({ 2020: 7 });
    const bench = m({ 2020: 100, 2021: 130, 2022: 169 });
    const out = rebaseOnto(bench, own)!;
    expect((out.get(2021) as number) / (out.get(2020) as number)).toBeCloseTo(1.3, 12);
    expect((out.get(2022) as number) / (out.get(2021) as number)).toBeCloseTo(1.3, 12);
  });

  it('refuses when the two share no period — any factor there is invented', () => {
    expect(rebaseOnto(m({ 2010: 100 }), m({ 2020: 500 }))).toBeNull();
  });

  it('skips a period where either side is null or non-positive as the anchor', () => {
    // A log axis cannot plot <= 0, and dividing by one would blow the scale up.
    const own = m({ 2019: 0, 2020: null, 2021: 400 });
    const bench = m({ 2019: 100, 2020: 105, 2021: 110 });
    expect(rebaseOnto(bench, own)?.get(2021)).toBe(400);
  });

  it('carries nulls through as nulls rather than as zeros', () => {
    const out = rebaseOnto(m({ 2020: 100, 2021: null }), m({ 2020: 500 }));
    expect(out?.get(2021)).toBeNull();
  });
});

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
  const drawn = m({ 2020: 5 });

  it('says nothing when no benchmark is selected', () => {
    expect(benchNote(null, null, null, null)).toBeNull();
  });

  it('says nothing when the line drew', () => {
    expect(benchNote(aex, { rows: [] }, null, drawn)).toBeNull();
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
