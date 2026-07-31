import { describe, it, expect } from 'vitest';
import { computeMarks, pickedSectors, type MarkDay } from './dailyHoldingsMarks';

const day = (date: string, ids: number[]): MarkDay => ({
  date,
  holdings: ids.map((company_id) => ({ company_id })),
});

describe('computeMarks — entered / sold per trading day', () => {
  const days = [
    day('2026-06-01', [1, 2, 3]),
    day('2026-06-02', [2, 3, 4]),   // 4 entered, 1 gone
    day('2026-06-03', [2, 4, 5]),   // 5 entered, 3 gone
  ];

  it('marks a holding that was not there the previous day', () => {
    expect([...computeMarks(days).get('2026-06-02')!.entered]).toEqual([4]);
  });

  it('marks a holding that is gone the next day', () => {
    // 3 is held on the 2nd and absent on the 3rd.
    expect([...computeMarks(days).get('2026-06-02')!.sold]).toEqual([3]);
  });

  it('⚠ the OLDEST day marks nothing as entered', () => {
    // Otherwise the window opening reads as a wholesale purchase — every holding green.
    expect(computeMarks(days).get('2026-06-01')!.entered.size).toBe(0);
  });

  it('⚠ the NEWEST day marks nothing as sold', () => {
    // There is no next day; nothing can be KNOWN to have been sold.
    expect(computeMarks(days).get('2026-06-03')!.sold.size).toBe(0);
  });

  it('the oldest day can still show a sale, and the newest an entry', () => {
    // The edges lose only the mark they have no neighbour for — not both.
    const m = computeMarks(days);
    expect([...m.get('2026-06-01')!.sold]).toEqual([1]);
    expect([...m.get('2026-06-03')!.entered]).toEqual([5]);
  });

  it('a one-day holding is BOTH entered and sold', () => {
    const d = [day('2026-06-01', [1]), day('2026-06-02', [1, 9]), day('2026-06-03', [1])];
    const m = computeMarks(d).get('2026-06-02')!;
    expect(m.entered.has(9)).toBe(true);
    expect(m.sold.has(9)).toBe(true);
  });

  it('accepts the days in either order', () => {
    const reversed = [...days].reverse();
    expect([...computeMarks(reversed).get('2026-06-02')!.entered]).toEqual([4]);
    expect(computeMarks(reversed).get('2026-06-01')!.entered.size).toBe(0);
  });

  it('a single day marks nothing at all', () => {
    const m = computeMarks([day('2026-06-01', [1, 2])]).get('2026-06-01')!;
    expect(m.entered.size).toBe(0);
    expect(m.sold.size).toBe(0);
  });

  it('an empty window returns an empty map rather than throwing', () => {
    expect(computeMarks([]).size).toBe(0);
  });
});

describe('pickedSectors — the order the colour squares are drawn in', () => {
  it('⚠ orders by sector_rank, not alphabetically', () => {
    // The first square must be the day's TOP-ranked sector. Alphabetical order would look
    // identical and mean nothing.
    const holdings = [
      { sector: 'Utilities', sector_rank: 1 },
      { sector: 'Basic Materials', sector_rank: 2 },
      { sector: 'Technology', sector_rank: 3 },
    ];
    expect(pickedSectors(holdings)).toEqual(['Utilities', 'Basic Materials', 'Technology']);
  });

  it('collapses a sector held by several companies into one square', () => {
    const holdings = [
      { sector: 'Technology', sector_rank: 2 },
      { sector: 'Technology', sector_rank: 2 },
      { sector: 'Energy', sector_rank: 1 },
    ];
    expect(pickedSectors(holdings)).toEqual(['Energy', 'Technology']);
  });

  it('takes a sector’s BEST rank when its holdings disagree', () => {
    const holdings = [
      { sector: 'Technology', sector_rank: 4 },
      { sector: 'Technology', sector_rank: 1 },
      { sector: 'Energy', sector_rank: 2 },
    ];
    expect(pickedSectors(holdings)).toEqual(['Technology', 'Energy']);
  });

  it('sorts an unranked holding LAST, never first', () => {
    // `?? MAX_SAFE_INTEGER` — an unranked pick is not the top pick, and a null coerced to 0
    // would put it at the head of the row.
    const holdings = [
      { sector: 'Unknown', sector_rank: null },
      { sector: 'Energy', sector_rank: 1 },
    ];
    expect(pickedSectors(holdings)).toEqual(['Energy', 'Unknown']);
  });

  it('skips holdings with no sector rather than emitting a blank square', () => {
    expect(pickedSectors([{ sector: null, sector_rank: 1 }, { sector: 'Energy', sector_rank: 2 }]))
      .toEqual(['Energy']);
  });

  it('is stable for equal ranks', () => {
    const holdings = [
      { sector: 'Zeta', sector_rank: 1 },
      { sector: 'Alpha', sector_rank: 1 },
    ];
    expect(pickedSectors(holdings)).toEqual(['Alpha', 'Zeta']);
  });

  it('an empty day yields no squares', () => {
    expect(pickedSectors([])).toEqual([]);
  });
});
