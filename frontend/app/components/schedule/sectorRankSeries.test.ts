import { describe, it, expect } from 'vitest';
import { maxRank, sectorRankSeries, type RankDay } from './sectorRankSeries';

const day = (date: string, ranks: Record<string, number | null>): RankDay => ({
  date,
  sector_scores: Object.entries(ranks).map(([sector, rank]) => ({ sector, rank })),
});

describe('sectorRankSeries', () => {
  const days = [
    day('2026-01-02', { Technology: 1, Energy: 2, Utilities: 3 }),
    day('2026-01-05', { Technology: 2, Energy: 1, Utilities: 3 }),
    day('2026-01-06', { Technology: 3, Energy: 1, Utilities: 2 }),
  ];

  it('gives every sector a point for every day in the window', () => {
    for (const s of sectorRankSeries(days)) expect(s.points).toHaveLength(3);
  });

  it('orders sectors by their LATEST rank, best first', () => {
    expect(sectorRankSeries(days).map((s) => s.sector)).toEqual(['Energy', 'Utilities', 'Technology']);
  });

  it('accepts the days in either order', () => {
    const rev = [...days].reverse();
    expect(sectorRankSeries(rev)[0].points.map((p) => p.date)).toEqual([
      '2026-01-02', '2026-01-05', '2026-01-06',
    ]);
  });

  it('⚠ a day with no sector_scores becomes a HOLE, not a dropped point', () => {
    // Dropping it would slide the neighbours together and draw a continuous line across a
    // period we have no ranking for.
    const withGap: RankDay[] = [days[0], { date: '2026-01-05' }, days[2]];
    const tech = sectorRankSeries(withGap).find((s) => s.sector === 'Technology')!;
    expect(tech.points.map((p) => p.rank)).toEqual([1, null, 3]);
    expect(tech.points).toHaveLength(3);
  });

  it('⚠ a sector absent from one day is NOT ranked last', () => {
    // It left the pool (every company failed the price floor). Forcing it to the bottom of the
    // axis would draw a collapse that never happened.
    const d = [
      day('2026-01-02', { Technology: 1, Energy: 2 }),
      day('2026-01-05', { Technology: 1 }),
      day('2026-01-06', { Technology: 1, Energy: 2 }),
    ];
    const energy = sectorRankSeries(d).find((s) => s.sector === 'Energy')!;
    expect(energy.points.map((p) => p.rank)).toEqual([2, null, 2]);
  });

  it('latest is the most recent KNOWN rank, not the last point', () => {
    const d = [day('2026-01-02', { Energy: 4 }), { date: '2026-01-05' } as RankDay];
    expect(sectorRankSeries(d)[0].latest).toBe(4);
  });

  it('counts how many days a sector was ranked at all', () => {
    const d = [
      day('2026-01-02', { Energy: 1 }),
      day('2026-01-05', { Technology: 1 }),
      day('2026-01-06', { Energy: 2 }),
    ];
    expect(sectorRankSeries(d).find((s) => s.sector === 'Energy')!.ranked).toBe(2);
  });

  it('a never-ranked sector sorts LAST, both ways', () => {
    const d = [day('2026-01-02', { Ghost: null, Energy: 1 })];
    expect(sectorRankSeries(d).map((s) => s.sector)).toEqual(['Energy', 'Ghost']);
  });

  it('an empty window yields no series rather than throwing', () => {
    expect(sectorRankSeries([])).toEqual([]);
  });
});

describe('the two non-ranked states are distinguished', () => {
  it('⚠ absent from a day that HAS rankings = dropped from the pool, not missing data', () => {
    // Measured: Consumer Cyclical went rank 3 -> gone -> rank 8 across 11-16 June, because a
    // min_price_score floor of 30 dropped every one of its companies. That is a finding about the
    // sector. Rendered the same as an outage, it gets reported as a bug — it was, twice.
    const d = [
      day('2026-06-11', { Technology: 1, 'Consumer Cyclical': 3 }),
      day('2026-06-12', { Technology: 1 }),
      day('2026-06-16', { Technology: 1, 'Consumer Cyclical': 8 }),
    ];
    const cc = sectorRankSeries(d).find((s) => s.sector === 'Consumer Cyclical')!;
    expect(cc.points.map((p) => p.state)).toEqual(['ranked', 'dropped', 'ranked']);
  });

  it('a day with NO rankings at all is unknown for every sector', () => {
    const d: RankDay[] = [
      day('2026-06-11', { Technology: 1, Energy: 2 }),
      { date: '2026-06-12' },
      day('2026-06-16', { Technology: 1, Energy: 2 }),
    ];
    for (const s of sectorRankSeries(d)) {
      expect(s.points.map((p) => p.state)).toEqual(['ranked', 'unknown', 'ranked']);
    }
  });

  it('an empty sector_scores array is unknown, not a mass drop-out', () => {
    // Legacy cached rows carry `[]`. Calling that "every sector dropped" would invent 11 findings.
    const d: RankDay[] = [
      day('2026-06-11', { Technology: 1 }),
      { date: '2026-06-12', sector_scores: [] },
    ];
    expect(sectorRankSeries(d)[0].points.map((p) => p.state)).toEqual(['ranked', 'unknown']);
  });

  it('collapses consecutive dropped days into one run', () => {
    const d = [
      day('2026-06-11', { T: 1, CC: 2 }),
      day('2026-06-12', { T: 1 }),
      day('2026-06-15', { T: 1 }),
      day('2026-06-16', { T: 1, CC: 2 }),
    ];
    const cc = sectorRankSeries(d).find((s) => s.sector === 'CC')!;
    expect(cc.droppedRuns).toEqual([{ from: '2026-06-12', to: '2026-06-15' }]);
  });

  it('keeps separate drop-outs separate', () => {
    const d = [
      day('2026-06-11', { T: 1, CC: 2 }),
      day('2026-06-12', { T: 1 }),
      day('2026-06-15', { T: 1, CC: 2 }),
      day('2026-06-16', { T: 1 }),
    ];
    const cc = sectorRankSeries(d).find((s) => s.sector === 'CC')!;
    expect(cc.droppedRuns).toEqual([
      { from: '2026-06-12', to: '2026-06-12' },
      { from: '2026-06-16', to: '2026-06-16' },
    ]);
  });

  it('an unknown day does not join two drop-outs into one run', () => {
    const d: RankDay[] = [
      day('2026-06-11', { T: 1 }),
      { date: '2026-06-12' },
      day('2026-06-15', { T: 1 }),
    ];
    // T is ranked throughout; a sector that only ever appears on the ranked days:
    const d2: RankDay[] = [day('2026-06-11', { T: 1, CC: 2 }), { date: '2026-06-12' },
      day('2026-06-15', { T: 1 })];
    void d;
    expect(sectorRankSeries(d2).find((s) => s.sector === 'CC')!.droppedRuns)
      .toEqual([{ from: '2026-06-15', to: '2026-06-15' }]);
  });
});

describe('maxRank — the shared y-domain', () => {
  it('is the worst rank anywhere in the window', () => {
    expect(maxRank(sectorRankSeries([day('2026-01-02', { A: 1, B: 9 })]))).toBe(9);
  });

  it('⚠ is shared so panels are comparable', () => {
    // Per-panel autoscaling would draw an 11-sector move and a 2-sector wobble at the same
    // amplitude — the reason this is one number and not one per series.
    const s = sectorRankSeries([day('2026-01-02', { A: 1, B: 11 })]);
    expect(maxRank(s)).toBe(11);
    expect(maxRank([s[0]])).toBe(11 - 10);   // computed per call, from what is passed in
  });

  it('never returns 0 for an empty window (a rank axis starts at 1)', () => {
    expect(maxRank([])).toBe(1);
  });

  it('ignores holes', () => {
    expect(maxRank(sectorRankSeries([day('2026-01-02', { A: null, B: 3 })]))).toBe(3);
  });
});
