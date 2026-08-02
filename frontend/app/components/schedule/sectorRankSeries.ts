/**
 * Per-sector rank over time, from the daily-holdings walk.
 *
 * Each day of the walk already carries `sector_scores` — every sector in the selection pool with
 * the rank the strategy gave it that day. This reshapes that into one series per sector so each
 * can be drawn as a timeseries.
 *
 * ⚠ A DAY WITH NO `sector_scores` IS A HOLE, NOT A RANK. Days cached before sector scores existed
 * carry none. Emitting `rank: null` keeps the gap visible (the chart draws a break); dropping the
 * day instead would slide the neighbouring points together and draw a continuous line across a
 * period we have no ranking for — a shape the reader would take as evidence.
 *
 * ⚠ AND A SECTOR MISSING FROM ONE DAY IS ALSO A HOLE. A sector can leave the pool entirely (every
 * one of its companies fails the price floor that day). That is not "rank last" — it is "not
 * ranked", and forcing it to the bottom of the axis would draw a dramatic collapse that never
 * happened.
 */

export type RankDay = {
  date: string;
  sector_scores?: { sector: string; rank?: number | null }[];
};

/**
 * Why a day has no rank. ⚠ THE TWO NON-RANKED STATES ARE DIFFERENT FACTS AND MUST NOT RENDER ALIKE.
 *
 *   dropped  the day WAS ranked — this sector just wasn't in the pool. With a `min_price_score`
 *            floor and no backfill, a name below it is dropped outright, so a sector whose every
 *            company falls under the floor vanishes for that day. That is a FINDING about the
 *            sector (measured: Consumer Cyclical went rank 3 → gone → rank 8 across 11–16 June on
 *            a floor of 30), not missing data.
 *   unknown  the day carries no ranking at all — nothing was computed or stored for it.
 *
 * Drawn identically, "dropped" reads as a data outage and gets reported as a bug. It was, twice.
 */
export type RankState = 'ranked' | 'dropped' | 'unknown';

export type SectorRankSeries = {
  sector: string;
  /** One point per day in the window, ascending. `rank` is null unless `state === 'ranked'`. */
  points: { date: string; rank: number | null; state: RankState }[];
  /** Most recent known rank, or null when the sector has no ranked day at all. */
  latest: number | null;
  /** How many days of the window this sector was ranked at all. */
  ranked: number;
  /** Consecutive stretches where the sector was ranked-out of the pool — for shading. */
  droppedRuns: { from: string; to: string }[];
};

/**
 * One series per sector, ordered by latest rank (best first); sectors never ranked sort last.
 *
 * `days` may arrive in either order — the table renders newest-first while a chart reads
 * left-to-right, so the ordering is fixed here rather than at each call site.
 */
export function sectorRankSeries(days: RankDay[]): SectorRankSeries[] {
  const asc = [...days].sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
  const dates = asc.map((d) => d.date);
  const sectors = new Set<string>();
  const byDate = new Map<string, Map<string, number | null>>();
  // A day that carries ANY sector ranking. Its absent sectors were dropped from the pool; a day
  // that carries none tells us nothing about any sector.
  const dayRanked = new Set<string>();

  for (const d of asc) {
    const m = new Map<string, number | null>();
    for (const s of d.sector_scores ?? []) {
      if (!s.sector) continue;
      sectors.add(s.sector);
      m.set(s.sector, s.rank ?? null);
    }
    if (m.size > 0) dayRanked.add(d.date);
    byDate.set(d.date, m);
  }

  const out: SectorRankSeries[] = [];
  for (const sector of sectors) {
    const points = dates.map((date) => {
      // `?? null` and NOT `?? someBigNumber` — see the header.
      const rank = byDate.get(date)?.get(sector) ?? null;
      const state: RankState = rank != null ? 'ranked'
        : dayRanked.has(date) ? 'dropped' : 'unknown';
      return { date, rank, state };
    });
    const known = points.filter((p) => p.rank != null);
    // Consecutive dropped days, collapsed into runs so the chart shades a stretch rather than
    // one band per day.
    const droppedRuns: { from: string; to: string }[] = [];
    for (const p of points) {
      if (p.state !== 'dropped') continue;
      const last = droppedRuns[droppedRuns.length - 1];
      const prevDate = dates[dates.indexOf(p.date) - 1];
      if (last && last.to === prevDate) last.to = p.date;
      else droppedRuns.push({ from: p.date, to: p.date });
    }
    out.push({
      sector,
      points,
      latest: known.length ? (known[known.length - 1].rank as number) : null,
      ranked: known.length,
      droppedRuns,
    });
  }
  out.sort((a, b) => {
    if (a.latest == null && b.latest == null) return a.sector.localeCompare(b.sector);
    if (a.latest == null) return 1;      // never-ranked sorts last, both ways
    if (b.latest == null) return -1;
    return a.latest - b.latest || a.sector.localeCompare(b.sector);
  });
  return out;
}

/** The worst rank seen anywhere in the window — the chart's shared y-domain bottom.
 *  Shared across panels on purpose: per-panel autoscaling would draw an 11-sector move and a
 *  2-sector wobble as the same amplitude. */
export function maxRank(series: SectorRankSeries[]): number {
  let m = 1;
  for (const s of series) {
    for (const p of s.points) if (p.rank != null && p.rank > m) m = p.rank;
  }
  return m;
}
