/**
 * Which holdings a strategy ENTERED and which it SOLD, per trading day.
 *
 * ⚠ THE EDGE DAYS GET NEITHER MARK, AND THAT IS THE WHOLE RULE. The oldest day in the window has
 * no previous day to compare against, so every one of its holdings would read as "entered" — two
 * dozen green rows announcing a wholesale purchase that is really just the window opening. The
 * newest day has no next day, so nothing there can be known to have been sold. Both are absence of
 * evidence, and absence is shown as absence rather than as the finding it resembles.
 *
 * ⚠ AND `null` IS NOT AN EMPTY SET. Treating "no previous day" as "held nothing yesterday" is
 * exactly how the first mistake gets made: the arithmetic works, every row comes back green, and
 * the output is confidently wrong. The neighbour is either known or it is not.
 */
export type MarkDay = { date: string; holdings: { company_id: number }[] };
export type SectorHolding = { sector?: string | null; sector_rank?: number | null };
export type DayMarks = { entered: Set<number>; sold: Set<number> };

/**
 * `days` may be in either order — it is sorted ascending internally, because the table renders
 * newest-first and the comparison is chronological. Returns one entry per day, keyed by date.
 */
export function computeMarks(days: MarkDay[]): Map<string, DayMarks> {
  const asc = [...days].sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
  const idsOf = (d: MarkDay) => new Set(d.holdings.map((h) => h.company_id));
  const out = new Map<string, DayMarks>();
  asc.forEach((d, i) => {
    // null = unknowable (no neighbour), NOT an empty basket. See the header.
    const prev = i > 0 ? idsOf(asc[i - 1]) : null;
    const next = i < asc.length - 1 ? idsOf(asc[i + 1]) : null;
    const entered = new Set<number>();
    const sold = new Set<number>();
    for (const h of d.holdings) {
      if (prev && !prev.has(h.company_id)) entered.add(h.company_id);
      if (next && !next.has(h.company_id)) sold.add(h.company_id);
    }
    out.set(d.date, { entered, sold });
  });
  return out;
}

/**
 * The sectors a day bought into, in the order the strategy RANKED them.
 *
 * ⚠ ORDERED BY `sector_rank`, NOT BY NAME AND NOT BY HOLDING COUNT. That field is the strategy's
 * own "1 = best-scoring sector picked", so the first entry is the top pick — which is the entire
 * value of reading the colour squares down a column. Any other order produces a stable-looking row
 * of squares whose position means nothing, and nothing on screen would reveal it.
 *
 * ⚠ DERIVED FROM THE HOLDINGS, NOT FROM `sector_scores`. Days cached before sector scores existed
 * carry none, and a column that silently empties for older rows reads as "no sectors were picked
 * that day" rather than "this day predates that field". `sector_rank` has always been on a holding.
 *
 * A holding with no rank sorts last rather than first — an unranked pick is not the top pick.
 */
export function pickedSectors(holdings: SectorHolding[]): string[] {
  const best = new Map<string, number>();
  for (const h of holdings) {
    if (!h.sector) continue;
    const r = h.sector_rank ?? Number.MAX_SAFE_INTEGER;
    const prev = best.get(h.sector);
    if (prev == null || r < prev) best.set(h.sector, r);
  }
  return [...best.entries()]
    .sort((a, b) => a[1] - b[1] || a[0].localeCompare(b[0]))
    .map(([s]) => s);
}
