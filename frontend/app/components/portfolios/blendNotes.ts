/**
 * Wording for `blend_notes` — the backend's answer to "the holdings have this metric, so why is
 * the portfolio chart empty?" (`_fundamental_blend.explain_empty`).
 *
 * ⚠ AN EMPTY CHART HAS TWO OPPOSITE CAUSES AND LOOKS IDENTICAL. Measured on a real book's
 * Dividends per Share: every holding carried the line and the card still read "No dividend/share
 * ingested for this portfolio" — a level series is rebased to 100 at its first year, and a
 * dividend series that starts at 0.00 cannot be, so member after member was dropped until no year
 * cleared the coverage floor. Telling the reader to go ingest data they already have is worse than
 * saying nothing: it sends them to spend GuruFocus quota on a problem that is not there.
 */

export type BlendNote = {
  kind: string;
  reporting: number;          // holdings that carry this metric
  reporting_pct: number;      // their share of blended weight
  contributing: number;       // of those, how many survived preparation
  dropped: Record<string, number>;
  best_covered_pct: number;
  floor_pct: number;
  years: number;
  years_below_floor: number;
  years_no_value: number;
};

/** The note for a card, whichever of its metric-code spellings the blend actually saw. */
export function noteFor(notes: Record<string, BlendNote> | undefined, codes: string[]) {
  for (const c of codes) if (notes?.[c]) return notes[c];
  return undefined;
}

/**
 * One short sentence naming the DOMINANT cause. Short because the card column is narrow, and one
 * cause because a list of four contributing factors is not something a reader acts on.
 */
export function whyNoLine(n: BlendNote): string {
  const rebase = n.dropped?.non_positive_base ?? 0;
  if (rebase > 0) {
    return `${rebase} of them start at 0, and a level series is charted as growth from its first year — which can't be rebased off zero.`;
  }
  if (n.best_covered_pct < n.floor_pct) {
    return `the best year covers only ${n.best_covered_pct.toFixed(0)}% of weight, under the ${n.floor_pct.toFixed(0)}% floor a blended figure needs.`;
  }
  if (n.years_no_value > 0) {
    return `no year has a usable value (a multiple needs positive earnings to invert).`;
  }
  return `the blend produced no points from them.`;
}

/** "9 holdings (41% of weight) report dividend/share" — the fact that contradicts "not ingested". */
export function reportingLine(n: BlendNote, noun: string): string {
  const one = n.reporting === 1;
  const w = n.reporting_pct > 0 ? ` (${n.reporting_pct.toFixed(0)}% of weight)` : '';
  return `${n.reporting} holding${one ? '' : 's'}${w} report${one ? 's' : ''} ${noun}`;
}
