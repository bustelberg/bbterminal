/** Which composition buckets are worth a row.
 *
 * ⚠⚠ "HIDE THE ZEROS" IS NOT "HIDE WHERE THE PORTFOLIO IS ZERO". A bucket the portfolio does not
 *     hold but the BENCHMARK does is one of the most informative rows on the chart — it is an
 *     unowned region/sector, and Brinson scores exactly that as an allocation bet. Dropping those
 *     would delete the "you own none of this and the index owns 6%" finding while leaving the
 *     chart looking complete. Only a bucket that is empty on BOTH sides carries nothing.
 *
 * ⚠ THE THRESHOLD IS THE DISPLAYED PRECISION, NOT ZERO. Filtering on `> 0` keeps rows that are
 *     visually indistinguishable from the ones removed — the reader sees some "0%" rows kept and
 *     others gone, with no rule they can infer.
 *
 * ⚠⚠ AND THE PRECISION IS DEFINED HERE, ONCE, BECAUSE GUESSING IT IS EXACTLY HOW THIS BROKE.
 *     The first cut hard-coded a 0.05 threshold on the assumption that values rendered at ONE
 *     decimal. They render at ZERO (`toFixed(0)`), so everything from 0.5% down still prints
 *     "0%" — and a 0.2% bucket sailed through a filter written to remove it. The threshold is
 *     derived from the displayed precision, so a bucket is hidden if and only if BOTH sides would
 *     round away to 0.00%, and the two cannot disagree.
 *
 * ⚠ A KEPT ROW WHOSE SIDE IS ZERO NOW **PRINTS** "0.00%" rather than blanking (2026-08-13) — see
 *     `formatPct`. That is a change to the label only; which rows exist is still this rule.
 */

export type CompositionRow = {
  bucket: string;
  portfolio_pct?: number | null;
  benchmark_pct?: number | null;
};

/** Decimals shown on a composition percentage. THE single source — see the module note.
 *
 * ⚠ RAISED 0 → 2 (2026-08-10, on request). The bars are read against the drill-down totals beneath
 * them, and at zero decimals a bar saying "35%" could not be checked against a list summing to
 * 31.24% — the reader could not tell a rounding from a disagreement. Two decimals makes the two
 * figures directly comparable, which is the only way the panel's claim that they match is worth
 * anything.
 *
 * ⚠ THE THRESHOLD FOLLOWS AUTOMATICALLY — that is the whole point of deriving `DISPLAY_EPSILON`
 * from this constant. It moves 0.5 → 0.005, so buckets that used to vanish for printing "0%" now
 * print "0.01%" and are kept, which is the same rule ("hide it if and only if it would render as
 * zero") applied at the new precision rather than a second decision. */
export const DISPLAY_DECIMALS = 2;

/** Below this, a percentage renders as "0%" and the bucket carries no visible information. */
export const DISPLAY_EPSILON = 0.5 / 10 ** DISPLAY_DECIMALS;

/**
 * A bucket percentage as shown.
 *
 * ⚠ A ZERO IS PRINTED AS "0.00%", NOT LEFT BLANK (2026-08-13, on request). It used to blank
 * anything under `DISPLAY_EPSILON` on the reasoning that a zero label is ink spent saying nothing.
 * On a two-series chart that is backwards: the rows where one side is zero are the ones the chart
 * exists for — a sector the index holds and the book does not is an unowned sector, which is a
 * FINDING (Brinson scores exactly that as an allocation bet), and the same is true mirrored. Blank
 * there does not read as "zero", it reads as "we could not work this out", which is the one thing
 * it does not mean.
 *
 * ⚠ ONLY A NON-NUMBER IS STILL BLANK, and that distinction is the whole reason this is not just
 * `toFixed`. `null`/`undefined`/NaN mean "no value for this side", which is genuinely different
 * from a measured zero and must not be dressed up as one.
 */
export function formatPct(v: unknown): string {
  // ⚠ `Number(null)` IS **0**, NOT NaN — so a `Number.isFinite` test alone would print a missing
  // side as a measured "0.00%", which is the one thing the paragraph above says must not happen.
  // It was harmless only while the epsilon blanked zeros anyway; it is not harmless now.
  if (v == null || v === '') return '';
  const n = Number(v);
  return Number.isFinite(n) ? `${n.toFixed(DISPLAY_DECIMALS)}%` : '';
}

/**
 * Rows to draw: everything with weight on at least one side.
 *
 * ⚠ THE THRESHOLD STILL LIVES HERE, AND IT IS NOW THE **ONLY** PLACE IT SHOWS. While `formatPct`
 * blanked its zeros, the two were one rule seen twice — a bucket was hidden if and only if it would
 * have rendered empty. The formatter now prints "0.00%", so visibility is decided here alone: a row
 * survives when EITHER side would render as something other than 0.00%. A bucket that is zero on
 * both sides is in neither universe and is the one row that genuinely carries nothing.
 */
export function visibleBuckets<T extends CompositionRow>(rows: T[]): T[] {
  return rows.filter((r) => (r.portfolio_pct ?? 0) >= DISPLAY_EPSILON
    || (r.benchmark_pct ?? 0) >= DISPLAY_EPSILON);
}
