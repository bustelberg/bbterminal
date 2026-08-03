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
 *     "0%" — and a 0.2% bucket sailed through a filter written to remove it. The formatter now
 *     comes from the same constant as the threshold: a number is hidden if and only if it would
 *     have rendered as "0%", and the two cannot disagree.
 */

export type CompositionRow = {
  bucket: string;
  portfolio_pct?: number | null;
  benchmark_pct?: number | null;
};

/** Decimals shown on a composition percentage. THE single source — see the module note. */
export const DISPLAY_DECIMALS = 0;

/** Below this, a percentage renders as "0%" and the bucket carries no visible information. */
export const DISPLAY_EPSILON = 0.5 / 10 ** DISPLAY_DECIMALS;

/** A bucket percentage as shown. ⚠ Empty below the threshold rather than "0%": a zero label on an
 *  absent bucket is ink spent saying nothing, and it crowds the values that matter. */
export function formatPct(v: unknown): string {
  const n = Number(v);
  return Number.isFinite(n) && n >= DISPLAY_EPSILON ? `${n.toFixed(DISPLAY_DECIMALS)}%` : '';
}

/** Rows to draw: everything with weight on at least one side. */
export function visibleBuckets<T extends CompositionRow>(rows: T[]): T[] {
  return rows.filter((r) => (r.portfolio_pct ?? 0) >= DISPLAY_EPSILON
    || (r.benchmark_pct ?? 0) >= DISPLAY_EPSILON);
}
