/**
 * ONE POINT PER MONTH, PLUS TODAY — the book-value chart's default resolution.
 *
 * ⚠⚠ IT IS A THINNING, NOT A RESAMPLING. Every point it keeps is a real observation on its real
 * date: the LAST one we hold in each calendar month. Nothing is averaged, interpolated or moved to
 * a month boundary, so a point still means "the book was worth this, that day" — which is the only
 * reason it is safe to draw the thinned series with the same ink as the full one.
 *
 * ⚠ THE CURRENT MONTH'S LAST POINT IS TODAY'S, so "each month and today" needs no special case:
 * the partial month contributes its newest observation exactly as a complete one does. Writing it
 * as two rules — last-of-month, then re-add the latest — would double the newest point on the last
 * day of a month and nowhere else.
 *
 * ⚠ THE DATES ARE `YYYY-MM-DD` AND ARE COMPARED AS STRINGS, deliberately: that ordering is the
 * calendar ordering for this format, and it keeps a timezone out of a decision that is about which
 * observation to keep rather than about when it happened.
 */

/** The minimum a point needs for this to thin it. */
export type Dated = { date: string };

/**
 * The last point in each calendar month, in order.
 *
 * ⚠ INPUT ORDER IS NOT ASSUMED. The server returns these sorted and this still sorts: a chart drawn
 * from an unsorted series is a scribble, and the cost here is a sort of a few dozen items.
 */
export function lastPerMonth<T extends Dated>(points: readonly T[]): T[] {
  const byMonth = new Map<string, T>();
  for (const p of points) {
    const month = p.date.slice(0, 7);
    const held = byMonth.get(month);
    if (!held || p.date > held.date) byMonth.set(month, p);
  }
  return [...byMonth.values()].sort((a, b) => a.date.localeCompare(b.date));
}
