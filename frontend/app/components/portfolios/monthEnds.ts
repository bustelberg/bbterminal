/**
 * One point per month — the most recent one in it. The book-return chart's resolution.
 *
 *  It is a thinning, not a resampling. Every point it keeps is a real observation on its real
 * date: the LAST one we hold in each calendar month. Nothing is averaged, interpolated or moved to
 * a month boundary, so a point still means "the book returned this, that day" — which is the only
 * reason it is safe to draw the thinned series with the same ink as the full one.
 *
 *  The current month is not a special case, and one was tried and removed the same day
 * (2026-09-03). The chart's last point read a day behind the `Return` chip beside it, so the
 * current month was made to contribute a point only when that point was dated TODAY — which
 * silently broke the property that makes the two agree: **the newest row is what the chip reads**,
 * so the last point of this series IS the chip's figure, always. Dropping the current month on a
 * morning AIRS has not published yet left the line ending at last month's close while the chip
 * and this chart's own header both stated yesterday's figure. One rule, no clock:
 * `most recent per month` ends on the newest row, which is the number beside it.
 *
 *  So a stale last point is a stale read, never a resolution. That was the actual cause of the
 * report: `BookReturnChart` fetches itself and did not take `refreshSeq`, so after a Refresh the
 * chip re-read the row the scrape had just written and the chart still held the response from
 * when the modal opened. Fixed there; nothing here can express it, and a rule that tries to
 * compensate for a stale read by hiding points makes the disagreement harder to see, not smaller.
 *
 *  The dates are `YYYY-MM-DD` AND ARE COMPARED AS STRINGS, deliberately: that ordering is the
 * calendar ordering for this format, and it keeps a timezone out of a decision that is about which
 * observation to keep rather than about when it happened.
 */

/** The minimum a point needs for this to thin it. */
export type Dated = { date: string };

/**
 * The last point in each calendar month, in order.
 *
 *  Input order is not assumed. The server returns these sorted and this still sorts: a chart drawn
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
