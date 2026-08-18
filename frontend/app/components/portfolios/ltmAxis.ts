/**
 * Where a TRAILING-TWELVE-MONTH point sits on an ANNUAL axis — the arithmetic, on its own.
 *
 * Extracted from `MetricGrowthCard` 2026-08-18 because both rules in here have already been got
 * wrong once each in a way that rendered as a plausible chart rather than as an error, and neither
 * had a test: the placement (a stub landing on top of the first forecast) and the COUNT (two stubs,
 * two "LTM" ticks, one axis).
 */

/** An LTM point as the chart carries it: a value, the x it is drawn at, and the real quarter-end
 *  its twelve months run to. `date` is the FACT; `year` is a coordinate. */
export type LtmPoint = { year: number; value: number; date?: string };

const YEAR_MS = 365.25 * 24 * 3600 * 1000;

/**
 * Where an LTM point belongs on an ANNUAL axis: a fraction of a year past the last reported one.
 *
 * ⚠⚠ THE QUARTER BUCKET PUTS IT ON TOP OF A FISCAL YEAR, AND WITH A FORECAST ON THE CHART THAT IS
 * NO LONGER INVISIBLE. A trailing year ending 2026-03-31 buckets to `2026 + (1−1)/4` = **2026.0** —
 * the same x as FY2026, which is where the analysts' first estimate sits. So the newest ACTUAL and
 * the first FORECAST landed on one tick, the LTM appeared to be a year further along than it is,
 * and the dotted leg started underneath it instead of after it.
 *
 * The fix cannot be "use the calendar fraction" either, because the annual axis is not a calendar:
 * `extractPoints` places a fiscal year at the YEAR IT ENDS IN, so an off-calendar filer's FY2026
 * (ending 2026-03-31) already sits at 2026 while occupying the same months this LTM does. Measuring
 * from the entity's OWN last fiscal year end sidesteps the whole question — three months past it is
 * three months past it, whatever calendar that year was labelled with.
 *
 * ⚠ CLAMPED INSIDE THE YEAR, never onto its neighbours. At 0 it would sit on the last actual and
 * hide it; at 1 it would sit on the first forecast, which is the bug this exists to fix.
 */
export function ltmYearX(
  ltmDate: string | undefined, last: { year: number; date?: string } | null,
): number | null {
  if (!last) return null;
  if (!ltmDate || !last.date) return last.year + 0.25;   // no date to measure from: a quarter on
  const gap = (Date.parse(`${ltmDate}T00:00:00Z`) - Date.parse(`${last.date}T00:00:00Z`)) / YEAR_MS;
  return last.year + Math.min(0.95, Math.max(0.05, gap));
}

/**
 * The ONE x every LTM point on a chart is drawn at.
 *
 * ⚠⚠ A CHART HAS EXACTLY ONE LTM SLOT. `ltmYearX` measures each entity's stub from ITS OWN last
 * fiscal year end — right for the LENGTH of that stub, wrong as a coordinate. Two entities on
 * different fiscal calendars therefore produced two x, so the axis grew a SECOND tick reading
 * "LTM" and the two trailing points sat side by side as if the index's twelve months had happened
 * later in time than the book's. Measured on the Revenue chart against AEX.
 *
 * ⚠ THE OFFSET IT WAS DRAWING IS BOOKKEEPING, NOT TIME. Both points mean the same thing — "the
 * latest twelve months available" — and that is one position, immediately after the last full
 * year. The windows genuinely end on different quarters; that difference is a fact about DATES
 * (see `ltmWindowsDiffer`), reported in the tooltip and the footnote where a reader can act on it,
 * rather than encoded as a horizontal gap that cannot be measured off two identical ticks.
 *
 * ⚠ THE SUBJECT'S x WINS. The card is about the book/company; the index is an overlay, so the
 * stub's length is the subject's. With no subject LTM the overlay's position is the only one there
 * is and it becomes the slot — a chart with one LTM line still has one LTM x.
 */
export function sharedLtmX(
  own: LtmPoint | null | undefined, bench: LtmPoint | null | undefined,
): number | null {
  return own?.year ?? bench?.year ?? null;
}

/**
 * Do the two lines' trailing twelve months end on DIFFERENT quarters?
 *
 * ⚠ COMPARED ON THE DATES, NEVER ON THE DRAWN POSITIONS. Once both points share an x the positions
 * are equal by construction, so a check written against them answers "no" always — and this is the
 * only remaining signal that part of the gap at the right-hand edge is calendar rather than
 * performance. It became MORE important the moment the split stopped being visible on the axis.
 *
 * ⚠ FALSE WHEN EITHER SIDE HAS NO WINDOW TO NAME. One line, or an undated point, is not a
 * disagreement — and a warning that cannot name both quarters explains nothing.
 */
export function ltmWindowsDiffer(
  own: LtmPoint | null | undefined, bench: LtmPoint | null | undefined,
): boolean {
  return !!(own?.date && bench?.date && own.date !== bench.date);
}

/** The overlay's LTM as it is DRAWN — its own value and window, at the chart's single LTM x. */
export function atSharedX<T extends LtmPoint>(p: T | null | undefined, x: number | null): T | null {
  if (!p) return null;
  return x == null ? p : { ...p, year: x };
}
