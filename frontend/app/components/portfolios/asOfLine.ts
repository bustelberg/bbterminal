/**
 * WHEN EACH SIDE OF A COMPARISON WAS MEASURED — dates only; the words live in `riskCopy`.
 *
 * ⚠⚠ THE TWO SIDES HAVE DIFFERENT CLOCKS AND THE CARD MUST SAY BOTH. Active share sets the book
 * against an index: the book's weights come from AIRS on its own valuation cadence, the index's
 * come from Yahoo market caps refreshed per constituent. Neither is "today", the two are rarely
 * the same day, and a single "as of" over the pair would be a date that is true of neither side.
 * The card prints both and lets the reader decide whether they are close enough to compare — which
 * is a judgement about the book in front of them, not one this code can make.
 *
 * ⚠⚠ AND IT NEVER SUBSTITUTES TODAY FOR A MISSING DATE. The string this replaced said "Today's
 * weights", which was an assumption printed as a fact: the weights are as of whenever AIRS last
 * valued the book, which on a Monday morning is Friday, and after a failed scrape is older still.
 * A null here stays null all the way to the sentence, where it is rendered as "not recorded" —
 * because "we do not know when" and "now" are opposite answers and only one of them is honest.
 *
 * ⚠ THE INDEX SIDE IS A RANGE, NOT A DATE. `market_cap_checked_at` is stamped per constituent, so
 * on a live index the stamps spread over days. Collapsing that to the newest would describe the
 * freshest name and imply it of all 1,700.
 */

/**
 * The calendar day out of an ISO date or timestamp — `null` for anything unusable.
 *
 * ⚠ A PURE PREFIX, NOT `new Date(...)`. Parsing to a Date and formatting back shifts a UTC
 * timestamp into the viewer's zone, so a cap stamped 2026-08-25T01:30Z would read as the 24th in
 * New York — a date that moves with who is looking at it is worse than no date. The stamps are
 * already ISO-8601 and their first ten characters ARE the day they name.
 */
/**
 * `2026-07-31` → `31 July 2026`, for a card's `When`.
 *
 * ⚠ IT LIVES HERE BECAUSE TWO PANELS NEED IT. It was a closure inside `DeepValuationTab`, and the
 * Reverse DCF beside it could only say "latest close" and "latest fiscal year" — descriptions of a
 * SELECTION RULE, not moments, which leave a two-year-old WACC looking current. Copying it would
 * have been two date formats one card apart.
 *
 * ⚠ UTC, LIKE EVERY OTHER DATE IN THIS APP. A bare `new Date('2026-07-31')` is parsed as UTC and
 * then FORMATTED in the viewer's zone, so anyone west of Greenwich reads the previous day.
 */
export function onDate(iso: string | null | undefined): string {
  if (!iso) return 'n/a';
  return new Date(`${iso.slice(0, 10)}T00:00:00Z`).toLocaleDateString('en-GB',
    { day: 'numeric', month: 'long', year: 'numeric', timeZone: 'UTC' });
}

export function dayOf(ts: string | null | undefined): string | null {
  if (typeof ts !== 'string') return null;
  const day = ts.slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(day) ? day : null;
}

/**
 * `"2026-08-25"` when the ends coincide, `"2026-08-22 → 2026-08-25"` when they do not, `null`
 * when neither end is known.
 *
 * ⚠ ONE KNOWN END STILL PRINTS. A range half-recorded is not nothing — "we have stamps back to
 * the 22nd" is worth saying — and refusing it would hide the only date there was.
 */
export function dayRange(from: string | null | undefined,
  to: string | null | undefined): string | null {
  const a = dayOf(from);
  const b = dayOf(to);
  if (!a && !b) return null;
  if (!a || !b || a === b) return (a ?? b) as string;
  // ⚠ ORDERED AS GIVEN, not sorted. The caller's `from` is the oldest by construction (the backend
  // sorts the stamps); re-sorting here would paper over a caller that had them backwards, which is
  // a bug worth seeing rather than a presentation to fix.
  return `${a} → ${b}`;
}
