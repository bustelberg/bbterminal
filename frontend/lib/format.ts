/**
 * Shared date/number formatting helpers.
 *
 * Centralizes formatters that were re-defined per component (`fmtTimestamp`
 * was copy-pasted verbatim in DailyMtdRefreshCard / LeonteqUniverse /
 * Schedule). The `Intl.*Format` instances are built once at module load and
 * reused — `Date.prototype.toLocaleString(...)` constructs a fresh formatter
 * on every call, which adds up across table renders.
 *
 * Locale is left as the runtime default (`undefined`), matching the previous
 * `toLocaleString` calls.
 */

// e.g. "Apr 03, 2026, 14:30"
const dateTimeFmt = new Intl.DateTimeFormat(undefined, {
  year: 'numeric',
  month: 'short',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
});

// e.g. "Mon, 14:30"
const clockFmt = new Intl.DateTimeFormat(undefined, {
  weekday: 'short',
  hour: '2-digit',
  minute: '2-digit',
});

/**
 * Format an ISO timestamp as a short date-time. `null`/empty → "—"; an
 * unparseable value falls back to the raw string.
 */
export function fmtTimestamp(iso: string | null | undefined): string {
  if (!iso) return '—';
  try {
    return dateTimeFmt.format(new Date(iso));
  } catch {
    return iso;
  }
}

/** Same format as {@link fmtTimestamp} for an already-parsed `Date`. */
export function fmtDateTime(d: Date): string {
  return dateTimeFmt.format(d);
}

/**
 * Weekday + clock, e.g. "Mon, 14:30". `null`/empty/unparseable → "".
 */
export function fmtClock(iso: string | null | undefined): string {
  if (!iso) return '';
  try {
    return clockFmt.format(new Date(iso));
  } catch {
    return '';
  }
}
