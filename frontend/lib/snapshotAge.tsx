/**
 * How old an AIRS snapshot is, and a badge that says so.
 *
 * AIRS values (airs_holding / airs_performance) only move when the daily scan runs — working days
 * ~11:00 Amsterdam. So a snapshot dated today is fresh, one dated the previous trading day is the
 * normal "today's scan hasn't run yet" state, and anything TWO or more trading days back means the
 * scan has genuinely missed days and the numbers on screen are stale.
 *
 * ⚠ This exists because a cached AIRS value shown as if it were current is exactly how a stale
 * holding gets trusted: a 4-day-old AMD value read €114,587 / +142% while AIRS-live was €107,086 /
 * +126%, and nothing on the page said the value was from a past scan. The date alone is not enough —
 * a reader does not compute "is 2026-07-16 stale today?" in their head. The badge does it for them.
 *
 * ⚠⚠ BUT IT MEASURES **AIRS's VALUATION DATE**, NOT OUR COPY — AND ITS TOOLTIP USED TO NAME A
 * REMEDY THAT CANNOT WORK. It said `"Refresh from AIRS" pulls the current book`, which reads as
 * "press the button and this clears". It does not: `_vermogen_most_recent` already walks back to
 * the most recent AVAILABLE valuation, so a refresh returns the same date whenever AIRS has
 * published nothing newer.
 *
 * Measured 2026-08-17, immediately after a full "Refresh all": 31 accounts were re-scanned and the
 * newest valuation AIRS returned for ANY of them was 2026-08-15. Twenty came back dated 2026-08-11
 * or 2026-08-12 — three to four trading days old — from a scan that had just succeeded. **32 of 40
 * rows wore the amber `!`, and not one could be cleared by the action the tooltip named.**
 *
 * The number is still true and still worth showing — these snapshots ARE old. What changed is that
 * the card now says whose lag it is, by pairing this date with `fetched_at` (when WE last read the
 * account). A recent fetch against an old valuation is AIRS's batch and there is nothing to do; an
 * old fetch is ours, and that is the one a refresh fixes.
 */

/** Trading (Mon–Fri) days between `asOf` (YYYY-MM-DD) and today. 0 when asOf is today or in the
 *  future. Weekends are skipped so a Friday snapshot read on Monday is 1 trading day old, not 3. */
export function businessDaysBehind(asOf: string): number {
  const start = new Date(`${asOf}T00:00:00`);
  if (Number.isNaN(start.getTime())) return 0;
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  start.setHours(0, 0, 0, 0);
  let n = 0;
  const cur = new Date(start);
  // Bounded so a garbage/very-old date can never spin: no AIRS snapshot is years old.
  for (let i = 0; i < 3650 && cur < today; i += 1) {
    cur.setDate(cur.getDate() + 1);
    const dow = cur.getDay();
    if (dow !== 0 && dow !== 6) n += 1;
  }
  return n;
}

export type SnapshotTone = 'fresh' | 'ok' | 'stale';

export function snapshotFreshness(asOf: string | null | undefined):
  { days: number; tone: SnapshotTone; label: string } | null {
  if (!asOf) return null;
  const days = businessDaysBehind(asOf);
  if (days <= 0) return { days, tone: 'fresh', label: 'today' };
  if (days === 1) return { days, tone: 'ok', label: '1 trading day old' };
  return { days, tone: 'stale', label: `${days} trading days old` };
}

/**
 * WHOSE lag an amber badge is describing — the source's, or ours.
 *
 * ⚠⚠ AN AMBER BADGE THAT NAMES NO ACTIONABLE CAUSE IS A DEAD END, and this one named the WRONG
 * action for weeks. `asOf` is the day AIRS VALUED a book; `fetchedAt` is when we last READ it. Only
 * the second is ours. Measured 2026-08-17, straight after a full "Refresh all": 31 accounts
 * re-scanned, the newest valuation AIRS returned for any of them 2026-08-15, and twenty still dated
 * 2026-08-11/12 — 32 of 40 rows amber, none of them clearable by refreshing.
 *
 * Returns null when there is nothing useful to say: the snapshot is not stale, or we do not know
 * when we last fetched. Inventing a verdict for a caller that cannot supply `fetchedAt` would be a
 * worse failure than the silence it replaces.
 *
 * `ours` is `fetchedAt` two or more trading days back — the same threshold `snapshotFreshness`
 * uses, because "we have not read it since the day before yesterday" is the same claim about a
 * different clock, and two thresholds would eventually disagree about one row.
 */
export type LagOwner = { side: 'source' | 'ours'; days: number; text: string };

export function lagOwner(
  asOf: string | null | undefined, fetchedAt: string | null | undefined,
): LagOwner | null {
  if (snapshotFreshness(asOf)?.tone !== 'stale') return null;
  if (!fetchedAt) return null;
  const days = businessDaysBehind(fetchedAt.slice(0, 10));
  if (days <= 1) {
    return { side: 'source', days,
      text: `We read this account from AIRS ${days === 0 ? 'today' : 'yesterday'}, so this is `
        + 'simply the newest valuation AIRS has for it — refreshing again cannot produce one it '
        + 'has not published.' };
  }
  return { side: 'ours', days,
    text: `We last read this account ${days} trading days ago, so our copy is behind — a Refresh `
      + 'will pull whatever AIRS has now.' };
}

/** "as of <date>" with a freshness tone: faint when fresh/expected, amber ⚠ when genuinely stale
 *  (≥2 trading days). The tooltip explains that AIRS values only change on a scan. */
export function SnapshotAge({ asOf, prefix = 'as of' }: {
  asOf?: string | null; prefix?: string;
}) {
  const f = snapshotFreshness(asOf);
  if (!asOf || !f) return null;
  const tone = f.tone === 'stale' ? 'text-warn-300'
    : f.tone === 'ok' ? 'text-fg-faint' : 'text-fg-subtle';
  return (
    <span className={tone}
      title={`AIRS snapshot dated ${asOf} — ${f.label}. This is the newest valuation AIRS HAS for `
        + 'this book; the scan already walks back to the most recent available one, so refreshing '
        + 're-reads AIRS but cannot produce a valuation AIRS has not published.'}>
      {f.tone === 'stale' && '⚠ '}{prefix} <span className="font-mono">{asOf}</span>
      {f.tone !== 'fresh' && <> · {f.label}</>}
    </span>
  );
}
