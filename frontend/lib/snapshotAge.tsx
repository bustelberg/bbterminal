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
      title={`AIRS snapshot dated ${asOf} — ${f.label}. AIRS values only change when the daily scan runs (working days ~11:00 Amsterdam); "Refresh from AIRS" pulls the current book.`}>
      {f.tone === 'stale' && '⚠ '}{prefix} <span className="font-mono">{asOf}</span>
      {f.tone !== 'fresh' && <> · {f.label}</>}
    </span>
  );
}
