/**
 * Small pure helpers for the `/schedule` page sub-components.
 * Display-formatting only — no React, no I/O.
 */

/** Curated subset of the strategy config to display on the strategy list
 * row + the add picker. The full breakdown lives in the detail view. */
export function strategySummary(cfg: Record<string, unknown> | null): string {
  if (!cfg) return '';
  const selection = (cfg.selection_mode as string | undefined) ?? 'momentum';
  const universe = (cfg.index_universe as string | null | undefined) ?? (cfg.universe_label as string | null | undefined) ?? 'all';
  const topSectors = cfg.top_n_sectors as number | undefined;
  const topPer = cfg.top_n_per_sector as number | undefined;
  const parts: string[] = [selection];
  if (universe) parts.push(`${universe}`);
  if (topSectors != null && topPer != null) parts.push(`top ${topSectors}×${topPer}`);
  return parts.join(' · ');
}

/** Compact "in 18h" / "in 6d" / "in 12m" / "now" relative formatter for a
 * future ISO timestamp, relative to `nowMs`. Returns '—' when null. */
export function relTime(iso: string | null, nowMs: number): string {
  if (!iso) return '—';
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return '—';
  const diffSec = Math.round((t - nowMs) / 1000);
  if (diffSec <= 0) return 'now';
  const m = Math.round(diffSec / 60);
  if (m < 60) return `in ${m}m`;
  const h = Math.round(diffSec / 3600);
  if (h < 48) return `in ${h}h`;
  const d = Math.round(diffSec / 86400);
  return `in ${d}d`;
}
