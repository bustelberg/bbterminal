/**
 * Small pure helpers for the `/schedule` page sub-components.
 * Display-formatting only — no React, no I/O.
 */
import type { CSSProperties } from 'react';
import { WEEKDAY_LABELS } from '../momentum/utils';

const FREQ_DISPLAY: Record<string, string> = {
  daily: 'Daily', weekly: 'Weekly', monthly: 'Monthly',
  bimonthly: 'Bimonthly', quarterly: 'Quarterly',
};

// Pretty display name per universe key (the raw keys are SHOUTY).
const UNIVERSE_DISPLAY: Record<string, string> = {
  LEONTEQ: 'Leonteq',
  ACWI: 'ACWI',
  ACWI_LEONTEQ: 'ACWI×Leonteq',
  SP500: 'S&P 500',
};

function universeName(u: string): string {
  return UNIVERSE_DISPLAY[u] ?? (u.charAt(0).toUpperCase() + u.slice(1).toLowerCase());
}

/** One config-derived property chip: its text + a fixed hue so the COLOUR
 * encodes which property it is (frequency=blue, direction=green, …). */
export type StrategyChip = { text: string; hue: number };

/** Derive the labelled property chips for a scheduled strategy from its
 * config blob — what the /schedule row shows after the name. Each property
 * category has a stable hue so the colour itself signals the property. */
export function strategyChips(
  cfg: Record<string, unknown> | null,
  frequency?: string | null,
): StrategyChip[] {
  if (!cfg) return [];
  const chips: StrategyChip[] = [];

  const freq = frequency ?? (cfg.rebalance_frequency as string | undefined);
  if (freq) chips.push({ text: FREQ_DISPLAY[freq] ?? (freq.charAt(0).toUpperCase() + freq.slice(1)), hue: 212 });

  const dir = (cfg.strategy_type as string | undefined) ?? 'long_only';
  chips.push({ text: dir === 'long_short' ? 'Long-short' : 'Long-only', hue: 150 });

  const uni = (cfg.index_universe as string | null | undefined)
    ?? (cfg.universe_label as string | null | undefined);
  if (uni) chips.push({ text: universeName(uni), hue: 34 });

  const grouping = (cfg.grouping as string | undefined) ?? 'sector';
  chips.push({ text: grouping === 'industry' ? 'By industry' : 'By sector', hue: 186 });

  const minScore = cfg.min_price_score as number | null | undefined;
  if (minScore != null && minScore > 0) chips.push({ text: `Min ${minScore}`, hue: 276 });

  const wd = (cfg.rebalance_weekday as number | undefined) ?? 0;
  chips.push({ text: `${WEEKDAY_LABELS[wd] ?? 'Monday'} rebalance`, hue: 246 });

  const topS = cfg.top_n_sectors as number | undefined;
  if (topS != null) chips.push({ text: `Top ${topS} sector${topS === 1 ? '' : 's'}`, hue: 320 });

  const topP = cfg.top_n_per_sector as number | undefined;
  if (topP != null) chips.push({ text: `Top ${topP} compan${topP === 1 ? 'y' : 'ies'}`, hue: 96 });

  return chips;
}

/** Inline style for a property chip (the @theme tokens only cover 4 colour
 * ramps, so qualitative per-property hues use HSL inline — same pattern as
 * `company/styles.ts::universeChipStyle` + `lib/sectorColors`). */
export function chipStyle(hue: number): CSSProperties {
  // Light-theme pill: a bright pastel fill, a vivid border, and deep saturated
  // text — high contrast + vibrant on the white "Paper" surfaces. (Was a dark
  // translucent fill + light text tuned for the old dark theme.)
  return {
    backgroundColor: `hsl(${hue} 95% 93%)`,
    borderColor: `hsl(${hue} 75% 55%)`,
    color: `hsl(${hue} 78% 30%)`,
  };
}

/** Compact "Xd Yh Zm Ws" duration for a non-negative second count — seconds are
 * ALWAYS the final unit so every timer reads live down to the second. Leading
 * zero-units are dropped ("12m 07s", "07s"); sub-units are zero-padded when a
 * larger unit precedes them so the width stays stable as it ticks. */
export function formatDur(totalSec: number): string {
  const s = Math.max(0, Math.floor(totalSec));
  const days = Math.floor(s / 86400);
  const hours = Math.floor((s % 86400) / 3600);
  const mins = Math.floor((s % 3600) / 60);
  const secs = s % 60;
  const pad = (n: number) => String(n).padStart(2, '0');
  if (days > 0) return `${days}d ${hours}h ${pad(mins)}m ${pad(secs)}s`;
  if (hours > 0) return `${hours}h ${pad(mins)}m ${pad(secs)}s`;
  if (mins > 0) return `${mins}m ${pad(secs)}s`;
  return `${secs}s`;
}

/** Relative time to the second, both directions: "in 5m 07s" / "3h 02m 15s ago"
 * / "now". Relative to `nowMs`; returns '—' when null/unparseable. */
export function relTime(iso: string | null, nowMs: number): string {
  if (!iso) return '—';
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return '—';
  const diffSec = Math.round((t - nowMs) / 1000);
  if (diffSec === 0) return 'now';
  return diffSec > 0 ? `in ${formatDur(diffSec)}` : `${formatDur(-diffSec)} ago`;
}

/** Exact execution time: weekday, date, HH:MM, and the viewer's timezone
 * abbreviation (e.g. "Mon, 30 Jun 2026, 12:00 GMT+2"). Rendered in the
 * browser's local timezone so "when it executes" reads in the user's own clock.
 * Returns '—' when null/unparseable. */
export function formatExecAt(iso: string | null): string {
  if (!iso) return '—';
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return '—';
  return new Date(t).toLocaleString(undefined, {
    weekday: 'short', day: '2-digit', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit', timeZoneName: 'short',
  });
}

/** Precise countdown to a future ISO timestamp, to the second: "2d 5h 12m 07s
 * left" / "5h 12m 07s left" / "12m 07s left" / "07s left" / "now". Relative to
 * `nowMs`; returns '—' when null. */
export function countdownLeft(iso: string | null, nowMs: number): string {
  if (!iso) return '—';
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return '—';
  const diffSec = Math.round((t - nowMs) / 1000);
  if (diffSec <= 0) return 'now';
  return `${formatDur(diffSec)} left`;
}
