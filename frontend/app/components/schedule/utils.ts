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
