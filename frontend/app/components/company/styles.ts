/**
 * Shared Tailwind class strings + deterministic universe-chip colours for
 * the `/companies` manager. Kept together so the inline edit/add rows and
 * the table header use the exact same input + header styling.
 */
import type { CSSProperties } from 'react';

export const inputCls = 'w-full bg-page border border-neutral-700 rounded-lg px-2.5 py-1.5 text-sm text-fg-strong focus:outline-none focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 transition-colors';
export const inputAddCls = 'w-full bg-page border border-pos-800/50 rounded-lg px-2.5 py-1.5 text-sm text-fg-strong focus:outline-none focus:border-pos-500 focus:ring-1 focus:ring-pos-500/30 transition-colors';
export const thCls = 'px-3 py-3 text-left text-xs font-medium cursor-pointer hover:text-fg-strong transition-colors';

// Light-theme pill from a hue: bright pastel fill, vivid border, deep
// saturated text — high contrast + vibrant on the white surfaces.
function hueStyle(hue: number): CSSProperties {
  return {
    backgroundColor: `hsl(${hue} 95% 93%)`,
    borderColor: `hsl(${hue} 75% 50%)`,
    color: `hsl(${hue} 78% 28%)`,
  };
}

/**
 * Build a maximally-distinct colour per universe label. Hashing each label to
 * a hue (the old approach) let the handful of real universes land on near-
 * identical hues. Instead we spread the *current* set of labels evenly around
 * the colour wheel by sorted index, so adjacent chips are always far apart in
 * hue. Deterministic for a given set; pass every label that can appear.
 */
export function buildUniverseStyles(labels: Iterable<string>): Map<string, CSSProperties> {
  const sorted = [...new Set(labels)].sort();
  const n = Math.max(sorted.length, 1);
  const map = new Map<string, CSSProperties>();
  sorted.forEach((label, i) => {
    // Even spread + a small offset so the first chip isn't pure red. A 60°
    // lightness jitter on alternate chips boosts distinctness when two hues
    // land in the same family (e.g. two blues).
    const hue = Math.round((i * 360) / n + 12) % 360;
    map.set(label, hueStyle(hue));
  });
  return map;
}

const FALLBACK_STYLE: CSSProperties = hueStyle(210);

/** Stable single-label style (used where the full set isn't known). Falls back
 * to a fixed accent hue. Prefer `buildUniverseStyles` for the chip set. */
export function universeChipStyle(label: string): CSSProperties {
  let h = 0;
  for (let i = 0; i < label.length; i++) h = (h * 31 + label.charCodeAt(i)) | 0;
  return hueStyle(Math.abs(h) % 360);
}

export { FALLBACK_STYLE };
