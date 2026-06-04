/**
 * Shared Tailwind class strings + deterministic universe-chip colours for
 * the `/companies` manager. Kept together so the inline edit/add rows and
 * the table header use the exact same input + header styling.
 */
import type { CSSProperties } from 'react';

export const inputCls = 'w-full bg-[#0f1117] border border-gray-700 rounded-lg px-2.5 py-1.5 text-sm text-white focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors';
export const inputAddCls = 'w-full bg-[#0f1117] border border-emerald-800/50 rounded-lg px-2.5 py-1.5 text-sm text-white focus:outline-none focus:border-emerald-500 focus:ring-1 focus:ring-emerald-500/30 transition-colors';
export const thCls = 'px-3 py-3 text-left text-xs font-medium cursor-pointer select-none hover:text-white transition-colors';

// Deterministic hue per universe label so the same universe always gets the
// same chip colour across renders. Cheap string hash → 0-359 hue, with fixed
// saturation + lightness tuned for legibility on the dark theme.
function hashHue(label: string): number {
  let h = 0;
  for (let i = 0; i < label.length; i++) h = (h * 31 + label.charCodeAt(i)) | 0;
  return Math.abs(h) % 360;
}

export function universeChipStyle(label: string): CSSProperties {
  const hue = hashHue(label);
  return {
    backgroundColor: `hsl(${hue} 70% 22% / 0.55)`,
    borderColor: `hsl(${hue} 70% 45% / 0.55)`,
    color: `hsl(${hue} 80% 78%)`,
  };
}
