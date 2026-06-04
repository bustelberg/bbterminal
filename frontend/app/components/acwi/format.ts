/**
 * Display formatters shared by the `/acwi` holdings + timeline tables.
 * Pure string→string helpers — no React.
 */

/** Parse + fixed-2-decimal format, passing the raw string through when
 * it isn't numeric. */
export function fmtNum(v: string): string {
  const n = parseFloat(v);
  if (isNaN(n)) return v;
  return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

/** Market-value formatter — abbreviates to $B / $M, falls back to a
 * plain dollar amount. */
export function fmtMv(v: string): string {
  const n = parseFloat(v);
  if (isNaN(n)) return v;
  if (n >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (n >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  return `$${n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

/** Tailwind classes for a constituent-change action badge (ADDED /
 * DELETED / ADDED+DELETED / fallback). */
export function actionStyle(action: string): string {
  switch (action) {
    case 'ADDED': return 'bg-emerald-500/15 text-emerald-400';
    case 'DELETED': return 'bg-rose-500/15 text-rose-400';
    case 'ADDED+DELETED': return 'bg-amber-500/15 text-amber-400';
    default: return 'bg-gray-500/15 text-gray-400';
  }
}
