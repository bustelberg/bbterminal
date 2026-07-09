// Shared formatting for AlphaLab risk/return figures.

export const pct = (x: number) => `${(x * 100).toFixed(1)}%`;
export const ratio = (x: number | null) => (x == null ? '—' : x.toFixed(2));

/** Sharpe/Sortino colour — ≥1 good (green), <0 bad (red), 0–1 mediocre (neutral). */
export const ratioColor = (x: number | null) =>
  x == null ? 'text-fg-faint' : x >= 1 ? 'text-pos-400' : x < 0 ? 'text-neg-400' : 'text-fg';

/** Return colour — green when up, red when down. */
export const retColor = (x: number) => (x >= 0 ? 'text-pos-400' : 'text-neg-400');
