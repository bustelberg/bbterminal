export type MarketKey = 'nasdaq' | 'acwi' | 'iwqu';
export type PriceLevelRow = { market: MarketKey; drawdown_pct: number };

export function parseDrawdown(value: string): number | null {
  const text = value.trim().replace(',', '.');
  if (!/^\d+(\.\d{1,2})?$/.test(text)) return null;
  const percentage = Number(text);
  return Number.isFinite(percentage) && percentage >= 0.01 && percentage <= 99.99 ? percentage : null;
}

/** Same currency and daily-high ATH as the displayed market snapshot. Hit means hit NOW. */
export function priceLevel(peak: number, latest: number, drawdown: number) {
  if (![peak, latest, drawdown].every(Number.isFinite) || peak <= 0 || latest <= 0 || drawdown <= 0 || drawdown >= 100) return null;
  const target = peak * (1 - drawdown / 100);
  const tolerance = Number.EPSILON * Math.max(peak, latest) * 4;
  return { drawdown, target, hit: latest <= target + tolerance };
}
