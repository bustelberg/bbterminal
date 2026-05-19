/**
 * Shared per-sector color palette. The same mapping is used by:
 *   - /backtest's `SectorTimelineChart.tsx` (Gantt bars colored by sector)
 *   - /schedule's `ScheduledStrategyDetail.tsx` (sector chips on each run row)
 *
 * Same hue-spread as the Gantt so a sector that appears in /schedule's
 * sector chip is instantly recognizable as the same one shown in a /backtest
 * sector chart. Single source of truth lives here; both consumers import it.
 *
 * Unknown sectors fall back to a small alternate palette, cycled by index
 * so adjacent unknowns still look distinct. Callers pass the sector's
 * index in the visible set (e.g. the array of sectors currently on screen)
 * — this only matters for the fallback path; mapped sectors are stable
 * regardless of the index.
 */

export const SECTOR_COLORS: Record<string, string> = {
  'Information Technology': '#3b82f6',
  'Communication Services': '#ec4899',
  'Health Care': '#10b981',
  Financials: '#06b6d4',
  'Consumer Discretionary': '#f97316',
  'Consumer Staples': '#92400e',
  Industrials: '#a855f7',
  Energy: '#ef4444',
  Utilities: '#fbbf24',
  Materials: '#84cc16',
  'Real Estate': '#14b8a6',
};

export const FALLBACK_PALETTE = [
  '#0891b2', '#7c3aed', '#16a34a', '#dc2626', '#d97706',
  '#0284c7', '#c026d3', '#65a30d',
];

export function colorForSector(sector: string, idx: number): string {
  return SECTOR_COLORS[sector] ?? FALLBACK_PALETTE[idx % FALLBACK_PALETTE.length];
}
