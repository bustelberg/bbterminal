/**
 * Shared per-sector color palette. The same mapping is used by:
 *   - /backtest's `SectorTimelineChart.tsx` (Gantt bars colored by sector)
 *   - /schedule's `ScheduledStrategyDetail.tsx` (sector chips on each run row)
 *
 * The DB carries TWO different sector taxonomies depending on the source
 * universe:
 *   - ACWI / iShares  → GICS names ("Financials", "Materials",
 *     "Information Technology", "Health Care", "Communication Services", …)
 *   - LEONTEQ / GuruFocus → GuruFocus names ("Financial",
 *     "Basic Materials", "Technology", "Healthcare",
 *     "Communication Services", "Consumer Cyclical", "Consumer Defensive",
 *     "Capital Goods", "Transportation", "Services", "Real Estate", …)
 *
 * Both naming conventions are mapped explicitly below so every sector
 * the data can actually produce gets a stable, distinct color. The
 * fallback path is for genuinely unknown labels — and it hashes by
 * NAME (not by position), so two unmapped sectors never collide just
 * because they happen to land at adjacent indices in a row.
 */

// Each color is picked to be visually distinct from every other color
// in this set against the dark #0f1117 background. If you add a new
// sector make sure to pick a hue that isn't already in use here.
export const SECTOR_COLORS: Record<string, string> = {
  // ── Technology + Communications ─────────────────────────────────
  'Information Technology': '#3b82f6',  // blue
  Technology:               '#3b82f6',  // blue (GF alias)
  'Communication Services': '#ec4899',  // pink
  Communication:            '#ec4899',  // pink (legacy short form)

  // ── Health Care ─────────────────────────────────────────────────
  'Health Care':            '#10b981',  // emerald
  Healthcare:               '#10b981',  // emerald (GF alias)

  // ── Financials ──────────────────────────────────────────────────
  Financials:               '#06b6d4',  // cyan
  'Financial Services':     '#06b6d4',  // cyan (GF alias)
  Financial:                '#06b6d4',  // cyan (GF short form)

  // ── Consumer ────────────────────────────────────────────────────
  'Consumer Discretionary': '#f97316',  // orange
  'Consumer Cyclical':      '#f97316',  // orange (GF alias)
  'Consumer Staples':       '#92400e',  // brown
  'Consumer Defensive':     '#92400e',  // brown (GF alias)
  'Consumer Goods':         '#92400e',  // brown (legacy alias)

  // ── Industrials / Capital Goods / Transport ─────────────────────
  Industrials:              '#a855f7',  // purple
  'Capital Goods':          '#7c3aed',  // violet (GF — distinct from Industrials)
  Transportation:           '#0ea5e9',  // sky

  // ── Energy ──────────────────────────────────────────────────────
  Energy:                   '#ef4444',  // red

  // ── Utilities ───────────────────────────────────────────────────
  Utilities:                '#fbbf24',  // amber

  // ── Materials ───────────────────────────────────────────────────
  Materials:                '#84cc16',  // lime
  'Basic Materials':        '#84cc16',  // lime (GF alias)

  // ── Real Estate ─────────────────────────────────────────────────
  'Real Estate':            '#14b8a6',  // teal

  // ── Services (GF, conglomerate-style services bucket) ───────────
  Services:                 '#f43f5e',  // rose
};

// Larger palette for genuinely-unknown sectors. We pick by hash of the
// name, so the same unknown sector always gets the same color and two
// distinct names rarely collide (the chance is ~1/N for N entries).
// 16 hues across the wheel — enough headroom that a row of 4–6 unknown
// sectors landing on the same hue is very unlikely.
export const FALLBACK_PALETTE = [
  '#0891b2', '#7c3aed', '#16a34a', '#dc2626',
  '#d97706', '#0284c7', '#c026d3', '#65a30d',
  '#9333ea', '#059669', '#e11d48', '#0d9488',
  '#ca8a04', '#1d4ed8', '#be185d', '#15803d',
];

/** Deterministic non-cryptographic string hash (djb2 variant). Returns
 * a non-negative integer. Same string → same number across renders, so
 * an unmapped sector keeps the same fallback color forever. */
function _hashSector(name: string): number {
  let h = 5381;
  for (let i = 0; i < name.length; i++) {
    // (h * 33) XOR char — classic djb2.
    h = ((h << 5) + h) ^ name.charCodeAt(i);
  }
  // Coerce to unsigned 32-bit.
  return h >>> 0;
}

/**
 * Pick the color for a sector. Mapped sectors get their hard-coded
 * color; unmapped sectors get a hash-derived color from FALLBACK_PALETTE.
 *
 * `idx` is preserved in the signature for backwards-compat with callers
 * that currently pass a row index — it's now ignored. The hash-by-name
 * approach is collision-resistant regardless of caller position, which
 * is what fixes the "Financial and Basic Materials share a color"
 * complaint.
 */
export function colorForSector(sector: string, _idx?: number): string {
  if (sector in SECTOR_COLORS) return SECTOR_COLORS[sector];
  if (!sector) return FALLBACK_PALETTE[0];
  return FALLBACK_PALETTE[_hashSector(sector) % FALLBACK_PALETTE.length];
}
