/**
 * Short codes + readable ink for sector chips — the SECONDARY ENCODING beside `sectorColors.ts`.
 *
 * ⚠ WHY COLOUR ALONE CANNOT DO THIS JOB, MEASURED RATHER THAN ASSERTED. The sector palette carries
 * 14 distinct hues, and `dataviz/scripts/validate_palette.js` over all pairs on the light surface
 * reports:
 *
 *     Normal-vision floor  FAIL  #f43f5e <-> #ef4444  (Services <-> Energy)      dE 3.5   floor 15
 *     CVD separation       FAIL  #a855f7 <-> #3b82f6  (Industrials <-> Technology) dE 0.9 deutan
 *     Contrast vs surface  WARN  7 of 14 below 3:1 on white
 *
 * Fourteen categories is roughly double what categorical colour can carry — the rule of thumb is
 * that a 9th series folds into "Other" or gains a second encoding, and every GICS sector matters
 * here so "Other" is not available. Two of those pairs are indistinguishable to a reader with FULL
 * colour vision, which is not a colourblindness edge case; it is the chart being unreadable.
 *
 * ⚠ AND TEXTURE WOULD NOT HAVE FIXED IT. Stripes/crosshatch are the standard answer for a CVD-band
 * failure, but they do not rescue a NORMAL-vision failure, and at a 12px chip a hatch pattern is
 * mush — the pattern needs more pixels than the mark has. A two-letter code is legible at that size,
 * survives greyscale, print and forced-colors mode, and needs no legend lookup at all.
 *
 * The codes are unique by construction: one letter is not enough (Technology/Transportation both
 * start T, and Communication/Consumer Cyclical/Consumer Defensive/Capital Goods all start C).
 */

/** Both taxonomies in the data — GICS ("Information Technology") and GuruFocus ("Technology") —
 *  map to the same code, exactly as they map to the same colour. */
export const SECTOR_CODES: Record<string, string> = {
  'Information Technology': 'TE',
  Technology: 'TE',
  'Communication Services': 'CM',
  Communication: 'CM',
  'Health Care': 'HC',
  Healthcare: 'HC',
  Financials: 'FI',
  'Financial Services': 'FI',
  Financial: 'FI',
  'Consumer Discretionary': 'CD',
  'Consumer Cyclical': 'CD',
  'Consumer Staples': 'CS',
  'Consumer Defensive': 'CS',
  'Consumer Goods': 'CS',
  Industrials: 'IN',
  'Capital Goods': 'CG',
  Transportation: 'TR',
  Energy: 'EN',
  Utilities: 'UT',
  Materials: 'MA',
  'Basic Materials': 'MA',
  'Real Estate': 'RE',
  Services: 'SV',
};

/**
 * Two-letter code for a sector. Unknown labels fall back to their first two alphanumerics.
 *
 * ⚠ The fallback can collide with a mapped code, and that is accepted: an unmapped sector is
 * already off the palette's fixed order, and the full name is on hover and in the legend either
 * way. Silently renaming it to avoid a collision would be worse — the code would stop matching
 * the label a reader sees everywhere else.
 */
export function sectorCode(sector: string | null | undefined): string {
  const s = (sector ?? '').trim();
  if (!s) return '—';
  const mapped = SECTOR_CODES[s];
  if (mapped) return mapped;
  const letters = s.replace(/[^A-Za-z0-9]/g, '');
  return (letters.slice(0, 2) || '—').toUpperCase();
}

/**
 * Readable ink for text sitting ON a filled swatch: near-black or white, whichever contrasts.
 *
 * ⚠ IT HAS TO BE COMPUTED, NOT FIXED. White text is the obvious default and it is unreadable on
 * the light half of this palette — Utilities `#fbbf24` and Materials `#84cc16` are the two the
 * validator already flags at 1.63 and 1.92 against a white surface. Relative luminance per
 * WCAG 2.x; the 0.45 threshold puts the crossover between those light hues and the mid-tone blues.
 */
export function inkForBackground(hex: string): string {
  const m = /^#?([0-9a-f]{6})$/i.exec(hex.trim());
  if (!m) return '#111827';
  const n = parseInt(m[1], 16);
  const lin = (c: number) => {
    const v = c / 255;
    return v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4;
  };
  const L = 0.2126 * lin((n >> 16) & 255) + 0.7152 * lin((n >> 8) & 255) + 0.0722 * lin(n & 255);
  return L > 0.45 ? '#111827' : '#ffffff';
}
