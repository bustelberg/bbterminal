/**
 * Shared formatters for the `/companies` manager so the static row and the
 * inline edit row render values identically.
 */

/** Format an absolute EUR market cap compactly (€3.95T / €420.5B / €88.0M). */
export function fmtMktCapEur(v: number): string {
  if (v >= 1e12) return `€${(v / 1e12).toFixed(2)}T`;
  if (v >= 1e9) return `€${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `€${(v / 1e6).toFixed(1)}M`;
  return `€${Math.round(v).toLocaleString()}`;
}
