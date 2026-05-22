/**
 * Single canonical builder for GuruFocus stock-summary URLs.
 *
 * GuruFocus URL convention: US-listed names go bare
 * (`/stock/AAPL/summary`), everything else gets an exchange prefix
 * (`/stock/XSWX:NESN/summary`). Mismatched URLs (a US name with `NYSE:`
 * prefix or a Swiss name without prefix) produce 404s; this helper
 * centralizes the rule so every caller in the frontend resolves to the
 * same URL for the same security.
 *
 * The same logic lives in `backend/ingest/gurufocus_url.py` — keep
 * them in sync if you change either.
 */

// GuruFocus exchange codes that produce a bare URL (no prefix). `US`
// is a catch-all on a few legacy rows; `CBOE BZX` is the iShares
// fund-file variant of the canonical `CBOE` DB code.
const GURUFOCUS_US_EXCHANGES = new Set([
  'NYSE', 'NASDAQ', 'AMEX', 'CBOE', 'CBOE BZX', 'US',
]);

// Frontend payloads sometimes carry literal "None"/"nan"/etc. when an
// exchange link is missing on the backend (pandas/JSON serialization
// + saved-bundle universes from before the snapshot-normalization fix).
// Treat them as no-exchange so we don't synthesize broken
// `None:TICKER`-style URLs. Exported so display-only helpers (the
// "(EXCH)" parens after a ticker) can apply the same fallback rules
// and stay consistent with the link's resolution.
export const EMPTY_EXCHANGE_TOKENS = new Set(['', 'NONE', 'NAN', 'NULL', 'UNDEFINED']);

/**
 * Heuristic: when the exchange link is genuinely missing, guess from
 * the ticker's shape. 4-5 digit numerics are almost always Hong Kong
 * (e.g. 01988 Bank of China, 00700 Tencent). Other patterns (LSE `.L`
 * suffixes, SHSE/SZSE 6-digit numerics, …) are left to expand later —
 * most real-world breakage comes from HKSE. Returns '' when no
 * confident guess is possible; callers fall back to the bare ticker.
 */
export function inferExchangeFromTicker(ticker: string): string {
  if (/^\d{4,5}$/.test(ticker)) return 'HKSE';
  return '';
}

/** Return the canonical GuruFocus summary URL. Never returns null —
 * falls back to a bare-ticker URL when the exchange is missing and no
 * heuristic guess works. */
export function guruFocusUrl(ticker: string, exchange: string | null | undefined): string {
  const t = (ticker ?? '').toUpperCase().trim();
  let e = (exchange ?? '').toUpperCase().trim();
  if (GURUFOCUS_US_EXCHANGES.has(e)) {
    return `https://www.gurufocus.com/stock/${t}/summary`;
  }
  if (EMPTY_EXCHANGE_TOKENS.has(e)) {
    e = inferExchangeFromTicker(t);
    if (!e) {
      // No exchange and no confident guess — bare ticker is the safest
      // bet (works for US-listed names).
      return `https://www.gurufocus.com/stock/${t}/summary`;
    }
  }
  return `https://www.gurufocus.com/stock/${e}:${t}/summary`;
}
