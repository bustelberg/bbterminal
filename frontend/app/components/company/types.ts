/**
 * Shared types for the `/companies` manager. Lifted out of
 * `CompanyManager.tsx` so the data hook, the filter hook, and the
 * row/table/modal components share one definition.
 */

export type Company = {
  company_id: number;
  company_name: string | null;
  gurufocus_ticker: string;
  gurufocus_exchange: string;
  /** ISIN — backfilled from GuruFocus + Leonteq. Null for out-of-scope
   * regions (AU/NZ/Russia/…) that GuruFocus doesn't cover. */
  isin?: string | null;
  country: string | null;
  /** Market-cap snapshot in EUR (absolute) + capture date — from the manual
   * "Refresh market caps" button (GuruFocus, converted to EUR). */
  market_cap_eur?: number | null;
  market_cap_date?: string | null;
  /** Latest known sector (from universe_membership), merged in via the slower
   * /api/companies/sectors roundtrip — null until it lands / if the company is
   * in no universe. */
  sector?: string | null;
  universes: string[];
  /** ISO timestamp set by the price phase when GuruFocus returns "delisted"
   * or "stock not found" for this (ticker, exchange). Companies with a
   * non-null value are excluded from the backtest gap warning and the
   * pipeline skips them entirely on subsequent runs. */
  delisted_at?: string | null;
  /** ISO timestamp set when GuruFocus returns "Stock not found" on the
   * primary exchange AND every fallback. Typically means the row's
   * exchange is wrong (e.g. NYSE:ASND when it should be NASDAQ:ASND).
   * UI renders a red "GF lookup" badge + a 'Find correct exchange'
   * button that probes the GuruFocus diagnostic endpoint. Cleared
   * automatically the next time a price fetch succeeds. */
  gurufocus_lookup_failed_at?: string | null;
  /** ISO timestamp set when an override in `gf_ticker_overrides.json`
   * flagged this (ticker, exchange) as `{"unavailable": true, ...}` —
   * the listing is on a real exchange we deliberately don't cover.
   * The reason string is shown in the OUT OF SCOPE badge's tooltip
   * so a user wondering "why isn't this in my backtest" sees an
   * explicit answer instead of a missing row. */
  out_of_scope_at?: string | null;
  out_of_scope_reason?: string | null;
};

export type SortField = 'company_name' | 'gurufocus_ticker' | 'gurufocus_exchange' | 'isin' | 'country' | 'sector' | 'market_cap_eur';
export type SortDir = 'asc' | 'desc';

/** A possible-duplicate match returned by `/api/companies/check-duplicates`,
 * surfaced inline under the add row. */
export type DupeMatch = {
  company_id: number;
  company_name: string | null;
  gurufocus_ticker: string;
  gurufocus_exchange: string | null;
};

/** The pending add payload held while the verify-listing modal is open. */
export type PendingAdd = {
  company_name: string;
  gurufocus_ticker: string;
  gurufocus_exchange: string;
};
