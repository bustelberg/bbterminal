/**
 * Shared types + the feasible-exchange set for the `/acwi` reconstruction
 * diagnostics view. Lifted out of `AcwiUniverse.tsx` so the data hook and
 * the per-section table components share one definition.
 */

// GuruFocus exchange prefixes considered "feasible" — regions covered by the
// current GuruFocus subscription: USA + Europe + Asia (incl. Middle East),
// excluding Russia / AU / NZ / Africa / LatAm.
// US stocks are identified separately by gf_exchange === null (empty prefix).
export const FEASIBLE_GF_EXCHANGES = new Set([
  // Europe
  'LSE', 'XTER', 'XPAR', 'XAMS', 'XBRU', 'XLIS', 'MIL', 'XMAD', 'XSWX',
  'OSTO', 'OCSE', 'OSL', 'OHEL', 'WAR', 'XPRA', 'ATH', 'DUB', 'BUD', 'IST',
  // Asia (East / SE / South)
  'TSE', 'HKSE', 'SHSE', 'SZSE', 'TPE', 'ROCO', 'XKRX',
  'NSE', 'BSE', 'SGX', 'XKLS', 'ISX', 'BKK', 'PHS',
  // Middle East
  'SAU', 'DSMD', 'KUW', 'XTAE', 'ADX', 'DFM',
]);

export type Holding = {
  Ticker: string;
  Name: string;
  Sector: string;
  'Asset Class': string;
  'Market Value': string;
  'Weight (%)': string;
  'Notional Value': string;
  Quantity: string;
  Price: string;
  Location: string;
  Exchange: string;
  Currency: string;
  'FX Rate': string;
  gurufocus_url: string | null;
  gf_exchange: string | null;
  gf_currency: string | null;
};

export type Detail = {
  standard: string | null;
  effective_date: string | null;
  loading?: boolean;
  error?: string;
};

export type Announcement = {
  date: string;
  title: string;
  href: string;
  is_constituent_change: boolean;
  is_other_country_coded: boolean;
  detail?: Detail;
};

export type NetAddition = {
  title: string;
  company_name: string;
  country: string;
  date: string;
  effective_date: string | null;
  href: string;
  matched: boolean;
  matched_ticker: string | null;
  matched_name: string | null;
  match_method: string;
};

/** One row of the matched-additions timeline, derived from net additions
 * joined to the feasible holdings (see `useAcwiData`). */
export type TimelineRow = {
  effective_date: string;
  ts: number;
  ticker: string;
  name: string;
  country: string;
  cc: string;
  sector: string;
  gf_exchange: string | null;
  gurufocus_url: string | null;
  href: string;
};

/** `detailSummary` shape — constituent announcements grouped by parsed action. */
export type DetailSummaryGroups = Record<string, { announcement: Announcement; detail: Detail }[]>;
