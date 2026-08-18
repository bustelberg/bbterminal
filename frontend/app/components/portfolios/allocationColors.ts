// The asset-class palette, shared by the allocation bar (PortfolioAnalysisModal) and the per-holding
// "Class" column (PortfolioOverviewPanel) so the two can never disagree.
//
// MAXIMALLY DISTINCT, with NO red and NO green (the CVD-confusable pair): the invested classes
// take the hues the no-red/no-green arc allows — blue, amber, magenta — and the two non-invested
// buckets are neutral greys (the only achromatic slices, so they read as "not a sleeve").
// Validated, not eyeballed: worst-pair CVD ΔE 8.7 (above the 8 target), normal-vision ΔE 17.5, via
// the dataviz `validate_palette.js`. Keys match `_airs_holding_isin.BUCKET_ORDER`.
//
// ⚠ THE TEAL IS GONE WITH `Equity ETF` (retired 2026-08-18 — an equity ETF invests in equity, so
// it is Stocks). Blue/teal were the CLOSEST pair in the palette, which is what the ΔE 8.7 above
// measured; dropping the split leaves the remaining hues further apart than the validation
// required, so the figure is now a floor rather than the actual worst case. ⚠ Re-run
// `validate_palette.js` before adding a sixth colour — do not assume the old headroom is still
// described by this comment.
export const ALLOC_COLOR: Record<string, string> = {
  'Equity': '#2f6fd0',       // blue
  'Bonds': '#c9992f',        // amber / gold
  'Alternatives': '#b04a9c', // magenta
  'Cash': '#6c757f',         // neutral grey — not invested
  'Unclassified': '#aeb6bf', // light grey
};

/** The bucket colour, falling back to Cash grey for an unknown label. */
export const allocColor = (bucket?: string | null) => (bucket && ALLOC_COLOR[bucket]) || '#6c757f';

/** The five Class labels, in order — mirrors `_airs_holding_isin.BUCKET_ORDER`. The manual-override
 *  picker offers these plus an "Auto" (revert to the calculated class). */
export const BUCKET_ORDER = Object.keys(ALLOC_COLOR);

/**
 * The classes the Analyse modal shows ALWAYS — in the bar AND in the holdings table — even when
 * the book holds none of them.
 *
 * ⚠⚠ MIRRORS `_airs_portfolio_analysis._ALWAYS_SHOWN`, AND THE TWO MUST NOT DRIFT. The backend
 * decides which slices exist (the bar, and the table's group ORDER); this decides which groups the
 * table keeps after filtering out the empty ones. A class listed in one and not the other appears
 * as a bar with no table section, or a table section under no bar — the two halves of one screen
 * disagreeing about what the book contains.
 *
 * ⚠ WHY AT ALL: an omitted class cannot state a zero. Three rows where there were four reads as
 * "bonds not computed" exactly as much as "no bonds held", and the reader has to remember which
 * fourth is missing to tell. It also restores the policy overlay's most important case — a
 * Defensief book holding NO bonds against a 55% minimum previously had no bar to draw the breach
 * on, so the largest possible violation was the one thing the bands could not show.
 *
 * ⚠ `Unclassified` IS NOT HERE, deliberately. It is not something anyone allocates to — it is our
 * own failure to classify, so an empty one is good news and printing it on every healthy book
 * advertises a problem that does not exist. It still appears the moment it has rows.
 */
export const ALWAYS_SHOWN_BUCKETS = ['Equity', 'Bonds', 'Alternatives', 'Cash'] as const;

/** The Cash bucket's label, named once. ⚠ Cash is the one class whose return is KNOWN without
 *  being computed — zero — so several places have to recognise it; a string literal repeated at
 *  each of them is a rename away from silently reclassifying cash as an ordinary asset. */
export const CASH_BUCKET = 'Cash';

/** The one class that CONTAINS operating companies — the only place owner earnings can apply.
 *
 *  ⚠ EVERY OTHER CLASS IS SOMETHING ELSE, NOT A WEAKER VERSION OF THIS ONE. `Alternatives` is
 *  crypto and commodities, which have no earnings at all. `Bonds` is a claim on a company, not a
 *  share of it: earnings per share answers a question a bondholder is not asking. `Unclassified`
 *  is by definition unknown.
 *
 *  ⚠⚠ AND THIS BUCKET IS NO LONGER SUFFICIENT ON ITS OWN — IT USED TO BE, WHICH IS THE TRAP. Until
 *  `Equity ETF` was retired (2026-08-18) an equity ETF had its own bucket, so `bucket === 'Equity'`
 *  meant "an operating company" and every owner-earnings gate in the app was written against it
 *  alone. Stocks now holds the ETFs too, and a fund has no earnings to blend (this app deliberately
 *  does not look through funds). EVERY such gate must therefore ALSO test `!is_fund`, the flag the
 *  backend now carries per holding for exactly this reason — see `BookHoldingDetail.is_fund`. */
export const EQUITY_BUCKET = 'Equity';

// DISPLAY labels only — the stored/computed bucket KEY stays "Equity" everywhere
// (classify_bucket, overrides, filtering, colours all key off it); we just show "Stocks" to the
// reader. Change the label here and nowhere else.
//
// ⚠ `Equity ETF` -> `Stock ETF` LIVED HERE AND IS GONE with the bucket (2026-08-18). Leaving the
// entry behind would have been harmless-looking and wrong: `bucketLabel` falls through to the key
// itself, so a stale `Equity ETF` arriving from an old client or an un-migrated row would render
// as "Stock ETF" — a slice with a name, a place in the legend and no colour, reading as a live
// class rather than as data that should not exist.
const BUCKET_LABEL: Record<string, string> = {
  'Equity': 'Stocks',
};

/** The reader-facing name for a bucket key (Equity → Stocks); identity for the rest. */
export const bucketLabel = (bucket?: string | null) => (bucket && BUCKET_LABEL[bucket]) || bucket || '';
