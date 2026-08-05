// The asset-class palette, shared by the allocation bar (PortfolioAnalysisModal) and the per-holding
// "Class" column (PortfolioOverviewPanel) so the two can never disagree.
//
// MAXIMALLY DISTINCT, with NO red and NO green (the CVD-confusable pair): the four invested classes
// take the four hues the no-red/no-green arc allows — blue, teal, amber, magenta — and the two
// non-invested buckets are neutral greys (the only achromatic slices, so they read as "not a
// sleeve"). Validated, not eyeballed: worst-pair CVD ΔE 8.7 (above the 8 target), normal-vision
// ΔE 17.5, via the dataviz `validate_palette.js`. Keys match `_airs_holding_isin.BUCKET_ORDER`.
export const ALLOC_COLOR: Record<string, string> = {
  'Equity': '#2f6fd0',       // blue
  'Equity ETF': '#17a7b3',   // teal
  'Bonds': '#c9992f',        // amber / gold
  'Alternatives': '#b04a9c', // magenta
  'Cash': '#6c757f',         // neutral grey — not invested
  'Unclassified': '#aeb6bf', // light grey
};

/** The bucket colour, falling back to Cash grey for an unknown label. */
export const allocColor = (bucket?: string | null) => (bucket && ALLOC_COLOR[bucket]) || '#6c757f';

/** The six Class labels, in order — mirrors `_airs_holding_isin.BUCKET_ORDER`. The manual-override
 *  picker offers these plus an "Auto" (revert to the calculated class). */
export const BUCKET_ORDER = Object.keys(ALLOC_COLOR);

/** The Cash bucket's label, named once. ⚠ Cash is the one class whose return is KNOWN without
 *  being computed — zero — so several places have to recognise it; a string literal repeated at
 *  each of them is a rename away from silently reclassifying cash as an ordinary asset. */
export const CASH_BUCKET = 'Cash';

// DISPLAY labels only — the stored/computed bucket KEY stays "Equity" / "Equity ETF" everywhere
// (classify_bucket, overrides, filtering, colours all key off it); we just show "Stocks" to the
// reader. Change the label here and nowhere else.
const BUCKET_LABEL: Record<string, string> = {
  'Equity': 'Stocks',
  'Equity ETF': 'Stock ETF',
};

/** The reader-facing name for a bucket key (Equity → Stocks); identity for the rest. */
export const bucketLabel = (bucket?: string | null) => (bucket && BUCKET_LABEL[bucket]) || bucket || '';
