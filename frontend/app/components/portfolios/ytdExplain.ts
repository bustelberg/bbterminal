/** The YTD derivation dump — `GET /api/airs/model-portfolios/{id}/ytd-explain`, printed to the
 *  browser console so two deployments can be diffed line by line.
 *
 *  Why the console and not the UI: this exists to answer "the same model reads 36.64% here and
 *  44.14% in production", and answering it means putting two full derivations side by side and
 *  looking for the FIRST line that differs. That is a diff, not a view — a panel would have to
 *  be opened twice, in two tabs, and read by eye.
 *
 *  Read the output top-down, because the levels are ordered by how far upstream a cause sits:
 *
 *    load       what this deployment fetched — price transport, windows, freshest close. An
 *               environment that is simply a week behind differs HERE, in one date, and every
 *               leg below it differs as a consequence. Check this before reading any leg.
 *    portfolio  the composition's effective date, the anchor it implies, and the weight that
 *               could actually be priced. A different `positions_datum` means the two
 *               deployments scanned AIRS at different times and are pricing DIFFERENT WEIGHTS
 *               over DIFFERENT WINDOWS — the numbers were never comparable.
 *    legs       one row per composition line, ordered by contribution. `contribution_pp` sums
 *               to the YTD exactly, so the discrepancy is visible as the rows whose
 *               contributions differ, in the order they matter.
 */

/** One composition line, as the backend traced it. Every field is optional because the five
 *  `status` values carry genuinely different facts: an unpriced leg has no marks, and cash has
 *  no ISIN, no series and no listing. */
export type ExplainLeg = {
  isin: string | null;
  fonds: string | null;
  weight: number;
  /** priced · cash · zero_weight · no_execution · no_price_series · no_mark_at_anchor.
   *  The last three are NOT synonyms — see the endpoint's docstring. */
  status: string;
  analysis_id?: number | null;
  yahoo_symbol?: string | null;
  currency?: string | null;
  lookthrough?: boolean;
  series_bars?: number;
  series_first?: string | null;
  series_last?: string | null;
  start_date?: string;
  start_price_eur?: number;
  start_interpolated?: boolean;
  start_gap_days?: number;
  end_date?: string;
  end_price_eur?: number;
  return_pct?: number;
  /** The leg's weight AFTER renormalising over what could be priced — the weight the return
   *  actually carries, which is not the model's own percentage whenever coverage < 100%. */
  weight_pct_of_priced?: number | null;
  contribution_pp?: number | null;
};

export type ExplainTrace = {
  load?: Record<string, unknown>;
  portfolio?: {
    portfolio_id: number;
    name: string;
    positions_datum: string | null;
    positions_scanned_at: string | null;
    ytd_anchor: string;
    anchor_is_inception: boolean;
    total_weight: number;
    priced_weight: number;
    covered_pct: number;
    low_coverage: boolean;
    resolved_holdings: number;
    unresolved_holdings: number;
    interpolated_holdings: number;
    ytd_pct: number | null;
    sum_of_contributions_pp: number;
    reconciles: boolean;
    [k: string]: unknown;
  } | null;
  legs?: ExplainLeg[];
  error?: string;
  [k: string]: unknown;
};

const pct = (v: number | null | undefined, dp = 2) =>
  (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(dp)}%`);

/** The one-line headline: which model, over which window, at what number. */
export function explainHeadline(t: ExplainTrace): string {
  const p = t.portfolio;
  if (!p) return `YTD derivation unavailable — ${t.error ?? 'no trace'}`;
  return `YTD derivation — ${p.name} (#${p.portfolio_id}) = ${pct(p.ytd_pct)} `
    + `from ${p.ytd_anchor}${p.anchor_is_inception ? ' (inception, PARTIAL year)' : ''}`;
}

/** The lines worth reading before any leg: the ones that make every leg differ at once.
 *  Returned as an array of strings rather than logged directly so it can be asserted on. */
export function explainWarnings(t: ExplainTrace): string[] {
  const out: string[] = [];
  const p = t.portfolio;
  if (!p) return out;
  if (!p.reconciles) {
    out.push(`⚠ contributions do NOT sum to the YTD: Σ ${pct(p.sum_of_contributions_pp, 6)} `
      + `vs ${pct(p.ytd_pct, 6)}. The legs below do not explain this number — treat the whole `
      + `dump as suspect and report it.`);
  }
  if (p.low_coverage) {
    out.push(`⚠ coverage ${p.covered_pct.toFixed(1)}% is under the floor — no YTD is returned. `
      + `The unpriced legs below are the reason.`);
  } else if (p.covered_pct < 99.9) {
    out.push(`⚠ only ${p.covered_pct.toFixed(1)}% of the model's weight could be priced, so the `
      + `return is renormalised over ${p.resolved_holdings} of `
      + `${p.resolved_holdings + p.unresolved_holdings} instruments. Two deployments that resolve `
      + `a DIFFERENT number of holdings will disagree on the YTD even with identical prices — `
      + `compare the unpriced legs first.`);
  }
  if (p.anchor_is_inception) {
    out.push(`ⓘ the window opens at the composition's own effective date (${p.positions_datum}), `
      + `not 1 January — this is a PARTIAL year. If the other deployment scanned AIRS at a `
      + `different time its anchor differs, and the two numbers were never measuring the same `
      + `window.`);
  }
  if (p.interpolated_holdings > 0) {
    out.push(`ⓘ ${p.interpolated_holdings} holding(s) were marked at an INTERPOLATED opening `
      + `price (no close near the anchor). Part of this return is an estimate.`);
  }
  return out;
}

/** The leg table, flattened for `console.table` — priced rows first, biggest contribution
 *  first, and the unpriced ones last (they have no contribution to rank by, but they ARE the
 *  renormalisation, so they are shown, never dropped). */
export function explainLegRows(t: ExplainTrace): Record<string, unknown>[] {
  const legs = t.legs ?? [];
  const priced = legs.filter((l) => l.contribution_pp != null);
  const rest = legs.filter((l) => l.contribution_pp == null);
  return [...priced, ...rest].map((l) => ({
    status: l.status + (l.lookthrough ? ' (look-through)' : ''),
    fonds: l.fonds ?? '',
    isin: l.isin ?? '',
    symbol: l.yahoo_symbol ?? '',
    ccy: l.currency ?? '',
    'weight %': l.weight,
    'norm %': l.weight_pct_of_priced ?? null,
    start: l.start_date ?? (l.series_first ? `(first bar ${l.series_first})` : ''),
    'start €': l.start_price_eur ?? null,
    interp: l.start_interpolated ? `yes (${l.start_gap_days}d)` : '',
    end: l.end_date ?? (l.series_last ? `(last bar ${l.series_last})` : ''),
    'end €': l.end_price_eur ?? null,
    'return %': l.return_pct ?? null,
    'contrib pp': l.contribution_pp ?? null,
    bars: l.series_bars ?? null,
  }));
}

/** Print the whole derivation. Collapsed by default — it is several screens, and it is only
 *  read when a number is being questioned. */
export function logYtdExplain(t: ExplainTrace): void {
  console.groupCollapsed(explainHeadline(t));
  if (t.error) console.warn(t.error);
  console.log('load (what THIS deployment fetched — compare first):', t.load);
  console.log('portfolio (window + coverage):', t.portfolio);
  for (const w of explainWarnings(t)) console.warn(w);
  const rows = explainLegRows(t);
  if (rows.length) console.table(rows);
  console.log('raw trace (copy this and diff it against the other environment):', t);
  console.groupEnd();
}
