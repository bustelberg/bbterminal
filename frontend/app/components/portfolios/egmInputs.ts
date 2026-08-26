/**
 * Pulling the EGM's four inputs (+ two reference hints) out of the company metrics payload the
 * modal already loads. Pure, so the metric-code traps below are unit-tested rather than discovered
 * as a wrong valuation.
 *
 * ⚠ NOTHING HERE RE-FETCHES. `/api/earnings/by-isin/{isin}/metrics` already returns every code —
 * daily closes, the forward-P/E indicator, the fiscal statement lines and the analyst estimates —
 * so a second request would only be a second chance to disagree with the other tabs.
 */

import { type MetricRow } from './quickValuation';

// ⚠ THREE NAMING SCHEMES IN ONE STREAM. A statement line is `annuals__Section__Line`, an analyst
// estimate is `annual_<field>` (SINGULAR, no section, no double underscore), and the forward P/E is
// `indicator_q_forward_pe_ratio` — an indicator, a third scheme again. Each is matched explicitly;
// a pattern that catches two of the three leaves its panel reading "n/a" beside ones that filled in.
const PRICE_CODE = 'close_price';
const FWD_PE_CODE = 'indicator_q_forward_pe_ratio';
const EPS_EST_CODE = 'annual_per_share_eps_estimate';
// Both section spellings, and the quarterly twin — a fresher point-in-time yield than the last
// fiscal year's, which can be nine months stale.
const DIV_YIELD_CODES = [
  'quarterly__Valuation Ratios__Dividend Yield %',
  'quarterly__valuation_ratios__Dividend Yield %',
  'annuals__Valuation Ratios__Dividend Yield %',
  'annuals__valuation_ratios__Dividend Yield %',
];
const EPS_CODES = [
  'annuals__Per Share Data__EPS without NRI',
  'annuals__per_share_data__EPS without NRI',
  'annuals__per_share_data_array__EPS without NRI',
];
const PRICE_PS_CODES = [
  'annuals__Per Share Data__Month End Stock Price',
  'annuals__per_share_data__Month End Stock Price',
  'annuals__per_share_data_array__Month End Stock Price',
];

// ── The reverse-DCF's inputs ────────────────────────────────────────────────────────────────
const SHARES_CODES = [
  'annuals__Income Statement__Shares Outstanding (Diluted Average)',
  'annuals__income_statement__Shares Outstanding (Diluted Average)',
];
const WACC_CODES = [
  'annuals__Ratios__WACC %',
  'annuals__ratios__WACC %',
];
const FCF_CODES = [
  'annuals__Cashflow Statement__Free Cash Flow',
  'annuals__cashflow_statement__Free Cash Flow',
];
/** The three lines the Reverse DCF normalises FCF with — see `normalisedFcf` for the arithmetic. */
const SBC_CODES = [
  'annuals__Cashflow Statement__Stock Based Compensation',
  'annuals__cashflow_statement__Stock Based Compensation',
];
const CAPEX_CODES = [
  'annuals__Cashflow Statement__Capital Expenditure',
  'annuals__cashflow_statement__Capital Expenditure',
];
/**
 * ⚠ THE **CASH FLOW** DEPRECIATION LINE, NOT THE INCOME STATEMENT'S. GuruFocus files both
 * (`Income Statement__Depreciation, Depletion and Amortization` and this one) and they are not
 * always equal. Capex comes off the cash flow statement, so its maintenance proxy has to as well —
 * comparing a cash figure with an accrual one is a difference in basis dressed up as growth spend.
 */
const DEP_CODES = [
  'annuals__Cashflow Statement__Cash Flow Depreciation, Depletion and Amortization',
  'annuals__cashflow_statement__Cash Flow Depreciation, Depletion and Amortization',
];
/**
 * The consensus OPERATING cash flow for the next fiscal year — the forward base's FALLBACK leg.
 *
 * ⚠⚠ `analyst_estimate` HAS NO FREE-CASH-FLOW KEY, AND FOR A WHILE THIS FILE CONCLUDED THAT THE
 * API HAD NONE ANYWHERE. Its annual block is revenue · ebit · ebitda · net_income · pretax_income ·
 * eps_nri · per_share_eps · operating_cash_flow(+_per_share) · book_value_per_share · dividend ·
 * gross_margin · roa · roe · pettm, plus the `future_*_growth` means — no FCF, no capex. That is
 * still true and the conclusion still did not follow: the figure is one endpoint over, in
 * `keyratios` (see `FCF_EST_CODE`). ⚠ The lesson is the shape of the search, not the field: this
 * app's catalogue already listed `keyratios` as real, and the ⚠ that the LEGACY API NEVER 404s
 * had made everyone probe for new PATHS while the answer was inside a payload we already had.
 *
 * ⚠ THE INGEST NEEDED NO CHANGE FOR THIS ONE: `_parse_analyst_estimates` stores every list-valued
 * key as `annual_<key>`, and `load_company_metric_rows` reads every `annual_%` prediction row, so
 * this code is already in the payload the modal loads.
 */
const OCF_EST_CODE = 'annual_operating_cash_flow_estimate';
/**
 * The consensus FREE cash flow — GuruFocus's own figure, not ours.
 *
 * ⚠⚠ IT IS REAL AND IT IS UNDOCUMENTED. `stock/{sym}/keyratios` → `Fundamental` →
 * `Estimated Free Cash Flow for Next FY1 End (M)`, ingested by `ingest/earnings/key_ratios.py`.
 * The endpoint had been in `gurufocus_api.json` as "real" the whole time; nobody had opened its
 * 264-key `Fundamental` section, so this app spent a while believing the field was Excel-only.
 *
 * ⚠ SAME CONSENSUS AS `OCF_EST_CODE` — AAPL's operating-cash-flow estimate reads 148323.41 in
 * `analyst_estimate` and 148323.411 here — so the two can be subtracted to get the consensus capex
 * (Meta FY2026: 134,330.10 − 5,412.45 = 128,917.65).
 *
 * ⚠ ABSENT FOR MOST COMPANIES TODAY, because the fetch is ON DEMAND. `forwardFcf` falls back to
 * `OCF_est − |capex|`; see its docstring for why that approximation is sound for the figure the
 * model values and where it is not.
 */
const FCF_EST_CODE = 'annual_fcf_estimate';
/** ⚠ FORWARD D&A IS `EBITDA_est − EBIT_est` — inferred, not published, and the only forward
 *  maintenance-capex proxy available. Both codes come from `analyst_estimate`, same consensus as
 *  the FCF and OCF estimates. See `normalisedFcf.forwardLegs`. */
const EBITDA_EST_CODE = 'annual_ebitda_estimate';
const EBIT_EST_CODE = 'annual_ebit_estimate';

/**
 * What GuruFocus calls each figure, for a card's `Where`.
 *
 * ⚠ EXPORTED FROM THE CONSTANTS THE READ ACTUALLY USES, never hand-typed into a tooltip. A code
 * spelled out in prose drifts the moment the read changes and there is nothing to catch it — the
 * card would then name a field this app does not read.
 *
 * ⚠ THE FIRST SPELLING, where a metric has two. Both are read (GuruFocus renamed its statement
 * sections); this is the one a reader searching the vendor's own screens will find.
 */
export const SOURCE_CODES = {
  price: PRICE_CODE,
  forwardPE: FWD_PE_CODE,
  dividendYield: DIV_YIELD_CODES[0],
  epsEstimate: EPS_EST_CODE,
  shares: SHARES_CODES[0],
  wacc: WACC_CODES[0],
  fcf: FCF_CODES[0],
  sbc: SBC_CODES[0],
  capex: CAPEX_CODES[0],
  dep: DEP_CODES[0],
  ocfEstimate: OCF_EST_CODE,
  fcfEstimate: FCF_EST_CODE,
} as const;

/**
 * A stored metric code as the VENDOR names it — `quarterly__Valuation Ratios__Dividend Yield %`
 * becomes `Dividend Yield %`.
 *
 * ⚠ THE STORED CODE IS OUR ENCODING, NOT GURUFOCUS'S NAME. The `annuals__`/`quarterly__` prefix and
 * the `__` separators are this app's; printing them in a card names a key no reader can look up on
 * the vendor's own screens, and reads as a leaked database identifier.
 *
 * ⚠ THE CADENCE PREFIX IS DROPPED ON PURPOSE. Which of the two was read is a question about the
 * WINDOW, and the card's `When` already answers it — carrying it here would say the same thing
 * twice in two vocabularies.
 *
 * ⚠ AN UNKNOWN CODE RETURNS ITSELF. Ugly beats wrong: a card naming the raw key is a bug report;
 * one naming a guessed pretty label is a bug nobody can see.
 */
const VENDOR_NAMES: Record<string, string> = {
  close_price: 'Close price',
  indicator_q_forward_pe_ratio: 'Forward PE Ratio',
  // ⚠ THE ADD-IN'S OWN WORDING for the estimate block — verified against `keyratios`'
  // `Fundamental` section, which is where these are read from.
  annual_fcf_estimate: 'Estimated Free Cash Flow for Next FY1 End',
  annual_operating_cash_flow_estimate: 'Estimated Operating Cash Flow for Next FY1 End',
  annual_ebitda_estimate: 'Estimated EBITDA for Next FY1 End',
  annual_ebit_estimate: 'Estimated EBIT for Next FY1 End',
  annual_per_share_eps_estimate: 'Estimated EPS for Next FY1 End',
  annual_eps_nri_estimate: 'Estimated EPS without NRI for Next FY1 End',
};

export function vendorName(code: string): string {
  const known = VENDOR_NAMES[code];
  if (known) return known;
  const m = /^(?:annuals|quarterly)__(?:.+?)__(.+)$/.exec(code);
  return m ? m[1] : code;
}

export type EgmSource = {
  price: number | null;
  /**
   * The close's OWN date — ⚠ THE PANEL CALLS THIS FIGURE "Share price now" AND HAD NO WAY TO KNOW
   * WHETHER IT WAS. `price` is the newest `close_price` row in the metrics payload, which is as
   * fresh as our stored price series happens to be; if ingest has not run, "now" is whenever it
   * last did. A label making a claim about time, over a number carrying no time, is the same bug
   * `QuickValuationTab` fixed on 2026-07-29 when it was printing a fiscal year-end close under the
   * word "current".
   */
  priceDate: string | null;
  forwardPE: number | null;
  /** ⚠ THE OBSERVATION'S OWN DATE. A card's `When` has to name a moment; "latest observation"
   *  describes the SELECTION RULE and says nothing about how old the figure is. Same reason
   *  `priceDate` rides beside `price`. */
  forwardPEDate: string | null;
  dividendYield: number | null;   // decimal
  epsNextFY: number | null;
  epsNextFYDate: string | null;   // which fiscal period the estimate is for
  analystGrowth5Y: number | null; // decimal — reference only, never fed to the math
  medianPE5Y: number | null;      // reference only
};

/** The most recent observation of any of these codes, whatever its date. */
function latest(metrics: MetricRow[], codes: string[]): { date: string; value: number } | null {
  const want = new Set(codes);
  let best: { date: string; value: number } | null = null;
  for (const m of metrics) {
    if (!want.has(m.metric_code) || m.numeric_value == null) continue;
    if (!best || m.target_date > best.date) best = { date: m.target_date, value: m.numeric_value };
  }
  return best;
}

/** {year: value} — the latest observation within each fiscal year. */
function byYear(metrics: MetricRow[], codes: string[]): Map<number, number> {
  const want = new Set(codes);
  const acc = new Map<number, { date: string; value: number }>();
  for (const m of metrics) {
    if (!want.has(m.metric_code) || m.numeric_value == null) continue;
    const y = parseInt(String(m.target_date).slice(0, 4), 10);
    if (!Number.isFinite(y)) continue;
    const cur = acc.get(y);
    if (!cur || m.target_date > cur.date) acc.set(y, { date: m.target_date, value: m.numeric_value });
  }
  return new Map([...acc].map(([y, v]) => [y, v.value]));
}

/**
 * The consensus figure for the NEXT fiscal year — the earliest estimate dated after `today`.
 *
 * ⚠ NOT THE FIRST ROW IN THE SERIES. The estimate block is stored from whenever it was fetched, so
 * its early periods can already be in the past; taking `[0]` would value the company on a year it
 * has since reported.
 *
 * ⚠ ONE RULE FOR EVERY CONSENSUS LINE. The EGM's EPS and the Reverse DCF's operating cash flow are
 * the same question asked of two codes — "which of these estimates is FY1?" — and answering it
 * twice is how the two panels would come to disagree about which year they are valuing.
 */
export function nextFyEstimate(metrics: MetricRow[], code: string,
  today: string): { date: string; value: number } | null {
  let best: { date: string; value: number } | null = null;
  for (const m of metrics) {
    if (m.metric_code !== code || m.numeric_value == null) continue;
    if (m.target_date <= today) continue;
    if (!best || m.target_date < best.date) best = { date: m.target_date, value: m.numeric_value };
  }
  return best;
}

/** The consensus EPS for the next fiscal year. See `nextFyEstimate` for the selection rule. */
export function nextFyEps(metrics: MetricRow[], today: string): { date: string; value: number } | null {
  return nextFyEstimate(metrics, EPS_EST_CODE, today);
}

/**
 * The compound growth the analyst EPS estimates imply, as a decimal.
 *
 * ⚠ THIS IS NOT GURUFOCUS'S `long_term_growth_rate_mean`, AND THE TWO ARE NOT INTERCHANGEABLE.
 * That field is a SCALAR, and the estimates parser only ingests list-valued fields (correctly — a
 * single number has no target_date to sit on), so it never reaches `metric_data` and cannot be read
 * here. This is the CAGR of the estimate series instead, which GuruFocus publishes separately as
 * `future_per_share_eps_estimate_growth`: near-identical for Apple (13.03 vs 13.01) and materially
 * different for a high-growth name (NVIDIA 45.72 vs 47.57). It is labelled for what it is, and it
 * is reference-only — nothing computes from it.
 */
export type EstimatePoint = { date: string; eps: number };
export type CagrWorking = {
  points: EstimatePoint[]; years: number | null; cagr: number | null;
};

/** The estimate series the CAGR is taken from, alongside the CAGR itself — so the drill-down shows
 *  the arithmetic rather than restating the conclusion. */
export function estimateCagrWorking(metrics: MetricRow[], today: string): CagrWorking {
  const points: EstimatePoint[] = metrics
    .filter((m) => m.metric_code === EPS_EST_CODE && m.numeric_value != null && m.target_date > today)
    .sort((a, b) => a.target_date.localeCompare(b.target_date))
    .map((m) => ({ date: m.target_date, eps: m.numeric_value as number }));
  if (points.length < 2) return { points, years: null, cagr: null };
  const first = points[0];
  const last = points[points.length - 1];
  const years = parseInt(last.date.slice(0, 4), 10) - parseInt(first.date.slice(0, 4), 10);
  if (!(first.eps > 0) || !(last.eps > 0) || years <= 0) {
    return { points, years: years > 0 ? years : null, cagr: null };   // no CAGR out of a loss
  }
  return { points, years, cagr: Math.pow(last.eps / first.eps, 1 / years) - 1 };
}

export function estimateCagr(metrics: MetricRow[], today: string): number | null {
  return estimateCagrWorking(metrics, today).cagr;
}

/**
 * The median P/E of the last five fiscal years, computed from the year-end price and that year's
 * normalised EPS.
 *
 * ⚠ DERIVED, BECAUSE `annuals__Valuation Ratios__PE Ratio` IS NOT IN THE INGESTED SET. A
 * loss-making year has no meaningful P/E and is skipped rather than contributing a negative — a
 * negative multiple would drag the median down and read as "historically cheap".
 */
export type PeYearRow = {
  year: number; price: number | null; eps: number | null; pe: number | null; used: boolean;
};
export type MedianPeWorking = { rows: PeYearRow[]; median: number | null };

/** Every year in the window with its two legs and the resulting multiple, plus the median of the
 *  usable ones. `used: false` marks a year shown but excluded — a loss year, whose negative
 *  multiple would drag the median down and read as "historically cheap". */
export function medianPEWorking(metrics: MetricRow[], years = 5): MedianPeWorking {
  const price = byYear(metrics, PRICE_PS_CODES);
  const eps = byYear(metrics, EPS_CODES);
  const rows: PeYearRow[] = [];
  for (const y of [...price.keys()].sort((a, b) => a - b).slice(-years)) {
    const p = price.get(y) ?? null;
    const e = eps.get(y) ?? null;
    const usable = p != null && e != null && e > 0 && p > 0;
    rows.push({ year: y, price: p, eps: e, pe: usable ? (p as number) / (e as number) : null, used: usable });
  }
  const pes = rows.filter((r) => r.used).map((r) => r.pe as number).sort((a, b) => a - b);
  if (!pes.length) return { rows, median: null };
  const m = Math.floor(pes.length / 2);
  return { rows, median: pes.length % 2 ? pes[m] : (pes[m - 1] + pes[m]) / 2 };
}

export function medianPE(metrics: MetricRow[], years = 5): number | null {
  return medianPEWorking(metrics, years).median;
}

export type YieldObs = { code: string; date: string; pct: number; chosen: boolean };

/** Every `Dividend Yield %` observation on offer, newest first, with the one the panel took.
 *  ⚠ The pick is by DATE alone across all four code spellings — the quarterly rows usually win
 *  because they are fresher, not because quarterly is preferred. */
export function dividendYieldWorking(metrics: MetricRow[]): { rows: YieldObs[]; chosen: YieldObs | null } {
  const want = new Set(DIV_YIELD_CODES);
  const rows: YieldObs[] = metrics
    .filter((m) => want.has(m.metric_code) && m.numeric_value != null)
    .map((m) => ({ code: m.metric_code, date: m.target_date, pct: m.numeric_value as number, chosen: false }))
    .sort((a, b) => b.date.localeCompare(a.date));
  if (!rows.length) return { rows, chosen: null };
  rows[0].chosen = true;
  return { rows, chosen: rows[0] };
}

export type ReverseDcfSource = {
  price: number | null;
  sharesOutstanding: number | null;
  fcf: number | null;
  /** The company's own cost of capital, as a DECIMAL — the discount rate's default. */
  wacc: number | null;
  /** ⚠ THE DATES THE CARDS' `When` FIELDS NAME. They exist on `ReverseDcfWorking` already;
   *  without them the panel could only say "latest close" and "latest fiscal year", which
   *  describe a SELECTION RULE rather than a moment and leave a two-year-old WACC looking
   *  current. */
  priceDate: string | null;
  sharesDate: string | null;
  waccDate: string | null;
  /** The normalisation legs, in the vendor's own signs (capex NEGATIVE). ⚠ Passed through rather
   *  than folded into `fcf`, so the panel can show reported and corrected side by side and a
   *  reader can see which corrections actually ran. See `normalisedFcf`. */
  sbc: number | null;
  capex: number | null;
  dep: number | null;
  /** Consensus OPERATING cash flow for FY1, in millions. ⚠ NOT free cash flow: where `fcfEstimate`
   *  is absent, `forwardFcf` takes capex off this instead. */
  ocfEstimate: number | null;
  /** ⚠ THE VENDOR'S OWN CONSENSUS **FREE** CASH FLOW for FY1, when we have it — `keyratios`, fetched
   *  on demand, so absent for most companies. Preferred over the derivation because it nets a
   *  FORWARD capex estimate we cannot otherwise see: on Meta FY2026 it is 5,412 against the
   *  derivation's 45,005, and the whole 39.6bn gap is capex the company has guided to and has not
   *  yet spent. See `FCF_EST_CODE`. */
  fcfEstimate: number | null;
  /** FY1 consensus EBITDA and EBIT — their difference is the forward D&A. See `EBITDA_EST_CODE`. */
  ebitdaEstimate: number | null;
  ebitEstimate: number | null;
  /** WHICH fiscal year that estimate is for. ⚠ The panel states it: a base labelled "next year"
   *  over a figure whose period nobody named is the same defect `priceDate` fixed for the close. */
  ocfEstimateDate: string | null;
  /** ⚠ THE WINDOW THE FOUR FLOW LEGS ARE MEASURED OVER — trailing twelve months (`ttm: true`, with
   *  the quarter it ends at) or the last fiscal year. The panel LABELS it: 51,075 on one basis and
   *  66,596 on the other is the same row saying two things, and the reader checking against
   *  GuruFocus needs to know which window they are looking at. See `flowLegs`. */
  flowBasis: { ttm: boolean; date: string | null };
};

/**
 * The reverse-DCF's inputs, from the same payload.
 *
 * The share price, the share count, the latest reported free cash flow — and the three lines that
 * normalise it (stock comp, capex, cash-flow depreciation).
 *
 * ⚠ `fcf` IS STILL EXACTLY AS FILED. The normalisation is computed from the legs beside it by
 * `normalisedFcf`, never folded into the source figure — so the panel can show the reported number
 * and the corrected one together, and a reader can see which adjustments actually ran. Folding
 * them in here would make an adjusted figure indistinguishable from a vendor's.
 */
/** One input as it was actually found: what the vendor filed (`raw`), what the model uses (`used`,
 *  sign-normalised where the two differ), and where it came from. */
export type SourceObs = {
  raw: number | null; used: number | null; date: string | null; code: string | null;
  /** ⚠ THE VALUE IS A SUM OF FOUR QUARTERS, NOT THE FIGURE FILED AT `date`. Without this the raw
   *  -data modal renders `quarterly__…Capital Expenditure · 2026-06-30 · −89,325` — a quarter-end
   *  date over a number four times its size, which reads as a vendor error rather than a window. */
  ttm?: boolean;
};

/** `annuals__Cashflow Statement__X` → `quarterly__Cashflow Statement__X`, for both section
 *  spellings. ⚠ THE BACKEND BUILDS THE TWINS THE SAME WAY (`_DASHBOARD_METRIC_CODES`), so every
 *  annual code in this payload has its quarterly counterpart already loaded beside it. */
const quarterlyTwin = (code: string) =>
  (code.startsWith('annuals__') ? `quarterly__${code.slice('annuals__'.length)}` : code);

/**
 * The TRAILING TWELVE MONTHS of a FLOW, from the four newest quarterly filings.
 *
 * ⚠⚠ IT IS WHAT GURUFOCUS'S OWN SCREEN SHOWS, AND THE GAP IS NOT SMALL. Measured on Meta
 * (2026-08-26): the last filed fiscal year has capex −69,691 and D&A 18,616, while the trailing
 * twelve months are **−89,325** and 22,729 — so the growth-capex correction reads 51,075 on the
 * annual basis against 66,596 on the trailing one. A reader checking the panel against GuruFocus
 * finds two different numbers with nothing on either screen to say why.
 *
 * ⚠ SUM, BECAUSE THESE ARE FLOWS. Capex, depreciation, stock comp and free cash flow are all
 * measured OVER a period, so four quarters are a year of it. A STOCK (a balance-sheet line) would
 * be the latest quarter, not the sum — see the backend's `_TTM_RULE`, which declares this per
 * metric precisely because the wrong rule produces a plausible number rather than an error.
 *
 * ⚠⚠ EXACTLY FOUR OR NOTHING. Three quarters summed is a nine-month figure wearing an annual
 * label — smaller than the year it claims to be, in the same direction for every company, and
 * invisible. A company that has not filed four quarters falls back to its annual line instead.
 */
export function ttmObs(metrics: MetricRow[], annualCodes: string[]): SourceObs {
  const want = new Set(annualCodes.map(quarterlyTwin));
  const rows = metrics
    .filter((m) => want.has(m.metric_code) && m.numeric_value != null)
    .sort((a, b) => b.target_date.localeCompare(a.target_date));
  // ⚠ DEDUPED BY DATE. Both section spellings can carry the same quarter, and summing a quarter
  // twice inflates the window by exactly one quarter — again in one direction, again invisibly.
  const byDate = new Map<string, MetricRow>();
  for (const m of rows) if (!byDate.has(m.target_date)) byDate.set(m.target_date, m);
  const four = [...byDate.values()].slice(0, 4);
  if (four.length < 4) return NONE;
  return {
    raw: four.reduce((s, m) => s + (m.numeric_value as number), 0),
    used: four.reduce((s, m) => s + (m.numeric_value as number), 0),
    date: four[0].target_date,
    code: four[0].metric_code,
    ttm: true,
  };
}
export type ReverseDcfWorking = {
  price: SourceObs; shares: SourceObs; fcf: SourceObs; wacc: SourceObs;
  /** The normalisation legs. ⚠ Each may be absent, and an absent one is NOT a zero — see
   *  `normalisedFcf`: a company with no SBC line is not a company that pays none. */
  sbc: SourceObs; capex: SourceObs; dep: SourceObs;
  /** The FY1 consensus operating cash flow. ⚠ ITS `date` IS A FUTURE PERIOD, unlike every other
   *  row here — that is what makes it the forward base rather than another filing. */
  ocfEst: SourceObs;
  /** The FY1 consensus FREE cash flow, where `keyratios` has been fetched for this company. */
  fcfEst: SourceObs;
  ebitdaEst: SourceObs;
  ebitEst: SourceObs;
};

const NONE: SourceObs = { raw: null, used: null, date: null, code: null };

/** The latest observation across a set of codes, keeping the code it came from. */
function latestObs(metrics: MetricRow[], codes: string[], magnitude = false): SourceObs {
  const want = new Set(codes);
  let best: MetricRow | null = null;
  for (const m of metrics) {
    if (!want.has(m.metric_code) || m.numeric_value == null) continue;
    if (!best || m.target_date > best.target_date) best = m;
  }
  if (!best) return NONE;
  const raw = best.numeric_value as number;
  return { raw, used: magnitude ? Math.abs(raw) : raw, date: best.target_date, code: best.metric_code };
}

/**
 * The four flow legs, on ONE basis: trailing twelve months if EVERY one of them has four quarters,
 * the last fiscal year otherwise.
 *
 * ⚠⚠ ONE BASIS, DECIDED ONCE — NOT LEG BY LEG. `normalisedFcf` subtracts stock comp from free cash
 * flow and adds the excess of capex over depreciation: a TTM capex against an annual free cash
 * flow is a split basis, and it would appear only on companies filing three quarters of one line
 * and four of another — i.e. rarely, unpredictably, and with no way to see it. All four move
 * together or none do.
 *
 * ⚠ THE FALLBACK IS THE OLD BEHAVIOUR, so a company GuruFocus files only annually is exactly as it
 * was rather than losing its corrections.
 */
function flowLegs(metrics: MetricRow[]): {
  fcf: SourceObs; sbc: SourceObs; capex: SourceObs; dep: SourceObs;
} {
  const codes = { fcf: FCF_CODES, sbc: SBC_CODES, capex: CAPEX_CODES, dep: DEP_CODES };
  const ttm = {
    fcf: ttmObs(metrics, codes.fcf), sbc: ttmObs(metrics, codes.sbc),
    capex: ttmObs(metrics, codes.capex), dep: ttmObs(metrics, codes.dep),
  };
  // ⚠ SBC IS EXCUSED FROM THE QUORUM, and only SBC. Plenty of companies report none at all, so
  // requiring four quarters of it would drop every other leg back to the annual basis over a line
  // that is legitimately absent. It still takes the TTM window when it has one.
  const all = ttm.fcf.used != null && ttm.capex.used != null && ttm.dep.used != null;
  if (!all) {
    return {
      fcf: latestObs(metrics, codes.fcf), sbc: latestObs(metrics, codes.sbc),
      capex: latestObs(metrics, codes.capex), dep: latestObs(metrics, codes.dep),
    };
  }
  return { ...ttm, sbc: ttm.sbc.used != null ? ttm.sbc : latestObs(metrics, codes.sbc) };
}

/** One FY1 consensus figure as a `SourceObs`. ⚠ THE SAME `nextFyEstimate` RULE for every code —
 *  they share one date axis, so a second selection rule here would let two legs of one arithmetic
 *  land on two different fiscal years. */
function fy1(metrics: MetricRow[], code: string, today: string): SourceObs {
  const f = nextFyEstimate(metrics, code, today);
  return f ? { raw: f.value, used: f.value, date: f.date, code } : NONE;
}

/** A `… %` observation as the DECIMAL the models want, keeping the vendor's figure in `raw`. */
function asDecimal(o: SourceObs): SourceObs {
  return o.used == null ? o : { ...o, used: o.used / 100 };
}

/** Every input the reverse DCF reads, with its provenance — the drill-down's whole content, and
 *  the source `reverseDcfSource` reduces to scalars, so the two cannot disagree. */
export function reverseDcfWorking(metrics: MetricRow[], today: string): ReverseDcfWorking {
  // ⚠ NOT `latestObs`. Every other row here is the newest filing; this one is the EARLIEST period
  // still in the future. The estimate block runs five years out, so "latest" would value the
  // company on a consensus for 2030 — and it can also reach into the PAST, because the block is
  // stored as fetched, which is the trap `nextFyEstimate` exists for.
  const est = nextFyEstimate(metrics, OCF_EST_CODE, today);
  return {
    ocfEst: est
      ? { raw: est.value, used: est.value, date: est.date, code: OCF_EST_CODE }
      : NONE,
    // ⚠ THE SAME FY1 RULE, over a different code — one consensus, one date axis.
    fcfEst: fy1(metrics, FCF_EST_CODE, today),
    ebitdaEst: fy1(metrics, EBITDA_EST_CODE, today),
    ebitEst: fy1(metrics, EBIT_EST_CODE, today),
    price: latestObs(metrics, [PRICE_CODE]),
    shares: latestObs(metrics, SHARES_CODES),
    // ⚠⚠ THE FOUR FLOW LEGS ARE TRAILING TWELVE MONTHS, FALLING BACK TO THE LAST FISCAL YEAR —
    // and they move TOGETHER on purpose. `normalisedFcf` subtracts one from another and adds a
    // third; a TTM capex against an annual free cash flow is a split basis, which is the shape of
    // most of the bugs this file has ever had. All four or none.
    //
    // ⚠ IT IS ALSO WHAT GURUFOCUS'S SCREEN SHOWS. Measured on Meta: capex −69,691 on the last
    // fiscal year against **−89,325** trailing, so the growth-capex row read 51,075 where the
    // vendor's own page says 66,596. See `ttmObs`.
    // ⚠ NOT `magnitude: true` on capex. The vendor files it NEGATIVE and `growthCapex` takes the
    // magnitude itself; normalising the sign here too would leave the drill-down showing a
    // positive number under "as filed", which is the one thing that panel exists to show.
    ...flowLegs(metrics),
    // ⚠ FILED AS A PERCENT, like every other `… %` line — 8.2 means 8.2%. Passed through unscaled
    // it would be an 820% discount rate, and every company on earth would read as worthless.
    wacc: asDecimal(latestObs(metrics, WACC_CODES)),
  };
}

export function reverseDcfSource(metrics: MetricRow[], today: string): ReverseDcfSource {
  const w = reverseDcfWorking(metrics, today);
  return {
    price: w.price.used, sharesOutstanding: w.shares.used, fcf: w.fcf.used, wacc: w.wacc.used,
    priceDate: w.price.date, sharesDate: w.shares.date, waccDate: w.wacc.date,
    sbc: w.sbc.used, capex: w.capex.used, dep: w.dep.used,
    ocfEstimate: w.ocfEst.used, ocfEstimateDate: w.ocfEst.date,
    fcfEstimate: w.fcfEst.used,
    ebitdaEstimate: w.ebitdaEst.used, ebitEstimate: w.ebitEst.used,
    // ⚠ TAKEN OFF CAPEX, which is in the quorum — `sbc` is excused from it and could be the one
    // leg on the other window, so reading the basis off that would mislabel the other three.
    flowBasis: { ttm: w.capex.ttm === true, date: w.capex.date },
  };
}

/** Everything the panel needs, from the one payload. `today` is passed in rather than read from the
 *  clock so the extraction stays pure and testable. */
export function egmSource(metrics: MetricRow[], today: string): EgmSource {
  const eps = nextFyEps(metrics, today);
  const dy = latest(metrics, DIV_YIELD_CODES);
  return {
    price: latest(metrics, [PRICE_CODE])?.value ?? null,
    priceDate: latest(metrics, [PRICE_CODE])?.date ?? null,
    forwardPE: latest(metrics, [FWD_PE_CODE])?.value ?? null,
    forwardPEDate: latest(metrics, [FWD_PE_CODE])?.date ?? null,
    // ⚠ THE FIELD IS NAMED `… %` AND HOLDS PERCENT UNITS — GuruFocus files 0.3 for 0.3%, exactly
    // as it does for `ROE %`. The model wants a decimal, and passing the percent through unscaled
    // would apply a 0.3% payer as a 30% one: on a ten-year compounder that is a ~3.4x fair value.
    dividendYield: dy ? dy.value / 100 : null,
    epsNextFY: eps?.value ?? null,
    epsNextFYDate: eps?.date ?? null,
    analystGrowth5Y: estimateCagr(metrics, today),
    medianPE5Y: medianPE(metrics),
  };
}
