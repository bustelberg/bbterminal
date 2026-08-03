/**
 * Quick Valuation: a company's SHARE PRICE against ONE per-share earning power series, over the last
 * ten fiscal years. Both are per-share amounts in the same reporting currency, from the same fiscal
 * rows, so the comparison needs no FX and no rebasing to be meaningful — and where it IS rebased,
 * the anchor is stated.
 *
 * The question it answers: has the price followed what the business earns per share, or has the
 * multiple done the work? Those are different reasons to be up 200%.
 *
 * ⚠ TWO BASES, ONE SET OF MATHS. The series is either FREE CASH FLOW PER SHARE or EPS (`BASIS`),
 * picked by a switch in the tab. Everything below is deliberately basis-agnostic — `value`, not
 * `fcf` — because the arithmetic genuinely is the same and a second copy of it, forked per basis,
 * is a second place for the index anchor and the yield sign convention to drift. What is NOT the
 * same is what the two numbers MEAN, and that difference lives in `BASIS` as copy the UI prints:
 * cash the business threw off vs accounting profit after non-cash charges. They diverge for real
 * companies and by a lot; a reader must be told which one is on screen.
 *
 * ⚠ SINGLE COMPANY ONLY. There is no portfolio share and no portfolio FCF/EPS per share; the
 * amounts sit in different currencies and cannot be summed. The portfolio-level version of this
 * question is the FCF-SBC yield card, which is currency-free by construction.
 */

export type MetricRow = { metric_code: string; target_date: string; numeric_value: number | null };

// ⚠ THREE SECTION SPELLINGS, AS EVERYWHERE ELSE. GuruFocus renamed its statement sections and
// `metric_data` holds whichever was current when a company was last fetched — capitalised
// `Per Share Data`, lowercase `per_share_data`, and `per_share_data_array` for part of the
// lowercase cohort. Match one and a whole cohort of companies reads as having no data.
export const PRICE_CODES = [
  'annuals__Per Share Data__Month End Stock Price',
  'annuals__per_share_data__Month End Stock Price',
  'annuals__per_share_data_array__Month End Stock Price',
];
export const FCF_PS_CODES = [
  'annuals__Per Share Data__Free Cash Flow per Share',
  'annuals__per_share_data__Free Cash Flow per Share',
  'annuals__per_share_data_array__Free Cash Flow per Share',
];
// ⚠ `EPS without NRI`, NOT `EPS (Diluted)`. GuruFocus carries both, and they differ by exactly the
// one-offs — an impairment, a disposal gain, a restructuring charge. The stripped series is the one
// the REST OF THIS APP already values on (`egmInputs.ts`, `earnings/types.ts`), so reading raw
// diluted EPS here would make this tab quietly disagree with the EGM and Deep Valuation tabs about
// the same company in the same modal. It is also the right series for a trend: a single write-off
// year otherwise drops out of the log fit entirely (a loss has no logarithm) and takes the R² with
// it, reporting a business as erratic when its accountants had one busy year.
export const EPS_PS_CODES = [
  'annuals__Per Share Data__EPS without NRI',
  'annuals__per_share_data__EPS without NRI',
  'annuals__per_share_data_array__EPS without NRI',
];

// The analyst consensus behind the FORWARD multiple, in priority order.
//
// ⚠ `eps_nri_estimate` FIRST, BECAUSE THE HISTORY IS NRI-STRIPPED. GuruFocus publishes both, and
// they are not the same number — AB Sagax's 2026 consensus is 12.09 on the NRI line and 13.30 on
// the other, 10% apart. Dividing today's price by the wrong one steps the multiple at the exact
// point where history hands over to forecast, and that step reads as a re-rating when it is pure
// bookkeeping. (`_asset_financials` deliberately prefers the OTHER one — it charts forward EPS
// standalone, with no NRI-stripped history for it to line up against.)
//
// ⚠ AND IT IS A PRIORITY LIST, NOT A UNION: `forwardEstimates` takes the first code that returns
// anything and stops. Filling 2027 from one series and 2028 from the other would put that same
// convention step INSIDE the forecast, where nothing marks it at all.
export const EPS_EST_CODES = ['annual_eps_nri_estimate', 'annual_per_share_eps_estimate'];

/** ⚠ `value`, NOT `fcf` — it holds EPS half the time. See the module note. */
export type YearPoint = { year: number; price: number | null; value: number | null };

export type Basis = 'fcf' | 'eps';

/**
 * What the switch actually switches: the metric codes, and the copy that keeps the panel honest
 * about which measure is on screen.
 *
 * ⚠ THE CAVEATS ARE NOT DECORATION AND THEY ARE NOT INTERCHANGEABLE. An FCF yield and an earnings
 * yield answer different questions and disagree loudly for capital-intensive businesses, for
 * companies with heavy stock compensation, and for anyone mid-acquisition. Rendering one under the
 * other's label would not look broken — it would look like a valuation.
 */
export const BASIS: Record<Basis, {
  /** The switch's own label. */
  tab: string;
  /** The per-share series' name, as it appears in headings, row labels and table lines. */
  perShare: string;
  /** Sentence-initial ("Earnings yield") vs mid-sentence ("Current earnings yield") — two fields
   *  because `"FCF yield".toLowerCase()` is "fcf yield", and no capitalisation helper survives an
   *  acronym. */
  yieldTitle: string;
  yieldInline: string;
  codes: string[];
  /** GuruFocus's own line name — the `where` of every info card on this tab. */
  source: string;
  /** What this measure IS, in one clause. */
  what: string;
  /** The caveat that belongs to THIS measure and to no other. */
  caveat: string;
  /** What a negative year is called. A cash-burn year and a loss year are not the same event. */
  negativeYear: string;
  /** How the multiple is written: `P/E`, `P/FCF`. */
  multiple: string;
  /** The analyst-consensus codes for the FORWARD multiple, or null when no consensus EXISTS for
   *  this measure — in which case there is NO forward half to the chart. Null is a fact about the
   *  vendor, not a gap in our ingest, and not an invitation to model one — see `forwardSource`. */
  estimateCodes: string[] | null;
  /** Where the forward numerator comes from, or why there is none. */
  forwardSource: string;
}> = {
  fcf: {
    tab: 'FCF',
    perShare: 'FCF / share',
    yieldTitle: 'FCF yield',
    yieldInline: 'FCF yield',
    codes: FCF_PS_CODES,
    source: 'GuruFocus `Free Cash Flow per Share`, as reported.',
    what: 'the cash the business threw off per share, after the capital spending needed to keep it running',
    caveat: '⚠ NOT SBC-ADJUSTED. The FCF-SBC cards on the Long Equity tab subtract stock compensation and are a lower number; this one is free cash flow as reported.',
    negativeYear: 'cash-burn',
    multiple: 'P/FCF',
    // ⚠ NO ANALYST FCF CONSENSUS EXISTS, AND THE ANSWER IS TO SHOW NO FORWARD — not to model one.
    // GuruFocus publishes EPS, revenue, EBIT, EBITDA, net income and OPERATING cash flow per share
    // — not free cash flow, because capex is not forecast. Two substitutes were tried and both are
    // refused:
    //
    //   OCF/share consensus  — OCF is FCF plus capex (Apple FY2025: 111,482 vs 98,767, 13% apart),
    //                          so a forward multiple on it sits ~13% BELOW the P/FCF history it is
    //                          plotted against. "Cheap on forward cash flow" straight out of a
    //                          swapped numerator.
    //   our own trend fit    — shipped briefly, then removed. It rendered identically to a
    //                          consensus: same line weight, same decimals, same axis. A badge is
    //                          not enough to make a house extrapolation read differently from what
    //                          the market actually expects, and a chart that has to warn you not to
    //                          believe half of it should not be drawing that half.
    //
    // A multiple is a fact about a price and a filing. Neither substitute is one.
    estimateCodes: null,
    forwardSource: 'There is no forward P/FCF, because no analyst publishes a free-cash-flow forecast. The chart shows the multiple this company has actually traded at, and where it trades today — every point measured, none projected.',
  },
  eps: {
    tab: 'EPS',
    perShare: 'EPS',
    yieldTitle: 'Earnings yield',
    yieldInline: 'earnings yield',
    codes: EPS_PS_CODES,
    source: 'GuruFocus `EPS without NRI` — diluted earnings per share with non-recurring items stripped out.',
    what: 'the accounting profit attributable to one share, after depreciation and other non-cash charges',
    caveat: '⚠ ACCRUAL, NOT CASH — and the two diverge for real companies, not just in theory: a capital-intensive business earns well and converts little, and revenue booked is not revenue collected. ⚠ The yield built on it is the inverse of the P/E.',
    negativeYear: 'loss',
    multiple: 'P/E',
    estimateCodes: EPS_EST_CODES,
    forwardSource: 'GuruFocus analyst consensus — the `EPS without NRI` estimate, the same basis as the history it continues.',
  },
};

/** {year: value} for a set of code spellings — the LATEST observation in each fiscal year (a
 *  company that changed its year-end reports twice in one year; the later close is the year's). */
function byYear(metrics: MetricRow[], codes: string[]): Map<number, number> {
  const want = new Set(codes);
  const latest = new Map<number, { date: string; value: number }>();
  for (const m of metrics) {
    if (!want.has(m.metric_code) || m.numeric_value == null) continue;
    const year = parseInt(String(m.target_date).slice(0, 4), 10);
    if (!Number.isFinite(year)) continue;
    const cur = latest.get(year);
    if (!cur || m.target_date > cur.date) latest.set(year, { date: m.target_date, value: m.numeric_value });
  }
  return new Map([...latest].map(([y, v]) => [y, v.value]));
}

/**
 * The `target_date` of the LATEST observation among `codes` — the fiscal year END the newest
 * point belongs to, not just its year.
 *
 * ⚠ THE DAY MATTERS, BECAUSE THE PRICE IS NOW LIVE. The forecast sits a fixed number of years
 * past this date, so the horizon a live price is annualised over is the distance from TODAY to
 * it — anywhere from one to two years, depending how long ago the company last reported. Rounding
 * that to "2 years" understates the CAGR by however stale the accounts are, which is exactly the
 * staleness this change exists to stop hiding.
 */
export function latestDateOf(metrics: MetricRow[], codes: string[]): string | null {
  const want = new Set(codes);
  let best: string | null = null;
  for (const m of metrics) {
    if (!want.has(m.metric_code) || m.numeric_value == null) continue;
    const d = String(m.target_date);
    if (best == null || d > best) best = d;
  }
  return best;
}

/** `iso` shifted `n` calendar years, as an ISO date. Feb 29 lands on Mar 1 in a common year,
 *  which is what `Date` does and is immaterial at this resolution. */
export function addYears(iso: string | null, n: number): string | null {
  if (!iso) return null;
  const d = new Date(`${iso}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return null;
  d.setUTCFullYear(d.getUTCFullYear() + n);
  return d.toISOString().slice(0, 10);
}

/**
 * Years from `from` to `to`, as a decimal.
 *
 * Null when either date is missing or the window has closed — a CAGR needs a positive horizon,
 * and a target date already in the past is not one. (It happens: a company that has not filed in
 * over two years has a forecast year that is already here.)
 */
export function yearsBetween(from: string | null, to: string | null): number | null {
  if (!from || !to) return null;
  const a = new Date(`${from}T00:00:00Z`).getTime();
  const b = new Date(`${to}T00:00:00Z`).getTime();
  if (Number.isNaN(a) || Number.isNaN(b)) return null;
  const y = (b - a) / (365.25 * 24 * 3600 * 1000);
  return y > 0 ? y : null;
}

/** The price and ONE per-share series paired by fiscal year, oldest first, capped to the last
 *  `years` fiscal years either series reports. A year present in only one is KEPT with a null on
 *  the other side — the gap is information (a company that stopped reporting FCF is not a company
 *  with a flat FCF). `codes` selects the basis; see `BASIS`. */
export function priceVsMetric(
  metrics: MetricRow[], codes: string[] = FCF_PS_CODES, years = 10,
): YearPoint[] {
  const price = byYear(metrics, PRICE_CODES);
  const value = byYear(metrics, codes);
  const all = [...new Set([...price.keys(), ...value.keys()])].sort((a, b) => a - b);
  return all.slice(-years).map((year) => ({
    year, price: price.get(year) ?? null, value: value.get(year) ?? null,
  }));
}

/**
 * The per-share series ÷ the fiscal year-end price, as a %. What the business earned that year per
 * euro of price — the reciprocal of P/FCF (or, on the EPS basis, of the P/E), and the one direction
 * of that pair that stays readable.
 *
 * ⚠ A NEGATIVE YIELD IS A REAL NUMBER AND IS KEPT. This is exactly where a yield and a multiple
 * part company: −20x sorts below every cheap year and reads as the cheapest the stock has ever
 * been, whereas −5% reads as what it is — a year the company burned cash (or lost money) equal to
 * 5% of its price. The ratio does not invert as it crosses zero, so nothing has to be dropped.
 */
export function yieldOf(value: number | null, price: number | null): number | null {
  if (value == null || price == null) return null;
  if (!(price > 0)) return null;
  return value / price * 100;
}

/**
 * The consensus for the fiscal years still AHEAD of `after`, oldest first.
 *
 * ⚠ FIRST CODE THAT ANSWERS WINS — it is a priority list, never a union (see `EPS_EST_CODES`).
 * ⚠ `after` DROPS ESTIMATES THAT HAVE BEEN OVERTAKEN. GuruFocus keeps an estimate row for a year
 * the company has since reported, so without this the "forward" ladder opens with a forecast of a
 * year we already have the actual for — the same year appearing twice, once measured and once
 * guessed, with no way for a reader to tell which point is which.
 */
export function forwardEstimates(
  metrics: MetricRow[], codes: string[], after: number | null,
): { year: number; value: number }[] {
  for (const code of codes) {
    const rows = [...byYear(metrics, [code]).entries()]
      .filter(([year, v]) => (after == null || year > after) && Number.isFinite(v))
      .sort((a, b) => a[0] - b[0])
      .map(([year, value]) => ({ year, value }));
    if (rows.length) return rows;
  }
  return [];
}

/**
 * The middle multiple, not the average one.
 *
 * ⚠ A MEAN IS THE WRONG CENTRE FOR A MULTIPLE. One year where earnings nearly touched zero prints
 * a 300× that no reader would call typical, and it drags a ten-point mean by ~30×. The median
 * ignores it — which is the correct treatment, because that year says something about the earnings
 * denominator, not about what the market pays for this business.
 */
export function medianOf(xs: number[]): number | null {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
}

/**
 * The share price a per-share amount implies at a demanded yield: `value/share ÷ yield`.
 *
 * ⚠ A NON-POSITIVE YIELD HAS NO PRICE, AND NEITHER DOES A NEGATIVE CASH FLOW OR A LOSS. At 0% the
 * division is infinite (any price is justified by no earnings); at a negative yield it flips sign
 * and returns a positive-looking figure built on nonsense. Both come back null rather than as a
 * number a reader would have no reason to distrust.
 */
export function priceAtYield(valuePerShare: number | null, yieldPct: number | null): number | null {
  if (valuePerShare == null || yieldPct == null) return null;
  if (!(yieldPct > 0) || !(valuePerShare > 0)) return null;
  const v = valuePerShare / (yieldPct / 100);
  return Number.isFinite(v) ? v : null;
}

/** Annualised change from `from` to `to` over `years` — the return the two prices imply. */
export function cagrBetween(from: number | null, to: number | null, years: number): number | null {
  if (from == null || to == null || !(from > 0) || !(to > 0) || !(years > 0)) return null;
  const v = Math.pow(to / from, 1 / years) - 1;
  return Number.isFinite(v) ? v : null;
}

export type PriceTarget = {
  /** Per share, on whichever `Basis` is switched on — FCF or EPS. */
  currentPs: number | null;
  currentPrice: number | null;
  currentYield: number | null;     // percent
  forecastPs: number | null;
  forecastYield: number | null;    // percent
  forecastPrice: number | null;
  cagr: number | null;             // decimal, annualised
};

/**
 * The whole price-target calculation in one place.
 *
 * ⚠ ONE COMPUTATION, TWO READERS. The calculator panel prints these and the chart draws the price
 * line out to `forecastPrice`; computing it twice would let the line land somewhere the panel does
 * not say. The chart's projected price IS this figure, not a second estimate of it.
 *
 * ⚠ `years` IS THE HORIZON FROM `currentPrice`'s OWN DATE, NOT THE PROJECTION'S LENGTH. The two
 * were the same while the price was the fiscal year-end close; with a live price the forecast is
 * nearer than two years away, and passing 2 anyway divides the return by too big a number —
 * silently, and by more the more stale the accounts are. `yearsBetween` computes it.
 */
export function priceTarget(
  currentPs: number | null, currentPrice: number | null,
  forecastPs: number | null, forecastYield: number | null, years: number,
): PriceTarget {
  const currentYield = currentPs != null && currentPrice != null && currentPrice > 0
    ? currentPs / currentPrice * 100 : null;
  const forecastPrice = priceAtYield(forecastPs, forecastYield);
  return {
    currentPs, currentPrice, currentYield, forecastPs, forecastYield, forecastPrice,
    cagr: cagrBetween(currentPrice, forecastPrice, years),
  };
}

export type Rebased = { anchor: number | null; rows: { year: number; price: number | null; value: number | null }[] };

/**
 * Both series as an index, 100 at the anchor year — the shape that makes "price ran ahead of the
 * business" visible at all, since €700 of price and €20 of FCF/share share no axis.
 *
 * ⚠ THE ANCHOR IS THE FIRST YEAR BOTH ARE POSITIVE, NOT THE FIRST YEAR ON THE CHART. Rebasing off
 * a cash-burn (or loss) year is division by a negative: every later point flips sign, and the chart
 * draws a company whose cash flow "fell" while it was in fact recovering. If no year has both
 * positive there is no index — `anchor` is null and the caller must not draw one.
 */
export function rebase(points: YearPoint[]): Rebased {
  const a = points.find((p) => p.price != null && p.price > 0 && p.value != null && p.value > 0);
  if (!a) return { anchor: null, rows: [] };
  const p0 = a.price as number;
  const f0 = a.value as number;
  return {
    anchor: a.year,
    rows: points.map((p) => ({
      year: p.year,
      price: p.price != null && p.price > 0 ? 100 * p.price / p0 : null,
      // A negative year is a real observation and it belongs on the index — it plots below zero,
      // which is exactly what a cash burn (or a loss) looks like against a base of 100.
      value: p.value != null ? 100 * p.value / f0 : null,
    })),
  };
}

/** Compound annual growth between the first and last POSITIVE observation of a series, with the
 *  window it spans. Null when fewer than two positive points — a CAGR off one point is not a rate,
 *  and one off a negative start is not a number. */
export function cagrOf(points: YearPoint[], pick: (p: YearPoint) => number | null) {
  const xs = points.map((p) => ({ year: p.year, v: pick(p) }))
    .filter((x): x is { year: number; v: number } => x.v != null && x.v > 0);
  if (xs.length < 2) return null;
  const first = xs[0];
  const last = xs[xs.length - 1];
  const n = last.year - first.year;
  if (n <= 0) return null;
  return { from: first.year, to: last.year, years: n, pct: (Math.pow(last.v / first.v, 1 / n) - 1) * 100 };
}
