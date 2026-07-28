/**
 * Quick Valuation: a company's SHARE PRICE against its FREE CASH FLOW PER SHARE, over the last ten
 * fiscal years. Both are per-share amounts in the same reporting currency, from the same fiscal
 * rows, so the comparison needs no FX and no rebasing to be meaningful — and where it IS rebased,
 * the anchor is stated.
 *
 * The question it answers: has the price followed the cash the business generates per share, or has
 * the multiple done the work? Those are different reasons to be up 200%.
 *
 * ⚠ SINGLE COMPANY ONLY. There is no portfolio share and no portfolio FCF per share; the amounts
 * sit in different currencies and cannot be summed. The portfolio-level version of this question is
 * the FCF-SBC yield card, which is currency-free by construction.
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

export type YearPoint = { year: number; price: number | null; fcf: number | null };

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

/** The two series paired by fiscal year, oldest first, capped to the last `years` fiscal years
 *  either series reports. A year present in only one is KEPT with a null on the other side — the
 *  gap is information (a company that stopped reporting FCF is not a company with a flat FCF). */
export function priceVsFcf(metrics: MetricRow[], years = 10): YearPoint[] {
  const price = byYear(metrics, PRICE_CODES);
  const fcf = byYear(metrics, FCF_PS_CODES);
  const all = [...new Set([...price.keys(), ...fcf.keys()])].sort((a, b) => a - b);
  return all.slice(-years).map((year) => ({
    year, price: price.get(year) ?? null, fcf: fcf.get(year) ?? null,
  }));
}

/**
 * Free cash flow per share ÷ the fiscal year-end price, as a %. The cash the business threw off
 * that year per euro of price — the reciprocal of P/FCF, and the one direction of that pair that
 * stays readable.
 *
 * ⚠ A NEGATIVE YIELD IS A REAL NUMBER AND IS KEPT. This is exactly where a yield and a multiple
 * part company: −20x sorts below every cheap year and reads as the cheapest the stock has ever
 * been, whereas −5% reads as what it is — a year the company burned cash equal to 5% of its price.
 * The ratio does not invert as it crosses zero, so nothing has to be dropped.
 */
export function fcfYieldOf(fcf: number | null, price: number | null): number | null {
  if (fcf == null || price == null) return null;
  if (!(price > 0)) return null;
  return fcf / price * 100;
}

/**
 * The share price a per-share cash flow implies at a demanded yield: `fcf/share ÷ yield`.
 *
 * ⚠ A NON-POSITIVE YIELD HAS NO PRICE, AND NEITHER DOES A NEGATIVE CASH FLOW. At 0% the division
 * is infinite (any price is justified by no cash flow); at a negative yield it flips sign and
 * returns a positive-looking figure built on nonsense. Both come back null rather than as a number
 * a reader would have no reason to distrust.
 */
export function priceAtYield(fcfPerShare: number | null, yieldPct: number | null): number | null {
  if (fcfPerShare == null || yieldPct == null) return null;
  if (!(yieldPct > 0) || !(fcfPerShare > 0)) return null;
  const v = fcfPerShare / (yieldPct / 100);
  return Number.isFinite(v) ? v : null;
}

/** Annualised change from `from` to `to` over `years` — the return the two prices imply. */
export function cagrBetween(from: number | null, to: number | null, years: number): number | null {
  if (from == null || to == null || !(from > 0) || !(to > 0) || !(years > 0)) return null;
  const v = Math.pow(to / from, 1 / years) - 1;
  return Number.isFinite(v) ? v : null;
}

export type PriceTarget = {
  currentFcfPs: number | null;
  currentPrice: number | null;
  currentYield: number | null;     // percent
  forecastFcfPs: number | null;
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
 */
export function priceTarget(
  currentFcfPs: number | null, currentPrice: number | null,
  forecastFcfPs: number | null, forecastYield: number | null, years: number,
): PriceTarget {
  const currentYield = currentFcfPs != null && currentPrice != null && currentPrice > 0
    ? currentFcfPs / currentPrice * 100 : null;
  const forecastPrice = priceAtYield(forecastFcfPs, forecastYield);
  return {
    currentFcfPs, currentPrice, currentYield, forecastFcfPs, forecastYield, forecastPrice,
    cagr: cagrBetween(currentPrice, forecastPrice, years),
  };
}

export type Rebased = { anchor: number | null; rows: { year: number; price: number | null; fcf: number | null }[] };

/**
 * Both series as an index, 100 at the anchor year — the shape that makes "price ran ahead of cash"
 * visible at all, since €700 of price and €20 of FCF/share share no axis.
 *
 * ⚠ THE ANCHOR IS THE FIRST YEAR BOTH ARE POSITIVE, NOT THE FIRST YEAR ON THE CHART. Rebasing off
 * a cash-burn year is division by a negative: every later point flips sign, and the chart draws a
 * company whose cash flow "fell" while it was in fact recovering. If no year has both positive
 * there is no index — `anchor` is null and the caller must not draw one.
 */
export function rebase(points: YearPoint[]): Rebased {
  const a = points.find((p) => p.price != null && p.price > 0 && p.fcf != null && p.fcf > 0);
  if (!a) return { anchor: null, rows: [] };
  const p0 = a.price as number;
  const f0 = a.fcf as number;
  return {
    anchor: a.year,
    rows: points.map((p) => ({
      year: p.year,
      price: p.price != null && p.price > 0 ? 100 * p.price / p0 : null,
      // A negative FCF year is a real observation and it belongs on the index — it plots below
      // zero, which is exactly what a cash burn looks like against a base of 100.
      fcf: p.fcf != null ? 100 * p.fcf / f0 : null,
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
