/**
 * The multiple through TIME — a decade of it, at the resolution the price moves rather than the
 * resolution the accounts are filed.
 *
 * The fiscal-year view answers "what did it trade at each year end"; this answers "what has it
 * traded at", which is the question a reader actually has when they see today's number. Everything
 * here is built from rows the tab ALREADY fetched (`/api/earnings/by-isin/{isin}/metrics` carries
 * 6,933 closes, 513 forward-P/E points and 113 quarterly FCF rows for ASML) — no second request,
 * so this chart cannot disagree with the ones beside it.
 *
 * TWO LINES, AND THEY COME FROM DIFFERENT PLACES:
 *
 *   Forward P/E   GuruFocus PUBLISHES this (`indicator_q_forward_pe_ratio`), weekly, back to 2015.
 *                 It is not computed here. Measured against argenx: dividing the close by it
 *                 recovers the CURRENT fiscal year's consensus EPS (23.20 implied vs 23.23
 *                 published), not a rolling twelve months — so early in a year it looks ~12 months
 *                 ahead and by December it is pricing earnings nearly banked.
 *
 *   Trailing      Computed: price ÷ the per-share figure last REPORTED at that date.
 *
 * ⚠ THERE IS NO FORWARD P/FCF LINE AND THERE CANNOT BE. Nobody forecasts capex, so no vendor
 * publishes a free-cash-flow consensus — current or historic. The FCF basis gets the trailing line
 * only, and the chart says so. (This is the same wall that removed the trend extrapolation from
 * the fiscal-year version.)
 */

import { type MetricRow } from './quickValuation';

export const CLOSE_CODE = 'close_price';
export const FORWARD_PE_CODE = 'indicator_q_forward_pe_ratio';

/** ⚠ PER-QUARTER, NOT CUMULATIVE — verified before this file was written, because the whole TTM
 *  idea is invalid otherwise. ASML: sum of the four quarters equals the annual row exactly
 *  (2023 8.24, 2024 23.08), so a rolling 4-quarter SUM is the trailing-twelve-month figure. Had
 *  they been year-to-date cumulatives, summing would have counted Q1 four times. */
export const QUARTERLY_FCF_CODES = [
  'quarterly__Per Share Data__Free Cash Flow per Share',
  'quarterly__per_share_data__Free Cash Flow per Share',
  'quarterly__per_share_data_array__Free Cash Flow per Share',
];
export const QUARTERLY_EPS_CODES = [
  'quarterly__Per Share Data__EPS without NRI',
  'quarterly__per_share_data__EPS without NRI',
  'quarterly__per_share_data_array__EPS without NRI',
];

/**
 * How long after a fiscal period ends before its figures are public.
 *
 * ⚠ WITHOUT THIS, EVERY MULTIPLE IS COMPUTED ON A NUMBER THE MARKET DID NOT HAVE. GuruFocus
 * stamps a fiscal row with the period END (`2015-12-31`), but ASML did not publish FY2015 until
 * late January 2016. Using it on 2016-01-05 is look-ahead: the series comes out cleaner and
 * cheaper-looking than anything anyone could have traded, and nothing about the chart reveals it.
 * 75 days is deliberately generous — a late filer is marked stale rather than clairvoyant.
 */
export const REPORT_LAG_DAYS = 75;

export type Point = { t: number; value: number };   // t = epoch ms

const ms = (iso: string) => new Date(`${iso}T00:00:00Z`).getTime();

/** Rows for the first code that returns anything, oldest first. A priority list, never a union —
 *  merging two section spellings interleaves two conventions into one line. */
export function pick(metrics: MetricRow[], codes: string[]): { date: string; value: number }[] {
  for (const code of codes) {
    const out = metrics
      .filter((m) => m.metric_code === code && m.numeric_value != null)
      .map((m) => ({ date: String(m.target_date), value: m.numeric_value as number }))
      .sort((a, b) => (a.date < b.date ? -1 : 1));
    if (out.length) return out;
  }
  return [];
}

/**
 * Rolling four-quarter sums — the trailing-twelve-month figure, stamped at the quarter it closes.
 *
 * ⚠ A GAP BREAKS THE WINDOW RATHER THAN SPANNING IT. Four consecutive ROWS are not four
 * consecutive QUARTERS if one is missing, and summing across the hole would silently report nine
 * months as a year — low, and indistinguishable from a bad year. Quarters more than ~400 days
 * apart end the window instead.
 */
export function ttm(quarters: { date: string; value: number }[]): { date: string; value: number }[] {
  const out: { date: string; value: number }[] = [];
  for (let i = 3; i < quarters.length; i++) {
    const win = quarters.slice(i - 3, i + 1);
    const span = (ms(win[3].date) - ms(win[0].date)) / 86_400_000;
    if (span > 400) continue;
    out.push({ date: win[3].date, value: win.reduce((s, q) => s + q.value, 0) });
  }
  return out;
}

/** The last value PUBLISHED on or before `iso` — period end plus `lagDays`. */
export function reportedAt(
  series: { date: string; value: number }[], iso: string, lagDays = REPORT_LAG_DAYS,
): number | null {
  const cutoff = ms(iso) - lagDays * 86_400_000;
  let best: number | null = null;
  for (const s of series) {
    if (ms(s.date) <= cutoff) best = s.value;
    else break;
  }
  return best;
}

/**
 * price ÷ the figure reported at that date, one point per sampled close.
 *
 * ⚠ A NON-POSITIVE DENOMINATOR YIELDS NO POINT, NOT A NEGATIVE ONE — the same refusal as
 * `multipleOf`. A loss year as a multiple is −20×, which sorts below every cheap year on any axis
 * and reads as the bargain of the decade.
 */
export function trailingMultiples(
  closes: { date: string; value: number }[],
  reported: { date: string; value: number }[],
  lagDays = REPORT_LAG_DAYS,
): Point[] {
  const out: Point[] = [];
  for (const c of closes) {
    const v = reportedAt(reported, c.date, lagDays);
    if (v != null && v > 0 && c.value > 0) out.push({ t: ms(c.date), value: c.value / v });
  }
  return out;
}

/** GuruFocus's own forward P/E, as published. Nothing is computed — see the module note. */
export function forwardSeries(metrics: MetricRow[]): Point[] {
  return pick(metrics, [FORWARD_PE_CODE])
    .filter((r) => r.value > 0)
    .map((r) => ({ t: ms(r.date), value: r.value }));
}

/**
 * Thin a daily series to roughly one point per `everyDays`.
 *
 * 6,933 daily closes over a decade is more marks than a 320px-tall chart has pixels, and recharts
 * pays for every one. Weekly is past the point where the line changes shape. ⚠ It KEEPS the last
 * point unconditionally — dropping it would end the chart days short of today, which on a
 * valuation chart reads as the multiple having stopped moving.
 */
export function thin(points: Point[], everyDays = 7): Point[] {
  if (points.length < 2) return points;
  const step = everyDays * 86_400_000;
  const out: Point[] = [points[0]];
  for (const p of points) if (p.t - out[out.length - 1].t >= step) out.push(p);
  const last = points[points.length - 1];
  if (out[out.length - 1].t !== last.t) out.push(last);
  return out;
}

/** Points from `fromYear` onward — the window the panel advertises. */
export function since(points: Point[], fromYear: number): Point[] {
  const cutoff = Date.UTC(fromYear, 0, 1);
  return points.filter((p) => p.t >= cutoff);
}

/** The median gap between consecutive observations — how often this series is actually sampled. */
export function medianSpacing(points: Point[]): number | null {
  if (points.length < 2) return null;
  const gaps = points.slice(1).map((p, i) => p.t - points[i].t).sort((a, b) => a - b);
  const mid = Math.floor(gaps.length / 2);
  return gaps.length % 2 ? gaps[mid] : (gaps[mid - 1] + gaps[mid]) / 2;
}

/**
 * Both series on ONE timeline, so they can be drawn as lines rather than confetti.
 *
 * ⚠ THE PROBLEM THIS SOLVES IS AN ARTEFACT OF MERGING, NOT A GAP IN THE DATA. The vendor's forward
 * indicator and our trailing series are sampled independently, so their timestamps essentially
 * never coincide. Merging them by timestamp therefore produces rows that hold ONE value and a null
 * for the other, alternating — and `connectNulls={false}`, which is right for real holes, then
 * correctly declines to join anything. Every point becomes an island: "disconnected lines and dots".
 *
 * So each series is carried forward onto the union of both timelines. A value is held only while
 * it is plausibly still current — `maxGap`, derived from that series' OWN median sampling interval
 * (× `gapFactor`) rather than hardcoded, because the same vendor feed is weekly for one company
 * and quarterly for another, and a fixed threshold would turn the sparse one back into dots.
 *
 * ⚠ A REAL GAP STILL BREAKS THE LINE. Beyond `maxGap` the carry stops and the row is null, so a
 * stretch with no observation is a visible break — a company that stopped being covered, or a run
 * of loss years with no multiple, must not be spanned by a confident straight line.
 */
export interface AlignedRow {
  /** Epoch ms. Always present — the index signature below permits null only for SERIES keys. */
  t: number;
  [series: string]: number | null;
}

export function align(series: Record<string, Point[]>, gapFactor = 3): AlignedRow[] {
  const keys = Object.keys(series).filter((k) => series[k].length > 0);
  if (!keys.length) return [];
  const grid = [...new Set(keys.flatMap((k) => series[k].map((p) => p.t)))].sort((a, b) => a - b);

  const cursor: Record<string, number> = {};
  const held: Record<string, Point | null> = {};
  const maxGap: Record<string, number> = {};
  for (const k of keys) {
    cursor[k] = 0;
    held[k] = null;
    // A single-point series has no spacing to measure; one week keeps it a dot rather than
    // letting it paint a decade.
    maxGap[k] = (medianSpacing(series[k]) ?? 7 * 86_400_000) * gapFactor;
  }

  return grid.map((t) => {
    const row: AlignedRow = { t };
    for (const k of keys) {
      const pts = series[k];
      while (cursor[k] < pts.length && pts[cursor[k]].t <= t) held[k] = pts[cursor[k]++];
      const h = held[k];
      row[k] = h && t - h.t <= maxGap[k] ? h.value : null;
    }
    return row;
  });
}
