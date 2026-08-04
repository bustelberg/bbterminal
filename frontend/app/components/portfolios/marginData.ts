/** Shared types + helpers for the FCF margin card and its drill-down. The margin is derived on
 *  the client from three raw lines so the numbers and the drill-down can't disagree.
 *
 *  ⚠ The SBC subtraction is no longer unconditional — it follows the tab-level correction toggle
 *  (`correctedFcf`). It used to be hardcoded here while two sibling cards never applied it, so one
 *  screen could describe the same book as both SBC-corrected and not. */

import { correctedFcf } from './sbcCorrection';

export type MarginRow = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
  revenue: Record<string, number | null>;
  fcf: Record<string, number | null>;
  sbc: Record<string, number | null>;
};
export type MarginInputs = { years: string[]; rows: MarginRow[] };

/** Amounts are millions of the reporting currency — compact B/T/M. */
export const fmtRevM = (v: number | null | undefined) => {
  if (v == null) return '—';
  const a = Math.abs(v);
  if (a >= 1e6) return `${(v / 1e6).toFixed(2)}T`;
  if (a >= 1e3) return `${(v / 1e3).toFixed(1)}B`;
  return `${v.toFixed(0)}M`;
};

/** The derived RATIO line every formula drill-down carries under its raw inputs — the ratio the card
 *  actually plots, per company per year. One decimal, matching the cards' tiles/tooltips; a `—`
 *  where the formula can't be computed (a missing leg or a non-positive denominator), never a 0. */
export const fmtRatioPct = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(1)}%`);

/**
 * ⚠ THE COVERAGE FLOOR, SHARED BY EVERY CARD ON THE TAB. A year's aggregate is only drawn when
 * this share of the charted holdings actually reported it.
 *
 * Without it the newest fiscal year is the dangerous one: books close on different dates, so early
 * in a year a handful of holdings have filed and the rest have not — and a weighted average that
 * renormalises over whoever reported turns that into a full-height point on the right edge of the
 * chart, drawn in the same ink as a year every holding reported. It reads as a move in the book
 * and it is a move in the sample. The same applies at the left edge, where holdings had not listed
 * yet.
 *
 * Kept in lock-step with the backend's `_fundamental_blend.MIN_BLEND_COVERAGE_PCT`, which does the
 * same job for the blended growth cards. Two floors that disagree would put two cards on the same
 * screen spanning different fractions of the same book.
 */
export const MIN_YEAR_COVERAGE_PCT = 80;

/**
 * A period LABEL from the server → the numeric x every card plots on.
 *
 * ⚠ THIS EXISTS BECAUSE `Number("2025-Q3")` IS **NaN**, AND NaN IS A VALID Map KEY. Every card on
 * this tab keyed its series with `Number(year)`, which was correct while the server only ever sent
 * "2025" — and the day it started sending trailing-twelve-month labels, all 42 quarterly periods
 * collapsed onto ONE NaN key and nine charts went blank. Not one of them errored: the drill-down
 * modals read the same payload as strings and rendered perfectly, so the data was visibly there
 * while the chart above it was empty.
 *
 * ⚠ FRACTIONAL YEARS, NOT AN INDEX. A quarter is a quarter OF A YEAR, so four points span exactly
 * 1.0 on the axis — which keeps the spacing honest when a series has gaps and keeps any per-year
 * arithmetic (the growth cards' CAGR) per year. `2025-Q3` → 2025.5.
 */
export const periodToX = (period: string): number => {
  const q = /^(\d{4})-Q([1-4])$/.exec(period);
  if (q) return Number(q[1]) + (Number(q[2]) - 1) / 4;
  // A DAILY label is an ISO date — the two yield cards' cadence. Same trap as the quarter one:
  // `Number("2026-07-31")` is NaN, so every trading day would land on one key. Placed on the same
  // fractional-year axis as the others, so a daily series and an annual one are directly
  // comparable and nothing downstream needs to know which cadence produced the point.
  const d = /^(\d{4})-(\d{2})-(\d{2})$/.exec(period);
  if (d) {
    const y = Number(d[1]);
    const start = Date.UTC(y, 0, 1);
    const days = (Date.UTC(y, Number(d[2]) - 1, Number(d[3])) - start) / 86_400_000;
    const inYear = (Date.UTC(y + 1, 0, 1) - start) / 86_400_000;   // 365 or 366
    return y + days / inYear;
  }
  return Number(period);
};

/** The inverse, for an axis tick or a tooltip: 2025 → "2025", 2025.5 → "2025 Q3". Without it an
 *  axis tick reads "2025.5", which is a year that does not exist.
 *
 *  ⚠ KNOWN AMBIGUITY, AND IT IS THE LEAST-BAD ONE: a Q1 point sits on an integer x (the quarter is
 *  offset by (q−1)/4 so that four quarters span exactly 1.0 — see `periodToX`), so it renders as
 *  the bare "2025" rather than "2025 Q1". The alternatives are worse: offsetting by q/4 puts Q4 on
 *  the NEXT year's integer, and taking a cadence argument means threading one through twelve chart
 *  components for a tick label. Nothing collides on screen — in quarterly mode there is no annual
 *  point to confuse it with, and the "2025" tick is visibly the first of that year's four. */
/** A DAILY axis tick: "Jul 2026". A daily series spans thousands of points, so the tick that helps
 *  is the month, not the day — and a fractional year like 2026.58 is unreadable either way. */
export const xToMonth = (x: number): string => {
  const y = Math.floor(x);
  const inYear = (Date.UTC(y + 1, 0, 1) - Date.UTC(y, 0, 1)) / 86_400_000;
  const d = new Date(Date.UTC(y, 0, 1) + Math.round((x - y) * inYear) * 86_400_000);
  return `${d.toLocaleString('en-US', { month: 'short', timeZone: 'UTC' })} ${d.getUTCFullYear()}`;
};

export const xToPeriod = (x: number): string => {
  const y = Math.floor(x);
  const q = Math.round((x - y) * 4);
  return q === 0 && Number.isInteger(x) ? String(y) : `${y} Q${q + 1}`;
};

/**
 * The one weighted-average-per-year used by every ratio card. Each card supplies the years it
 * could have a value for and a per-holding value; this applies the weights, the renormalisation
 * and the floor in ONE place.
 *
 * ⚠ THE DENOMINATOR IS THE CHARTED SET, NOT THE BOOK. `weight_pct` is the share of the WHOLE book
 * (cash and bonds sit in its denominator), so measuring coverage against 100 would mean a
 * portfolio holding 20% cash could never clear an 80% floor and every chart would go blank.
 * Coverage here answers "of the companies this chart aggregates, how many reported this year".
 */
export function weightedByYear<T extends { weight_pct: number }>(
  rows: T[],
  yearsOf: (r: T) => string[],
  valueOf: (r: T, year: string) => number | null,
): Map<number, number> {
  const total = rows.reduce((a, r) => a + r.weight_pct, 0);
  const out = new Map<number, number>();
  if (total <= 0) return out;
  const years = new Set<string>();
  for (const r of rows) for (const y of yearsOf(r)) years.add(y);
  for (const y of years) {
    let num = 0;
    let den = 0;
    for (const r of rows) {
      const v = valueOf(r, y);
      if (v == null) continue;
      num += r.weight_pct * v;
      den += r.weight_pct;
    }
    if (den > 0 && 100 * den / total >= MIN_YEAR_COVERAGE_PCT) out.set(periodToX(y), num / den);
  }
  return out;
}

/** The share of the charted set that reported in each year — the same denominator and the same
 *  per-holding test `weightedByYear` uses, so a card can state the coverage behind a point it
 *  drew (and a year below the floor is visible as a fact rather than as a hole). */
export function coverageByYear<T extends { weight_pct: number }>(
  rows: T[],
  yearsOf: (r: T) => string[],
  valueOf: (r: T, year: string) => number | null,
): Map<number, number> {
  const total = rows.reduce((a, r) => a + r.weight_pct, 0);
  const out = new Map<number, number>();
  if (total <= 0) return out;
  const years = new Set<string>();
  for (const r of rows) for (const y of yearsOf(r)) years.add(y);
  for (const y of years) {
    let den = 0;
    for (const r of rows) if (valueOf(r, y) != null) den += r.weight_pct;
    out.set(periodToX(y), 100 * den / total);
  }
  return out;
}

/** One company's FCF-SBC margin for a year, or null when it can't be computed. SBC missing is
 *  treated as 0 (many companies report none); revenue must be positive. */
export function marginOf(
  rev: number | null | undefined, fcf: number | null | undefined, sbc: number | null | undefined,
  correct = true,
) {
  if (rev == null || rev <= 0) return null;
  const num = correctedFcf(fcf, sbc, correct);
  return num == null ? null : num / rev * 100;
}

/** The book's FCF-SBC margin per year — a WEIGHT-weighted average of each company's margin (each is
 *  a currency-free ratio, so averaging is currency-safe; summing mixed-currency euros/£/$ is not).
 *  For a single company this is just that company's margin. */
export function marginByYear(rows: MarginRow[], correct = true): Map<number, number> {
  return weightedByYear(rows, (r) => Object.keys(r.revenue),
    (r, y) => marginOf(r.revenue[y], r.fcf[y], r.sbc[y], correct));
}

export const meanOf = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);

/** A y-axis domain a little beyond the data, so the min/max points don't sit clipped on the axis
 *  edge. Pads ~10% of the range (at least 1 unit, for a near-flat series) and rounds to whole units
 *  so the ticks stay tidy. Returns undefined for no data (let the axis auto-scale). */
export function paddedDomain(values: number[]): [number, number] | undefined {
  const xs = values.filter((v) => Number.isFinite(v));
  if (!xs.length) return undefined;
  const min = Math.min(...xs);
  const max = Math.max(...xs);
  const pad = Math.max((max - min) * 0.1, 1);
  return [Math.floor(min - pad), Math.ceil(max + pad)];
}

/** Same idea for a LOG axis, where padding must be MULTIPLICATIVE — a fixed delta means nothing on
 *  a log scale, so the min is divided and the max multiplied by a factor (~15% headroom). Only
 *  positive values (a log axis can't plot ≤ 0). Returns undefined for no data. */
export function paddedLogDomain(values: number[]): [number, number] | undefined {
  const xs = values.filter((v) => Number.isFinite(v) && v > 0);
  if (!xs.length) return undefined;
  const f = 1.15;
  return [Math.min(...xs) / f, Math.max(...xs) * f];
}
