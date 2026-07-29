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
    if (den > 0 && 100 * den / total >= MIN_YEAR_COVERAGE_PCT) out.set(Number(y), num / den);
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
    out.set(Number(y), 100 * den / total);
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
