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
 * ⚠ 60 → 80 (2026-07-28) → 50 (2026-08-12, on request: half the constituents in a period should
 * draw that period). `<` is the comparison, so exactly 50% clears. The newest-year artifact above
 * is now an ACCEPTED cost rather than a prevented one — see the backend constant's ⚠⚠, and note
 * that the targeted fix, if it ever bites, is a stricter bar on the LATEST period alone.
 *
 * ⚠ ONE NUMBER, AND EVERY MENTION OF IT READS IT. The two `benchNote` sentences used to spell "80%"
 * into their text, so lowering the floor would have left the legend confidently quoting a floor
 * that no longer existed.
 *
 * Kept in lock-step with the backend's `_fundamental_blend.MIN_BLEND_COVERAGE_PCT`, which does the
 * same job for the blended growth cards. Two floors that disagree would put two cards on the same
 * screen spanning different fractions of the same book.
 */
export const MIN_YEAR_COVERAGE_PCT = 50;

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

/**
 * Can this period label be placed on the axis at all?
 *
 * ⚠⚠ A PERIOD `periodToX` CANNOT PLACE MUST BE DROPPED, NOT PLOTTED AT NaN. `Number("LTM")` is NaN,
 * NaN is a valid `Map` key, and every such period therefore lands on ONE key — the exact failure
 * `periodToX` documents, where nine charts went blank while their drill-downs rendered perfectly.
 * The server now sends an `LTM` period to the six flow-ratio endpoints, so this is a live label and
 * not a hypothetical one; until each card places it at the response's `ltm_date`, dropping it makes
 * the new period INERT — the charts read exactly as they did before it existed. Being a year short
 * is a visible absence; a point silently fused onto NaN is not.
 */
export const plottable = (period: string): boolean => Number.isFinite(periodToX(period));

export const xToPeriod = (x: number): string => {
  const y = Math.floor(x);
  const q = Math.round((x - y) * 4);
  return q === 0 && Number.isInteger(x) ? String(y) : `${y} Q${q + 1}`;
};

/**
 * The axis tick (and tooltip header) for a chart that also carries TRAILING-TWELVE-MONTH points.
 *
 * ⚠⚠ AN LTM POINT SITS ON A QUARTER-END x, SO `xToPeriod` NAMES IT A FISCAL QUARTER. On an annual
 * chart every reported point is on a whole year and the LTM extension is the only fractional x
 * there is — 2026-06-30 → 2026.25 → **"2026 Q2"**, a quarter nobody filed, in the one place a
 * reader looks for the newest figure. Measured 2026-08-14 on the ACWI overlay of `EPS (excl.
 * non-recurring)`: the portfolio blend emitted no LTM row at all, so the card had no LTM x of its
 * own to match against, and the INDEX's LTM was labelled "2026 Q2" while its line ran a quarter
 * past the book's. It reads as "the index has reported and we have not", which it was not.
 *
 * ⚠⚠ `ltmXs` IS A SET FOR ROBUSTNESS, BUT A CHART MUST PUT ITS LTM STUB ON **ONE** x (2026-08-18).
 * It briefly held two: each blend stamps its trailing year with the newest filing behind it and
 * `ltmYearX` measures the stub from that entity's OWN last fiscal year end, so a book and an index
 * on different fiscal calendars produced two positions — a second "LTM" tick on the axis, and two
 * trailing points side by side reading as though the index's twelve months happened later in time
 * than the book's. They did not: both mean "the latest twelve months available", and the axis has
 * exactly one such slot. The windows really do end on different quarters, and that is a fact about
 * DATES — stated in the tooltip and the footnote, where a reader can act on it, not encoded as a
 * horizontal offset nobody can measure off two ticks that both say "LTM". See `MetricGrowthCard`'s
 * `ltmX`. The set stays a set so this helper cannot itself become the reason a second one appears.
 */
export const periodTick = (x: number, ltmXs?: ReadonlySet<number>): string =>
  (ltmXs?.has(x) ? 'LTM' : xToPeriod(x));

/** One point's change from the previous point the SAME series had: the figure, and the x it is
 *  measured FROM. `pct: null` means there is a previous point but no honest ratio to it. */
export type Step = { pct: number | null; from: number };

/**
 * Each point's change from the period before it, keyed by x.
 *
 * ⚠⚠ THIS IS WHAT A LEVEL CHART CANNOT BE READ FOR. The number on the axis is an INDEX, i.e.
 * cumulative growth since the anchor — so "are we ahead of the benchmark since 2015" is already the
 * SHAPE of the two lines and needs no figure, while "did we out-grow it THIS year" is invisible on
 * a log axis where both lines are rising, and it is the question a reader hovers a point to ask.
 *
 * ⚠ IT IS NOT "YoY", AND CALLING IT THAT WOULD BE WRONG THREE WAYS ON THIS TAB: the LTM point sits
 * a quarter or two past the last fiscal year (which is why it is out of the CAGR fit), the
 * quarterly basis steps one QUARTER at a time on a trailing-twelve-month series, and a period the
 * coverage floor withheld leaves a two-year gap drawn as one segment. So `from` comes back and the
 * caller NAMES the interval — "+11.4% vs 2024" — instead of asserting one.
 *
 * ⚠ A NON-POSITIVE BASE GETS `null`, NEVER A PERCENTAGE. Same rule as `transformSeries`' YoY view
 * and the server's `step_growth`, and the same reason this card withholds its CAGR across a sign
 * change: −2 → −1 reads as "+50% growth" for a company still making a loss, and −1 → +2 is not
 * "+300%" in any sense that compounds.
 *
 * ⚠ SAFE ON EITHER BASIS. A rebase is one constant multiplier per series, so it divides out of
 * `v / prev` — the step is identical whether computed on the indexed values or the raw ones. Feed
 * it the RAW series so the answer cannot change when the axis flips to absolute on a sign change.
 */
export function stepChanges(series: ReadonlyMap<number, number | null>): Map<number, Step> {
  const out = new Map<number, Step>();
  let prev: { x: number; v: number } | null = null;
  for (const x of [...series.keys()].sort((a, b) => a - b)) {
    const v = series.get(x);
    if (v == null) continue;
    // ⚠ AGAINST THE PREVIOUS POINT THIS SERIES HAS, not the previous column — a line with a hole
    // would otherwise show two periods of growth in the same ink as everyone else's one.
    if (prev) out.set(x, { pct: prev.v > 0 ? 100 * (v / prev.v - 1) : null, from: prev.x });
    prev = { x, v };
  }
  return out;
}


/**
 * The one weighted-average-per-year used by every ratio card. Each card supplies the years it
 * could have a value for and a per-holding value; this applies the weights, the renormalisation
 * and the floor in ONE place.
 *
 * ⚠ THE DENOMINATOR IS THE CHARTED SET, NOT THE BOOK. `weight_pct` is the share of the WHOLE book
 * (cash and bonds sit in its denominator), so measuring coverage against 100 would mean a
 * portfolio holding 20% cash could never clear the floor and every chart would go blank.
 * Coverage here answers "of the companies this chart aggregates, how many reported this year".
 */
export type Weighted = {
  weight_pct: number;
  /** INDEX ROWS ONLY — the market cap as at each fiscal period, EUR, converted at that period's
   *  own end date (`period_caps_eur`). Absent for a portfolio: a holding weight is not a market
   *  cap and has no history, so the single `weight_pct` applies to every period. */
  market_cap_by_period?: Record<string, number>;
};

/**
 * The weight in force for one row in one period — the frontend twin of the backend's
 * `_fundamental_blend._weight_at`, and the reason every card on this tab weights the same way.
 *
 * ⚠ AN INDEX IS WEIGHTED BY THE CAP IT HAD IN THAT PERIOD. Weighting 2018's margin by today's cap
 * is look-ahead bias — measured on the S&P, NVIDIA is carried at 7.46% of a year it was 0.63% of.
 * Null (never 0) when a constituent has no cap that period: it is out of that period's average
 * entirely rather than weighted on a different basis from its neighbours.
 */
export function weightAt(r: Weighted, year: string): number | null {
  const per = r.market_cap_by_period;
  if (per) {
    const v = per[year];
    if (v && v > 0) return v;
    // ⚠ AS-OF, MIRRORING THE BACKEND'S `_weight_at`. A market cap is a stock: the last one filed
    // stands until a newer one exists. The server already carries the newest year forward once in
    // the payload, so this only bites on an older gap — but the two sides must resolve a missing
    // cap the same way or a card and the chart behind it weight the same period differently.
    const earlier = Object.keys(per).filter((k) => k <= year && per[k] > 0);
    if (earlier.length) return per[earlier.reduce((a, b) => (a > b ? a : b))];
    return null;
  }
  return r.weight_pct > 0 ? r.weight_pct : null;
}

/**
 * Each period's value for one row: its own, or the latest one before it — with `reported` saying
 * which. The client twin of `_fundamental_blend.carry_forward`; see that docstring for why.
 *
 * ⚠ A CARRIED VALUE NEVER COUNTS AS COVERAGE. It keeps the contributor set stable (without it the
 * line alternates between the companies that file quarterly and the ones that file at Jun/Dec — a
 * ±20% sawtooth of composition), while the floor still sees only who actually reported, so the
 * newest period cannot slip through on carried figures.
 *
 * ⚠ BOUNDED to one year, so a holding that stops reporting falls out instead of being held at a
 * frozen value for the rest of the axis. Periods are compared on their own calendar ends —
 * `periodToX` puts a year and its quarters on one numeric axis, and a year is 1.0 of it.
 */
export const MAX_CARRY_YEARS = 1.05;          // ~400 days, matching `_MAX_CARRY_DAYS`

export function carryForward(
  own: Map<number, number>, axis: number[],
): Map<number, { value: number; reported: boolean }> {
  const out = new Map<number, { value: number; reported: boolean }>();
  let last: { x: number; value: number } | null = null;
  for (const x of axis) {
    const v = own.get(x);
    if (v != null) {
      last = { x, value: v };
      out.set(x, { value: v, reported: true });
    } else if (last && x - last.x <= MAX_CARRY_YEARS) {
      out.set(x, { value: last.value, reported: false });
    }
  }
  return out;
}

/** What a drill-down column shows. Lives here rather than beside the table because
 *  {@link transformSeries} is the thing that gives it meaning. */
export type SeriesView = 'reported' | 'rebased' | 'yoy';

/**
 * One line of one company, ordered along the axis, under one of the three views.
 *
 * ⚠ AN ARRAY IN, AN ARRAY OUT — the transforms are relative to the row's OWN reported history, so
 * they cannot be computed a cell at a time without re-deriving that history per cell. `null` means
 * "not reported" on the way in and "cannot be stated" on the way out, and the two are deliberately
 * the same hole: a period a company did not report has no index level and no growth rate either.
 *
 * ⚠ NULL, NOT A NUMBER, ON A NON-POSITIVE BASE. `100 × v/0` is undefined and a negative base
 * inverts the curve — the same refusal `_fundamental_blend._prepare` makes server-side, which is
 * why a company whose equity opens negative is out of the blended line entirely rather than in it
 * upside-down. The same test guards YoY's denominator.
 *
 * ⚠ YoY IS AGAINST THE PREVIOUS PERIOD **THIS ROW REPORTED**, not the previous column. A company
 * that skipped a period would otherwise show two periods of growth in the same ink as everyone
 * else's one.
 */
export function transformSeries(
  values: (number | null | undefined)[], view: SeriesView,
): (number | null)[] {
  const vs = values.map((v) => v ?? null);
  if (view === 'reported') return vs;
  if (view === 'rebased') {
    const base = vs.find((v) => v != null);
    return base != null && base > 0 ? vs.map((v) => (v == null ? null : 100 * v / base)) : vs.map(() => null);
  }
  let prev: number | null = null;
  return vs.map((v) => {
    if (v == null) return null;
    const before = prev;
    prev = v;
    return before != null && before > 0 ? 100 * (v / before - 1) : null;
  });
}

export function weightedByYear<T extends Weighted>(
  rows: T[],
  rawYearsOf: (r: T) => string[],
  valueOf: (r: T, year: string) => number | null,
): Map<number, number> {
  const yearsOf = (r: T) => rawYearsOf(r).filter(plottable);
  const total = rows.reduce((a, r) => a + r.weight_pct, 0);
  const out = new Map<number, number>();
  if (total <= 0 || !rows.length) return out;
  // The axis every row is carried across — the union of what anybody reported.
  const xs = new Set<number>();
  for (const r of rows) for (const y of yearsOf(r)) if (valueOf(r, y) != null) xs.add(periodToX(y));
  const axis = [...xs].sort((a, b) => a - b);
  const label = new Map<number, string>();
  for (const r of rows) for (const y of yearsOf(r)) label.set(periodToX(y), y);

  const num = new Map<number, number>();
  const den = new Map<number, number>();
  // ⚠⚠ COVERAGE IS ACCUMULATED ON THE **STABLE** WEIGHT, NOT THE PER-PERIOD CAP, AND CONFUSING
  // THE TWO DISABLES THE FLOOR COMPLETELY. The per-period cap comes out of the same GuruFocus
  // blob as the figure, so a company that has not filed FY2026 has no FY2026 cap either —
  // measure coverage with it and you divide the filers by the filers, which reads ~100% in
  // exactly the period where almost nobody has reported. Measured on the S&P revenue blend:
  // FY2026 is 13.4% covered on this basis and read 100.0% on the per-period one, which drew a
  // full-height point built almost entirely out of NVIDIA.
  const cov = new Map<number, number>();
  const names = new Map<number, number>();
  for (const r of rows) {
    const own = new Map<number, number>();
    for (const y of yearsOf(r)) {
      const v = valueOf(r, y);
      if (v != null) own.set(periodToX(y), v);
    }
    for (const [x, { value, reported }] of carryForward(own, axis)) {
      const w = weightAt(r, label.get(x) ?? String(x));
      if (w == null) continue;
      num.set(x, (num.get(x) ?? 0) + w * value);
      den.set(x, (den.get(x) ?? 0) + w);
      if (reported) {
        cov.set(x, (cov.get(x) ?? 0) + r.weight_pct);
        names.set(x, (names.get(x) ?? 0) + 1);
      }
    }
  }
  // ⚠ BOTH FLOORS, AND ONLY ON WHAT WAS REPORTED. Weight alone lets one giant draw a period (AEX
  // 2026-Q2: two constituents, 53.8% of cap); names alone would let ten tiny ones outvote a
  // missing giant. A carried value counts toward neither — that is what stops the carry defeating
  // the floor in the newest period.
  for (const x of axis) {
    const d = den.get(x) ?? 0;
    if (d <= 0) continue;
    if (100 * (cov.get(x) ?? 0) / total < MIN_YEAR_COVERAGE_PCT) continue;
    if (100 * (names.get(x) ?? 0) / rows.length < MIN_YEAR_COVERAGE_PCT) continue;
    out.set(x, (num.get(x) ?? 0) / d);
  }
  return out;
}

/** The share of the charted set that reported in each year — the same denominator and the same
 *  per-holding test `weightedByYear` uses, so a card can state the coverage behind a point it
 *  drew (and a year below the floor is visible as a fact rather than as a hole). */
/**
 * The denominator each period's weighted average ACTUALLY divided by — `{period: Σ weight}`.
 *
 * ⚠ IT MUST APPLY THE SAME TWO TESTS AS `weightedByYear`, AND THAT IS THE ONLY REASON IT EARNS ITS
 * PLACE HERE RATHER THAN IN THE MODAL. A drill-down exists to show the arithmetic behind a line; a
 * denominator derived "the same way" somewhere else is how a table comes to show weights that do
 * not sum to the line above them. Value present AND a usable weight — a company with a figure but
 * no cap that period is out of the average, so it is out of this sum too.
 *
 * By construction, dividing each contributor's `weightAt` by this gives a column that sums to
 * exactly 100% in every period — which is what makes the drill-down checkable.
 */
export function periodDenoms<T extends Weighted>(
  rows: T[],
  rawYearsOf: (r: T) => string[],
  valueOf: (r: T, year: string) => number | null,
): Record<string, number> {
  // ⚠ THE SAME FILTER AS `weightedByYear`, FOR THE SAME REASON — and because these two must agree.
  // This function exists to prove the line's weights sum to 100%; a period one of them can see and
  // the other cannot would make that proof fail on a period nobody plotted. See `plottable`.
  const yearsOf = (r: T) => rawYearsOf(r).filter(plottable);
  const out: Record<string, number> = {};
  const years = new Set<string>();
  for (const r of rows) for (const y of yearsOf(r)) years.add(y);
  // ⚠ THE CARRIED ROWS ARE IN THE DENOMINATOR, BECAUSE THEY ARE IN THE AVERAGE. `weightedByYear`
  // divides by every row that contributed a figure — its own or its latest — so a denominator
  // computed over the reporters alone would be smaller than the one the line used, and the
  // drill-down's weights would not sum to 100%. The point of this function is that they do.
  const xs = [...new Set([...years].map(periodToX))].sort((a, b) => a - b);
  const label = new Map<number, string>();
  for (const r of rows) for (const y of yearsOf(r)) label.set(periodToX(y), y);
  for (const r of rows) {
    const own = new Map<number, number>();
    for (const y of yearsOf(r)) {
      const v = valueOf(r, y);
      if (v != null) own.set(periodToX(y), v);
    }
    for (const x of carryForward(own, xs).keys()) {
      const y = label.get(x);
      if (y == null) continue;
      const w = weightAt(r, y);
      if (w == null) continue;
      out[y] = (out[y] ?? 0) + w;
    }
  }
  return out;
}

export function coverageByYear<T extends Weighted>(
  rows: T[],
  rawYearsOf: (r: T) => string[],
  valueOf: (r: T, year: string) => number | null,
): Map<number, number> {
  // Same filter as the two above: coverage is reported per drawn period, so it must see the same
  // set of periods the line does. See `plottable`.
  const yearsOf = (r: T) => rawYearsOf(r).filter(plottable);
  const total = rows.reduce((a, r) => a + r.weight_pct, 0);
  const out = new Map<number, number>();
  if (total <= 0) return out;
  const years = new Set<string>();
  for (const r of rows) for (const y of yearsOf(r)) years.add(y);
  for (const y of years) {
    let den = 0;
    // ⚠ THE SAME TWO TESTS `weightedByYear` APPLIES — a value AND a usable weight. A constituent
    // with a figure but no cap that period is out of the average, so counting it as covered would
    // let a period clear the floor on the strength of rows that contributed nothing to it. And the
    // same STABLE weight, for the reason spelled out there.
    for (const r of rows) {
      if (valueOf(r, y) != null && weightAt(r, y) != null) den += r.weight_pct;
    }
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
