import type { FundamentalGridColumn, FundamentalGridRow } from '../../../lib/types/api';

/**
 * The arithmetic and the period algebra behind the benchmark fundamentals grid.
 *
 * Extracted from the component because every one of these is a claim that can be wrong in a way
 * the screen cannot show you — a weighted mean over the wrong denominator, a quarter offered for a
 * year that has none, a total summed over columns that must not be summed.
 */

/** `2025` (a fiscal year) or `2025-Q3` (trailing twelve months ENDING in that quarter). */
export type Period = string;

/**
 * The two sliders' axes, derived from the periods the server actually returned.
 *
 * ⚠ THE QUARTER AXIS IS PER YEAR, NOT GLOBAL, AND THAT IS THE POINT. A TTM point needs four
 * quarters behind it, and the current year has only the quarters that have been filed — so a
 * global `[Q1..Q4]` would offer positions that resolve to no data and read as "the index has
 * nothing here". `quartersByYear` is what lets the control grey out what does not exist rather
 * than render an empty table.
 */
export function periodAxes(periods: readonly Period[]): {
  years: string[];
  quartersByYear: Record<string, number[]>;
} {
  const years = new Set<string>();
  const byYear: Record<string, Set<number>> = {};
  for (const p of periods) {
    const y = p.slice(0, 4);
    years.add(y);
    const q = p.length > 4 ? Number(p.slice(6)) : null;
    if (q) (byYear[y] ??= new Set()).add(q);
  }
  return {
    years: [...years].sort(),
    quartersByYear: Object.fromEntries(
      Object.entries(byYear).map(([y, qs]) => [y, [...qs].sort((a, b) => a - b)]),
    ),
  };
}

/** The two slider positions → the period key the payload is stored under. */
export function periodKey(year: string, quarter: number | null): Period {
  return quarter ? `${year}-Q${quarter}` : year;
}

/**
 * How the selected period should be NAMED on screen.
 *
 * ⚠ `TTM → Q3` AND NEVER `Q3`. The quarterly basis here is trailing twelve months (see
 * `_benchmark_fundamental_grid`), so a cell under a `Q3` heading would read as three months of
 * revenue when it is twelve — the number is about 4x what the label implies, and nothing on screen
 * would contradict it. The label is the only thing standing between the reader and that.
 */
export function periodTitle(period: Period): string {
  if (period.length <= 4) return `FY${period}`;
  return `TTM → ${period.slice(5)} ${period.slice(0, 4)}`;
}

/** A row's market cap in the period, or null. The weight's numerator, kept in one place. */
export function capOf(row: FundamentalGridRow, period: Period): number | null {
  const v = (row.v as Record<string, Record<string, number>>)[period];
  const c = v?.market_cap;
  return typeof c === 'number' && c > 0 ? c : null;
}

/** One cell, in EUR (or native units for `shares` / `percent` — see the column's `unit`). */
export function valueOf(
  row: FundamentalGridRow, period: Period, key: string,
): number | null {
  const v = (row.v as Record<string, Record<string, number>>)[period]?.[key];
  return typeof v === 'number' ? v : null;
}

export type AggCell = { value: number; contributors: number };

/**
 * The index row: what the whole benchmark looked like in this period.
 *
 * ⚠ THREE KINDS OF COLUMN, AND USING THE WRONG ONE PRODUCES A NUMBER THAT STILL RENDERS FINE.
 *  * `sum` — a currency amount (revenue, market cap, equity). The index total is the sum.
 *  * `weighted_mean` — already a RATE (ROIC %). Summing 400 companies' ROIC gives ~5,000%, which
 *    the cell would happily print with a % sign after it. The only honest aggregate is the mean,
 *    and the only honest mean here is cap-weighted.
 *  * `none` — ⚠ A REFUSAL, AND IT IS AN ANSWER. A share COUNT and a PER-SHARE amount have no
 *    index-level total: "the S&P 500's share count" is not a quantity, and summed dividends-per-
 *    share across 500 companies is a well-formed number with no referent. The cell shows a dash.
 * The kind is declared by the server off the column's UNIT (`column.agg`) — not off its TTM
 * roll-up rule, which answers aggregation over TIME and disagrees on exactly the lines that matter.
 *
 * ⚠ A WEIGHTED MEAN RENORMALISES OVER THE ROWS THAT HAVE THE METRIC, and `contributors` is how
 * that stops being silent. A rate present for 200 of 480 constituents is a real average of those
 * 200; presented without the count it reads as the index's. A SUM is not renormalisable at all —
 * a missing company just understates the total — which is why the whole row is withheld under the
 * coverage floor rather than fixed per column.
 */
export function aggregateRow(
  rows: readonly FundamentalGridRow[],
  period: Period,
  columns: readonly FundamentalGridColumn[],
): Record<string, AggCell> {
  const out: Record<string, AggCell> = {};
  for (const col of columns) {
    // The refusal is honoured HERE, not by the renderer. A column left out of the result cannot be
    // accidentally totalled by a later caller that only looks at the values.
    if (col.agg === 'none') continue;
    if (col.agg === 'weighted_mean') {
      let num = 0;
      let den = 0;
      let n = 0;
      for (const r of rows) {
        const v = valueOf(r, period, col.key);
        const cap = capOf(r, period);
        if (v === null || cap === null) continue;
        num += v * cap;
        den += cap;
        n += 1;
      }
      if (den > 0) out[col.key] = { value: num / den, contributors: n };
      continue;
    }
    let total = 0;
    let n = 0;
    for (const r of rows) {
      const v = valueOf(r, period, col.key);
      if (v === null) continue;
      total += v;
      n += 1;
    }
    if (n) out[col.key] = { value: total, contributors: n };
  }
  return out;
}

/**
 * EUR millions → a figure a person can read.
 *
 * ⚠ THE INPUT IS MILLIONS, which is the convention everywhere GuruFocus financials are handled in
 * this app. Rendering 181,171 as "€181,171" (rather than €181.2bn) is not a formatting preference
 * — it is off by a factor of a million and looks like a plausible euro amount.
 */
export function fmtMillions(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—';
  const a = Math.abs(v);
  const s = v < 0 ? '−' : '';
  if (a >= 1_000_000) return `${s}€${(a / 1_000_000).toFixed(2)}tn`;
  if (a >= 1_000) return `${s}€${(a / 1_000).toFixed(1)}bn`;
  return `${s}€${a.toFixed(0)}M`;
}

/** A cell, formatted per its declared unit. The unit decides the shape, never a guess at the key. */
export function fmtCell(v: number | null | undefined, unit: string): string {
  if (v == null || !Number.isFinite(v)) return '—';
  if (unit === 'percent') return `${v.toFixed(1)}%`;
  if (unit === 'shares') return `${v.toLocaleString('en-US', { maximumFractionDigits: 0 })}M`;
  if (unit === 'per_share') return `€${v.toFixed(2)}`;
  return fmtMillions(v);
}

/**
 * THE ROW ORDER — and it is deliberately blind to the period on screen.
 *
 * ⚠⚠ THE `anchor` IS NOT THE SELECTED PERIOD, AND SUBSTITUTING IT WOULD BE THE BUG THIS EXISTS TO
 * PREVENT. Ranking companies by the period being viewed means every row moves as the slider moves:
 * the eye cannot track one company across periods, and "the third-largest" is a different business
 * at each stop — which makes a cross-period comparison, the only reason to have a slider, actually
 * impossible. `anchor` is one fixed period (the newest annual one) for the life of the table, so
 * the controls re-value the cells and never reshuffle them.
 *
 * `identity` is the UNION of companies across both cadences and decides WHICH rows exist; `order`
 * is the single payload the ranking is read from. Keeping them separate is what lets a company the
 * current basis cannot answer for hold its place as a row of dashes instead of vanishing.
 */
export function orderedIds(
  identity: ReadonlyMap<number, FundamentalGridRow>,
  order: ReadonlyMap<number, FundamentalGridRow>,
  opts: { anchor: Period; sortKey: string; dir: 'asc' | 'desc'; needle?: string },
): number[] {
  const { anchor, sortKey, dir, needle = '' } = opts;
  const val = (id: number): number | null => {
    const r = order.get(id);
    if (!r) return null;
    return sortKey === 'market_cap' ? capOf(r, anchor) : valueOf(r, anchor, sortKey);
  };
  return [...identity.entries()]
    .filter(([, r]) =>
      !needle || `${r.name ?? ''} ${r.ticker ?? ''}`.toLowerCase().includes(needle))
    .sort(([ia], [ib]) => {
      const x = val(ia);
      const y = val(ib);
      // ⚠ ABSENT SORTS LAST IN BOTH DIRECTIONS. A company we could not price is not the smallest
      // company in the index, and letting it head an ascending sort says exactly that.
      if (x === null && y === null) return ia - ib;
      if (x === null) return 1;
      if (y === null) return -1;
      // A stable tiebreak, so equal values cannot swap places between two renders.
      return (dir === 'desc' ? y - x : x - y) || ia - ib;
    })
    .map(([id]) => id);
}

/**
 * COLUMN WIDTHS, IN REM — COMPUTED FROM THE HEADINGS, NEVER FROM THE DATA.
 *
 * ⚠⚠ THIS IS WHY THE TABLE USES `table-fixed`. An auto-layout table sizes its columns from their
 * CONTENT, so the geometry is a function of the period on screen: 2018 holds fewer figures and
 * shorter ones than 2025, columns shrank to fit, and the headings wrapped onto a second line —
 * the whole header bar changing shape as the slider moved. It is the same defect class as the
 * re-sorting rows: the controls are supposed to re-value the table, not rebuild it.
 *
 * The width therefore depends on the one thing that does not change with the period — the heading
 * — with a floor for the widest figure a cell can hold. `€181.2bn`, `24,514M` and `133.0%` are all
 * inside ten characters, so a ten-character floor covers every value; past that the heading is
 * always the longer of the two (`Non-current liabilities` is 23).
 *
 * 0.45rem per character is deliberately generous for text-xs — the ask was "large enough", and a
 * column that is 20% too wide costs a little scrolling while one that is 2% too narrow costs the
 * wrap this exists to prevent.
 */
export const COL_CHAR_REM = 0.45;
export const COL_PAD_REM = 1.2;
export const COL_MIN_CHARS = 10;

export function measureWidthRem(label: string): number {
  return COL_CHAR_REM * Math.max(label.length, COL_MIN_CHARS) + COL_PAD_REM;
}

/** The identity columns, which hold names rather than figures and so are sized by hand.
 *  ⚠ THE LIST MUST LINE UP WITH THE COLUMNS ACTUALLY RENDERED — a `<col>` list one short does not
 *  fail, every column after the gap silently takes its neighbour's width. Weight is unconditional
 *  (see `weightPct`), so this is a constant rather than the flag it briefly was. */
export function fixedWidthsRem(withFetch: boolean): number[] {
  //      #  Company  [Fetch]  Ticker  Ccy  Cap (€)  Weight
  //
  // ⚠ THE 3rem `#` COLUMN IS ALSO THE STICKY OFFSET. It and Company both pin when the table
  // scrolls sideways, so Company sticks at `left-[3rem]` — this number and that class have to
  // agree or the name column slides over the row numbers and hides them.
  //
  // ⚠ THE FETCH COLUMN IS ADMIN-ONLY, hence the flag: the ingest it fires spends GuruFocus quota
  // and the API gate holds it to admins, so a non-admin must not be shown a button that 403s. The
  // column count therefore varies by ROLE — never by data, which is the thing that has to stay
  // fixed (see the ⚠⚠ on `measureWidthRem`).
  //
  // ⚠ IT WAS BRIEFLY TWO COLUMNS (`Table` / `All`) AND ONE IS RIGHT. A per-row button should
  // fetch what its table SHOWS, and all nineteen columns here come from the statements feed. The
  // other two feeds are reachable where they are displayed — `/api/earnings/{cid}/refresh` has a
  // per-source refresh on the page that draws them — so offering them here only spent two extra
  // calls on data this grid cannot render.
  return withFetch ? [3, 16, 4.5, 5.5, 4, 7, 6] : [3, 16, 5.5, 4, 7, 6];
}

/** Every column's width, in order, plus the total the table itself must be set to — `table-fixed`
 *  needs a definite width or the browser falls back to distributing the container's. */
export function gridWidths(labels: readonly string[], withFetch: boolean): {
  widths: number[]; total: number;
} {
  const widths = [...fixedWidthsRem(withFetch), ...labels.map(measureWidthRem)];
  return { widths, total: widths.reduce((a, b) => a + b, 0) };
}

/**
 * `w = this company's cap / the sum of the caps we HAVE`, as a percent.
 *
 * ⚠⚠ IT IS A SHARE OF WHAT WE COULD PRICE, NOT THE INDEX'S WEIGHT, AND THE TWO DIVERGE MOST WHERE
 * IT MATTERS. Two separate reasons, both live:
 *
 *  * A CAPPED INDEX. Euronext caps an AEX constituent at 15% at each review — uncapped, ASML is
 *    37.53% of it. This column will show ~37%, and that figure is a true statement about the caps
 *    on screen and a false one about the AEX. `INDEX_CAP_PCT` is where the real weighting lives
 *    (`index_weights`); nothing here reimplements it.
 *  * MISSING CONSTITUENTS. The denominator is the caps we hold, so a name we cannot price does not
 *    dilute anyone — it inflates everybody else pro rata. On the AEX, Shell, Unilever and RELX
 *    carry no stored cap at all, so every weight here is overstated by their share.
 *
 * Shown unconditionally on request (2026-08-06), because as *defined* — cap ÷ Σ available caps —
 * it is arithmetic over observed numbers rather than a claim about the index, the same reasoning
 * that lets the Total row be unconditional. The header tooltip carries both caveats; the
 * per-line cap-weighted aggregates keep their gates, because those DO claim to describe the index.
 *
 * Null when this company has no cap in the period, or when nothing does — never 0, which would
 * read as "a real but negligible holding".
 */
export function weightPct(
  row: FundamentalGridRow, period: Period, totalCap: number | null | undefined,
): number | null {
  const cap = capOf(row, period);
  if (cap === null || !totalCap) return null;
  return (cap / totalCap) * 100;
}
