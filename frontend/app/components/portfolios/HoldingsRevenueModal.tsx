'use client';

import { useVirtualizer } from '@tanstack/react-virtual';
import { useEffect, useMemo, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import { invalidateReadCache } from '../../../lib/readCache';
import { cancelJob, startJob, type JobToast } from '../../../lib/stores/jobs';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { BADGE_TONE, StateBadge } from '../StateBadge';
import { MIN_YEAR_COVERAGE_PCT } from './marginData';
import PortfolioFundamentalsRefresh, { type RefreshScope } from './PortfolioFundamentalsRefresh';
import { type BenchTarget } from './benchSeries';

/**
 * Everything behind one growth chart: the per-company figures its line was built from, and — when
 * a benchmark is active — the same for the index it is drawn against.
 *
 * ⚠ TWO TABLES, ONE VIEW SWITCH. `Reported | Rebased | YoY %` is owned here and passed to both, so
 * the book and the index are always on the same basis; per-table switches would let a reader
 * compare a rebased index against reported euros and take the gap for a finding. `Rebased` is the
 * one the chart actually weights, and each table's footer carries the weighted result.
 *
 * ⚠ NO "PLOTTED SERIES" TABLE. There was one, listing the chart's own points. It is gone: on a
 * single company it repeated the reported figures three ways, and on a portfolio it repeated the
 * Rebased footer — the same numbers in a second place, which is one more place for them to
 * disagree. The footer row IS the line.
 */

/**
 * ⚠ THE PAYLOAD SHAPE AND THE PERIOD HELPERS MOVED TO `fundamentalBlend.ts`, with the blend maths
 * that is their only real consumer. Re-exported here because two dozen cards import `Target` (and a
 * few import the period helpers) from this module, and moving a type should not move a call site.
 */
export type { Resp, Row, WeightBasis } from './fundamentalBlend';
export { isEstimatePeriod, periodOrder } from './fundamentalBlend';
import { buildBlend, isEstimatePeriod, periodOrder, type Resp, type Row } from './fundamentalBlend';

/**
 * `2026-08-04T09:12:00Z` → `4 August 2026`. The date we concluded something, in the form a person
 * reads rather than the one a database stores.
 *
 * ⚠ FORMATTED IN UTC, NOT THE VIEWER'S ZONE. The stamp is written in UTC; rendered locally, a
 * fetch at 22:30Z becomes "5 August" for anyone east of London and "4 August" for anyone west —
 * the same event, two dates, in a tooltip whose whole job is to pin down when we looked.
 */
export const longDate = (iso: string): string =>
  new Date(iso).toLocaleDateString('en-GB',
    { day: 'numeric', month: 'long', year: 'numeric', timeZone: 'UTC' });

/**
 * The placeholder a three-line period cell uses for a line it has no value for.
 *
 * ⚠⚠ A NO-BREAK SPACE, NOT `' '`, AND WRITTEN AS AN ESCAPE. A block box whose only content is
 * COLLAPSIBLE whitespace generates no line box and is zero pixels tall — so a company with a gap
 * period gets a two-line cell among three-line ones, the `<td>` centres its content, and every
 * figure in that row sits half a line off its neighbours. It reads as a CSS problem and it is a
 * missing character.
 *
 * ⚠ IT IS THE LITERAL U+00A0 BYTE AND IT IS INVISIBLE HERE — nothing on screen distinguishes it
 * from the plain space it must not be. Centralising it IS the mitigation: one occurrence to
 * protect instead of three scattered through the cell markup, and a NAME at each use site saying
 * what is meant. If a bulk rewrite normalises whitespace this is the line to check; the symptom is
 * figures sitting half a line off their neighbours in any row with a gap period.
 */
const NBSP = ' ';



/** A period label → the date it ends on. The client twin of `_fundamental_blend.period_end`, and
 *  the only reason a cell can tell "we asked and there is nothing" from "nobody has asked yet": a
 *  fetch covers a period only if the period had already ENDED when the fetch ran. */
export const periodEndDate = (period: string): string => {
  // ⚠ `LTM` IS NOT A DATE AND MUST NOT BE PARSED AS ONE. Split on `-Q` it yields the string
  // `LTM-12-31`, which compares against real ISO dates as garbage — `'L' > '2'`, so every date
  // test it feeds silently takes the wrong branch. The trailing twelve months end at the company's
  // newest filing, which is by definition on or before today, so today is the honest bound: a
  // company fetched since then reads `No data` for a missing LTM (we asked, nothing newer), and
  // one not fetched since reads `—`.
  if (period === 'LTM') return new Date().toISOString().slice(0, 10);
  // ⚠ AND `2026e` IS NOT A DATE EITHER — split as-is it yields the string `2026e-12-31`, which is
  // not garbage that throws but garbage that COMPARES: `'2026e-12-31' > '2026-12-31'` is true, so
  // every date test it feeds quietly takes a branch nobody chose. The forecast for a fiscal year
  // ends when that year does; the `e` says who published it, not when it falls.
  if (isEstimatePeriod(period)) return periodEndDate(period.slice(0, -1));
  const [head, q] = period.split('-Q');
  return q ? `${head}-${['03-31', '06-30', '09-30', '12-31'][Number(q) - 1]}` : `${head}-12-31`;
};

export type Target = {
  portfolio_id?: number;
  holdings?: { isin: string; name?: string; weight: number }[];
  /** Set INSTEAD of the two above to read an index's constituents — see `benchSeries`. */
  universe?: string;
  /** ⚠ `'daily'` IS A REAL THIRD VALUE, not a typo — the two yield cards send it (a daily market
   *  cap off the daily close). Narrowing this to the tab's two would reject them at the type
   *  level, so it stays a string here and the tab-level toggle owns the other two. */
  cadence?: string;
};

// Sort key is a fixed column ('name'|'weight'|'ccy') OR a year string ('2018').
function cmp(a: number | string | null | undefined, b: number | string | null | undefined, dir: 'asc' | 'desc') {
  if (a == null && b == null) return 0;
  if (a == null) return 1;        // nulls last, both directions
  if (b == null) return -1;
  const r = (typeof a === 'string' || typeof b === 'string') ? String(a).localeCompare(String(b)) : a - b;
  return dir === 'desc' ? -r : r;
}

/**
 * The per-company matrix — one row per constituent, one column per period.
 *
 * ⚠ ONE COMPONENT FOR THE BOOK AND FOR THE INDEX. They are the same payload from the same
 * endpoint, and a second copy for the benchmark is how the two tables come to format a number, or
 * sort a null, differently — on a screen whose whole purpose is comparing them. Only the ingest
 * action differs, so it arrives as an optional callback rather than as a separate table.
 */
/**
 * What the period columns show.
 *
 * ⚠ `rebased` IS THE ONE THE CHART ACTUALLY WEIGHTS, and `yoy` — the obvious thing to ask for — is
 * NOT. The blend rebases each member to 100 at its own first period and takes a weighted average
 * of those LEVELS; it never averages growth rates. The two are different constructions and give
 * different lines whenever membership changes mid-series, so both are offered and the footer says
 * which one reproduces the chart.
 */
/**
/**
 * ⚠⚠ `contrib` IS THE ONLY ONE OF THE FOUR CELL VIEWS THAT IS **ADDITIVE**, and that is what it is
 * for. Reported, Rebased and YoY are each a transform of ONE company's own figures, so a column of
 * them ranks companies by size or by their own growth — neither of which is impact on the line.
 * `contrib` is that company's share of the LINE's move, in percentage points of it: weight × its own
 * growth over the interval, summing down the column to the footer. Sorting it is therefore a real
 * most-to-least-impact ranking, which sorting the other three is not (and they deliberately keep
 * ranking on the reported figure — see the period header's tooltip).
 */
type View = 'reported' | 'rebased' | 'yoy' | 'contrib';

/**
 * A period cell's contents, at a WIDTH THAT DOES NOT DEPEND ON THE STRING.
 *
 * ⚠ A `w-*` ON THE `<td>` IS NOT ENOUGH, AND THAT WAS THE FIRST ATTEMPT. In an auto-layout table a
 * cell's width is a suggestion: once the sticky Company column is capped (`max-w-0`, which is what
 * lets it truncate), the browser has no percentage column left to absorb slack and distributes it
 * across the rest in proportion to their CONTENT widths. So "6.3B" (reported), "108.6" (rebased)
 * and "+8.6%" (YoY) each produced a different layout and every column moved on a switch.
 *
 * Fixing the CONTENT width fixes the layout under any algorithm: the base widths are identical in
 * all three views, so whatever the browser distributes, it distributes the same way.
 */
/** `sum` → `+`, `mean` → `·`, `last` → `→`. ⚠ THE OPERATOR IS THE EXPLANATION, not decoration: a
 *  share count is the MEAN of its quarters (it is already an average over each one), so a `+`
 *  between them would say the figure is four times what it is. */
const LTM_JOIN: Record<string, string> = { sum: '+', mean: '·', last: '→' };
const LTM_RULE_TEXT: Record<string, string> = {
  sum: 'Summed — a flow measured over each period, so the filings in the window add to a year of it.',
  mean: 'AVERAGED, not summed — this line is already an average over each quarter, so adding them '
    + 'would report several times the figure.',
  last: 'The latest filing in the window — a balance is a point in time, not a flow.',
};

/**
 * The arithmetic behind one LTM cell.
 *
 * ⚠ THE WINDOW IS NOT "THE LAST FOUR QUARTERS", which is why the dates are listed rather than
 * implied: it is `k` consecutive filings — two for a semi-annual filer — refused outright when they
 * span more than a year, and a quarter dropped as implausible is absent from it. What is printed is
 * what the number was computed from, which is deliberately not everything the vendor filed.
 *
 * ⚠ ALWAYS THE REPORTED FIGURE, whatever the view switch says. Rebased and YoY transform the cell
 * AFTER the roll-up, so filings that reconciled to an index of 118.4 would be arithmetic the reader
 * cannot check. The tip names the reported value it does add to.
 */
function LtmTip({ parts, rule, reported, fmt }: {
  parts: { date: string; value: number }[];
  rule?: string | null;
  reported: number | null;
  fmt: (v: number | null | undefined) => string;
}) {
  const op = LTM_JOIN[rule ?? 'sum'] ?? '·';
  return (
    <AspectCard
      what="Trailing twelve months — the latest full year of trading, which ends mid-fiscal-year rather than on a fiscal year end."
      where={<span className="font-mono text-[11px]">
        {parts.map((p) => fmt(p.value)).join(` ${op} `)} = {fmt(reported)}
      </span>}
      when={<span className="font-mono text-[11px]">{parts.map((p) => p.date).join('  ·  ')}</span>}
      how={LTM_RULE_TEXT[rule ?? 'sum'] ?? 'Per this line’s declared roll-up.'} />
  );
}

const Cell = ({ children }: { children: React.ReactNode }) => (
  <span className="inline-block w-[4.5rem] text-right tabular-nums">{children}</span>
);

/**
 * The same trick for the IDENTITY columns — exchange, ticker, currency, cap.
 *
 * ⚠⚠ THIS IS WHAT MAKES VIRTUALIZATION SAFE ON AN AUTO-LAYOUT TABLE, and without it the table
 * visibly rebuilds as you scroll. `table-auto` sizes a column from the content it can SEE, and a
 * virtualized body only mounts ~40 rows — so a window containing `NASDAQ` and `GOOGL` gives wider
 * columns than one containing `NYSE` and `A`, and every column to the right of them slides as the
 * rows recycle. It is the same defect the period columns were already fixed for ("A FIXED WIDTH,
 * SO THE VIEW SWITCH MOVES NUMBERS AND NOTHING ELSE"), arriving by a different route: there the
 * content changed under a fixed row set, here the row set changes under fixed content.
 *
 * Fixing the CONTENT width fixes it under any layout algorithm, which is why this is a span rather
 * than a `w-*` on the `<th>` — on an auto table that class is a suggestion the browser may ignore.
 * The alternative is `table-fixed` + a colgroup (what the fundamentals grid does), which would
 * also mean giving up the `w-full`/`max-w-0` pair that lets Company absorb the slack.
 */
const Ident = ({ w, children }: { w: string; children: React.ReactNode }) => (
  <span className={`inline-block ${w} truncate align-bottom`}>{children}</span>
);

const VIEWS: [View, string, string][] = [
  ['reported', 'Reported', 'The figures as filed, in each company’s own reporting currency.'],
  ['rebased', 'Rebased', 'Each company indexed to 100 at ITS OWN first period — exactly what the '
    + 'chart weight-averages. The footer row is the weighted average, i.e. the plotted line.'],
  ['yoy', 'YoY %', 'Growth from that company’s previous reported period. ⚠ The chart does NOT '
    + 'average these: it averages the Rebased levels. The footer is the plotted line’s own '
    + 'period-on-period change, not the average of the column above it.'],
  ['contrib', 'Contribution', 'How many percentage points of the LINE’s own move each company '
    + 'accounts for — its weight in that period × its own growth over the interval. ⚠ The column '
    + 'SUMS to the footer, which is what makes it a decomposition rather than a ranking: sort a '
    + 'period column here and you get most-to-least impact, the drivers at the top and the '
    + 'detractors at the bottom. This is the one view whose sort does NOT rank on the figure.'],
];

function MatrixTable({ data, fmt, noun, metricLabel, valueIsCurrency, view, onRefresh }: {
  data: Resp;
  fmt: (v: number | null | undefined) => string;
  noun: string;
  /** The chart's own name — 'Revenue', 'FCF per share', 'ROIC'. Names the FIRST line of every period
   *  cell, so the three stacked numbers are all identified rather than only the two derived ones.
   *  Display-cased by the caller (`seriesLabel`); `noun` is the lowercase prose form and reads
   *  wrong as a column label. */
  metricLabel: string;
  /**
   * Whether this metric's figures are MONEY — `millions` / `per_share`, never `percent` or
   * `shares`.
   *
   * ⚠ DECIDED FROM THE DECLARED UNIT, NEVER SNIFFED FROM THE NAME. Same rule and same two members
   * as the backend's `_benchmark_fundamental_grid._CURRENCY_UNITS`: a share COUNT is a plain
   * number in millions like the currency lines around it, and `EPS (Diluted)` does not contain the
   * words "per share". Getting it wrong here only mislabels a column — getting it wrong there
   * divides a share count by an FX rate — but it is the same question and it gets the same answer
   * from the same place.
   */
  valueIsCurrency: boolean;
  /** ⚠ OWNED BY THE MODAL, NOT HERE — one switch drives the book's table and the index's together.
   *  Two independent switches would let a reader compare a rebased index against reported euros
   *  and read the gap as a finding. */
  view: View;
  /**
   * Re-fetch ONE company as a cancellable JOB, keyed on `company_id`. Returns the handle so the row
   * can offer the Cancel; the caller reloads the table when it lands.
   *
   * ⚠ IT FORCES, AND WITHOUT THAT IT IS A NO-OP THAT LOOKS LIKE A BUTTON. `is_cache_fresh` calls
   * the stored GuruFocus blob fresh for months, so an un-forced "refresh" of an already-loaded
   * company rewrites identical rows, spends nothing and changes nothing on screen.
   */
  onRefresh?: (row: Row) => Promise<{ id: string; done: Promise<JobToast> }>;
}) {
  /**
   * The sort, AND THE VIEW IT WAS CHOSEN UNDER.
   *
   * ⚠⚠ `basis` EXISTS SO SWITCHING VIEW DOES NOT REORDER THE TABLE. A period column ranks on the
   * reported figure in three views and on the contribution in `contrib`, so `view` in the
   * comparator meant every switch into or out of `contrib` silently reshuffled every row — while
   * the reader was looking at it, having asked for none of it. Freezing the basis at the moment
   * the sort is CHOSEN keeps the order until they choose again, which is what a sort is.
   *
   * ⚠ IT IS NOT THE SAME AS DROPPING `contrib`'s OWN RULE. Click a period column while in
   * `contrib` and it still ranks by impact; the rule now applies at click time rather than at
   * render time, so it decides the order once instead of re-deciding it on every switch.
   */
  const [sort, setSort] = useState<{ key: string; dir: 'asc' | 'desc'; basis: View }>(
    { key: 'weight', dir: 'desc', basis: 'reported' });
  /** Per-row refresh state, keyed by ISIN — separate from `ingest` because the two controls are
   *  different actions on the same row (first load vs re-ask the vendor) and can each be mid-flight
   *  with their own outcome. */
  const [refresh, setRefresh] = useState<
    Record<string, { busy?: boolean; jobId?: string; msg?: string }>>({});
  /** Rows whose Cancel was pressed before their job id came back. See `refreshOne`. */
  const pendingCancel = useRef<Set<string>>(new Set());
  /** Only the index carries a cap — see the row type. A column of dashes on a portfolio would
   *  imply the caps are missing rather than inapplicable. */
  const hasCap = data.rows.some((r) => r.market_cap_eur != null);
  /** €bn, because a raw 628076627000 beside a 51.76% is unreadable and the point of the column is
   *  that the division can be checked by eye. */
  const capBn = (v: number | null | undefined) => (
    v == null ? '—' : `${(v / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 })}`);

  /**
   * Every row's rebased series + the weight it carries, and the blend's own denominator.
   *
   * ⚠ THE DENOMINATOR IS THE **CONTRIBUTING** WEIGHT, NOT THE TABLE'S. `blend_series` is handed
   * only the members that carry the metric, and `_prepare` then drops any whose base is ≤ 0
   * (100 × v/0 is undefined; a negative base inverts the curve). Using the table's full weight
   * would put SP500's 264 contributors over its 489 listed rows and every coverage figure — and
   * the floor decision that rides on it — would be wrong by that ratio.
   */
  const blend = useMemo(() => buildBlend(data), [data]);

  /**
   * Per-row facts that every one of that row's period cells would otherwise re-derive.
   *
   * ⚠⚠ `Object.keys(r.revenue).filter(…).sort(periodOrder)` USED TO RUN PER CELL, and a row has as
   * many cells as the table has periods. `cellState` wants the newest period a row reported;
   * `cellOf` wants the ordered list, because the Rebased base is its FIRST period and the YoY
   * comparison its PREVIOUS one. On an index that is an allocate-filter-sort per cell — ~500 of them
   * to draw one screenful of ACWI, repeated on every scroll step that mounts a row, for an answer
   * that depends on the ROW alone and cannot change between two cells of it.
   *
   * ⚠ KEYED ON THE ROW OBJECT, NOT ON THE ISIN — the same rule as `blend.partOf` above. A payload
   * can carry one ISIN twice (a model listing an instrument at two weights), and an ISIN key would
   * hand both rows the first one's periods.
   */
  const rowFacts = useMemo(() => {
    const m = new Map<Row, { reported: string[]; newest: string | undefined; hasEstimate: boolean }>();
    for (const r of data.rows) {
      const reported = Object.keys(r.revenue).filter((p) => r.revenue[p] != null).sort(periodOrder);
      m.set(r, {
        reported,
        newest: reported[reported.length - 1],
        // ⚠ WHETHER THE ROW CARRIES A CONSENSUS FOR **ANY** YEAR — what separates "analysts do not
        // forecast this far" from "the estimates feed has never been fetched for this company".
        // `reported` is already filtered to non-null, so this is the same test `stateTitle` made.
        hasEstimate: reported.some(isEstimatePeriod),
      });
    }
    return m;
  }, [data]);

  /** Today, once per render instead of once per cell. `stateTitle` asks whether a period has ended
   *  yet, and `new Date().toISOString()` per empty cell is a Date allocation for a constant. */
  const todayISO = new Date().toISOString().slice(0, 10);

  /** The cap the weight beneath it was divided out of — the middle line of a period cell.
   *  Index only; a portfolio has no market cap behind its holding weights. */
  const capAt = (r: Row, y: string): number | null => r.market_cap_by_period?.[y] ?? null;
  /** Whether the payload carries per-period caps at all. A book stays two-line. */
  const hasPeriodCap = data.rows.some(
    (r) => r.market_cap_by_period && Object.keys(r.market_cap_by_period).length > 0);

  /**
   * One row's share of the plotted line IN THAT PERIOD — the number the second line of each cell
   * shows.
   *
   * ⚠ IT IS READ OFF `blend`, NEVER RECOMPUTED. Same `w`, same per-period denominator, so the
   * weights shown are the weights the footer actually used. A second derivation "the same way" is
   * how a table comes to disagree with the line it exists to explain.
   *
   * ⚠ NULL WHERE THE ROW DID NOT CONTRIBUTE, never 0. Three ways that happens and all of them mean
   * "this row is not in this period's average": it has no value that period, it has no weight, or
   * `_prepare` dropped it for a non-positive base (100 × v/0 is undefined). A 0% would read as a
   * holding so small it did not matter, which is a different claim.
   *
   * By construction this column sums to 100.00% within any period — which is what makes the cells
   * checkable against the footer.
   */
  /** Which period this row's figure for `y` came from — `y` itself, or the earlier period it was
   *  carried from. Null when the row does not contribute to `y` at all. */
  const sourceOf = (r: Row, y: string): string | null => {
    const p = blend.partOf.get(r);
    if (!p) return null;
    if (p.idx[y] != null) return y;
    return blend.from[r.isin]?.[y] ?? null;
  };

  const weightAt = (r: Row, y: string): number | null => {
    const d = blend.denom[y];
    // ⚠ A CARRIED ROW HAS A WEIGHT, because its figure is in the average. Testing `p.idx[y]` here
    // (its OWN value) left carried rows blank in the weight column while they sat in the
    // denominator — so the column added to less than 100% and the Total row would have said so.
    if (!d || !sourceOf(r, y)) return null;
    const w = blend.wAt(r, y);
    return w ? 100 * w / d : null;
  };

  /** The value a period column shows for one row, under the current view. */
  const cellOf = (r: Row, y: string): number | null => {
    /**
     * ⚠⚠ READ OFF `blend`, NEVER DERIVED FROM THE CELLS AROUND IT — the same rule as `weightAt`, and
     * here it is the whole feature. The honest denominator is the weight that spans the INTERVAL,
     * which is not `blend.denom[y]` and is not visible from this row (see `fundamentalBlend`'s
     * `step`); anything computed here from this row's two figures and its weight would look right,
     * sort plausibly, and not add up to the line's move.
     *
     * ⚠ IT DOES NOT FOLLOW THE CARRY BELOW. A carried row IS in the step — with the same value at
     * both ends, so its contribution is exactly 0.00pp, which is the true statement "it was in the
     * average and it did not move". Recursing to the source period would instead print the pp of a
     * DIFFERENT interval in this column.
     */
    if (view === 'contrib') return blend.contrib.get(r)?.[y]?.pp ?? null;
    // ⚠ A CARRIED PERIOD SHOWS THE FIGURE THE LINE USED, from the period it came from. Leaving it
    // blank while its weight sits in the column below would show a share of a number that is not
    // on screen. `yoy` is the exception: nothing new was reported, so there is no growth to state.
    const src = sourceOf(r, y);
    if (src && src !== y) return view === 'yoy' ? null : cellOf(r, src);
    const v = r.revenue[y];
    if (v == null) return null;
    if (view === 'reported') return v;
    const periods = rowFacts.get(r)?.reported ?? [];   // computed once per row — see `rowFacts`
    if (view === 'rebased') {
      const base = r.revenue[periods[0]] as number;
      return base > 0 ? 100 * v / base : null;
    }
    // ⚠ THE PREVIOUS PERIOD **THIS ROW REPORTED**, not the previous column. A company that skipped
    // a year would otherwise show its two-year growth in the same ink as everyone's one-year.
    const i = periods.indexOf(y);
    if (i <= 0) return null;                  // its first period has nothing to grow from
    const prev = r.revenue[periods[i - 1]] as number;
    return prev > 0 ? 100 * (v / prev - 1) : null;
  };

  /**
   * What the FIRST of a cell's stacked numbers is called — in the `Line` column and in the footer's
   * copy of it.
   *
   * ⚠ IT CANNOT BE THE METRIC'S NAME IN `contrib`. Reported, Rebased and YoY are all statements
   * about the metric (its level, its index, its growth), so naming the line `Revenue` is right in
   * each; a pp contribution is a share of the LINE's move and naming it `Revenue` would label the
   * one number here that is not a revenue figure after one. Same reason the total row carries no
   * currency.
   */
  const firstLineLabel = view === 'contrib' ? 'pp of move' : metricLabel;

  const cellText = (v: number | null) => (
    v == null ? '—'
      : view === 'reported' ? fmt(v)
        : view === 'rebased' ? v.toFixed(1)
          // ⚠ `pp`, NOT `%`, AND TWO DECIMALS. A percentage point of a line's move and a percent of
          // a company's own revenue are different quantities that would otherwise wear the same
          // sign; and on a 1,500-name index most contributions are hundredths, so 1dp would round
          // the whole tail to +0.0 and make the sort look broken where it is working.
          : view === 'contrib' ? `${v >= 0 ? '+' : ''}${v.toFixed(2)}pp`
            : `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`);

  /**
   * WHAT ONE PERIOD CELL IS, and there are exactly four possibilities.
   *
   * ⚠⚠ THE TWO EMPTY ONES ARE OPPOSITE FACTS AND USED TO RENDER IDENTICALLY. A blank period is
   * either "we asked GuruFocus and it publishes nothing for this period" or "no fetch has ever
   * covered a period this recent" — the first is about the company, the second about us, and only
   * one of them is fixed by pressing Refresh. `financials_fetched_at` is what separates them: a
   * period that ENDED before our last fetch was covered by it.
   *
   *   value        a number we hold
   *   no_data      asked, nothing published
   *   not_tried    the period ends after our last fetch — or we have never fetched at all
   *   unsubscribed the exchange is outside the GuruFocus subscription, so it cannot be asked
   *
   * ⚠ `unsubscribed` IS A ROW FACT RENDERED PER CELL, deliberately. It is true of every period, and
   * saying so in each one costs a little repetition and buys the ability to read a row the same
   * way everywhere — the alternative (one cell spanning the row) meant a row was either all
   * figures or all prose, and a period could not be read at all.
   */
  /**
   * What an empty cell says, in one place — the badge and the cell around it read from this, so
   * hovering anywhere in the cell gives the same sentence as hovering the chip.
   *
   * ⚠ THE DATE IS THE POINT OF `No data`. It is a CONCLUSION ("GuruFocus publishes nothing here"),
   * and a conclusion without a date is untestable — the vendor may have filled the period in since.
   * With no stamp it says so rather than implying a recent check: the row's figures prove a fetch
   * happened, they just cannot say when.
   */
  const stateTitle = (r: Row,
                      state: 'no_data' | 'not_tried' | 'unsubscribed' | 'not_covered'
                        | 'reported_actual',
                      y: string): string => {
    /**
     * ⚠⚠ IT STATES WHAT IS STORED, NOT WHY — because the two candidate reasons are not
     * distinguishable from here. Either analysts do not cover this company (most of a broad index:
     * measured 2026-08-14, 351 of ACWI's 1,715 charted names carry a consensus), or the estimates
     * feed has never been fetched for it. Unlike the reported statements there is NO per-feed
     * timestamp to separate them — `needs()` probes for a sentinel row, so an absence is an absence.
     *
     * ⚠ AN EARLIER WORDING ASSERTED THE FIRST AND SAID "Refresh cannot fill it". That was wrong
     * twice: the cause was never established, and the row Refresh was at the time narrowed to
     * `feeds=statements` and did not even ask for estimates. Both halves are fixed — the button
     * now asks (`refreshRow`) and this sentence points at it instead of ruling it out.
     */
    if (state === 'reported_actual') {
      return `This company has already REPORTED ${y.slice(0, -1)} — the figure is in the `
        + `${y.slice(0, -1)} column to the left. The forecast columns are the union across `
        + 'holdings, so a December filer whose newest year is one back puts this column on the '
        + 'table while a June filer beside it has already closed that year. Nothing is missing.';
    }
    if (state === 'not_covered') {
      // ⚠⚠ TWO SENTENCES, BECAUSE ONE OF THEM IS ACTIONABLE AND THE OTHER IS NOT. A row that
      // carries a consensus for ANY year proves the estimates feed was fetched for it, so an
      // empty year is analysts not forecasting that far — pressing Refresh again cannot help. A
      // row with none at all is genuinely ambiguous (never asked, or asked and uncovered), and
      // there is no per-feed timestamp to separate those — so it points at the button instead of
      // ruling it out. Saying the first sentence over the second case is what sent me looking for
      // a missing fetch on a company that simply files in June.
      const has = rowFacts.get(r)?.hasEstimate ?? false;   // once per row — see `rowFacts`
      return has
        ? `Analysts cover this company, but publish no ${y.slice(0, -1)} figure — their `
          + 'consensus does not reach this year. Refreshing cannot add it.'
        : 'No analyst consensus stored for this company. Estimates are a separate GuruFocus feed '
          + 'from the reported statements, so press Refresh on this row to ask for it. Most '
          + 'companies in a broad index are not covered at all, in which case an empty cell after '
          + 'that is the answer rather than a gap.';
    }
    // ⚠⚠ A PERIOD THAT HAS NOT ENDED IS NOT "NOT TRIED" — nobody can have reported it, and no
    // number of presses will change that. Both sentences are arithmetically true ("this period
    // ends after our last fetch"), but one sends the reader to press Refresh for a fiscal year
    // that is still running. Measured: Adyen's FY2026 ends 31 December, we fetched 12 August, and
    // the cell asked for a refresh that could never help. The LTM column is the answer for the
    // year in progress, so the message points there instead.
    if (state === 'not_tried' && periodEndDate(y) > todayISO) {
      return `${y} has not ended yet, so nobody has reported it — this is not something a refresh `
        + 'can fill. The LTM column is the trailing twelve months to the newest quarterly filing, '
        + 'which is what this company has published so far.';
    }
    if (state === 'unsubscribed') {
      return `${r.ticker ?? ''}@${r.exchange ?? '?'} is on an exchange outside our GuruFocus `
        + 'subscription, so this period cannot be fetched at all.';
    }
    if (state === 'not_tried') {
      return r.financials_fetched_at
        ? `Not tried: this period ends after our last fetch `
          + `(${longDate(r.financials_fetched_at)}). Press Refresh on this row to ask for it.`
        : 'Not tried: this period is newer than anything we hold for this company, and we have no '
          + 'record of asking since. Press Refresh on this row.';
    }
    return (r.financials_fetched_at
      ? `Checked ${longDate(r.financials_fetched_at)}: we asked GuruFocus for this company and it `
        + `publishes no ${noun} for this period.`
      : `We asked GuruFocus for this company — the figures in this row are the answer — and it `
        + `publishes no ${noun} for this period. We have no record of WHEN we last checked; press `
        + 'Refresh to stamp it.')
      + ' Refreshing will not change the answer unless GuruFocus has published something since.';
  };

  const cellState = (r: Row, y: string):
  'value' | 'no_data' | 'not_tried' | 'unsubscribed' | 'not_covered' | 'reported_actual' => {
    if (r.revenue[y] != null) return 'value';
    /**
     * ⚠⚠ AN EMPTY FORECAST CELL IS NOT ONE FACT BUT THREE, and `No data` / `—` would misdescribe
     * all of them. There is no "did we fetch this period" question for a year that has not
     * happened, so the ordinary two states do not apply.
     *
     * ⚠ THE FIRST ONE IS THE SURPRISE: THE COMPANY HAS ALREADY REPORTED THAT YEAR. The estimate
     * columns are the UNION across holdings, so a December filer whose newest filing is 2025 puts a
     * `2026e` column on the table — and a JUNE filer sitting beside it has already closed FY2026.
     * Measured on KLA Corp: actual FY2026 = 3.76 filed 2026-06-30, with a consensus running
     * 2027e–2031e. Its `2026e` cell is empty because that year is history for KLA, and calling that
     * "no analyst consensus" is wrong twice over — it has six years of it, and the figure the
     * reader wants is one column to the left.
     */
    if (isEstimatePeriod(y)) {
      return r.revenue[y.slice(0, -1)] != null ? 'reported_actual' : 'not_covered';
    }
    // ⚠ A CARRIED PERIOD IS A VALUE, NOT A GAP. The line used a number here — this row's latest —
    // so badging it `No data` would deny a figure that is visibly in the average, and the weight
    // column beneath it would be a share of nothing. The tooltip says where it came from.
    if (sourceOf(r, y)) return 'value';
    if (r.status === 'unsubscribed') return 'unsubscribed';
    // ⚠⚠ A ROW WITH ANY FIGURE HAS BEEN FETCHED — THE PROOF IS THE FIGURE. GuruFocus returns the
    // whole history in one blob, so every period up to the newest one we hold was covered by that
    // fetch: a blank there is GuruFocus publishing nothing, not us never asking. Universal Music
    // is the case that made this obvious — nine annual rows from 2018, nothing for 2015/2016
    // (it was inside Vivendi until the 2021 spin-off), and those cells read "not tried" purely
    // because `financials_fetched_at` is NULL on every row until its next fetch. The data in front
    // of us answers the question the timestamp was added for.
    const newest = rowFacts.get(r)?.newest;              // once per row — see `rowFacts`
    // Lexical order is chronological for both vocabularies — `2015` < `2018`, `2025-Q1` < `2025-Q3`
    // — and every period in one table shares one vocabulary.
    if (newest && y <= newest) return 'no_data';
    // ⚠ ONLY THE TRAILING EDGE IS GENUINELY AMBIGUOUS, and only there does the timestamp decide.
    // The period's own END vs the fetch date: `2025-Q3` ends 2025-09-30, and a fetch on 2025-08-01
    // could not have seen it — calling that "no data" would blame GuruFocus for our own gap.
    const asked = r.financials_fetched_at;
    return asked && periodEndDate(y) <= asked.slice(0, 10) ? 'no_data' : 'not_tried';
  };
  // ⚠ `basis: view` ON BOTH BRANCHES — a re-click in a different view is a NEW sort and must
  // re-rank on what is on screen now, not on whatever was showing when the column was first
  // picked. Only an untouched sort survives a view switch.
  const toggle = (key: string) => setSort((s) => (s.key === key
    ? { key, dir: s.dir === 'desc' ? 'asc' : 'desc', basis: view }
    // Names/currency read A→Z first; weight, cap and figures read biggest-first.
    : { key, dir: (key === 'name' || key === 'ccy') ? 'asc' : 'desc', basis: view }));
  const caret = (k: string) => (sort.key === k ? (sort.dir === 'desc' ? ' ▾' : ' ▴') : '');

  /**
   * ⚠⚠ A PERIOD COLUMN SORTS ON THE **REPORTED** FIGURE IN EVERY VIEW BUT ONE, and that is a promise
   * the header makes in writing. Rebased and YoY are per-company transforms, so ranking a column of
   * them answers "who is biggest / who grew fastest", not "what moved this line" — and switching
   * view under a reader is not the moment to also switch what their sort means.
   *
   * ⚠ `contrib` IS THE EXCEPTION BECAUSE IT IS THE POINT OF IT: the pp column is additive, so its
   * order IS the impact ranking. Sorting it by the reported figure instead would rank a
   * decomposition by company size — the exact thing this view exists to stop doing.
   *
   * ⚠ SIGNED, NOT `Math.abs`. Descending puts the drivers at the top and the detractors at the
   * bottom, so one column read top-and-bottom answers both halves; ranking on magnitude interleaves
   * a +5pp driver with a −5pp drag and the sign — which is the finding — stops being visible in the
   * order at all. One click flips it. Rows with no contribution stay NULL and `cmp` keeps nulls last
   * in both directions: absent is not a small number.
   */
  const rows = useMemo(() => {
    const get = (r: Row): number | string | null | undefined => (
      sort.key === 'name' ? r.name.toLowerCase()
        : sort.key === 'exchange' ? (r.exchange ?? '')
          : sort.key === 'ticker' ? (r.ticker ?? '')
            : sort.key === 'weight' ? r.weight_pct
              : sort.key === 'cap' ? (r.market_cap_eur ?? null)
                : sort.key === 'ccy' ? (r.currency ?? '')
                  : sort.basis === 'contrib' ? (blend.contrib.get(r)?.[sort.key]?.pp ?? null)
                    : r.revenue[sort.key]);    // a period column
    return [...data.rows].sort((a, b) => cmp(get(a), get(b), sort.dir));
    // ⚠ `view` IS DELIBERATELY NOT A DEPENDENCY. It used to be, and that is precisely what made
    // the table reorder on a tab switch — see `sort.basis`.
  }, [data, sort, blend]);

  /**
   * ⚠⚠ ROW VIRTUALIZATION — AND THIS TABLE HAS ITS OWN SCROLL BOX BECAUSE OF IT.
   *
   * It used to scroll inside the modal BODY, which holds the book's matrix and the index's
   * stacked together. Virtualizing against a shared scroll parent means each table's offsets are
   * measured from a container it does not start at, so both need a `scrollMargin` that changes
   * whenever the notes above them wrap. Giving each matrix its own bounded box removes the problem
   * rather than compensating for it, and it is also better to read: the header and the total row
   * stay put while you scroll 1,514 constituents.
   *
   * Measured on ACWI revenue: 1,514 rows x 12 periods is **45,420 cells** with the weight line
   * added (27,252 before it). ~40 rows mounted turns that into ~1,200.
   *
   * ⚠ `measureElement` RATHER THAN A FIXED ESTIMATE, because the rows here are genuinely NOT all
   * the same height — an `unsubscribed` or `no_data` row spans the period columns with a single
   * line of text and no weight beneath it, so it is shorter than a normal two-line row. That is
   * the case a hardcoded `estimateSize` gets wrong, and on an index it is hundreds of rows.
   */
  const scrollRef = useRef<HTMLDivElement>(null);
  const rowVirtualizer = useVirtualizer<HTMLDivElement, HTMLTableRowElement>({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 56,               // a three-line row at the default rem scale
    // The virtualizer measures from the container top, which is the sticky header rather than the
    // first data row. That error is constant and about a row wide; the overscan absorbs it.
    overscan: 14,
  });
  const vItems = rowVirtualizer.getVirtualItems();
  const padTop = vItems.length ? vItems[0].start : 0;
  const padBottom = vItems.length
    ? rowVirtualizer.getTotalSize() - vItems[vItems.length - 1].end
    : 0;
  /** ⚠ MUST TRACK THE HEADER EXACTLY — a spacer one short leaves the table free to re-fit its
   *  columns around the gap, which is the jitter `Ident` exists to prevent. #, Company, [Refresh],
   *  GF exch, Ticker, [Mkt cap], Ccy, Line, then one per period. */
  const colCount = 6 + (onRefresh ? 1 : 0) + (hasCap ? 1 : 0) + data.years.length;

  /**
   * The per-row refresh, as a JOB.
   *
   * ⚠ A JOB RATHER THAN A BLOCKING FETCH, FOR THE CANCEL — not for the progress bar. A plain
   * request holds the connection open for as long as GuruFocus takes and gives the reader no way
   * to stop it: abort the fetch and the server carries on, having already decided to spend the
   * quota. Through `startJob` the row gets the generic toast (its outcome, the running quota spend,
   * a Cancel that outlives this modal) and several rows can run at once.
   *
   * ⚠ `jobId` IS KEPT SO THE ROW ITSELF CAN CANCEL. Losing it would leave the toast as the only
   * way to stop a fetch the reader started from here, which on a 1,500-row table means hunting for
   * the right card.
   */
  const refreshOne = async (r: Row) => {
    if (!onRefresh || r.company_id == null) return;
    // ⚠ `busy` FROM THE CLICK, NOT FROM THE JOB ID. The button has exactly TWO states — Refresh and
    // Cancel — so it must flip on the press, before the id exists. A third "Refreshing…" state for
    // the ~200 ms `startJob` takes is a label nobody can act on that flickers past on every press.
    setRefresh((s) => ({ ...s, [r.isin]: { busy: true } }));
    try {
      const { id, done } = await onRefresh(r);
      setRefresh((s) => ({ ...s, [r.isin]: { busy: true, jobId: id } }));
      // ⚠ A CANCEL PRESSED DURING THAT GAP IS HONOURED HERE, or the two-state button is a lie: the
      // reader pressed Cancel, the label said Cancel, and the fetch would have run to completion
      // anyway because there was nothing to cancel yet. A ref, not state — this closure captured
      // its `refresh` on the way in and would never see a later press.
      if (pendingCancel.current.delete(r.isin)) await cancelJob(id);
      const job = await done;
      setRefresh((s) => ({ ...s, [r.isin]: job.status === 'failed'
        ? { msg: job.summary || 'the fetch failed — see the console' } : {} }));
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      // The row gets one short line (a ⚠ with the text in its tooltip, where there is no room for
      // more); the console gets it in full, as everywhere else here.
      console.warn(`[bb:fundamentals] refresh ${r.name} (${r.isin}): ${msg}`, e);
      pendingCancel.current.delete(r.isin);
      setRefresh((s) => ({ ...s, [r.isin]: { msg } }));
    }
  };

  /**
   * How much room the Company column gets, before truncation.
   *
   * ⚠ AN EXPLICIT FLOOR, BECAUSE `w-full` + `max-w-0` GIVES IT NONE. That pair is what makes this
   * column take the slack AND truncate instead of stretching the table — but on a wide matrix (a
   * dozen period columns, each with three stacked numbers) there is no slack left, so its
   * min-content width is the truncation point: near zero. Every name ended up clipped to a few
   * characters. `min-width` beats `max-width` when the two conflict, so this widens it without
   * touching the truncation.
   *
   * ⚠ ONE CONSTANT, THREE CELLS. The header, the row and the footer are the same column; declaring
   * the width in three string literals is how a sticky column comes to have three widths, and the
   * pinned cells then misalign against each other as you scroll.
   */
  const nameCol = 'min-w-[24rem]';

  /**
   * The row-number column's width, and the offset the Company column is pinned at.
   *
   * 3.5rem, close to the `#` column on the /benchmarks fundamentals grid (`fixedWidthsRem`), so the
   * two tables that list the same constituents indent about the same.
   *
   * ⚠⚠ ONE NUMBER, USED TWICE, AND THAT IS THE WHOLE POINT. Two frozen columns only line up while
   * the first one's WIDTH and the second one's `left` OFFSET are identical; expressed as `w-14` +
   * `left-14` they were two separate Tailwind rules, and a table's auto layout is free to overrule
   * a cell width — the `w-full` Company column claimed the slack, squeezed the number column to its
   * min-content, and a strip of scrolling table showed through the difference.
   *
   * The cells carry `px-2`, not `px-3`: at 3.5rem the wider padding leaves a three-digit row number
   * nowhere to go, and an overflowing number pushes the column back out — the same gap, from the
   * other side.
   */
  const NUM_W = '3.5rem';
  /** ⚠ A STYLE ATTRIBUTE, NOT A UTILITY CLASS — the one place in this file that earns it. The
   *  agreement is between two cells and is a MEASUREMENT, not a token; inline it is applied
   *  directly and both sides read the same constant. */
  const numCell = { width: NUM_W, minWidth: NUM_W, maxWidth: NUM_W };
  /**
   * ⚠⚠ THE COMPANY COLUMN IS NOW A FIXED WIDTH, AND IT HAD TO BECOME ONE. A third frozen column
   * pins at `first + second`, so the second's width has to be a NUMBER — and `w-full` made it
   * whatever slack the table had left, which is a different value on every screen and on every
   * cadence (twelve period columns or forty). Pinned against a guess, the Refresh column would sit
   * over the names on a wide table and leave a gap on a narrow one.
   *
   * ⚠ THE TABLE STILL FILLS: `<table className="w-full">` distributes what is left across the
   * period columns instead. They are the ones that should absorb it — a wider window shows more of
   * the figures, not more whitespace under a company name.
   */
  const NAME_W = '24rem';
  const nameStick = { left: NUM_W, width: NAME_W, minWidth: NAME_W, maxWidth: NAME_W };
  /** The third frozen column, at the sum of the two before it. One expression, so it cannot drift
   *  from the widths it is derived from. */
  const refreshStick = { left: `calc(${NUM_W} + ${NAME_W})` };

  const cancelRow = async (isin: string) => {
    const id = refresh[isin]?.jobId;
    // ⚠ NO INLINE MESSAGE — `cancelJob` puts "cancelling…" on the job's own card the instant it is
    // pressed, and that card carries the outcome. Two places reporting one job is two to keep in
    // step.
    if (id) { await cancelJob(id); return; }
    // Pressed before the job id came back. Remembered, and `refreshOne` cancels the moment it has
    // something to cancel — see the ⚠ there.
    pendingCancel.current.add(isin);
  };

  /**
   * ⚠⚠ RENDERED ROWS, KEPT BY INDEX — THIS IS WHAT MAKES SCROLLING CHEAP, AND WITHOUT IT
   * VIRTUALIZATION ONLY HALF WORKS.
   *
   * The virtualiser re-renders this component on every scroll step while the window moves by one or
   * two rows — but the map below rebuilt ALL ~40 of them each time. A row here is not cheap markup:
   * ~20 cells, a `title` concatenated per period cell, and an `InfoTip` (two `useState`, a
   * `useLayoutEffect`, an `useEffect`) for every EMPTY one. On ACWI that is several hundred stateful
   * components re-rendered per frame to move two rows past the header.
   *
   * React bails out of re-rendering a subtree when it is handed back the IDENTICAL element object,
   * so returning the cached element for an unchanged row skips all of it. That is exactly what a
   * `React.memo` row component buys, without having to thread a dozen helpers through as stable
   * props — which on this row would mean `useCallback`-ing every one of `cellOf`, `cellState`,
   * `stateTitle`, `weightAt`… and any one of them missed silently disables the memo again.
   *
   * ⚠⚠ THE DEPENDENCY LIST IS THE CORRECTNESS ARGUMENT, NOT AN OPTIMISATION HINT. A cached element
   * holds the closures it was built with, so EVERY value those closures read has to invalidate this
   * map. They read `data`, `view`, `blend`, `rowFacts`, `refresh`, `fmt`, `noun`, `metricLabel`,
   * `valueIsCurrency`, `onRefresh`, `hasCap`, `hasPeriodCap` and the sorted `rows` — all listed. The
   * helper FUNCTIONS themselves are deliberately absent: they are re-created every render, so
   * listing them would clear the cache on every frame and undo the whole thing. That is safe
   * precisely because each is a pure closure over the values above.
   *
   * ⚠ SO ARE `numCell` / `nameStick` / `refreshStick` — fresh object literals every render, over
   * constants that never change. Listing them would also clear the cache every frame.
   *
   * ⚠ IT IS PRUNED TO THE WINDOW, or it would retain 1,949 element trees for a table showing forty.
   * That is not a regression against `React.memo`: a row scrolled out of range is unmounted there
   * too, and rebuilt on the way back.
   */
  const rowCache = useMemo(
    () => new Map<number, React.ReactElement>(),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [rows, data, view, blend, rowFacts, refresh, fmt, noun, metricLabel, valueIsCurrency,
      onRefresh, hasCap, hasPeriodCap, todayISO],
  );
  if (vItems.length && rowCache.size > vItems.length * 4) {
    // ⚠ ONLY WHEN IT HAS GROWN WELL PAST THE WINDOW, not every frame — walking the map on each
    // scroll step to delete two entries is the kind of tidying that costs more than it saves.
    const lo = vItems[0].index;
    const hi = vItems[vItems.length - 1].index;
    for (const k of rowCache.keys()) if (k < lo || k > hi) rowCache.delete(k);
  }

  return (
    /* ⚠ `max-h-`, NOT `h-`. The grid's own note argues the opposite ("a fixed height means the box
       is the same size before and after"), and the reason it does not apply here is that this
       modal stacks TWO of these: a fixed 46vh each would leave the AEX's 22 rows sitting in a
       half-empty box above another half-empty box, in a dialog that is only 84vh tall. A book of
       twenty holdings is the common case and it should not have to scroll past dead space to
       reach the index below it. The cost — the box growing once as the fetch lands — is paid
       inside a modal that opened on a loading line anyway. */
    <div ref={scrollRef} className="overflow-auto max-h-[46vh] rounded-lg border border-neutral-800/40">
      <table className="w-full text-xs">
        {/* ⚠ STICKY, NOW THAT THE BOX SCROLLS ITSELF. Scrolling 1,514 constituents past a header
            that has left the screen makes the period columns unreadable — and the footer IS the
            plotted line, which is the one row you want in view while reading any other. */}
        <thead className="bg-page sticky top-0 z-20">
          <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40 [&>th]:cursor-pointer [&>th]:select-none [&>th:hover]:text-fg-soft">
            {/* Company takes the slack so the table fills the width; periods keep natural size.
                ⚠ z ABOVE ITS OWN ROW: this cell pins in BOTH directions, so it has to outrank the
                sticky header beside it and the sticky name cells below it. */}
            {/* ⚠ NOT SORTABLE, and `cursor-default!` opts it out of the row-level rule that makes
                every other header a sort toggle. There is nothing to sort BY: the number is the
                position, so "sort by position" is whatever sort you are already in. */}
            <th className={`px-2 py-1.5 font-medium text-right sticky left-0 bg-page z-30
                            cursor-default!`} style={numCell}
              title="Row number in the current sort — it renumbers when you re-sort.">#</th>
            <th className={`px-3 py-1.5 font-medium text-left sticky bg-page z-30 ${nameCol}`} style={nameStick}
              onClick={() => toggle('name')}>Company{caret('name')}</th>
            {/* ⚠ ITS OWN COLUMN, AND NOT SORTABLE — every other header here toggles a sort, so this
                one carries `cursor-default` explicitly to opt out of the row-level rule above. The
                buttons under it are self-labelling, so the heading only has to name the column. */}
            {onRefresh && (
              <th className="px-2 py-1.5 font-medium text-left whitespace-nowrap cursor-default! sticky bg-page z-30" style={refreshStick}
                title="Re-fetch ONE company's financials from GuruFocus — that row only, one API call, bypassing our stored copy of its blob. Progress and a Cancel appear in the pop-ups bottom-right.">
                Refresh
              </th>
            )}
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('exchange')}>GF exch{caret('exchange')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('ticker')}>Ticker{caret('ticker')}</th>
            {hasCap && (
              <th className="px-3 py-1.5 font-medium text-right whitespace-nowrap" onClick={() => toggle('cap')}
                title="company.market_cap_eur as stored today — full cap, not free-float. This is the numerator of the Weight beside it: cap ÷ the total of this column.">
                Mkt cap €bn{caret('cap')}
              </th>
            )}
            {/* ⚠ THE `Weight` COLUMN WAS REMOVED (2026-08-10, on request) — the weight now lives
                inside every period cell, on the basis that period actually used. Keeping both put
                two different percentages under one word on one screen: this one was today's cap
                over the table's total, the cell's is that period's cap over that period's
                reporters. For an index the column was simply the wrong one of the two.

                ⚠ WHAT WENT WITH IT, ON THE **BOOK'S** TABLE ONLY: its total was the share of the
                whole book these companies make up — under 100%, because cash, bonds and anything
                unpriceable are in that denominator and are not listed here. The per-cell weight
                cannot say that: it renormalises over whoever reported, so it sums to 100% by
                construction. If "these holdings are 87% of the portfolio" is wanted back, it
                belongs as a line under the table, not as a column that looks like the cell weight.

                ⚠ `sort.key` STILL DEFAULTS TO `'weight'` AND THAT IS DELIBERATE, not a leftover.
                It is the order the server already returns (`rows.sort` by weight desc), so the
                table opens biggest-first; it is simply no longer reachable from a header, because
                there is no header for it to be wrong about. */}
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('ccy')}>Ccy{caret('ccy')}</th>
            {/* ⚠ THE LINE-LABEL COLUMN — names the three numbers stacked in every period cell.
                It sits LAST of the identity columns so it is adjacent to the figures it names.

                ⚠ IT IS NOT STICKY, AND IT CANNOT BE. Company is the only pinned column because it
                is also the `w-full`/`max-w-0` slack absorber — its rendered width is decided by
                layout, so nothing after it has a left offset CSS could be given. Scrolled right to
                2024 these labels are off screen; the three lines keep a fixed ORDER
                (figure, cap, weight) for exactly that reason, and the footer repeats the names.

                ⚠ NOT SORTABLE — it holds no data. Every other header here toggles a sort, so this
                one deliberately drops the pointer/hover affordance rather than looking dead. */}
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap
                           cursor-default hover:!text-fg-faint"
              title="What each of the three numbers in a period cell is: the figure itself, the
market cap it was weighted by in that period, and the weight that produced.">
              Line
            </th>
            {/* ⚠ A FIXED WIDTH, SO THE VIEW SWITCH MOVES NUMBERS AND NOTHING ELSE. "6.3B", "108.6"
                and "+8.6%" are different lengths, and on an auto-layout table every column
                re-measures on each switch — the row you were reading slides sideways, which is
                exactly what makes two views hard to compare. 6rem holds the longest of the three
                at this size; the Company column still absorbs the slack. */}
            {data.years.map((y) => (
              <th key={y} className="px-3 py-1.5 font-medium text-right whitespace-nowrap w-24"
                onClick={() => toggle(y)}
                title={`${y}. Each cell carries the figure on top and, beneath it, that company’s `
                  + 'share of the weight behind THIS period’s line — which moves from period to '
                  + 'period as companies enter and leave the average, even though the Weight '
                  + 'column beside the name is a single stored number. The second line sums to '
                  + '100% down the column; the footer says what share of the index that is. '
                  + (view === 'contrib'
                    ? 'Sorting ranks on the pp impact in this view — most to least, drivers at the '
                      + 'top and detractors at the bottom.'
                    : 'Sorting still ranks on the figure, not the weight.')}>
                {y}{caret(y)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {padTop > 0 && <tr aria-hidden><td colSpan={colCount} style={{ height: padTop }} /></tr>}
          {vItems.map((vi) => {
            // ⚠ THE SAME ELEMENT OBJECT FOR AN UNCHANGED ROW — see `rowCache`. This is the line
            // that tells React to skip the row's whole subtree; building it again would be
            // indistinguishable on screen and is the cost this exists to remove.
            const cached = rowCache.get(vi.index);
            if (cached) return cached;
            const r = rows[vi.index];
            const i = vi.index;
            const el = (
            <tr key={`${r.isin}-${i}`} data-index={i} ref={rowVirtualizer.measureElement}
              // ⚠⚠ THE RULE IS ON THE CELLS, NOT THE ROW. A `border-b` on a `<tr>` is drawn by the
              // TABLE under `border-collapse: collapse` (Preflight sets it), and a `position:
              // sticky` cell paints its own background in a later stacking context — so the two
              // frozen columns, and any cell whose height the virtualiser has re-measured, can
              // cover the row's border and leave the line broken in patches. Per-cell borders are
              // painted by each cell, so every column draws its own segment and the rule is
              // continuous whatever is pinned or re-measured above it.
              className="group [&>td]:border-b [&>td]:border-neutral-800/20
                         hover:bg-overlay/[0.02]">
              {/* ⚠ THE POSITION IN THE SORTED LIST, NOT AN ID. It renumbers 1..n when you re-sort,
                  which is the point: it answers "how far down is this" and "how many are there",
                  and it gives two people looking at the same screen a way to say which row they
                  mean. `vi.index` is the index in `rows` — the sorted array — so the virtualiser's
                  windowing cannot make it skip. */}
              <td className={`px-2 py-1.5 text-right font-mono text-fg-faint tabular-nums
                              sticky left-0 bg-card z-10`} style={numCell}>{i + 1}</td>
              {/* ⚠ THE BADGE LIVES IN THE PINNED NAME CELL, like the fundamentals grid's. The
                  moment you are asking "why is this row's weight empty?" you are scrolled right
                  looking at the empty cells, and a badge in any other column has gone with them.
                  `shrink-0` + `truncate` so a long name yields space to it rather than pushing it
                  out. */}
              <td className={`px-3 py-1.5 text-fg-soft sticky bg-card z-10 ${nameCol}`} style={nameStick}>
                <span className="flex items-center gap-1.5 min-w-0">
                  {/* ⚠ THE `NOT IN LINE` BADGE WAS REMOVED (2026-08-12, on request) — THE EXCLUSION
                      IT ANNOUNCED WAS NOT. `_prepare` still drops a member whose first reported
                      period is <= 0 (a level series is indexed to 100 at its own first point, and
                      dividing by zero is undefined), so such a row still contributes nothing to the
                      line while its weight stays in the coverage denominator. Universal Music is
                      2.68% of the AEX and is out of the annual revenue line for exactly that
                      reason. The REASON survives in the period cells' tooltips (`blend.excluded`,
                      below) — it is no longer announced, only available. */}
                  <span className="truncate" title={r.name}>{r.name}</span>
                  {/* The stated reason, where the `no_data` cell below cannot carry it — this row
                      has figures, so it renders no such cell. One glyph, full text in the tooltip
                      and in the console. */}
                  {refresh[r.isin]?.msg && r.status !== 'no_data' && (
                    <span className="shrink-0 text-[11px] text-warn-300 cursor-help"
                      title={refresh[r.isin]?.msg}>⚠</span>
                  )}
                </span>
              </td>
              {/* ⚠ ONE COMPANY, ONE API CALL — the whole point of a per-row control. The two
                  table-level buttons above are bulk: the book's re-asks every holding, and the
                  index's fills only the constituents missing the feed ENTIRELY, so it can never
                  touch one that already carries the sentinel. This is the only way to move a single
                  row, and on a 1,900-constituent index it is the difference between one call and a
                  four-figure spend.

                  ⚠ NOT ON AN `unsubscribed` ROW — that exchange is outside the GuruFocus
                  subscription, so the call is spent and nothing comes back. The cell already says
                  so, and a control that can only fail is worse than no control.

                  ⚠ NOR WITHOUT A `company_id`. The job is keyed on it; rendering a button that
                  cannot be wired up would be a control that does nothing when pressed. */}
              {onRefresh && (
                <td className="px-2 py-1.5 text-center align-top sticky bg-card z-10" style={refreshStick}>
                  {/* ⚠ EXACTLY TWO STATES: Refresh, or Cancel. Not three — a "Refreshing…" label
                      for the ~200 ms `startJob` takes is a state nobody can act on that flickers
                      past on every press. The flip is driven by `busy`, set on the CLICK, and a
                      Cancel pressed before the job id exists is honoured the moment it does (see
                      `refreshOne`), so the label never promises something it cannot do.

                      ⚠ ONE CONTROL, as the two bulk buttons above the tables are. The toast carries
                      a Cancel too and both are correct — the reader who wants to stop it is looking
                      at the row they just pressed. */}
                  {r.status !== 'unsubscribed' && r.company_id != null && (
                    <button type="button"
                      onClick={() => {
                        if (refresh[r.isin]?.busy) { void cancelRow(r.isin); } else { void refreshOne(r); }
                      }}
                      title={refresh[r.isin]?.busy
                        ? 'Stop this fetch. It halts at the next feed boundary; whatever was already written stays written.'
                        : `Re-fetch ${r.name}'s financials from GuruFocus — this company only, one `
                          + 'API call. It bypasses our stored copy of its blob, which is the only '
                          + 'thing that can update a row that already has figures. Progress and a '
                          + 'Cancel appear in the pop-ups bottom-right.'}
                      className={`cursor-pointer text-[12px] px-2 py-0.5 rounded-lg border
                                  whitespace-nowrap transition-colors ${refresh[r.isin]?.busy
                        ? 'border-warn-500/50 text-warn-400 hover:bg-warn-500/10'
                        : 'border-accent-600/40 text-accent-400 hover:bg-overlay/5'}`}>
                      {refresh[r.isin]?.busy ? 'Cancel' : 'Refresh'}
                    </button>
                  )}
                </td>
              )}
              <td className="px-3 py-1.5 font-mono text-[12px] text-fg-subtle whitespace-nowrap">
                <Ident w="w-14">{r.exchange ?? '—'}</Ident>
              </td>
              <td className="px-3 py-1.5 font-mono text-[12px] whitespace-nowrap">
                <Ident w="w-[4.75rem]">
                  {r.ticker
                    ? <a href={guruFocusUrl(r.ticker, r.exchange)} target="_blank" rel="noopener noreferrer"
                        className="text-accent-400 hover:underline" title="Open the GuruFocus page">{r.ticker} ↗</a>
                    : '—'}
                </Ident>
              </td>
              {hasCap && (
                <td className="px-3 py-1.5 text-right font-mono text-fg-muted whitespace-nowrap">
                  <span className="inline-block w-14 text-right tabular-nums">{capBn(r.market_cap_eur)}</span>
                </td>
              )}
              <td className="px-3 py-1.5 font-mono text-[12px] text-fg-subtle whitespace-nowrap">
                <Ident w="w-9">{r.currency ?? '—'}</Ident>
              </td>
              {/* ⚠ ONLY ON A ROW THAT HAS THE LINES. An `unsubscribed` / `no_data` row spans the
                  period columns with a single line of text, so labelling lines it does not have
                  would make it taller than the answer it carries. The cell still renders, because
                  a skipped `<td>` would shift every period column left on that row. */}
              {/* ⚠⚠ THE CURRENCY IS ONLY SHOWN WHERE THE NUMBER IS ACTUALLY MONEY, which is what
                  makes it worth showing at all. Three tests, and each excludes a real case here:
                    * the UNIT — `percent` (ROIC) and `shares` are not currency amounts, and a
                      share count is a plain number in millions exactly like the money lines;
                    * the VIEW — only `Reported` is in the company's own currency. `Rebased` is an
                      INDEX (100 at its own first period) and `YoY` is a percentage; putting "USD"
                      against either would relabel a ratio as money, which is the same class of
                      error as the `… %` line the backend's `_ITEMS` bans outright;
                    * the ROW — each company reports in its OWN currency, so this belongs per row
                      and could never be a column heading.
                  The cap is always EUR by construction (`period_caps_eur` converts at each
                  period's own end date), so it is stated flatly rather than conditionally. */}
              <td className="px-3 py-1.5 whitespace-nowrap align-top">
                {r.status !== 'unsubscribed' && r.status !== 'no_data' && (
                  <span className="block">
                    <span className="block text-[12px] text-fg-subtle">
                      {firstLineLabel}
                      {valueIsCurrency && view === 'reported' && r.currency
                        ? ` (${r.currency})` : ''}
                    </span>
                    {hasPeriodCap && (
                      <span className="block text-[11px] leading-tight text-fg-dim">cap (EUR)</span>
                    )}
                    <span className="block text-[11px] leading-tight text-fg-faint">weight</span>
                  </span>
                )}
              </td>
              {(
                /* ⚠⚠ EVERY PERIOD CELL ANSWERS FOR ITSELF — there are no row-spanning states any
                   more. `Unsubscribed` and `no data ingested` used to be ONE cell stretched across
                   every column, which said the fact once but cost the reader the ability to read
                   any single period: a row was either all numbers or all prose. Each cell is now
                   exactly one of four things (see `cellState`), and a row can legitimately be a
                   mixture — figures up to our last fetch, then dashes for the periods nobody has
                   asked about yet. The ACTION moved with it: the `Fetch financials` button lives in
                   the Refresh column, so the state and the thing you do about it are separated. */
                data.years.map((y) => {
                  // ⚠ THE WEIGHT SITS UNDER THE VALUE IT WEIGHTS, not in a column of its own,
                  // because the two are only meaningful as a pair: the line is Σ(weight × value)
                  // and reading a company's contribution means multiplying two numbers that have
                  // to be adjacent. It is the second line rather than the first because the value
                  // is what the column is named after and what sorting ranks on.
                  const w = weightAt(r, y);
                  const cap = capAt(r, y);
                  const state = cellState(r, y);
                  // The period this cell's figure was actually reported in, when that is not this
                  // one — see `cellOf`. Dims the number and names the source in the tooltip.
                  const src = sourceOf(r, y);
                  const carried = src != null && src !== y ? src : null;
                  return (
                    // ⚠ THE REASON MUST BE THE ACTUAL ONE. This tooltip used to assert "no cap
                    // filed for it" for every empty weight — true for an index constituent missing
                    // a period cap, and flatly WRONG for a row the rebase excluded (AMD on
                    // FCF/share), which is the more common case on a book. A confident wrong
                    // explanation is worse than none: it sends the reader to fix a cap that was
                    // never the problem.
                    <td key={y} className={`px-3 py-1.5 text-right font-mono whitespace-nowrap ${
                      state === 'unsubscribed' ? 'text-warn-300'
                        : state === 'not_tried' ? 'text-fg-faint'
                          // ⚠ A CARRIED FIGURE IS DIMMED — it is in the average and it is not this
                          // period's news. Same ink as a real figure would say the company reported
                          // twice; a blank would deny the number its own weight column divides.
                          : carried ? 'text-fg-faint italic' : 'text-fg-soft'}`}
                      // ⚠ NO NATIVE TITLE ON A STATE CELL — its `InfoTip` owns the explanation and
                      // two tooltips for one cell is how the first version came to show a slow,
                      // different sentence over the fast one. The `value` cells keep theirs: they
                      // explain the WEIGHT, not the state, and there is no badge to hang a tip on.
                      title={
                        state !== 'value' ? undefined
                          : (carried
                            ? `Carried from ${carried}: this company did not report ${y}, so the `
                              + 'line holds its latest figure until it reports again. '
                            : '')
                          + `${w == null
                                ? (blend.excluded.has(r)
                                  ? `Not in the ${metricLabel} line at all: ${blend.excluded.get(r)}`
                                  : 'No market cap filed for this period, so it is out of both the '
                                    + 'numerator and the denominator of this period’s average.')
                                : `${w.toFixed(2)}% of this period’s line`
                                  + (cap != null ? ` — cap €${(cap / 1e9).toFixed(1)}bn as at this `
                                    + 'period ÷ the Σ cap on the total row' : '')}`
                                + (view === 'reported' ? '' : ` · ${fmt(r.revenue[y])} as reported`)
                                /**
                                 * ⚠ THE MULTIPLICATION, SPELLED OUT — `share × growth = pp`, exact,
                                 * so the impact ranking can be checked on any row rather than
                                 * trusted. `sharePct` and not the weight on the line below it: that
                                 * one is over the whole period, this one over the members that span
                                 * the interval, and only the second reaches the pp. They differ
                                 * only when `spanPct` < 100, which is the case the clause names.
                                 */
                                + (() => {
                                  const c = view === 'contrib'
                                    ? blend.contrib.get(r)?.[y] : null;
                                  if (!c) {
                                    // ⚠ TWO DIFFERENT DASHES, AND THEY ARE NOT THE SAME FACT. A
                                    // whole column of them is "there is no move here to divide up"
                                    // (the line's first drawn period, or one the coverage floor
                                    // omits); a single one is about THIS row.
                                    if (view !== 'contrib') return '';
                                    return blend.step[y]
                                      ? ' · Not in this period’s move: it needs a figure at BOTH '
                                        + 'ends of the interval (and a base big enough to divide '
                                        + 'by), so it sits this one out and rejoins at the next.'
                                      : ' · No move to divide up in this column — it is the first '
                                        + 'period the line is drawn at, or one the coverage floor '
                                        + 'keeps off the chart, so nothing is measured from it.';
                                  }
                                  const st = blend.step[y];
                                  return ` · ${c.sharePct.toFixed(2)}% × `
                                    + `${c.growthPct >= 0 ? '+' : ''}${c.growthPct.toFixed(1)}% = `
                                    + `${c.pp >= 0 ? '+' : ''}${c.pp.toFixed(2)}pp of the line’s `
                                    + `${st ? `${st.from} → ${y} move `
                                      + `(${st.growthPct >= 0 ? '+' : ''}${st.growthPct.toFixed(1)}%)`
                                      : 'move'}`
                                    + (st && st.spanPct < 99.95
                                      ? ` — over the ${st.spanPct.toFixed(1)}% of this period’s `
                                        + 'weight that spans the interval, which is why this share '
                                        + 'is not the weight on the line below'
                                      : '');
                                })()}>
                      {/* ⚠ THE SAME BADGES AS THE /asset-pipeline GRID, from `StateBadge`. The two
                          tables answer the same question about the same instruments, so the same
                          word has to look the same in both — `UNSUBSCRIBED` there and here is one
                          fact, and two hand-rolled spans is how it becomes two.
                          ⚠ THE BARE DASH IS RESERVED FOR "NOT TRIED": the only state that says
                          nothing about the company gets the mark that says nothing, and it is
                          deliberately NOT a badge — a badge is an answer, and this one is the
                          absence of one. */}
                      {/* ⚠ `InfoTip`, NOT `title=`. The native tooltip sits for a second or two
                          before appearing and the delay is not configurable — long enough for a
                          reader to conclude the badge means nothing and move on. `className="block"`
                          makes the whole line the trigger rather than the 10px chip, which is the
                          other half of why the old one felt broken: you had to aim at it.
                          ⚠ AND NO `title` ON THE BADGE HERE, or both tooltips fire. */}
                      <span className="block">{
                        state === 'value'
                          // ⚠ THE ⓘ ONLY WHERE THERE IS ARITHMETIC TO SHOW — this column, this row,
                          // and only when the company actually has a trailing year. Every other
                          // cell is a filed figure with nothing underneath it, and an icon there
                          // would promise a derivation that does not exist.
                          ? (y === 'LTM' && r.ltm_parts?.length
                            ? (
                              <span className="inline-flex items-center gap-1">
                                <Cell>{cellText(cellOf(r, y))}</Cell>
                                <InfoTip content={<LtmTip parts={r.ltm_parts} rule={r.ltm_rule}
                                  reported={r.revenue.LTM ?? null} fmt={fmt} />} />
                              </span>
                            )
                            : <Cell>{cellText(cellOf(r, y))}</Cell>)
                          : (
                            // ⚠ `cursor-default` — the badge already reads as a state, so the help
                            // cursor adds nothing but a question mark dragged across the table.
                            // Naming a cursor drops `InfoTip`'s default; see its `className` note.
                            <InfoTip text={stateTitle(r, state, y)} className="block cursor-default">
                              {state === 'unsubscribed'
                                ? <StateBadge label="Unsubscribed" tone={BADGE_TONE.warn} />
                                : state === 'no_data'
                                  ? <StateBadge label="No data" tone={BADGE_TONE.warnSoft} />
                                  /* ⚠⚠ A FORECAST CELL NEVER RENDERS A BARE DASH. After pressing
                                     Refresh on a row, a dash is unreadable: it cannot say whether
                                     the request landed, whether the year is covered, or whether
                                     the company simply filed it already. Each of those is a
                                     different next step, so each gets a word. The dash stays for
                                     `not_tried` alone — the one state that really is "we have not
                                     asked", and the only one where saying nothing is honest. */
                                  /* ⚠ "Already filed", NOT "Reported" — measured on a reader, which
                                     is the only test a label has. In a column headed `2026e` the
                                     word "Reported" reads as a statement about the ESTIMATE ("the
                                     estimate has been reported"), which is the opposite of what it
                                     means. The badge marks a row for which this column does not
                                     apply, because that year is already history for it. */
                                  : state === 'reported_actual'
                                    ? <StateBadge label="Already filed" tone={BADGE_TONE.warnSoft} />
                                    : state === 'not_covered'
                                      ? <StateBadge label="No estimate" tone={BADGE_TONE.warnSoft} />
                                      : <Cell>—</Cell>}
                            </InfoTip>
                          )
                      }</span>
                      {/* ⚠ THE CAP THAT PERIOD, NOT TODAY'S — the numerator of the percentage
                          under it, so the division can be checked against the total row rather
                          than trusted. Only on an index; a book's holding weight has no market
                          cap behind it and the row stays two lines. Dimmer than the weight
                          because it is the input and the weight is the answer. */}
                      {hasPeriodCap && (
                        <span className="block text-[11px] leading-tight text-fg-dim">
                          <Cell>{cap == null || w == null ? NBSP : capBn(cap)}</Cell>
                        </span>
                      )}
                      {/* ⚠ ` `, NOT `''` AND NOT A PLAIN `' '`. A block box whose only content
                          is COLLAPSIBLE whitespace generates no line box and is zero pixels tall,
                          so a company with a gap year would get a one-line cell among two-line
                          ones — and a `<td>` centres its content, so every figure in that row
                          would sit half a line off from its neighbours. A no-break space is not
                          collapsible.

                          ⚠ IT IS A LITERAL U+00A0 IN THIS FILE AND IT IS INVISIBLE HERE. Written
                          as `' '` it reads back as the character, so there is nothing on
                          screen to distinguish it from the plain space it must not be. If a bulk
                          rewrite of this file ever normalises whitespace, this is the byte that
                          quietly breaks — the symptom is figures sitting half a line off their
                          neighbours in any row with a gap year, which reads as a CSS problem. */}
                      <span className="block text-[11px] leading-tight text-fg-faint">
                        <Cell>{w == null ? ' ' : `${w.toFixed(2)}%`}</Cell>
                      </span>
                    </td>
                  );
                })
              )}
            </tr>
            );
            rowCache.set(vi.index, el);
            return el;
          })}
          {padBottom > 0 && <tr aria-hidden><td colSpan={colCount} style={{ height: padBottom }} /></tr>}
        </tbody>
        {/* ⚠ PINNED TO THE BOTTOM OF THE BOX. In `rebased` this row IS the plotted line, so it is
            the one thing a reader checks every other row against — scrolling it out of view is
            what made the old unbounded table hard to use on an index. */}
        <tfoot className="sticky bottom-0 z-20">
          {/* Sum of the shown companies' weights — under 100% because cash / bonds / any holding
              we can't price aren't listed. */}
          <tr className="border-t border-neutral-800/40 bg-page font-semibold text-fg-strong">
            {/* ⚠ PINNED AND EMPTY, NOT ABSENT. The totals row is not a row of the list, so it has
                no number — but the cell has to exist and has to be `sticky left-0` like the ones
                above it, or the label beside it slides over the numbers as you scroll right. */}
            <td className={`px-2 py-1.5 sticky left-0 bg-page z-10`} style={numCell} />
            <td className={`px-3 py-1.5 sticky bg-page z-10 ${nameCol}`} style={nameStick}
              title={view === 'rebased'
                ? `Weighted average of the ${blend.contributors} contributing rows — this row IS the plotted line.`
                : view === 'yoy'
                  ? 'The plotted line’s own period-on-period change. NOT the average of the column above: the chart averages rebased levels, never growth rates.'
                  /* ⚠⚠ THE ONE VIEW WHERE THE FOOTER IS THE **SUM** OF THE COLUMN, and saying so is
                     the whole guarantee: every pp above is a share of THIS move, taken over the same
                     denominator, so they add to it exactly. If a column ever does not add up here,
                     the decomposition is wrong — not the display.
                     ⚠ AND IT IS THE MOVE FROM THE LAST **DRAWN** PERIOD, which is the previous
                     column only while every column is drawn. Under the coverage floor the interval
                     is longer, so it is named per cell rather than assumed to be one period. */
                  : view === 'contrib'
                    ? 'The plotted line’s own move, in percentage points — and the sum of the pp '
                      + 'column above it, exactly. Each cell names the interval it spans, which is '
                      + 'the move from the last period the chart DRAWS (not necessarily the '
                      + 'previous column).'
                    : undefined}>
              {view === 'rebased' ? 'Weighted (= the line)'
                : view === 'yoy' ? 'Line YoY'
                  : view === 'contrib' ? 'Line move (= Σ pp)' : 'Total'}
            </td>
            {/* ⚠ THE FOOTER TRACKS THE HEADER COLUMN FOR COLUMN. A cell short here and every period
                figure in this row sits under the wrong year — a totals line that is quietly one
                column out is worse than no totals line. (`colCount` guards the virtualiser's
                spacers for the same reason.) */}
            {onRefresh && <td className="px-2 py-1.5 sticky bg-page z-10" style={refreshStick} />}
            <td className="px-3 py-1.5" />
            <td className="px-3 py-1.5" />
            {/* Σ of the Mkt cap column — TODAY's caps, matching the column above it. ⚠ It is NOT
                the denominator of any weight on this table any more: those divide by the `Σ cap`
                line inside each period, which is that period's own total and a different number
                in every column. */}
            {hasCap && (
              <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap"
                title="Σ of the Mkt cap column — today's caps. The weights inside the period cells
divide by that period's own Σ cap instead, on the line directly below this row's figure.">
                {capBn(rows.reduce((a, r) => a + (r.market_cap_eur ?? 0), 0))}
              </td>
            )}
            <td className="px-3 py-1.5" />
            {/* The line-label column, for the total row — same order and the same dim inks as the
                per-company labels above, so the whole column reads as one list of names.

                ⚠⚠ THE THIRD LINE MEANS SOMETHING DIFFERENT HERE, AND THE LABEL IS WHAT MAKES THAT
                SAFE. A company's third line is its WEIGHT; this row's is COVERAGE — deliberately
                not the sum of the weights above, which is 100% by construction and would tell a
                reader nothing. Scanning down a period column you therefore pass a run of weights
                and land on a percentage that is not their total. Unlabelled that is a trap; named
                `weight` on every row and `covered` on this one, they are two quantities that
                simply share a line.

                The first line carries the metric's own name, exactly as every row above does — it
                IS that line, weight-averaged. What KIND of average is on the sticky cell at the
                far left (`Weighted (= the line)`), which stays in view when this column does
                not. */}
            {/* ⚠ THE TOTAL ROW CARRIES NO CURRENCY ON ITS FIRST LINE, AND THE ASYMMETRY IS THE
                POINT. Every row above says e.g. `Revenue (USD)` in Reported view; this one cannot,
                because there is nothing here to put a currency on — Reported has no total at all
                (the columns are different currencies, which is exactly why the chart rebases), and
                Rebased and YoY are an index and a percentage. A reader noticing the missing "(USD)"
                has noticed the real reason the blend works the way it does. */}
            <td className="px-3 py-1.5 whitespace-nowrap align-top font-normal">
              <span className="block text-[12px] text-fg-subtle">{firstLineLabel}</span>
              {hasPeriodCap && (
                <span className="block text-[11px] leading-tight text-fg-dim">Σ cap (EUR)</span>
              )}
              <span className="block text-[11px] leading-tight text-fg-faint">covered</span>
            </td>
            {/* ⚠ THE ROW THAT MAKES THE TABLE CHECKABLE — and it is only a sum in one of the three
                views. Reported: nothing to total, the columns are different currencies. Rebased:
                the weighted average IS the plotted line. YoY: the plotted line's own change, NOT
                the average of the column above it (the chart averages levels, never growth rates)
                — labelling it "average YoY" would present the chain-linked construction as if it
                were the chart's. A period under the coverage floor is greyed and says so: it is in
                this table and absent from the chart. */}
            {data.years.map((y) => {
              const lv = blend.level[y];
              const prevY = data.years[data.years.indexOf(y) - 1];
              const prev = prevY ? blend.level[prevY] : undefined;
              /**
               * ⚠ THE pp TOTAL IS READ FROM `blend.step`, NOT FROM THE RATIO OF TWO DRAWN LEVELS.
               * The two agree whenever the previous column is drawn — and where it is not, the
               * ratio has nothing to divide by while the STEP still exists (the chain measures from
               * the last drawn period). Deriving it here would leave the one column whose cells sum
               * to the footer showing a blank footer for a move its own cells decompose.
               */
              const value = view === 'reported' ? null
                : view === 'rebased' ? lv?.value ?? null
                  : view === 'contrib' ? blend.step[y]?.growthPct ?? null
                    : (lv && prev && prev.value > 0) ? 100 * (lv.value / prev.value - 1) : null;
              // ⚠ BOTH FLOORS, as the chart applies them: half the weight AND half the names must
              // have REPORTED. Weight alone let one giant draw a period (AEX 2026-Q2: two
              // constituents, 53.8% of cap); names alone would let ten tiny ones outvote a missing
              // giant. A carried figure counts toward neither.
              const covN = blend.coveredNames[y] ?? 0;
              const thin = lv != null
                && (lv.covered < MIN_YEAR_COVERAGE_PCT || covN < MIN_YEAR_COVERAGE_PCT);
              return (
                <td key={y}
                  className={`px-3 py-1.5 text-right font-mono whitespace-nowrap ${
                    thin ? 'text-fg-faint font-normal' : ''}`}
                  title={lv == null ? undefined
                    : `${lv.covered.toFixed(1)}% of the contributing weight and ${covN.toFixed(0)}% `
                      + 'of the companies reported this period'
                      + (thin ? ` — under the ${MIN_YEAR_COVERAGE_PCT}% floor, so the chart omits it`
                        : '')}>
                  <span className="block">
                    <Cell>
                      {value == null ? ' ' : view === 'rebased' ? value.toFixed(1)
                        // ⚠ 2dp AND `pp`, MATCHING THE CELLS IT IS THE SUM OF. At 1dp a column of
                        // hundredths visibly fails to add up to its own total, which reads as the
                        // decomposition being broken rather than as rounding.
                        : view === 'contrib' ? `${value >= 0 ? '+' : ''}${value.toFixed(2)}pp`
                          : `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`}
                    </Cell>
                  </span>
                  {/* ⚠⚠ THE DENOMINATOR, SPELLED OUT — Σ of the caps this period's weights were
                      each divided by. Every weight in the column above is that row's cap ÷ this
                      number, so the table is checkable rather than asserted. It also makes the
                      per-period basis visible at a glance: this figure GROWS down the years
                      because the index was worth less in 2015, which is exactly the fact that
                      weighting by today's cap threw away. */}
                  {hasPeriodCap && (
                    <span className="block text-[11px] leading-tight text-fg-dim">
                      <Cell>{blend.denom[y] == null ? NBSP : capBn(blend.denom[y])}</Cell>
                    </span>
                  )}
                  {/* ⚠ COVERAGE, PROMOTED OUT OF THE TOOLTIP — it is what the weights above are
                      shares OF. The column of weights sums to 100% within a period by
                      construction; this says what share of the index that 100% actually is. A
                      period under the floor is greyed with the rest of the cell. */}
                  <span className="block text-[11px] leading-tight text-fg-faint">
                    <Cell>{blend.weightSum[y] == null ? ' ' : `${blend.weightSum[y].toFixed(2)}%`}</Cell>
                  </span>
                </td>
              );
            })}
          </tr>
        </tfoot>
      </table>
    </div>
  );
}

export default function HoldingsRevenueModal({
  target, metric = 'revenue', unit = 'millions', noun = 'revenue', portfolioName, onClose,
  seriesLabel, benchLabel, benchTarget,
}: {
  target: Target;
  metric?: string;
  unit?: 'millions' | 'per_share' | 'percent' | 'shares';
  noun?: string;
  portfolioName?: string | null;
  onClose: () => void;
  /** Names the metric in the modal title — the card's own column heading. */
  seriesLabel?: string;
  benchLabel?: string | null;
  /** Set when a benchmark is active — lets the modal load the INDEX's constituents on demand. */
  benchTarget?: BenchTarget | null;
}) {
  /** Is this metric MONEY? ⚠ Declared once, from the unit, and read by both tables — the rule that
   *  `shares` is a plain count and `percent` is already a ratio is easy to state and easy to get
   *  backwards, and two copies of it is how one table comes to label a share count "(USD)". Same
   *  two members as the backend's `_CURRENCY_UNITS`. */
  const valueIsCurrency = unit === 'millions' || unit === 'per_share';
  // millions/shares → compact B/T/M; per_share → a plain per-share figure; percent → a % ratio.
  const fmtM = (v: number | null | undefined) => {
    if (v == null) return '—';
    if (unit === 'percent') return `${v.toFixed(1)}%`;
    if (unit === 'per_share') return v.toFixed(2);
    const a = Math.abs(v);
    if (a >= 1e6) return `${(v / 1e6).toFixed(2)}T`;
    if (a >= 1e3) return `${(v / 1e3).toFixed(1)}B`;
    return `${v.toFixed(0)}M`;
  };
  const [data, setData] = useState<Resp | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [reloadKey, setReloadKey] = useState(0);
  /**
   * What a "Refresh fundamentals" press here would fill — DERIVED FROM THE TABLE IT SITS OVER, not
   * from how the modal was opened.
   *
   * ⚠ A CONTROL'S SCOPE MUST MATCH ITS SCREEN. This modal is opened from ONE card, over one
   * portfolio (or one company, or an index) — a button that quietly refetched something else would
   * spend quota on rows the reader cannot see. The three shapes map straight onto `Target`'s three:
   * a stored model, an ad-hoc basket, and — on the single-company cards, which send a basket of one
   * — that company. See `RefreshScope`.
   */
  const scope: RefreshScope | null = useMemo(() => {
    if (target.portfolio_id != null) {
      return { kind: 'portfolio', id: target.portfolio_id, name: portfolioName || 'this portfolio' };
    }
    if (target.universe) {
      return { kind: 'universe', label: target.universe, name: target.universe };
    }
    const hs = target.holdings ?? [];
    if (!hs.length) return null;
    if (hs.length === 1) {
      return { kind: 'company', isin: hs[0].isin, name: hs[0].name || portfolioName || hs[0].isin };
    }
    return { kind: 'basket', holdings: hs.map((h) => ({ isin: h.isin })),
      name: portfolioName || 'this portfolio' };
  }, [target, portfolioName]);
  /**
   * ⚠ ONE SWITCH FOR BOTH TABLES. The book above and the index below are here to be read against
   * each other; per-table switches would let someone compare a rebased index with reported euros
   * and take the gap for a finding. It is a view over rows already in hand — no refetch — so
   * flipping it cannot land the two tables on different vintages of the same accounts either.
   */
  const [view, setView] = useState<View>('reported');

  /**

  /**
   * ⚠ A ZERO ON A MONEY LINE IS A PLACEHOLDER, NOT A MEASUREMENT — AND IT READS AS A FACT.
   *
   * GuruFocus returns a company's whole history as one rectangular block, so a period that predates
   * the company's separate accounts comes back as `0` rather than being left out. Universal Music
   * is the case: it was inside Vivendi until the September 2021 spin-off, its carve-out accounts
   * begin at 2018 (6,023), and 2017 arrives as `0`. Rendered as "0M" that is a claim — a €48bn
   * business earning nothing — sitting in the same column, same font, as nine real filings.
   *
   * The chart has ignored these all along (`_prepare` rebases a level series on its first POSITIVE
   * period, which is what put UMG back in the AEX line), so blanking them here does not change a
   * number — it makes the table agree with the line it is supposed to explain. Downstream this is
   * one edit rather than four: `cellState`, the carry, the rebased view and the sort all read
   * `revenue`, so a gap declared once is a gap everywhere.
   *
   * ⚠ MONEY ONLY, AND EXACTLY ZERO. A margin of `0%` is a real reading and a share count is a plain
   * count; blanking those would hide measurements instead of placeholders. Negative figures are
   * untouched — a loss is a fact, and the whole point is to stop inventing ones.
   */
  const zeroesAreGaps = (resp: Resp): Resp => {
    if (!valueIsCurrency) return resp;
    return {
      ...resp,
      rows: resp.rows.map((row) => {
        if (!Object.values(row.revenue).some((v) => v === 0)) return row;
        const revenue: Record<string, number | null> = {};
        for (const [p, v] of Object.entries(row.revenue)) revenue[p] = v === 0 ? null : v;
        return { ...row, revenue };
      }),
    };
  };

  const load = async (body: Target, onlyCompanyId?: number): Promise<Resp> => {
    const r = await apiFetch(`${API_URL}/api/earnings/portfolio-revenue-matrix`
      + `?metric=${encodeURIComponent(metric)}`
      + (onlyCompanyId != null ? `&only_company_id=${onlyCompanyId}` : ''), {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    return zeroesAreGaps(b as Resp);
  };

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const b = await load(target);
        if (alive) setData(b);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target, metric, reloadKey]);

  /**
   * The index's constituents — every one, with its weight and its reported figures, so the line
   * can be checked by hand.
   *
   * ⚠ IT LOADS WITH THE MODAL, NOT BEHIND A BUTTON, AND THAT ONLY BECAME REASONABLE ONCE THE READ
   * WAS FIXED. It used to be one metric read per company: the S&P's 489 constituents took **64.5
   * s**, which is why it was gated. Prefetched it is one chunked, paged query — **0.19 s**
   * measured — so hiding the table now costs a click and buys nothing.
   */
  const [bench, setBench] = useState<Resp | null>(null);
  const [benchErr, setBenchErr] = useState<string | null>(null);
  /** ⚠ ITS OWN RELOAD KEY, NOT `reloadKey`. Filling the index moves the table below; filling the
   *  book moves the one above. One key would make each button re-read both tables — twice the wait
   *  for half a reason, and on ACWI the constituent read is the expensive one. */
  const [benchReload, setBenchReload] = useState(0);
  const benchKey = benchTarget ? `${benchTarget.label}|${benchTarget.cadence}` : '';
  useEffect(() => {
    let alive = true;
    void (async () => {
      setBench(null); setBenchErr(null);
      if (!benchTarget) return;
      try {
        const b = await load(benchTarget);
        if (alive) setBench(b);
      } catch (e) {
        console.warn('[bb:bench] constituent matrix:', e);
        if (alive) setBenchErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [benchKey, metric, benchReload]);

  /**


  /**
   * Re-fetch ONE row's company from GuruFocus as a cancellable job, then reload that row's table.
   *
   * ⚠ A JOB, SO PROGRESS AND CANCEL BELONG TO THE TOAST STACK — the same generic system the two
   * bulk buttons above use. It outlives this modal, which matters: three feeds against GuruFocus
   * is not instant, and a reader who closes the drill-down has not cancelled anything.
   *
   * ⚠⚠ `feeds=smart`: EVERY FEED, BUT ONLY WHERE A CALL CAN BUY SOMETHING. Per feed it fetches
   * what we are MISSING or what can plausibly have changed — statements when a new fiscal period
   * is due (`period_due`), the consensus and the weekly forward-P/E series when our copy is over a
   * week old. A company with nothing new costs nothing; one that has just filed is picked up.
   *
   * ⚠ THE TWO OBVIOUS SETTINGS ARE BOTH WRONG HERE. `all` spends three calls every press on data
   * we may already hold; an un-forced run tests PRESENCE (`needs()`), which is a no-op on exactly
   * the company a reader pressed Refresh for — KLA holds financials, so presence skips it and the
   * FY2026 figures it just filed are never fetched. See `smart_flags`.
   *
   * ⚠ AND AN UNSUBSCRIBED EXCHANGE COSTS NOTHING EITHER, already: `eligible()` refuses the
   * company before any feed is considered, so no probe and no call is spent on a venue GuruFocus
   * cannot answer for.
   *
   * It used to be `statements` — one call, deliberately.
   * That narrowing was correct and measured (DSM-Firmenich, 2026-08-12: the row refresh was
   * spending 3 calls where 1 sufficed) on the argument that "analyst estimates and indicators
   * contribute NOTHING to that table". They do now: the `2026e…` columns to the right of LTM ARE
   * the estimates feed. Left narrowed, this button ran, succeeded, and left the very cells a reader
   * pressed it for still empty — a refresh that cannot fill a visible column is a broken
   * affordance, and the cost is two calls on ONE deliberately-pressed row.
   *
   * ⚠ THE BULK PATHS STAY ON `statements`, and the distinction is the whole point:
   * `FundamentalGridPane` and `BenchmarksPanel` fill hundreds to ~1,700 constituents at a time, so
   * tripling them is a quota event, not a rounding error — and the grid those two draw genuinely
   * does not show estimates. One row, pressed on purpose, is the only place the extra calls buy
   * something the reader asked for.
   *
   * ⚠ `force` is what makes it able to change a row that already has figures (see `onRefresh`).
   *
   * ⚠ AND THE READ CACHES ARE DROPPED WHEN IT LANDS, not when it started. `apiFetch` invalidates on
   * the request that STARTS a job, which is minutes before the data moves; without this the reload
   * below would be served from entries cached during the fetch and the row would come back exactly
   * as it was — a refresh that visibly does nothing. Same rule as `PortfolioFundamentalsRefresh`.
   */
  /**
   * Replace ONE row in a loaded table, keeping everything else — including the columns.
   *
   * ⚠⚠ THE NARROWED RESPONSE IS NOT A TABLE. Its `years` describe the one company, so taking
   * them would collapse a twelve-column table to whatever that constituent reports. The columns
   * belong to the union and are already on screen; only the row is news.
   *
   * ⚠ MATCHED ON `company_id`, NOT ON THE ARRAY INDEX. The table is sorted by whatever the
   * reader last clicked, and a job that lands after a re-sort would otherwise overwrite a
   * different company with these figures — silently, since every row looks alike in shape.
   */
  const patchRow = (prev: Resp | null, fresh: Resp, companyId: number): Resp | null => {
    if (!prev) return prev;
    const one = fresh.rows.find((x) => x.company_id === companyId);
    if (!one) return prev;
    return { ...prev, rows: prev.rows.map((x) => (x.company_id === companyId ? one : x)) };
  };

  const refreshRow = (
    target_: Target,
    apply: (patch: (p: Resp | null) => Resp | null) => void,
  ) => async (row: Row) => {
    const started = await startJob(
      `${API_URL}/api/benchmarks/company/${row.company_id}/fundamentals/ingest/job`
      + '?feeds=smart',
      `${row.name} fundamentals`);
    void started.done.then(async (job) => {
      if (job.status === 'failed' || row.company_id == null) return;
      // ⚠ THE CACHE DROP STAYS, AND IT STAYS FIRST. `apiFetch` invalidates on the request that
      // STARTS a job, which is minutes before the data moves; without this the re-read below is
      // served from entries cached during the fetch and the row comes back exactly as it was.
      invalidateReadCache(`fundamentals refetched for ${row.name}`);
      try {
        // ⚠⚠ ONE ROW, NOT THE TABLE. This used to bump a reload key, which re-POSTed the whole
        // matrix — on ACWI every constituent's series, every period cap and every LTM window, the
        // most expensive read on the tab, to update a line already on screen. Exactly one company's
        // `metric_data` changed; `only_company_id` reads exactly that.
        const fresh = await load(target_, row.company_id);
        apply((p) => patchRow(p, fresh, row.company_id as number));
      } catch (e) {
        // ⚠ A FAILED PATCH LEAVES THE OLD ROW, WHICH IS STALE BUT TRUE. The fetch succeeded, so the
        // data IS newer than what is shown; saying so beats silently implying the table is current.
        console.warn('[bb:matrix] row patch failed — the table still shows the pre-refresh figures'
          + ' for this company; reopen the drill-down to pick them up', e);
      }
    });
    return started;
  };

  const section = 'text-[12px] uppercase tracking-wide text-fg-muted';
  /**
   * ⚠ A ONE-ROW MATRIX IS THE PLOTTED TABLE AGAIN. On a single company the line IS that company's
   * reported figures, so the "as reported" table repeats every number above it and adds a 100.00%
   * weight column and a Total row over one row — noise that makes the modal look like it holds two
   * findings when it holds one. It stays the moment there is a second row to compare against.
   */
  /** A one-row table: the modal titles itself with that company's listing instead of
   *  "1 companies". */
  const only = (data?.rows.length ?? 0) === 1 ? data!.rows[0] : null;

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[88vw] h-[84vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">{seriesLabel ?? noun} — everything behind the chart</h2>
          {portfolioName && <span className="text-sm text-fg-soft truncate max-w-[24ch]" title={portfolioName}>{portfolioName}</span>}
          {/* ⚠ THE PROVENANCE SURVIVES THE TABLE IT LIVED IN. Dropping the one-row matrix would
              otherwise take the GuruFocus listing + reporting currency with it, and those are how
              a reader checks the figures against the source. */}
          {only ? (
            <span className="text-[12px] text-fg-faint font-mono">
              {only.exchange ?? '—'}
              {only.ticker && <>
                {' '}
                <a href={guruFocusUrl(only.ticker, only.exchange)} target="_blank" rel="noopener noreferrer"
                  className="text-accent-400 hover:underline" title="Open the GuruFocus page">{only.ticker} ↗</a>
              </>}
              {only.currency && ` · ${only.currency}`}
            </span>
          ) : data && <span className="text-[12px] text-fg-faint">{data.rows.length} companies</span>}
          {benchLabel && <span className="text-[12px]" style={{ color: chartTheme.pos }}>vs {benchLabel}</span>}
          <button type="button" onClick={onClose} className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-5">
          {/* 0 — the switch, above everything it governs. */}
          <div className="flex items-center gap-2 text-[11px]">
            <div className="inline-flex rounded-lg border border-neutral-700 overflow-hidden">
              {VIEWS.map(([k, label, note]) => (
                <button key={k} type="button" onClick={() => setView(k)} title={note}
                  aria-pressed={view === k}
                  className={`cursor-pointer px-2.5 py-0.5 font-medium transition-colors ${
                    view === k ? 'bg-accent-600 text-white' : 'text-fg-muted hover:bg-overlay/5'}`}>
                  {label}
                </button>
              ))}
            </div>
            <span className="text-fg-faint">
              {view === 'reported' ? 'as filed, each in its own currency'
                : view === 'rebased' ? 'indexed to 100 at each company’s first period — what the chart weights'
                  : view === 'contrib'
                      ? 'percentage points of the line’s own move — sums to the footer, so sorting a '
                        + 'column ranks impact'
                      : 'growth on that company’s previous reported period'}
              {view !== 'reported' && ' · hover a cell for the reported figure'}
            </span>
          </div>


          {/* 1 — the book (or the single company), on the same three views as the index below it. */}
          <div className="space-y-1.5">
            {/* ⚠ THE FILL SITS ON THE TABLE IT FILLS, and there are two of them. A `no_data` row
                here has a per-row Fetch already; this is the same action over every company at
                once, which is what you want when the table is half empty rather than missing one
                name. The index has its own button below — same component, different endpoint and a
                deliberately different spend (see `RefreshScope`). */}
            <div className="flex items-baseline gap-3">
              <h3 className={section}>
                {portfolioName ? `${portfolioName} — ` : ''}{noun} by period
              </h3>
              {scope && (
                <span className="ml-auto shrink-0">
                  <PortfolioFundamentalsRefresh scope={scope}
                    // ⚠ "portfolio" WOULD BE A LIE ON THE SINGLE-COMPANY CARDS, which open this
                    // modal with a basket of one and title it with that company's listing. Named
                    // for what it acts on, as its neighbour below is.
                    label={scope.kind === 'company' ? 'Refresh company' : 'Refresh portfolio'}
                    onDone={() => setReloadKey((k) => k + 1)} />
                </span>
              )}
            </div>
            {err && <p className="text-xs text-neg-300">{err}</p>}
            {!data && !err && <p className="text-xs text-fg-subtle">Loading…</p>}
            {data && data.rows.length === 0 && !err && (
              <p className="text-xs text-fg-subtle">No held company has {noun} ingested.</p>
            )}
            {data && data.rows.length > 0 && (
              <MatrixTable data={data} fmt={fmtM} noun={noun} metricLabel={seriesLabel ?? noun}
                valueIsCurrency={valueIsCurrency} view={view}
                onRefresh={refreshRow(target, setData)} />
            )}
          </div>

          {/* 3 — the same, for the index, on demand. */}
          {benchTarget && (
            <div className="space-y-1.5">
              <div className="flex items-baseline gap-3">
                <h3 className={section}>{benchLabel} constituents — {noun} by period</h3>
                {/* ⚠ SEPARATE FROM THE BOOK'S, AND IT MUST BE. The two fills are different work
                    over different companies with wildly different quota costs — ACWI is ~1,900
                    constituents against a book's twenty — so one button doing both would make the
                    cheap press unavailable. It fills only the constituents we are MISSING, which is
                    what raises this table's row count and the line's coverage with it. */}
                <span className="ml-auto shrink-0">
                  <PortfolioFundamentalsRefresh
                    /* ⚠⚠ `smart` — THE PER-ROW REFRESH, RUN ACROSS THE INDEX, AND THE NAME IS NOW
                       HONEST BECAUSE THE BEHAVIOUR IS. It fetches per constituent exactly the feeds
                       that are missing or that can plausibly have changed, so a name with nothing
                       new costs nothing — which is what makes "refresh the whole benchmark" an
                       affordable thing for a button to promise.
                       ⚠ IT IS THE SAME RULE THE ROW BUTTON USES (`smart_flags` / `smart_flags_bulk`,
                       sharing `_is_stale`), pinned that way on purpose: the moment the two diverge,
                       the big button stops being N presses of the small one and this label goes back
                       to being a claim rather than a description.
                       ⚠ MEASURED COST, ACWI 2026-08-14: ~5,701 calls on a FIRST press — nearly
                       everything has something new, so smart ≈ all until the index is current. The
                       saving is on the second press, not the first. `feeds: 'estimates'` is the
                       cheaper, narrower fill (~1,597) if only the forecast columns are wanted. */
                    scope={{ kind: 'universe', label: benchTarget.label,
                      name: benchLabel || benchTarget.label, feeds: 'smart' }}
                    label="Refresh benchmark"
                    onDone={() => setBenchReload((k) => k + 1)} />
                </span>
              </div>
              {!bench && !benchErr && (
                <p className="text-xs text-fg-subtle">Loading {benchLabel} constituents…</p>
              )}
              {benchErr && <p className="text-xs text-neg-300">{benchErr}</p>}
              {bench && (
                <>
                  {/* ⚠ EVERY CAVEAT IS STILL HERE — IT MOVED TO THE `title`, IT DID NOT GO. Each is
                      a thing a reader would otherwise assume, and each is false: the weight is
                      CURRENT full cap (not backed out to the start of the window the way the price
                      index does, and not float-adjusted or 15%-capped like the published AEX); it
                      spans every constituent while the line renormalises over the covered ones
                      period by period; and a constituent with no stored cap is not in the index at
                      all — systematically the names GuruFocus does not cover. */}
                  <p className="text-[11px] text-fg-faint">
                    {bench.rows.length} constituents ·{' '}
                    <span className="underline decoration-dotted underline-offset-2"
                      title="company.market_cap_eur as stored today. Full cap — not free-float, and not capped per constituent the way the published index is. Not backed out to the start of the window either, so a company that has since grown carries its post-growth weight over its whole history.">
                      current full market cap
                    </span>{' '}
                    · {bench.rows.filter((r) => r.status === 'ok').length} with {noun} feed the line,
                    renormalised each period
                  </p>
                  {/* ⚠ THE EXCLUDED-CONSTITUENT LINE WAS REMOVED ON REQUEST (2026-08-10). It named
                      every dropped constituent inline — `470/1998 excluded, weights renormalised:
                      BANCO ESPIRITO SANTO CLASS N SA (delisted) · …` — which is fine for the AEX's
                      three and unreadable for ACWI's 470, where it buried the table under a wall of
                      names.

                      ⚠ THE FACT IT REPORTED IS STILL TRUE AND IS NOW UNSTATED ON THIS SCREEN: a
                      constituent with no stored market cap is not in the index at ANY weight, so
                      the weights shown are renormalised over the survivors and every one of them
                      is larger than that constituent's share of the real index. Measured on ACWI,
                      that is ~22% of the published membership, and it goes a whole market at a time
                      (NSE 157, SHSE 83, LSE 72, SZSE 61) rather than at random.

                      `weight_basis` is still computed and still on the API response — nothing was
                      removed from `_benchmark_index.weight_basis` — so a compact form (a count, with
                      the names in a tooltip) can be put back without touching the backend. */}
                  <MatrixTable data={bench} fmt={fmtM} noun={noun} metricLabel={seriesLabel ?? noun}
                valueIsCurrency={valueIsCurrency} view={view}
                onRefresh={refreshRow(benchTarget!, setBench)} />
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
