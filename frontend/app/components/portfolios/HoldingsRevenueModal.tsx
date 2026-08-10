'use client';

import { useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import { MIN_YEAR_COVERAGE_PCT } from './marginData';

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

type Row = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data'; revenue: Record<string, number | null>;
  /** INDEX ROWS ONLY — the numerator the weight beside it was divided out of (cap ÷ Σcap).
   *  Absent on a portfolio, where the weight is a holding weight and no cap is involved. */
  market_cap_eur?: number | null;
};
/** Universe requests only: how the weights were arrived at, and who fell out. See the backend's
 *  `weight_basis` — the names it lists are NOT in the index at any weight. */
type WeightBasis = {
  members: number; weighted: number; excluded: { name: string | null; reason: string }[];
};
type Resp = { years: string[]; rows: Row[]; holdings: number; weight_basis?: WeightBasis };
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
type View = 'reported' | 'rebased' | 'yoy';

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
const Cell = ({ children }: { children: React.ReactNode }) => (
  <span className="inline-block w-[4.5rem] text-right tabular-nums">{children}</span>
);

const VIEWS: [View, string, string][] = [
  ['reported', 'Reported', 'The figures as filed, in each company’s own reporting currency.'],
  ['rebased', 'Rebased', 'Each company indexed to 100 at ITS OWN first period — exactly what the '
    + 'chart weight-averages. The footer row is the weighted average, i.e. the plotted line.'],
  ['yoy', 'YoY %', 'Growth from that company’s previous reported period. ⚠ The chart does NOT '
    + 'average these: it averages the Rebased levels. The footer is the plotted line’s own '
    + 'period-on-period change, not the average of the column above it.'],
];

function MatrixTable({ data, fmt, noun, view, onFetch }: {
  data: Resp;
  fmt: (v: number | null | undefined) => string;
  noun: string;
  /** ⚠ OWNED BY THE MODAL, NOT HERE — one switch drives the book's table and the index's together.
   *  Two independent switches would let a reader compare a rebased index against reported euros
   *  and read the gap as a finding. */
  view: View;
  /** Holdings only: fetch a `no_data` company's financials. Absent ⇒ the cell states the gap,
   *  which is right for an index nobody is curating row by row. */
  onFetch?: (isin: string, name: string) => Promise<void>;
}) {
  const [sort, setSort] = useState<{ key: string; dir: 'asc' | 'desc' }>({ key: 'weight', dir: 'desc' });
  const [ingest, setIngest] = useState<Record<string, { busy?: boolean; msg?: string }>>({});
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
  const blend = useMemo(() => {
    const parts: { w: number; idx: Record<string, number> }[] = [];
    // ⚠ KEYED ON THE ROW OBJECT, NOT ON THE ISIN. A payload can carry the same ISIN twice (a model
    // listing one instrument at two weights — VTopSelectie holds CapitaLand at 2% and 3%), and an
    // ISIN key would give both rows the first one's weight. `rows` below is a sort of these same
    // objects, so identity is stable for the render.
    const partOf = new Map<Row, { w: number; idx: Record<string, number> }>();
    let total = 0;
    for (const r of data.rows) {
      const periods = Object.keys(r.revenue).filter((p) => r.revenue[p] != null).sort();
      const w = r.market_cap_eur ?? r.weight_pct;
      if (!periods.length || !w) continue;
      // ⚠ COUNTED IN THE DENOMINATOR **BEFORE** THE BASE TEST, because that is the order
      // `blend_series` uses: it takes `total_w` over every member handed to it, and `_prepare`
      // drops the non-positive bases afterwards. Filtering first would shrink the denominator,
      // lift every coverage figure, and let a period slip over the floor that the chart omits.
      total += w;
      const base = r.revenue[periods[0]] as number;
      if (!(base > 0)) continue;              // matches `_prepare`'s non_positive_base drop
      const idx: Record<string, number> = {};
      for (const p of periods) idx[p] = 100 * (r.revenue[p] as number) / base;
      const part = { w, idx };
      parts.push(part);
      partOf.set(r, part);
    }
    const level: Record<string, { value: number; covered: number }> = {};
    // ⚠⚠ THE DENOMINATOR IN FORCE FOR EACH PERIOD, AND IT IS WHY A PER-YEAR WEIGHT EXISTS AT ALL.
    // The weighted average below divides by the weight that REPORTED this period, not by the
    // table's total — so a company's real share of the line moves from year to year even though
    // its cap here is a single stored number. A company absent in 2015 and present in 2020 lifts
    // everyone else's share in 2015 and dilutes it in 2020. The `Weight` column (cap ÷ Σcap) is
    // therefore not the weight used in ANY period; `weightAt` below is.
    const denom: Record<string, number> = {};
    for (const y of data.years) {
      const hit = parts.filter((p) => p.idx[y] != null);
      const w = hit.reduce((a, p) => a + p.w, 0);
      if (w > 0) {
        denom[y] = w;
        level[y] = { value: hit.reduce((a, p) => a + p.w * p.idx[y], 0) / w,
          covered: 100 * w / total };
      }
    }
    return { level, denom, partOf, contributors: parts.length };
  }, [data]);

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
  const weightAt = (r: Row, y: string): number | null => {
    const p = blend.partOf.get(r);
    const d = blend.denom[y];
    if (!p || !d || p.idx[y] == null) return null;
    return 100 * p.w / d;
  };

  /** The value a period column shows for one row, under the current view. */
  const cellOf = (r: Row, y: string): number | null => {
    const v = r.revenue[y];
    if (v == null) return null;
    if (view === 'reported') return v;
    const periods = Object.keys(r.revenue).filter((p) => r.revenue[p] != null).sort();
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

  const cellText = (v: number | null) => (
    v == null ? '—'
      : view === 'reported' ? fmt(v)
        : view === 'rebased' ? v.toFixed(1)
          : `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`);
  const toggle = (key: string) => setSort((s) => (s.key === key
    ? { key, dir: s.dir === 'desc' ? 'asc' : 'desc' }
    // Names/currency read A→Z first; weight, cap and figures read biggest-first.
    : { key, dir: (key === 'name' || key === 'ccy') ? 'asc' : 'desc' }));
  const caret = (k: string) => (sort.key === k ? (sort.dir === 'desc' ? ' ▾' : ' ▴') : '');

  const rows = useMemo(() => {
    const get = (r: Row): number | string | null | undefined => (
      sort.key === 'name' ? r.name.toLowerCase()
        : sort.key === 'exchange' ? (r.exchange ?? '')
          : sort.key === 'ticker' ? (r.ticker ?? '')
            : sort.key === 'weight' ? r.weight_pct
              : sort.key === 'cap' ? (r.market_cap_eur ?? null)
                : sort.key === 'ccy' ? (r.currency ?? '')
                  : r.revenue[sort.key]);      // a period column
    return [...data.rows].sort((a, b) => cmp(get(a), get(b), sort.dir));
  }, [data, sort]);

  const fetchOne = async (isin: string, name: string) => {
    if (!onFetch) return;
    setIngest((s) => ({ ...s, [isin]: { busy: true } }));
    try {
      await onFetch(isin, name);
      setIngest((s) => ({ ...s, [isin]: {} }));
    } catch (e) {
      // ⚠ THE REASON STAYS ON THE ROW. A fetch that loaded financials carrying no income statement
      // is a real answer; swallowing it would read as "nothing happened".
      setIngest((s) => ({ ...s, [isin]: { msg: e instanceof Error ? e.message : String(e) } }));
    }
  };

  return (
    <div className="overflow-auto rounded-lg border border-neutral-800/40">
      <table className="w-full text-xs">
        <thead className="bg-page">
          <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40 [&>th]:cursor-pointer [&>th]:select-none [&>th:hover]:text-fg-soft">
            {/* Company takes the slack so the table fills the width; periods keep natural size. */}
            <th className="px-3 py-1.5 font-medium text-left sticky left-0 bg-page z-10 w-full" onClick={() => toggle('name')}>Company{caret('name')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('exchange')}>GF exch{caret('exchange')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('ticker')}>Ticker{caret('ticker')}</th>
            {hasCap && (
              <th className="px-3 py-1.5 font-medium text-right whitespace-nowrap" onClick={() => toggle('cap')}
                title="company.market_cap_eur as stored today — full cap, not free-float. This is the numerator of the Weight beside it: cap ÷ the total of this column.">
                Mkt cap €bn{caret('cap')}
              </th>
            )}
            {/* ⚠ NOT THE WEIGHT ANY PERIOD ACTUALLY USES — that is the second line inside each
                period cell. This is the row's share of the whole table (cap ÷ Σcap), which is
                what makes the sort and the footer total meaningful; the average in any given
                period renormalises over whoever reported it, so the two differ by more the
                thinner the period. Saying so here is the difference between two weights and one
                weight that looks wrong. */}
            <th className="px-3 py-1.5 font-medium text-right whitespace-nowrap" onClick={() => toggle('weight')}
              title="Share of this table: cap ÷ the total of the Mkt cap column. ⚠ NOT the weight
used in any single period — a period renormalises over the companies that reported it, and that
figure is the small second line inside each period cell.">
              Weight{caret('weight')}
            </th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('ccy')}>Ccy{caret('ccy')}</th>
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
                  + 'Sorting still ranks on the figure, not the weight.'}>
                {y}{caret(y)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={`${r.isin}-${i}`} className="border-b border-neutral-800/20 hover:bg-overlay/[0.02]">
              <td className="px-3 py-1.5 text-fg-soft sticky left-0 bg-card z-10 max-w-0">
                <span className="block truncate" title={r.name}>{r.name}</span>
              </td>
              <td className="px-3 py-1.5 font-mono text-[11px] text-fg-subtle whitespace-nowrap">{r.exchange ?? '—'}</td>
              <td className="px-3 py-1.5 font-mono text-[11px] whitespace-nowrap">
                {r.ticker
                  ? <a href={guruFocusUrl(r.ticker, r.exchange)} target="_blank" rel="noopener noreferrer"
                      className="text-accent-400 hover:underline" title="Open the GuruFocus page">{r.ticker} ↗</a>
                  : '—'}
              </td>
              {hasCap && <td className="px-3 py-1.5 text-right font-mono text-fg-muted whitespace-nowrap">{capBn(r.market_cap_eur)}</td>}
              {/* ⚠ TWO DECIMALS, MATCHING THE SERVER. `weight_pct` is rounded to 2 there, and
                  printing 1 here made cap ÷ Σcap fail to reproduce the number beside it — on the
                  one table whose purpose is that the division can be checked. */}
              <td className="px-3 py-1.5 text-right font-mono text-fg-muted whitespace-nowrap">{r.weight_pct.toFixed(2)}%</td>
              <td className="px-3 py-1.5 font-mono text-[11px] text-fg-subtle whitespace-nowrap">{r.currency ?? '—'}</td>
              {r.status === 'unsubscribed' ? (
                // Can't fetch it — exchange outside the GuruFocus subscription.
                <td colSpan={data.years.length} className="px-3 py-1.5 text-warn-300"
                  title={`No ${noun}: ${r.ticker ?? ''}@${r.exchange ?? '?'} is on an exchange outside our GuruFocus subscription.`}>
                  Unsubscribed
                </td>
              ) : r.status === 'no_data' ? (
                <td colSpan={data.years.length} className="px-3 py-1.5">
                  {ingest[r.isin]?.busy ? (
                    <span className="text-[11px] text-fg-faint">fetching…</span>
                  ) : onFetch ? (
                    <span className="inline-flex items-center gap-2">
                      <button type="button" onClick={() => void fetchOne(r.isin, r.name)}
                        title="Fetch this company's financials from GuruFocus."
                        className="cursor-pointer text-[11px] px-2 py-0.5 rounded-lg border border-accent-600/40 text-accent-400 hover:bg-overlay/5">
                        Fetch financials
                      </button>
                      {ingest[r.isin]?.msg && (
                        <span className="text-[10px] text-warn-300" title={ingest[r.isin]?.msg}>
                          {ingest[r.isin]?.msg}
                        </span>
                      )}
                    </span>
                  ) : (
                    <span className="text-[11px] text-fg-faint">no {noun} ingested</span>
                  )}
                </td>
              ) : (
                data.years.map((y) => {
                  // ⚠ THE WEIGHT SITS UNDER THE VALUE IT WEIGHTS, not in a column of its own,
                  // because the two are only meaningful as a pair: the line is Σ(weight × value)
                  // and reading a company's contribution means multiplying two numbers that have
                  // to be adjacent. It is the second line rather than the first because the value
                  // is what the column is named after and what sorting ranks on.
                  const w = weightAt(r, y);
                  return (
                    <td key={y} className="px-3 py-1.5 text-right font-mono text-fg-soft whitespace-nowrap"
                      title={`${w == null
                        ? 'Not in this period’s average.'
                        : `${w.toFixed(2)}% of the weight behind this period’s line`}`
                        + (view === 'reported' ? '' : ` · ${fmt(r.revenue[y])} as reported`)}>
                      <span className="block"><Cell>{cellText(cellOf(r, y))}</Cell></span>
                      {/* ⚠ A NON-BREAKING SPACE, NOT AN EMPTY STRING, WHERE THERE IS NO WEIGHT. An
                          empty span collapses to zero height, so a row with a gap year would be
                          shorter than its neighbours and the table would ripple. */}
                      <span className="block text-[10px] leading-tight text-fg-faint">
                        <Cell>{w == null ? ' ' : `${w.toFixed(2)}%`}</Cell>
                      </span>
                    </td>
                  );
                })
              )}
            </tr>
          ))}
        </tbody>
        <tfoot>
          {/* Sum of the shown companies' weights — under 100% because cash / bonds / any holding
              we can't price aren't listed. */}
          <tr className="border-t border-neutral-800/40 bg-page font-semibold text-fg-strong">
            <td className="px-3 py-1.5 sticky left-0 bg-page z-10"
              title={view === 'rebased'
                ? `Weighted average of the ${blend.contributors} contributing rows — this row IS the plotted line.`
                : view === 'yoy'
                  ? 'The plotted line’s own period-on-period change. NOT the average of the column above: the chart averages rebased levels, never growth rates.'
                  : undefined}>
              <span className="block">
                {view === 'rebased' ? 'Weighted (= the line)' : view === 'yoy' ? 'Line YoY' : 'Total'}
              </span>
              {/* Names the second line every period cell in this row now carries. */}
              <span className="block text-[10px] leading-tight font-normal text-fg-faint">
                covered
              </span>
            </td>
            <td className="px-3 py-1.5" />
            <td className="px-3 py-1.5" />
            {/* ⚠ THE DENOMINATOR, SPELLED OUT. Without it the Weight column is a set of numbers to
                take on trust; with it, every row is cap ÷ this. */}
            {hasCap && (
              <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap"
                title="The sum every weight in this column was divided by.">
                {capBn(rows.reduce((a, r) => a + (r.market_cap_eur ?? 0), 0))}
              </td>
            )}
            <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
              {rows.reduce((a, r) => a + r.weight_pct, 0).toFixed(2)}%
            </td>
            <td className="px-3 py-1.5" />
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
              const value = view === 'reported' ? null
                : view === 'rebased' ? lv?.value ?? null
                  : (lv && prev && prev.value > 0) ? 100 * (lv.value / prev.value - 1) : null;
              const thin = lv != null && lv.covered < MIN_YEAR_COVERAGE_PCT;
              return (
                <td key={y}
                  className={`px-3 py-1.5 text-right font-mono whitespace-nowrap ${
                    thin ? 'text-fg-faint font-normal' : ''}`}
                  title={lv == null ? undefined
                    : `${lv.covered.toFixed(1)}% of the contributing weight reported this period`
                      + (thin ? ` — under the ${MIN_YEAR_COVERAGE_PCT}% floor, so the chart omits it`
                        : '')}>
                  <span className="block">
                    <Cell>
                      {value == null ? ' ' : view === 'rebased' ? value.toFixed(1)
                        : `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`}
                    </Cell>
                  </span>
                  {/* ⚠ COVERAGE, PROMOTED OUT OF THE TOOLTIP — it is the footer's second line
                      because every cell above it now has one, and because it is what the weights
                      above are shares OF. The column of weights sums to 100% within a period by
                      construction; this says what share of the index that 100% actually is. A
                      period under the floor is greyed with the rest of the cell. */}
                  <span className="block text-[10px] leading-tight text-fg-faint">
                    <Cell>{lv == null ? ' ' : `${lv.covered.toFixed(0)}%`}</Cell>
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
  benchTarget?: { universe: string; cadence: 'annual' | 'quarterly' } | null;
}) {
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
   * ⚠ ONE SWITCH FOR BOTH TABLES. The book above and the index below are here to be read against
   * each other; per-table switches would let someone compare a rebased index with reported euros
   * and take the gap for a finding. It is a view over rows already in hand — no refetch — so
   * flipping it cannot land the two tables on different vintages of the same accounts either.
   */
  const [view, setView] = useState<View>('reported');

  const load = async (body: Target): Promise<Resp> => {
    const r = await apiFetch(`${API_URL}/api/earnings/portfolio-revenue-matrix?metric=${encodeURIComponent(metric)}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    return b as Resp;
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
  const benchKey = benchTarget ? `${benchTarget.universe}|${benchTarget.cadence}` : '';
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
  }, [benchKey, metric]);

  /** Fetch a `no_data` holding's financials, then reload so its figures appear. Throws the stated
   *  reason on anything else, which the row renders. Admin-only endpoint. */
  const fetchRevenue = async (isin: string, name: string) => {
    const r = await apiFetch(`${API_URL}/api/earnings/fundamental-coverage/ingest`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ isin, name }),
    });
    const j = (await r.json().catch(() => null)) as { status?: string; detail?: string } | null;
    if (r.ok && j?.status === 'ingested') {
      setReloadKey((k) => k + 1);
      return;
    }
    throw new Error(j?.detail ?? j?.status ?? `HTTP ${r.status}`);
  };

  const section = 'text-[11px] uppercase tracking-wide text-fg-muted';
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
            <span className="text-[11px] text-fg-faint font-mono">
              {only.exchange ?? '—'}
              {only.ticker && <>
                {' '}
                <a href={guruFocusUrl(only.ticker, only.exchange)} target="_blank" rel="noopener noreferrer"
                  className="text-accent-400 hover:underline" title="Open the GuruFocus page">{only.ticker} ↗</a>
              </>}
              {only.currency && ` · ${only.currency}`}
            </span>
          ) : data && <span className="text-[11px] text-fg-faint">{data.rows.length} companies</span>}
          {benchLabel && <span className="text-[11px]" style={{ color: chartTheme.pos }}>vs {benchLabel}</span>}
          <button type="button" onClick={onClose} className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-5">
          {/* 0 — the switch, above everything it governs. */}
          <div className="flex items-center gap-2 text-[10px]">
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
                  : 'growth on that company’s previous reported period'}
              {view !== 'reported' && ' · hover a cell for the reported figure'}
            </span>
          </div>

          {/* 1 — the book (or the single company), on the same three views as the index below it. */}
          <div className="space-y-1.5">
            <h3 className={section}>
              {portfolioName ? `${portfolioName} — ` : ''}{noun} by period
            </h3>
            {err && <p className="text-xs text-neg-300">{err}</p>}
            {!data && !err && <p className="text-xs text-fg-subtle">Loading…</p>}
            {data && data.rows.length === 0 && !err && (
              <p className="text-xs text-fg-subtle">No held company has {noun} ingested.</p>
            )}
            {data && data.rows.length > 0 && (
              <MatrixTable data={data} fmt={fmtM} noun={noun} view={view} onFetch={fetchRevenue} />
            )}
          </div>

          {/* 3 — the same, for the index, on demand. */}
          {benchTarget && (
            <div className="space-y-1.5">
              <h3 className={section}>{benchLabel} constituents — {noun} by period</h3>
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
                  <p className="text-[10px] text-fg-faint">
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
                  <MatrixTable data={bench} fmt={fmtM} noun={noun} view={view} />
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
