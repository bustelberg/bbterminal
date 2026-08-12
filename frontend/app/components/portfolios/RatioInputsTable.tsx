'use client';

import { Fragment, useMemo, useState } from 'react';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import {
  fmtRatioPct, fmtRevM, periodDenoms, transformSeries, type SeriesView, type Weighted,
} from './marginData';
import { CapWeightLines } from './capWeightLines';

/**
 * THE drill-down table behind every ratio card on the Long Equity tab — one component, eleven
 * callers, and the book and the index both render through it.
 *
 * ⚠⚠ IT EXISTS BECAUSE THERE WERE ELEVEN COPIES OF IT. Same six columns, same status rows, same
 * `LINES` loop, same derived row, same footer — differing only in which lines they list and which
 * ratio they derive, both of which are already per-card constants. Eleven copies is eleven places
 * for the BOOK's table and the INDEX's to come to format a figure, sort a null or hide a status
 * differently, on the one screen whose entire purpose is comparing them. It is also why adding the
 * cap/weight lines was a ten-file edit and why the benchmark only ever got built into one of them.
 *
 * ⚠ THE DERIVED ROW CALLS THE CARD'S OWN FUNCTION. `derived.of` is the same `marginOf` /
 * `debtRatioOf` / … the chart aggregates, so a drill-down cannot recompute the formula its own way
 * and quietly disagree with the line it is explaining. `periodDenoms` is fed that same function,
 * which is what makes the `weight` line sum to exactly 100% of what the chart drew.
 *
 * ⚠ EACH LINE CARRIES A GETTER, NOT A KEY. `{ key: 'revenue' }` would need an index-signature cast
 * to read `r[key]`, which throws away the row type and would happily accept a key the row does not
 * have. `of: (r, y) => r.revenue[y]` is checked.
 */

export type InputsRow = Weighted & {
  isin: string;
  name: string;
  currency: string | null;
  ticker: string | null;
  exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
};

export type InputsData<R extends InputsRow> = { years: string[]; rows: R[] };

export type InputsLine<R> = {
  label: string;
  of: (r: R, y: string) => number | null | undefined;
  /** Dimmer — a subtracted or netted-out line (SBC, goodwill) rather than a headline figure. */
  muted?: boolean;
};

/**
 * What the period columns show — the sibling of `HoldingsRevenueModal`'s `View`, and offered ONLY
 * on a card whose derived line is an AMOUNT.
 *
 * ⚠⚠ REBASING A RATIO IS NOT REBASING AN AMOUNT, AND THE DIFFERENCE IS NOT COSMETIC. Invested
 * capital is a currency level: the chart CANNOT plot it directly (mixed reporting currencies do not
 * sum), so it rebases each company to 100 at its own first period and weight-averages the indices —
 * `rebased` is literally what that chart draws. A MARGIN is already a ratio, and the same two
 * transforms are nonsense on it: indexing "11.4%" to 100 yields a number whose unit is nothing, and
 * YoY of a percentage is a percent OF a percent — a margin going 10% → 11% is +1pp and +10% YoY,
 * and a column showing `10` means neither. Both would render as perfectly ordinary numbers.
 *
 * So `derived.kind` is DECLARED by the card, never inferred from the values or from whether `fmt`
 * was passed — the same rule as the backend `_ITEMS` registry's `unit` (a share count is not
 * currency and `EPS (Diluted)` does not contain the words "per share"). A ratio card gets no switch
 * at all rather than a switch with two wrong options.
 */
export type InputsView = SeriesView;

const VIEWS: [InputsView, string, string][] = [
  ['reported', 'Reported', 'The figures as filed, in each company’s own reporting currency. ⚠ The '
    + 'only view in which the component lines still ADD to the derived line.'],
  ['rebased', 'Rebased', 'Every line indexed to 100 at ITS OWN first period. The derived line is '
    + 'exactly what the chart weight-averages — mixed reporting currencies cannot be summed, so a '
    + 'rebase is how the chart exists at all. ⚠ Indices do not add: the components no longer sum '
    + 'to the derived line here.'],
  ['yoy', 'YoY %', 'Growth on that company’s previous reported period — its own, not the previous '
    + 'column, so a skipped period does not show two years of growth in the same ink as everyone '
    + 'else’s one. ⚠ The chart does NOT average these: it averages the Rebased levels.'],
];

/**
 * The switch itself.
 *
 * ⚠ THE STATE BELONGS TO THE MODAL, NOT TO THE TABLE — which is why this is a separate export
 * rather than a `useState` inside `RatioInputsTable`. Every one of these modals renders the table
 * TWICE (the book, then the index), and a switch per table would let a reader set one to Rebased
 * and the other to Reported and read the gap between an index and a pile of euros as a finding.
 * One switch, above both tables, same rule as `HoldingsRevenueModal`'s.
 */
export function InputsViewSwitch({ view, onChange }: {
  view: InputsView; onChange: (v: InputsView) => void;
}) {
  return (
    <div className="flex items-center gap-2 text-[11px]">
      <div className="inline-flex rounded-lg border border-neutral-700 overflow-hidden">
        {VIEWS.map(([k, label, note]) => (
          <button key={k} type="button" onClick={() => onChange(k)} title={note}
            aria-pressed={view === k}
            className={`cursor-pointer px-2.5 py-0.5 font-medium transition-colors ${
              view === k ? 'bg-accent-600 text-white' : 'text-fg-muted hover:bg-overlay/5'}`}>
            {label}
          </button>
        ))}
      </div>
      <span className="text-fg-faint">
        {view === 'reported' ? 'as filed, each in its own currency — the components add up here'
          : view === 'rebased' ? 'indexed to 100 at each company’s first period — what the chart weights'
            : 'growth on that company’s previous reported period'}
        {view !== 'reported' && ' · hover a cell for the reported figure'}
      </span>
    </div>
  );
}

/**
 * The fixed columns, plus `impact:<period>` — one key per period column.
 *
 * ⚠ IMPACT IS WHAT THE READER ACTUALLY WANTS, AND IT IS NEITHER OF THE TWO THINGS THE TABLE SHOWED.
 * Sorted by weight you get the big names, which move the line only if they also moved; sorted by
 * the period's value you get the extremes, which move the line only if they are also big. The
 * question behind every one of these drill-downs — "who made the line do that?" — is the PRODUCT,
 * and a reader cannot form it by eye across a 25-row table with the two factors in different
 * columns. Clicking a period header ranks by it directly.
 */
type SortKey = 'name' | 'exchange' | 'ticker' | 'weight' | 'ccy' | `impact:${string}`;

function cmp(a: number | string | null | undefined, b: number | string | null | undefined,
  dir: 'asc' | 'desc') {
  if (a == null && b == null) return 0;
  if (a == null) return 1;                 // nulls last, both directions
  if (b == null) return -1;
  const r = (typeof a === 'string' || typeof b === 'string')
    ? String(a).localeCompare(String(b)) : a - b;
  return dir === 'desc' ? -r : r;
}

export function RatioInputsTable<R extends InputsRow>({
  data, lines, derived, fmtValue = fmtRevM, view = 'reported', onFetch,
}: {
  data: InputsData<R>;
  lines: InputsLine<R>[];
  /** The line the CARD plots, from the raw lines above it. `fmt` defaults to a percentage — pass
   *  one for a derived AMOUNT (invested capital) rather than a ratio. */
  derived: {
    label: string;
    of: (r: R, y: string) => number | null;
    fmt?: (v: number | null | undefined) => string;
    /**
     * ⚠ REQUIRED, AND NOT INFERRED FROM `fmt`. It decides two things a wrong answer renders
     * plausibly for: whether the view switch applies at all (see `InputsView`), and — the quieter
     * one — what the impact ranking is computed on. An `amount` card's chart plots a REBASED
     * index, so its steps are steps in index points; ranking it on Δ millions would rank by
     * reporting currency (a ¥ filer's Δ dwarfs a € filer's) while looking like a ranking of moves.
     */
    kind: 'amount' | 'ratio';
  };
  /** How a RAW line prints. Millions of the reporting currency for most cards; per-share amounts
   *  for the dividend-yield inputs. */
  fmtValue?: (v: number | null | undefined) => string;
  /** Owned by the MODAL so the book's table and the index's move together — see
   *  {@link InputsViewSwitch}. Ignored outright on a `ratio` card, which has no valid rebase. */
  view?: InputsView;
  /** Holdings only. An index is not curated row by row, so its `no_data` cells just say so. */
  onFetch?: (isin: string, name: string) => Promise<void>;
}) {
  const [sort, setSort] = useState<{ key: SortKey; dir: 'asc' | 'desc' }>(
    { key: 'weight', dir: 'desc' });
  const [ingest, setIngest] = useState<Record<string, { busy?: boolean; msg?: string }>>({});
  // ⚠ Memoised: it is a dependency of `denoms`, and `?? []` mints a fresh array every render.
  const years = useMemo(() => data.years ?? [], [data]);
  const derivedFmt = derived.fmt ?? fmtRatioPct;
  const rebasable = derived.kind === 'amount';
  /** ⚠ A `ratio` CARD IS PINNED TO `reported` HERE, not merely un-switched. The switch is already
   *  withheld from those callers, but a view arriving by any other route would silently render the
   *  two transforms this card exists to refuse. */
  const v: InputsView = rebasable ? view : 'reported';

  type Getter = (r: R, y: string) => number | null | undefined;
  /** One line of one row across the axis, under the current view. */
  const viewed = (r: R, of: Getter) => transformSeries(years.map((y) => of(r, y)), v);

  const cellText = (val: number | null, asFiled: (x: number | null | undefined) => string) => (
    val == null ? '—'
      : v === 'reported' ? asFiled(val)
        : v === 'rebased' ? val.toFixed(1)
          : `${val >= 0 ? '+' : ''}${val.toFixed(1)}%`);

  /**
   * The figure the CHART actually plots for each row — independent of the view switch.
   *
   * ⚠⚠ FOR AN AMOUNT THAT IS THE REBASED INDEX, NOT THE REPORTED LEVEL, and this is what the
   * impact ranking and the weight denominators must both be built on. The invested-capital line is
   * a weighted average of per-company indices precisely because mixed-currency levels cannot be
   * summed; ranking its moves on Δ millions would sort by reporting currency, and a denominator
   * built on the reported line would include rows the rebase drops (non-positive base) so the
   * `weight` column would no longer sum to 100% of the line it is explaining.
   */
  const plotted = useMemo(() => {
    const m = new Map<string, Record<string, number | null>>();
    for (const r of data.rows) {
      const s = transformSeries(years.map((y) => derived.of(r, y)),
        rebasable ? 'rebased' : 'reported');
      m.set(r.isin, Object.fromEntries(years.map((y, i) => [y, s[i]])));
    }
    return m;
    // ⚠ `derived` is an object literal in every caller, so a fresh identity each render — listing
    // it would defeat the memo entirely. `data` is what actually changes the values.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, years, rebasable]);
  const plotOf = (r: R, y: string): number | null => plotted.get(r.isin)?.[y] ?? null;

  /**
   * This row's contribution to the line's MOVE into `y`, in the plotted unit: `weight × Δvalue`.
   *
   * ⚠ THE CHANGE, NOT THE LEVEL. The blended line is a weighted average, so its step from one
   * period to the next is `Σ w·Δ` — a company sitting still contributes nothing to the move however
   * heavy it is, and a small name that doubled can outrank a mega-cap that drifted. Ranking on
   * `w × value` instead would just reproduce the weight order with extra steps.
   *
   * ⚠ NULL, NOT ZERO, WHEN EITHER END IS MISSING. A row that cannot be compared across the step did
   * not contribute nothing — we do not know what it contributed, and `cmp` puts nulls last in both
   * directions so an unknown never ranks as a small number. The first period has no predecessor and
   * is therefore all nulls, which is correct: nothing has moved yet.
   */
  const impactAt = (r: R, y: string): number | null => {
    const i = years.indexOf(y);
    if (i <= 0) return null;
    const now = plotOf(r, y);
    const before = plotOf(r, years[i - 1]);
    if (now == null || before == null) return null;
    return (r.weight_pct / 100) * (now - before);
  };

  const rows = useMemo(() => {
    const get: Record<string, (r: R) => number | string | null | undefined> = {
      name: (r) => r.name.toLowerCase(),
      exchange: (r) => r.exchange ?? '',
      ticker: (r) => r.ticker ?? '',
      weight: (r) => r.weight_pct,
      ccy: (r) => r.currency ?? '',
    };
    const of = sort.key.startsWith('impact:')
      ? (r: R) => impactAt(r, sort.key.slice('impact:'.length))
      : get[sort.key];
    return [...data.rows].sort((a, b) => cmp(of(a), of(b), sort.dir));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, sort, years]);

  /** The denominator each period's weighted average divided by — from the SAME function the
   *  derived row renders, so the `weight` line sums to exactly 100% of the plotted line. */
  const denoms = useMemo(
    () => periodDenoms(rows, () => years, plotOf),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [rows, years],
  );

  const toggle = (key: SortKey) => setSort((s) => (s.key === key
    ? { key, dir: s.dir === 'desc' ? 'asc' : 'desc' }
    // Impact reads biggest-first, like weight: the question is who moved it MOST. A second click
    // gives the other end, which is the one that answers "who dragged it down".
    : { key, dir: (key === 'weight' || key.startsWith('impact:')) ? 'desc' : 'asc' }));
  const caret = (k: SortKey) => (sort.key === k ? (sort.dir === 'desc' ? ' ▾' : ' ▴') : '');

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
          <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40 [&>th]:cursor-pointer [&>th]:select-none [&>th:hover]:text-fg-soft">
            <th className="px-3 py-1.5 font-medium text-left sticky left-0 bg-page z-10 w-full" onClick={() => toggle('name')}>Company{caret('name')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('exchange')}>GF exch{caret('exchange')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('ticker')}>Ticker{caret('ticker')}</th>
            <th className="px-3 py-1.5 font-medium text-right whitespace-nowrap" onClick={() => toggle('weight')}
              title="Share of this table. ⚠ NOT the weight used in any single period — a period renormalises over the companies that reported it, which is the `weight` line inside each company's block.">
              Weight{caret('weight')}
            </th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('ccy')}>Ccy{caret('ccy')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap">Line</th>
            {years.map((y, i) => (
              <th key={y} className={`px-3 py-1.5 font-medium text-right${i > 0 ? ' cursor-pointer' : ''}`}
                onClick={i > 0 ? () => toggle(`impact:${y}`) : undefined}
                title={i > 0
                  ? `Sort by impact on the ${y} move: each row's weight × its change from `
                    + `${years[i - 1]}. This is what moved the line, not what is biggest — a large `
                    + 'holding that did not change contributes nothing to the step. Rows we cannot '
                    + 'compare across the step sort last in both directions.'
                    + (rebasable ? ' ⚠ Measured on the REBASED index, which is what the chart '
                      + 'plots — a change in millions would rank by reporting currency.' : '')
                  : `${y} is the first period drawn, so nothing has moved into it yet — there is no `
                    + 'impact to rank.'}>
                {/* ⚠ Sized in `em`, not px — this table sits inside the tab-wide rem scale, and a
                    hardcoded size would stop tracking the header it belongs to at other densities.
                    `leading-none` keeps the taller glyph from adding a row of header height. */}
                {y}<span className="text-[1.5em] leading-none align-middle">
                  {caret(`impact:${y}`)}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            // Company-level cells — rendered on the FIRST line of the company's block only.
            const head = (
              <>
                <td className="px-3 py-1 text-fg-soft sticky left-0 bg-card z-10 max-w-[22ch]">
                  <span className="block truncate" title={r.name}>{r.name}</span>
                </td>
                <td className="px-3 py-1 font-mono text-[12px] text-fg-subtle whitespace-nowrap">{r.exchange ?? '—'}</td>
                <td className="px-3 py-1 font-mono text-[12px] whitespace-nowrap">
                  {r.ticker ? <a href={guruFocusUrl(r.ticker, r.exchange)} target="_blank" rel="noopener noreferrer" className="text-accent-400 hover:underline">{r.ticker} ↗</a> : '—'}
                </td>
                <td className="px-3 py-1 text-right font-mono text-fg-muted whitespace-nowrap">{r.weight_pct.toFixed(1)}%</td>
                <td className="px-3 py-1 font-mono text-[12px] text-fg-subtle whitespace-nowrap">{r.currency ?? '—'}</td>
              </>
            );
            if (r.status !== 'ok') {
              return (
                <tr key={r.isin} className="border-t border-neutral-800/40 hover:bg-overlay/[0.02]">
                  {head}
                  {r.status === 'unsubscribed' ? (
                    <td colSpan={years.length + 1} className="px-3 py-1 text-warn-300"
                      title={`${r.ticker ?? ''}@${r.exchange ?? '?'} is on an exchange outside our GuruFocus subscription.`}>Unsubscribed</td>
                  ) : (
                    <td colSpan={years.length + 1} className="px-3 py-1">
                      {ingest[r.isin]?.busy ? <span className="text-[12px] text-fg-faint">fetching…</span> : onFetch ? (
                        <span className="inline-flex items-center gap-2">
                          <button type="button" onClick={() => fetchOne(r.isin, r.name)}
                            className="text-[12px] px-2 py-0.5 rounded-lg border border-accent-600/40 text-accent-400 hover:bg-overlay/5">Fetch financials</button>
                          {ingest[r.isin]?.msg && <span className="text-[11px] text-warn-300" title={ingest[r.isin]?.msg}>{ingest[r.isin]?.msg}</span>}
                        </span>
                      ) : <span className="text-[12px] text-fg-faint">no figures ingested</span>}
                    </td>
                  )}
                </tr>
              );
            }
            return (
              <Fragment key={r.isin}>
                {lines.map((ln, li) => {
                  const shown = viewed(r, ln.of);
                  return (
                    <tr key={ln.label} className={`${li === 0 ? 'border-t border-neutral-800/40' : ''} hover:bg-overlay/[0.02]`}>
                      {li === 0 ? head : (
                        <>
                          <td className="px-3 py-1 sticky left-0 bg-card z-10" />
                          <td /><td /><td /><td />
                        </>
                      )}
                      <td className={`px-3 py-1 whitespace-nowrap ${ln.muted ? 'text-fg-muted' : 'text-fg-soft'}`}>{ln.label}</td>
                      {years.map((y, yi) => (
                        <td key={y} className="px-3 py-1 text-right font-mono text-fg-soft"
                          // ⚠ THE FIGURE AS FILED STAYS REACHABLE IN EVERY VIEW. An index point or
                          // a growth % with no way back to the number it came from is a claim the
                          // reader cannot check, which is the opposite of what a drill-down is for.
                          title={v === 'reported' ? undefined
                            : `${ln.label} ${y}: ${fmtValue(ln.of(r, y))} as reported`}>
                          {cellText(shown[yi], fmtValue)}
                        </td>
                      ))}
                    </tr>
                  );
                })}
                {/* The plotted figure, from the lines above it. */}
                {(() => {
                  const shown = viewed(r, derived.of);
                  return (
                    <tr className="hover:bg-overlay/[0.02]">
                      <td className="px-3 py-1 sticky left-0 bg-card z-10" /><td /><td /><td /><td />
                      <td className="px-3 py-1 whitespace-nowrap text-fg-soft font-medium">{derived.label}</td>
                      {years.map((y, yi) => (
                        <td key={y} className="px-3 py-1 text-right font-mono text-fg-soft font-medium"
                          title={v === 'reported' ? undefined
                            : `${derived.label} ${y}: ${derivedFmt(derived.of(r, y))} as reported`}>
                          {cellText(shown[yi], derivedFmt)}
                        </td>
                      ))}
                    </tr>
                  );
                })()}
                <CapWeightLines row={r} years={years} denoms={denoms} />
              </Fragment>
            );
          })}
        </tbody>
        <tfoot>
          <tr className="border-t border-neutral-800/40 bg-page font-semibold text-fg-strong">
            <td className="px-3 py-1.5 sticky left-0 bg-page z-10">Total</td>
            <td /><td />
            <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
              {rows.reduce((a, r) => a + r.weight_pct, 0).toFixed(1)}%
            </td>
            <td /><td />
            {years.map((y) => <td key={y} />)}
          </tr>
        </tfoot>
      </table>
    </div>
  );
}
