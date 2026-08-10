'use client';

import { Fragment, useMemo, useState } from 'react';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import { fmtRatioPct, fmtRevM, periodDenoms, type Weighted } from './marginData';
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

type SortKey = 'name' | 'exchange' | 'ticker' | 'weight' | 'ccy';

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
  data, lines, derived, fmtValue = fmtRevM, onFetch,
}: {
  data: InputsData<R>;
  lines: InputsLine<R>[];
  /** The line the CARD plots, from the raw lines above it. `fmt` defaults to a percentage — pass
   *  one for a derived AMOUNT (invested capital) rather than a ratio. */
  derived: {
    label: string;
    of: (r: R, y: string) => number | null;
    fmt?: (v: number | null | undefined) => string;
  };
  /** How a RAW line prints. Millions of the reporting currency for most cards; per-share amounts
   *  for the dividend-yield inputs. */
  fmtValue?: (v: number | null | undefined) => string;
  /** Holdings only. An index is not curated row by row, so its `no_data` cells just say so. */
  onFetch?: (isin: string, name: string) => Promise<void>;
}) {
  const [sort, setSort] = useState<{ key: SortKey; dir: 'asc' | 'desc' }>(
    { key: 'weight', dir: 'desc' });
  const [ingest, setIngest] = useState<Record<string, { busy?: boolean; msg?: string }>>({});
  // ⚠ Memoised: it is a dependency of `denoms`, and `?? []` mints a fresh array every render.
  const years = useMemo(() => data.years ?? [], [data]);
  const derivedFmt = derived.fmt ?? fmtRatioPct;

  const rows = useMemo(() => {
    const get: Record<SortKey, (r: R) => number | string | null | undefined> = {
      name: (r) => r.name.toLowerCase(),
      exchange: (r) => r.exchange ?? '',
      ticker: (r) => r.ticker ?? '',
      weight: (r) => r.weight_pct,
      ccy: (r) => r.currency ?? '',
    };
    return [...data.rows].sort((a, b) => cmp(get[sort.key](a), get[sort.key](b), sort.dir));
  }, [data, sort]);

  /** The denominator each period's weighted average divided by — from the SAME function the
   *  derived row renders, so the `weight` line sums to exactly 100% of the plotted line. */
  const denoms = useMemo(
    () => periodDenoms(rows, () => years, derived.of),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [rows, years],
  );

  const toggle = (key: SortKey) => setSort((s) => (s.key === key
    ? { key, dir: s.dir === 'desc' ? 'asc' : 'desc' }
    : { key, dir: key === 'weight' ? 'desc' : 'asc' }));
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
            {years.map((y) => <th key={y} className="px-3 py-1.5 font-medium text-right">{y}</th>)}
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
                {lines.map((ln, li) => (
                  <tr key={ln.label} className={`${li === 0 ? 'border-t border-neutral-800/40' : ''} hover:bg-overlay/[0.02]`}>
                    {li === 0 ? head : (
                      <>
                        <td className="px-3 py-1 sticky left-0 bg-card z-10" />
                        <td /><td /><td /><td />
                      </>
                    )}
                    <td className={`px-3 py-1 whitespace-nowrap ${ln.muted ? 'text-fg-muted' : 'text-fg-soft'}`}>{ln.label}</td>
                    {years.map((y) => (
                      <td key={y} className="px-3 py-1 text-right font-mono text-fg-soft">{fmtValue(ln.of(r, y))}</td>
                    ))}
                  </tr>
                ))}
                {/* The plotted figure, from the lines above it. */}
                <tr className="hover:bg-overlay/[0.02]">
                  <td className="px-3 py-1 sticky left-0 bg-card z-10" /><td /><td /><td /><td />
                  <td className="px-3 py-1 whitespace-nowrap text-fg-soft font-medium">{derived.label}</td>
                  {years.map((y) => (
                    <td key={y} className="px-3 py-1 text-right font-mono text-fg-soft font-medium">
                      {derivedFmt(derived.of(r, y))}
                    </td>
                  ))}
                </tr>
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
