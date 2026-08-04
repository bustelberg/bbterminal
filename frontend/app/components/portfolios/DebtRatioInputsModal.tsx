'use client';

import { Fragment, useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import { fmtRatioPct, fmtRevM, MIN_YEAR_COVERAGE_PCT } from './marginData';
import { debtRatioOf, type DebtRatioInputs, type DebtRatioRow } from './debtRatioData';
import { type Target } from './HoldingsRevenueModal';
import { type BenchTarget } from './benchSeries';

/** The base inputs behind the LTD / (Total Assets − Goodwill) ratio — THREE rows per company
 * (Long-Term Debt, Total Assets, Goodwill), each in the company's own reporting currency
 * (millions). Same columns / Unsubscribed / Fetch behaviour as the other drill-downs. Self-fetches
 * (so Fetch can reload). Mirrors {@link ./MarginInputsModal}.
 *
 * ⚠ NO REBASED/YoY SWITCH HERE, AND THAT IS NOT AN OMISSION. The growth charts plot a LEVEL, which
 * has to be rebased to 100 per member before it can be averaged — so their drill-down offers that
 * basis. A ratio is already normalised per company: the chart is a plain weighted average of the
 * ratios, and the ratio is printed per company per year on its own row. There is no second basis
 * to switch to; rebasing a percentage would produce a number that means nothing.
 *
 * ⚠ THE BENCHMARK GETS THE SAME TABLE, FROM THE SAME ENDPOINT. `{holdings|portfolio_id}` swapped
 * for `{universe}` — one component renders both, so the book's rows and the index's cannot come to
 * format a figure or hide a status differently on the one screen built for comparing them.
 */

const LINES: { key: 'long_term_debt' | 'total_assets' | 'goodwill'; label: string; muted?: boolean }[] = [
  { key: 'long_term_debt', label: 'Long-Term Debt' },
  { key: 'total_assets', label: 'Total Assets' },
  { key: 'goodwill', label: 'Goodwill', muted: true },
];

type SortKey = 'name' | 'exchange' | 'ticker' | 'weight' | 'ccy';
function cmp(a: number | string | null | undefined, b: number | string | null | undefined, dir: 'asc' | 'desc') {
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;
  const r = (typeof a === 'string' || typeof b === 'string') ? String(a).localeCompare(String(b)) : a - b;
  return dir === 'desc' ? -r : r;
}

/** One cohort's table — the book's holdings, or an index's constituents. */
function InputsTable({ data, onFetch }: {
  data: DebtRatioInputs;
  /** Holdings only. An index is not curated row by row, so its `no_data` cells just say so. */
  onFetch?: (isin: string, name: string) => Promise<void>;
}) {
  const [sort, setSort] = useState<{ key: SortKey; dir: 'asc' | 'desc' }>({ key: 'weight', dir: 'desc' });
  const [ingest, setIngest] = useState<Record<string, { busy?: boolean; msg?: string }>>({});
  // Memoised: it is a `useMemo` dep below, and `?? []` mints a fresh array on every render.
  const years = useMemo(() => data.years ?? [], [data]);

  const toggle = (key: SortKey) => setSort((s) => (s.key === key
    ? { key, dir: s.dir === 'desc' ? 'asc' : 'desc' }
    : { key, dir: (key === 'weight') ? 'desc' : 'asc' }));
  const caret = (k: SortKey) => (sort.key === k ? (sort.dir === 'desc' ? ' ▾' : ' ▴') : '');

  const rows = useMemo(() => {
    const get: Record<SortKey, (r: DebtRatioRow) => number | string | null | undefined> = {
      name: (r) => r.name.toLowerCase(),
      exchange: (r) => r.exchange ?? '',
      ticker: (r) => r.ticker ?? '',
      weight: (r) => r.weight_pct,
      ccy: (r) => r.currency ?? '',
    };
    return [...data.rows].sort((a, b) => cmp(get[sort.key](a), get[sort.key](b), sort.dir));
  }, [data, sort]);

  /**
   * The plotted line, per year, with the share of weight behind each point.
   *
   * ⚠ THIS ROW IS WHY A YEAR IS MISSING FROM THE CHART, AND WITHOUT IT THE MODAL LOOKS LIKE IT
   * CONTRADICTS THE CHART. Measured on the AEX 2026-08-04: 13 of 22 constituents carry a ratio in
   * 2015 — "most of them" — but only **75.74% of the weight**, under the 80% floor, so the chart
   * omits the year while the table plainly shows the figures. Two causes, both worth seeing:
   * six financials (ING, ABN AMRO, NN, ASR, Aegon, EXOR) report total assets and goodwill but NO
   * Long-Term Debt line at all — a bank's balance sheet has deposits and issued securities instead
   * — and three constituents (Prosus, Universal Music, DSM-Firmenich) did not exist as listed
   * companies in 2015. Together 24.26%.
   */
  const line = useMemo(() => {
    const out: Record<string, { value: number; covered: number }> = {};
    const total = data.rows.reduce((a, r) => a + r.weight_pct, 0);
    if (total <= 0) return out;
    for (const y of years) {
      let w = 0;
      let num = 0;
      for (const r of data.rows) {
        const v = debtRatioOf(r.long_term_debt[y], r.total_assets[y], r.goodwill[y]);
        if (v == null) continue;
        w += r.weight_pct;
        num += r.weight_pct * v;
      }
      if (w > 0) out[y] = { value: num / w, covered: 100 * w / total };
    }
    return out;
  }, [data, years]);

  const fetchOne = async (isin: string, name: string) => {
    if (!onFetch) return;
    setIngest((s) => ({ ...s, [isin]: { busy: true } }));
    try {
      await onFetch(isin, name);
      setIngest((s) => ({ ...s, [isin]: {} }));
    } catch (e) {
      setIngest((s) => ({ ...s, [isin]: { msg: e instanceof Error ? e.message : String(e) } }));
    }
  };

  return (
    <div className="overflow-auto rounded-lg border border-neutral-800/40">
      <table className="w-full text-xs">
        <thead className="bg-page">
          <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40 [&>th]:cursor-pointer [&>th]:select-none [&>th:hover]:text-fg-soft">
            <th className="px-3 py-1.5 font-medium text-left sticky left-0 bg-page z-10 w-full" onClick={() => toggle('name')}>Company{caret('name')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('exchange')}>GF exch{caret('exchange')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('ticker')}>Ticker{caret('ticker')}</th>
            <th className="px-3 py-1.5 font-medium text-right whitespace-nowrap" onClick={() => toggle('weight')}>Weight{caret('weight')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('ccy')}>Ccy{caret('ccy')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap">Line</th>
            {years.map((y) => <th key={y} className="px-3 py-1.5 font-medium text-right">{y}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const head = (
              <>
                <td className="px-3 py-1 text-fg-soft sticky left-0 bg-card z-10 max-w-[22ch]">
                  <span className="block truncate" title={r.name}>{r.name}</span>
                </td>
                <td className="px-3 py-1 font-mono text-[11px] text-fg-subtle whitespace-nowrap">{r.exchange ?? '—'}</td>
                <td className="px-3 py-1 font-mono text-[11px] whitespace-nowrap">
                  {r.ticker ? <a href={guruFocusUrl(r.ticker, r.exchange)} target="_blank" rel="noopener noreferrer" className="text-accent-400 hover:underline">{r.ticker} ↗</a> : '—'}
                </td>
                <td className="px-3 py-1 text-right font-mono text-fg-muted whitespace-nowrap">{r.weight_pct.toFixed(2)}%</td>
                <td className="px-3 py-1 font-mono text-[11px] text-fg-subtle whitespace-nowrap">{r.currency ?? '—'}</td>
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
                      {ingest[r.isin]?.busy ? <span className="text-[11px] text-fg-faint">fetching…</span> : onFetch ? (
                        <span className="inline-flex items-center gap-2">
                          <button type="button" onClick={() => void fetchOne(r.isin, r.name)}
                            className="cursor-pointer text-[11px] px-2 py-0.5 rounded-lg border border-accent-600/40 text-accent-400 hover:bg-overlay/5">Fetch financials</button>
                          {ingest[r.isin]?.msg && <span className="text-[10px] text-warn-300" title={ingest[r.isin]?.msg}>{ingest[r.isin]?.msg}</span>}
                        </span>
                      ) : <span className="text-[11px] text-fg-faint">no figures ingested</span>}
                    </td>
                  )}
                </tr>
              );
            }
            return (
              <Fragment key={r.isin}>
                {LINES.map((ln, li) => (
                  <tr key={`${r.isin}-${ln.key}`} className={`${li === 0 ? 'border-t border-neutral-800/40' : ''} hover:bg-overlay/[0.02]`}>
                    {li === 0 ? head : (
                      <>
                        <td className="px-3 py-1 sticky left-0 bg-card z-10" />
                        <td /><td /><td /><td />
                      </>
                    )}
                    <td className={`px-3 py-1 whitespace-nowrap ${ln.muted ? 'text-fg-muted' : 'text-fg-soft'}`}>{ln.label}</td>
                    {years.map((y) => (
                      <td key={y} className="px-3 py-1 text-right font-mono text-fg-soft">{fmtRevM(r[ln.key][y])}</td>
                    ))}
                  </tr>
                ))}
                {/* The plotted ratio, from the lines above it. */}
                <tr className="hover:bg-overlay/[0.02]">
                  <td className="px-3 py-1 sticky left-0 bg-card z-10" /><td /><td /><td /><td />
                  <td className="px-3 py-1 whitespace-nowrap text-fg-soft font-medium">Debt / assets ex-GW</td>
                  {years.map((y) => (
                    <td key={y} className="px-3 py-1 text-right font-mono text-fg-soft font-medium">
                      {fmtRatioPct(debtRatioOf(r.long_term_debt[y], r.total_assets[y], r.goodwill[y]))}
                    </td>
                  ))}
                </tr>
              </Fragment>
            );
          })}
        </tbody>
        <tfoot>
          <tr className="border-t border-neutral-800/40 bg-page font-semibold text-fg-strong">
            <td className="px-3 py-1.5 sticky left-0 bg-page z-10">Total</td>
            <td /><td />
            <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
              {rows.reduce((a, r) => a + r.weight_pct, 0).toFixed(2)}%
            </td>
            <td /><td />
            {years.map((y) => <td key={y} />)}
          </tr>
          {/* ⚠ A GREYED FIGURE IS A YEAR THE CHART DOES NOT DRAW. It is computed and shown anyway,
              because the alternative — a blank — makes the modal look like it is missing data when
              the data is right there in the rows above; the fact is that too little of the WEIGHT
              reported. Hover for the exact share. */}
          <tr className="bg-page font-semibold text-fg-strong">
            <td className="px-3 py-1.5 sticky left-0 bg-page z-10" />
            <td /><td /><td /><td />
            <td className="px-3 py-1.5 whitespace-nowrap"
              title="Weight-averaged over the companies reporting in each year — this row is the plotted line. Greyed years fall under the 80% coverage floor and are omitted from the chart.">
              Weighted (= the line)
            </td>
            {years.map((y) => {
              const lv = line[y];
              const thin = lv != null && lv.covered < MIN_YEAR_COVERAGE_PCT;
              return (
                <td key={y}
                  className={`px-3 py-1.5 text-right font-mono whitespace-nowrap ${
                    thin ? 'text-fg-faint font-normal' : ''}`}
                  title={lv == null ? undefined
                    : `${lv.covered.toFixed(2)}% of the weight reported`
                      + (thin ? ` — under the ${MIN_YEAR_COVERAGE_PCT}% floor, so the chart omits this year` : '')}>
                  {fmtRatioPct(lv?.value ?? null)}
                </td>
              );
            })}
          </tr>
        </tfoot>
      </table>
    </div>
  );
}

export default function DebtRatioInputsModal({ target, portfolioName, benchTarget, benchLabel, onClose }: {
  target: Target; portfolioName?: string | null; onClose: () => void;
  /** The index the chart is drawn against, when one is ticked. Same endpoint, same table. */
  benchTarget?: BenchTarget | null;
  benchLabel?: string | null;
}) {
  const [data, setData] = useState<DebtRatioInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [reloadKey, setReloadKey] = useState(0);

  const load = async (body: Target | BenchTarget): Promise<DebtRatioInputs> => {
    const r = await apiFetch(`${API_URL}/api/earnings/debt-ratio-inputs`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    return b as DebtRatioInputs;
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
  }, [target, reloadKey]);

  /** The index's constituents. Silent on failure: it is an addition to a modal that works. */
  const [bench, setBench] = useState<DebtRatioInputs | null>(null);
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
        console.warn('[bb:bench] debt-ratio constituents:', e);
        if (alive) setBenchErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [benchKey]);

  /** Fetch a `no_data` holding's financials, then reload. Throws the stated reason otherwise, which
   *  the row renders — a fetch that loaded financials carrying no balance sheet is a real answer. */
  const fetchFinancials = async (isin: string, name: string) => {
    const r = await apiFetch(`${API_URL}/api/earnings/fundamental-coverage/ingest`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ isin, name }),
    });
    const j = (await r.json().catch(() => null)) as { status?: string; detail?: string } | null;
    if (r.ok && j?.status === 'ingested') { setReloadKey((k) => k + 1); return; }
    throw new Error(j?.detail ?? j?.status ?? `HTTP ${r.status}`);
  };

  const section = 'text-[11px] uppercase tracking-wide text-fg-muted';

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[88vw] h-[84vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">Debt / assets ex-GW — everything behind the chart</h2>
          {portfolioName && <span className="text-sm text-fg-soft truncate max-w-[24ch]" title={portfolioName}>{portfolioName}</span>}
          {data && <span className="text-[11px] text-fg-faint">{data.rows.length} companies</span>}
          {benchLabel && <span className="text-[11px]" style={{ color: chartTheme.pos }}>vs {benchLabel}</span>}
          <button type="button" onClick={onClose} className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-5">
          <p className="text-[11px] text-fg-faint">
            Long-Term Debt, Total Assets and Goodwill as reported (millions, native currency).
            Ratio = Long-Term Debt ÷ (Total Assets − Goodwill), weight-averaged across companies —
            a ratio is already per-company normalised, so it is averaged directly and never rebased.
          </p>

          <div className="space-y-1.5">
            <h3 className={section}>{portfolioName ? `${portfolioName} — ` : ''}inputs by year</h3>
            {err && <p className="text-xs text-neg-300">{err}</p>}
            {!data && !err && <p className="text-xs text-fg-subtle">Loading…</p>}
            {data && data.rows.length === 0 && !err && <p className="text-xs text-fg-subtle">No held company has these figures ingested.</p>}
            {data && data.rows.length > 0 && <InputsTable data={data} onFetch={fetchFinancials} />}
          </div>

          {benchTarget && (
            <div className="space-y-1.5">
              <h3 className={section}>{benchLabel} constituents — inputs by year</h3>
              {!bench && !benchErr && <p className="text-xs text-fg-subtle">Loading {benchLabel} constituents…</p>}
              {benchErr && <p className="text-xs text-neg-300">{benchErr}</p>}
              {bench && (
                <>
                  {/* ⚠ THE COVERAGE GAP IS STATED, as on the growth drill-down: only constituents
                      with these lines ingested feed the benchmark line, so a table longer than the
                      contributing set is not a mismatch. */}
                  <p className="text-[10px] text-fg-faint">
                    {bench.rows.length} constituents ·{' '}
                    {bench.rows.filter((r) => r.status === 'ok').length} with figures feed the line,
                    renormalised each period
                  </p>
                  <InputsTable data={bench} />
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
