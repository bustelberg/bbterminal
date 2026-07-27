'use client';

import { useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import { fmtRevM } from './marginData';
import { type CashReturnInputs, type CashReturnRow } from './cashReturnData';
import { type Target } from './HoldingsRevenueModal';

/** The base inputs behind Cash return on capital — THREE rows per company (Free Cash Flow,
 * Non-current liabilities, Equity), each in the company's own reporting currency (millions). Same
 * columns / Unsubscribed / Fetch behaviour as the other drill-downs. Self-fetches (so Fetch can
 * reload). Mirrors {@link ./DebtRatioInputsModal}. */

const LINES: { key: 'fcf' | 'noncurrent_liabilities' | 'total_equity'; label: string; muted?: boolean }[] = [
  { key: 'fcf', label: 'Free Cash Flow' },
  { key: 'noncurrent_liabilities', label: 'Non-curr. liabilities', muted: true },
  { key: 'total_equity', label: 'Total equity', muted: true },
];

type SortKey = 'name' | 'exchange' | 'ticker' | 'weight' | 'ccy';
function cmp(a: number | string | null | undefined, b: number | string | null | undefined, dir: 'asc' | 'desc') {
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;
  const r = (typeof a === 'string' || typeof b === 'string') ? String(a).localeCompare(String(b)) : a - b;
  return dir === 'desc' ? -r : r;
}

export default function CashReturnInputsModal({ target, portfolioName, onClose }: {
  target: Target; portfolioName?: string | null; onClose: () => void;
}) {
  const [data, setData] = useState<CashReturnInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [sort, setSort] = useState<{ key: SortKey; dir: 'asc' | 'desc' }>({ key: 'weight', dir: 'desc' });
  const [reloadKey, setReloadKey] = useState(0);
  const [ingest, setIngest] = useState<Record<string, { busy?: boolean; msg?: string }>>({});

  useEffect(() => {
    let alive = true;
    (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/cash-return-inputs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(target),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as CashReturnInputs);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [target, reloadKey]);

  const fetchFinancials = async (isin: string, name: string) => {
    setIngest((s) => ({ ...s, [isin]: { busy: true } }));
    try {
      const r = await apiFetch(`${API_URL}/api/earnings/fundamental-coverage/ingest`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ isin, name }),
      });
      const j = (await r.json().catch(() => null)) as { status?: string; detail?: string } | null;
      setIngest((s) => ({
        ...s,
        [isin]: { msg: j?.status === 'ingested' ? 'fetched — no figures reported' : (j?.detail ?? j?.status ?? `HTTP ${r.status}`) },
      }));
      setReloadKey((k) => k + 1);
    } catch (e) {
      setIngest((s) => ({ ...s, [isin]: { msg: e instanceof Error ? e.message : String(e) } }));
    }
  };

  const toggle = (key: SortKey) => setSort((s) =>
    s.key === key ? { key, dir: s.dir === 'desc' ? 'asc' : 'desc' } : { key, dir: (key === 'name' || key === 'ccy' || key === 'exchange' || key === 'ticker') ? 'asc' : 'desc' });
  const caret = (k: SortKey) => (sort.key === k ? (sort.dir === 'desc' ? ' ▾' : ' ▴') : '');
  const years = data?.years ?? [];

  const rows = useMemo(() => {
    const get: Record<SortKey, (r: CashReturnRow) => number | string | null | undefined> = {
      name: (r) => r.name.toLowerCase(),
      exchange: (r) => r.exchange ?? '',
      ticker: (r) => r.ticker ?? '',
      weight: (r) => r.weight_pct,
      ccy: (r) => r.currency ?? '',
    };
    return [...(data?.rows ?? [])].sort((a, b) => cmp(get[sort.key](a), get[sort.key](b), sort.dir));
  }, [data, sort]);

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[88vw] h-[84vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">Holdings — cash-return inputs by year</h2>
          {portfolioName && <span className="text-sm text-fg-soft truncate max-w-[24ch]" title={portfolioName}>{portfolioName}</span>}
          {data && <span className="text-[11px] text-fg-faint">{data.rows.length} companies</span>}
          <button type="button" onClick={onClose} className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-3">
          <p className="text-[11px] text-fg-faint">Free Cash Flow, non-current liabilities and total equity as reported (millions, native currency). Ratio = FCF ÷ (non-current liabilities + total equity).</p>
          {err && <p className="text-xs text-neg-300">{err}</p>}
          {!data && !err && <p className="text-xs text-fg-subtle">Loading…</p>}
          {data && data.rows.length === 0 && !err && <p className="text-xs text-fg-subtle">No held company has these figures ingested.</p>}

          {data && data.rows.length > 0 && (
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
                        <td className="px-3 py-1 text-right font-mono text-fg-muted whitespace-nowrap">{r.weight_pct.toFixed(1)}%</td>
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
                              {ingest[r.isin]?.busy ? <span className="text-[11px] text-fg-faint">fetching…</span> : (
                                <span className="inline-flex items-center gap-2">
                                  <button type="button" onClick={() => fetchFinancials(r.isin, r.name)}
                                    className="text-[11px] px-2 py-0.5 rounded-lg border border-accent-600/40 text-accent-400 hover:bg-overlay/5">Fetch financials</button>
                                  {ingest[r.isin]?.msg && <span className="text-[10px] text-warn-300" title={ingest[r.isin]?.msg}>{ingest[r.isin]?.msg}</span>}
                                </span>
                              )}
                            </td>
                          )}
                        </tr>
                      );
                    }
                    return LINES.map((ln, li) => (
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
                    ));
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
          )}
        </div>
      </div>
    </div>
  );
}
