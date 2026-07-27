'use client';

import { useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';

/** One plain table: each equity the portfolio HOLDS — its weight, native currency, and actual
 * revenue per fiscal year (2015+), as reported. Reads `POST /api/earnings/portfolio-revenue-matrix`
 * (the holdings, looked through any linked certificate, deduped by ISIN). */

type Row = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data'; revenue: Record<string, number | null>;
};
type Resp = { years: string[]; rows: Row[]; holdings: number };
export type Target = { portfolio_id?: number; holdings?: { isin: string; name?: string; weight: number }[] };

// Sort key is a fixed column ('name'|'weight'|'ccy') OR a year string ('2018').
function cmp(a: number | string | null | undefined, b: number | string | null | undefined, dir: 'asc' | 'desc') {
  if (a == null && b == null) return 0;
  if (a == null) return 1;        // nulls last, both directions
  if (b == null) return -1;
  const r = (typeof a === 'string' || typeof b === 'string') ? String(a).localeCompare(String(b)) : a - b;
  return dir === 'desc' ? -r : r;
}

export default function HoldingsRevenueModal({ target, metric = 'revenue', unit = 'millions', noun = 'revenue', portfolioName, onClose }: {
  target: Target;
  metric?: string;
  unit?: 'millions' | 'per_share' | 'percent';
  noun?: string;
  portfolioName?: string | null;
  onClose: () => void;
}) {
  // millions → compact B/T/M; per_share → a plain per-share figure; percent → a % ratio.
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
  const [sort, setSort] = useState<{ key: string; dir: 'asc' | 'desc' }>({ key: 'weight', dir: 'desc' });
  const [reloadKey, setReloadKey] = useState(0);
  // Per-ISIN ingest state: busy while fetching, then the outcome if it didn't yield revenue.
  const [ingest, setIngest] = useState<Record<string, { busy?: boolean; msg?: string }>>({});

  useEffect(() => {
    let alive = true;
    (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/portfolio-revenue-matrix?metric=${encodeURIComponent(metric)}`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(target),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as Resp);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [target, metric, reloadKey]);

  // Fetch financials for a holding on a subscribed exchange (a `no_data` row), then reload the
  // table so the revenue appears. Uses the shared ingest endpoint (admin-only).
  const fetchRevenue = async (isin: string, name: string) => {
    setIngest((s) => ({ ...s, [isin]: { busy: true } }));
    try {
      const r = await apiFetch(`${API_URL}/api/earnings/fundamental-coverage/ingest`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ isin, name }),
      });
      const j = (await r.json().catch(() => null)) as { status?: string; detail?: string } | null;
      // On success WITH revenue the reload flips the row to numbers and this msg is never rendered
      // (the `no_data` branch is gone). If it stays `no_data`, the msg explains why — so a fetch
      // that loaded financials but no income statement doesn't read as "nothing happened".
      setIngest((s) => ({
        ...s,
        [isin]: { msg: j?.status === 'ingested' ? 'fetched — no revenue reported' : (j?.detail ?? j?.status ?? `HTTP ${r.status}`) },
      }));
      setReloadKey((k) => k + 1);
    } catch (e) {
      setIngest((s) => ({ ...s, [isin]: { msg: e instanceof Error ? e.message : String(e) } }));
    }
  };

  const toggle = (key: string) => setSort((s) =>
    s.key === key ? { key, dir: s.dir === 'desc' ? 'asc' : 'desc' }
      // Names/currency read A→Z first; weight and revenue read biggest-first.
      : { key, dir: (key === 'name' || key === 'ccy') ? 'asc' : 'desc' });
  const caret = (k: string) => (sort.key === k ? (sort.dir === 'desc' ? ' ▾' : ' ▴') : '');

  const rows = useMemo(() => {
    const get = (r: Row): number | string | null | undefined => (
      sort.key === 'name' ? r.name.toLowerCase()
        : sort.key === 'exchange' ? (r.exchange ?? '')
          : sort.key === 'ticker' ? (r.ticker ?? '')
            : sort.key === 'weight' ? r.weight_pct
              : sort.key === 'ccy' ? (r.currency ?? '')
                : r.revenue[sort.key]);      // a year column
    return [...(data?.rows ?? [])].sort((a, b) => cmp(get(a), get(b), sort.dir));
  }, [data, sort]);

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[88vw] h-[84vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">Holdings — {noun} by year</h2>
          {portfolioName && <span className="text-sm text-fg-soft truncate max-w-[24ch]" title={portfolioName}>{portfolioName}</span>}
          {data && <span className="text-[11px] text-fg-faint">{data.rows.length} companies</span>}
          <button type="button" onClick={onClose} className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-3">
          {err && <p className="text-xs text-neg-300">{err}</p>}
          {!data && !err && <p className="text-xs text-fg-subtle">Loading…</p>}
          {data && data.rows.length === 0 && !err && (
            <p className="text-xs text-fg-subtle">No held company has {noun} ingested.</p>
          )}

          {data && data.rows.length > 0 && (
            <div className="overflow-auto rounded-lg border border-neutral-800/40">
              <table className="w-full text-xs">
                <thead className="bg-page">
                  <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40 [&>th]:cursor-pointer [&>th]:select-none [&>th:hover]:text-fg-soft">
                    {/* Company takes the slack so the table fills the width; years keep natural size. */}
                    <th className="px-3 py-1.5 font-medium text-left sticky left-0 bg-page z-10 w-full" onClick={() => toggle('name')}>Company{caret('name')}</th>
                    <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('exchange')}>GF exch{caret('exchange')}</th>
                    <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('ticker')}>Ticker{caret('ticker')}</th>
                    <th className="px-3 py-1.5 font-medium text-right whitespace-nowrap" onClick={() => toggle('weight')}>Weight{caret('weight')}</th>
                    <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('ccy')}>Ccy{caret('ccy')}</th>
                    {data.years.map((y) => (
                      <th key={y} className="px-3 py-1.5 font-medium text-right whitespace-nowrap" onClick={() => toggle(y)}>{y}{caret(y)}</th>
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
                      <td className="px-3 py-1.5 text-right font-mono text-fg-muted whitespace-nowrap">{r.weight_pct.toFixed(1)}%</td>
                      <td className="px-3 py-1.5 font-mono text-[11px] text-fg-subtle whitespace-nowrap">{r.currency ?? '—'}</td>
                      {r.status === 'unsubscribed' ? (
                        // Can't fetch it — exchange outside the GuruFocus subscription.
                        <td colSpan={data.years.length} className="px-3 py-1.5 text-warn-300"
                          title={`No revenue: ${r.ticker ?? ''}@${r.exchange ?? '?'} is on an exchange outside our GuruFocus subscription.`}>
                          Unsubscribed
                        </td>
                      ) : r.status === 'no_data' ? (
                        <td colSpan={data.years.length} className="px-3 py-1.5">
                          {ingest[r.isin]?.busy ? (
                            <span className="text-[11px] text-fg-faint">fetching…</span>
                          ) : (
                            <span className="inline-flex items-center gap-2">
                              <button type="button" onClick={() => fetchRevenue(r.isin, r.name)}
                                title="Fetch this company's financials from GuruFocus."
                                className="text-[11px] px-2 py-0.5 rounded-lg border border-accent-600/40 text-accent-400 hover:bg-overlay/5">
                                Fetch financials
                              </button>
                              {ingest[r.isin]?.msg && (
                                <span className="text-[10px] text-warn-300" title={ingest[r.isin]?.msg}>
                                  {ingest[r.isin]?.msg}
                                </span>
                              )}
                            </span>
                          )}
                        </td>
                      ) : (
                        data.years.map((y) => (
                          <td key={y} className="px-3 py-1.5 text-right font-mono text-fg-soft whitespace-nowrap">{fmtM(r.revenue[y])}</td>
                        ))
                      )}
                    </tr>
                  ))}
                </tbody>
                <tfoot>
                  {/* Sum of the shown companies' weights — under 100% because cash / bonds / any
                      holding we can't price aren't listed. */}
                  <tr className="border-t border-neutral-800/40 bg-page font-semibold text-fg-strong">
                    <td className="px-3 py-1.5 sticky left-0 bg-page z-10">Total</td>
                    <td className="px-3 py-1.5" />
                    <td className="px-3 py-1.5" />
                    <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
                      {rows.reduce((a, r) => a + r.weight_pct, 0).toFixed(1)}%
                    </td>
                    <td className="px-3 py-1.5" />
                    {data.years.map((y) => <td key={y} className="px-3 py-1.5" />)}
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
