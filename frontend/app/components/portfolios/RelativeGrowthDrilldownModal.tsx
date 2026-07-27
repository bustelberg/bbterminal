'use client';

import { useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';

/** The holdings behind ONE year of the Share-Price-vs-Owner-Earnings chart.
 *
 * ⚠ IT DOES NOT COMPUTE ANYTHING. Every figure is read from
 * `POST /api/earnings/relative-growth-breakdown`, which decomposes BOTH lines through the SAME
 * level `blend_breakdown` the chart is built from — so the price index, the OE index and their
 * ratio here are the same numbers the lines above are made of, not a second computation. */

type Member = {
  isin: string | null; name: string | null; weight_pct: number | null;
  price_index?: number | null; price_raw?: number | null;
  oe_index?: number | null; oe_raw?: number | null; ratio?: number | null;
};
type Excluded = { isin: string | null; name: string | null; weight_pct: number; reason: string };
type Data = {
  period: string;
  price: { value: number | null; covered_pct: number };
  oe: { value: number | null; covered_pct: number };
  ratio: number | null;
  members: Member[];
  excluded: Excluded[];
};

const REASON: Record<string, string> = {
  no_point_in_period: 'no figure for this year',
  non_positive_base: 'no positive base year, so it cannot be rebased',
  no_data: 'no price/earnings history for this metric',
  no_weight: 'zero weight in the model',
};

type SortKey = 'holding' | 'weight' | 'price' | 'oe' | 'ratio';

function cmp(a: number | string | null | undefined, b: number | string | null | undefined,
  dir: 'asc' | 'desc'): number {
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;
  const r = (typeof a === 'string' || typeof b === 'string')
    ? String(a).localeCompare(String(b)) : (a as number) - (b as number);
  return dir === 'desc' ? -r : r;
}

export default function RelativeGrowthDrilldownModal({
  title, period, portfolioId, basket, onClose,
}: {
  title?: string;
  period: string;
  portfolioId?: number;
  basket?: { holdings: { isin: string; weight: number; name?: string }[] };
  onClose: () => void;
}) {
  const [data, setData] = useState<Data | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [sort, setSort] = useState<{ key: SortKey; dir: 'asc' | 'desc' }>({ key: 'weight', dir: 'desc' });

  useEffect(() => {
    let alive = true;
    (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/relative-growth-breakdown`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            period,
            ...(basket
              ? { holdings: basket.holdings.map((h) => ({ isin: h.isin, name: h.name, weight: h.weight })) }
              : { portfolio_id: portfolioId }),
          }),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as Data);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [period, portfolioId, basket]);

  const toggleSort = (key: SortKey) => setSort((s) =>
    s.key === key ? { key, dir: s.dir === 'desc' ? 'asc' : 'desc' }
      : { key, dir: key === 'holding' ? 'asc' : 'desc' });
  const caret = (k: SortKey) => (sort.key === k ? (sort.dir === 'desc' ? ' ▾' : ' ▴') : '');

  const rows = useMemo(() => {
    const get: Record<SortKey, (m: Member) => number | string | null | undefined> = {
      holding: (m) => (m.name ?? m.isin ?? '').toLowerCase(),
      weight: (m) => m.weight_pct,
      price: (m) => m.price_index,
      oe: (m) => m.oe_index,
      ratio: (m) => m.ratio,
    };
    return [...(data?.members ?? [])].sort((a, b) => cmp(get[sort.key](a), get[sort.key](b), sort.dir));
  }, [data, sort]);

  const idx = (v: number | null | undefined) => (v == null ? '—' : v.toFixed(1));
  const ratio = (v: number | null | undefined) => (v == null ? '—' : v.toFixed(2));
  const raw = (v: number | null | undefined) => (v == null ? undefined
    : `as reported: ${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}`);

  // Column help — WHAT / WHERE / WHEN / HOW, the shared template.
  const INFO: Record<SortKey, { what: string; where: string; when: string; how: string }> = {
    holding: { what: 'The company behind this point.',
      where: "Our company record, matched from the model position's ISIN.",
      when: "The model's current composition.", how: 'One row per position.' },
    weight: { what: "The holding's weight in the portfolio.", where: 'The AIRS model composition.',
      when: 'Current composition.', how: 'Renormalised over the holdings reporting this year.' },
    price: { what: 'Share-price growth, indexed to 100 at the start year.',
      where: 'GuruFocus — metric_data, “Month End Stock Price”.', when: 'Up to this fiscal year.',
      how: "Month-end price rebased to 100 at the holding's first year; the weighted average of "
        + "these is the chart's price line." },
    oe: { what: 'Owner-Earnings (EPS ex-NRI) growth, indexed to 100.',
      where: 'GuruFocus — metric_data, “EPS without NRI”.', when: 'Up to this fiscal year.',
      how: "EPS rebased to 100 at the holding's first year; the weighted average is the chart's OE "
        + 'line. Dividends are not added — a dividend is a distribution of EPS, not extra earnings.' },
    ratio: { what: 'Price ÷ OE — how much this holding\'s earnings multiple has moved.',
      where: 'Computed: price index ÷ OE index.', when: 'Up to this fiscal year.',
      how: '> 1 means price grew faster than earnings (getting more expensive); < 1 means earnings '
        + 'outran price (getting cheaper). Rebasing to a common start cancels in the ratio.' },
  };

  const th = (key: SortKey, label: string, align: 'left' | 'right', extra = '') => (
    <th className={`px-3 py-2 font-medium ${align === 'left' ? 'text-left' : 'text-right whitespace-nowrap'} ${extra}`}
      onClick={() => toggleSort(key)}>
      <span className={`inline-flex items-center gap-1 ${align === 'right' ? 'justify-end' : ''}`}>
        {label}{caret(key)}
        <InfoTip content={<AspectCard {...INFO[key]} />} />
      </span>
    </th>
  );

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[80vw] h-[80vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">{title ?? 'Share Price vs. Owner Earnings'}</h2>
          <span className="text-fg-muted text-sm">{period}</span>
          {data?.ratio != null && (
            <span className="font-mono text-fg-strong">
              price {idx(data.price.value)} / OE {idx(data.oe.value)} → ×{ratio(data.ratio)}
            </span>
          )}
          <button type="button" onClick={onClose}
            className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-4">
          {err && <p className="text-xs text-neg-300">{err}</p>}
          {!data && !err && <p className="text-xs text-fg-subtle">Loading breakdown…</p>}

          {data && (
            <>
              <p className="text-[11px] text-fg-faint max-w-3xl">
                Each holding indexed to 100 at its start year: Price is month-end share price, OE is
                Owner Earnings (EPS ex-NRI). Price ÷ OE &gt; 1 means the multiple expanded. The
                Portfolio row is the weighted blend — the two lines on the chart.
              </p>

              <div className="overflow-x-auto rounded-lg border border-neutral-800/40">
                <table className="w-full text-xs">
                  <thead className="bg-page">
                    <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40 [&>th]:cursor-pointer [&>th]:select-none [&>th:hover]:text-fg-soft [&>th]:transition-colors">
                      {th('holding', 'Holding', 'left', 'w-full')}
                      {th('weight', 'Weight', 'right')}
                      {th('price', 'Price', 'right')}
                      {th('oe', 'OE', 'right')}
                      {th('ratio', 'Price ÷ OE', 'right')}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((m, i) => (
                      <tr key={`${m.isin ?? m.name}-${i}`}
                        className="border-b border-neutral-800/20 last:border-0 hover:bg-overlay/[0.03]">
                        <td className="px-3 py-1.5 text-fg-soft max-w-0">
                          <span className="block truncate" title={m.name ?? m.isin ?? ''}>{m.name ?? m.isin}</span>
                        </td>
                        <td className="px-3 py-1.5 text-right font-mono text-fg-muted whitespace-nowrap">
                          {m.weight_pct == null ? '—' : `${m.weight_pct.toFixed(1)}%`}
                        </td>
                        <td className="px-3 py-1.5 text-right font-mono text-fg-strong whitespace-nowrap" title={raw(m.price_raw)}>
                          {idx(m.price_index)}
                        </td>
                        <td className="px-3 py-1.5 text-right font-mono text-fg-strong whitespace-nowrap" title={raw(m.oe_raw)}>
                          {idx(m.oe_index)}
                        </td>
                        <td className={`px-3 py-1.5 text-right font-mono whitespace-nowrap ${
                          (m.ratio ?? 1) > 1 ? 'text-warn-300' : (m.ratio ?? 1) < 1 ? 'text-pos-400' : 'text-fg-muted'}`}>
                          {ratio(m.ratio)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                  {/* The weighted blend — literally the two chart lines at this year, and their ratio. */}
                  <tfoot>
                    <tr className="border-t border-neutral-800/40 bg-page font-semibold text-fg-strong">
                      <td className="px-3 py-2 text-left">Portfolio</td>
                      <td className="px-3 py-2" />
                      <td className="px-3 py-2 text-right font-mono whitespace-nowrap">{idx(data.price.value)}</td>
                      <td className="px-3 py-2 text-right font-mono whitespace-nowrap">{idx(data.oe.value)}</td>
                      <td className="px-3 py-2 text-right font-mono whitespace-nowrap">{ratio(data.ratio)}</td>
                    </tr>
                  </tfoot>
                </table>
              </div>

              {data.excluded.length > 0 && (
                <div className="space-y-1">
                  <h3 className="text-xs font-semibold text-fg-strong">
                    Not in this figure ({data.excluded.length})
                  </h3>
                  <div className="overflow-auto rounded-lg border border-neutral-800/40">
                    <table className="w-auto text-xs whitespace-nowrap">
                      <tbody>
                        {data.excluded.map((e, i) => (
                          <tr key={`${e.isin ?? e.name}-${i}`} className="border-b border-neutral-800/20">
                            <td className="px-3 py-1.5 text-fg-soft">
                              <span className="inline-block max-w-[28ch] truncate align-bottom">{e.name ?? e.isin}</span>
                            </td>
                            <td className="px-3 py-1.5 text-right font-mono text-fg-muted">{e.weight_pct.toFixed(1)}%</td>
                            <td className="px-3 py-1.5 text-fg-faint">{REASON[e.reason] ?? e.reason}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
