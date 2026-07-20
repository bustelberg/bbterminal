'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import PerformanceTable, { type PerfWindow } from './PerformanceTable';

type Perf = { isin?: string | null; symbol?: string | null; currency?: string | null; label?: string | null; windows?: PerfWindow[] };
/** A group of holdings to aggregate as a value-weighted basket, instead of a single ISIN.
 *  `name` is carried only for progress display (the backend ignores it). */
export type Basket = { holdings: { isin: string; weight: number; name?: string }[]; label: string };

/**
 * Performance for one holding — returns AND risk — across 2/5/10-year trailing windows, from the
 * daily EUR price (the same `asset_price` source the rest of /portfolios uses). A stat table, not a
 * chart. Each window pairs a CAGR with the R² of its log-linear price fit, so you see not just how
 * fast it compounded but how STEADILY. Everything is EUR (FX vol included).
 */
export default function PerformanceModal({
  isin, name, basket, onClose,
}: {
  isin?: string;
  name?: string | null;
  basket?: Basket;
  onClose: () => void;
}) {
  const [data, setData] = useState<Perf | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  // Stable key so an inline-created `basket` object doesn't refetch every render.
  const reqKey = basket ? `b:${basket.holdings.map((h) => `${h.isin}:${h.weight}`).join(',')}` : `i:${isin}`;

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true); setError(null);
      try {
        const r = basket
          ? await apiFetch(`${API_URL}/api/asset-pipeline/basket/performance`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ holdings: basket.holdings, label: basket.label }),
          })
          : await apiFetch(`${API_URL}/api/asset-pipeline/risk/isin/${encodeURIComponent(isin ?? '')}`);
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as Perf);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reqKey]);

  const windows = data?.windows ?? [];
  // All windows end at the same latest close; use it as the WHEN for every metric's ⓘ card.
  const asOf = windows.find((w) => w.to_date)?.to_date ?? null;
  const title = name ?? basket?.label ?? isin ?? '';

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-scrim/60"
      onClick={onClose} role="dialog" aria-modal="true">
      <div className="bg-card border border-neutral-800/40 rounded-xl shadow-xl w-[96vw] max-w-[620px] max-h-[94vh] overflow-auto p-4"
        onClick={(e) => e.stopPropagation()}>

        <div className="flex items-start justify-between gap-3 mb-3">
          <div className="min-w-0">
            <div className="flex items-baseline gap-2 flex-wrap">
              <span className="text-base font-semibold text-fg-strong">Performance</span>
              {basket
                ? <span className="text-sm text-fg-soft truncate">{title} · group</span>
                : <>
                    <span className="text-sm font-mono text-fg-soft">{isin}</span>
                    {name && <span className="text-sm text-fg-soft truncate">{name}</span>}
                  </>}
            </div>
            <div className="text-[11px] text-fg-faint mt-0.5">
              {basket
                ? `Value-weighted EUR index of ${basket.holdings.length} holdings · trailing windows`
                : `Daily EUR returns${data?.symbol ? ` · ${data.symbol}` : ''} · trailing windows`}
            </div>
          </div>
          <button type="button" onClick={onClose} aria-label="Close"
            className="text-fg-faint hover:text-fg-strong text-xl leading-none px-1 -mt-1">×</button>
        </div>

        {error && (
          <div className="py-14 text-center max-w-md mx-auto space-y-2">
            <p className="text-sm text-fg-soft">No performance metrics for this instrument.</p>
            <p className="text-[11px] text-fg-faint">{error}</p>
          </div>
        )}

        {loading && <p className="text-[11px] text-fg-subtle py-14 text-center">Loading performance…</p>}

        {!loading && !error && windows.length > 0 && (
          <div className="space-y-2">
            <p className="text-[11px] text-fg-muted">
              Compare each metric across windows — when the recent regime (2Y) diverges sharply from
              the long run (8Y), the {basket ? 'sleeve' : "company"}&apos;s return/risk distribution has <strong>drifted</strong>.
            </p>
            <PerformanceTable windows={windows} asOf={asOf} />
          </div>
        )}
      </div>
    </div>
  );
}
