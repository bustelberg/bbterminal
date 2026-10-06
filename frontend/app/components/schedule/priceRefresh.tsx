'use client';

import { useCallback, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { startLocalJob } from '../../../lib/stores/jobs';

/** The compact result of a single-stock GuruFocus price refresh
 * (`POST /api/admin/company-price-refresh`). */
export type PriceRefreshResult = {
  ticker: string;
  exchange: string;
  resolved_exchange: string | null;
  request: { method: string; url: string | null; symbol: string };
  response: {
    http_status: number | null;
    source: string;
    points: number;
    excerpt: string | null;
    error: string | null;
    is_delisted: boolean;
    is_forbidden: boolean;
  };
  db: { rows_loaded: number; latest_before: string | null; latest_after: string | null; advanced: boolean };
  /** The 2 newest + 2 oldest close dates returned by the fetch. */
  dates: { newest: string[]; oldest: string[] };
  api_calls: number;
  logs: string[];
  repriced: boolean;
};

/** State + action for refreshing individual stocks' prices, keyed by
 * company_id. `refresh` calls the admin endpoint (bypassing cache), records the
 * compact request/response, and — when the fetch succeeded — invokes
 * `onSuccess` so the caller can reload dependent views. Shared by the /schedule
 * Price-update held table and the Current-portfolio card. */
export function useStockRefresh(onSuccess?: () => void | Promise<void>) {
  const [refreshing, setRefreshing] = useState<Set<number>>(new Set());
  const [results, setResults] = useState<Map<number, PriceRefreshResult | { error: string }>>(new Map());

  const refresh = useCallback((companyId: number, strategyId?: number | null) => {
    startLocalJob("Refreshing price", "portfolio-price-refresh", async (signal, report) => {
      setRefreshing((s) => new Set(s).add(companyId));
      report({ done: 0, total: 0, message: "Fetching the latest market price…" });
      try {
        const r = await apiFetch(`${API_URL}/api/admin/company-price-refresh`, {
          method: 'POST', signal,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ company_id: companyId, strategy_id: strategyId ?? null }),
        });
        const body = await r.json().catch(() => null);
        if (!r.ok) {
          const message = body?.detail ?? `HTTP ${r.status}`;
          setResults((m) => new Map(m).set(companyId, { error: message }));
          throw new Error(message);
        }
        const result = body as PriceRefreshResult;
        setResults((m) => new Map(m).set(companyId, result));
        await onSuccess?.();
        report({ done: 1, total: 1, message: "Price data updated" });
        return result.db.advanced ? "Price data updated" : "Price data already current";
      } finally {
        setRefreshing((s) => { const n = new Set(s); n.delete(companyId); return n; });
      }
    }, { cancellable: false });
  }, [onSuccess]);

  const clear = useCallback((companyId: number) => {
    setResults((m) => { const n = new Map(m); n.delete(companyId); return n; });
  }, []);

  return { refreshing, results, refresh, clear };
}

/** Single-line request/response readout for a single-stock refresh: the actual
 * GuruFocus GET that was issued and the 2 newest + 2 oldest close dates it
 * returned. */
export function PriceRefreshPanel({ result, onClose }: { result: PriceRefreshResult | { error: string }; onClose: () => void }) {
  if ('error' in result) {
    return (
      <div className="flex items-center justify-between gap-2 text-[12px] font-mono text-neg-300">
        <span className="truncate">Refresh failed: {result.error}</span>
        <button type="button" onClick={onClose} className="text-fg-faint hover:text-fg-soft shrink-0"></button>
      </div>
    );
  }
  const { request, response } = result;
  const newest = result.dates?.newest ?? [];
  const oldest = result.dates?.oldest ?? [];
  const url = request.url ?? request.symbol;
  const datesLabel = response.error
    ? response.error
    : (newest.length || oldest.length)
      ? `${newest.join(', ') || '—'} … ${oldest.join(', ') || '—'}`
      : `no dates (${response.source})`;
  return (
    <div className="flex items-center gap-2 text-[12px] font-mono">
      <span className="text-fg-faint shrink-0">{request.method}</span>
      <span className="text-fg-muted truncate min-w-0" title={url}>{url}</span>
      <span className="text-fg-faint shrink-0">·</span>
      <span className={`shrink-0 ${response.error ? 'text-neg-400' : 'text-fg-soft'}`}>{datesLabel}</span>
      <button type="button" onClick={onClose} className="text-fg-faint hover:text-fg-soft shrink-0 ml-auto"></button>
    </div>
  );
}
