/**
 * Shared data-fetching hooks for endpoints fetched by multiple components.
 *
 * Each hook returns `{ data, loading, error }` and is backed by the
 * module-level cache in `fetchCache.ts` — 5-min TTL, in-flight
 * deduplication, synchronous cache peek on mount. Callers don't need
 * their own useEffect / cancel tokens / loading state.
 *
 * Previously each of these endpoints was fetched via inline
 * `useEffect(() => fetch(…))` in 2+ components, each with its own
 * cancel-token + loading flag + error state (~15 lines per call site).
 */
import { useEffect, useMemo, useState } from 'react';
import { API_URL } from '../apiUrl';
import { getCachedOrFetch, peekCache } from './fetchCache';

// Shape returned by `GET /api/universe-templates`.
export type UniverseTemplate = {
  template_key: string;
  label?: string;
  earliest_date: string;
  earliest_captured_month: string | null;
  latest_captured_month: string | null;
  months_captured: number;
  latest_membership_count: number;
};

export type Benchmark = {
  benchmark_id: number;
  ticker: string;
  name?: string | null;
  sector?: string | null;
  currency?: string | null;
};

export type CompanyRow = {
  company_id: number;
  company_name?: string | null;
  gurufocus_ticker?: string | null;
  gurufocus_exchange?: string | null;
  country?: string | null;
  delisted_at?: string | null;
};

export type SignalDef = {
  key: string;
  name: string;
  group?: string;
  default_weight: number;
};

export type ExchangeFeeRow = {
  exchange_code: string;
  fee_bps: number;
  is_broker_supported?: boolean;
};

// ─── Endpoint fetchers (private — go through the hooks below) ───────

const KEYS = {
  universeTemplates: 'GET /api/universe-templates',
  benchmarks: 'GET /api/benchmarks',
  companies: 'GET /api/companies',
  momentumSignals: 'GET /api/momentum/signals',
  exchangeFees: 'GET /api/exchange-fees',
} as const;

async function _fetchUniverseTemplates(): Promise<UniverseTemplate[]> {
  const r = await fetch(`${API_URL}/api/universe-templates`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const data = (await r.json()) as UniverseTemplate[];
  // Templates that have never been refreshed (no captured months) are
  // filtered out — they'd render a dropdown row with no usable data.
  return data.filter((u) => u.earliest_captured_month && u.latest_captured_month);
}

async function _fetchBenchmarks(): Promise<Benchmark[]> {
  const r = await fetch(`${API_URL}/api/benchmarks`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return (await r.json()) as Benchmark[];
}

async function _fetchCompanies(): Promise<CompanyRow[]> {
  const r = await fetch(`${API_URL}/api/companies`);
  if (!r.ok) return [];
  return (await r.json()) as CompanyRow[];
}

async function _fetchMomentumSignals(): Promise<{ signals: SignalDef[]; categories: string[] }> {
  const r = await fetch(`${API_URL}/api/momentum/signals`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const d = await r.json();
  return {
    signals: (d.signals ?? []) as SignalDef[],
    categories: (d.categories ?? []) as string[],
  };
}

async function _fetchExchangeFees(): Promise<ExchangeFeeRow[]> {
  const r = await fetch(`${API_URL}/api/exchange-fees`);
  if (!r.ok) return [];
  return (await r.json()) as ExchangeFeeRow[];
}

// ─── Generic hook builder ───────────────────────────────────────────

type HookResult<T> = { data: T | null; loading: boolean; error: string | null };

/** Build a cached-fetch hook for a given key + fetcher. Generic so the
 * specifics below stay one-liners. */
function _buildHook<T>(
  key: string,
  fetcher: () => Promise<T>,
): (opts?: { enabled?: boolean }) => HookResult<T> {
  return function useCachedFetch(opts) {
    const enabled = opts?.enabled ?? true;
    const initial = enabled ? peekCache<T>(key) : null;
    const [data, setData] = useState<T | null>(initial);
    const [loading, setLoading] = useState(enabled && initial == null);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
      if (!enabled) return;
      let cancelled = false;
      const cached = peekCache<T>(key);
      if (cached) {
        setData(cached);
        setLoading(false);
        return () => { cancelled = true; };
      }
      setLoading(true);
      setError(null);
      getCachedOrFetch(key, fetcher)
        .then((d) => { if (!cancelled) setData(d); })
        .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : String(e)); })
        .finally(() => { if (!cancelled) setLoading(false); });
      return () => { cancelled = true; };
    }, [enabled]);

    return { data, loading, error };
  };
}

// ─── Public hooks ───────────────────────────────────────────────────

export const useUniverseTemplates = _buildHook(KEYS.universeTemplates, _fetchUniverseTemplates);
export const useBenchmarks = _buildHook(KEYS.benchmarks, _fetchBenchmarks);
export const useCompanies = _buildHook(KEYS.companies, _fetchCompanies);
export const useMomentumSignals = _buildHook(KEYS.momentumSignals, _fetchMomentumSignals);
export const useExchangeFees = _buildHook(KEYS.exchangeFees, _fetchExchangeFees);

/**
 * Derived hook: `Map<exchange_code, fee_bps>` built once from the shared
 * `/api/exchange-fees` fetch and used by every per-trade-fee net-stat
 * calculation in the momentum result rendering (EquityCurveCard,
 * MonthlyHoldingsTable, VariantSummaryTable). Returns null when no
 * non-zero fees are configured so callers can short-circuit
 * `(gross / net)` parentheticals and render the gross stat alone.
 */
export function useExchangeFeeMap(): Map<string, number> | null {
  const { data } = useExchangeFees();
  return useMemo(() => {
    if (!data) return null;
    const m = new Map<string, number>();
    for (const r of data) {
      if (r.exchange_code && r.fee_bps > 0) m.set(r.exchange_code, r.fee_bps);
    }
    return m.size > 0 ? m : null;
  }, [data]);
}

/**
 * Derived hook: returns a `Map<company_id, exchange_code>` built from the
 * shared `/api/companies` fetch. Two components currently want exactly
 * this shape (MomentumBacktester for holdings-table exchange labels +
 * SnapshotHoldings for the same on saved-snapshot views). Saves them
 * from duplicating the loop.
 */
export function useCompanyExchangeMap(): Map<number, string> {
  const { data } = useCompanies();
  return useMemo(() => {
    const m = new Map<number, string>();
    for (const c of data ?? []) {
      const exch = (c.gurufocus_exchange ?? '').trim();
      if (exch) m.set(c.company_id, exch);
    }
    return m;
  }, [data]);
}
