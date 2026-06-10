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
import { apiFetch } from '../apiFetch';
import { getCachedOrFetch, peekCache } from './fetchCache';
import { DEFAULT_FEE_CONFIG, type FeeConfig } from '../../app/components/momentum/feeModel';

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
  /** ISIN (International Securities Identification Number). Backfilled from
   * GuruFocus (`summary.company_data.isin`) + the Leonteq scrape. Nullable —
   * out-of-scope regions (AU/NZ/Russia/…) have no GuruFocus ISIN. */
  isin?: string | null;
  country?: string | null;
  delisted_at?: string | null;
  gurufocus_lookup_failed_at?: string | null;
  /** ISO timestamp set when an entry in
   * `backend/index_universe/gf_ticker_overrides.json` flags the
   * (ticker, exchange) pair as `{"unavailable": true, ...}` — meaning
   * we know the listing exists on a real exchange we deliberately
   * don't cover (e.g. Varta on Hamburg). UI renders an amber
   * OUT OF SCOPE badge with `out_of_scope_reason` in the tooltip. */
  out_of_scope_at?: string | null;
  out_of_scope_reason?: string | null;
};

export type SignalDef = {
  key: string;
  label: string;
  description: string;
  default_weight: number;
  group?: string;
};

export type ExchangeFeeRow = {
  exchange_code: string;
  fee_bps: number;
  is_broker_supported?: boolean;
};

// ─── Endpoint fetchers (private — go through the hooks below) ───────

const KEYS = {
  universeTemplates: 'GET /api/universe-templates',
  staticUniverses: 'GET /api/static-universes',
  benchmarks: 'GET /api/benchmarks',
  companies: 'GET /api/companies',
  momentumSignals: 'GET /api/momentum/signals',
  exchangeFees: 'GET /api/exchange-fees',
  feeConfig: 'GET /api/fee-config',
} as const;

async function _fetchUniverseTemplates(): Promise<UniverseTemplate[]> {
  const r = await apiFetch(`${API_URL}/api/universe-templates`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const data = (await r.json()) as UniverseTemplate[];
  // Templates that have never been refreshed (no captured months) are
  // filtered out — they'd render a dropdown row with no usable data.
  return data.filter((u) => u.earliest_captured_month && u.latest_captured_month);
}

async function _fetchStaticUniverses(): Promise<UniverseTemplate[]> {
  // Frozen snapshots (`/api/static-universes`). Same `_summary` shape as the
  // templates, with `template_key` carrying the snapshot's label (the value
  // the backtest sends as `index_universe`, resolved via the label fallback).
  const r = await apiFetch(`${API_URL}/api/static-universes`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const data = (await r.json()) as UniverseTemplate[];
  return data.filter((u) => u.earliest_captured_month && u.latest_captured_month);
}

async function _fetchBenchmarks(): Promise<Benchmark[]> {
  const r = await apiFetch(`${API_URL}/api/benchmarks`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return (await r.json()) as Benchmark[];
}

async function _fetchCompanies(): Promise<CompanyRow[]> {
  const r = await apiFetch(`${API_URL}/api/companies`);
  if (!r.ok) return [];
  return (await r.json()) as CompanyRow[];
}

async function _fetchMomentumSignals(): Promise<{ signals: SignalDef[]; categories: string[] }> {
  const r = await apiFetch(`${API_URL}/api/momentum/signals`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const d = await r.json();
  return {
    signals: (d.signals ?? []) as SignalDef[],
    categories: (d.categories ?? []) as string[],
  };
}

async function _fetchExchangeFees(): Promise<ExchangeFeeRow[]> {
  const r = await apiFetch(`${API_URL}/api/exchange-fees`);
  if (!r.ok) return [];
  return (await r.json()) as ExchangeFeeRow[];
}

async function _fetchFeeConfig(): Promise<FeeConfig> {
  const r = await apiFetch(`${API_URL}/api/fee-config`);
  if (!r.ok) return DEFAULT_FEE_CONFIG;
  const d = await r.json();
  return {
    leonteq_annual_bps: Number(d.leonteq_annual_bps ?? DEFAULT_FEE_CONFIG.leonteq_annual_bps),
    transaction_bps: Number(d.transaction_bps ?? DEFAULT_FEE_CONFIG.transaction_bps),
    bustelberg_mgmt_bps: Number(d.bustelberg_mgmt_bps ?? DEFAULT_FEE_CONFIG.bustelberg_mgmt_bps),
    bustelberg_perf_pct: Number(d.bustelberg_perf_pct ?? DEFAULT_FEE_CONFIG.bustelberg_perf_pct),
  };
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
export const useStaticUniverses = _buildHook(KEYS.staticUniverses, _fetchStaticUniverses);
export const useBenchmarks = _buildHook(KEYS.benchmarks, _fetchBenchmarks);
export const useCompanies = _buildHook(KEYS.companies, _fetchCompanies);
export const useMomentumSignals = _buildHook(KEYS.momentumSignals, _fetchMomentumSignals);
export const useExchangeFees = _buildHook(KEYS.exchangeFees, _fetchExchangeFees);
const _useFeeConfigRaw = _buildHook(KEYS.feeConfig, _fetchFeeConfig);

/**
 * The global fee config (Leonteq + Bustelberg parameters) backing the
 * backtest fee waterfall. Always returns a usable config — defaults until
 * the shared `/api/fee-config` fetch resolves — so callers never have to
 * null-check before computing the waterfall.
 */
export function useFeeConfig(): FeeConfig {
  const { data } = _useFeeConfigRaw();
  return data ?? DEFAULT_FEE_CONFIG;
}

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

/**
 * Derived hook: returns a `Map<company_id, isin>` built from the shared
 * `/api/companies` fetch. Used by the holdings table to show an ISIN
 * column without each holding row having to carry the field. Companies
 * with no ISIN (out-of-scope regions) are simply absent from the map.
 */
export function useCompanyIsinMap(): Map<number, string> {
  const { data } = useCompanies();
  return useMemo(() => {
    const m = new Map<number, string>();
    for (const c of data ?? []) {
      const isin = (c.isin ?? '').trim();
      if (isin) m.set(c.company_id, isin);
    }
    return m;
  }, [data]);
}
