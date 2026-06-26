'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { apiFetch } from '../../../lib/apiFetch';
import { trackedFetch } from '../../../lib/loading';
import type { BacktestStats, CorrelationResponse, OptimizeResponse } from '../../../lib/types/api';

/** A saved backtest as the list endpoint returns it (metadata + config). */
export type SavedBacktest = {
  run_id: number;
  name: string;
  created_at: string;
  config?: Record<string, unknown> | null;
};

/** An ETF candidate — stored as a `benchmark` row (reused infra). */
export type Etf = {
  benchmark_id: number;
  ticker: string;
  name: string;
  sector: string | null;
  price_from: string | null;
  price_to: string | null;
};

/**
 * All state + server calls for the diversifier page. The component stays
 * presentational. Covers: loading saved backtests + the ETF universe,
 * add/refresh/delete of ETFs (reusing the /api/benchmarks endpoints), and
 * the correlation run (incl. the variant-bundle prompt the backend raises
 * as a 400 carrying `available_variant_keys`).
 */
export function useDiversifier() {
  const [backtests, setBacktests] = useState<SavedBacktest[]>([]);
  const [etfs, setEtfs] = useState<Etf[]>([]);
  const [loadingLists, setLoadingLists] = useState(true);

  const [selectedRunId, setSelectedRunId] = useState<number | null>(null);
  const [variantKey, setVariantKey] = useState<string | null>(null);
  const [variantOptions, setVariantOptions] = useState<string[] | null>(null);
  const [backtestStats, setBacktestStats] = useState<BacktestStats | null>(null);
  const [selectedEtfIds, setSelectedEtfIds] = useState<Set<number>>(new Set());
  // History cutoff YEAR: keep only ETFs whose history starts BEFORE Jan 1 of
  // this year (first price < {year}-01-01) — young funds otherwise shorten the
  // optimizer's common window. Empty = no filter.
  const [cutoffYear, setCutoffYearState] = useState('2008');

  const [riskFreePct, setRiskFreePct] = useState(2);
  // Minimum weight held in the strategy itself; the ETF sleeve gets the rest
  // (cap = 100 − this). 0 = let the optimizer allocate freely.
  const [minStrategyPct, setMinStrategyPct] = useState(50);
  const [objective, setObjective] = useState<'sharpe' | 'sortino'>('sharpe');

  const [adding, setAdding] = useState(false);
  const [busyEtfId, setBusyEtfId] = useState<number | null>(null);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<CorrelationResponse | null>(null);
  const [optimizeResult, setOptimizeResult] = useState<OptimizeResponse | null>(null);
  const [optimizing, setOptimizing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadLists = useCallback(async () => {
    setLoadingLists(true);
    try {
      const [btRes, etfRes] = await Promise.all([
        apiFetch(`${API_URL}/api/momentum/backtests`),
        apiFetch(`${API_URL}/api/benchmarks`),
      ]);
      setBacktests(btRes.ok ? await btRes.json() : []);
      setEtfs(etfRes.ok ? await etfRes.json() : []);
    } catch {
      setError('Failed to load backtests / ETFs');
    }
    setLoadingLists(false);
  }, []);

  useEffect(() => {
    loadLists();
  }, [loadLists]);

  /** Fetch the backtest's saved headline stats (and discover variants) when a
   * run or variant is selected, so the baseline shows immediately. */
  const loadStrategyStats = useCallback(async (runId: number, vKey: string | null) => {
    try {
      const qs = vKey ? `?variant_key=${encodeURIComponent(vKey)}` : '';
      const res = await apiFetch(`${API_URL}/api/momentum/diversifier/strategy-stats/${runId}${qs}`);
      if (!res.ok) return;
      const stats: BacktestStats = await res.json();
      setBacktestStats(stats);
      // Variant bundle, no variant picked yet → surface the picker up front.
      if (stats.available_variant_keys && stats.available_variant_keys.length > 0) {
        setVariantOptions(stats.available_variant_keys);
      } else {
        setVariantOptions(null);
      }
    } catch {
      /* non-fatal — the baseline display is best-effort */
    }
  }, []);

  // ETF passes if its history starts strictly before Jan 1 of the cutoff year.
  const _passesCutoff = useCallback(
    (e: Etf, year: string) => !year || (!!e.price_from && e.price_from < `${year}-01-01`),
    [],
  );

  /** ETFs whose history starts before the cutoff year (or all, if no cutoff),
   * sorted alphabetically by ticker. (filter() returns a fresh array, so the
   * in-place sort doesn't mutate `etfs`.) */
  const visibleEtfs = useMemo(
    () => etfs.filter((e) => _passesCutoff(e, cutoffYear)).sort((a, b) => a.ticker.localeCompare(b.ticker)),
    [etfs, cutoffYear, _passesCutoff],
  );

  /** Set the cutoff year and drop any now-hidden ETFs from the selection so
   * the analysis always matches what's visible. */
  const setCutoffYear = useCallback(
    (v: string) => {
      const year = v.replace(/\D/g, '').slice(0, 4);   // digits only, max 4
      setCutoffYearState(year);
      setSelectedEtfIds((prev) => {
        const allowed = new Set(etfs.filter((e) => _passesCutoff(e, year)).map((e) => e.benchmark_id));
        return new Set([...prev].filter((id) => allowed.has(id)));
      });
    },
    [etfs, _passesCutoff],
  );

  const toggleEtf = useCallback((id: number) => {
    setSelectedEtfIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  /** Select every visible ETF, or clear if they're all already selected. */
  const toggleSelectAll = useCallback(() => {
    setSelectedEtfIds((prev) => {
      const allSelected = visibleEtfs.length > 0 && visibleEtfs.every((e) => prev.has(e.benchmark_id));
      return allSelected ? new Set() : new Set(visibleEtfs.map((e) => e.benchmark_id));
    });
  }, [visibleEtfs]);

  /** Add an ETF by ticker. Resolves the display name from GuruFocus first
   * (so the user only types a ticker), then creates the benchmark row. */
  const addEtf = useCallback(
    async (rawTicker: string) => {
      const ticker = rawTicker.trim().toUpperCase();
      if (!ticker) return;
      setAdding(true);
      setError(null);
      try {
        let name = ticker;
        try {
          const nameRes = await apiFetch(
            `${API_URL}/api/momentum/diversifier/resolve-name?ticker=${encodeURIComponent(ticker)}`,
          );
          if (nameRes.ok) name = (await nameRes.json()).name || ticker;
        } catch {
          /* name resolution is best-effort; fall back to the ticker */
        }
        const res = await trackedFetch(`Adding ${ticker}`, `${API_URL}/api/benchmarks`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ticker, name }),
        });
        if (!res.ok) {
          const data = await res.json().catch(() => ({}));
          throw new Error(typeof data.detail === 'string' ? data.detail : `HTTP ${res.status}`);
        }
        await loadLists();
      } catch (e) {
        setError(`Add failed: ${e instanceof Error ? e.message : e}`);
      }
      setAdding(false);
    },
    [loadLists],
  );

  const refreshEtf = useCallback(async (id: number) => {
    setBusyEtfId(id);
    setError(null);
    try {
      const res = await trackedFetch('Refreshing ETF prices', `${API_URL}/api/benchmarks/${id}/refresh`, {
        method: 'POST',
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(typeof data.detail === 'string' ? data.detail : `HTTP ${res.status}`);
      }
      // Refresh updates the price range; reload to reflect the new last date.
      const etfRes = await apiFetch(`${API_URL}/api/benchmarks`);
      if (etfRes.ok) setEtfs(await etfRes.json());
    } catch (e) {
      setError(`Refresh failed: ${e instanceof Error ? e.message : e}`);
    }
    setBusyEtfId(null);
  }, []);

  const deleteEtf = useCallback(async (id: number) => {
    setBusyEtfId(id);
    setError(null);
    try {
      const res = await trackedFetch('Deleting ETF', `${API_URL}/api/benchmarks/${id}`, { method: 'DELETE' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(typeof data.detail === 'string' ? data.detail : `HTTP ${res.status}`);
      }
      setEtfs((prev) => prev.filter((e) => e.benchmark_id !== id));
      setSelectedEtfIds((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
    } catch (e) {
      setError(`Delete failed: ${e instanceof Error ? e.message : e}`);
    }
    setBusyEtfId(null);
  }, []);

  const runCorrelation = useCallback(async () => {
    if (selectedRunId == null || selectedEtfIds.size === 0) return;
    setRunning(true);
    setError(null);
    try {
      const res = await trackedFetch('Computing correlations', `${API_URL}/api/momentum/diversifier/correlation`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          backtest_run_id: selectedRunId,
          benchmark_ids: [...selectedEtfIds],
          variant_key: variantKey,
          risk_free_rate_pct: riskFreePct,
          max_etf_weight_pct: 100 - minStrategyPct,
          objective,
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        // Variant-bundle backtest: the backend hands back the available
        // variant keys so we can show a picker and let the user re-run.
        const detail = data.detail;
        if (res.status === 400 && detail && Array.isArray(detail.available_variant_keys)) {
          setVariantOptions(detail.available_variant_keys as string[]);
          throw new Error('This backtest has multiple variants — pick one and run again.');
        }
        throw new Error(typeof detail === 'string' ? detail : `HTTP ${res.status}`);
      }
      setResult(await res.json());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setRunning(false);
  }, [selectedRunId, selectedEtfIds, variantKey, riskFreePct, minStrategyPct, objective]);

  /** Optimize weights across the strategy + all selected ETFs (portfolio
   * mode), maximizing the chosen objective subject to the ETF-sleeve cap. */
  const runOptimize = useCallback(async () => {
    if (selectedRunId == null || selectedEtfIds.size === 0) return;
    setOptimizing(true);
    setError(null);
    try {
      const res = await trackedFetch('Optimizing portfolio', `${API_URL}/api/momentum/diversifier/optimize`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          backtest_run_id: selectedRunId,
          benchmark_ids: [...selectedEtfIds],
          variant_key: variantKey,
          risk_free_rate_pct: riskFreePct,
          objective,
          max_total_etf_weight_pct: 100 - minStrategyPct,
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        const detail = data.detail;
        if (res.status === 400 && detail && Array.isArray(detail.available_variant_keys)) {
          setVariantOptions(detail.available_variant_keys as string[]);
          throw new Error('This backtest has multiple variants — pick one and run again.');
        }
        throw new Error(typeof detail === 'string' ? detail : `HTTP ${res.status}`);
      }
      setOptimizeResult(await res.json());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setOptimizing(false);
  }, [selectedRunId, selectedEtfIds, variantKey, riskFreePct, minStrategyPct, objective]);

  /** Switching the selected backtest invalidates any variant choice + results,
   * and loads the new backtest's baseline stats (+ variant list). */
  const selectBacktest = useCallback((runId: number | null) => {
    setSelectedRunId(runId);
    setVariantKey(null);
    setVariantOptions(null);
    setResult(null);
    setOptimizeResult(null);
    setBacktestStats(null);
    if (runId != null) loadStrategyStats(runId, null);
  }, [loadStrategyStats]);

  /** Picking a variant re-loads the baseline for that variant. */
  const selectVariant = useCallback((vKey: string | null) => {
    setVariantKey(vKey);
    setResult(null);
    setOptimizeResult(null);
    if (selectedRunId != null && vKey) loadStrategyStats(selectedRunId, vKey);
  }, [selectedRunId, loadStrategyStats]);

  return {
    // data
    backtests,
    etfs,
    visibleEtfs,
    cutoffYear,
    setCutoffYear,
    loadingLists,
    result,
    optimizeResult,
    error,
    // selection
    selectedRunId,
    selectBacktest,
    backtestStats,
    variantKey,
    selectVariant,
    variantOptions,
    selectedEtfIds,
    toggleEtf,
    toggleSelectAll,
    // params
    riskFreePct,
    setRiskFreePct,
    minStrategyPct,
    setMinStrategyPct,
    objective,
    setObjective,
    // actions
    addEtf,
    adding,
    refreshEtf,
    deleteEtf,
    busyEtfId,
    runCorrelation,
    running,
    runOptimize,
    optimizing,
  };
}
