'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { apiFetch } from '../../../lib/apiFetch';
import { trackedFetch } from '../../../lib/loading';
import type { BacktestStats, CorrelationResponse, OptimizeResponse, PortfolioStateResponse, SavedPortfolio } from '../../../lib/types/api';

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
  // Selected funds (bonds and ETFs are treated identically — all diversifiers).
  const [selectedEtfIds, setSelectedEtfIds] = useState<Set<number>>(new Set());
  // History cutoff YEAR: keep only ETFs whose history starts BEFORE Jan 1 of
  // this year (first price < {year}-01-01) — young funds otherwise shorten the
  // optimizer's common window. Empty = no filter.
  const [cutoffYear, setCutoffYearState] = useState('2008');

  const [riskFreePct, setRiskFreePct] = useState(2);
  // Strategy is held at this weight; the diversifier sleeve (1 − this) is
  // optimized across the selected funds. Reset to target whenever the strategy
  // drifts more than `rebalanceBandPct` away (above or below).
  const [startStrategyPct, setStartStrategyPct] = useState(60);
  const [rebalanceBandPct, setRebalanceBandPct] = useState(10);
  const [objective, setObjective] = useState<'sharpe' | 'sortino'>('sharpe');

  const [adding, setAdding] = useState(false);
  const [busyEtfId, setBusyEtfId] = useState<number | null>(null);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<CorrelationResponse | null>(null);
  const [optimizeResult, setOptimizeResult] = useState<OptimizeResponse | null>(null);
  const [optimizing, setOptimizing] = useState(false);
  // Manual-portfolio backtest (the section below the optimizer): per-holding
  // target weight + rebalance band, keyed by "strategy" or String(benchmark_id).
  const [manualWeights, setManualWeights] = useState<Record<string, { weight: number; band: number }>>({});
  const [manualResult, setManualResult] = useState<OptimizeResponse | null>(null);
  const [simulating, setSimulating] = useState(false);
  // Saved diversified portfolios + the currently-viewed one's live state.
  const [savedPortfolios, setSavedPortfolios] = useState<SavedPortfolio[]>([]);
  const [portfolioState, setPortfolioState] = useState<PortfolioStateResponse | null>(null);
  const [savingPortfolio, setSavingPortfolio] = useState(false);
  // Scheduled strategies — the live bases a portfolio can be scheduled against.
  const [scheduledStrategies, setScheduledStrategies] = useState<{ id: number; name: string | null }[]>([]);
  const [error, setError] = useState<string | null>(null);

  const loadSavedPortfolios = useCallback(async () => {
    try {
      const [pRes, sRes] = await Promise.all([
        apiFetch(`${API_URL}/api/momentum/diversifier/portfolios`),
        apiFetch(`${API_URL}/api/scheduled-strategies`),
      ]);
      if (pRes.ok) setSavedPortfolios(await pRes.json());
      if (sRes.ok) {
        const list = await sRes.json();
        setScheduledStrategies((list as { id: number; name: string | null }[]).map((s) => ({ id: s.id, name: s.name })));
      }
    } catch { /* non-fatal */ }
  }, []);

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
    loadSavedPortfolios();
  }, [loadSavedPortfolios]);

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

  /** Set the cutoff year and drop any now-hidden funds from the selection so
   * the analysis always matches what's visible. */
  const setCutoffYear = useCallback(
    (v: string) => {
      const year = v.replace(/\D/g, '').slice(0, 4);   // digits only, max 4
      setCutoffYearState(year);
      const allowed = new Set(etfs.filter((e) => _passesCutoff(e, year)).map((e) => e.benchmark_id));
      setSelectedEtfIds((prev) => new Set([...prev].filter((id) => allowed.has(id))));
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

  /** Select every visible fund, or clear if they're all already selected. */
  const toggleSelectAll = useCallback(() => {
    setSelectedEtfIds((prev) => {
      const allSelected = visibleEtfs.length > 0 && visibleEtfs.every((e) => prev.has(e.benchmark_id));
      return allSelected ? new Set() : new Set(visibleEtfs.map((e) => e.benchmark_id));
    });
  }, [visibleEtfs]);

  /** Add a benchmark by ticker (resolving its name from GuruFocus first). */
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
      setSelectedEtfIds((prev) => { const n = new Set(prev); n.delete(id); return n; });
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
          max_etf_weight_pct: 100 - startStrategyPct,
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
  }, [selectedRunId, selectedEtfIds, variantKey, riskFreePct, startStrategyPct, objective]);

  /** Optimize the strategy + diversifier sleeve, maximizing the objective with
   * a drift-rebalance band. */
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
          core_weight_pct: startStrategyPct,
          rebalance_band_pct: rebalanceBandPct,
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
  }, [selectedRunId, selectedEtfIds, variantKey, riskFreePct, startStrategyPct, rebalanceBandPct, objective]);

  /** Set one holding's target weight or band (manual-portfolio section). */
  const setManualField = useCallback((key: string, field: 'weight' | 'band', val: number) => {
    setManualWeights((prev) => ({
      ...prev,
      [key]: { weight: prev[key]?.weight ?? 0, band: prev[key]?.band ?? 10, [field]: val },
    }));
  }, []);

  /** Default manual weights: strategy at the start %, and the ETF sleeve
   * (100 − start) split across the selected funds — by the optimizer's
   * proportions when available, else evenly — each ROUNDED to the nearest 10
   * while still summing to the sleeve (largest-remainder apportionment). e.g.
   * GLD 22 / UUP 18 → 20 / 20. Bands default to 10. These show in the inputs;
   * user edits (manualWeights) override per field. */
  const manualDefaults = useMemo(() => {
    const out: Record<string, { weight: number; band: number }> = {
      strategy: { weight: startStrategyPct, band: 10 },
    };
    const ids = [...selectedEtfIds];
    if (ids.length === 0) return out;
    const units = Math.max(0, Math.round((100 - startStrategyPct) / 10));   // 10%-blocks for the sleeve
    const tickerToId = new Map(etfs.map((e) => [e.ticker, e.benchmark_id]));
    // Proportions: optimizer weights if present (and non-zero), else even.
    let props = ids.map((id) => ({ id, w: 0 }));
    if (optimizeResult) {
      const byId = new Map<number, number>();
      for (const w of optimizeResult.weights) {
        if (w.group === 'strategy') continue;
        const id = tickerToId.get(w.label);
        if (id != null) byId.set(id, w.weight);
      }
      props = ids.map((id) => ({ id, w: byId.get(id) ?? 0 }));
    }
    if (props.every((p) => p.w === 0)) props = ids.map((id) => ({ id, w: 1 }));
    // Largest-remainder: hand out `units` 10%-blocks by proportion.
    const total = props.reduce((a, p) => a + p.w, 0) || 1;
    const exact = props.map((p) => (p.w / total) * units);
    const alloc = exact.map((x) => Math.floor(x));
    let rem = units - alloc.reduce((a, b) => a + b, 0);
    const order = exact.map((x, i) => ({ i, frac: x - Math.floor(x) })).sort((a, b) => b.frac - a.frac);
    for (let k = 0; rem > 0 && order.length; k++, rem--) alloc[order[k % order.length].i] += 1;
    props.forEach((p, i) => { out[String(p.id)] = { weight: alloc[i] * 10, band: 10 }; });
    return out;
  }, [startStrategyPct, selectedEtfIds, optimizeResult, etfs]);

  /** Clear all manual overrides (revert the inputs to the defaults). */
  const resetManualWeights = useCallback(() => setManualWeights({}), []);

  /** Effective value for a holding (user override ?? default). */
  const manualVal = useCallback(
    (key: string, field: 'weight' | 'band') =>
      manualWeights[key]?.[field] ?? manualDefaults[key]?.[field] ?? (field === 'band' ? 10 : 0),
    [manualWeights, manualDefaults],
  );

  /** The holdings payload (strategy + selected funds) for save/simulate. */
  const buildHoldings = useCallback(() => [
    { benchmark_id: null, weight_pct: manualVal('strategy', 'weight'), band_pct: manualVal('strategy', 'band') },
    ...[...selectedEtfIds].map((id) => ({
      benchmark_id: id,
      weight_pct: manualVal(String(id), 'weight'),
      band_pct: manualVal(String(id), 'band'),
    })),
  ], [manualVal, selectedEtfIds]);

  /** Backtest the hand-specified portfolio (strategy + selected funds). */
  const runSimulate = useCallback(async () => {
    if (selectedRunId == null) return;
    setSimulating(true);
    setError(null);
    try {
      const res = await trackedFetch('Backtesting portfolio', `${API_URL}/api/momentum/diversifier/simulate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          backtest_run_id: selectedRunId,
          variant_key: variantKey,
          risk_free_rate_pct: riskFreePct,
          holdings: buildHoldings(),
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
      setManualResult(await res.json());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setSimulating(false);
  }, [selectedRunId, variantKey, riskFreePct, buildHoldings]);

  /** Save the current manual portfolio (strategy + selected funds + bands). */
  const savePortfolio = useCallback(async (name: string) => {
    if (selectedRunId == null || !name.trim()) return;
    setSavingPortfolio(true);
    setError(null);
    try {
      const res = await trackedFetch('Saving portfolio', `${API_URL}/api/momentum/diversifier/portfolios`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: name.trim(),
          backtest_run_id: selectedRunId,
          variant_key: variantKey,
          risk_free_rate_pct: riskFreePct,
          holdings: buildHoldings(),
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(typeof data.detail === 'string' ? data.detail : `HTTP ${res.status}`);
      }
      await loadSavedPortfolios();
    } catch (e) {
      setError(`Save failed: ${e instanceof Error ? e.message : e}`);
    }
    setSavingPortfolio(false);
  }, [selectedRunId, variantKey, riskFreePct, buildHoldings, loadSavedPortfolios]);

  /** Schedule the current overlay LIVE against a chosen scheduled strategy
   * (shows on /schedule, tracks live). */
  const scheduleLivePortfolio = useCallback(async (name: string, scheduledStrategyId: number) => {
    if (!name.trim() || !scheduledStrategyId) return;
    setSavingPortfolio(true);
    setError(null);
    try {
      const res = await trackedFetch('Scheduling portfolio', `${API_URL}/api/momentum/diversifier/portfolios`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: name.trim(),
          scheduled_strategy_id: scheduledStrategyId,
          risk_free_rate_pct: riskFreePct,
          holdings: buildHoldings(),
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(typeof data.detail === 'string' ? data.detail : `HTTP ${res.status}`);
      }
      await loadSavedPortfolios();
    } catch (e) {
      setError(`Schedule failed: ${e instanceof Error ? e.message : e}`);
    }
    setSavingPortfolio(false);
  }, [riskFreePct, buildHoldings, loadSavedPortfolios]);

  const deletePortfolio = useCallback(async (id: number) => {
    setError(null);
    try {
      const res = await trackedFetch('Deleting portfolio', `${API_URL}/api/momentum/diversifier/portfolios/${id}`, { method: 'DELETE' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setSavedPortfolios((prev) => prev.filter((p) => p.id !== id));
      setPortfolioState((prev) => (prev?.id === id ? null : prev));
    } catch (e) {
      setError(`Delete failed: ${e instanceof Error ? e.message : e}`);
    }
  }, []);

  /** Fetch a saved portfolio's live state (current weights + rebalance-needed). */
  const viewPortfolioState = useCallback(async (id: number) => {
    setError(null);
    try {
      const res = await trackedFetch('Loading portfolio state', `${API_URL}/api/momentum/diversifier/portfolios/${id}/state`);
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(typeof data.detail === 'string' ? data.detail : `HTTP ${res.status}`);
      }
      setPortfolioState(await res.json());
    } catch (e) {
      setError(`Couldn't load state: ${e instanceof Error ? e.message : e}`);
    }
  }, []);

  /** Switching the selected backtest invalidates any variant choice + results,
   * and loads the new backtest's baseline stats (+ variant list). */
  const selectBacktest = useCallback((runId: number | null) => {
    setSelectedRunId(runId);
    setVariantKey(null);
    setVariantOptions(null);
    setResult(null);
    setOptimizeResult(null);
    setManualResult(null);
    setBacktestStats(null);
    if (runId != null) loadStrategyStats(runId, null);
  }, [loadStrategyStats]);

  /** Picking a variant re-loads the baseline for that variant. */
  const selectVariant = useCallback((vKey: string | null) => {
    setVariantKey(vKey);
    setResult(null);
    setOptimizeResult(null);
    setManualResult(null);
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
    startStrategyPct,
    setStartStrategyPct,
    rebalanceBandPct,
    setRebalanceBandPct,
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
    // manual portfolio backtest
    manualWeights,
    setManualField,
    manualDefaults,
    resetManualWeights,
    runSimulate,
    simulating,
    manualResult,
    // saved portfolios
    savedPortfolios,
    savePortfolio,
    savingPortfolio,
    deletePortfolio,
    viewPortfolioState,
    portfolioState,
    scheduledStrategies,
    scheduleLivePortfolio,
  };
}
