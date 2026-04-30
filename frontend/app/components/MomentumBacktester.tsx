'use client';

import { Fragment, useState, useEffect, useMemo, useRef } from 'react';

import ApiUsageBadge from './ApiUsageBadge';
import { dialog } from '../../lib/dialog';
import ProgressTimeline from './ProgressTimeline';
import {
  momentumStore,
  startBacktest,
  cancelBacktest,
  loadCurrentPicksSnapshots,
  loadCurrentPicksSnapshot,
  refreshCurrentPicksMTD,
  type BacktestStartConfig,
} from '../../lib/stores/momentum';
import CellInfoTip from './momentum/CellInfoTip';
import DailyPicksHistory from './momentum/DailyPicksHistory';
import EquityCurveCard from './momentum/EquityCurveCard';
import {
  EXCHANGE_NAMES,
  fmtPct,
  fmtPrice,
  guruFocusUrl,
} from './momentum/utils';
import type {
  SavedRun,
  SignalDef,
} from './momentum/types';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function MomentumBacktester() {
  // Signal definitions from backend
  const [signalDefs, setSignalDefs] = useState<SignalDef[]>([]);
  const [weights, setWeights] = useState<Record<string, number>>({});
  const [categories, setCategories] = useState<string[]>([]);
  const [categoryWeights, setCategoryWeights] = useState<Record<string, number>>({});

  // Config
  const currentYear = new Date().getFullYear();
  const [startDate, setStartDate] = useState('2017-01');
  const [endDate, setEndDate] = useState(`${currentYear}-01`);
  const [topSectors, setTopSectors] = useState(4);
  const [topPerSector, setTopPerSector] = useState(6);
  const [noCache, setNoCache] = useState(false);
  const [maxCompanies, setMaxCompanies] = useState(0);
  const [selectionMode, setSelectionMode] = useState<'momentum' | 'random'>('momentum');
  const [randomSeed, setRandomSeed] = useState<number>(42);
  const [nTrials, setNTrials] = useState<number>(1);

  // Backtest run state lives in a module-scoped store so the SSE stream
  // keeps running when the user navigates away from /momentum.
  const running = momentumStore.use((s) => s.running);
  const progress = momentumStore.use((s) => s.progress);
  const result = momentumStore.use((s) => s.result);
  const currentPortfolio = momentumStore.use((s) => s.currentPortfolio);
  const currentPicksSnapshots = momentumStore.use((s) => s.currentPicksSnapshots);
  const refreshingMTD = momentumStore.use((s) => s.refreshingMTD);
  const universe = momentumStore.use((s) => s.universe);
  const error = momentumStore.use((s) => s.error);
  const warnings = momentumStore.use((s) => s.warnings);
  const infos = momentumStore.use((s) => s.infos);
  const loadedRunId = momentumStore.use((s) => s.loadedRunId);

  const exchangeByCompany = useMemo(() => {
    const m = new Map<number, string>();
    for (const u of universe) m.set(u.company_id, u.exchange);
    return m;
  }, [universe]);

  // Purely local UI state — safe to reset on navigation
  const [showWarnings, setShowWarnings] = useState(true);
  const [showInfos, setShowInfos] = useState(false);
  const [expandedMonth, setExpandedMonth] = useState<string | null>(null);

  // Save/load state
  const [savedRuns, setSavedRuns] = useState<SavedRun[]>([]);
  const [saveName, setSaveName] = useState('');
  const [saving, setSaving] = useState(false);

  const [savedDropdownOpen, setSavedDropdownOpen] = useState(false);
  const savedDropdownRef = useRef<HTMLDivElement>(null);

  // Universe selection state — all universes live in the same table and are served
  // by /api/index-universe/indexes with enriched metadata.
  const [indexUniverses, setIndexUniverses] = useState<{ index_name: string; start_month: string; end_month: string; month_count: number; total_unique_tickers: number }[]>([]);
  const [selectedIndexUniverse, setSelectedIndexUniverse] = useState<string>('');
  const [universesLoading, setUniversesLoading] = useState(true);
  const [universesError, setUniversesError] = useState<string | null>(null);
  const [universesElapsed, setUniversesElapsed] = useState(0);

  // Load signal definitions + saved runs
  useEffect(() => {
    fetch(`${API_URL}/api/momentum/signals`)
      .then((r) => r.json())
      .then((d) => {
        const defs: SignalDef[] = d.signals ?? [];
        setSignalDefs(defs);
        const w: Record<string, number> = {};
        defs.forEach((s) => (w[s.key] = s.default_weight));
        setWeights(w);
        const cats: string[] = d.categories ?? [];
        setCategories(cats);
        const cw: Record<string, number> = {};
        cats.forEach((c) => (cw[c] = 50));
        setCategoryWeights(cw);
      })
      .catch(() => {});
    loadSavedRuns();
    loadCurrentPicksSnapshots();
    const universesStart = Date.now();
    const tick = setInterval(() => setUniversesElapsed(Math.round((Date.now() - universesStart) / 1000)), 500);
    setUniversesLoading(true);
    setUniversesError(null);
    fetch(`${API_URL}/api/index-universe/indexes`)
      .then((r) => {
        if (!r.ok) throw new Error(`${r.status}`);
        return r.json();
      })
      .then((data) => setIndexUniverses(data))
      .catch((e) => setUniversesError(e instanceof Error ? e.message : String(e)))
      .finally(() => {
        clearInterval(tick);
        setUniversesLoading(false);
      });
    return () => clearInterval(tick);
  }, []);

  // When universe selection changes, auto-set start/end dates from the range.
  // Some universes store start/end as YYYY-MM-DD instead of YYYY-MM — slice so
  // the <input type="month"> can accept the value.
  const handleUniverseChange = (value: string) => {
    setSelectedIndexUniverse(value);
    if (value) {
      const entry = indexUniverses.find(i => i.index_name === value);
      if (entry) {
        setStartDate(entry.start_month.slice(0, 7));
        setEndDate(entry.end_month.slice(0, 7));
      }
    }
  };

  const universeDropdownValue = selectedIndexUniverse;

  useEffect(() => {
    if (!savedDropdownOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (savedDropdownRef.current && !savedDropdownRef.current.contains(e.target as Node)) {
        setSavedDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [savedDropdownOpen]);

  const loadSavedRuns = () => {
    fetch(`${API_URL}/api/momentum/backtests`)
      .then((r) => r.json())
      .then((data) => setSavedRuns(data))
      .catch(() => {});
  };

  // One-way turnover per month: % of current holdings that weren't held last month.
  // First month has no prior portfolio → null.
  const turnoverByDate = useMemo<Record<string, number | null>>(() => {
    const map: Record<string, number | null> = {};
    if (!result) return map;
    let prevIds: Set<number> | null = null;
    for (const r of result.monthly_records) {
      const currIds = new Set(r.holdings.map(h => h.company_id));
      if (prevIds === null || currIds.size === 0) {
        map[r.date] = null;
      } else {
        let added = 0;
        for (const id of currIds) if (!prevIds.has(id)) added += 1;
        map[r.date] = (added / currIds.size) * 100;
      }
      prevIds = currIds;
    }
    return map;
  }, [result]);


  // Run backtest — delegates to the module-scoped momentumStore, which owns
  // the fetch/reader loop so it survives navigation away from /momentum.
  const runBacktest = () => {
    setExpandedMonth(null);
    return startBacktest({
      start_date: `${startDate}-01`,
      end_date: `${endDate}-01`,
      signal_weights: weights,
      category_weights: categoryWeights,
      top_n_sectors: topSectors,
      top_n_per_sector: topPerSector,
      max_companies: maxCompanies,
      universe_label: null,
      index_universe: selectedIndexUniverse || null,
      selection_mode: selectionMode,
      random_seed: selectionMode === 'random' ? randomSeed : null,
      n_trials: selectionMode === 'random' ? Math.max(1, nTrials) : 1,
      force_recompute: noCache,
    });
  };

  const _currentPortfolioConfig = (force: boolean): BacktestStartConfig => ({
    start_date: `${startDate}-01`,
    end_date: `${endDate}-01`,
    signal_weights: weights,
    category_weights: categoryWeights,
    top_n_sectors: topSectors,
    top_n_per_sector: topPerSector,
    max_companies: maxCompanies,
    universe_label: null,
    index_universe: selectedIndexUniverse || null,
    selection_mode: 'momentum',
    random_seed: null,
    n_trials: 1,
    mode: 'current_portfolio',
    force_recompute: force,
  });

  // Hit the backend for "what is my strategy holding right now?". The backend
  // serves from cache if (this strategy, this month) is already stored.
  // Random mode is unsupported here.
  const showCurrentPicks = async () => {
    await startBacktest(_currentPortfolioConfig(false));
    loadCurrentPicksSnapshots();
  };

  // Force a fresh full compute. Slow (signals + scoring + price fetch),
  // but persists a new snapshot in the DB so future loads are instant.
  const recomputeCurrentPortfolio = async () => {
    await startBacktest(_currentPortfolioConfig(true));
    loadCurrentPicksSnapshots();
  };

  const saveBacktest = async () => {
    if (!result || !saveName.trim()) return;
    setSaving(true);
    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtests`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: saveName.trim(),
          config: {
            start_date: `${startDate}-01`,
            end_date: `${endDate}-01`,
            signal_weights: weights,
            category_weights: categoryWeights,
            top_n_sectors: topSectors,
            top_n_per_sector: topPerSector,
            universe_label: null,
            index_universe: selectedIndexUniverse || null,
            selection_mode: selectionMode,
            random_seed: selectionMode === 'random' ? randomSeed : null,
            n_trials: selectionMode === 'random' ? Math.max(1, nTrials) : 1,
          },
          summary: result.summary,
          monthly_records: result.monthly_records,
          universe,
        }),
      });
      if (resp.ok) {
        const saved = await resp.json();
        setSaveName('');
        momentumStore.set({ loadedRunId: saved.run_id });
        loadSavedRuns();
      }
    } catch {}
    setSaving(false);
  };

  const loadBacktest = async (runId: number) => {
    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtests/${runId}`);
      if (!resp.ok) return;
      const data = await resp.json();

      // Restore config
      const cfg = data.config ?? {};
      if (cfg.start_date) setStartDate(cfg.start_date.slice(0, 7));
      if (cfg.end_date) setEndDate(cfg.end_date.slice(0, 7));
      if (cfg.signal_weights) setWeights(cfg.signal_weights);
      if (cfg.category_weights) setCategoryWeights(cfg.category_weights);
      if (cfg.top_n_sectors) setTopSectors(cfg.top_n_sectors);
      if (cfg.top_n_per_sector) setTopPerSector(cfg.top_n_per_sector);
      if (cfg.selection_mode === 'random' || cfg.selection_mode === 'momentum') setSelectionMode(cfg.selection_mode);
      if (typeof cfg.random_seed === 'number') setRandomSeed(cfg.random_seed);
      if (typeof cfg.n_trials === 'number') setNTrials(cfg.n_trials);
      // Legacy saved runs may have used universe_label; both hit the same table now.
      setSelectedIndexUniverse(cfg.index_universe ?? cfg.universe_label ?? '');

      // Restore result — saved runs store the payload under `result`.
      const saved = data.result ?? data;
      momentumStore.set({
        result: {
          monthly_records: saved.monthly_records ?? [],
          summary: saved.summary ?? {
            total_return_pct: 0,
            annualized_return_pct: 0,
            max_drawdown_pct: 0,
            sharpe_ratio: null,
            avg_monthly_turnover_pct: 0,
            total_months: 0,
            avg_holdings: 0,
            top_drawdowns: [],
          },
        },
        universe: saved.universe ?? [],
        loadedRunId: runId,
        error: null,
        warnings: [],
        infos: [],
        progress: [],
      });
      setExpandedMonth(null);
    } catch {
      momentumStore.set({ error: 'Failed to load backtest' });
    }
  };

  const deleteBacktest = async (runId: number) => {
    setSavedRuns(prev => prev.filter(r => r.run_id !== runId));
    if (loadedRunId === runId) momentumStore.set({ loadedRunId: null });
    try {
      await fetch(`${API_URL}/api/momentum/backtests/${runId}`, { method: 'DELETE' });
    } catch {
      loadSavedRuns();
    }
  };

  const renameBacktest = async (runId: number, currentName: string) => {
    const next = await dialog.prompt('New name for this backtest:', {
      title: 'Rename backtest',
      defaultValue: currentName,
    });
    if (!next || next.trim() === '' || next === currentName) return;
    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtests/${runId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: next.trim() }),
      });
      if (!resp.ok) throw new Error(String(resp.status));
      loadSavedRuns();
    } catch (e) {
      dialog.alert(`Rename failed: ${e instanceof Error ? e.message : e}`, { title: 'Rename failed' });
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-8 py-5 border-b border-gray-800/60 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold text-white">Momentum Backtester</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            Price momentum portfolio — equal-weight, monthly rebalancing, sector-filtered
          </p>
        </div>
        <div className="flex items-center gap-3">
          <ApiUsageBadge />
        {savedRuns.length > 0 && (
          <div className="relative" ref={savedDropdownRef}>
            <button
              type="button"
              onClick={() => setSavedDropdownOpen((o) => !o)}
              className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white flex items-center gap-2 hover:border-indigo-500 focus:outline-none focus:border-indigo-500 transition-colors min-w-[220px]"
            >
              <span className="truncate">
                {loadedRunId
                  ? savedRuns.find((r) => r.run_id === loadedRunId)?.name ?? 'Load saved backtest...'
                  : 'Load saved backtest...'}
              </span>
              <svg className={`w-3.5 h-3.5 text-gray-500 ml-auto transition-transform ${savedDropdownOpen ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M5.23 7.21a.75.75 0 011.06.02L10 11.06l3.71-3.83a.75.75 0 111.08 1.04l-4.25 4.39a.75.75 0 01-1.08 0L5.21 8.27a.75.75 0 01.02-1.06z" clipRule="evenodd" />
              </svg>
            </button>
            {savedDropdownOpen && (
              <div className="absolute right-0 mt-1 w-80 bg-[#151821] border border-gray-700 rounded-lg shadow-xl z-50 max-h-96 overflow-auto">
                {savedRuns.map((r) => {
                  const isActive = r.run_id === loadedRunId;
                  return (
                    <div
                      key={r.run_id}
                      className={`group flex items-center gap-2 px-3 py-2 border-b border-gray-800/40 last:border-b-0 hover:bg-white/[0.03] transition-colors ${isActive ? 'bg-indigo-500/10' : ''}`}
                    >
                      <button
                        type="button"
                        onClick={() => { loadBacktest(r.run_id); setSavedDropdownOpen(false); }}
                        className="flex-1 text-left min-w-0"
                      >
                        <div className={`text-sm truncate ${isActive ? 'text-indigo-300' : 'text-gray-200'}`}>{r.name}</div>
                        <div className="text-[10px] text-gray-500 font-mono">{new Date(r.created_at).toLocaleDateString()}</div>
                      </button>
                      <button
                        type="button"
                        onClick={(e) => { e.stopPropagation(); renameBacktest(r.run_id, r.name); }}
                        className="p-1.5 rounded text-gray-500 hover:text-indigo-400 hover:bg-white/5 opacity-0 group-hover:opacity-100 transition-opacity"
                        title="Rename"
                      >
                        <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                          <path d="M13.586 3.586a2 2 0 112.828 2.828l-.793.793-2.828-2.828.793-.793zM11.379 5.793L3 14.172V17h2.828l8.38-8.379-2.83-2.828z" />
                        </svg>
                      </button>
                      <button
                        type="button"
                        onClick={async (e) => {
                          e.stopPropagation();
                          if (await dialog.confirm(`Delete "${r.name}"?`, { destructive: true, confirmLabel: 'Delete' })) {
                            deleteBacktest(r.run_id);
                          }
                        }}
                        className="p-1.5 rounded text-gray-500 hover:text-rose-400 hover:bg-rose-500/10 opacity-0 group-hover:opacity-100 transition-opacity"
                        title="Delete"
                      >
                        <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                          <path fillRule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clipRule="evenodd" />
                        </svg>
                      </button>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}
        </div>
      </div>

      <div className="flex-1 overflow-auto px-8 py-5 space-y-5">
        {/* Config Panel */}
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
          <div className="flex flex-wrap items-end gap-5 mb-5">
            {/* Universe Label */}
            <div>
              <label className="text-gray-500 text-xs mb-1 flex items-center gap-2">
                <span>Universe</span>
                {universesLoading && (
                  <span className="flex items-center gap-1.5 text-indigo-400">
                    <span className="w-1.5 h-1.5 rounded-full bg-indigo-400 animate-pulse" />
                    <span className="text-[10px]">loading stats from DB… {universesElapsed}s</span>
                  </span>
                )}
                {!universesLoading && !universesError && indexUniverses.length > 0 && (
                  <span className="text-[10px] text-gray-600">{indexUniverses.length} loaded</span>
                )}
                {universesError && (
                  <span className="text-[10px] text-rose-400">failed: {universesError}</span>
                )}
              </label>
              <select
                value={universeDropdownValue}
                onChange={(e) => handleUniverseChange(e.target.value)}
                disabled={universesLoading}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none disabled:opacity-60 disabled:cursor-wait"
              >
                {universesLoading ? (
                  <option value="">Loading universes… ({universesElapsed}s)</option>
                ) : (
                  <>
                    <option value="">All companies</option>
                    {indexUniverses.map(i => (
                      <option key={i.index_name} value={i.index_name}>
                        {i.index_name} ({i.start_month.slice(0, 7)} – {i.end_month.slice(0, 7)}, {i.total_unique_tickers} tickers)
                      </option>
                    ))}
                  </>
                )}
              </select>
            </div>
            {/* Date Range */}
            <div>
              <label className="text-gray-500 text-xs block mb-1">Start</label>
              <input
                type="month"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">End</label>
              <input
                type="month"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">Top Sectors</label>
              <input
                type="number"
                min={1}
                max={20}
                value={topSectors}
                onChange={(e) => setTopSectors(Number(e.target.value))}
                className="w-16 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">Per Sector</label>
              <input
                type="number"
                min={1}
                max={20}
                value={topPerSector}
                onChange={(e) => setTopPerSector(Number(e.target.value))}
                className="w-16 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">Max Companies</label>
              <input
                type="number"
                min={0}
                max={500}
                value={maxCompanies}
                onChange={(e) => setMaxCompanies(Number(e.target.value))}
                className="w-20 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                title="0 = all companies, otherwise limit alphabetically"
              />
              <span className="text-gray-600 text-xs ml-1">0 = all</span>
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">Strategy</label>
              <select
                value={selectionMode}
                onChange={(e) => setSelectionMode(e.target.value as 'momentum' | 'random')}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                title="Random ignores all signal weights and picks sectors/stocks at random — use as a noise-floor baseline."
              >
                <option value="momentum">Momentum</option>
                <option value="random">Random (baseline)</option>
              </select>
            </div>
            {selectionMode === 'random' && (
              <>
                <div>
                  <label className="text-gray-500 text-xs block mb-1">Seed</label>
                  <input
                    type="number"
                    value={randomSeed}
                    onChange={(e) => setRandomSeed(Number(e.target.value))}
                    className="w-20 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                    title="Same seed reproduces the same random picks. With Trials > 1, trials use seed, seed+1, ..."
                  />
                </div>
                <div>
                  <label className="text-gray-500 text-xs block mb-1">Trials</label>
                  <input
                    type="number"
                    min={1}
                    max={100}
                    value={nTrials}
                    onChange={(e) => setNTrials(Number(e.target.value))}
                    className="w-16 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                    title="Number of independent random trials. Summary shows mean ± std across trials."
                  />
                </div>
              </>
            )}
            <button
              onClick={runBacktest}
              disabled={running}
              className="px-5 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {running ? 'Running...' : 'Run Backtest'}
            </button>
            <button
              onClick={showCurrentPicks}
              disabled={running || selectionMode === 'random'}
              className="px-4 py-2 rounded-lg text-sm font-medium border border-gray-700 text-gray-300 hover:bg-white/5 hover:text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              title={
                selectionMode === 'random'
                  ? 'Current Picks is unavailable for random selection mode'
                  : currentPicksSnapshots.length > 0
                    ? `Load most recent snapshot (${currentPicksSnapshots[0].as_of_date}, ${currentPicksSnapshots[0].triggered_by})`
                    : 'No saved snapshot yet — first click will run a full compute and save it'
              }
            >
              Current Picks
            </button>
            <label
              className="flex items-center gap-2 cursor-pointer select-none self-center pt-4"
              title="Bypass the replay cache and recompute the backtest from scratch."
            >
              <input
                type="checkbox"
                checked={noCache}
                onChange={(e) => setNoCache(e.target.checked)}
                className="accent-indigo-500 w-4 h-4 cursor-pointer"
              />
              <span className="text-gray-400 text-xs">Don&apos;t use cache</span>
            </label>
            {running && (
              <button
                onClick={cancelBacktest}
                className="px-4 py-2 rounded-lg text-sm font-medium text-gray-400 hover:text-rose-400 hover:bg-rose-500/10 transition-colors"
              >
                Cancel
              </button>
            )}
          </div>

          {/* Signal Weights */}
          <div className="space-y-4">
            {['price', 'volume'].map((group) => {
              const groupSignals = signalDefs.filter((s) => (s.group ?? 'price') === group);
              if (groupSignals.length === 0) return null;
              return (
                <div key={group}>
                  <h3 className="text-gray-400 text-xs font-medium mb-2.5 uppercase tracking-wider">
                    {group === 'price' ? 'Price Momentum' : 'Volume Confirmation'}
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-2.5">
                    {groupSignals.map((s) => (
                      <div key={s.key} className="flex items-center gap-3">
                        <div className="w-36 shrink-0 flex items-center gap-1.5">
                          <span className="text-gray-300 text-xs font-medium">{s.label}</span>
                          <span className="relative group/tip">
                            <span className="text-gray-600 hover:text-gray-400 cursor-help text-xs">&#9432;</span>
                            <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 hidden group-hover/tip:block w-64 px-3 py-2 rounded-lg bg-gray-800 border border-gray-700 text-gray-300 text-xs leading-relaxed shadow-xl z-50 pointer-events-none">
                              {s.description}
                            </span>
                          </span>
                        </div>
                        <input
                          type="range"
                          min={0}
                          max={10}
                          step={1}
                          value={weights[s.key] ?? 0}
                          onChange={(e) => setWeights((prev) => ({ ...prev, [s.key]: Number(e.target.value) }))}
                          className="flex-1 h-1 accent-indigo-500 cursor-pointer"
                        />
                        <span className="text-gray-500 text-xs w-5 text-right font-mono shrink-0">{weights[s.key] ?? 0}</span>
                      </div>
                    ))}
                  </div>
                </div>
              );
            })}
          {/* Category Weights */}
          {categories.length > 1 && (
            <div>
              <h3 className="text-gray-400 text-xs font-medium mb-2.5 uppercase tracking-wider">Category Weights</h3>
              <div className="flex items-center gap-6">
                {categories.map((cat) => (
                  <div key={cat} className="flex items-center gap-2">
                    <span className="text-gray-300 text-xs font-medium w-28">
                      {cat === 'price' ? 'Price Momentum' : cat === 'volume' ? 'Volume Confirmation' : cat}
                    </span>
                    <input
                      type="range"
                      min={0}
                      max={100}
                      step={5}
                      value={categoryWeights[cat] ?? 50}
                      onChange={(e) => setCategoryWeights((prev) => ({ ...prev, [cat]: Number(e.target.value) }))}
                      className="w-32 h-1 accent-indigo-500 cursor-pointer"
                    />
                    <span className="text-gray-500 text-xs w-8 text-right font-mono">{categoryWeights[cat] ?? 50}%</span>
                  </div>
                ))}
              </div>
            </div>
          )}
          </div>
        </div>

        {/* Progress */}
        {(running || error || progress.length > 0) && (
          <ProgressTimeline
            steps={[]}
            log={progress.map(p => p.message)}
            pct={progress[progress.length - 1]?.pct ?? 0}
            errorMessage={error}
            running={running}
            defaultLogOpen
            title="Backtest progress"
          />
        )}

        {/* Notifications — warnings on top (critical), info below (expected) */}
        {(warnings.length > 0 || infos.length > 0) && (
          <div className="bg-[#151821] border border-gray-800/40 rounded-lg overflow-hidden divide-y divide-gray-800/40">
            {warnings.length > 0 && (
              <div className="bg-amber-500/10">
                <button
                  type="button"
                  onClick={() => setShowWarnings((v) => !v)}
                  className="w-full flex items-center justify-between px-4 py-2.5 text-left hover:bg-amber-500/5 transition-colors"
                >
                  <span className="text-amber-300 text-sm font-medium">
                    {warnings.length} warning{warnings.length === 1 ? '' : 's'}
                  </span>
                  <span className="text-amber-400/70 text-xs font-mono">{showWarnings ? '▾' : '▸'}</span>
                </button>
                {showWarnings && (
                  <ul className="max-h-64 overflow-auto border-t border-amber-500/20 divide-y divide-amber-500/10">
                    {warnings.map((w, i) => (
                      <li key={i} className="px-4 py-2 text-xs text-amber-200 flex gap-2">
                        <span className="uppercase text-[10px] tracking-wider font-mono text-amber-400/70 shrink-0 w-16">
                          {w.scope}
                        </span>
                        <span className="break-words">{w.message}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            )}
            {infos.length > 0 && (
              <div className="bg-sky-500/10">
                <button
                  type="button"
                  onClick={() => setShowInfos((v) => !v)}
                  className="w-full flex items-center justify-between px-4 py-2.5 text-left hover:bg-sky-500/5 transition-colors"
                >
                  <span className="text-sky-300 text-sm font-medium">
                    {infos.length} note{infos.length === 1 ? '' : 's'}
                  </span>
                  <span className="text-sky-400/70 text-xs font-mono">{showInfos ? '▾' : '▸'}</span>
                </button>
                {showInfos && (
                  <ul className="max-h-64 overflow-auto border-t border-sky-500/20 divide-y divide-sky-500/10">
                    {infos.map((n, i) => (
                      <li key={i} className="px-4 py-2 text-xs text-sky-200 flex gap-2">
                        <span className="uppercase text-[10px] tracking-wider font-mono text-sky-400/70 shrink-0 w-16">
                          {n.scope}
                        </span>
                        <span className="break-words">{n.message}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            )}
          </div>
        )}

        {/* Current Portfolio (MTD) — shown above backtest results, independent */}
        {currentPortfolio && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-800/40 flex items-center justify-between flex-wrap gap-3">
              <div className="flex items-center gap-3 flex-wrap">
                <div>
                  <div className="text-sm font-medium text-white">Current Picks</div>
                  <div className="text-xs text-gray-500">
                    Rebalance as of <span className="font-mono text-gray-400">{currentPortfolio.as_of_date}</span>
                    {currentPortfolio.latest_price_date && (
                      <> · MTD through <span className="font-mono text-gray-400">{currentPortfolio.latest_price_date}</span></>
                    )}
                    {' · '}{currentPortfolio.holdings.length} holdings
                  </div>
                </div>
                {/* Snapshot picker */}
                {currentPicksSnapshots.length > 0 && (
                  <select
                    value={currentPortfolio.snapshot_id ?? ''}
                    onChange={(e) => {
                      const id = Number(e.target.value);
                      if (id) loadCurrentPicksSnapshot(id);
                    }}
                    className="bg-[#0f1117] border border-gray-700 rounded-lg px-2 py-1 text-xs text-gray-300 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                    title="Switch to a historic snapshot"
                  >
                    {currentPortfolio.snapshot_id == null && <option value="">(unsaved)</option>}
                    {currentPicksSnapshots.map((s) => (
                      <option key={s.snapshot_id} value={s.snapshot_id}>
                        {s.created_at.slice(0, 16).replace('T', ' ')} · {s.triggered_by} · {s.as_of_date.slice(0, 7)}
                      </option>
                    ))}
                  </select>
                )}
                {/* Refresh MTD button — only meaningful when a saved snapshot is loaded */}
                {currentPortfolio.snapshot_id != null && (
                  <button
                    onClick={() => refreshCurrentPicksMTD(currentPortfolio.snapshot_id!)}
                    disabled={refreshingMTD || running}
                    className="px-2.5 py-1 rounded-lg text-xs font-medium border border-gray-700 text-gray-300 hover:bg-white/5 hover:text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed inline-flex items-center gap-1.5"
                    title="Refresh month-to-date returns using the latest available prices (does not re-run signals)"
                  >
                    <span className="text-emerald-400">✓</span>
                    {refreshingMTD ? 'Refreshing…' : 'Refresh MTD'}
                  </button>
                )}
                {/* Force a new full compute */}
                <button
                  onClick={recomputeCurrentPortfolio}
                  disabled={running}
                  className="px-2.5 py-1 rounded-lg text-xs font-medium border border-gray-700 text-gray-300 hover:bg-white/5 hover:text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                  title="Run the full strategy now and save a new snapshot (slow)"
                >
                  Recompute
                </button>
              </div>
              {currentPortfolio.holdings.length > 0 && (() => {
                const validReturns = currentPortfolio.holdings
                  .map(h => h.forward_return_pct)
                  .filter((r): r is number => r != null);
                if (validReturns.length === 0) return null;
                const portMTD = validReturns.reduce((a, b) => a + b, 0) / validReturns.length;
                return (
                  <div className="text-right">
                    <div className="text-xs text-gray-500">Portfolio MTD (equal-weight)</div>
                    <div className={`text-lg font-mono font-medium ${portMTD >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                      {portMTD >= 0 ? '+' : ''}{portMTD.toFixed(2)}%
                    </div>
                  </div>
                );
              })()}
            </div>
            {currentPortfolio.holdings.length > 0 ? (
              <div className="bg-[#0f1117] px-5 py-3">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="text-gray-600">
                      <th className="text-left py-1 font-medium">
                        Ticker<CellInfoTip>The stock&apos;s ticker on its primary exchange. Click to open in GuruFocus.</CellInfoTip>
                      </th>
                      <th className="text-left py-1 font-medium">
                        Company<CellInfoTip>Issuer name. Click to open in GuruFocus.</CellInfoTip>
                      </th>
                      <th className="text-left py-1 font-medium">
                        Sector<CellInfoTip>GICS sector. The strategy picks the top sectors by aggregate momentum, then the top stocks within each.</CellInfoTip>
                      </th>
                      {categories.map((cat) => (
                        <th key={cat} className="text-right py-1 font-medium">
                          {cat === 'price' ? 'Price' : cat === 'volume' ? 'Vol' : cat}
                          <CellInfoTip>
                            {cat === 'price'
                              ? 'Composite 0–100 score across the price-momentum signals (12-1 return, 6m return, vol-adj return, drawdown, above-200MA), min-max normalized within the universe at this date.'
                              : cat === 'volume'
                              ? 'Composite 0–100 score across the volume signals (Volume Surge, Volume Trend 3M), min-max normalized within the universe at this date.'
                              : `${cat} category score, 0–100 normalized across the universe.`}
                          </CellInfoTip>
                        </th>
                      ))}
                      <th className="text-right py-1 font-medium">
                        Total<CellInfoTip>Weighted combination of the category scores. Selection ranks by this number.</CellInfoTip>
                      </th>
                      <th className="text-right py-1 font-medium pl-4">
                        Start (local)<CellInfoTip>Entry price in the stock&apos;s local currency on (or just after) the first of the month.</CellInfoTip>
                      </th>
                      <th className="text-right py-1 font-medium">
                        End (local)<CellInfoTip>Latest available close in local currency through the row&apos;s reporting date.</CellInfoTip>
                      </th>
                      <th className="text-right py-1 font-medium pl-4">
                        Start (€)<CellInfoTip>Entry price converted to EUR using the day&apos;s ECB FX rate.</CellInfoTip>
                      </th>
                      <th className="text-right py-1 font-medium">
                        End (€)<CellInfoTip>Exit price converted to EUR using the day&apos;s ECB FX rate.</CellInfoTip>
                      </th>
                      <th className="text-right py-1 font-medium pl-4">
                        Return<CellInfoTip>Per-stock month-to-date return in EUR: (End € ÷ Start €) − 1, assuming the position was held since month start.</CellInfoTip>
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {[...currentPortfolio.holdings]
                      .sort((a, b) => {
                        const sec = a.sector.localeCompare(b.sector);
                        return sec !== 0 ? sec : b.score - a.score;
                      })
                      .map((h) => {
                        const exch = exchangeByCompany.get(h.company_id) ?? '';
                        const href = guruFocusUrl(h.ticker, exch);
                        return (
                          <tr key={h.company_id} className="border-t border-gray-800/20">
                            <td className="py-1.5 font-mono whitespace-nowrap">
                              <a
                                href={href}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-indigo-400 hover:text-indigo-300 hover:underline"
                              >
                                {h.ticker}
                              </a>
                              {exch && (
                                <span
                                  className="ml-1 text-[10px] text-gray-500"
                                  title={EXCHANGE_NAMES[exch.toUpperCase()] ?? exch}
                                >
                                  ({exch})
                                </span>
                              )}
                            </td>
                            <td className="py-1.5 truncate max-w-[200px]">
                              <a
                                href={href}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-gray-300 hover:text-indigo-300 hover:underline"
                              >
                                {h.company_name}
                              </a>
                            </td>
                            <td className="py-1.5 text-gray-500">{h.sector}</td>
                            {categories.map((cat) => (
                              <td key={cat} className="text-right py-1.5 text-gray-400 font-mono">
                                {h.category_scores?.[cat] != null ? h.category_scores[cat]!.toFixed(0) : '—'}
                              </td>
                            ))}
                            <td className="text-right py-1.5 text-white font-mono font-medium">{h.score.toFixed(1)}</td>
                            <td className="text-right py-1.5 text-gray-400 font-mono pl-4">
                              {fmtPrice(h.entry_price_local)}
                              {h.currency && <span className="text-gray-600 text-[10px] ml-1">{h.currency}</span>}
                              {h.entry_date && (
                                <CellInfoTip>
                                  <div className="text-gray-400">Trading date</div>
                                  <div className="font-mono text-gray-200">{h.entry_date}</div>
                                </CellInfoTip>
                              )}
                            </td>
                            <td className="text-right py-1.5 text-gray-400 font-mono">
                              {fmtPrice(h.exit_price_local)}
                              {h.exit_date && (
                                <CellInfoTip>
                                  <div className="text-gray-400">Trading date</div>
                                  <div className="font-mono text-gray-200">{h.exit_date}</div>
                                </CellInfoTip>
                              )}
                            </td>
                            <td className="text-right py-1.5 text-gray-400 font-mono pl-4">
                              {fmtPrice(h.entry_price_eur)}
                              {(h.entry_date || (h.entry_price_eur != null && h.entry_price_local)) && (
                                <CellInfoTip>
                                  {h.entry_date && (
                                    <>
                                      <div className="text-gray-400">Trading date</div>
                                      <div className="font-mono text-gray-200 mb-1">{h.entry_date}</div>
                                    </>
                                  )}
                                  {h.entry_price_eur != null && h.entry_price_local && h.entry_price_local > 0 && (
                                    <>
                                      <div className="text-gray-400">FX rate</div>
                                      <div className="font-mono text-gray-200">
                                        1 {h.currency ?? 'LCL'} = {(h.entry_price_eur / h.entry_price_local).toFixed(4)} EUR
                                      </div>
                                    </>
                                  )}
                                </CellInfoTip>
                              )}
                            </td>
                            <td className="text-right py-1.5 text-gray-400 font-mono">
                              {fmtPrice(h.exit_price_eur)}
                              {(h.exit_date || (h.exit_price_eur != null && h.exit_price_local)) && (
                                <CellInfoTip>
                                  {h.exit_date && (
                                    <>
                                      <div className="text-gray-400">Trading date</div>
                                      <div className="font-mono text-gray-200 mb-1">{h.exit_date}</div>
                                    </>
                                  )}
                                  {h.exit_price_eur != null && h.exit_price_local && h.exit_price_local > 0 && (
                                    <>
                                      <div className="text-gray-400">FX rate</div>
                                      <div className="font-mono text-gray-200">
                                        1 {h.currency ?? 'LCL'} = {(h.exit_price_eur / h.exit_price_local).toFixed(4)} EUR
                                      </div>
                                    </>
                                  )}
                                </CellInfoTip>
                              )}
                            </td>
                            <td className={`text-right py-1.5 font-mono pl-4 ${h.forward_return_pct != null ? (h.forward_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                              {fmtPct(h.forward_return_pct)}
                            </td>
                          </tr>
                        );
                      })}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="px-4 py-6 text-center text-sm text-gray-500">
                No holdings selected for this month — universe or signals returned empty.
              </div>
            )}
            <DailyPicksHistory
              currentPortfolio={currentPortfolio}
              categories={categories}
              exchangeByCompany={exchangeByCompany}
            />
          </div>
        )}

        {/* Results */}
        {result && (
          <>
            <EquityCurveCard
              result={result}
              loadedRunId={loadedRunId}
              savedRuns={savedRuns}
            />

            {/* Monthly Portfolio Table */}
            <div className="bg-[#151821] rounded-xl border border-gray-800/40">
              <div className="px-5 py-4 border-b border-gray-800/40">
                <h3 className="text-white text-sm font-medium">Monthly Portfolios</h3>
              </div>
              <div className="max-h-[500px] overflow-auto">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-[#151821] z-20">
                    <tr className="text-gray-500 text-xs border-b border-gray-800/40">
                      <th className="text-left px-5 py-2.5 font-medium">
                        Month<CellInfoTip>The rebalance month (YYYY-MM). The strategy enters the month&apos;s portfolio at the first trading day and holds until the next first-of-month.</CellInfoTip>
                      </th>
                      <th className="text-right px-3 py-2.5 font-medium">
                        Holdings<CellInfoTip>Number of stocks in the portfolio for this month (equal-weighted). Determined by top_n_sectors × top_n_per_sector, minus any failures.</CellInfoTip>
                      </th>
                      <th className="text-right px-3 py-2.5 font-medium">
                        Return<CellInfoTip>Equal-weighted portfolio return for this month: mean of holdings&apos; (next-month-entry ÷ this-month-entry) − 1, in EUR.</CellInfoTip>
                      </th>
                      <th className="text-right px-3 py-2.5 font-medium">
                        Turnover<CellInfoTip>Percentage of this month&apos;s holdings not held in the previous month. 0% means the strategy held the same portfolio; 100% means it replaced everything.</CellInfoTip>
                      </th>
                      <th className="text-right px-5 py-2.5 font-medium">
                        Cumulative<CellInfoTip>Cumulative return through the end of this month, since the backtest start: chain-linked product of all prior monthly returns.</CellInfoTip>
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.monthly_records.map((r) => (
                      <Fragment key={r.date}>
                        <tr
                          className="border-b border-gray-800/20 hover:bg-white/[0.02] cursor-pointer transition-colors"
                          onClick={() => setExpandedMonth(expandedMonth === r.date ? null : r.date)}
                        >
                          <td className="px-5 py-2.5 text-gray-300 font-mono">
                            <span className="text-gray-600 mr-2">{expandedMonth === r.date ? '▾' : '▸'}</span>
                            {r.date}
                          </td>
                          <td className="text-right px-3 py-2.5 text-gray-400 font-mono">{r.holdings.length}</td>
                          <td className={`text-right px-3 py-2.5 font-mono ${r.portfolio_return_pct != null ? (r.portfolio_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                            {fmtPct(r.portfolio_return_pct)}
                          </td>
                          <td className="text-right px-3 py-2.5 font-mono text-gray-400">
                            {turnoverByDate[r.date] != null ? `${turnoverByDate[r.date]!.toFixed(1)}%` : '—'}
                          </td>
                          <td className={`text-right px-5 py-2.5 font-mono ${r.cumulative_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                            {fmtPct(r.cumulative_return_pct)}
                          </td>
                        </tr>
                        {expandedMonth === r.date && r.holdings.length > 0 && (
                          <tr key={`${r.date}-detail`}>
                            <td colSpan={5} className="bg-[#0f1117] px-5 py-3">
                              <table className="w-full text-xs">
                                <thead>
                                  <tr className="text-gray-600">
                                    <th className="text-left py-1 font-medium">
                                      Ticker<CellInfoTip>The stock&apos;s ticker on its primary exchange. Click to open in GuruFocus.</CellInfoTip>
                                    </th>
                                    <th className="text-left py-1 font-medium">
                                      Company<CellInfoTip>Issuer name. Click to open in GuruFocus.</CellInfoTip>
                                    </th>
                                    <th className="text-left py-1 font-medium">
                                      Sector<CellInfoTip>GICS sector. Selection picks top sectors then top stocks within each.</CellInfoTip>
                                    </th>
                                    {categories.map((cat) => (
                                      <th key={cat} className="text-right py-1 font-medium">
                                        {cat === 'price' ? 'Price' : cat === 'volume' ? 'Vol' : cat}
                                        <CellInfoTip>
                                          {cat === 'price'
                                            ? 'Composite 0–100 score across the price-momentum signals, min-max normalized within the universe at this date.'
                                            : cat === 'volume'
                                            ? 'Composite 0–100 score across the volume signals, min-max normalized within the universe at this date.'
                                            : `${cat} category score, 0–100 normalized across the universe.`}
                                        </CellInfoTip>
                                      </th>
                                    ))}
                                    <th className="text-right py-1 font-medium">
                                      Total<CellInfoTip>Weighted combination of the category scores. Selection ranks by this.</CellInfoTip>
                                    </th>
                                    <th className="text-right py-1 font-medium pl-4">
                                      Start (local)<CellInfoTip>Entry price in local currency at the first trading day of this month.</CellInfoTip>
                                    </th>
                                    <th className="text-right py-1 font-medium">
                                      End (local)<CellInfoTip>Exit price in local currency at the first trading day of the next month.</CellInfoTip>
                                    </th>
                                    <th className="text-right py-1 font-medium pl-4">
                                      Start (€)<CellInfoTip>Entry price converted to EUR using the day&apos;s ECB FX rate.</CellInfoTip>
                                    </th>
                                    <th className="text-right py-1 font-medium">
                                      End (€)<CellInfoTip>Exit price converted to EUR using the day&apos;s ECB FX rate.</CellInfoTip>
                                    </th>
                                    <th className="text-right py-1 font-medium pl-4">
                                      Return<CellInfoTip>Per-stock return in EUR over this month: (End € ÷ Start €) − 1.</CellInfoTip>
                                    </th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {[...r.holdings]
                                    .sort((a, b) => {
                                      const sec = a.sector.localeCompare(b.sector);
                                      return sec !== 0 ? sec : b.score - a.score;
                                    })
                                    .map((h) => {
                                      const exch = exchangeByCompany.get(h.company_id) ?? '';
                                      const href = guruFocusUrl(h.ticker, exch);
                                      return (
                                        <tr key={h.company_id} className="border-t border-gray-800/20">
                                          <td className="py-1.5 font-mono whitespace-nowrap">
                                            <a
                                              href={href}
                                              target="_blank"
                                              rel="noopener noreferrer"
                                              className="text-indigo-400 hover:text-indigo-300 hover:underline"
                                            >
                                              {h.ticker}
                                            </a>
                                            {exch && (
                                              <span
                                                className="ml-1 text-[10px] text-gray-500"
                                                title={EXCHANGE_NAMES[exch.toUpperCase()] ?? exch}
                                              >
                                                ({exch})
                                              </span>
                                            )}
                                          </td>
                                          <td className="py-1.5 truncate max-w-[200px]">
                                            <a
                                              href={href}
                                              target="_blank"
                                              rel="noopener noreferrer"
                                              className="text-gray-300 hover:text-indigo-300 hover:underline"
                                            >
                                              {h.company_name}
                                            </a>
                                          </td>
                                          <td className="py-1.5 text-gray-500">{h.sector}</td>
                                          {categories.map((cat) => (
                                            <td key={cat} className="text-right py-1.5 text-gray-400 font-mono">
                                              {h.category_scores?.[cat] != null ? h.category_scores[cat]!.toFixed(0) : '—'}
                                            </td>
                                          ))}
                                          <td className="text-right py-1.5 text-white font-mono font-medium">{h.score.toFixed(1)}</td>
                                          <td className="text-right py-1.5 text-gray-400 font-mono pl-4">
                                            {fmtPrice(h.entry_price_local)}
                                            {h.currency && <span className="text-gray-600 text-[10px] ml-1">{h.currency}</span>}
                                            {h.entry_date && (
                                              <CellInfoTip>
                                                <div className="text-gray-400">Trading date</div>
                                                <div className="font-mono text-gray-200">{h.entry_date}</div>
                                              </CellInfoTip>
                                            )}
                                          </td>
                                          <td className="text-right py-1.5 text-gray-400 font-mono">
                                            {fmtPrice(h.exit_price_local)}
                                            {h.exit_date && (
                                              <CellInfoTip>
                                                <div className="text-gray-400">Trading date</div>
                                                <div className="font-mono text-gray-200">{h.exit_date}</div>
                                              </CellInfoTip>
                                            )}
                                          </td>
                                          <td className="text-right py-1.5 text-gray-400 font-mono pl-4">
                                            {fmtPrice(h.entry_price_eur)}
                                            {(h.entry_date || (h.entry_price_eur != null && h.entry_price_local)) && (
                                              <CellInfoTip>
                                                {h.entry_date && (
                                                  <>
                                                    <div className="text-gray-400">Trading date</div>
                                                    <div className="font-mono text-gray-200 mb-1">{h.entry_date}</div>
                                                  </>
                                                )}
                                                {h.entry_price_eur != null && h.entry_price_local && h.entry_price_local > 0 && (
                                                  <>
                                                    <div className="text-gray-400">FX rate</div>
                                                    <div className="font-mono text-gray-200">
                                                      1 {h.currency ?? 'LCL'} = {(h.entry_price_eur / h.entry_price_local).toFixed(4)} EUR
                                                    </div>
                                                  </>
                                                )}
                                              </CellInfoTip>
                                            )}
                                          </td>
                                          <td className="text-right py-1.5 text-gray-400 font-mono">
                                            {fmtPrice(h.exit_price_eur)}
                                            {(h.exit_date || (h.exit_price_eur != null && h.exit_price_local)) && (
                                              <CellInfoTip>
                                                {h.exit_date && (
                                                  <>
                                                    <div className="text-gray-400">Trading date</div>
                                                    <div className="font-mono text-gray-200 mb-1">{h.exit_date}</div>
                                                  </>
                                                )}
                                                {h.exit_price_eur != null && h.exit_price_local && h.exit_price_local > 0 && (
                                                  <>
                                                    <div className="text-gray-400">FX rate</div>
                                                    <div className="font-mono text-gray-200">
                                                      1 {h.currency ?? 'LCL'} = {(h.exit_price_eur / h.exit_price_local).toFixed(4)} EUR
                                                    </div>
                                                  </>
                                                )}
                                              </CellInfoTip>
                                            )}
                                          </td>
                                          <td className={`text-right py-1.5 font-mono pl-4 ${h.forward_return_pct != null ? (h.forward_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                                            {fmtPct(h.forward_return_pct)}
                                          </td>
                                        </tr>
                                      );
                                    })}
                                </tbody>
                              </table>
                            </td>
                          </tr>
                        )}
                        {expandedMonth === r.date && r.holdings.length === 0 && (
                          <tr key={`${r.date}-empty`}>
                            <td colSpan={4} className="bg-[#0f1117] px-5 py-4">
                              <div className="text-xs text-gray-500">
                                {r.empty_reason || 'No holdings for this month (unknown reason)'}
                              </div>
                            </td>
                          </tr>
                        )}
                      </Fragment>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Save */}
            {!loadedRunId && (
              <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-4 flex items-center gap-3">
                <input
                  value={saveName}
                  onChange={(e) => setSaveName(e.target.value)}
                  placeholder="Backtest name..."
                  className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white flex-1 max-w-xs focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 placeholder-gray-600 transition-colors"
                  onKeyDown={(e) => { if (e.key === 'Enter') saveBacktest(); }}
                />
                <button
                  onClick={saveBacktest}
                  disabled={saving || !saveName.trim()}
                  className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {saving ? 'Saving...' : 'Save Backtest'}
                </button>
              </div>
            )}
            {loadedRunId && (
              <div className="bg-indigo-500/10 border border-indigo-500/20 rounded-lg px-4 py-3 text-indigo-400 text-sm flex items-center gap-2">
                <span>Loaded from saved run</span>
                <span className="text-indigo-300 font-medium">
                  {savedRuns.find((r) => r.run_id === loadedRunId)?.name}
                </span>
              </div>
            )}

            {/* Disclaimer */}
            <p className="text-gray-600 text-xs">
              Note: Uses current company universe applied retroactively (survivorship bias). Returns are hypothetical and do not account for transaction costs.
            </p>
          </>
        )}
      </div>
    </div>
  );
}
