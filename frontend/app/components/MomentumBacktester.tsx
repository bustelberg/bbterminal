'use client';

import { Fragment, useState, useEffect, useMemo, useRef } from 'react';

import ApiUsageBadge from './ApiUsageBadge';
import { dialog } from '../../lib/dialog';
import ProgressTimeline from './ProgressTimeline';
import {
  momentumStore,
  startBacktest,
  cancelBacktest,
  startVariantsBacktest,
  cancelVariantsBacktest,
  loadCurrentPicksSnapshots,
  loadCurrentPicksSnapshot,
  refreshCurrentPicksMTD,
  deleteCurrentPicksSnapshot,
  renameCurrentPicksSnapshot,
  VARIANT_DEFS,
  type BacktestStartConfig,
  type VariantKey,
  type VariantOutcome,
} from '../../lib/stores/momentum';
import CellInfoTip from './momentum/CellInfoTip';
import CollapsibleCard from './momentum/CollapsibleCard';
import DailyPicksHistory from './momentum/DailyPicksHistory';
import EquityCurveCard from './momentum/EquityCurveCard';
import MonthlyHoldingsTable from './momentum/MonthlyHoldingsTable';
import SectorTimelineChart from './momentum/SectorTimelineChart';
import VariantSummaryTable from './momentum/VariantSummaryTable';
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

/** Inline spinner used everywhere a small async indicator is needed in this
 * page (per-row dropdown loads, delete/rename in flight, etc.). The 12-px
 * variant fits inline next to text or in icon-button slots without bumping
 * the row height. */
function Spinner({ size = 12 }: { size?: number }) {
  return (
    <svg
      className="animate-spin text-indigo-400"
      style={{ width: size, height: size }}
      viewBox="0 0 24 24"
      fill="none"
      aria-label="Loading"
    >
      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
    </svg>
  );
}

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

  // Variant sweep selection — which (frequency × strategy) combos to run.
  // Default to all selected so the "Run variants" button matches its
  // pre-selection behavior. Controls live in a popover next to the button.
  const [selectedVariantKeys, setSelectedVariantKeys] = useState<Set<VariantKey>>(
    () => new Set(VARIANT_DEFS.map((v) => v.key)),
  );
  const [variantsPickerOpen, setVariantsPickerOpen] = useState(false);
  const variantsPickerRef = useRef<HTMLDivElement>(null);
  const toggleVariantKey = (key: VariantKey) => {
    setSelectedVariantKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key); else next.add(key);
      return next;
    });
  };
  // Backend rejects `long_short` + `random` (long-short without a
  // signal-driven score is meaningless), so when Random is selected we
  // hide the long-short rows from the picker and only run long-only.

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
  const runStartedAt = momentumStore.use((s) => s.runStartedAt);
  const runEndedAt = momentumStore.use((s) => s.runEndedAt);
  // Variants sweep — populated by the "Run variants" button. When a variant
  // is active, the detail views below render that variant's BacktestResult
  // instead of the single-run `result`.
  const variants = momentumStore.use((s) => s.variants);
  const activeVariantKey = momentumStore.use((s) => s.activeVariantKey);
  const variantsRun = momentumStore.use((s) => s.variantsRun);
  const variantsRunning = variantsRun != null && variantsRun.current != null;
  const hasVariants = Object.keys(variants).length > 0;
  const activeVariantOutcome = activeVariantKey ? variants[activeVariantKey] : undefined;
  const activeVariantResult =
    activeVariantOutcome?.status === 'ok' ? activeVariantOutcome.result : null;
  // The detail views (equity curve, holdings, sector timeline) prefer the
  // active variant's result when one is selected; otherwise they fall back
  // to the single-run result. Saving and "loaded run" flows only apply to
  // the single-run path.
  const displayResult = activeVariantResult ?? result;

  // Re-render once per second while a run is active so the "Running 47s"
  // counter and per-line "+1.3s" timestamps stay live. Cheap; only fires
  // while running.
  const [, setNowTick] = useState(0);
  useEffect(() => {
    if (!running) return;
    const id = setInterval(() => setNowTick((n) => n + 1), 1000);
    return () => clearInterval(id);
  }, [running]);

  // Total elapsed for the panel header.
  const totalElapsedMs = useMemo<number | null>(() => {
    if (runStartedAt == null) return null;
    const end = running ? Date.now() : (runEndedAt ?? Date.now());
    return end - runStartedAt;
  }, [running, runStartedAt, runEndedAt]);

  // Per-entry log with relative timestamps so the timeline shows when each
  // step actually fired (and how big the gaps between steps are).
  const progressLog = useMemo(
    () =>
      progress.map((p) => ({
        message: p.message,
        relativeMs: runStartedAt != null ? p.t - runStartedAt : undefined,
      })),
    [progress, runStartedAt],
  );

  const exchangeByCompany = useMemo(() => {
    const m = new Map<number, string>();
    for (const u of universe) m.set(u.company_id, u.exchange);
    return m;
  }, [universe]);

  // Purely local UI state — safe to reset on navigation
  const [showWarnings, setShowWarnings] = useState(true);
  const [showInfos, setShowInfos] = useState(false);

  // Save/load state
  const [savedRuns, setSavedRuns] = useState<SavedRun[]>([]);
  const [saving, setSaving] = useState(false);

  const [savedDropdownOpen, setSavedDropdownOpen] = useState(false);
  const savedDropdownRef = useRef<HTMLDivElement>(null);
  const [picksDropdownOpen, setPicksDropdownOpen] = useState(false);
  const picksDropdownRef = useRef<HTMLDivElement>(null);

  // Per-row spinners for the saved-backtests dropdown. The current-picks
  // dropdown reads its equivalents from the store so the actions can also
  // be driven from elsewhere (e.g. an inline rename in the picks card).
  const [loadingRunId, setLoadingRunId] = useState<number | null>(null);
  const [deletingRunId, setDeletingRunId] = useState<number | null>(null);
  const [renamingRunId, setRenamingRunId] = useState<number | null>(null);
  const loadingSnapshotId = momentumStore.use((s) => s.loadingSnapshotId);
  const deletingSnapshotId = momentumStore.use((s) => s.deletingSnapshotId);
  const renamingSnapshotId = momentumStore.use((s) => s.renamingSnapshotId);

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

  useEffect(() => {
    if (!picksDropdownOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (picksDropdownRef.current && !picksDropdownRef.current.contains(e.target as Node)) {
        setPicksDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [picksDropdownOpen]);

  useEffect(() => {
    if (!variantsPickerOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (variantsPickerRef.current && !variantsPickerRef.current.contains(e.target as Node)) {
        setVariantsPickerOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [variantsPickerOpen]);

  const loadSavedRuns = () => {
    fetch(`${API_URL}/api/momentum/backtests`)
      .then((r) => r.json())
      .then((data) => setSavedRuns(data))
      .catch(() => {});
  };


  // Variant sweep — fans the current config out across the selected
  // (frequency × strategy) combos. Backend cache keys on both axes so
  // re-runs are fast. When Random is the active strategy, long-short
  // variants are filtered out (the backend rejects that combination).
  const runVariantsBacktest = () => {
    const eligibleKeys = VARIANT_DEFS
      .filter((v) => selectedVariantKeys.has(v.key))
      .filter((v) => selectionMode !== 'random' || v.strategy !== 'long_short')
      .map((v) => v.key);
    if (eligibleKeys.length === 0) return;
    momentumStore.set({ result: null, loadedRunId: null });
    return startVariantsBacktest(
      {
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
      },
      eligibleKeys,
    );
  };

  const _currentPortfolioConfig = (opts: { force: boolean; dbOnly: boolean }): BacktestStartConfig => ({
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
    force_recompute: opts.force,
    db_only: opts.dbOnly,
  });

  // Hit the backend for "what is my strategy holding right now?". By
  // default, runs DB-only — no GuruFocus / ECB calls, just whatever is
  // already in Supabase. With "Don't use cache" checked, both the
  // snapshot cache AND the db_only guard are disabled, so missing
  // prices/volumes/FX are fetched fresh (same path as Recompute).
  const showCurrentPicks = async () => {
    await startBacktest(_currentPortfolioConfig({ force: noCache, dbOnly: !noCache }));
    loadCurrentPicksSnapshots();
  };

  // "Recompute" is the explicit "I want fresh data" path: it bypasses
  // both the snapshot cache (force_recompute) AND the db_only guard, so
  // the backend will fetch any missing prices / volumes / FX from the
  // upstream APIs. Slow, but produces a new snapshot.
  const recomputeCurrentPortfolio = async () => {
    await startBacktest(_currentPortfolioConfig({ force: true, dbOnly: false }));
    loadCurrentPicksSnapshots();
  };

  // Persist every `ok` variant from the current sweep as one row. Variants
  // still pending / running / cancelled / errored are skipped — only
  // completed-ok payloads land in the bundle. Loading later rehydrates the
  // sweep state so the detail views switch between variants exactly like
  // they did during the live sweep.
  /** Auto-save default: "{universe or 'All companies'} · {YYYY-MM-DD HH:MM}".
   * Universe gives strategic context; the timestamp disambiguates re-runs
   * of the same config so they don't collide in the dropdown. */
  const defaultVariantsBundleName = (): string => {
    const universe = (selectedIndexUniverse || '').trim() || 'All companies';
    const now = new Date();
    const ts = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')} ${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
    return `${universe} · ${ts}`;
  };

  const saveVariantsBundle = async (overrideName?: string) => {
    const name = (overrideName ?? defaultVariantsBundleName()).trim();
    if (!name) return;
    const okEntries = VARIANT_DEFS.flatMap((v) => {
      const o = variants[v.key];
      return o?.status === 'ok' ? [{ def: v, result: o.result }] : [];
    });
    if (okEntries.length === 0) return;
    setSaving(true);
    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtests`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name,
          config: {
            start_date: `${startDate}-01`,
            end_date: `${endDate}-01`,
            signal_weights: weights,
            category_weights: categoryWeights,
            top_n_sectors: topSectors,
            top_n_per_sector: topPerSector,
            universe_label: null,
            index_universe: selectedIndexUniverse || null,
            selection_mode: 'momentum',
            random_seed: null,
            n_trials: 1,
          },
          variants: okEntries.map(({ def, result: r }) => ({
            key: def.key,
            label: def.label,
            frequency: def.frequency,
            strategy: def.strategy,
            summary: r.summary,
            monthly_records: r.monthly_records,
            // Persist the daily curve so the chart renders intra-period
            // moves on reload. Omitted from older saves; the loader falls
            // back to monthly when absent.
            daily_records: r.daily_records ?? [],
          })),
          universe,
        }),
      });
      if (resp.ok) {
        const saved = await resp.json();
        momentumStore.set({ loadedRunId: saved.run_id });
        loadSavedRuns();
      }
    } catch {}
    setSaving(false);
  };

  const loadBacktest = async (runId: number) => {
    setLoadingRunId(runId);
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

      // Variant bundle: rehydrate the sweep state instead of the single
      // `result`. The detail views (equity curve, holdings, sector
      // timeline) switch on `activeVariantKey`, so they pick this up
      // automatically.
      if (saved.kind === 'variants' && Array.isArray(saved.variants)) {
        const next: Partial<Record<VariantKey, VariantOutcome>> = {};
        let firstKey: VariantKey | null = null;
        for (const v of saved.variants) {
          const key = v?.key as VariantKey | undefined;
          if (!key) continue;
          next[key] = {
            status: 'ok',
            result: {
              summary: v.summary,
              monthly_records: v.monthly_records ?? [],
              daily_records: v.daily_records ?? [],
            },
          };
          if (firstKey == null) firstKey = key;
        }
        momentumStore.set({
          result: null,
          variants: next,
          activeVariantKey: firstKey,
          variantsRun: null,
          universe: saved.universe ?? [],
          loadedRunId: runId,
          error: null,
          warnings: [],
          infos: [],
          progress: [],
        });
        return;
      }

      // Single-run shape (legacy + default).
      momentumStore.set({
        result: {
          monthly_records: saved.monthly_records ?? [],
          daily_records: saved.daily_records ?? [],
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
        variants: {},
        activeVariantKey: null,
        variantsRun: null,
        universe: saved.universe ?? [],
        loadedRunId: runId,
        error: null,
        warnings: [],
        infos: [],
        progress: [],
      });
    } catch {
      momentumStore.set({ error: 'Failed to load backtest' });
    } finally {
      setLoadingRunId(null);
    }
  };

  const deleteBacktest = async (runId: number) => {
    setDeletingRunId(runId);
    if (loadedRunId === runId) momentumStore.set({ loadedRunId: null });
    try {
      await fetch(`${API_URL}/api/momentum/backtests/${runId}`, { method: 'DELETE' });
      setSavedRuns(prev => prev.filter(r => r.run_id !== runId));
    } catch {
      loadSavedRuns();
    } finally {
      setDeletingRunId(null);
    }
  };

  /** Display label for a current-picks snapshot. Prefers the user's custom
   * name when set; otherwise falls back to the auto-generated date/trigger
   * pair so the dropdown is never empty. */
  const snapshotLabel = (s: { name?: string | null; created_at: string; triggered_by: string; as_of_date: string }): string => {
    const trimmed = (s.name ?? '').trim();
    if (trimmed) return trimmed;
    return `${s.created_at.slice(0, 10)} · ${s.triggered_by} · ${s.as_of_date.slice(0, 7)}`;
  };

  const renameSnapshot = async (snapshotId: number, currentName: string | null | undefined) => {
    const next = await dialog.prompt('Name for this snapshot (leave empty to clear):', {
      title: 'Rename snapshot',
      defaultValue: currentName ?? '',
    });
    if (next == null) return; // user cancelled
    const trimmed = next.trim();
    if (trimmed === (currentName ?? '').trim()) return; // no change
    await renameCurrentPicksSnapshot(snapshotId, trimmed === '' ? null : trimmed);
  };

  const confirmDeleteSnapshot = async (s: { snapshot_id: number; name?: string | null; created_at: string; triggered_by: string; as_of_date: string }) => {
    const label = snapshotLabel(s);
    if (await dialog.confirm(`Delete snapshot "${label}"?`, { destructive: true, confirmLabel: 'Delete' })) {
      await deleteCurrentPicksSnapshot(s.snapshot_id);
    }
  };

  // Auto-save the variant bundle when a sweep finishes successfully. Fires
  // exactly once per sweep, keyed on `runStartedAt` so re-runs trigger again.
  // Skips if (a) the result was loaded from a saved run, (b) no variants
  // completed OK, or (c) a save is already in flight. The dropdown's
  // existing rename / delete buttons cover post-save tweaks.
  const lastAutoSavedRunStartedAtRef = useRef<number | null>(null);
  useEffect(() => {
    if (running) return;
    if (runEndedAt == null || runStartedAt == null) return;
    if (lastAutoSavedRunStartedAtRef.current === runStartedAt) return;
    if (loadedRunId != null) return; // already saved (loaded from disk or just saved)
    if (saving) return;
    const okCount = VARIANT_DEFS.filter((v) => variants[v.key]?.status === 'ok').length;
    if (okCount === 0) return;
    lastAutoSavedRunStartedAtRef.current = runStartedAt;
    void saveVariantsBundle();
    // The dependency list intentionally omits saveVariantsBundle (it
    // closes over many setters and would re-fire spuriously); the ref
    // guard ensures we only auto-save once per (runStartedAt) anyway.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [running, runEndedAt, runStartedAt, variants, loadedRunId, saving]);

  const renameBacktest = async (runId: number, currentName: string) => {
    const next = await dialog.prompt('New name for this backtest:', {
      title: 'Rename backtest',
      defaultValue: currentName,
    });
    if (!next || next.trim() === '' || next === currentName) return;
    setRenamingRunId(runId);
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
    } finally {
      setRenamingRunId(null);
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
        {currentPicksSnapshots.length > 0 && (
          <div className="relative" ref={picksDropdownRef}>
            <button
              type="button"
              onClick={() => setPicksDropdownOpen((o) => !o)}
              className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white flex items-center gap-2 hover:border-indigo-500 focus:outline-none focus:border-indigo-500 transition-colors min-w-[220px]"
              title="Load a saved current-picks snapshot"
            >
              {loadingSnapshotId != null && <Spinner />}
              <span className="truncate">
                {currentPortfolio?.snapshot_id != null
                  ? (() => {
                      const snap = currentPicksSnapshots.find((s) => s.snapshot_id === currentPortfolio.snapshot_id);
                      return snap ? snapshotLabel(snap) : 'Load saved current picks...';
                    })()
                  : 'Load saved current picks...'}
              </span>
              <svg className={`w-3.5 h-3.5 text-gray-500 ml-auto transition-transform ${picksDropdownOpen ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M5.23 7.21a.75.75 0 011.06.02L10 11.06l3.71-3.83a.75.75 0 111.08 1.04l-4.25 4.39a.75.75 0 01-1.08 0L5.21 8.27a.75.75 0 01.02-1.06z" clipRule="evenodd" />
              </svg>
            </button>
            {picksDropdownOpen && (
              <div className="absolute right-0 mt-1 w-80 bg-[#151821] border border-gray-700 rounded-lg shadow-xl z-50 max-h-96 overflow-auto">
                {currentPicksSnapshots.map((s) => {
                  const isActive = s.snapshot_id === currentPortfolio?.snapshot_id;
                  const isLoadingThis = loadingSnapshotId === s.snapshot_id;
                  const isDeletingThis = deletingSnapshotId === s.snapshot_id;
                  const isRenamingThis = renamingSnapshotId === s.snapshot_id;
                  const customName = (s.name ?? '').trim();
                  return (
                    <div
                      key={s.snapshot_id}
                      className={`group flex items-center gap-2 px-3 py-2 border-b border-gray-800/40 last:border-b-0 hover:bg-white/[0.03] transition-colors ${isActive ? 'bg-indigo-500/10' : ''}`}
                    >
                      <button
                        type="button"
                        onClick={() => { loadCurrentPicksSnapshot(s.snapshot_id); setPicksDropdownOpen(false); }}
                        disabled={isLoadingThis || isDeletingThis}
                        className="flex-1 text-left min-w-0 disabled:opacity-60"
                      >
                        <div className={`text-sm truncate flex items-center gap-1.5 ${isActive ? 'text-indigo-300' : 'text-gray-200'}`}>
                          {isLoadingThis && <Spinner />}
                          <span className="truncate">
                            {customName || `${s.created_at.slice(0, 16).replace('T', ' ')}`}
                          </span>
                        </div>
                        <div className="text-[10px] text-gray-500 font-mono">
                          {customName
                            ? `${s.created_at.slice(0, 10)} · ${s.triggered_by} · as of ${s.as_of_date.slice(0, 10)}`
                            : `${s.triggered_by} · as of ${s.as_of_date.slice(0, 10)}`}
                          {s.latest_price_date && <> · MTD through {s.latest_price_date}</>}
                        </div>
                      </button>
                      <button
                        type="button"
                        onClick={(e) => { e.stopPropagation(); renameSnapshot(s.snapshot_id, s.name); }}
                        disabled={isRenamingThis || isDeletingThis}
                        className="p-1.5 rounded text-gray-500 hover:text-indigo-400 hover:bg-white/5 opacity-0 group-hover:opacity-100 transition-opacity disabled:opacity-100 disabled:cursor-wait"
                        title="Rename"
                      >
                        {isRenamingThis ? (
                          <Spinner size={14} />
                        ) : (
                          <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                            <path d="M13.586 3.586a2 2 0 112.828 2.828l-.793.793-2.828-2.828.793-.793zM11.379 5.793L3 14.172V17h2.828l8.38-8.379-2.83-2.828z" />
                          </svg>
                        )}
                      </button>
                      <button
                        type="button"
                        onClick={(e) => { e.stopPropagation(); confirmDeleteSnapshot(s); }}
                        disabled={isDeletingThis || isRenamingThis}
                        className="p-1.5 rounded text-gray-500 hover:text-rose-400 hover:bg-rose-500/10 opacity-0 group-hover:opacity-100 transition-opacity disabled:opacity-100 disabled:cursor-wait"
                        title="Delete"
                      >
                        {isDeletingThis ? (
                          <Spinner size={14} />
                        ) : (
                          <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                            <path fillRule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clipRule="evenodd" />
                          </svg>
                        )}
                      </button>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}
        {savedRuns.length > 0 && (
          <div className="relative" ref={savedDropdownRef}>
            <button
              type="button"
              onClick={() => setSavedDropdownOpen((o) => !o)}
              className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white flex items-center gap-2 hover:border-indigo-500 focus:outline-none focus:border-indigo-500 transition-colors min-w-[220px]"
            >
              {loadingRunId != null && <Spinner />}
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
                  const isLoadingThis = loadingRunId === r.run_id;
                  const isDeletingThis = deletingRunId === r.run_id;
                  const isRenamingThis = renamingRunId === r.run_id;
                  return (
                    <div
                      key={r.run_id}
                      className={`group flex items-center gap-2 px-3 py-2 border-b border-gray-800/40 last:border-b-0 hover:bg-white/[0.03] transition-colors ${isActive ? 'bg-indigo-500/10' : ''}`}
                    >
                      <button
                        type="button"
                        onClick={() => { loadBacktest(r.run_id); setSavedDropdownOpen(false); }}
                        disabled={isLoadingThis || isDeletingThis}
                        className="flex-1 text-left min-w-0 disabled:opacity-60"
                      >
                        <div className={`text-sm truncate flex items-center gap-1.5 ${isActive ? 'text-indigo-300' : 'text-gray-200'}`}>
                          {isLoadingThis && <Spinner />}
                          <span className="truncate">{r.name}</span>
                        </div>
                        <div className="text-[10px] text-gray-500 font-mono">{new Date(r.created_at).toLocaleDateString()}</div>
                      </button>
                      <button
                        type="button"
                        onClick={(e) => { e.stopPropagation(); renameBacktest(r.run_id, r.name); }}
                        disabled={isRenamingThis || isDeletingThis}
                        className="p-1.5 rounded text-gray-500 hover:text-indigo-400 hover:bg-white/5 opacity-0 group-hover:opacity-100 transition-opacity disabled:opacity-100 disabled:cursor-wait"
                        title="Rename"
                      >
                        {isRenamingThis ? (
                          <Spinner size={14} />
                        ) : (
                          <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                            <path d="M13.586 3.586a2 2 0 112.828 2.828l-.793.793-2.828-2.828.793-.793zM11.379 5.793L3 14.172V17h2.828l8.38-8.379-2.83-2.828z" />
                          </svg>
                        )}
                      </button>
                      <button
                        type="button"
                        onClick={async (e) => {
                          e.stopPropagation();
                          if (await dialog.confirm(`Delete "${r.name}"?`, { destructive: true, confirmLabel: 'Delete' })) {
                            deleteBacktest(r.run_id);
                          }
                        }}
                        disabled={isDeletingThis || isRenamingThis}
                        className="p-1.5 rounded text-gray-500 hover:text-rose-400 hover:bg-rose-500/10 opacity-0 group-hover:opacity-100 transition-opacity disabled:opacity-100 disabled:cursor-wait"
                        title="Delete"
                      >
                        {isDeletingThis ? (
                          <Spinner size={14} />
                        ) : (
                          <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                            <path fillRule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clipRule="evenodd" />
                          </svg>
                        )}
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
            {(() => {
              const eligibleCount = VARIANT_DEFS
                .filter((v) => selectedVariantKeys.has(v.key))
                .filter((v) => selectionMode !== 'random' || v.strategy !== 'long_short')
                .length;
              return (
                <div className="relative inline-flex" ref={variantsPickerRef}>
                  <button
                    onClick={runVariantsBacktest}
                    disabled={running || variantsRunning || eligibleCount === 0}
                    className="px-5 py-2 rounded-l-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                    title={
                      eligibleCount === 0
                        ? selectionMode === 'random'
                          ? 'Random selection requires at least one long-only variant (long-short is unsupported in random mode)'
                          : 'Select at least one variant to run'
                        : `Run ${eligibleCount} selected variant${eligibleCount === 1 ? '' : 's'} and compare them in one table`
                    }
                  >
                    {variantsRunning
                      ? `Running variants ${variantsRun?.completed ?? 0}/${variantsRun?.total ?? 0}…`
                      : `Run variants (${eligibleCount})`}
                  </button>
                  <button
                    type="button"
                    onClick={() => setVariantsPickerOpen((o) => !o)}
                    disabled={running || variantsRunning}
                    className="px-2 py-2 rounded-r-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                    title="Choose which variants to include in the sweep"
                    aria-label="Choose variants"
                  >
                    <svg className={`w-3.5 h-3.5 transition-transform ${variantsPickerOpen ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                      <path fillRule="evenodd" d="M5.23 7.21a.75.75 0 011.06.02L10 11.06l3.71-3.83a.75.75 0 111.08 1.04l-4.25 4.39a.75.75 0 01-1.08 0L5.21 8.27a.75.75 0 01.02-1.06z" clipRule="evenodd" />
                    </svg>
                  </button>
                  {variantsPickerOpen && (
                    <div className="absolute top-full left-0 mt-1 w-72 bg-[#151821] border border-gray-700 rounded-lg shadow-xl z-50 p-3">
                      <div className="flex items-center justify-between mb-2">
                        <span className="text-xs font-medium text-gray-300">Variants to run</span>
                        <div className="flex items-center gap-2 text-[11px]">
                          <button
                            type="button"
                            onClick={() => setSelectedVariantKeys(new Set(VARIANT_DEFS.map((v) => v.key)))}
                            className="text-indigo-400 hover:text-indigo-300"
                          >
                            All
                          </button>
                          <span className="text-gray-700">·</span>
                          <button
                            type="button"
                            onClick={() => setSelectedVariantKeys(new Set())}
                            className="text-gray-400 hover:text-gray-200"
                          >
                            None
                          </button>
                        </div>
                      </div>
                      {selectionMode === 'random' && (
                        <div className="mb-2 px-2 py-1.5 text-[10px] text-amber-300/80 bg-amber-500/5 border border-amber-500/20 rounded">
                          Long-short is disabled in random mode (no signal-driven score to short on).
                        </div>
                      )}
                      <ul className="space-y-1 max-h-72 overflow-auto">
                        {VARIANT_DEFS.map((v) => {
                          const checked = selectedVariantKeys.has(v.key);
                          const disabled = selectionMode === 'random' && v.strategy === 'long_short';
                          return (
                            <li key={v.key}>
                              <label className={`flex items-center gap-2 px-2 py-1.5 rounded text-xs ${disabled ? 'text-gray-600 cursor-not-allowed' : 'text-gray-300 hover:bg-white/5 cursor-pointer'}`}>
                                <input
                                  type="checkbox"
                                  checked={checked && !disabled}
                                  disabled={disabled}
                                  onChange={() => toggleVariantKey(v.key)}
                                  className="accent-indigo-500 w-3.5 h-3.5 cursor-pointer disabled:cursor-not-allowed"
                                />
                                <span>{v.label}</span>
                              </label>
                            </li>
                          );
                        })}
                      </ul>
                    </div>
                  )}
                </div>
              );
            })()}
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
            {running && !variantsRunning && (
              <button
                onClick={cancelBacktest}
                className="px-4 py-2 rounded-lg text-sm font-medium text-gray-400 hover:text-rose-400 hover:bg-rose-500/10 transition-colors"
              >
                Cancel
              </button>
            )}
            {variantsRunning && (
              <button
                onClick={cancelVariantsBacktest}
                className="px-4 py-2 rounded-lg text-sm font-medium text-gray-400 hover:text-rose-400 hover:bg-rose-500/10 transition-colors"
              >
                Cancel variants
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
            log={progressLog}
            pct={progress[progress.length - 1]?.pct ?? 0}
            errorMessage={error}
            running={running}
            defaultLogOpen
            title="Backtest progress"
            totalElapsedMs={totalElapsedMs}
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
        {currentPortfolio && (() => {
          const portMTD = (() => {
            if (currentPortfolio.holdings.length === 0) return null;
            const validReturns = currentPortfolio.holdings
              .map(h => h.forward_return_pct)
              .filter((r): r is number => r != null);
            if (validReturns.length === 0) return null;
            return validReturns.reduce((a, b) => a + b, 0) / validReturns.length;
          })();
          return (
          <CollapsibleCard
            title={
              <div className="min-w-0">
                <div>Current Picks</div>
                <div className="text-xs text-gray-500 font-normal mt-0.5">
                  Rebalance as of <span className="font-mono text-gray-400">{currentPortfolio.as_of_date}</span>
                  {currentPortfolio.latest_price_date && (
                    <> · MTD through <span className="font-mono text-gray-400">{currentPortfolio.latest_price_date}</span></>
                  )}
                  {' · '}{currentPortfolio.holdings.length} holdings
                </div>
              </div>
            }
            rightSlot={
              <>
                {/* Refresh MTD button — only meaningful when a saved snapshot is loaded */}
                {currentPortfolio.snapshot_id != null && (
                  <button
                    onClick={(e) => { e.stopPropagation(); refreshCurrentPicksMTD(currentPortfolio.snapshot_id!); }}
                    disabled={refreshingMTD || running}
                    className="px-2.5 py-1 rounded-lg text-xs font-medium border border-gray-700 text-gray-300 hover:bg-white/5 hover:text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed inline-flex items-center gap-1.5"
                    title="Refresh month-to-date returns using the latest available prices (does not re-run signals)"
                  >
                    {refreshingMTD ? <Spinner /> : <span className="text-emerald-400">✓</span>}
                    {refreshingMTD ? 'Refreshing…' : 'Refresh MTD'}
                  </button>
                )}
                {/* Force a new full compute */}
                <button
                  onClick={(e) => { e.stopPropagation(); recomputeCurrentPortfolio(); }}
                  disabled={running}
                  className="px-2.5 py-1 rounded-lg text-xs font-medium border border-gray-700 text-gray-300 hover:bg-white/5 hover:text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed inline-flex items-center gap-1.5"
                  title="Run the full strategy now and save a new snapshot (slow)"
                >
                  {running && <Spinner />}
                  Recompute
                </button>
                {portMTD != null && (
                  <div className="text-right ml-2">
                    <div className="text-[10px] text-gray-500">Portfolio MTD</div>
                    <div className={`text-base font-mono font-medium ${portMTD >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                      {portMTD >= 0 ? '+' : ''}{portMTD.toFixed(2)}%
                    </div>
                  </div>
                )}
              </>
            }
          >
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
          </CollapsibleCard>
          );
        })()}

        {/* Variant sweep summary — appears as soon as one variant outcome
            lands and stays visible alongside the active variant's detail
            views below. Hidden entirely when no sweep has run. */}
        {hasVariants && <VariantSummaryTable />}

        {/* Results — either the single-run `result` or, when a variant is
            active, that variant's BacktestResult. The detail components
            don't care which path the data came from. */}
        {displayResult && (
          <>
            <EquityCurveCard
              result={displayResult}
              loadedRunId={activeVariantResult ? null : loadedRunId}
              savedRuns={savedRuns}
            />

            <SectorTimelineChart result={displayResult} />

            <MonthlyHoldingsTable
              result={displayResult}
              categories={categories}
              exchangeByCompany={exchangeByCompany}
              scoringConfig={{
                universe_label: null,
                index_universe: selectedIndexUniverse || null,
                signal_weights: weights,
                category_weights: categoryWeights,
              }}
            />

            {/* Auto-save status. Variant sweeps + single-strategy runs are
                saved automatically with a default name when they finish.
                The pill shows the resulting name; rename / delete are in
                the saved-backtests dropdown in the page header. */}
            {saving && hasVariants && (
              <div className="bg-[#151821] border border-gray-800/40 rounded-lg px-4 py-3 text-gray-400 text-sm flex items-center gap-2">
                <Spinner />
                <span>Auto-saving variant bundle…</span>
              </div>
            )}
            {loadedRunId && (
              <div className="bg-indigo-500/10 border border-indigo-500/20 rounded-lg px-4 py-3 text-indigo-400 text-sm flex items-center gap-2">
                <span>Saved as</span>
                <span className="text-indigo-300 font-medium">
                  {savedRuns.find((r) => r.run_id === loadedRunId)?.name}
                </span>
                {hasVariants && (
                  <span className="text-indigo-400/70 text-xs">
                    · {Object.keys(variants).length} variants
                  </span>
                )}
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
