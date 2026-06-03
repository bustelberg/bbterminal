'use client';

import { Fragment, useState, useEffect, useMemo, useRef } from 'react';

import ApiUsageBadge from './ApiUsageBadge';
import LoadingDots from './LoadingDots';
import Spinner from './Spinner';
import { API_URL } from '../../lib/apiUrl';
import ProgressTimeline from './ProgressTimeline';
import NotificationsPanel from './momentum/NotificationsPanel';
import { useClickOutside } from '../../lib/hooks/useClickOutside';
import {
  useCompanyExchangeMap,
  useUniverseTemplates,
} from '../../lib/hooks/apiData';
import {
  momentumStore,
  cancelBacktest,
  cancelVariantsBacktest,
  loadCurrentPicksSnapshots,
  loadCurrentPicksSnapshot,
  refreshCurrentPicksMTD,
  VARIANT_DEFS,
  parseVariantKey,
  variantLabel,
  type VariantKey,
} from '../../lib/stores/momentum';
import CellInfoTip from './momentum/CellInfoTip';
import CollapsibleCard from './momentum/CollapsibleCard';
import DailyPicksHistory from './momentum/DailyPicksHistory';
import DateRangeRow from './momentum/DateRangeRow';
import FeeWaterfallPanel from './momentum/FeeWaterfallPanel';
import EquityCurveCard from './momentum/EquityCurveCard';
import MonthlyHoldingsTable from './momentum/MonthlyHoldingsTable';
import RandomParamsInputs from './momentum/RandomParamsInputs';
import RunControls from './momentum/RunControls';
import SavedRunsDropdown from './momentum/SavedRunsDropdown';
import SectorTimelineChart from './momentum/SectorTimelineChart';
import SignalWeightSliders from './momentum/SignalWeightSliders';
import StrategyModeSelect from './momentum/StrategyModeSelect';
import VariantAttribution from './momentum/VariantAttribution';
import VariantsPanel from './momentum/VariantsPanel';
import TableDownloadButton from './TableDownloadButton';
import VariantSummaryTable from './momentum/VariantSummaryTable';
import { useBacktestConfig } from './momentum/useBacktestConfig';
import { useBacktestRun } from './momentum/useBacktestRun';
import { useCurrentPicksSnapshots } from './momentum/useCurrentPicksSnapshots';
import { useSavedRuns } from './momentum/useSavedRuns';
import { useVariantsBundle } from './momentum/useVariantsBundle';
import { useSectorEtfs } from './momentum/useSectorEtfs';
import { useVariantSelection } from './momentum/useVariantSelection';
import {
  EXCHANGE_NAMES,
  fmtPct,
  fmtPrice,
  guruFocusUrl,
} from './momentum/utils';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** A single column of the Variants picker — header with All/None links,
 * then a bordered scrollable list of items rendered via the caller's
 * `renderItem` callback. Generic over the option type so the same shell
 * carries frequencies (strings), strategies (StrategyType), universes
 * (strings), and groupings ('sector' | 'industry'). */
// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function MomentumBacktester() {
  // Core backtest configuration (signal/category weights, dates, sizing,
  // grouping, selection mode, random-baseline knobs) lives in its own
  // hook so this component stops owning ~15 useState slots + the
  // signal-defaults effect. Each value comes back with the setter the
  // config panel, the universe date-autofill effect, and the saved-config
  // loader write through.
  const config = useBacktestConfig();
  const {
    signalDefs,
    weights, setWeights,
    categories,
    categoryWeights, setCategoryWeights,
    startDate, setStartDate,
    endDate, setEndDate,
    topSectors,
    topPerSector,
    noCache, setNoCache,
    maxCompanies, setMaxCompanies,
    minPriceScore,
    selectionMode, setSelectionMode,
    randomSeed, setRandomSeed,
    nTrials, setNTrials,
    rebalanceWeekday, setRebalanceWeekday,
  } = config;

  // Sector → benchmark_id mapping for selection_mode='sector_etf'.
  // Encapsulated in `useSectorEtfs`: lazy-loads from /api/benchmarks
  // only when sector_etf mode is active and refreshes whenever the user
  // pops back to that mode. `setSectorEtfs` is exposed so the saved-
  // config loader downstream can overwrite the map directly.
  const {
    sectorEtfs,
    setSectorEtfs,
    sectorEtfsLoading,
    sectorEtfsError,
  } = useSectorEtfs({ active: selectionMode === 'sector_etf' });

  // Variant sweep selection — the 5-axis cross-product picker. Each
  // permutation in the cartesian product becomes one VariantParams sent
  // to the backend. All state + derived cross-product math lives in
  // `useVariantSelection` so it's testable in isolation. The base
  // config's top_n / per_sector flow in so `variantSize` can fall back
  // to them when a variant leaves the axis undefined.
  const variantSel = useVariantSelection({ topSectors, topPerSector });
  // `variantSel` is passed whole to <VariantsPanel/>, <useVariantsBundle>, and
  // <useSavedRuns> (which write through its setters). This component reads
  // only `selectedUniverses` (the universe date-autofill effect) and
  // `allPermutations` / `variantsToRun` (the totalPerms / eligibleVariants
  // derivations below).
  const {
    selectedUniverses,
    allPermutations, variantsToRun,
  } = variantSel;
  // Backend rejects `long_short` + `random` (long-short without a
  // signal-driven score is meaningless), so when Random is selected we
  // hide the long-short rows from the picker and only run long-only
  // (enforced by the `longShortBlocked` guard below + in VariantsPanel).

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

  // Total elapsed for the panel header. Intentionally NOT memoized —
  // `setNowTick` re-renders us every second to keep this fresh, but a
  // useMemo with [running, runStartedAt, runEndedAt] deps would cache
  // the first-render value forever (none of those change on a tick),
  // freezing the badge at the initial mount time. Inline math is
  // cheap; recompute every render.
  const totalElapsedMs: number | null =
    runStartedAt == null
      ? null
      : (running ? Date.now() : (runEndedAt ?? Date.now())) - runStartedAt;

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

  // Live company directory loaded on mount — used as a fallback exchange
  // source when the active backtest's universe payload is missing the
  // link (saved variant bundles from before the snapshot-normalization
  // fix carry empty/None exchange strings, so ORCL/NYSE etc. would
  // otherwise render as "—" in the holdings table even though the GF
  // URL helper still resolves correctly via the bare-ticker fallback).
  // Shared cached fetch (deduped with SnapshotHoldings when both render).
  const companyExchangeMap = useCompanyExchangeMap();

  const exchangeByCompany = useMemo(() => {
    const m = new Map<number, string>();
    // Primary source: the universe payload bundled with the active
    // backtest result. Skip junk strings ("None"/"nan") so the company
    // map below can fill in.
    for (const u of universe) {
      const e = (u.exchange ?? '').trim();
      if (!e) continue;
      const upper = e.toUpperCase();
      if (upper === 'NONE' || upper === 'NAN' || upper === 'NULL') continue;
      m.set(u.company_id, e);
    }
    // Fallback: live company directory. Only fills in entries the
    // universe didn't supply, so a fresh run still wins over the static
    // directory if anything was renamed.
    for (const [cid, exch] of companyExchangeMap) {
      if (!m.has(cid)) m.set(cid, exch);
    }
    return m;
  }, [universe, companyExchangeMap]);

  // Purely local UI state — safe to reset on navigation
  const [showWarnings, setShowWarnings] = useState(true);
  const [showInfos, setShowInfos] = useState(false);

  // Save/load state

  const [picksDropdownOpen, setPicksDropdownOpen] = useState(false);
  const picksDropdownRef = useRef<HTMLDivElement>(null);
  // First-fetch loading state for the current-picks dropdown (the saved-
  // backtests dropdown has its own `savedRunsLoading` further down). Both
  // header dropdowns render unconditionally and surface this as a spinner
  // + "Loading saved …" label, instead of disappearing until data lands.
  const [picksListLoading, setPicksListLoading] = useState(true);

  // Per-row spinners for the saved-backtests dropdown. The current-picks
  // dropdown reads its equivalents from the store so the actions can also
  // be driven from elsewhere (e.g. an inline rename in the picks card).
  const loadingSnapshotId = momentumStore.use((s) => s.loadingSnapshotId);
  const deletingSnapshotId = momentumStore.use((s) => s.deletingSnapshotId);
  const renamingSnapshotId = momentumStore.use((s) => s.renamingSnapshotId);
  // Multi-select set for the current-picks dropdown's bulk delete. The
  // saved-backtests dropdown owns its own selection internally now (the
  // component was extracted into SavedRunsDropdown.tsx).
  const {
    selectedSnapshotIds, setSelectedSnapshotIds, bulkDeletingSnapshots,
    snapshotLabel, renameSnapshot, confirmDeleteSnapshot, bulkDeleteSnapshots, toggleSnapshotSelected,
  } = useCurrentPicksSnapshots();

  // Universe selection state — only TEMPLATE-MANAGED universes are
  // listed here (currently just ACWI). User-created static snapshots
  // and the SP500 legacy import are intentionally hidden: the only
  // universes that backtests should run against are the ones the
  // pipeline keeps continuously up-to-date.
  // Source: GET /api/universe-templates — one entry per registered
  // template, mapped into the {index_name, hard_backstop, ...} shape
  // the rest of this page is wired for.
  const [indexUniverses, setIndexUniverses] = useState<{
    index_name: string;
    /** Human-readable display name (template.label). Falls back to
     * `index_name` when the API doesn't provide one. The dropdown
     * shows this; the form value still uses `index_name`
     * (= template_key) so the backend lookup is stable. */
    display_label: string;
    /** The template's permanent earliest date (e.g. ACWI: 2002-01-01).
     * Used as the default backtest start when this universe is picked.
     * Different from `start_month` (latest data captured so far), which
     * we ignore now that we have the proper hard-backstop value. */
    hard_backstop: string;
    start_month: string;
    end_month: string;
    month_count: number;
    total_unique_tickers: number;
  }[]>([]);
  const [selectedIndexUniverse, setSelectedIndexUniverse] = useState<string>('');
  // The Universe `<select>` used to show "loading X (Ns)" + an error
  // label — those signals lived in the now-removed dropdown. State is
  // removed; the AxisColumn for universes below reads the loaded list
  // straight from `indexUniverses` and just renders nothing while it's
  // empty.
  // Latest available close-price date, fed by GET /api/data/latest-price-date.
  // Cached for the lifetime of the page mount — the data refresh runs
  // weekly, so revalidating per-render is wasteful.
  const [latestPriceDate, setLatestPriceDate] = useState<string | null>(null);

  // Memoized so `MonthlyHoldingsTable`'s React.memo barrier holds —
  // an inline `{...}` literal would create a fresh object reference
  // every parent re-render, busting shallow-equality on the props
  // and forcing the (heavy) table to re-render even when nothing it
  // actually consumes has changed.
  const scoringConfig = useMemo(() => ({
    universe_label: null as string | null,
    index_universe: selectedIndexUniverse || null,
    signal_weights: weights,
    category_weights: categoryWeights,
  }), [selectedIndexUniverse, weights, categoryWeights]);

  // Universe templates — shared cached hook. The Variants AxisColumn
  // wants the locally-shaped `IndexUniverseEntry` so we map after load.
  const { data: _utRaw } = useUniverseTemplates();
  useEffect(() => {
    if (!_utRaw) return;
    setIndexUniverses(
      _utRaw.map((t) => ({
        index_name: t.template_key,
        display_label: t.label || t.template_key,
        hard_backstop: t.earliest_date,
        start_month: t.earliest_captured_month!,
        end_month: t.latest_captured_month!,
        month_count: t.months_captured,
        // No cheap COUNT DISTINCT yet for "ever in this universe", so
        // the dropdown shows the latest month's count as a representative
        // number. Good enough for "is this the right universe?".
        total_unique_tickers: t.latest_membership_count,
      })),
    );
  }, [_utRaw]);

  // One-time bookkeeping at mount: saved runs, current-picks snapshots,
  // and the latest-price-date fetch. The endpoint hooks above own all
  // the recurring fetches.
  useEffect(() => {
    loadSavedRuns();
    setPicksListLoading(true);
    loadCurrentPicksSnapshots().finally(() => setPicksListLoading(false));
    fetch(`${API_URL}/api/data/latest-price-date`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`${r.status}`))))
      .then((d: { date: string | null }) => setLatestPriceDate(d.date))
      .catch(() => { /* silent — the user can still pick a date manually */ });
    return () => undefined;
    // Intentionally mount-only — `loadSavedRuns` (from useSavedRuns) is a
    // fresh reference each render, so including it would re-fire every render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // When universe selection changes, auto-set start/end dates per the
  // /backtest convention: start = the universe's hard backstop (its
  // permanent earliest date — e.g. ACWI: 2002-01), end = the latest
  // close-price date we have data for (independent of universe).
  // Both are stored as YYYY-MM-DD on the server; the `<input
  // type="month">` wants YYYY-MM, so slice.
  // When the user picks exactly one universe in the Variants panel
  // below, adopt its hard-backstop as the default start date so the
  // backtest doesn't try to look further back than the universe was
  // first captured. End date follows the latest available price.
  // Triggered by changes to `selectedUniverses` rather than the
  // (now-removed) top-row Universe `<select>`. We only auto-adjust on
  // single-selection so a multi-universe sweep doesn't yank the dates
  // around each toggle.
  useEffect(() => {
    if (selectedUniverses.size !== 1) return;
    const only = Array.from(selectedUniverses)[0];
    const entry = indexUniverses.find((i) => i.index_name === only);
    if (!entry) return;
    setStartDate(entry.hard_backstop.slice(0, 7));
    setEndDate((latestPriceDate ?? entry.end_month).slice(0, 7));
    // setStartDate / setEndDate are referentially-stable hook setters, so
    // listing them doesn't widen the effect's re-run scope.
  }, [selectedUniverses, indexUniverses, latestPriceDate, setStartDate, setEndDate]);

  useClickOutside(picksDropdownRef, () => setPicksDropdownOpen(false), picksDropdownOpen);

  // Reset current-picks multi-select when its dropdown closes — selection
  // should not persist across opens. The saved-backtests dropdown manages
  // its own selection internally.
  useEffect(() => {
    if (!picksDropdownOpen) setSelectedSnapshotIds(new Set());
  }, [picksDropdownOpen, setSelectedSnapshotIds]);

  // null = first fetch in flight; [] = loaded but empty; non-empty array =
  // populated. The header dropdown reads this to show a spinner + "Loading
  // saved backtests…" instead of disappearing while the request is in
  // flight (which previously made the dropdown look like it materialized
  // out of nowhere when the response landed).
  const {
    savedRuns, savedRunsLoading, loadingRunId, deletingRunId, renamingRunId, bulkDeletingRuns,
    loadSavedRuns, loadBacktest, deleteBacktest, bulkDeleteRuns, renameBacktest,
  } = useSavedRuns({ config, variantSel, setSectorEtfs, setSelectedIndexUniverse });


  // Long-short variants are filtered out when the selection mode
  // doesn't produce a meaningful top vs bottom split (random / all-
  // universe / sector_etf all share the same constraint — backend
  // rejects long-short under those modes). Computed here so the
  // permutations preview can disable + grey those rows, AND so
  // `runVariantsBacktest` only ships the eligible ones.
  const longShortBlocked =
    selectionMode === 'random' || selectionMode === 'all' || selectionMode === 'sector_etf';
  const eligibleVariants = useMemo(
    () => variantsToRun.filter((v) => !longShortBlocked || v.strategy !== 'long_short'),
    [variantsToRun, longShortBlocked],
  );
  const eligibleCount = eligibleVariants.length;
  const totalPerms = allPermutations.length;
  // Pre-flight check the Run button's tooltip / Banner surface to the
  // user. Today's only fail-fast is "no universe selected" — without
  // that there's nothing for the per-combo loader to fetch.
  const variantsBlockReason: string | null =
    selectedUniverses.size === 0 ? 'Pick at least one universe.' : null;

  // Run / current-portfolio orchestration — assembles the request from
  // the current config + selections and dispatches to the store. Lives in
  // `useBacktestRun` so this component just wires the buttons to it.
  const {
    runVariantsBacktest,
    showCurrentPicks,
    recomputeCurrentPortfolio,
  } = useBacktestRun({ config, selectedIndexUniverse, sectorEtfs, eligibleVariants });

  const {
    saving,
    handleAddVariantToSchedule,
    saveVariantsBundle,
    defaultVariantsBundleName,
  } = useVariantsBundle({ config, variantSel, selectedIndexUniverse, sectorEtfs, loadSavedRuns });



  /** Display label for a current-picks snapshot. Prefers the user's custom
   * name when set; otherwise falls back to the auto-generated date/trigger
   * pair so the dropdown is never empty. */

  // Auto-save the variant bundle when a sweep finishes successfully. Fires
  // exactly once per sweep, keyed on `runStartedAt` so re-runs trigger again.
  // Skips if (a) the result was loaded from a saved run, (b) no variants
  // completed OK, or (c) a save is already in flight. The dropdown's
  // existing rename / delete buttons cover post-save tweaks.
  const lastAutoSavedRunStartedAtRef = useRef<number | null>(null);
  useEffect(() => {
    if (running) return;
    if (runEndedAt == null || runStartedAt == null) return;
    if (lastAutoSavedRunStartedAtRef.current === runStartedAt) {
      // already auto-saved this run — nothing to log, this is the
      // steady-state path on every re-render after the save.
      return;
    }
    if (loadedRunId != null) {
      // Skipping because the result was loaded from disk (or another
      // auto-save already landed). Surface the reason so a user
      // wondering "why didn't my run auto-save" can see in the console.
      console.info('[momentum] auto-save skipped: loadedRunId already set', { loadedRunId });
      lastAutoSavedRunStartedAtRef.current = runStartedAt;
      return;
    }
    if (saving) return;
    // Iterate `variants` directly so cross-product keys (e.g.
    // `monthly__long_only__s4__p6`) are counted alongside legacy
    // 2-segment ones. Splitting okKeys/errKeys lets the warn log spell
    // out which variants the auto-save bailed on.
    const okKeys: string[] = [];
    const errKeys: string[] = [];
    for (const [k, o] of Object.entries(variants)) {
      if (o?.status === 'ok') okKeys.push(k);
      else if (o?.status === 'error') errKeys.push(k);
    }
    if (okKeys.length === 0) {
      console.warn('[momentum] auto-save skipped: 0 successful variants', {
        attempted: Object.keys(variants).length,
        ok: 0,
        errored: errKeys.length,
        errors: errKeys.map((k) => ({ key: k, msg: (variants[k as VariantKey] as { message?: string })?.message })),
      });
      lastAutoSavedRunStartedAtRef.current = runStartedAt;
      return;
    }
    lastAutoSavedRunStartedAtRef.current = runStartedAt;
    void saveVariantsBundle();
    // The dependency list intentionally omits saveVariantsBundle (it
    // closes over many setters and would re-fire spuriously); the ref
    // guard ensures we only auto-save once per (runStartedAt) anyway.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [running, runEndedAt, runStartedAt, variants, loadedRunId, saving]);


  return (
    <div className="flex flex-col h-full overflow-x-hidden">
      {/* Header */}
      <div className="px-8 py-5 border-b border-gray-800/60 flex flex-wrap items-center justify-between gap-y-3">
        <div className="min-w-0">
          <h1 className="text-lg font-semibold text-white">Momentum Backtester</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            Price momentum portfolio — equal-weight, monthly rebalancing, sector-filtered
          </p>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-3 min-w-0">
          <ApiUsageBadge />
        {(() => {
          const picksEmpty = !picksListLoading && currentPicksSnapshots.length === 0;
          const triggerLabel = picksListLoading
            ? <LoadingDots label="Loading saved current picks" />
            : picksEmpty
              ? 'No saved current picks yet'
              : currentPortfolio?.snapshot_id != null
                ? (() => {
                    const snap = currentPicksSnapshots.find((s) => s.snapshot_id === currentPortfolio.snapshot_id);
                    return snap ? snapshotLabel(snap) : 'Load saved current picks...';
                  })()
                : 'Load saved current picks...';
          return (
          <div className="relative" ref={picksDropdownRef}>
            <button
              type="button"
              onClick={() => { if (!picksListLoading && !picksEmpty) setPicksDropdownOpen((o) => !o); }}
              disabled={picksListLoading || picksEmpty}
              className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white flex items-center gap-2 hover:border-indigo-500 focus:outline-none focus:border-indigo-500 transition-colors min-w-[220px] disabled:opacity-70 disabled:cursor-default disabled:hover:border-gray-700"
              title="Load a saved current-picks snapshot"
            >
              {(picksListLoading || loadingSnapshotId != null) && <Spinner />}
              <span className="truncate">{triggerLabel}</span>
              <svg className={`w-3.5 h-3.5 text-gray-500 ml-auto transition-transform ${picksDropdownOpen ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M5.23 7.21a.75.75 0 011.06.02L10 11.06l3.71-3.83a.75.75 0 111.08 1.04l-4.25 4.39a.75.75 0 01-1.08 0L5.21 8.27a.75.75 0 01.02-1.06z" clipRule="evenodd" />
              </svg>
            </button>
            {picksDropdownOpen && (
              <div className="absolute right-0 mt-1 w-max min-w-[280px] max-w-[90vw] bg-[#151821] border border-gray-700 rounded-lg shadow-xl z-50 max-h-96 overflow-auto">
                {selectedSnapshotIds.size > 0 && (
                  <div className="sticky top-0 z-10 bg-[#1a1d27] border-b border-gray-700 px-3 py-2 flex items-center justify-between gap-2">
                    <span className="text-xs text-gray-300">
                      {selectedSnapshotIds.size} selected
                    </span>
                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() => setSelectedSnapshotIds(new Set())}
                        className="text-[11px] text-gray-500 hover:text-gray-300 px-2 py-1 rounded transition-colors"
                      >
                        clear
                      </button>
                      <button
                        type="button"
                        onClick={bulkDeleteSnapshots}
                        disabled={bulkDeletingSnapshots}
                        className="text-[11px] font-medium px-2 py-1 rounded bg-rose-500/15 text-rose-300 border border-rose-500/30 hover:bg-rose-500/25 transition-colors disabled:opacity-50 disabled:cursor-not-allowed inline-flex items-center gap-1.5"
                      >
                        {bulkDeletingSnapshots && <Spinner size={12} />}
                        Delete {selectedSnapshotIds.size}
                      </button>
                    </div>
                  </div>
                )}
                {currentPicksSnapshots.map((s) => {
                  const isActive = s.snapshot_id === currentPortfolio?.snapshot_id;
                  const isLoadingThis = loadingSnapshotId === s.snapshot_id;
                  const isDeletingThis = deletingSnapshotId === s.snapshot_id;
                  const isRenamingThis = renamingSnapshotId === s.snapshot_id;
                  const isSelected = selectedSnapshotIds.has(s.snapshot_id);
                  const customName = (s.name ?? '').trim();
                  return (
                    <div
                      key={s.snapshot_id}
                      className={`group flex items-center gap-2 px-3 py-2 border-b border-gray-800/40 last:border-b-0 hover:bg-white/[0.03] transition-colors ${isActive ? 'bg-indigo-500/10' : ''} ${isSelected ? 'bg-rose-500/[0.06]' : ''}`}
                    >
                      <input
                        type="checkbox"
                        checked={isSelected}
                        onChange={(e) => { e.stopPropagation(); toggleSnapshotSelected(s.snapshot_id); }}
                        onClick={(e) => e.stopPropagation()}
                        className="accent-indigo-500 w-3.5 h-3.5 shrink-0 cursor-pointer"
                        title="Select for bulk delete"
                      />
                      <button
                        type="button"
                        onClick={() => { loadCurrentPicksSnapshot(s.snapshot_id); setPicksDropdownOpen(false); }}
                        disabled={isLoadingThis || isDeletingThis}
                        className="flex-1 text-left disabled:opacity-60"
                      >
                        <div className={`text-sm flex items-center gap-1.5 whitespace-nowrap ${isActive ? 'text-indigo-300' : 'text-gray-200'}`}>
                          {isLoadingThis && <Spinner />}
                          <span>
                            {customName || `${s.created_at.slice(0, 16).replace('T', ' ')}`}
                          </span>
                        </div>
                        <div className="text-[10px] text-gray-500 font-mono whitespace-nowrap">
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
          );
        })()}
        <SavedRunsDropdown
          savedRuns={savedRuns}
          loading={savedRunsLoading}
          loadedRunId={loadedRunId}
          loadingRunId={loadingRunId}
          deletingRunId={deletingRunId}
          renamingRunId={renamingRunId}
          bulkDeleting={bulkDeletingRuns}
          onLoad={loadBacktest}
          onDelete={deleteBacktest}
          onRename={renameBacktest}
          onBulkDelete={bulkDeleteRuns}
        />
        </div>
      </div>

      <div className="flex-1 overflow-y-auto overflow-x-hidden px-8 py-5 space-y-5">
        {/* Config Panel */}
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
          <div className="flex flex-wrap items-end gap-5 mb-5">
            {/* Universe is now picked per-variant in the Variants panel
                below — no top-level dropdown. The first selected variant
                drives the base config's `index_universe` for cache
                identity (see runVariantsBacktest). */}
            <DateRangeRow
              startDate={startDate}
              setStartDate={setStartDate}
              endDate={endDate}
              setEndDate={setEndDate}
              maxCompanies={maxCompanies}
              setMaxCompanies={setMaxCompanies}
              rebalanceWeekday={rebalanceWeekday}
              setRebalanceWeekday={setRebalanceWeekday}
            />
            {/* Top Sectors / Per Sector / Min Price Score moved to the
                Strategy parameters section below — they only apply to
                certain strategies (e.g. min_price_score is momentum-
                only; top-N pair is meaningless for "all universe").
                Keeping them out of the universe/date row leaves only
                strategy-agnostic inputs at the top level. */}
            <StrategyModeSelect
              selectionMode={selectionMode}
              setSelectionMode={setSelectionMode}
              sectorEtfs={sectorEtfs}
              sectorEtfsLoading={sectorEtfsLoading}
              sectorEtfsError={sectorEtfsError}
            />
            {/* Random-mode params (Trials, Seed) live in the
                "Strategy parameters" section below — same place as the
                momentum signal/category weights — so the inline config
                row only has to carry universe-level inputs. */}
            <RunControls
              runVariantsBacktest={runVariantsBacktest}
              showCurrentPicks={showCurrentPicks}
              running={running}
              variantsRunning={variantsRunning}
              eligibleCount={eligibleCount}
              variantsBlockReason={variantsBlockReason}
              longShortBlocked={longShortBlocked}
              selectionMode={selectionMode}
              variantsRun={variantsRun}
              currentPicksSnapshots={currentPicksSnapshots}
            />
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

          {/* Variants cross-product sweep — picker + permutations preview. */}
          <VariantsPanel
            variantSel={variantSel}
            selectionMode={selectionMode}
            longShortBlocked={longShortBlocked}
            variantsBlockReason={variantsBlockReason}
            topSectors={topSectors}
            topPerSector={topPerSector}
            minPriceScore={minPriceScore}
            indexUniverses={indexUniverses}
            eligibleCount={eligibleCount}
            totalPerms={totalPerms}
          />

          {/* Strategy parameters — content swaps based on the selected
              strategy. Momentum shows the price/volume signal weights
              and category weights; Random shows only the Trials and
              Seed inputs (which control how many independent random
              selections are run in parallel for the mean ± std
              reporting). The header makes the section's role explicit
              so it's not mistaken for a generic config block. */}
          <div className="border-t border-gray-800/60 pt-4">
            <div className="flex items-baseline gap-3 mb-3">
              <h2 className="text-gray-300 text-xs font-semibold uppercase tracking-wider">
                Strategy parameters
              </h2>
              <span className="text-[10px] text-gray-500">
                {selectionMode === 'random'
                  ? 'Random baseline · trials run independently with sequential seeds'
                  : selectionMode === 'all'
                    ? 'All universe · holds every eligible name each rebalance, equal-weighted'
                    : selectionMode === 'sector_etf'
                      ? 'Sector ETF · picks top N sectors by aggregate score, holds the mapped ETF per sector'
                      : 'Momentum · ranks the universe by signal-weighted score'}
              </span>
            </div>

            {/* Top Sectors / Per Sector / Group By / Min Price Score
                are now per-variant axes in the Variants panel below.
                Leave the dials empty there to inherit the base values
                (topSectors=4, topPerSector=6, grouping='sector',
                min_price_score=off) — comma-separate two or more values
                to sweep across them. */}

            {selectionMode === 'momentum' && (
              <SignalWeightSliders
                signalDefs={signalDefs}
                weights={weights}
                setWeights={setWeights}
                categories={categories}
                categoryWeights={categoryWeights}
                setCategoryWeights={setCategoryWeights}
              />
            )}

            {selectionMode === 'random' && (
              <RandomParamsInputs
                nTrials={nTrials}
                setNTrials={setNTrials}
                randomSeed={randomSeed}
                setRandomSeed={setRandomSeed}
              />
            )}

            {selectionMode === 'all' && (
              <div className="bg-[#0f1117] border border-gray-800/60 rounded-lg px-4 py-3 text-xs text-gray-400 leading-relaxed max-w-[640px]">
                <div className="text-gray-300 font-medium mb-1">No tunable parameters.</div>
                Each rebalance period holds every company in the selected universe&apos;s month-snapshot,
                equal-weighted. Use this as an index-proxy benchmark to compare against the momentum
                strategy. Only <span className="text-gray-300">long-only</span> is supported — long-short would need a top/bottom split that doesn&apos;t exist in this mode.
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
        <NotificationsPanel
          warnings={warnings}
          infos={infos}
          showWarnings={showWarnings}
          showInfos={showInfos}
          onToggleWarnings={() => setShowWarnings((v) => !v)}
          onToggleInfos={() => setShowInfos((v) => !v)}
        />


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
                <TableDownloadButton
                  rows={currentPortfolio.holdings.map((h) => ({
                    ...h,
                    exchange: exchangeByCompany.get(h.company_id) ?? '',
                  }))}
                  columns={(() => {
                    const cols: import('../../lib/tableExport').Column<typeof currentPortfolio.holdings[number] & { exchange: string }>[] = [
                      { key: 'ticker', header: 'Ticker', accessor: (h) => h.ticker },
                      { key: 'exchange', header: 'Exchange', accessor: (h) => h.exchange },
                      { key: 'company_name', header: 'Company', accessor: (h) => h.company_name },
                      { key: 'sector', header: 'Sector', accessor: (h) => h.sector },
                    ];
                    for (const cat of categories) {
                      cols.push({
                        key: `score_${cat}`,
                        header: cat === 'price' ? 'Price score' : cat === 'volume' ? 'Volume score' : `${cat} score`,
                        accessor: (h) => h.category_scores?.[cat] ?? null,
                      });
                    }
                    cols.push(
                      { key: 'total_score', header: 'Total score', accessor: (h) => h.score },
                      { key: 'currency', header: 'Currency', accessor: (h) => h.currency ?? '' },
                      { key: 'entry_price_local', header: 'Start (local)', accessor: (h) => h.entry_price_local ?? null },
                      { key: 'exit_price_local', header: 'End (local)', accessor: (h) => h.exit_price_local ?? null },
                      { key: 'entry_price_eur', header: 'Start (EUR)', accessor: (h) => h.entry_price_eur ?? null },
                      { key: 'exit_price_eur', header: 'End (EUR)', accessor: (h) => h.exit_price_eur ?? null },
                      { key: 'return_pct', header: 'Return (%)', accessor: (h) => h.forward_return_pct ?? null },
                      { key: 'gurufocus_url', header: 'GuruFocus URL', accessor: (h) => guruFocusUrl(h.ticker, h.exchange) },
                    );
                    return cols;
                  })()}
                  filename={`current_picks_${currentPortfolio.as_of_date}`}
                  title={`Download ${currentPortfolio.holdings.length} current picks as CSV / XLSX`}
                />
              </>
            }
          >
            {currentPortfolio.holdings.length > 0 ? (
              <div className="bg-[#0f1117] px-5 py-3 overflow-x-auto">
                <table className="w-full text-xs min-w-max">
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
        {hasVariants && (
          <VariantSummaryTable
            onAddToSchedule={handleAddVariantToSchedule}
          />
        )}

        {/* Variant attribution — per-axis marginal averages so the user
            can see at a glance which axis values consistently produce
            better metrics. Self-hides when fewer than 2 successful
            variants are available (or when no axis has 2+ distinct
            values, i.e. nothing to compare). */}
        {hasVariants && <VariantAttribution />}

        {/* Results — either the single-run `result` or, when a variant is
            active, that variant's BacktestResult. The detail components
            don't care which path the data came from. */}
        {displayResult && (() => {
          // Full label for the active strategy row in EquityCurveCard's
          // Summary table. Matches the format a saved comparison would
          // show: "{base name} · {variant label}" — e.g. "ACWI-mei ·
          // Momentum · 2002-2026 · Every 12 months · Long-only". Base
          // comes from the loaded saved-run name when one is loaded,
          // otherwise from defaultVariantsBundleName() (the same name
          // the auto-save would pick). Variant suffix is folded in
          // whenever a sweep variant is active.
          const baseName = loadedRunId != null
            ? savedRuns.find((r) => r.run_id === loadedRunId)?.name
            : undefined;
          const labelBase = baseName ?? defaultVariantsBundleName();
          // Renamed from `variantLabel` to avoid shadowing the imported
          // helper function (`variantLabel(params)`). Falls back through
          // VARIANT_DEFS for legacy 2-segment keys, then the helper for
          // extended cross-product keys (e.g. `monthly__long_only__s4__p6`).
          const activeVariantSuffix = activeVariantKey
            ? (VARIANT_DEFS.find((v) => v.key === activeVariantKey)?.label
                ?? (parseVariantKey(activeVariantKey)
                  ? variantLabel(parseVariantKey(activeVariantKey)!)
                  : undefined))
            : undefined;
          const activeStrategyLabel = activeVariantSuffix
            ? `${labelBase} · ${activeVariantSuffix}`
            : labelBase;
          return (
          <>
            <EquityCurveCard
              result={displayResult}
              loadedRunId={activeVariantResult ? null : loadedRunId}
              savedRuns={savedRuns}
              activeStrategyLabel={activeStrategyLabel}
            />

            <FeeWaterfallPanel result={displayResult} />

            <SectorTimelineChart result={displayResult} />

            <MonthlyHoldingsTable
              result={displayResult}
              categories={categories}
              exchangeByCompany={exchangeByCompany}
              scoringConfig={scoringConfig}
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
          );
        })()}
      </div>
    </div>
  );
}
