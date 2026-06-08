'use client';

import { Fragment, useState, useEffect, useMemo, useRef } from 'react';

import ApiUsageBadge from './ApiUsageBadge';
import Spinner from './Spinner';
import { API_URL } from '../../lib/apiUrl';
import { apiFetch } from '../../lib/apiFetch';
import ProgressTimeline from './ProgressTimeline';
import NotificationsPanel from './momentum/NotificationsPanel';
import {
  useCompanyExchangeMap,
  useUniverseTemplates,
  useStaticUniverses,
  type UniverseTemplate,
} from '../../lib/hooks/apiData';
import {
  momentumStore,
  cancelVariantsBacktest,
  VARIANT_DEFS,
  parseVariantKey,
  variantLabel,
  type VariantKey,
} from '../../lib/stores/momentum';
import DateRangeRow from './momentum/DateRangeRow';
import FeeWaterfallPanel from './momentum/FeeWaterfallPanel';
import EquityCurveCard from './momentum/EquityCurveCard';
import MonthlyHoldingsTable from './momentum/MonthlyHoldingsTable';
import RandomParamsInputs from './momentum/RandomParamsInputs';
import RunControls from './momentum/RunControls';
import SavedRunsDropdown from './momentum/SavedRunsDropdown';
import SectorTimelineChart from './momentum/SectorTimelineChart';
import DailyReturnsHistograms from './momentum/DailyReturnsHistograms';
import MonthlyReturnsHeatmap from './momentum/MonthlyReturnsHeatmap';
import SignalWeightSliders from './momentum/SignalWeightSliders';
import StrategyModeSelect from './momentum/StrategyModeSelect';
import VariantAttribution from './momentum/VariantAttribution';
import VariantsPanel from './momentum/VariantsPanel';
import VariantSummaryTable from './momentum/VariantSummaryTable';
import { useBacktestConfig } from './momentum/useBacktestConfig';
import { useBacktestRun } from './momentum/useBacktestRun';
import { useSavedRuns } from './momentum/useSavedRuns';
import { useVariantsBundle } from './momentum/useVariantsBundle';
import { useSectorEtfs } from './momentum/useSectorEtfs';
import { useVariantSelection } from './momentum/useVariantSelection';

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

  // Universe options = live templates + frozen static snapshots. Snapshots
  // are pipeline-immune (reproducible) and resolve by label; their
  // `template_key` field carries that label (the value sent as
  // `index_universe`). Both map into the locally-shaped `IndexUniverseEntry`
  // the Variants AxisColumn wants. (Snapshots are CREATED on the Leonteq
  // page; here they just appear as another pickable universe.)
  const { data: _utRaw, loading: _utLoading } = useUniverseTemplates();
  const { data: _staticHook, loading: _staticLoading } = useStaticUniverses();
  const universesLoading = _utLoading || _staticLoading;
  useEffect(() => {
    const mapUni = (t: UniverseTemplate) => ({
      index_name: t.template_key,
      display_label: t.label || t.template_key,
      hard_backstop: t.earliest_date,
      start_month: t.earliest_captured_month!,
      end_month: t.latest_captured_month!,
      month_count: t.months_captured,
      // No cheap COUNT DISTINCT yet for "ever in this universe", so the
      // dropdown shows the latest month's count as a representative number.
      total_unique_tickers: t.latest_membership_count,
    });
    const all = [...(_utRaw ?? []), ...(_staticHook ?? [])];
    if (all.length === 0) return;
    setIndexUniverses(all.map(mapUni));
  }, [_utRaw, _staticHook]);

  // One-time bookkeeping at mount: saved runs and the latest-price-date
  // fetch. The endpoint hooks above own all the recurring fetches.
  useEffect(() => {
    loadSavedRuns();
    apiFetch(`${API_URL}/api/data/latest-price-date`)
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

  // Run orchestration — assembles the variant-sweep request from the
  // current config + selections and dispatches to the store. Lives in
  // `useBacktestRun` so this component just wires the buttons to it.
  const { runVariantsBacktest } = useBacktestRun({
    config, sectorEtfs, eligibleVariants,
  });

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
      <div className="px-8 py-5 border-b border-neutral-800/60 flex flex-wrap items-center justify-between gap-y-3">
        <div className="min-w-0">
          <h1 className="text-lg font-semibold text-fg-strong">Backtester</h1>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-3 min-w-0">
          <ApiUsageBadge />
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
        <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
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
              running={running}
              variantsRunning={variantsRunning}
              eligibleCount={eligibleCount}
              variantsBlockReason={variantsBlockReason}
              longShortBlocked={longShortBlocked}
              selectionMode={selectionMode}
              variantsRun={variantsRun}
            />
            <label
              className="flex items-center gap-2 cursor-pointer self-center pt-4"
              title="Bypass the replay cache and recompute the backtest from scratch."
            >
              <input
                type="checkbox"
                checked={noCache}
                onChange={(e) => setNoCache(e.target.checked)}
                className="accent-accent-500 w-4 h-4 cursor-pointer"
              />
              <span className="text-fg-muted text-xs">Don&apos;t use cache</span>
            </label>
            {variantsRunning && (
              <button
                onClick={cancelVariantsBacktest}
                className="px-4 py-2 rounded-lg text-sm font-medium text-fg-muted hover:text-neg-400 hover:bg-neg-500/10 transition-colors"
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
            universesLoading={universesLoading}
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
          <div className="border-t border-neutral-800/60 pt-4">
            <div className="flex items-baseline gap-3 mb-3">
              <h2 className="text-fg-soft text-xs font-semibold uppercase tracking-wider">
                Strategy parameters
              </h2>
              <span className="text-[10px] text-fg-subtle">
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
              <div className="bg-page border border-neutral-800/60 rounded-lg px-4 py-3 text-xs text-fg-muted leading-relaxed max-w-[640px]">
                <div className="text-fg-soft font-medium mb-1">No tunable parameters.</div>
                Each rebalance period holds every company in the selected universe&apos;s month-snapshot,
                equal-weighted. Use this as an index-proxy benchmark to compare against the momentum
                strategy. Only <span className="text-fg-soft">long-only</span> is supported — long-short would need a top/bottom split that doesn&apos;t exist in this mode.
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

            <DailyReturnsHistograms result={displayResult} />

            <MonthlyReturnsHeatmap result={displayResult} />

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
              <div className="bg-card border border-neutral-800/40 rounded-lg px-4 py-3 text-fg-muted text-sm flex items-center gap-2">
                <Spinner />
                <span>Auto-saving variant bundle…</span>
              </div>
            )}
            {loadedRunId && (
              <div className="bg-accent-500/10 border border-accent-500/20 rounded-lg px-4 py-3 text-accent-400 text-sm flex items-center gap-2">
                <span>Saved as</span>
                <span className="text-accent-300 font-medium">
                  {savedRuns.find((r) => r.run_id === loadedRunId)?.name}
                </span>
                {hasVariants && (
                  <span className="text-accent-400/70 text-xs">
                    · {Object.keys(variants).length} variants
                  </span>
                )}
              </div>
            )}

            {/* Disclaimer */}
            <p className="text-fg-faint text-xs">
              Note: Uses current company universe applied retroactively (survivorship bias). Returns are hypothetical and do not account for transaction costs.
            </p>
          </>
          );
        })()}
      </div>
    </div>
  );
}
