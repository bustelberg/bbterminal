/**
 * `useSavedRuns` — saved-backtest CRUD for `/backtest`: list (`loadSavedRuns`),
 * load a saved run back into the UI (`loadBacktest` — applies its config to
 * the config + variant-selection state), delete / bulk-delete, and rename.
 * Lifted out of `MomentumBacktester.tsx`. `loadBacktest` is the wide one — it
 * writes through nearly every config + variantSel setter, so the bags are
 * passed whole and their setters destructured below. Owns the saved-runs
 * list + per-row spinner state.
 */
import { useState } from 'react';

import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { dialog } from '../../../lib/dialog';
import {
  makeVariantKey,
  momentumStore,
  parseVariantKey,
  VARIANT_DEFS,
  type RebalanceFrequency,
  type StrategyType,
  type VariantKey,
  type VariantOutcome,
  type VariantParams,
} from '../../../lib/stores/momentum';

import type { UseBacktestConfigResult } from './useBacktestConfig';
import type { UseVariantSelectionResult } from './useVariantSelection';
import type { SavedRun } from './types';

export function useSavedRuns({
  config,
  variantSel,
  setSectorEtfs,
  setSelectedIndexUniverse,
}: {
  config: UseBacktestConfigResult;
  variantSel: UseVariantSelectionResult;
  setSectorEtfs: (m: Record<string, number>) => void;
  setSelectedIndexUniverse: (s: string) => void;
}) {
  const loadedRunId = momentumStore.use((s) => s.loadedRunId);
  const [savedRuns, setSavedRuns] = useState<SavedRun[]>([]);
  const [savedRunsLoading, setSavedRunsLoading] = useState(true);
  const [loadingRunId, setLoadingRunId] = useState<number | null>(null);
  const [deletingRunId, setDeletingRunId] = useState<number | null>(null);
  const [renamingRunId, setRenamingRunId] = useState<number | null>(null);
  const [bulkDeletingRuns, setBulkDeletingRuns] = useState(false);

  const {
    setWeights, setCategoryWeights, setStartDate, setEndDate,
    setTopSectors, setTopPerSector, setGrouping, setMinPriceScore,
    setSelectionMode, setRandomSeed, setNTrials,
  } = config;
  const {
    setSelectedFreqs, setSelectedStrategies, setSelectedUniverses,
    setSelectedGroupings, setTopSectorsSweep, setPerSectorSweep,
    setMinScoreSweep, setDisabledPerms,
  } = variantSel;

  const loadSavedRuns = () => {
    setSavedRunsLoading(true);
    fetch(`${API_URL}/api/momentum/backtests`)
      .then((r) => r.json())
      .then((data) => setSavedRuns(Array.isArray(data) ? data : []))
      .catch(() => {})
      .finally(() => setSavedRunsLoading(false));
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
      // Saved runs from before the grouping feature have no `grouping`
      // field — default to 'sector' (the historic behavior). The auto-
      // coerce effect kicks in if the loaded run had grouping='industry'
      // but the universe doesn't carry industry data anymore.
      setGrouping(cfg.grouping === 'industry' ? 'industry' : 'sector');
      // min_price_score may be null/undefined (no floor) or a number 0-100.
      // Convert to a string so the input's empty/zero distinction round-trips.
      setMinPriceScore(
        cfg.min_price_score == null ? '' : String(cfg.min_price_score)
      );
      if (
        cfg.selection_mode === 'random'
        || cfg.selection_mode === 'momentum'
        || cfg.selection_mode === 'all'
        || cfg.selection_mode === 'sector_etf'
      ) setSelectionMode(cfg.selection_mode);
      if (cfg.selection_mode === 'sector_etf' && cfg.sector_etfs && typeof cfg.sector_etfs === 'object') {
        // Rehydrate the saved sector→benchmark_id mapping so the badge
        // status under the strategy dropdown matches what the saved run
        // was using (and a re-run goes through the same mapping).
        setSectorEtfs(cfg.sector_etfs as Record<string, number>);
      }
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
        const savedKeys = new Set<VariantKey>();
        const paramsList: VariantParams[] = [];
        for (const v of saved.variants) {
          const key = v?.key as VariantKey | undefined;
          if (!key) continue;
          next[key] = {
            status: 'ok',
            result: {
              summary: v.summary,
              monthly_records: v.monthly_records ?? [],
              daily_records: v.daily_records ?? [],
              universe_daily_records: v.universe_daily_records ?? [],
            },
          };
          if (firstKey == null) firstKey = key;
          savedKeys.add(key);
          const p = parseVariantKey(key);
          if (p) paramsList.push(p);
        }

        // ── Restore the picker state so a re-run produces the same
        //    set of variants. Without this, the user loads a saved
        //    bundle of 32 variants but the picker still points at
        //    whatever axes were selected before — hitting "Run
        //    variants" then yields a completely different sweep.
        //
        //    Logic per axis:
        //      - frequency / strategy: every variant carries these,
        //        collect the distinct values into the multi-select.
        //      - universe / grouping: collect distinct values when
        //        any variant overrode the axis; otherwise leave the
        //        current selection alone (legacy 2-segment bundles
        //        inherit from the base config which we already set
        //        above).
        //      - top_n_sectors / top_n_per_sector / min_price_score:
        //        collect distinct override values across variants
        //        and write them as a comma-joined string into the
        //        sweep text input. Variants that inherited (no
        //        override) contribute nothing to the text input —
        //        the result is an exact axis representation when all
        //        variants either all-overrode or all-inherited that
        //        axis, and a lossy "use the override values, ignore
        //        the inheritors" when mixed (rare).
        //      - disabledPerms: when the picker's cross-product
        //        would produce permutations the saved bundle DOESN'T
        //        include (e.g. the user originally disabled some
        //        rows in the permutations preview), reconstruct
        //        those marks so a re-run produces exactly the saved
        //        set, not a superset.
        if (paramsList.length > 0) {
          const freqs = new Set<RebalanceFrequency>(paramsList.map((p) => p.frequency));
          const strats = new Set<StrategyType>(paramsList.map((p) => p.strategy));
          const universes = new Set<string>(
            paramsList.map((p) => p.universe).filter((u): u is string => u != null),
          );
          const groupings = new Set<'sector' | 'industry'>(
            paramsList.map((p) => p.grouping).filter((g): g is 'sector' | 'industry' => g != null),
          );

          const distinctNums = (k: 'top_n_sectors' | 'top_n_per_sector'): number[] => {
            const out = new Set<number>();
            for (const p of paramsList) {
              const v = p[k];
              if (v != null) out.add(v);
            }
            return Array.from(out).sort((a, b) => a - b);
          };
          const distinctMins = (): (number | null)[] => {
            const seen = new Set<string>();
            const out: (number | null)[] = [];
            for (const p of paramsList) {
              const v = p.min_price_score;
              if (v === undefined) continue;
              const tok = v === null ? 'off' : String(v);
              if (!seen.has(tok)) {
                seen.add(tok);
                out.push(v);
              }
            }
            return out;
          };

          const topList = distinctNums('top_n_sectors');
          const perList = distinctNums('top_n_per_sector');
          const minList = distinctMins();

          setSelectedFreqs(freqs);
          setSelectedStrategies(strats);
          if (universes.size > 0) setSelectedUniverses(universes);
          if (groupings.size > 0) setSelectedGroupings(groupings);
          setTopSectorsSweep(topList.join(','));
          setPerSectorSweep(perList.join(','));
          setMinScoreSweep(minList.map((v) => (v === null ? 'off' : String(v))).join(','));

          // Reconstruct the cross-product the picker WOULD generate
          // from those axes, then mark every permutation that isn't
          // in the saved set as user-disabled. Mirrors the same
          // algorithm as the `allPermutations` memo so a round-trip
          // produces the same cross-product.
          const topAxis: (number | undefined)[] = topList.length === 0 ? [undefined] : topList;
          const perAxis: (number | undefined)[] = perList.length === 0 ? [undefined] : perList;
          const minAxis: (number | null | undefined)[] = minList.length === 0 ? [undefined] : minList;
          const uniAxis: (string | undefined)[] = universes.size === 0 ? [undefined] : Array.from(universes);
          const grpAxis: ('sector' | 'industry' | undefined)[] =
            groupings.size === 0 ? [undefined] : Array.from(groupings);
          const disabled = new Set<VariantKey>();
          for (const def of VARIANT_DEFS) {
            if (!freqs.has(def.frequency)) continue;
            if (!strats.has(def.strategy)) continue;
            for (const t of topAxis) for (const p of perAxis) for (const m of minAxis)
            for (const u of uniAxis) for (const g of grpAxis) {
              const candidate: VariantParams = {
                frequency: def.frequency,
                strategy: def.strategy,
                ...(t !== undefined ? { top_n_sectors: t } : {}),
                ...(p !== undefined ? { top_n_per_sector: p } : {}),
                ...(m !== undefined ? { min_price_score: m } : {}),
                ...(u !== undefined ? { universe: u } : {}),
                ...(g !== undefined ? { grouping: g } : {}),
              };
              const k = makeVariantKey(candidate);
              if (!savedKeys.has(k)) disabled.add(k);
            }
          }
          setDisabledPerms(disabled);
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
          universe_daily_records: saved.universe_daily_records ?? [],
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
    // Optimistic remove — the prod round-trip is ~500ms and the user
    // shouldn't watch the row sit there waiting. Refetch on failure so
    // a row only stays gone when the server actually deleted it.
    setSavedRuns(prev => prev.filter(r => r.run_id !== runId));
    try {
      const resp = await apiFetch(`${API_URL}/api/momentum/backtests/${runId}`, { method: 'DELETE' });
      if (!resp.ok) loadSavedRuns();
    } catch {
      loadSavedRuns();
    } finally {
      setDeletingRunId(null);
    }
  };

  const bulkDeleteRuns = async (ids: number[]) => {
    if (ids.length === 0) return;
    const ok = await dialog.confirm(
      `Delete ${ids.length} saved backtest${ids.length === 1 ? '' : 's'}?`,
      { destructive: true, confirmLabel: `Delete ${ids.length}` },
    );
    if (!ok) return;
    const idSet = new Set(ids);
    setBulkDeletingRuns(true);
    // Optimistic remove — same reasoning as deleteBacktest above. If
    // anything failed server-side, the refetch below restores those rows.
    setSavedRuns((prev) => prev.filter((r) => !idSet.has(r.run_id)));
    if (loadedRunId != null && idSet.has(loadedRunId)) {
      momentumStore.set({ loadedRunId: null });
    }
    try {
      const results = await Promise.all(
        ids.map((runId) =>
          apiFetch(`${API_URL}/api/momentum/backtests/${runId}`, { method: 'DELETE' })
            .then((r) => r.ok)
            .catch(() => false)
        ),
      );
      if (results.some((ok) => !ok)) loadSavedRuns();
    } finally {
      setBulkDeletingRuns(false);
    }
  };

  const renameBacktest = async (runId: number, currentName: string) => {
    const next = await dialog.prompt('New name for this backtest:', {
      title: 'Rename backtest',
      defaultValue: currentName,
    });
    if (!next || next.trim() === '' || next === currentName) return;
    setRenamingRunId(runId);
    try {
      const resp = await apiFetch(`${API_URL}/api/momentum/backtests/${runId}`, {
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

  return {
    savedRuns, savedRunsLoading, loadingRunId, deletingRunId, renamingRunId, bulkDeletingRuns,
    loadSavedRuns, loadBacktest, deleteBacktest, bulkDeleteRuns, renameBacktest,
  };
}
