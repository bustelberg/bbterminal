/**
 * `useVariantsBundle` — the save / schedule handlers for a completed
 * variant sweep on `/backtest`: persist every `ok` variant as one saved
 * `backtest_run` bundle (`saveVariantsBundle`, with `defaultVariantsBundleName`
 * for the auto-save name), and pin a single variant to `/schedule`
 * (`handleAddVariantToSchedule`). Lifted out of `MomentumBacktester.tsx`.
 *
 * These are the component's "glue" handlers — they read the whole config
 * (`config` bag), the variant-sweep selection (`variantSel` bag), the live
 * `variants` / `universe` from the store, plus the universe + sector-ETF
 * selections — and call the saved-runs reloader. Owns the `saving` flag.
 */
import { useState } from 'react';

import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { dialog } from '../../../lib/dialog';
import {
  momentumStore,
  parseVariantKey,
  variantLabel,
  type VariantKey,
} from '../../../lib/stores/momentum';

import type { UseBacktestConfigResult } from './useBacktestConfig';
import type { UseVariantSelectionResult } from './useVariantSelection';
import { parseMinScoreList, parseNumList } from './variantHelpers';

export function useVariantsBundle({
  config,
  variantSel,
  selectedIndexUniverse,
  sectorEtfs,
  loadSavedRuns,
}: {
  config: UseBacktestConfigResult;
  variantSel: UseVariantSelectionResult;
  selectedIndexUniverse: string;
  sectorEtfs: Record<string, number>;
  loadSavedRuns: () => void;
}) {
  const variants = momentumStore.use((s) => s.variants);
  const universe = momentumStore.use((s) => s.universe);
  const [saving, setSaving] = useState(false);

  const {
    selectionMode, maxCompanies, topSectors, topPerSector, grouping,
    startDate, endDate, minPriceScore, weights, categoryWeights,
    randomSeed, nTrials, rebalanceWeekday,
  } = config;
  const {
    selectedUniverses, selectedStrategies, selectedFreqs, selectedGroupings,
    variantsToRun, topSectorsSweep, perSectorSweep, minScoreSweep,
  } = variantSel;

  const handleAddVariantToSchedule = async (variantKey: VariantKey, variantLabel: string) => {
    const v = parseVariantKey(variantKey);
    if (!v) {
      await dialog.alert(`Couldn't parse variant key "${variantKey}".`, { title: 'Schedule add failed' });
      return;
    }
    // Map the variant's `rebalance_frequency` to a schedule cadence.
    // Off-cadence months (4/5/7/8/10/11) round to quarterly — those
    // variants are rare and the cadence pinning isn't strict (the
    // strategy still rebalances on its own internal cadence; this
    // controls how often the pipeline refreshes the snapshot).
    const FREQ_MAP: Record<string, string> = {
      daily: 'daily', weekly: 'weekly', monthly: 'monthly',
      every_2_months: 'bimonthly',
      every_3_months: 'quarterly', every_4_months: 'quarterly',
      every_5_months: 'quarterly', every_6_months: 'quarterly',
      every_7_months: 'quarterly', every_8_months: 'quarterly',
      every_9_months: 'quarterly', every_10_months: 'quarterly',
      every_11_months: 'quarterly', every_12_months: 'quarterly',
    };
    const scheduleFreq = FREQ_MAP[v.frequency] ?? 'monthly';

    // Include the selection sizing (top N sectors × top N per sector) in
    // the default name so scheduled strategies are distinguishable at a
    // glance on /schedule even when they differ only in those dials.
    const nameTopSectors = v.top_n_sectors ?? topSectors;
    const nameTopPer = v.top_n_per_sector ?? topPerSector;
    const defaultName = `${variantLabel} · ${v.universe ?? selectedIndexUniverse ?? 'ACWI_LEONTEQ'} · top ${nameTopSectors}×${nameTopPer}`;
    const enteredName = await dialog.prompt(
      `Save this variant to /schedule. Pipeline cadence: ${scheduleFreq} (mapped from "${v.frequency}").`,
      { title: 'Add variant to schedule', defaultValue: defaultName, placeholder: 'Strategy name' },
    );
    if (!enteredName || !enteredName.trim()) return;

    // Build the full config. Merge order: base from this component's
    // state, then variant overrides (frequency, strategy_type, and any
    // per-axis dials that differ from base).
    const runConfig: Record<string, unknown> = {
      selection_mode: selectionMode,
      index_universe: v.universe ?? selectedIndexUniverse ?? null,
      universe_label: null,
      max_companies: maxCompanies,
      strategy_type: v.strategy,
      rebalance_frequency: v.frequency,
      // Pin the variant's OWN rebalance weekday (falls back to the base
      // inherit value when the variant didn't sweep the dimension). This
      // is fixed at schedule time and is read-only on /schedule.
      rebalance_weekday: v.rebalance_weekday ?? rebalanceWeekday,
      top_n_sectors: v.top_n_sectors ?? topSectors,
      top_n_per_sector: v.top_n_per_sector ?? topPerSector,
      grouping: v.grouping ?? grouping,
      start_date: `${startDate}-01`,
      end_date: `${endDate}-01`,
    };
    if (selectionMode === 'momentum') {
      const baseMs = minPriceScore.trim() === '' ? null : Number(minPriceScore);
      // Variant `min_price_score === null` means "explicit OFF" (sweep
      // axis entered "none"/"off"); undefined means "inherit base".
      runConfig.min_price_score =
        v.min_price_score === undefined ? baseMs : v.min_price_score;
      runConfig.signal_weights = weights;
      runConfig.category_weights = categoryWeights;
    }
    if (selectionMode === 'random') {
      runConfig.random_seed = randomSeed;
      runConfig.n_trials = Math.max(1, nTrials);
    }
    if (selectionMode === 'sector_etf') {
      runConfig.sector_etfs = sectorEtfs;
    }

    // Grab the variant's actual BacktestResult so we can persist it as
    // a saved backtest_run. The scheduled_strategy then links to that
    // run via `backtest_run_id` — /schedule renders the full equity
    // curve + monthly history from this on expansion, no recompute
    // needed. The pipeline still produces live snapshots on every
    // tick; those get appended past the "scheduled at" date with
    // visually-distinct styling so the cutover is clear.
    const variantOutcome = momentumStore.get().variants[variantKey];
    if (!variantOutcome || variantOutcome.status !== 'ok') {
      await dialog.alert('Variant has no completed backtest result to save.', { title: 'Schedule add failed' });
      return;
    }
    const result = variantOutcome.result;
    try {
      // 1. Persist the variant's BacktestResult as a backtest_run row.
      const saveResp = await apiFetch(`${API_URL}/api/momentum/backtests`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: enteredName.trim(),
          config: runConfig,
          summary: result.summary,
          monthly_records: result.monthly_records,
          daily_records: result.daily_records,
          universe_daily_records: result.universe_daily_records,
          universe: momentumStore.get().universe,
        }),
      });
      if (!saveResp.ok) {
        const body = await saveResp.text().catch(() => '');
        await dialog.alert(
          `Could not save backtest before scheduling: ${saveResp.status} ${body.slice(0, 240)}`,
          { title: 'Schedule add failed' },
        );
        return;
      }
      const saved = await saveResp.json() as { run_id?: number };
      const backtest_run_id = saved.run_id ?? null;

      // 2. Create the scheduled_strategy linked to the saved backtest.
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: enteredName.trim(),
          frequency: scheduleFreq,
          config: runConfig,
          backtest_run_id,
        }),
      });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        await dialog.alert(
          `Failed to schedule "${enteredName}": ${r.status} ${body.slice(0, 240)}`,
          { title: 'Schedule add failed' },
        );
        return;
      }
      await dialog.alert(
        `"${enteredName}" added to /schedule. The full backtest history is preserved; the next pipeline tick will start appending live snapshots.`,
        { title: 'Variant scheduled' },
      );
    } catch (e) {
      await dialog.alert(
        `Failed to schedule "${enteredName}": ${e instanceof Error ? e.message : String(e)}`,
        { title: 'Schedule add failed' },
      );
    }
  };

  const defaultVariantsBundleName = (): string => {
    // Universe(s): when the picker has explicit universe selections, list
    // them (they're what ran). When empty, the base universe applies to
    // every variant.
    const uniList = Array.from(selectedUniverses);
    const universe = uniList.length > 1
      ? uniList.join('+')
      : uniList.length === 1
        ? uniList[0]
        : ((selectedIndexUniverse || '').trim() || 'All companies');

    // Strategy label: when multiple strategies were swept, list them
    // ("Long+L/S") so it's obvious from the name. For random / all /
    // sector_etf the strategy axis is N/A — the selection mode label
    // wins.
    const stratList = Array.from(selectedStrategies);
    const strategyLabel = selectionMode === 'random'
      ? 'Random'
      : selectionMode === 'all'
        ? 'All-universe'
        : selectionMode === 'sector_etf'
          ? 'Sector ETF'
          : stratList.length > 1
            ? stratList.map((s) => s === 'long_short' ? 'L/S' : 'Long').join('+')
            : 'Momentum';

    const startYear = startDate.slice(0, 4);
    const endYear = endDate.slice(0, 4);
    const range = startYear === endYear ? startYear : `${startYear}-${endYear}`;
    const trimmedFloor = minPriceScore.trim();
    const floorPart = trimmedFloor === '' ? '' : ` · price≥${trimmedFloor}`;

    const n = variantsToRun.length;
    const nPart = n > 1 ? ` · ${n} vars` : '';

    // Compact swept-axes annotation: every axis with >1 selected value
    // gets an `axis×k` entry. Helps distinguish e.g. an 8-variant freq
    // sweep from an 8-variant top-N sweep — without this they'd produce
    // identical names.
    const sweptAxes: string[] = [];
    if (selectedFreqs.size > 1) sweptAxes.push(`freq×${selectedFreqs.size}`);
    if (stratList.length > 1 && selectionMode !== 'random' && selectionMode !== 'all' && selectionMode !== 'sector_etf') {
      sweptAxes.push(`strat×${stratList.length}`);
    }
    if (uniList.length > 1) sweptAxes.push(`uni×${uniList.length}`);
    if (selectedGroupings.size > 1) sweptAxes.push(`grp×${selectedGroupings.size}`);
    const topList = parseNumList(topSectorsSweep);
    if (topList.length > 1) sweptAxes.push(`top×${topList.length}`);
    const perList = parseNumList(perSectorSweep);
    if (perList.length > 1) sweptAxes.push(`per×${perList.length}`);
    const minList = parseMinScoreList(minScoreSweep);
    if (minList.length > 1) sweptAxes.push(`min×${minList.length}`);
    const sweepPart = sweptAxes.length > 0 ? ` (${sweptAxes.join(', ')})` : '';

    return `${universe} · ${strategyLabel}${floorPart} · ${range}${nPart}${sweepPart}`;
  };

  const saveVariantsBundle = async (overrideName?: string) => {
    const name = (overrideName ?? defaultVariantsBundleName()).trim();
    if (!name) return;
    // Iterate the live `variants` map rather than VARIANT_DEFS so
    // cross-product keys (e.g. `monthly__long_only__s4__p6`) survive.
    // The parsed `params` powers the per-entry `label` so the saved
    // bundle reads naturally on reload no matter which axes overrode.
    const okEntries = Object.entries(variants).flatMap(([key, o]) => {
      if (o?.status !== 'ok') return [];
      const params = parseVariantKey(key as VariantKey);
      if (!params) return [];
      return [{ key: key as VariantKey, params, result: o.result }];
    });
    if (okEntries.length === 0) return;
    setSaving(true);
    try {
      const resp = await apiFetch(`${API_URL}/api/momentum/backtests`, {
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
            min_price_score: minPriceScore.trim() === '' ? null : Number(minPriceScore),
            universe_label: null,
            index_universe: selectedIndexUniverse || null,
            grouping,
            // Persist the actual selection_mode used for this sweep —
            // "momentum" was hardcoded, which made All-universe and
            // Random saves report the wrong strategy on reload.
            selection_mode: selectionMode,
            random_seed: selectionMode === 'random' ? randomSeed : null,
            n_trials: selectionMode === 'random' ? Math.max(1, nTrials) : 1,
            sector_etfs: selectionMode === 'sector_etf' ? sectorEtfs : null,
          },
          variants: okEntries.map(({ key, params, result: r }) => ({
            key,
            label: variantLabel(params),
            frequency: params.frequency,
            strategy: params.strategy,
            top_n_sectors: params.top_n_sectors,
            top_n_per_sector: params.top_n_per_sector,
            min_price_score: params.min_price_score,
            summary: r.summary,
            // Full per-period holdings for EVERY variant — so a loaded sweep is
            // identical to a fresh run. This used to be stripped to dodge
            // Supabase's statement_timeout, but the backend now gzips the whole
            // result blob and uploads it to the `backtest-results` Storage
            // bucket (see backend/routers/momentum/backtest_crud.py), which has
            // no size ceiling, so stripping is no longer needed.
            monthly_records: r.monthly_records,
            // Keep the daily equity curve so the chart line, intra-period
            // max-DD overlays, and the √252 Sharpe recompute survive reload.
            // The backend compacts these into a `{dates, returns}`
            // parallel-array form before insert (~3× smaller JSONB) and
            // re-expands them on load, so even a 24y × 14-variant bundle
            // fits comfortably under Supabase's statement_timeout.
            daily_records: r.daily_records ?? [],
            // The equal-weight universe baseline's daily curve — same
            // treatment. Without it the "vs universe" + alpha charts fall
            // back to monthly on reload (seriesFromUniverseBaseline has no
            // daily source). Backend compacts/expands it identically.
            universe_daily_records: r.universe_daily_records ?? [],
          })),
          universe,
        }),
      });
      if (resp.ok) {
        const saved = await resp.json();
        momentumStore.set({ loadedRunId: saved.run_id });
        loadSavedRuns();
      } else {
        // Surface save failures instead of swallowing — the previous
        // catch-all hid the All-universe statement-timeout case, which
        // looked to the user like "auto-save just doesn't fire".
        let detail = `${resp.status}`;
        try { detail = `${resp.status} ${await resp.text()}`.slice(0, 240); } catch {}
        console.error('[momentum] auto-save failed:', detail);
        momentumStore.set((s) => ({
          warnings: [...s.warnings, { scope: 'save', message: `Auto-save failed (${detail}). Bundle was not persisted.` }],
        }));
      }
    } catch (e) {
      console.error('[momentum] auto-save threw:', e);
      momentumStore.set((s) => ({
        warnings: [...s.warnings, { scope: 'save', message: `Auto-save error: ${e instanceof Error ? e.message : String(e)}` }],
      }));
    }
    setSaving(false);
  };

  return { saving, handleAddVariantToSchedule, saveVariantsBundle, defaultVariantsBundleName };
}
