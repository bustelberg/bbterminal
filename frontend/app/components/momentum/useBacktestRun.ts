/**
 * `useBacktestRun` — the run / orchestration handlers for `/backtest`:
 * kicking off a variant sweep, and the "Current Picks" / "Recompute"
 * current-portfolio paths. Lifted out of `MomentumBacktester.tsx` so the
 * giant component stops owning the request-assembly + store-dispatch
 * plumbing and just wires buttons to these handlers.
 *
 * Depends on the whole `useBacktestConfig` bag (the request is built from
 * it) plus the three selections the handlers also read: the chosen index
 * universe, the sector→ETF map, and the eligible variant permutations.
 * Each handler is recreated per render (matching the previous inline
 * consts), so there's nothing to memoize.
 */
import {
  loadCurrentPicksSnapshots,
  momentumStore,
  startBacktest,
  startVariantsBacktest,
  type BacktestStartConfig,
  type VariantParams,
} from '../../../lib/stores/momentum';

import type { UseBacktestConfigResult } from './useBacktestConfig';

export function useBacktestRun({
  config,
  selectedIndexUniverse,
  sectorEtfs,
  eligibleVariants,
}: {
  config: UseBacktestConfigResult;
  selectedIndexUniverse: string;
  sectorEtfs: Record<string, number>;
  eligibleVariants: VariantParams[];
}) {
  const {
    startDate, endDate, weights, categoryWeights, topSectors, topPerSector,
    maxCompanies, minPriceScore, selectionMode, randomSeed, nTrials,
    grouping, noCache, rebalanceWeekday,
  } = config;

  // Variant sweep — fans the current config out across the cross-product
  // permutations selected in the inline picker. `index_universe` +
  // `grouping` are derived from the first variant (the legacy top-row
  // inputs for those are gone).
  const runVariantsBacktest = () => {
    const targets = eligibleVariants;
    if (targets.length === 0) return;
    const universeFromVariants = targets[0]?.universe ?? null;
    const groupingFromVariants = targets[0]?.grouping ?? 'sector';
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
        min_price_score: minPriceScore.trim() === '' ? null : Number(minPriceScore),
        universe_label: null,
        index_universe: universeFromVariants,
        grouping: groupingFromVariants,
        selection_mode: selectionMode,
        random_seed: selectionMode === 'random' ? randomSeed : null,
        n_trials: selectionMode === 'random' ? Math.max(1, nTrials) : 1,
        sector_etfs: selectionMode === 'sector_etf' ? sectorEtfs : undefined,
        force_recompute: noCache,
        rebalance_weekday: rebalanceWeekday,
      },
      targets,
    );
  };

  const currentPortfolioConfig = (
    opts: { force: boolean; dbOnly: boolean },
  ): BacktestStartConfig => ({
    start_date: `${startDate}-01`,
    end_date: `${endDate}-01`,
    signal_weights: weights,
    category_weights: categoryWeights,
    top_n_sectors: topSectors,
    top_n_per_sector: topPerSector,
    max_companies: maxCompanies,
    universe_label: null,
    index_universe: selectedIndexUniverse || null,
    grouping,
    selection_mode: 'momentum',
    random_seed: null,
    n_trials: 1,
    mode: 'current_portfolio',
    force_recompute: opts.force,
    db_only: opts.dbOnly,
    rebalance_weekday: rebalanceWeekday,
  });

  // "Current Picks": what is my strategy holding right now? DB-only by
  // default (no GuruFocus / ECB calls — just what's already in Supabase).
  // "Don't use cache" disables both the snapshot cache and the db_only
  // guard, so missing prices/volumes/FX are fetched fresh.
  const showCurrentPicks = async () => {
    await startBacktest(currentPortfolioConfig({ force: noCache, dbOnly: !noCache }));
    loadCurrentPicksSnapshots();
  };

  // "Recompute": the explicit fresh-data path — bypasses the snapshot
  // cache AND the db_only guard, so the backend refetches any missing
  // prices / volumes / FX upstream. Slow, but produces a new snapshot.
  const recomputeCurrentPortfolio = async () => {
    await startBacktest(currentPortfolioConfig({ force: true, dbOnly: false }));
    loadCurrentPicksSnapshots();
  };

  return { runVariantsBacktest, showCurrentPicks, recomputeCurrentPortfolio };
}
