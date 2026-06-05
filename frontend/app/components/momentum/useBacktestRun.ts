/**
 * `useBacktestRun` — the run orchestration handler for `/backtest`:
 * kicking off a variant sweep. Lifted out of `MomentumBacktester.tsx` so
 * the giant component stops owning the request-assembly + store-dispatch
 * plumbing and just wires the button to this handler.
 *
 * Depends on the whole `useBacktestConfig` bag (the request is built from
 * it) plus the two selections the handler also reads: the sector→ETF map
 * and the eligible variant permutations. The handler is recreated per
 * render (matching the previous inline const), so there's nothing to
 * memoize.
 */
import {
  momentumStore,
  startVariantsBacktest,
  type VariantParams,
} from '../../../lib/stores/momentum';

import type { UseBacktestConfigResult } from './useBacktestConfig';

export function useBacktestRun({
  config,
  sectorEtfs,
  eligibleVariants,
}: {
  config: UseBacktestConfigResult;
  sectorEtfs: Record<string, number>;
  eligibleVariants: VariantParams[];
}) {
  const {
    startDate, endDate, weights, categoryWeights, topSectors, topPerSector,
    maxCompanies, minPriceScore, selectionMode, randomSeed, nTrials,
    noCache, rebalanceWeekday,
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

  return { runVariantsBacktest };
}
