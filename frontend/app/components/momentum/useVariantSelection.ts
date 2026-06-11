/**
 * `useVariantSelection` — state + derived computations for the
 * variants-sweep picker on `/backtest`. Lifted out of
 * `MomentumBacktester.tsx` so the cross-product math is testable in
 * isolation and the giant component shrinks by one cohesive concern.
 *
 * Inputs: the base `top_n_sectors` / `top_n_per_sector` from the main
 * config (so `variantSize` can fall back to those when a variant
 * leaves the axis undefined).
 *
 * Returns: every piece of state (with setters) and every derived value
 * the variant-sweep UI consumes. Long-short filtering by selectionMode
 * stays in the parent — that's a downstream concern that uses
 * `variantsToRun`, not a selection concern.
 */
import { useCallback, useMemo, useState } from 'react';

import {
  makeVariantKey,
  VARIANT_DEFS,
  type RebalanceFrequency,
  type StrategyType,
  type VariantKey,
  type VariantParams,
} from '../../../lib/stores/momentum';

// VARIANT_DEFS is used here only to derive the All-Frequencies /
// All-Strategies pickers; the cross-product math itself reads it
// inside `buildAllPermutations`.

import { buildAllPermutations } from './variantHelpers';

export type UseVariantSelectionResult = ReturnType<typeof useVariantSelection>;

export function useVariantSelection({
  topSectors,
  topPerSector,
}: {
  topSectors: number;
  topPerSector: number;
}) {
  // Off-cadence months (4, 5, 7, 8, 10, 11) are valid rebalance
  // schedules backend-side but rarely interesting in a sweep — hide
  // them from the picker without removing from VARIANT_DEFS so saved
  // backtests pinned to one keep loading.
  const HIDDEN_FREQS = useMemo(
    () => new Set<RebalanceFrequency>([
      'every_4_months', 'every_5_months', 'every_7_months',
      'every_8_months', 'every_10_months', 'every_11_months',
    ]),
    [],
  );
  const ALL_FREQS = useMemo(
    () => Array.from(new Set(VARIANT_DEFS.map((v) => v.frequency)))
      .filter((f) => !HIDDEN_FREQS.has(f)),
    [HIDDEN_FREQS],
  );
  const ALL_STRATEGIES = useMemo(
    () => Array.from(new Set(VARIANT_DEFS.map((v) => v.strategy))),
    [],
  );

  // ── Axis selection state ─────────────────────────────────────────
  const [selectedFreqs, setSelectedFreqs] = useState<Set<RebalanceFrequency>>(
    () => new Set<RebalanceFrequency>(['monthly']),
  );
  const [selectedStrategies, setSelectedStrategies] = useState<Set<StrategyType>>(
    () => new Set<StrategyType>(['long_only']),
  );
  const [selectedUniverses, setSelectedUniverses] = useState<Set<string>>(
    () => new Set<string>(['LEONTEQ']),
  );
  const [selectedGroupings, setSelectedGroupings] = useState<Set<'sector' | 'industry'>>(
    () => new Set<'sector' | 'industry'>(['sector']),
  );
  // Rebalance weekdays to sweep (0=Mon..6=Sun). Defaults to Monday ticked;
  // empty = don't sweep the dimension (every variant inherits the base
  // config's rebalance weekday).
  const [selectedWeekdays, setSelectedWeekdays] = useState<Set<number>>(
    () => new Set<number>([0]),
  );
  // Comma-separated numeric overrides; empty means "inherit base, don't
  // sweep this dimension." Parsing lives in `./variantHelpers.ts`.
  const [topSectorsSweep, setTopSectorsSweep] = useState<string>('');
  const [perSectorSweep, setPerSectorSweep] = useState<string>('');
  const [minScoreSweep, setMinScoreSweep] = useState<string>('');
  // Vol-target axis: comma list of annualized vol % targets, with `off`
  // for the original non-targeted strategy. Blank = don't sweep (off).
  const [volTargetSweep, setVolTargetSweep] = useState<string>('');
  // Regime-filter axis: comma list of risk-off exposure floors in [0,1]
  // (`off` = no filter). Blank = don't sweep (off).
  const [regimeFloorSweep, setRegimeFloorSweep] = useState<string>('');
  // When on, fan each variant into plain + daily tit-for-tat-timed.
  const [sweepDailyTiming, setSweepDailyTiming] = useState<boolean>(false);

  // ── Auto-skip by portfolio size ─────────────────────────────────
  // Effective portfolio size is `top_n_sectors × top_n_per_sector`.
  // Blank inherits the sensible default (12 / 50) so the filter is on
  // by default; explicit 0 disables that side; positive integer overrides.
  const [minPortfolioSizeRaw, setMinPortfolioSizeRaw] = useState<string>('');
  const [maxPortfolioSizeRaw, setMaxPortfolioSizeRaw] = useState<string>('');
  const minPortfolioSize = minPortfolioSizeRaw === ''
    ? 12
    : Math.max(0, parseInt(minPortfolioSizeRaw, 10) || 0);
  const maxPortfolioSize = maxPortfolioSizeRaw === ''
    ? 50
    : Math.max(0, parseInt(maxPortfolioSizeRaw, 10) || 0);

  // ── Per-row disable in the permutations preview ─────────────────
  const [disabledPerms, setDisabledPerms] = useState<Set<VariantKey>>(() => new Set());
  const togglePermDisabled = useCallback((key: VariantKey) => {
    setDisabledPerms((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key); else next.add(key);
      return next;
    });
  }, []);

  // ── Cross-product of the five axes ───────────────────────────────
  // Math lives in the pure `buildAllPermutations` helper so it's
  // testable without a renderer; the hook just memoizes the result.
  const allPermutations = useMemo<VariantParams[]>(
    () => buildAllPermutations({
      selectedFreqs,
      selectedStrategies,
      selectedUniverses,
      selectedGroupings,
      selectedWeekdays,
      topSectorsSweep,
      perSectorSweep,
      minScoreSweep,
      volTargetSweep,
      regimeFloorSweep,
      sweepDailyTiming,
    }),
    [selectedFreqs, selectedStrategies, selectedUniverses, selectedGroupings, selectedWeekdays, topSectorsSweep, perSectorSweep, minScoreSweep, volTargetSweep, regimeFloorSweep, sweepDailyTiming],
  );

  // ── Effective size + min/max filters ─────────────────────────────
  // Variants leave `top_n_sectors`/`top_n_per_sector` `undefined` to
  // inherit the base config values — substitute those so the floor
  // check reflects what'll actually run.
  const variantSize = useCallback(
    (p: VariantParams) =>
      (p.top_n_sectors ?? topSectors) * (p.top_n_per_sector ?? topPerSector),
    [topSectors, topPerSector],
  );
  const belowMinSize = useMemo<Set<VariantKey>>(() => {
    if (minPortfolioSize <= 0) return new Set();
    return new Set(
      allPermutations
        .filter((p) => variantSize(p) < minPortfolioSize)
        .map(makeVariantKey),
    );
  }, [allPermutations, minPortfolioSize, variantSize]);
  const aboveMaxSize = useMemo<Set<VariantKey>>(() => {
    if (maxPortfolioSize <= 0) return new Set();
    return new Set(
      allPermutations
        .filter((p) => variantSize(p) > maxPortfolioSize)
        .map(makeVariantKey),
    );
  }, [allPermutations, maxPortfolioSize, variantSize]);

  // The list the "Run variants" button will actually submit — after
  // user-disabled rows AND size-filtered rows are dropped. Long-short
  // filtering by selectionMode happens downstream of this hook in the
  // parent.
  const variantsToRun = useMemo(
    () => allPermutations.filter((p) => {
      const k = makeVariantKey(p);
      return !disabledPerms.has(k) && !belowMinSize.has(k) && !aboveMaxSize.has(k);
    }),
    [allPermutations, disabledPerms, belowMinSize, aboveMaxSize],
  );

  return {
    // State + setters
    selectedFreqs, setSelectedFreqs,
    selectedStrategies, setSelectedStrategies,
    selectedUniverses, setSelectedUniverses,
    selectedGroupings, setSelectedGroupings,
    selectedWeekdays, setSelectedWeekdays,
    topSectorsSweep, setTopSectorsSweep,
    perSectorSweep, setPerSectorSweep,
    minScoreSweep, setMinScoreSweep,
    volTargetSweep, setVolTargetSweep,
    regimeFloorSweep, setRegimeFloorSweep,
    sweepDailyTiming, setSweepDailyTiming,
    minPortfolioSizeRaw, setMinPortfolioSizeRaw,
    maxPortfolioSizeRaw, setMaxPortfolioSizeRaw,
    disabledPerms, setDisabledPerms,
    // Constants
    HIDDEN_FREQS, ALL_FREQS, ALL_STRATEGIES,
    // Derived
    minPortfolioSize, maxPortfolioSize,
    allPermutations, variantSize, belowMinSize, aboveMaxSize, variantsToRun,
    // Helpers
    togglePermDisabled,
  };
}
