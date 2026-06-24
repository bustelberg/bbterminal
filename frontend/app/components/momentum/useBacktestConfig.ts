/**
 * `useBacktestConfig` — the core backtest configuration state for
 * `/backtest`: the signal + category weight maps (seeded from
 * /api/momentum/signals), the date range, sector / per-sector sizing,
 * grouping, selection mode, and the random-baseline trial knobs.
 *
 * Lifted out of `MomentumBacktester.tsx` so the giant component stops
 * owning ~15 useState slots plus the signal-defaults effect as one
 * cohesive concern (mirrors `useVariantSelection` / `useSectorEtfs`).
 * Every value is returned with its setter so the downstream writers —
 * the config panel's onChange handlers, the universe-driven date
 * autofill effect, and the saved-config loader — keep writing exactly as
 * before. `signalDefs` / `categories` are read-only outputs (only the
 * internal effect writes them), so their setters aren't exposed.
 */
import { useEffect, useState } from 'react';

import { useMomentumSignals } from '../../../lib/hooks/apiData';
import type { SignalDef } from './types';

export type SelectionMode = 'momentum' | 'momentum_extra' | 'random' | 'all' | 'sector_etf';
export type Grouping = 'sector' | 'industry';

export type UseBacktestConfigResult = ReturnType<typeof useBacktestConfig>;

export function useBacktestConfig() {
  // Signal definitions from the backend + the weight/category maps seeded
  // from their defaults (filled by the effect below once the spec loads).
  const [signalDefs, setSignalDefs] = useState<SignalDef[]>([]);
  const [weights, setWeights] = useState<Record<string, number>>({});
  const [categories, setCategories] = useState<string[]>([]);
  // Categories incl. the MomentumExtra "trend" pillar (price+volume+trend).
  const [extraCategories, setExtraCategories] = useState<string[]>([]);
  const [categoryWeights, setCategoryWeights] = useState<Record<string, number>>({});

  // Date range is independent of the selected universe (universes are now
  // single frozen sets, not per-month series). Default to the full window:
  // start at Jan 2002, end at the current month.
  const _now = new Date();
  const _currentMonth = `${_now.getFullYear()}-${String(_now.getMonth() + 1).padStart(2, '0')}`;
  const [startDate, setStartDate] = useState('2002-01');
  const [endDate, setEndDate] = useState(_currentMonth);
  const [topSectors, setTopSectors] = useState(4);
  const [topPerSector, setTopPerSector] = useState(6);
  // 'sector' is universal; 'industry' is only meaningful for LEONTEQ /
  // ACWI_LEONTEQ universes (where universe_membership.industry is
  // populated). The parent guards on `groupingAllowed` and coerces back
  // to 'sector' when a non-Leonteq universe is picked.
  const [grouping, setGrouping] = useState<Grouping>('sector');
  const [noCache, setNoCache] = useState(false);
  const [maxCompanies, setMaxCompanies] = useState(0);
  // Optional price-score floor for long selection. Empty string = no
  // filter (sent to backend as null); a number sets a strict
  // greater-than gate, so e.g. 30 means "must beat 30/100".
  const [minPriceScore, setMinPriceScore] = useState<string>('');
  const [selectionMode, setSelectionMode] = useState<SelectionMode>('momentum');
  const [randomSeed, setRandomSeed] = useState<number>(42);
  const [nTrials, setNTrials] = useState<number>(1);
  // Weekday each rebalance lands on within its period: 0=Mon … 6=Sun.
  // Default Monday matches the historical engine behavior; the saved-
  // config loader overwrites it from a loaded run's config.
  const [rebalanceWeekday, setRebalanceWeekday] = useState<number>(0);

  // Seed the weight + category maps from the signal definitions once the
  // shared cached hook resolves. The saved-config loader may overwrite
  // `weights` / `categoryWeights` afterwards.
  const { data: signalsData } = useMomentumSignals();
  useEffect(() => {
    if (!signalsData) return;
    // Seed-from-fetch: the maps stay writable so the saved-config loader
    // can overwrite them later, so this is a real useState + effect (not
    // a derived useMemo) — same shape, and same lint suppression, as
    // useSectorEtfs.
    /* eslint-disable react-hooks/set-state-in-effect */
    // Load ALL signal defs (price+volume + the MomentumExtra trend pillar) and
    // seed their default weights — so switching to MomentumExtra has the trend
    // sliders ready. The request builder only SENDS the active strategy's
    // signals/categories, so carrying the extras here is harmless for Momentum.
    const defs = [...signalsData.signals, ...(signalsData.extraSignals ?? [])];
    setSignalDefs(defs);
    const w: Record<string, number> = {};
    defs.forEach((s) => (w[s.key] = s.default_weight));
    setWeights(w);
    const cats = signalsData.categories;
    const extraCats = signalsData.extraCategories?.length ? signalsData.extraCategories : cats;
    setCategories(cats);
    setExtraCategories(extraCats);
    const cw: Record<string, number> = {};
    extraCats.forEach((c) => (cw[c] = 50));
    setCategoryWeights(cw);
    /* eslint-enable react-hooks/set-state-in-effect */
  }, [signalsData]);

  // The pillars active for the selected strategy: MomentumExtra adds "trend".
  const activeCategories = selectionMode === 'momentum_extra' ? extraCategories : categories;

  // Weights filtered to the active pillars — what gets SENT to the backend.
  // Filtering category_weights is what keeps classic Momentum byte-identical:
  // leaking the trend key in would change the price/volume normalization split.
  // (The full `weights`/`categoryWeights` maps stay editable for the sliders.)
  const _activeCats = new Set(activeCategories);
  const _activeSignalKeys = new Set(
    signalDefs.filter((d) => _activeCats.has(d.group ?? 'price')).map((d) => d.key),
  );
  const activeWeights: Record<string, number> = Object.fromEntries(
    Object.entries(weights).filter(([k]) => _activeSignalKeys.has(k)),
  );
  const activeCategoryWeights: Record<string, number> = Object.fromEntries(
    Object.entries(categoryWeights).filter(([c]) => _activeCats.has(c)),
  );

  return {
    signalDefs,
    weights, setWeights,
    categories,
    extraCategories,
    activeCategories,
    activeWeights,
    activeCategoryWeights,
    categoryWeights, setCategoryWeights,
    startDate, setStartDate,
    endDate, setEndDate,
    topSectors, setTopSectors,
    topPerSector, setTopPerSector,
    grouping, setGrouping,
    noCache, setNoCache,
    maxCompanies, setMaxCompanies,
    minPriceScore, setMinPriceScore,
    selectionMode, setSelectionMode,
    randomSeed, setRandomSeed,
    nTrials, setNTrials,
    rebalanceWeekday, setRebalanceWeekday,
  };
}
