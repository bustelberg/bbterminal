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

export type SelectionMode = 'momentum' | 'random' | 'all' | 'sector_etf';
export type Grouping = 'sector' | 'industry';

export type UseBacktestConfigResult = ReturnType<typeof useBacktestConfig>;

export function useBacktestConfig() {
  // Signal definitions from the backend + the weight/category maps seeded
  // from their defaults (filled by the effect below once the spec loads).
  const [signalDefs, setSignalDefs] = useState<SignalDef[]>([]);
  const [weights, setWeights] = useState<Record<string, number>>({});
  const [categories, setCategories] = useState<string[]>([]);
  const [categoryWeights, setCategoryWeights] = useState<Record<string, number>>({});

  const currentYear = new Date().getFullYear();
  const [startDate, setStartDate] = useState('2017-01');
  const [endDate, setEndDate] = useState(`${currentYear}-01`);
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
    const defs = signalsData.signals;
    setSignalDefs(defs);
    const w: Record<string, number> = {};
    defs.forEach((s) => (w[s.key] = s.default_weight));
    setWeights(w);
    const cats = signalsData.categories;
    setCategories(cats);
    const cw: Record<string, number> = {};
    cats.forEach((c) => (cw[c] = 50));
    setCategoryWeights(cw);
    /* eslint-enable react-hooks/set-state-in-effect */
  }, [signalsData]);

  return {
    signalDefs,
    weights, setWeights,
    categories,
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
  };
}
