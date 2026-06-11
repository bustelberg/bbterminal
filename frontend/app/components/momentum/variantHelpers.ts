/**
 * Pure helpers used by the variant-sweep UI on /backtest.
 *
 * Extracted from MomentumBacktester.tsx so the cross-product / input-
 * parsing logic can be tested in isolation and re-used (today only by
 * the backtester; tomorrow maybe by /schedule too).
 */
import type { Dispatch, SetStateAction } from 'react';

import {
  VARIANT_DEFS,
  type RebalanceFrequency,
  type StrategyType,
  type VariantParams,
} from '../../../lib/stores/momentum';

/** Comma-separated number list parser. Filters non-finite values, dedups,
 * trims whitespace. Empty / whitespace-only input → empty array.
 *
 *   parseNumList("4, 6,8")     → [4, 6, 8]
 *   parseNumList("4, 4, foo")  → [4]
 *   parseNumList("")           → []
 */
export function parseNumList(s: string): number[] {
  const out: number[] = [];
  const seen = new Set<number>();
  for (const tok of s.split(',')) {
    const t = tok.trim();
    if (!t) continue;
    const n = Number(t);
    if (!Number.isFinite(n)) continue;
    if (seen.has(n)) continue;
    seen.add(n);
    out.push(n);
  }
  return out;
}

/** Same as parseNumList but recognizes the literal `none` / `off` tokens
 * (case-insensitive) as `null` — used by the min-price-score axis where
 * `null` means "filter disabled for this variant" (distinct from any
 * numeric value).
 *
 *   parseMinScoreList("30, 50, none")       → [30, 50, null]
 *   parseMinScoreList("none, OFF, 50")       → [null, 50]   (null dedup'd)
 *   parseMinScoreList("not_a_number, 30")    → [30]
 */
export function parseMinScoreList(s: string): (number | null)[] {
  const out: (number | null)[] = [];
  let sawNull = false;
  const seen = new Set<number>();
  for (const tok of s.split(',')) {
    const t = tok.trim();
    if (!t) continue;
    const lower = t.toLowerCase();
    if (lower === 'none' || lower === 'off') {
      if (!sawNull) { out.push(null); sawNull = true; }
      continue;
    }
    const n = Number(t);
    if (!Number.isFinite(n)) continue;
    if (seen.has(n)) continue;
    seen.add(n);
    out.push(n);
  }
  return out;
}

/** Vol-target axis parser. Like `parseNumList` but recognizes the literal
 * `off` / `none` tokens (case-insensitive) as `undefined` — meaning "no
 * vol targeting for this variant", i.e. the original fully-invested
 * momentum strategy. Sweeping `off, 12` therefore yields the plain
 * strategy AND the 12%-vol-targeted strategy as two separate rows.
 * Non-positive / non-finite numbers are dropped (a vol target must be > 0).
 *
 *   parseVolTargetList("off, 10, 12")  → [undefined, 10, 12]
 *   parseVolTargetList("none, 12, 12") → [undefined, 12]   (dedup)
 *   parseVolTargetList("")             → []
 */
export function parseVolTargetList(s: string): (number | undefined)[] {
  const out: (number | undefined)[] = [];
  let sawOff = false;
  const seen = new Set<number>();
  for (const tok of s.split(',')) {
    const t = tok.trim();
    if (!t) continue;
    const lower = t.toLowerCase();
    if (lower === 'off' || lower === 'none') {
      if (!sawOff) { out.push(undefined); sawOff = true; }
      continue;
    }
    const n = Number(t);
    if (!Number.isFinite(n) || n <= 0) continue;
    if (seen.has(n)) continue;
    seen.add(n);
    out.push(n);
  }
  return out;
}

/** Regime-floor axis parser. `off` / `none` → `undefined` (no trend
 * filter, the original strategy). Numbers are the risk-off exposure floor
 * and must lie in [0, 1] — `0` (all cash) is valid, unlike the vol-target
 * axis. Out-of-range / non-finite tokens are dropped.
 *
 *   parseRegimeFloorList("off, 0, 0.5") → [undefined, 0, 0.5]
 *   parseRegimeFloorList("1.5, -1, 0.5") → [0.5]
 *   parseRegimeFloorList("")            → []
 */
export function parseRegimeFloorList(s: string): (number | undefined)[] {
  const out: (number | undefined)[] = [];
  let sawOff = false;
  const seen = new Set<number>();
  for (const tok of s.split(',')) {
    const t = tok.trim();
    if (!t) continue;
    const lower = t.toLowerCase();
    if (lower === 'off' || lower === 'none') {
      if (!sawOff) { out.push(undefined); sawOff = true; }
      continue;
    }
    const n = Number(t);
    if (!Number.isFinite(n) || n < 0 || n > 1) continue;
    if (seen.has(n)) continue;
    seen.add(n);
    out.push(n);
  }
  return out;
}

/** Cross-product of the sweep axes against VARIANT_DEFS. For each
 * (frequency × strategy) pair in VARIANT_DEFS that's selected, fan out
 * across whichever numeric/categorical axes have at least one value.
 * Empty axes are treated as a single `undefined` marker — that maps to
 * "inherit base, don't sweep this dimension" on the backend
 * `VariantSpec`.
 *
 * Lives outside the hook so the cross-product math can be tested
 * directly (no React renderer needed).
 */
export function buildAllPermutations({
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
}: {
  selectedFreqs: ReadonlySet<RebalanceFrequency>;
  selectedStrategies: ReadonlySet<StrategyType>;
  selectedUniverses: ReadonlySet<string>;
  selectedGroupings: ReadonlySet<'sector' | 'industry'>;
  // Rebalance weekdays to sweep (0=Mon..6=Sun). Empty/omitted → don't
  // sweep the dimension (inherit the base request's rebalance weekday).
  selectedWeekdays?: ReadonlySet<number>;
  topSectorsSweep: string;
  perSectorSweep: string;
  minScoreSweep: string;
  // Comma list of annualized vol targets (percent). `off`/`none` tokens
  // emit the original, non-targeted strategy. Blank/omitted → don't sweep
  // (every variant inherits the base = off).
  volTargetSweep?: string;
  // Comma list of regime-filter risk-off floors in [0, 1] (`off` = no
  // filter). Blank/omitted → don't sweep (inherit base = off).
  regimeFloorSweep?: string;
  // When true, fan each variant into a plain + a daily tit-for-tat-timed
  // version so they can be compared side by side. False → off only.
  sweepDailyTiming?: boolean;
}): VariantParams[] {
  const topList = parseNumList(topSectorsSweep);
  const perList = parseNumList(perSectorSweep);
  const minList = parseMinScoreList(minScoreSweep);
  const volList = parseVolTargetList(volTargetSweep ?? '');
  const regimeList = parseRegimeFloorList(regimeFloorSweep ?? '');
  const uniList = Array.from(selectedUniverses);
  const grpList = Array.from(selectedGroupings);
  const wdList = Array.from(selectedWeekdays ?? []);
  const topAxis: (number | undefined)[] = topList.length === 0 ? [undefined] : topList;
  const perAxis: (number | undefined)[] = perList.length === 0 ? [undefined] : perList;
  const minAxis: (number | null | undefined)[] = minList.length === 0 ? [undefined] : minList;
  const volAxis: (number | undefined)[] = volList.length === 0 ? [undefined] : volList;
  const regimeAxis: (number | undefined)[] = regimeList.length === 0 ? [undefined] : regimeList;
  // Boolean axis: [off] normally, [off, on] when comparing tit-for-tat.
  const timingAxis: (boolean | undefined)[] = sweepDailyTiming ? [undefined, true] : [undefined];
  const uniAxis: (string | undefined)[] = uniList.length === 0 ? [undefined] : uniList;
  const grpAxis: ('sector' | 'industry' | undefined)[] =
    grpList.length === 0 ? [undefined] : grpList;
  const wdAxis: (number | undefined)[] = wdList.length === 0 ? [undefined] : wdList;
  const out: VariantParams[] = [];
  for (const v of VARIANT_DEFS) {
    if (!selectedFreqs.has(v.frequency)) continue;
    if (!selectedStrategies.has(v.strategy)) continue;
    for (const t of topAxis) for (const p of perAxis) for (const m of minAxis)
    for (const u of uniAxis) for (const g of grpAxis) for (const w of wdAxis)
    for (const vt of volAxis) for (const rf of regimeAxis) for (const dt of timingAxis) {
      out.push({
        frequency: v.frequency,
        strategy: v.strategy,
        ...(t !== undefined ? { top_n_sectors: t } : {}),
        ...(p !== undefined ? { top_n_per_sector: p } : {}),
        ...(m !== undefined ? { min_price_score: m } : {}),
        ...(u !== undefined ? { universe: u } : {}),
        ...(g !== undefined ? { grouping: g } : {}),
        ...(w !== undefined ? { rebalance_weekday: w } : {}),
        ...(vt !== undefined ? { vol_target: vt } : {}),
        ...(rf !== undefined ? { regime_floor: rf } : {}),
        ...(dt ? { daily_timing: true } : {}),
      });
    }
  }
  return out;
}

/** Generic immutable Set toggle for the four checkbox-list axes.
 * Mutates via the React setter pattern (prev → next) so the parent's
 * useState batching works correctly. */
export function toggleInSet<T>(
  setter: Dispatch<SetStateAction<Set<T>>>,
  value: T,
): void {
  setter((prev) => {
    const next = new Set(prev);
    if (next.has(value)) next.delete(value); else next.add(value);
    return next;
  });
}
