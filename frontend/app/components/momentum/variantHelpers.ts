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

/** Cross-product of the five sweep axes against VARIANT_DEFS. For each
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
  topSectorsSweep,
  perSectorSweep,
  minScoreSweep,
}: {
  selectedFreqs: ReadonlySet<RebalanceFrequency>;
  selectedStrategies: ReadonlySet<StrategyType>;
  selectedUniverses: ReadonlySet<string>;
  selectedGroupings: ReadonlySet<'sector' | 'industry'>;
  topSectorsSweep: string;
  perSectorSweep: string;
  minScoreSweep: string;
}): VariantParams[] {
  const topList = parseNumList(topSectorsSweep);
  const perList = parseNumList(perSectorSweep);
  const minList = parseMinScoreList(minScoreSweep);
  const uniList = Array.from(selectedUniverses);
  const grpList = Array.from(selectedGroupings);
  const topAxis: (number | undefined)[] = topList.length === 0 ? [undefined] : topList;
  const perAxis: (number | undefined)[] = perList.length === 0 ? [undefined] : perList;
  const minAxis: (number | null | undefined)[] = minList.length === 0 ? [undefined] : minList;
  const uniAxis: (string | undefined)[] = uniList.length === 0 ? [undefined] : uniList;
  const grpAxis: ('sector' | 'industry' | undefined)[] =
    grpList.length === 0 ? [undefined] : grpList;
  const out: VariantParams[] = [];
  for (const v of VARIANT_DEFS) {
    if (!selectedFreqs.has(v.frequency)) continue;
    if (!selectedStrategies.has(v.strategy)) continue;
    for (const t of topAxis) for (const p of perAxis) for (const m of minAxis)
    for (const u of uniAxis) for (const g of grpAxis) {
      out.push({
        frequency: v.frequency,
        strategy: v.strategy,
        ...(t !== undefined ? { top_n_sectors: t } : {}),
        ...(p !== undefined ? { top_n_per_sector: p } : {}),
        ...(m !== undefined ? { min_price_score: m } : {}),
        ...(u !== undefined ? { universe: u } : {}),
        ...(g !== undefined ? { grouping: g } : {}),
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
