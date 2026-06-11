import { describe, expect, it } from 'vitest';
import { buildAllPermutations, parseMinScoreList, parseNumList, parseRegimeFloorList, parseVolTargetList, toggleInSet } from './variantHelpers';
import type { RebalanceFrequency, StrategyType } from '../../../lib/stores/momentum';

describe('parseNumList', () => {
  it('parses a comma-separated list of numbers', () => {
    expect(parseNumList('4, 6,8')).toEqual([4, 6, 8]);
  });

  it('trims whitespace around tokens', () => {
    expect(parseNumList('  4  , 6 ,  8  ')).toEqual([4, 6, 8]);
  });

  it('dedupes duplicates (preserves first-seen order)', () => {
    expect(parseNumList('4, 6, 4, 8, 6')).toEqual([4, 6, 8]);
  });

  it('filters non-finite tokens silently', () => {
    expect(parseNumList('4, foo, 6, NaN, 8')).toEqual([4, 6, 8]);
  });

  it('returns [] on empty or whitespace input', () => {
    expect(parseNumList('')).toEqual([]);
    expect(parseNumList('   ')).toEqual([]);
    expect(parseNumList(',,, ,')).toEqual([]);
  });

  it('handles negative numbers and decimals', () => {
    expect(parseNumList('-1, 0.5, 2.5')).toEqual([-1, 0.5, 2.5]);
  });
});

describe('parseMinScoreList', () => {
  it('recognizes none/off (any case) as null', () => {
    expect(parseMinScoreList('30, none, 50')).toEqual([30, null, 50]);
    expect(parseMinScoreList('NONE, 50')).toEqual([null, 50]);
    expect(parseMinScoreList('off, 50')).toEqual([null, 50]);
    expect(parseMinScoreList('OFF, 50')).toEqual([null, 50]);
  });

  it('dedupes null sentinel separately from numeric dedup', () => {
    expect(parseMinScoreList('none, off, NONE, 30')).toEqual([null, 30]);
    expect(parseMinScoreList('30, 30, none, none')).toEqual([30, null]);
  });

  it('treats other strings as parse failures (silent drop)', () => {
    expect(parseMinScoreList('low, medium, 30')).toEqual([30]);
  });

  it('returns [] on empty input', () => {
    expect(parseMinScoreList('')).toEqual([]);
    expect(parseMinScoreList('  ')).toEqual([]);
  });
});

describe('parseVolTargetList', () => {
  it('parses positive vol targets', () => {
    expect(parseVolTargetList('10, 12, 15')).toEqual([10, 12, 15]);
  });

  it('maps off/none (any case) to undefined (the plain strategy)', () => {
    expect(parseVolTargetList('off, 12')).toEqual([undefined, 12]);
    expect(parseVolTargetList('NONE, 12')).toEqual([undefined, 12]);
  });

  it('dedups the off sentinel and numeric values', () => {
    expect(parseVolTargetList('off, none, 12, 12')).toEqual([undefined, 12]);
  });

  it('drops non-positive and non-finite tokens (a target must be > 0)', () => {
    expect(parseVolTargetList('0, -5, foo, 12')).toEqual([12]);
  });

  it('returns [] on empty input', () => {
    expect(parseVolTargetList('')).toEqual([]);
    expect(parseVolTargetList('  ')).toEqual([]);
  });
});

describe('parseRegimeFloorList', () => {
  it('parses floors in [0,1] including 0', () => {
    expect(parseRegimeFloorList('0, 0.5, 1')).toEqual([0, 0.5, 1]);
  });

  it('maps off/none to undefined (no filter)', () => {
    expect(parseRegimeFloorList('off, 0, 0.5')).toEqual([undefined, 0, 0.5]);
    expect(parseRegimeFloorList('NONE, 0.5')).toEqual([undefined, 0.5]);
  });

  it('drops out-of-range and non-finite tokens', () => {
    expect(parseRegimeFloorList('-0.1, 1.5, foo, 0.5')).toEqual([0.5]);
  });

  it('dedups the off sentinel and numeric values', () => {
    expect(parseRegimeFloorList('off, none, 0.5, 0.5')).toEqual([undefined, 0.5]);
  });

  it('returns [] on empty input', () => {
    expect(parseRegimeFloorList('')).toEqual([]);
  });
});

describe('buildAllPermutations', () => {
  // Minimal "empty axes everywhere" baseline so each test focuses on one
  // dimension at a time.
  const base = {
    selectedFreqs: new Set<RebalanceFrequency>(['monthly']),
    selectedStrategies: new Set<StrategyType>(['long_only']),
    selectedUniverses: new Set<string>(),
    selectedGroupings: new Set<'sector' | 'industry'>(),
    topSectorsSweep: '',
    perSectorSweep: '',
    minScoreSweep: '',
  };

  it('produces a single permutation when all sweep axes are empty', () => {
    const out = buildAllPermutations(base);
    expect(out).toHaveLength(1);
    expect(out[0]).toEqual({ frequency: 'monthly', strategy: 'long_only' });
  });

  it('omits axis fields entirely when the sweep is empty (not undefined values)', () => {
    const out = buildAllPermutations(base);
    expect(Object.keys(out[0])).toEqual(['frequency', 'strategy']);
  });

  it('fans out across each non-empty axis (full cross-product)', () => {
    const out = buildAllPermutations({
      ...base,
      topSectorsSweep: '4,6',         // 2
      perSectorSweep: '8,10,12',      // 3
      selectedUniverses: new Set(['ACWI', 'ACWI_LEONTEQ']), // 2
    });
    // 2 × 3 × 2 = 12
    expect(out).toHaveLength(12);
    expect(out.every((p) => p.frequency === 'monthly' && p.strategy === 'long_only')).toBe(true);
  });

  it('multiplies by selected frequencies and strategies', () => {
    const out = buildAllPermutations({
      ...base,
      selectedFreqs: new Set<RebalanceFrequency>(['monthly', 'every_2_months']),
      selectedStrategies: new Set<StrategyType>(['long_only', 'long_short']),
    });
    expect(out).toHaveLength(4);
    const pairs = out.map((p) => `${p.frequency}__${p.strategy}`).sort();
    expect(pairs).toEqual([
      'every_2_months__long_only',
      'every_2_months__long_short',
      'monthly__long_only',
      'monthly__long_short',
    ]);
  });

  it('fans out across selected rebalance weekdays', () => {
    const out = buildAllPermutations({
      ...base,
      selectedWeekdays: new Set<number>([0, 2]), // Mon, Wed
    });
    expect(out).toHaveLength(2);
    expect(out.map((p) => p.rebalance_weekday).sort()).toEqual([0, 2]);
  });

  it('omits rebalance_weekday entirely when the weekday axis is empty', () => {
    const out = buildAllPermutations({ ...base, selectedWeekdays: new Set<number>() });
    expect(out).toHaveLength(1);
    expect('rebalance_weekday' in out[0]).toBe(false);
  });

  it('includes weekday=0 as an explicit value (distinct from inherit)', () => {
    const out = buildAllPermutations({ ...base, selectedWeekdays: new Set<number>([0]) });
    expect(out[0].rebalance_weekday).toBe(0);
  });

  it('multiplies the weekday axis into the full cross-product', () => {
    const out = buildAllPermutations({
      ...base,
      topSectorsSweep: '4,6',                 // 2
      selectedWeekdays: new Set<number>([0, 2, 4]), // 3
    });
    expect(out).toHaveLength(6);
  });

  it('emits the plain strategy and a vol-targeted variant from `off, 12`', () => {
    const out = buildAllPermutations({ ...base, volTargetSweep: 'off, 12' });
    expect(out).toHaveLength(2);
    // `off` → field omitted (original strategy); 12 → vol_target: 12
    expect(out.some((p) => !('vol_target' in p))).toBe(true);
    expect(out.some((p) => p.vol_target === 12)).toBe(true);
  });

  it('omits vol_target entirely when the axis is blank', () => {
    const out = buildAllPermutations({ ...base, volTargetSweep: '' });
    expect(out).toHaveLength(1);
    expect('vol_target' in out[0]).toBe(false);
  });

  it('emits plain + regime-filtered variants from `off, 0, 0.5`', () => {
    const out = buildAllPermutations({ ...base, regimeFloorSweep: 'off, 0, 0.5' });
    expect(out).toHaveLength(3);
    expect(out.some((p) => !('regime_floor' in p))).toBe(true); // off → plain
    expect(out.some((p) => p.regime_floor === 0)).toBe(true);
    expect(out.some((p) => p.regime_floor === 0.5)).toBe(true);
  });

  it('fans each variant into plain + tit-for-tat when sweepDailyTiming is on', () => {
    const out = buildAllPermutations({ ...base, sweepDailyTiming: true });
    expect(out).toHaveLength(2);
    expect(out.some((p) => !('daily_timing' in p))).toBe(true); // plain
    expect(out.some((p) => p.daily_timing === true)).toBe(true); // timed
  });

  it('omits daily_timing when the toggle is off', () => {
    const out = buildAllPermutations({ ...base, sweepDailyTiming: false });
    expect(out).toHaveLength(1);
    expect('daily_timing' in out[0]).toBe(false);
  });

  it('threads min_price_score `null` through as an explicit value', () => {
    const out = buildAllPermutations({ ...base, minScoreSweep: 'none, 30' });
    expect(out).toHaveLength(2);
    const minScores = out.map((p) => p.min_price_score);
    expect(minScores).toContain(null);
    expect(minScores).toContain(30);
  });

  it('returns [] when no (frequency, strategy) pair from VARIANT_DEFS is selected', () => {
    const out = buildAllPermutations({
      ...base,
      selectedFreqs: new Set<RebalanceFrequency>(),
    });
    expect(out).toEqual([]);
  });

  it('only emits (frequency, strategy) pairs that exist in VARIANT_DEFS', () => {
    // VARIANT_DEFS contains every (f, s) pair so this currently passes
    // trivially. The test guards against future filtering of the
    // catalog — if a pair is removed, the picker shouldn't emit it.
    const out = buildAllPermutations({
      ...base,
      selectedFreqs: new Set<RebalanceFrequency>(['monthly']),
      selectedStrategies: new Set<StrategyType>(['long_only', 'long_short']),
    });
    expect(out).toHaveLength(2);
  });
});

describe('toggleInSet', () => {
  it('adds the value when absent', () => {
    let state = new Set<string>(['a']);
    const setter = (next: Set<string> | ((prev: Set<string>) => Set<string>)) => {
      if (typeof next === 'function') state = next(state);
      else state = next;
    };
    toggleInSet(setter, 'b');
    expect(state).toEqual(new Set(['a', 'b']));
  });

  it('removes the value when present', () => {
    let state = new Set<string>(['a', 'b']);
    const setter = (next: Set<string> | ((prev: Set<string>) => Set<string>)) => {
      if (typeof next === 'function') state = next(state);
      else state = next;
    };
    toggleInSet(setter, 'a');
    expect(state).toEqual(new Set(['b']));
  });

  it('returns a new Set instance (does not mutate the prior)', () => {
    const prior = new Set<string>(['a']);
    let state: Set<string> = prior;
    const setter = (next: Set<string> | ((prev: Set<string>) => Set<string>)) => {
      if (typeof next === 'function') state = next(state);
      else state = next;
    };
    toggleInSet(setter, 'b');
    expect(state).not.toBe(prior);
    expect(prior).toEqual(new Set(['a'])); // unchanged
  });
});
