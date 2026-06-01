import { describe, expect, it } from 'vitest';
import { buildAllPermutations, parseMinScoreList, parseNumList, toggleInSet } from './variantHelpers';
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
