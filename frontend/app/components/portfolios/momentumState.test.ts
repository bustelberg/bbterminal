import { describe, expect, it } from 'vitest';
import {
  MOMENTUM_STATE_LABELS,
  MOMENTUM_STATE_TONES,
  isMomentumState,
  ordinalPercentile,
  stateLabel,
  stateTone,
  type MomentumState,
} from './momentumState';

/**
 * ⚠ The chip is a RANK. Everything here is about not letting it be read as a return — see the
 * module docstring. The arithmetic that produces the state lives on the server; these pin the
 * presentation rules that make it legible.
 */
describe('the seven states', () => {
  it('covers -3..+3 and nothing else', () => {
    const keys = Object.keys(MOMENTUM_STATE_LABELS).map(Number).sort((a, b) => a - b);
    expect(keys).toEqual([-3, -2, -1, 0, 1, 2, 3]);
    expect(Object.keys(MOMENTUM_STATE_TONES).length).toBe(7);
  });

  it('gives every state a tone, so none can render untoned by omission', () => {
    for (const k of Object.keys(MOMENTUM_STATE_LABELS).map(Number) as MomentumState[]) {
      expect(MOMENTUM_STATE_TONES[k]).toBeTruthy();
    }
  });

  it('⚠ keeps the neutral state colourless — a tone would make no signal look like one', () => {
    expect(MOMENTUM_STATE_TONES[0]).toBe('text-fg-subtle');
  });

  it('⚠⚠ routes every tone through a design token, never a hex or a raw Tailwind colour', () => {
    for (const tone of Object.values(MOMENTUM_STATE_TONES)) {
      expect(tone).toMatch(/^text-(neg|pos|fg)-/);
      expect(tone).not.toMatch(/#|\[|red|green|slate|gray|grey/);
    }
  });

  it('is symmetric — the weak side has as many states as the strong side', () => {
    const labels = (n: MomentumState) => MOMENTUM_STATE_LABELS[n].length;
    expect(labels(-3)).toBe(labels(3));
    expect(labels(-2)).toBe(labels(2));
    expect(labels(-1)).toBe(labels(1));
  });
});

describe('guarding a state that did not arrive', () => {
  it.each([null, undefined, 4, -4, 1.5, NaN])('rejects %s', (v) => {
    expect(isMomentumState(v as number)).toBe(false);
    expect(stateLabel(v as number)).toBeNull();
  });

  it('⚠ an absent state falls back to a neutral tone, never to a colour', () => {
    expect(stateTone(null)).toBe('text-fg-subtle');
    expect(stateTone(undefined)).toBe('text-fg-subtle');
  });

  it('accepts every real state', () => {
    for (const v of [-3, -2, -1, 0, 1, 2, 3]) expect(isMomentumState(v)).toBe(true);
  });
});

describe('the percentile ordinal', () => {
  it('takes a fraction and prints an ordinal', () => {
    expect(ordinalPercentile(0.82)).toBe('82nd');
    expect(ordinalPercentile(0.01)).toBe('1st');
    expect(ordinalPercentile(0.03)).toBe('3rd');
    expect(ordinalPercentile(0.5)).toBe('50th');
    expect(ordinalPercentile(1)).toBe('100th');
  });

  it('⚠ 11/12/13 take "th", not "st/nd/rd"', () => {
    expect(ordinalPercentile(0.11)).toBe('11th');
    expect(ordinalPercentile(0.12)).toBe('12th');
    expect(ordinalPercentile(0.13)).toBe('13th');
  });

  it('⚠⚠ Dutch takes a single "e" — "82nd" inside a Dutch sentence reads as a broken number', () => {
    expect(ordinalPercentile(0.82, 'nl')).toBe('82e');
    expect(ordinalPercentile(0.01, 'nl')).toBe('1e');
    expect(ordinalPercentile(0.11, 'nl')).toBe('11e');
  });

  it('⚠ never prints a 0th percentile — the weakest member is still 1st of N, not 0th', () => {
    expect(ordinalPercentile(0)).toBe('1st');
    expect(ordinalPercentile(0.0001)).toBe('1st');
  });

  it('returns null rather than a guess when there is no rank', () => {
    expect(ordinalPercentile(null)).toBeNull();
    expect(ordinalPercentile(undefined)).toBeNull();
    expect(ordinalPercentile(NaN)).toBeNull();
  });
});
