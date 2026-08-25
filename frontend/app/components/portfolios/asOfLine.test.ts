/**
 * ⚠⚠ THE CLAIM WORTH PINNING IS THAT AN ABSENT DATE STAYS ABSENT. The string this replaced read
 * "Today's weights", which was an assumption printed as a fact; a helper that quietly substituted
 * today for a missing stamp would put the same lie back one layer down.
 */
import { describe, expect, it } from 'vitest';
import { dayOf, dayRange } from './asOfLine';

describe('dayOf', () => {
  it('takes the day off an ISO date or timestamp', () => {
    expect(dayOf('2026-08-25')).toBe('2026-08-25');
    expect(dayOf('2026-08-25T01:30:00Z')).toBe('2026-08-25');
    expect(dayOf('2026-08-25T01:30:00+02:00')).toBe('2026-08-25');
  });

  it('⚠ does not move the date into the viewer\'s timezone', () => {
    // ⚠⚠ THE REASON IT IS A STRING PREFIX AND NOT `new Date(...)`. Parsed and reformatted, this
    // stamp reads as the 24th anywhere west of UTC — a date that changes with who is looking at
    // it is worse than no date, and this test fails the moment someone "improves" the parsing.
    expect(dayOf('2026-08-25T00:30:00Z')).toBe('2026-08-25');
    expect(dayOf('2026-08-25T23:30:00Z')).toBe('2026-08-25');
  });

  it('refuses anything it cannot read, and never guesses today', () => {
    expect(dayOf(null)).toBeNull();
    expect(dayOf(undefined)).toBeNull();
    expect(dayOf('')).toBeNull();
    expect(dayOf('not a date')).toBeNull();
    expect(dayOf('25-08-2026')).toBeNull();
    expect(dayOf(0 as unknown as string)).toBeNull();
  });
});

describe('dayRange', () => {
  it('collapses to one date when both ends are the same day', () => {
    expect(dayRange('2026-08-25T01:00:00Z', '2026-08-25T22:00:00Z')).toBe('2026-08-25');
  });

  it('spans when they differ', () => {
    expect(dayRange('2026-08-22', '2026-08-25')).toBe('2026-08-22 → 2026-08-25');
  });

  it('still prints the one end it has', () => {
    // ⚠ HALF A RANGE IS NOT NOTHING — refusing it would hide the only date there was.
    expect(dayRange('2026-08-22', null)).toBe('2026-08-22');
    expect(dayRange(null, '2026-08-25')).toBe('2026-08-25');
  });

  it('is null only when it knows nothing', () => {
    expect(dayRange(null, null)).toBeNull();
    expect(dayRange(undefined, undefined)).toBeNull();
    expect(dayRange('rubbish', '')).toBeNull();
  });

  it('does not reorder its ends', () => {
    // ⚠ THE BACKEND SORTS THE STAMPS, so backwards ends mean a caller bug — visible here rather
    // than tidied away into a plausible-looking range.
    expect(dayRange('2026-08-25', '2026-08-22')).toBe('2026-08-25 → 2026-08-22');
  });
});
