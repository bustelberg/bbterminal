import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { lagOwner } from './snapshotAge';

/**
 * WHOSE lag the amber `!` is describing.
 *
 * ⚠⚠ THE REPORTED SYMPTOM: "I refreshed all of them but most still show stale." They did, and it
 * did. The badge is computed from `as_of` — the day AIRS VALUED a book — and its tooltip said
 * `"Refresh from AIRS" pulls the current book`, which reads as "press this and it clears". It
 * cannot: `_vermogen_most_recent` already walks back to the most recent AVAILABLE valuation, so a
 * refresh returns the same date whenever AIRS has published nothing newer.
 *
 * Measured 2026-08-17, immediately after a full "Refresh all":
 *
 *     accounts re-scanned that day                          31
 *     newest valuation AIRS returned for ANY of them        2026-08-15
 *     of those 31, still dated 2026-08-11 / 2026-08-12      20
 *     rows wearing the amber `!`                            32 of 40
 *
 * The number was true — those snapshots ARE old — and the remedy was false. Pairing `as_of` with
 * `fetched_at` is what separates the two: a recent fetch against an old valuation is AIRS's
 * batch and there is nothing to do; an old fetch is ours, and that is the one a Refresh fixes.
 */

const TODAY = new Date('2026-08-17T09:00:00');          // a Monday

beforeEach(() => { vi.useFakeTimers(); vi.setSystemTime(TODAY); });
afterEach(() => { vi.useRealTimers(); });

describe('lagOwner', () => {
  it('⚠ THE MEASURED CASE — scanned today, valuation four trading days old = AIRS is behind', () => {
    // DividendTopSelectie Offensief, straight out of the DB after the refresh.
    const got = lagOwner('2026-08-11', '2026-08-17T13:15:11+00:00');
    expect(got?.side).toBe('source');
    expect(got?.text).toMatch(/cannot produce one it has not published/);
  });

  it('⚠ AND THE OTHER ONE — an old fetch IS ours to fix, and says so', () => {
    // BUS_BM_AAN_kw_USD_2026_d: valued 2026-07-29, last read 2026-07-30.
    const got = lagOwner('2026-07-29', '2026-07-30T14:16:30+00:00');
    expect(got?.side).toBe('ours');
    expect(got?.text).toMatch(/Refresh will pull/);
  });

  it('says nothing when the snapshot is not stale — a fresh row needs no explanation', () => {
    expect(lagOwner('2026-08-14', '2026-08-17T13:15:11+00:00')).toBeNull();   // 1 trading day
    expect(lagOwner('2026-08-17', '2026-08-17T13:15:11+00:00')).toBeNull();   // today
  });

  it('⚠ SAYS NOTHING RATHER THAN GUESSING when we do not know when we last fetched', () => {
    // Most `Provenance` call sites have no such fact. A verdict invented for them would be a
    // worse failure than the silence it replaces.
    expect(lagOwner('2026-07-16', null)).toBeNull();
    expect(lagOwner('2026-07-16', undefined)).toBeNull();
  });

  it('handles a bare date as well as a timestamp — the column is a timestamptz', () => {
    expect(lagOwner('2026-08-11', '2026-08-17')?.side).toBe('source');
  });

  it('⚠⚠ YESTERDAY IS NOW OURS — THIS ASSERTION WAS REVERSED (2026-08-19), so both sides are on '
     + 'the record. It used to demand `source` for a read one trading day old, on the grounds that '
     + 'the fleet scan runs daily and "read yesterday" is the healthy state. The rule is now: not '
     + 'read TODAY is outdated. A figure read on Monday and still on screen on Wednesday looked as '
     + 'current as one read an hour ago, which is what that threshold cost', () => {
    const got = lagOwner('2026-07-16', '2026-08-14T06:00:00+00:00');   // Friday, read on Monday
    expect(got?.side).toBe('ours');
    expect(got?.days).toBe(1);
  });

  it('⚠ THE TWO SIDES STILL USE ONE THRESHOLD, and it is now "today". A second constant would '
     + 'eventually disagree with the badge it explains — which is the whole reason this function '
     + 'and `provenanceFreshness` share a definition', () => {
    expect(lagOwner('2026-07-16', '2026-08-13T06:00:00+00:00')?.side).toBe('ours');
    expect(lagOwner('2026-07-16', '2026-08-14T06:00:00+00:00')?.side).toBe('ours');
    // Read TODAY is the only thing that is not our lag.
    expect(lagOwner('2026-07-16', '2026-08-17T06:00:00+00:00')?.side).toBe('source');
  });

  it('a month-old valuation read today is STILL the source — old is not the same as our fault', () => {
    // Five books have not been valued since 2026-07-16. That is worth showing and is not
    // something any button on this page can change.
    const got = lagOwner('2026-07-16', '2026-08-17T13:15:11+00:00');
    expect(got?.side).toBe('source');
  });
});
