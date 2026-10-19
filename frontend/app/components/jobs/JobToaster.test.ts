import { describe, expect, it } from 'vitest';

import { friendlyJobMessage, unavailableNotice } from './JobToaster';
import { isAirsBusy } from '../../../lib/stores/jobs';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

describe('AIRS busy receipts', () => {
  it.each(['refresh', 'scan'])('recognizes a refused %s so it cannot advance the workflow', (action) => {
    expect(isAirsBusy({ status: 'done', summary:
      `Another AIRS ${action} is already running — nothing was re-read` })).toBe(true);
  });

  it('keeps successful refreshes and genuine failures distinct', () => {
    expect(isAirsBusy({ status: 'done', summary: '44/44 accounts, 1200 holdings' })).toBe(false);
    expect(isAirsBusy({ status: 'failed', summary: 'Another AIRS scan is already running — nothing was re-read' })).toBe(false);
    expect(isAirsBusy({ status: 'done', summary: null })).toBe(false);
  });
});

describe('unavailableNotice', () => {
  it('turns the old TSX receipt into a complete user-facing explanation', () => {
    expect(unavailableNotice(
      'Alimentation Couche — unavailable: Alimentation Couche-Tard: '
      + 'TSX is outside the GuruFocus subscription',
    )).toEqual({
      company: 'Alimentation Couche-Tard',
      reason: 'This is a Canadian listing (TSX), which is outside our GuruFocus subscription.',
    });
  });

  it('preserves an already explanatory reason', () => {
    expect(unavailableNotice(
      'Holding — unavailable: Issuer: This market is not supported.',
    )).toEqual({ company: 'Issuer', reason: 'This market is not supported.' });
  });

  it('does not reinterpret ordinary job summaries', () => {
    expect(unavailableNotice('3 companies refetched')).toBeNull();
  });
});

describe('the job cancel control', () => {
  it('uses the reader language instead of a hard-coded English label', () => {
    const source = readFileSync(join('app', 'components', 'jobs', 'JobToaster.tsx'), 'utf8');
    expect(source).toContain("cancel: 'Annuleren'");
    expect(source).toContain('{copy.cancel}');
    expect(source).toContain('{job.cancelRequested && running ? copy.cancelling');
  });
});

describe('friendlyJobMessage', () => {
  it('makes a cancelled portfolio refresh concise and reader-facing', () => {
    expect(friendlyJobMessage('cancelled before Bustelberg Offensief \u2014 nothing was read', 'en'))
      .toBe('Cancelled before Bustelberg Offensief. No changes were made.');
  });

  it('uses Dutch cancellation copy when Dutch is selected', () => {
    expect(friendlyJobMessage('cancelled before Bustelberg Offensief \u2014 nothing was read', 'nl'))
      .toBe('Geannuleerd vóór Bustelberg Offensief. Er zijn geen wijzigingen gemaakt.');
  });
});
