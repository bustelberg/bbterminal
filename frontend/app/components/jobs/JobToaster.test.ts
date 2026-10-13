import { describe, expect, it } from 'vitest';

import { unavailableNotice } from './JobToaster';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

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
