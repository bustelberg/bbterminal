import { describe, expect, it } from 'vitest';

import { unavailableNotice } from './JobToaster';

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
