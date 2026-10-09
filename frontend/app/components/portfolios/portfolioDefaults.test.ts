import { describe, expect, it } from 'vitest';

import { defaultLookThroughCertificates } from './portfolioDefaults';

describe('defaultLookThroughCertificates', () => {
  it('starts every named Toppenberg portfolio expanded', () => {
    expect(defaultLookThroughCertificates('Toppenberg Defensief')).toBe(true);
    expect(defaultLookThroughCertificates('Toppenberg Beperkt Offensief')).toBe(true);
    expect(defaultLookThroughCertificates('Toppenberg Offensief')).toBe(true);
  });

  it('recognises a Toppenberg AIRS account code when that is the available identity', () => {
    expect(defaultLookThroughCertificates('Unlabelled account', 'TOPS_DEF_BEH_DYN')).toBe(true);
  });

  it('keeps the normal certificate-wrapper view for other portfolios', () => {
    expect(defaultLookThroughCertificates('FamilieTopSelectie')).toBe(false);
    expect(defaultLookThroughCertificates('TopSelectie', 'BUS_FTS_OFF_DYN')).toBe(false);
  });
});
