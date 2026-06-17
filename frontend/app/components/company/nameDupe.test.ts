import { describe, it, expect } from 'vitest';
import { nameDupeKey, computeNameDupes } from './nameDupe';
import type { Company } from './types';

const co = (company_id: number, company_name: string, isin: string | null): Company => ({
  company_id, company_name, isin,
  gurufocus_ticker: 'X', gurufocus_exchange: 'X', country: null, universes: [],
});

describe('nameDupeKey', () => {
  it('strips trailing corporate suffixes', () => {
    expect(nameDupeKey('Celestica Inc')).toBe('celestica');
    expect(nameDupeKey('Celestica')).toBe('celestica');
    expect(nameDupeKey('HDFC Bank Ltd')).toBe('hdfc bank');
    expect(nameDupeKey('Itau Unibanco Holding SA (ADR)')).toBe('itau unibanco');
  });
  it('does not strip meaningful words', () => {
    expect(nameDupeKey('BYD Co Ltd')).toBe('byd');
    expect(nameDupeKey('BYD Electronic')).toBe('byd electronic'); // stays distinct
  });
  it('is punctuation/case insensitive', () => {
    expect(nameDupeKey('APPLE INC.')).toBe('apple');
  });
});

describe('computeNameDupes', () => {
  it('flags same-name pairs where ≥1 has no ISIN', () => {
    const m = computeNameDupes([
      co(21, 'Celestica', null),
      co(5101, 'Celestica Inc', 'CA15101Q2071'),
    ]);
    expect(m.has(21)).toBe(true);
    expect(m.has(5101)).toBe(true);
    expect(m.get(21)?.[0].company_id).toBe(5101);
  });

  it('does NOT flag same-name pairs that both have ISINs (e.g. share classes)', () => {
    const m = computeNameDupes([
      co(1, 'Alphabet Inc', 'US02079K3059'),
      co(2, 'Alphabet Inc', 'US02079K1079'),
    ]);
    expect(m.size).toBe(0);
  });

  it('does NOT fold different corporate suffixes (Siemens Ltd vs AG)', () => {
    // Siemens Ltd (India, no ISIN) is a different entity from Siemens AG.
    const m = computeNameDupes([
      co(10, 'SIEMENS LTD', null),
      co(11, 'Siemens AG', 'DE0007236101'),
    ]);
    expect(m.size).toBe(0);
  });

  it('folds when the stub is the winner name minus a suffix (Waste Connections)', () => {
    const m = computeNameDupes([
      co(20, 'Waste Connections', null),
      co(21, 'WASTE CONNECTIONS INC', 'CA94106B1013'),
    ]);
    expect(m.size).toBe(2);
  });

  it('ignores unique names', () => {
    const m = computeNameDupes([co(1, 'Apple Inc', null), co(2, 'Microsoft Corp', null)]);
    expect(m.size).toBe(0);
  });
});
