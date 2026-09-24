import { describe, expect, it } from 'vitest';

import { stockSectorParts } from './stockSectorParts';

describe('stockSectorParts', () => {
  it('groups every company by sector and keeps the row order within each sector', () => {
    const asml = { name: 'ASML', sector: 'Information Technology' };
    const jnj = { name: 'Johnson & Johnson', sector: 'Health Care' };
    const nvidia = { name: 'Nvidia', sector: 'Information Technology' };

    const parts = stockSectorParts([asml, jnj, nvidia]);

    expect(parts.map((part) => part.label)).toEqual(['Health Care', 'Information Technology']);
    expect(parts[1].rows).toEqual([asml, nvidia]);
    expect(parts.flatMap((part) => part.rows)).toHaveLength(3);
  });

  it('puts missing and explicit unclassified sectors together at the end', () => {
    const parts = stockSectorParts([
      { name: 'Unknown A', sector: null },
      { name: 'Bank', sector: 'Financials' },
      { name: 'Unknown B', sector: 'Unclassified' },
    ]);

    expect(parts.map((part) => part.label)).toEqual(['Financials', 'Unclassified']);
    expect(parts[1].rows.map((row) => row.name)).toEqual(['Unknown A', 'Unknown B']);
  });
});
