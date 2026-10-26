import { describe, expect, it } from 'vitest';
import { soldLookThroughRows } from './soldLookThroughRows';

const wrapper = {
  name: 'Star Selection Index', isin: 'CERT', closed_out: true,
  last_sale: '2026-10-12', contribution_pct: .2, result_eur: 200,
  lookthrough_positions: [
    { name: 'Constellation', isin: 'CSU', weight_pct: 60, composition_as_of: '2026-06-09', since_close_pct: 11.63 },
    { name: 'ServiceNow', isin: 'NOW', weight_pct: 40, composition_as_of: '2026-06-09', since_close_pct: 13.16 },
  ],
};

describe('sold look-through regular rows', () => {
  it('uses the same row shape for children without duplicating accounting results', () => {
    const rows = soldLookThroughRows([wrapper], true);
    expect(rows.map((row) => row.name)).toEqual(['Star Selection Index', 'Constellation', 'ServiceNow']);
    expect(rows[1].via_certificate).toBe('Star Selection Index');
    expect(rows[1].certificate_weight_pct).toBe(60);
    expect(rows[1].composition_as_of).toBe('2026-06-09');
    expect(rows[1].since_close_pct).toBe(11.63);
    expect(rows[1].result_eur).toBeUndefined();
    expect(rows[1].contribution_pct).toBeUndefined();
    expect(rows.reduce((sum, row) => sum + (row.contribution_pct ?? 0), 0)).toBe(.2);
    expect(rows.reduce((sum, row) => sum + (row.result_eur ?? 0), 0)).toBe(200);
  });

  it('keeps the compact certificate row with look-through disabled', () => {
    expect(soldLookThroughRows([wrapper], false)).toHaveLength(1);
  });

  it('keeps unavailable certificates and ordinary sold stocks intact', () => {
    const stock = { name: 'Microsoft', contribution_pct: .1 };
    const rows = soldLookThroughRows([stock, { name: 'Unknown certificate' }], true);
    expect(rows).toHaveLength(2);
    expect(rows[0].contribution_pct).toBe(.1);
  });

  it('does not mutate the input and keeps repeated instrument routes distinct', () => {
    const before = JSON.stringify(wrapper);
    const rows = soldLookThroughRows([wrapper, wrapper], true);
    expect(new Set(rows.map((row) => row.row_key)).size).toBe(rows.length);
    expect(JSON.stringify(wrapper)).toBe(before);
  });
});
