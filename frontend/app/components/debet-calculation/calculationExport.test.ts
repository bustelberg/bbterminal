import { describe, expect, it } from 'vitest';
import { calculationExportData } from './calculationExport';

describe('calculationExportData', () => {
  it('keeps each uploaded order source in its own signed calculation column', () => {
    const result = calculationExportData([
      { portfolio: 'P-1', name: 'Ada', cash: 1_000 },
      { portfolio: 'P-2', name: 'Ben', cash: 500 },
    ], [
      { name: 'purchases.xlsx', sheetName: 'Orders', rows: [{ portfolio: 'P-1', trade: 200 }, { portfolio: 'P-1', trade: 50 }] },
      { name: 'sales.xlsx', sheetName: 'Orders', rows: [{ portfolio: 'P-1', trade: -80 }, { portfolio: 'P-2', trade: -20 }] },
    ]);

    expect(result.headers).toEqual([
      'Portefeuille', 'Naam', 'Liquid cash',
      'Purchase / sale: purchases.xlsx — Orders',
      'Purchase / sale: sales.xlsx — Orders',
      'Total purchases / sales', 'Projected cash',
    ]);
    expect(result.rows).toEqual([
      ['P-1', 'Ada', 1_000, 250, -80],
      ['P-2', 'Ben', 500, 0, -20],
    ]);
  });
});
