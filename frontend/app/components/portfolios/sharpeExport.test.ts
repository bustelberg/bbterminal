import { expect, it } from 'vitest';
import { sharpeExportRows } from './sharpeExport';

it('keeps scores numeric, missing values blank and category averages in order', () => {
  const rows = sharpeExportRows([{ bucket: 'Equity', average_sharpe: 1.236,
    period: { start: '2025-01-01', end: '2026-11-01' }, instruments: [
      { name: 'First', overall: { sharpe: -1.234, start: '2025-01-01', end: '2026-11-01' } },
      { name: 'Missing', overall: null },
    ] }, { bucket: 'Cash', average_sharpe: null, instruments: [] }], value => value);
  expect(rows.map(row => row.instrument)).toEqual(['First', 'Missing', 'Average instrument Sharpe', 'Average instrument Sharpe']);
  expect(rows[0].sharpe).toBe(-1.23);
  expect(rows[1]).toEqual({ category: 'Stocks', instrument: 'Missing', sharpe: null, start: null, end: null });
  expect(rows[2].sharpe).toBe(1.24);
  expect(rows[3].sharpe).toBeNull();
});
