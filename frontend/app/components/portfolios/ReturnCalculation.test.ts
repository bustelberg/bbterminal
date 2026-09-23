import { describe, expect, it } from 'vitest';

import { returnCalculation } from './ReturnCalculation';

describe('returnCalculation', () => {
  it('substitutes each actual holding weight and return into the bucket return', () => {
    const calculation = returnCalculation([
      { name: 'A', weight_pct: 4, return_pct: 10 },
      { name: 'B', weight_pct: 6, return_pct: -5 },
      { name: 'Unpriced', weight_pct: 2, return_pct: null },
    ]);

    expect(calculation.denominator).toBe(10);
    expect(calculation.rows.map(({ contribution }) => contribution)).toEqual([4, -3]);
    expect(calculation.rows.reduce((sum, row) => sum + row.contribution, 0)).toBe(1);
  });
});
