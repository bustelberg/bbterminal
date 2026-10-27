import { expect, it } from 'vitest';
import { activeShareTableRows } from './activeShareTable';

it('uses actual stock allocation and profile ACWI allocation without changing normalised inputs', () => {
  const source = [{ name: 'Company', portfolio_pct: 20, benchmark_pct: 5, active_pct: 15 }];
  const [neutral] = activeShareTableRows(source, 45, 60);
  expect(neutral.portfolio_pct).toBe(9);
  expect(neutral.benchmark_pct).toBe(3);
  expect(neutral.active_pct).toBe(6);
  const [offensive] = activeShareTableRows(source, 80, 96);
  expect(offensive.portfolio_pct).toBe(16);
  expect(offensive.benchmark_pct).toBe(4.8);
  expect(offensive.active_pct).toBe(11.2);
  expect(source[0]).toEqual({ name: 'Company', portfolio_pct: 20, benchmark_pct: 5, active_pct: 15 });
});
