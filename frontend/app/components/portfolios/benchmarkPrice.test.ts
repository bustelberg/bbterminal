import { expect, it } from 'vitest';
import { benchmarkPriceEur } from './benchmarkPrice';

it('converts both quotes using their own FX rates, matching the EUR return', () => {
  const opening = benchmarkPriceEur(100, 'USD', 1.25)!;
  const closing = benchmarkPriceEur(120, 'USD', 1.2)!;
  expect(opening).toBe(80);
  expect(closing).toBe(100);
  expect((closing / opening - 1) * 100).toBe(25);
  expect(benchmarkPriceEur(157.43, 'EUR', null)).toBe(157.43);
});

it('does not relabel unconverted foreign prices as EUR when FX is missing or invalid', () => {
  for (const fx of [undefined, null, 0, -1, NaN, Infinity]) {
    expect(benchmarkPriceEur(140.58, 'USD', fx)).toBeNull();
  }
  expect(benchmarkPriceEur(null, 'USD', 1.2)).toBeNull();
  expect(benchmarkPriceEur(100, null, 1.2)).toBeNull();
});
