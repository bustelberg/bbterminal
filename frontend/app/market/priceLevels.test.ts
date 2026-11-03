import { expect, it } from 'vitest';
import { parseDrawdown, priceLevel } from './priceLevels';

it('marks a level hit at or below its target, independently for each drawdown', () => {
  expect(priceLevel(200, 180, 10)).toEqual({ drawdown: 10, target: 180, hit: true });
  expect(priceLevel(200, 179, 10)?.hit).toBe(true);
  expect(priceLevel(200, 181, 10)?.hit).toBe(false);
  expect(priceLevel(200, 180, 20)?.hit).toBe(false);
});

it('updates target prices with a new ATH and clears hit status after a rebound', () => {
  expect(priceLevel(100, 95, 10)?.hit).toBe(false);
  expect(priceLevel(110, 95, 10)).toEqual({ drawdown: 10, target: 99, hit: true });
  expect(priceLevel(110, 100, 10)?.hit).toBe(false);
});

it('handles fractional percentages and floating point equality without false hits', () => {
  expect(priceLevel(100, 90.00000000000001, 10)?.hit).toBe(true);
  expect(priceLevel(100, 90.0001, 10)?.hit).toBe(false);
  expect(priceLevel(1000, 925, 7.5)).toEqual({ drawdown: 7.5, target: 925, hit: true });
});

it('accepts percentages with at most two decimals, including a Dutch comma', () => {
  expect(parseDrawdown('10')).toBe(10);
  expect(parseDrawdown('10.00')).toBe(10);
  expect(parseDrawdown(' 7,5 ')).toBe(7.5);
  expect(parseDrawdown('0.01')).toBe(.01);
  expect(parseDrawdown('99.99')).toBe(99.99);
  for (const value of ['', '0', '-10', '100', 'Infinity', 'NaN', '0.001', '1e1', 'ten']) expect(parseDrawdown(value)).toBeNull();
});

it('does not invent a target from invalid market data', () => {
  expect(priceLevel(0, 10, 10)).toBeNull();
  expect(priceLevel(100, NaN, 10)).toBeNull();
  expect(priceLevel(100, 90, 100)).toBeNull();
});
