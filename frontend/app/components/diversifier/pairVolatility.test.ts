import { describe, expect, it } from 'vitest';
import { annualizedMonthlyVolatility, monthlySharpeRatio, monthlyMaxDrawdown, compoundAnnualGrowth, topDailyDrawdowns } from './pairVolatility';

describe('annualizedMonthlyVolatility', () => {
  it('annualizes the sample deviation of fractional monthly returns', () => {
    expect(annualizedMonthlyVolatility([-0.1, 0, 0.1])).toBeCloseTo(0.1 * Math.sqrt(12));
  });

  it('reports zero for a flat series and no measurement for insufficient history', () => {
    expect(annualizedMonthlyVolatility([0, 0, 0])).toBe(0);
    expect(annualizedMonthlyVolatility([])).toBeNull();
    expect(annualizedMonthlyVolatility([0.1])).toBeNull();
    expect(annualizedMonthlyVolatility([0.1, NaN])).toBeNull();
  });
});

describe('monthlySharpeRatio', () => {
  it('annualizes daily observations with 252 periods', () => {
    const returns = [-0.01, 0, 0.01];
    const vol = 0.01 * Math.sqrt(252);
    expect(annualizedMonthlyVolatility(returns, 252)).toBeCloseTo(vol);
    expect(monthlySharpeRatio(returns, 0.02, 252)).toBeCloseTo((0.9999 ** 84 - 1 - 0.02) / vol);
  });
  it('uses geometric annual return and subtracts the annual risk-free rate', () => {
    const returns = [-0.1, 0, 0.1];
    const expected = (0.99 ** 4 - 1 - 0.02) / (0.1 * Math.sqrt(12));
    expect(monthlySharpeRatio(returns, 0.02)).toBeCloseTo(expected);
  });

  it('has no ratio for zero volatility or insufficient history', () => {
    expect(monthlySharpeRatio([0, 0, 0], 0.02)).toBeNull();
    expect(monthlySharpeRatio([0.1], 0.02)).toBeNull();
  });
});

describe('compoundAnnualGrowth', () => {
  it('compounds a full trading year and matches the return used by Sharpe', () => {
    const returns = Array(252).fill(0.001);
    expect(compoundAnnualGrowth(returns)).toBeCloseTo(1.001 ** 252 - 1);
    const varied = [-0.01, 0, 0.02];
    expect(compoundAnnualGrowth(varied)).toBeCloseTo(
      monthlySharpeRatio(varied, 0.02, 252)! * annualizedMonthlyVolatility(varied, 252)! + 0.02,
    );
  });

  it('handles total loss and unavailable or invalid observations', () => {
    expect(compoundAnnualGrowth([-1, 0])).toBe(-1);
    expect(compoundAnnualGrowth([0, 0])).toBe(0);
    expect(compoundAnnualGrowth([])).toBeNull();
    expect(compoundAnnualGrowth([NaN, 0])).toBeNull();
    expect(compoundAnnualGrowth([-1.1, 0])).toBeNull();
  });
});

describe('monthlyMaxDrawdown', () => {
  it('compounds losses from the preceding peak even across a partial recovery', () => {
    expect(monthlyMaxDrawdown([0.2, -0.1, 0.05, -0.2])).toBeCloseTo(-0.244);
  });

  it('counts losses from inception and handles recovery, gains, and a total loss', () => {
    expect(monthlyMaxDrawdown([-0.2, 0.25, -0.1])).toBeCloseTo(-0.2);
    expect(monthlyMaxDrawdown([0.1, 0.2])).toBe(0);
    expect(monthlyMaxDrawdown([-1])).toBe(-1);
    expect(monthlyMaxDrawdown([])).toBeNull();
    expect(monthlyMaxDrawdown([NaN])).toBeNull();
  });
});

describe('topDailyDrawdowns', () => {
  const points = (returns: number[]) => returns.map((value, index) => ({ date: `2024-01-${String(index + 1).padStart(2, '0')}`, return: value }));

  it('counts an unrecovered decline once, including partial recoveries, and sorts by depth', () => {
    const episodes = topDailyDrawdowns(points([0, -0.1, 1 / 9, -0.2, 0.1, -0.1]));
    expect(episodes).toHaveLength(2);
    expect(episodes[0].depth).toBeCloseTo(-0.208);
    expect(episodes[0].peakDate).toBe('2024-01-03');
    expect(episodes[0].troughDate).toBe('2024-01-06');
    expect(episodes[0].recoveryDate).toBeNull();
    expect(episodes[0].remainingDrawdown).toBeCloseTo(-0.208);
    expect(episodes[1].recoveryDate).toBe('2024-01-03');
    expect(episodes[1].remainingDrawdown).toBeNull();
    expect(episodes[0].depth).toBeCloseTo(monthlyMaxDrawdown([0, -0.1, 1 / 9, -0.2, 0.1, -0.1])!);
  });

  it('includes a decline from the initial value and limits to five distinct episodes', () => {
    expect(topDailyDrawdowns(points([-0.2, 0.25]))[0]).toMatchObject({ peakDate: null, troughDate: '2024-01-01', recoveryDate: '2024-01-02' });
    expect(topDailyDrawdowns(points(Array.from({ length: 12 }, (_, index) => index % 2 ? 1 / 9 : -0.1)))).toHaveLength(5);
    expect(topDailyDrawdowns(points([0.1, 0.1]))).toEqual([]);
    expect(topDailyDrawdowns([])).toEqual([]);
  });

  it('shows the remaining decline after a partial recovery rather than the trough depth', () => {
    const [episode] = topDailyDrawdowns(points([0, -0.2, 0.1]));
    expect(episode.depth).toBeCloseTo(-0.2);
    expect(episode.remainingDrawdown).toBeCloseTo(-0.12);
    expect(episode.recoveryDate).toBeNull();
  });
});
