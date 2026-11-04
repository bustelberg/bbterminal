import { describe, expect, it } from 'vitest';

import {
  fundamentalRefreshQuery,
} from './PortfolioFundamentalsRefresh';

describe('fundamentalRefreshQuery', () => {
  it('requests only statements, estimates and prices for an EPS or OCF row', () => {
    expect(fundamentalRefreshQuery({
      allPeriods: true,
      everything: false,
      feeds: 'statements_estimates',
      prices: true,
      keyRatios: false,
    })).toBe('?force=true&only_due=false&feeds=statements_estimates&prices=true');
  });

  it('adds key ratios for Reverse DCF without fetching indicators', () => {
    expect(fundamentalRefreshQuery({
      allPeriods: true,
      everything: false,
      feeds: 'statements_estimates',
      prices: true,
      keyRatios: true,
    })).toContain('&feeds=statements_estimates&key_ratios=true&prices=true');
  });

  it('keeps the full-company refresh on every source', () => {
    expect(fundamentalRefreshQuery({
      allPeriods: true,
      everything: true,
      feeds: 'statements',
      prices: false,
      keyRatios: false,
    })).toContain('&feeds=all&key_ratios=true&prices=true');
  });
});
