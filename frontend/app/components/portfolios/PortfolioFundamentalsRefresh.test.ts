import { describe, expect, it } from 'vitest';

import {
  fundamentalRefreshQuery, refreshScopes, type RefreshScope,
} from './PortfolioFundamentalsRefresh';

describe('refreshScopes', () => {
  const book: RefreshScope = { kind: 'company', isin: 'NL0000000001', name: 'Book company' };

  it('refreshes the active benchmark after the displayed scope', () => {
    const benchmark: RefreshScope = { kind: 'universe', label: 'ACWI', name: 'ACWI' };
    expect(refreshScopes(book, benchmark)).toEqual([book, benchmark]);
  });

  it('does not spend twice when the comparison is the displayed company', () => {
    const same: RefreshScope = { kind: 'company', isin: book.isin, name: 'Same listing' };
    expect(refreshScopes(book, same)).toEqual([book]);
  });

  it('treats baskets with the same holdings in another order as the same scope', () => {
    const a: RefreshScope = { kind: 'basket', name: 'A', holdings: [{ isin: 'B' }, { isin: 'A' }] };
    const b: RefreshScope = { kind: 'basket', name: 'B', holdings: [{ isin: 'A' }, { isin: 'B' }] };
    expect(refreshScopes(a, b)).toEqual([a]);
  });
});

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
