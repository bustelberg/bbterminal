import { describe, expect, it } from 'vitest';

import { refreshScopes, type RefreshScope } from './PortfolioFundamentalsRefresh';

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
