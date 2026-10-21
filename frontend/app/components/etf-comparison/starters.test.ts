import { describe, expect, it } from 'vitest';
import { ETF_STARTERS, starterEntries } from './starters';

describe('ETF comparison defaults', () => {
  it('includes only the five identified funds, each with an explicit Yahoo symbol and ISIN', () => {
    expect(ETF_STARTERS.map((etf) => etf.isin)).toEqual([
      'IE00BP3QZ825', 'IE000MEQP5U8', 'IE00BF4RFH31', 'IE000R4ZNTN3', 'IE000OEF25S1',
    ]);
    expect(new Set(ETF_STARTERS.map((etf) => etf.ticker)).size).toBe(5);
    expect(ETF_STARTERS.every((etf) => /^[A-Z0-9]+\.(L|AS|F)$/.test(etf.ticker))).toBe(true);
  });

  it('gives every ETF its own editable date and unique row identity', () => {
    const rows = starterEntries('2026-01-01');
    expect(rows).toHaveLength(5);
    expect(new Set(rows.map((row) => row.id)).size).toBe(5);
    rows[0].start = '2025-06-01';
    expect(rows[1].start).toBe('2026-01-01');
    expect(starterEntries('2026-01-01')[0].start).toBe('2026-01-01');
  });
});
