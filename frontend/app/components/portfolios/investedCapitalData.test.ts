import { describe, expect, it } from 'vitest';
import { investedCapitalIndexByYear } from './investedCapitalData';
import { type CashReturnRow } from './cashReturnData';

/**
 * The rebase base for the invested-capital blend.
 *
 * ⚠ THE FIXTURE IS REAL. These are Vertiv Holdings' stored figures (company 2852, `VRT`), which
 * listed via SPAC in Feb 2020 — so the pre-2020 fiscal years under that ticker belong to the
 * blank-cheque shell: $24–25k of founder capital, then the June 2018 IPO trust, then the actual
 * business in 2020. `base > 0` waved 0.024 through and put a single row into the S&P 500 line at an
 * index of 2,784,248 (2018) and 31,221,600 (2025) — measured, the drawn line read 33,849 in 2025
 * where the honest figure is ~561, and the whole 2017→2018 "skyrocket" was this one company.
 */
const VERTIV = (caps: Record<string, number> | undefined): CashReturnRow => ({
  isin: 'US92537N1081',
  name: 'Vertiv Holdings Co',
  weight_pct: 100,
  ...(caps ? { market_cap_by_period: caps } : {}),
  currency: 'USD',
  ticker: 'VRT',
  exchange: 'NYSE',
  status: 'ok',
  fcf: {},
  sbc: {},
  roic: {},
  noncurrent_liabilities: { '2016': 0, '2017': 0, '2018': 24.15, '2020': 2820.1 },
  total_equity: { '2016': 0.025, '2017': 0.024, '2018': 671.912, '2020': 512.1 },
});

/** Its real caps: nothing until the June 2018 IPO. A cap of 0 is not a cap. */
const CAPS = { '2016': 0, '2017': 0, '2018': 845.25, '2020': 6385.607 };

describe('investedCapitalIndexByYear', () => {
  it('bases an index row at the first period it can be WEIGHTED in, not the first it reports', () => {
    const idx = investedCapitalIndexByYear([VERTIV(CAPS)]);
    // The shell years carry no cap, so they are not in any period's average — and therefore
    // cannot be the base of the index that average is taken over.
    expect(idx.get(2016)).toBeUndefined();
    expect(idx.get(2017)).toBeUndefined();
    expect(idx.get(2018)).toBe(100);
    // 2020: (2820.1 + 512.1) / (24.15 + 671.912) = 4.787…
    expect(idx.get(2020)).toBeCloseTo(478.72, 1);
  });

  it('does NOT let a $24k founder-capital base through just because it is positive', () => {
    // The old rule's answer, pinned so a regression is unmistakable rather than merely large.
    const idx = investedCapitalIndexByYear([VERTIV(CAPS)]);
    expect(idx.get(2018)).not.toBeCloseTo(2_784_248, -2);
  });

  it('is inert for a PORTFOLIO row, which has no per-period cap to read', () => {
    // ⚠ A DOCUMENTED LIMIT, NOT AN OVERSIGHT. A holding weight has no history, so there is nothing
    // that says when the name became investable — and inventing it would be worse than showing the
    // shell's base. The fix reaches index rows, where the cap makes the answer knowable.
    const idx = investedCapitalIndexByYear([VERTIV(undefined)]);
    expect(idx.get(2016)).toBe(100);
    expect(idx.get(2017)).toBeCloseTo(96, 0);
  });
});
