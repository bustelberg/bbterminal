import { describe, it, expect } from 'vitest';
import type { Holding, PeriodRecord } from '../../../lib/stores/momentum';
import {
  collectHeldCompanies,
  computeTurnoverByDate,
  repriceOpenPeriod,
  splitAtGoLive,
  type PriceMap,
} from './monthlyHoldings';

const hold = (p: Partial<Holding>) => p as unknown as Holding;
const rec = (p: Partial<PeriodRecord>) => p as unknown as PeriodRecord;
const curve = (pts: Array<[string, number]>) => pts.map(([date, c]) => ({ date, cumulative_return_pct: c }));

describe('collectHeldCompanies', () => {
  it('dedupes by company_id and sorts by ticker', () => {
    const records = [
      rec({ date: '2024-01', holdings: [hold({ company_id: 2, ticker: 'ZZZ', company_name: 'Z' }), hold({ company_id: 1, ticker: 'AAA', company_name: 'A' })] }),
      rec({ date: '2024-02', holdings: [hold({ company_id: 1, ticker: 'AAA', company_name: 'A' }), hold({ company_id: 3, ticker: 'MMM', company_name: 'M' })] }),
    ];
    expect(collectHeldCompanies(records).map((c) => c.ticker)).toEqual(['AAA', 'MMM', 'ZZZ']);
  });
});

describe('computeTurnoverByDate', () => {
  it('is null for the first + empty periods, % of new names otherwise', () => {
    const records = [
      rec({ date: '2024-01', holdings: [hold({ company_id: 1 }), hold({ company_id: 2 })] }),
      rec({ date: '2024-02', holdings: [hold({ company_id: 2 }), hold({ company_id: 3 })] }), // 1 of 2 new
      rec({ date: '2024-03', holdings: [] }),
    ];
    expect(computeTurnoverByDate(records)).toEqual({ '2024-01': null, '2024-02': 50, '2024-03': null });
  });
});

describe('repriceOpenPeriod', () => {
  const base = () => [
    rec({ date: '2024-01', is_open: false, cumulative_return_pct: 0, holdings: [] }),
    rec({
      date: '2024-02', is_open: true, cumulative_return_pct: 10,
      portfolio_return_pct: 10, as_of_date: '2024-02-05',
      holdings: [
        hold({ company_id: 1, side: 'long', weight: 1, entry_price_eur: 100, exit_price_eur: 110 }),
        hold({ company_id: 2, side: 'short', weight: 1, entry_price_eur: 100, exit_price_eur: 90 }),
      ],
    }),
  ];

  it('returns records unchanged when there is nothing to re-price', () => {
    const recs = base();
    expect(repriceOpenPeriod(recs, null)).toBe(recs);
  });

  it('re-prices the open period and recomputes long-short return + chained cumulative', () => {
    const prices: PriceMap = {
      '1': { price_local: 0, price_eur: 120, target_date: '2024-02-10' },
      '2': { price_local: 0, price_eur: 95, target_date: '2024-02-10' },
    };
    const out = repriceOpenPeriod(base(), { date: '2024-02-10', prices });
    const last = out[out.length - 1];
    // long +20% (120/100), short −5% (95/100) → long − short = 25%.
    expect(last.portfolio_return_pct).toBeCloseTo(25, 6);
    // prevCum 0 chained with +25% → 25%.
    expect(last.cumulative_return_pct).toBeCloseTo(25, 6);
    expect(last.as_of_date).toBe('2024-02-10');
    expect(last.holdings[0].exit_price_eur).toBe(120);
  });
});

describe('splitAtGoLive', () => {
  // Full-date period records — the shape /schedule produces, where a period
  // start aligns with a daily-curve date.
  const records = [
    rec({ date: '2024-01-01', holdings: [hold({ company_id: 1 })], cumulative_return_pct: 21 }),
    rec({ date: '2024-02-01', holdings: [hold({ company_id: 1 })], cumulative_return_pct: 30 }),
  ];

  it('passes through unchanged with no marker', () => {
    const out = splitAtGoLive({ records, dailyRecords: undefined, universeDailyRecords: undefined, markerDate: undefined, goLivePrices: null });
    expect(out.map((r) => r.key)).toEqual(['2024-01-01', '2024-02-01']);
    expect(out.every((r) => r.net && r.label === null)).toBe(true);
  });

  it('splits the period containing the marker into pre/post with relative returns from the daily curve', () => {
    const strat = curve([['2024-01-01', 0], ['2024-01-15', 10], ['2024-02-01', 21]]);
    const uni = curve([['2024-01-01', 0], ['2024-01-15', 5], ['2024-02-01', 10.25]]);
    const out = splitAtGoLive({
      records, dailyRecords: strat, universeDailyRecords: uni,
      markerDate: '2024-01-15', goLivePrices: null,
    });
    // period 0 split into two; period 1 passes through → 3 rows.
    expect(out.map((r) => r.key)).toEqual(['2024-01-01__pre', '2024-01-01__post', '2024-02-01']);

    const [pre, post, feb] = out;
    // pre: 2024-01-01 → 2024-01-15  (strat 0→10, uni 0→5)
    expect(pre.row.portfolio_return_pct).toBeCloseTo(10, 6);
    expect(pre.row.universe_return_pct).toBeCloseTo(5, 6);
    expect(pre.row.cumulative_return_pct).toBeCloseTo(10, 6);
    expect(pre.row.is_open).toBe(false);
    expect(pre.net).toBe(false);
    // post: 2024-01-15 → 2024-02-01  (strat 10→21 ≈ +10%, uni 5→10.25 ≈ +5%)
    expect(post.row.portfolio_return_pct).toBeCloseTo(10, 6);
    expect(post.row.universe_return_pct).toBeCloseTo(5, 6);
    expect(post.row.cumulative_return_pct).toBeCloseTo(21, 6);
    // the non-containing period is a plain passthrough
    expect(feb.label).toBeNull();
    expect(feb.net).toBe(true);
  });
});
