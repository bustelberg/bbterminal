import { describe, it, expect } from 'vitest';
import {
  dividendYieldWorking, egmSource, estimateCagr, estimateCagrWorking, medianPE, medianPEWorking,
  nextFyEps, reverseDcfSource, reverseDcfWorking,
} from './egmInputs';
import { type MetricRow } from './quickValuation';

const TODAY = '2026-07-28';
const m = (metric_code: string, target_date: string, numeric_value: number | null): MetricRow =>
  ({ metric_code, target_date, numeric_value });

const EPS_EST = 'annual_per_share_eps_estimate';

describe('nextFyEps', () => {
  it('⚠ takes the next FUTURE period, not the first row of the series', () => {
    // The estimate block is stored from whenever it was fetched, so its early periods can already
    // be in the past — [0] would value the company on a year it has since reported.
    const rows = [m(EPS_EST, '2025-09-30', 9.1), m(EPS_EST, '2026-09-30', 11.46),
      m(EPS_EST, '2027-09-30', 12.9)];
    expect(nextFyEps(rows, TODAY)).toEqual({ date: '2026-09-30', value: 11.46 });
  });

  it('is null when every estimate is stale', () => {
    expect(nextFyEps([m(EPS_EST, '2025-09-30', 9.1)], TODAY)).toBeNull();
  });
});

describe('estimateCagr', () => {
  it('compounds the first future estimate into the last', () => {
    const rows = [m(EPS_EST, '2026-09-30', 10), m(EPS_EST, '2029-09-30', 13.31)];
    expect(estimateCagr(rows, TODAY)).toBeCloseTo(0.10, 6);   // 1.1^3
  });

  it('needs two future points, and refuses to compound out of a loss', () => {
    expect(estimateCagr([m(EPS_EST, '2026-09-30', 10)], TODAY)).toBeNull();
    expect(estimateCagr(
      [m(EPS_EST, '2026-09-30', -2), m(EPS_EST, '2029-09-30', 5)], TODAY)).toBeNull();
  });
});

describe('medianPE', () => {
  const PRICE = 'annuals__Per Share Data__Month End Stock Price';
  const EPS = 'annuals__Per Share Data__EPS without NRI';

  it('is year-end price over that year\'s normalised EPS', () => {
    const rows = [
      m(PRICE, '2022-12-31', 100), m(EPS, '2022-12-31', 5),    // 20x
      m(PRICE, '2023-12-31', 120), m(EPS, '2023-12-31', 5),    // 24x
      m(PRICE, '2024-12-31', 130), m(EPS, '2024-12-31', 5),    // 26x
    ];
    expect(medianPE(rows)).toBeCloseTo(24, 6);
  });

  it('⚠ skips a loss year rather than counting a negative multiple', () => {
    // A negative P/E would drag the median down and read as "historically cheap".
    const rows = [
      m(PRICE, '2022-12-31', 100), m(EPS, '2022-12-31', -5),
      m(PRICE, '2023-12-31', 120), m(EPS, '2023-12-31', 5),    // 24x
      m(PRICE, '2024-12-31', 130), m(EPS, '2024-12-31', 5),    // 26x
    ];
    expect(medianPE(rows)).toBeCloseTo(25, 6);
    expect(medianPE(rows) as number).toBeGreaterThan(0);
  });

  it('takes the last n fiscal years, not the first', () => {
    const rows = [2018, 2019, 2020, 2021, 2022, 2023].flatMap((y, i) => [
      m(PRICE, `${y}-12-31`, 100 + i * 100), m(EPS, `${y}-12-31`, 10),
    ]);
    // last 5 -> 20,30,40,50,60 -> median 40
    expect(medianPE(rows, 5)).toBeCloseTo(40, 6);
  });

  it('is null when nothing is computable', () => {
    expect(medianPE([])).toBeNull();
  });
});

describe('egmSource', () => {
  const rows = [
    m('close_price', '2026-07-27', 281.365),
    m('close_price', '2026-07-20', 270.0),
    m('indicator_q_forward_pe_ratio', '2026-06-30', 25),
    m('annuals__Valuation Ratios__Dividend Yield %', '2025-09-30', 0.30),
    m(EPS_EST, '2026-09-30', 11.46),
  ];

  it('takes the latest close as the price', () => {
    expect(egmSource(rows, TODAY).price).toBeCloseTo(281.365, 6);
  });

  it('⚠ converts the percent-unit dividend yield into the decimal the model wants', () => {
    // The field is named `… %` and holds 0.30 for 0.30%, exactly as `ROE %` does. Passing it
    // through unscaled applies a 0.3% payer as a 30% one — on a ten-year compounder, a ~3.4x
    // fair value.
    expect(egmSource(rows, TODAY).dividendYield).toBeCloseTo(0.003, 9);
  });

  it('prefers the fresher quarterly dividend yield over the fiscal-year one', () => {
    const withQ = [...rows, m('quarterly__Valuation Ratios__Dividend Yield %', '2026-03-31', 0.42)];
    expect(egmSource(withQ, TODAY).dividendYield).toBeCloseTo(0.0042, 9);
  });

  it('leaves every absent input null rather than substituting a zero', () => {
    const s = egmSource([], TODAY);
    expect(s).toMatchObject({
      price: null, forwardPE: null, dividendYield: null, epsNextFY: null,
      analystGrowth5Y: null, medianPE5Y: null,
    });
  });
});

describe('the working behind each hint', () => {
  const PRICE = 'annuals__Per Share Data__Month End Stock Price';
  const EPS = 'annuals__Per Share Data__EPS without NRI';

  it('⚠ the CAGR working and the scalar are ONE computation', () => {
    const rows = [m(EPS_EST, '2026-09-30', 10), m(EPS_EST, '2029-09-30', 13.31)];
    const w = estimateCagrWorking(rows, TODAY);
    expect(w.points.map((p) => p.eps)).toEqual([10, 13.31]);
    expect(w.years).toBe(3);
    expect(w.cagr).toBeCloseTo(estimateCagr(rows, TODAY) as number, 12);
  });

  it('the median working shows a loss year but excludes it', () => {
    const rows = [
      m(PRICE, '2023-12-31', 100), m(EPS, '2023-12-31', -5),
      m(PRICE, '2024-12-31', 120), m(EPS, '2024-12-31', 5),
    ];
    const w = medianPEWorking(rows);
    expect(w.rows.map((r) => [r.year, r.used])).toEqual([[2023, false], [2024, true]]);
    expect(w.rows[0].pe).toBeNull();
    expect(w.median).toBeCloseTo(medianPE(rows) as number, 12);
  });

  it('the yield working names the observation actually taken — newest by date', () => {
    const rows = [
      m('annuals__Valuation Ratios__Dividend Yield %', '2025-12-31', 3.9),
      m('quarterly__Valuation Ratios__Dividend Yield %', '2026-03-31', 4.54),
      m('quarterly__Valuation Ratios__Dividend Yield %', '2025-12-31', 3.9),
    ];
    const w = dividendYieldWorking(rows);
    expect(w.rows[0].date).toBe('2026-03-31');
    expect(w.chosen?.pct).toBeCloseTo(4.54, 9);
    expect(w.rows.filter((r) => r.chosen)).toHaveLength(1);
    // The panel divides by 100 — the working stays in the vendor's percent units.
    expect(egmSource(rows, TODAY).dividendYield).toBeCloseTo(0.0454, 9);
  });

  it('has no chosen observation when none is reported', () => {
    expect(dividendYieldWorking([]).chosen).toBeNull();
  });
});


describe('reverseDcfSource', () => {
  const FCF = 'annuals__Cashflow Statement__Free Cash Flow';
  const rows = [
    m('close_price', '2026-07-27', 100),
    m('annuals__Income Statement__Shares Outstanding (Diluted Average)', '2025-12-31', 1000),
    m(FCF, '2024-12-31', 8000),
    m(FCF, '2025-12-31', 10000),
  ];

  it('⚠ reads the REPORTED free cash flow — no forecast, no adjustment', () => {
    // It was consensus OCF less trailing capex, then that minus stock comp plus a growth-capex
    // add-back. A plain DCF compounds the cash flow the company reported; every adjustment on top
    // is an opinion the reader did not ask for.
    expect(reverseDcfSource(rows))
      .toEqual({ price: 100, sharesOutstanding: 1000, fcf: 10000, wacc: null });
  });

  it('⚠ converts the percent-unit WACC into the decimal the discount rate wants', () => {
    // Filed as 8.2 for 8.2%, like every other `… %` line. Passed through unscaled it is an 820%
    // discount rate, and every company on earth reads as worthless.
    const withWacc = [...rows, m('annuals__Ratios__WACC %', '2025-12-31', 8.2)];
    expect(reverseDcfSource(withWacc).wacc).toBeCloseTo(0.082, 9);
    expect(reverseDcfWorking(withWacc).wacc.raw).toBeCloseTo(8.2, 9);   // the vendor's figure
  });

  it('takes the latest fiscal year, and carries its provenance', () => {
    const w = reverseDcfWorking(rows);
    expect(w.fcf).toMatchObject({ used: 10000, date: '2025-12-31', code: FCF });
    expect(w.price).toMatchObject({ used: 100, date: '2026-07-27', code: 'close_price' });
  });

  it('is the one computation `reverseDcfSource` reduces', () => {
    const w = reverseDcfWorking(rows);
    const s = reverseDcfSource(rows);
    expect([s.price, s.sharesOutstanding, s.fcf])
      .toEqual([w.price.used, w.shares.used, w.fcf.used]);
  });

  it('leaves everything null on an empty payload', () => {
    expect(reverseDcfSource([]))
      .toEqual({ price: null, sharesOutstanding: null, fcf: null, wacc: null });
  });
});
