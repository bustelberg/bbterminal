import { describe, it, expect } from 'vitest';
import {
  dividendYieldWorking, egmSource, estimateCagr, estimateCagrWorking, medianPE, medianPEWorking,
  nextFyEps, reverseDcfSource, reverseDcfWorking, ttmObs,
} from './egmInputs';
import { forwardLegs } from './normalisedFcf';
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

  it('⚠ `fcf` is the REPORTED free cash flow — no forecast, nothing folded in', () => {
    // It was once consensus OCF less trailing capex, then that minus stock comp plus a
    // growth-capex add-back, all BAKED INTO THIS FIGURE — which is why it was removed: the one
    // number on screen silently disagreed with the company's filing and nothing said so.
    //
    // ⚠⚠ THE NORMALISATION CAME BACK 2026-08-18 AND THIS ASSERTION IS WHY IT IS STILL SAFE. The
    // legs ride ALONGSIDE `fcf`, never inside it: `normalisedFcf` combines them and the panel
    // shows reported, −SBC, +growth capex and the total as four separate rows. If this test ever
    // has to change because `fcf` moved, the adjustment has been folded back in and the whole
    // reason it was ripped out has been undone.
    expect(reverseDcfSource(rows, TODAY)).toEqual({
      price: 100, sharesOutstanding: 1000, fcf: 10000, wacc: null,
      sbc: null, capex: null, dep: null, ocfEstimate: null, ocfEstimateDate: null,
      fcfEstimate: null, ebitdaEstimate: null, ebitEstimate: null,
      flowBasis: { ttm: false, date: null },
    });
  });

  it('carries the three normalisation legs in the signs the vendor filed them in', () => {
    // ⚠ CAPEX STAYS NEGATIVE. `growthCapex` takes the magnitude itself; normalising the sign here
    // would leave the drill-down showing a positive number under "as filed".
    const full = [...rows,
      m('annuals__Cashflow Statement__Stock Based Compensation', '2025-12-31', 202.3),
      m('annuals__Cashflow Statement__Capital Expenditure', '2025-12-31', -1631.2),
      m('annuals__Cashflow Statement__Cash Flow Depreciation, Depletion and Amortization',
        '2025-12-31', 1025.9)];
    expect(reverseDcfSource(full, TODAY)).toMatchObject({ sbc: 202.3, capex: -1631.2, dep: 1025.9 });
    // ⚠ THE CASH-FLOW DEPRECIATION LINE, NOT THE INCOME STATEMENT'S — capex is a cash figure, so
    // its maintenance proxy has to be one too.
    const wrongDep = [...rows,
      m('annuals__Income Statement__Depreciation, Depletion and Amortization', '2025-12-31', 999)];
    expect(reverseDcfSource(wrongDep, TODAY).dep).toBeNull();
  });

  it('⚠ converts the percent-unit WACC into the decimal the discount rate wants', () => {
    // Filed as 8.2 for 8.2%, like every other `… %` line. Passed through unscaled it is an 820%
    // discount rate, and every company on earth reads as worthless.
    const withWacc = [...rows, m('annuals__Ratios__WACC %', '2025-12-31', 8.2)];
    expect(reverseDcfSource(withWacc, TODAY).wacc).toBeCloseTo(0.082, 9);
    expect(reverseDcfWorking(withWacc, TODAY).wacc.raw).toBeCloseTo(8.2, 9);   // the vendor's figure
  });

  it('takes the latest fiscal year, and carries its provenance', () => {
    const w = reverseDcfWorking(rows, TODAY);
    expect(w.fcf).toMatchObject({ used: 10000, date: '2025-12-31', code: FCF });
    expect(w.price).toMatchObject({ used: 100, date: '2026-07-27', code: 'close_price' });
  });

  it('is the one computation `reverseDcfSource` reduces', () => {
    const w = reverseDcfWorking(rows, TODAY);
    const s = reverseDcfSource(rows, TODAY);
    expect([s.price, s.sharesOutstanding, s.fcf])
      .toEqual([w.price.used, w.shares.used, w.fcf.used]);
  });

  it('leaves everything null on an empty payload', () => {
    expect(reverseDcfSource([], TODAY)).toEqual({
      price: null, sharesOutstanding: null, fcf: null, wacc: null,
      sbc: null, capex: null, dep: null, ocfEstimate: null, ocfEstimateDate: null,
      fcfEstimate: null, ebitdaEstimate: null, ebitEstimate: null,
      flowBasis: { ttm: false, date: null },
    });
  });

  /**
   * ⚠⚠ THE WIRING THE PANEL DEPENDS ON, WITH META'S REAL STORED ROWS. `forwardLegs` was already
   * pinned on these numbers, but nothing asserted that `reverseDcfSource` EXTRACTS them — which is
   * exactly where a wiring bug hides: every unit correct, the panel still drawing the derivation.
   */
  describe('the FY1 consensus free cash flow reaches the source', () => {
    const meta = [
      ...rows,
      m('annual_fcf_estimate', '2026-12-31', 5412.45),
      m('annual_fcf_estimate', '2027-12-31', -6211.87),
      m('annual_operating_cash_flow_estimate', '2026-12-31', 134330.10),
      m('annual_ebitda_estimate', '2026-12-31', 140801.99),
      m('annual_ebit_estimate', '2026-12-31', 88857.90),
    ];

    it('carries the vendor consensus, its EBITDA and its EBIT', () => {
      const s = reverseDcfSource(meta, TODAY);
      expect(s.fcfEstimate).toBeCloseTo(5412.45, 6);
      expect(s.ocfEstimate).toBeCloseTo(134330.10, 6);
      expect(s.ebitdaEstimate).toBeCloseTo(140801.99, 6);
      expect(s.ebitEstimate).toBeCloseTo(88857.90, 6);
    });

    it('⚠ FY1, NOT THE NEGATIVE FY2 — the earliest FUTURE period, same rule as every other leg', () => {
      expect(reverseDcfSource(meta, TODAY).fcfEstimate).not.toBeCloseTo(-6211.87, 6);
    });

    it('⚠⚠ and the panel then values the VENDOR base, not the derivation', () => {
      // The end-to-end assertion: source -> forwardLegs -> the figure on the card.
      const s = reverseDcfSource(meta, TODAY);
      const legs = forwardLegs({
        ocfEstimate: s.ocfEstimate, fcfEstimate: s.fcfEstimate,
        ebitdaEstimate: s.ebitdaEstimate, ebitEstimate: s.ebitEstimate,
        capex: s.capex, dep: s.dep, normalise: true,
      });
      expect(legs.vendor).toBe(true);
      expect(legs.fcf).toBeCloseTo(5412.45, 6);      // NOT 45,005 — the derivation
      expect(legs.capex).toBeCloseTo(128917.65, 4);
      expect(legs.dep).toBeCloseTo(51944.09, 4);
    });
  });

  /**
   * ⚠⚠ THE FOUR CASH-FLOW LEGS ARE TRAILING TWELVE MONTHS, AND THE GAP IS NOT SMALL. Measured on
   * Meta (2026-08-26, the figures below are the real ones): the last filed fiscal year has capex
   * −69,691 and D&A 18,616, while the four newest quarters sum to **−89,325** and 22,729 — so the
   * growth-capex correction reads 51,075 on the annual basis against 66,596 on the trailing one.
   * GuruFocus's own page shows the trailing figure, so a reader checking the panel against the
   * vendor found two different numbers with nothing on either screen to say why.
   */
  describe('the trailing-twelve-month window', () => {
    const Q = (code: string, date: string, v: number) =>
      m(`quarterly__Cashflow Statement__${code}`, date, v);
    //        quarter end,  capex,  D&A,  FCF — Meta's real filings.
    const QUARTERS: [string, number, number, number][] = [
      ['2026-06-30', -30116, 6356, 1746],
      ['2026-03-31', -18997, 5999, 13229],
      ['2025-12-31', -21383, 5411, 14831],
      ['2025-09-30', -18829, 4963, 11170],
    ];
    const meta = [
      ...rows,
      m('annuals__Cashflow Statement__Capital Expenditure', '2025-12-31', -69691),
      m('annuals__Cashflow Statement__Cash Flow Depreciation, Depletion and Amortization',
        '2025-12-31', 18616),
      ...QUARTERS.flatMap(([d, capex, dep, fcf]) => [
        Q('Capital Expenditure', d, capex),
        Q('Cash Flow Depreciation, Depletion and Amortization', d, dep),
        Q('Free Cash Flow', d, fcf),
      ]),
    ];

    it('sums the four newest quarters — the figure the vendor prints', () => {
      const s = reverseDcfSource(meta, TODAY);
      expect(s.capex).toBe(-89325);
      expect(s.dep).toBe(22729);
      expect(s.flowBasis).toEqual({ ttm: true, date: '2026-06-30' });
    });

    it('⚠ EXACTLY FOUR OR NOTHING — three quarters is a nine-month figure under an annual label', () => {
      // Smaller than the year it claims to be, in the same direction for every company, invisible.
      const three = meta.filter((r) => r.target_date !== '2025-09-30');
      const s = reverseDcfSource(three, TODAY);
      expect(s.capex).toBe(-69691);                     // fell back to the fiscal year
      expect(s.flowBasis).toEqual({ ttm: false, date: '2025-12-31' });
    });

    it('⚠⚠ ONE BASIS FOR ALL FOUR LEGS, decided once', () => {
      // `normalisedFcf` subtracts one leg from another: a TTM capex against an annual free cash
      // flow is a split basis, appearing only on companies that filed four quarters of one line
      // and three of another — rarely, unpredictably, and with no way to see it.
      const noQuarterlyFcf = meta.filter(
        (r) => r.metric_code !== 'quarterly__Cashflow Statement__Free Cash Flow');
      const s = reverseDcfSource(noQuarterlyFcf, TODAY);
      expect(s.flowBasis.ttm).toBe(false);
      expect(s.capex).toBe(-69691);
      expect(s.dep).toBe(18616);
    });

    it('⚠ but a missing stock-comp line does NOT drag the other three back', () => {
      // Plenty of companies report none at all; requiring four quarters of it would put every
      // other leg on the annual basis over a line that is legitimately absent.
      expect(reverseDcfSource(meta, TODAY).flowBasis.ttm).toBe(true);
      expect(reverseDcfSource(meta, TODAY).sbc).toBeNull();
    });

    it('⚠ a quarter filed under BOTH section spellings is counted once', () => {
      // Summing one quarter twice inflates the window by exactly one quarter — again in one
      // direction, again invisibly.
      const dupes = [...meta, ...QUARTERS.map(([d, capex]) =>
        m(`quarterly__cashflow_statement__Capital Expenditure`, d, capex))];
      expect(reverseDcfSource(dupes, TODAY).capex).toBe(-89325);
    });

    it('takes nothing at all when there are no quarterly rows', () => {
      expect(ttmObs(rows, ['annuals__Cashflow Statement__Capital Expenditure']).used).toBeNull();
    });
  });

  /**
   * ⚠⚠ THE ONE ROW WHOSE PERIOD IS IN THE FUTURE, and the ONLY one not taken with `latestObs`.
   * The estimate block runs five years out and can also reach into the past (it is stored as
   * fetched), so "latest" would value the company on a 2030 consensus and "first" on a year it has
   * already reported. Both mistakes produce a perfectly plausible number.
   */
  describe('the FY1 consensus operating cash flow', () => {
    const OCF_EST = 'annual_operating_cash_flow_estimate';
    const est = [
      m(OCF_EST, '2025-12-31', 14000),   // already reported — must be ignored
      m(OCF_EST, '2026-12-31', 17000),   // FY1
      m(OCF_EST, '2027-12-31', 19000),
      m(OCF_EST, '2030-12-31', 26000),
    ];

    it('takes the EARLIEST future period, not the latest and not the first row', () => {
      const s = reverseDcfSource([...rows, ...est], TODAY);
      expect(s.ocfEstimate).toBe(17000);
      expect(s.ocfEstimateDate).toBe('2026-12-31');
    });

    it('⚠ is null when every estimate is stale — not the newest stale one', () => {
      // A company that stopped being covered has no forward base. Falling back to the last
      // estimate anybody made would date the panel's "next fiscal year" to a year in the past.
      expect(reverseDcfSource([...rows, m(OCF_EST, '2025-12-31', 14000)], TODAY).ocfEstimate)
        .toBeNull();
    });

    it('carries its provenance, and its date is the FUTURE period', () => {
      const w = reverseDcfWorking([...rows, ...est], TODAY);
      expect(w.ocfEst).toMatchObject({ used: 17000, date: '2026-12-31', code: OCF_EST });
      // ⚠ And it has NOT displaced the filed figure — the reported line is still reported.
      expect(w.fcf).toMatchObject({ used: 10000, date: '2025-12-31' });
    });

    it('⚠ does not read the PER-SHARE estimate, which is a different quantity', () => {
      // `annual_operating_cash_flow_per_share_estimate` sits in the same block under a name one
      // word longer. Read by mistake it is off by the share count — ~1,000x here — and the panel
      // would solve a mega-cap against a base of seventeen.
      const perShare = [...rows,
        m('annual_operating_cash_flow_per_share_estimate', '2026-12-31', 17)];
      expect(reverseDcfSource(perShare, TODAY).ocfEstimate).toBeNull();
    });
  });
});
