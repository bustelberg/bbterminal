import { describe, it, expect } from 'vitest';
import {
  addYears, BASIS, cagrBetween, cagrOf, EPS_EST_CODES, EPS_PS_CODES, FCF_PS_CODES, forwardEstimates,
  forwardFigures, latestDateOf, medianOf, multipleOf, priceAtYield, priceTarget, priceVsMetric,
  rebase, yearsBetween, yieldOf, type MetricRow,
} from './quickValuation';

const PRICE = 'annuals__Per Share Data__Month End Stock Price';
const FCF = 'annuals__Per Share Data__Free Cash Flow per Share';
const EPS = 'annuals__Per Share Data__EPS without NRI';

const m = (metric_code: string, year: number, numeric_value: number | null): MetricRow =>
  ({ metric_code, target_date: `${year}-12-31`, numeric_value });

describe('priceVsMetric', () => {
  it('pairs the two series by fiscal year, oldest first', () => {
    const out = priceVsMetric([m(PRICE, 2023, 100), m(FCF, 2023, 5), m(PRICE, 2024, 120), m(FCF, 2024, 6)]);
    expect(out).toEqual([
      { year: 2023, price: 100, value: 5 },
      { year: 2024, price: 120, value: 6 },
    ]);
  });

  it('reads every section spelling — one cohort each', () => {
    const out = priceVsMetric([
      m('annuals__per_share_data__Month End Stock Price', 2024, 100),
      m('annuals__per_share_data_array__Free Cash Flow per Share', 2024, 4),
    ]);
    expect(out).toEqual([{ year: 2024, price: 100, value: 4 }]);
  });

  it('keeps a year only one series reports — the gap is information', () => {
    // A company that stopped reporting FCF is not a company whose FCF went flat.
    const out = priceVsMetric([m(PRICE, 2023, 100), m(FCF, 2023, 5), m(PRICE, 2024, 120)]);
    expect(out[1]).toEqual({ year: 2024, price: 120, value: null });
  });

  it('takes the LAST n fiscal years', () => {
    const rows = [2015, 2016, 2017, 2018].flatMap((y) => [m(PRICE, y, y), m(FCF, y, 1)]);
    expect(priceVsMetric(rows, FCF_PS_CODES, 2).map((p) => p.year)).toEqual([2017, 2018]);
  });

  it('keeps the later observation when a year-end change reports twice', () => {
    const rows: MetricRow[] = [
      { metric_code: PRICE, target_date: '2024-03-31', numeric_value: 90 },
      { metric_code: PRICE, target_date: '2024-12-31', numeric_value: 110 },
    ];
    expect(priceVsMetric(rows)[0].price).toBe(110);
  });
});

// The FCF | EPS switch. The maths is deliberately shared; what must NOT be shared is which rows it
// reads and what the panel calls them.
describe('BASIS — the two bases the tab switches between', () => {
  const rows = [
    m(PRICE, 2024, 100),
    m(FCF, 2024, 5),      // 5% FCF yield
    m(EPS, 2024, 8),      // 8% earnings yield — a capital-light business earns more than it converts
  ];

  it('reads a DIFFERENT series per basis off the same payload', () => {
    expect(priceVsMetric(rows, BASIS.fcf.codes)[0].value).toBe(5);
    expect(priceVsMetric(rows, BASIS.eps.codes)[0].value).toBe(8);
  });

  it('⚠ EPS is `EPS without NRI`, not `EPS (Diluted)`', () => {
    // The rest of the app (egmInputs, earnings/types) values on the NRI-stripped line. Reading raw
    // diluted EPS here would make this tab disagree with the EGM tab in the same modal.
    expect(EPS_PS_CODES.every((c) => c.endsWith('EPS without NRI'))).toBe(true);
    expect(EPS_PS_CODES.some((c) => c.includes('Diluted'))).toBe(false);
  });

  it('covers all three section spellings on both bases', () => {
    // Match one spelling and a whole cohort of companies reads as having no data.
    for (const codes of [FCF_PS_CODES, EPS_PS_CODES]) {
      expect(codes).toHaveLength(3);
      expect(new Set(codes.map((c) => c.split('__')[1]))).toEqual(
        new Set(['Per Share Data', 'per_share_data', 'per_share_data_array']));
    }
  });

  it('never lets the two bases share a label', () => {
    // The failure mode is an earnings yield rendered under an FCF label: not a broken panel, a
    // plausible valuation of a company nobody analysed.
    const f = BASIS.fcf; const e = BASIS.eps;
    for (const k of ['tab', 'perShare', 'yieldTitle', 'yieldInline', 'negativeYear'] as const) {
      expect(f[k]).not.toBe(e[k]);
    }
  });

  it('yields the inverse of the P/E on the EPS basis', () => {
    // 8 EPS on a 100 price is a 12.5 P/E and an 8% earnings yield — the same fact twice.
    const y = yieldOf(8, 100);
    expect(y).toBe(8);
    expect(100 / (y as number)).toBeCloseTo(12.5, 6);
  });

  it('⚠ has an analyst consensus for EPS and NONE for FCF', () => {
    // Not a gap in our ingest — nobody forecasts capex, so no free-cash-flow consensus exists to
    // fetch. `null` is what makes the forward half of the chart absent rather than modelled.
    expect(BASIS.eps.estimateCodes).toEqual(EPS_EST_CODES);
    expect(BASIS.fcf.estimateCodes).toBeNull();
  });

  it('⚠ prefers the NRI-stripped estimate, matching the NRI-stripped history', () => {
    // AB Sagax 2026: 12.09 on the NRI line vs 13.30 on the other. Dividing today's price by the
    // wrong one steps the multiple exactly where history hands over to forecast — a re-rating
    // that is pure bookkeeping.
    expect(EPS_EST_CODES[0]).toBe('annual_eps_nri_estimate');
    expect(BASIS.eps.codes[0]).toContain('EPS without NRI');
  });
});

describe('multipleOf — the forward chart', () => {
  it('is the reciprocal of the yield, times 100', () => {
    expect(multipleOf(100, 8)).toBeCloseTo(12.5, 6);
    expect(multipleOf(100, 8) as number).toBeCloseTo(100 / (yieldOf(8, 100) as number), 9);
  });

  it('⚠ REFUSES a non-positive denominator — the opposite rule to yieldOf, on purpose', () => {
    // −5% is a real, plottable yield. The same year as a multiple is −20×, which sorts below
    // every cheap year on any axis and reads as the bargain of the decade.
    expect(yieldOf(-2, 40)).toBeCloseTo(-5);      // kept
    expect(multipleOf(40, -2)).toBeNull();        // dropped
    expect(multipleOf(40, 0)).toBeNull();
  });

  it('needs a positive price too', () => {
    expect(multipleOf(null, 8)).toBeNull();
    expect(multipleOf(0, 8)).toBeNull();
    expect(multipleOf(100, null)).toBeNull();
  });
});

describe('forwardEstimates', () => {
  const est = (code: string, year: number, v: number): MetricRow =>
    ({ metric_code: code, target_date: `${year}-12-31`, numeric_value: v });

  it('returns the future fiscal years, oldest first', () => {
    const rows = [2026, 2027, 2028].map((y) => est(EPS_EST_CODES[0], y, y - 2013));
    expect(forwardEstimates(rows, EPS_EST_CODES, 2025)).toEqual([
      { year: 2026, value: 13 }, { year: 2027, value: 14 }, { year: 2028, value: 15 },
    ]);
  });

  it('⚠ drops an estimate the company has already reported', () => {
    // GuruFocus keeps the row after the actual lands. Left in, the ladder opens with a forecast
    // of a year we hold the actual for — the same year twice, once measured and once guessed.
    const rows = [est(EPS_EST_CODES[0], 2025, 11), est(EPS_EST_CODES[0], 2026, 13)];
    expect(forwardEstimates(rows, EPS_EST_CODES, 2025).map((f) => f.year)).toEqual([2026]);
  });

  it('⚠ is a PRIORITY list, not a union', () => {
    // Filling 2026 from the NRI series and 2027 from the other puts that convention step INSIDE
    // the forecast, where nothing marks it at all. First code that answers wins, outright.
    const rows = [est(EPS_EST_CODES[0], 2026, 12.09), est(EPS_EST_CODES[1], 2027, 14.06)];
    expect(forwardEstimates(rows, EPS_EST_CODES, 2025)).toEqual([{ year: 2026, value: 12.09 }]);
  });

  it('falls through to the next code when the first has nothing', () => {
    const rows = [est(EPS_EST_CODES[1], 2026, 13.3)];
    expect(forwardEstimates(rows, EPS_EST_CODES, 2025)).toEqual([{ year: 2026, value: 13.3 }]);
  });

  it('is empty when there are no estimates at all', () => {
    expect(forwardEstimates([], EPS_EST_CODES, 2025)).toEqual([]);
  });
});

// ⚠ THE RULE THIS FILE EXISTS TO HOLD: the forward half of the multiple chart is published
// consensus or it is NOTHING. A trend fit shipped there briefly and was removed — it drew at the
// same weight, with the same decimals, on the same axis as a consensus, and a reader has no way to
// tell a house extrapolation from what the market actually expects.
describe('forwardFigures — nothing on the multiple chart is predicted by us', () => {
  const est = (year: number, v: number): MetricRow =>
    ({ metric_code: EPS_EST_CODES[0], target_date: `${year}-12-31`, numeric_value: v });
  const rows = [est(2026, 12.09), est(2027, 12.75)];

  it('returns the consensus on a basis that has one', () => {
    expect(forwardFigures(rows, BASIS.eps, 2025)).toEqual([
      { year: 2026, value: 12.09 }, { year: 2027, value: 12.75 },
    ]);
  });

  it('⚠ returns NOTHING on FCF — even with estimate rows sitting right there', () => {
    // The rows above are EPS estimates and would happily divide into a price. Reaching for them,
    // or for the fitted trend, is how the chart grows a forecast nobody published.
    expect(forwardFigures(rows, BASIS.fcf, 2025)).toEqual([]);
  });

  it('returns nothing for a company analysts do not cover, on either basis', () => {
    expect(forwardFigures([], BASIS.eps, 2025)).toEqual([]);
    expect(forwardFigures([], BASIS.fcf, 2025)).toEqual([]);
  });
});

describe('medianOf', () => {
  it('takes the middle, and averages the middle pair on an even count', () => {
    expect(medianOf([10, 30, 20])).toBe(20);
    expect(medianOf([10, 20, 30, 40])).toBe(25);
    expect(medianOf([])).toBeNull();
  });

  it('⚠ ignores the collapsed-earnings year a mean would follow', () => {
    // Nine ordinary years and one where earnings nearly touched zero. The mean says this company
    // typically trades at 45×; it has traded at ~20× in nine years out of ten.
    const xs = [18, 19, 20, 21, 22, 20, 19, 21, 20, 300];
    const mean = xs.reduce((a, v) => a + v, 0) / xs.length;
    expect(mean).toBeGreaterThan(45);
    expect(medianOf(xs)).toBeCloseTo(20, 6);
  });
});

// The live-price horizon. The whole reason these exist: with the "current" price now being
// today's yfinance close rather than the fiscal year-end one, the CAGR's divisor is the distance
// from the PRICE's date to the target — not the projection's nominal length.
describe('latestDateOf', () => {
  const CODES = [PRICE, 'annuals__per_share_data__Month End Stock Price'];

  it('returns the newest target_date across every code spelling', () => {
    expect(latestDateOf([
      m(PRICE, 2023, 100),
      { metric_code: 'annuals__per_share_data__Month End Stock Price', target_date: '2025-06-30', numeric_value: 130 },
      m(PRICE, 2024, 120),
    ], CODES)).toBe('2025-06-30');
  });

  it('ignores other metrics and null values', () => {
    // A null is not an observation — dating the series off one would claim a year we cannot price.
    expect(latestDateOf([m(PRICE, 2023, 100), m(FCF, 2030, 9), m(PRICE, 2024, null)], CODES))
      .toBe('2023-12-31');
  });

  it('is null when nothing matches', () => {
    expect(latestDateOf([m(FCF, 2024, 5)], CODES)).toBeNull();
  });
});

describe('addYears / yearsBetween', () => {
  it('shifts the fiscal year end by whole years', () => {
    expect(addYears('2025-09-30', 2)).toBe('2027-09-30');
    expect(addYears(null, 2)).toBeNull();
  });

  it('measures the horizon a live price actually has', () => {
    // Filed to 2025-09-30, target 2027-09-30, priced today (say 2026-07-29): ~1.17 years left,
    // NOT the 2 the projection is described by. Dividing by 2 would understate the return.
    expect(yearsBetween('2026-07-29', '2027-09-30')).toBeCloseTo(1.17, 2);
  });

  it('is exactly the projection length when the price is the fiscal one', () => {
    expect(yearsBetween('2025-09-30', addYears('2025-09-30', 2))).toBeCloseTo(2.0, 2);
  });

  it('refuses a horizon that has already closed', () => {
    // A company that has not filed in over two years has a target year already in the past. A
    // negative exponent turns a target BELOW the price into a positive-looking CAGR.
    expect(yearsBetween('2026-07-29', '2026-01-01')).toBeNull();
    expect(yearsBetween('2026-07-29', '2026-07-29')).toBeNull();
    expect(yearsBetween(null, '2027-09-30')).toBeNull();
  });

  it('annualises the same gain differently over the two horizons', () => {
    // The bug this guards: same prices, same target, divisor 2 vs 1.17.
    expect(cagrBetween(100, 150, 2)).toBeCloseTo(0.2247, 4);
    expect(cagrBetween(100, 150, 1.17)).toBeCloseTo(0.4142, 4);
  });
});

describe('yieldOf', () => {
  it('is the per-share figure over the year-end price, as a percent', () => {
    expect(yieldOf(6, 120)).toBeCloseTo(5);
  });

  it('⚠ KEEPS a negative yield — this is where a yield and a multiple part company', () => {
    // −5% reads as "burned cash equal to 5% of the price", which is what happened. The same year
    // as a multiple would be −20x, sorting below every cheap year as if it were the bargain of
    // the decade. The ratio does not invert across zero, so nothing is dropped.
    expect(yieldOf(-2, 40)).toBeCloseTo(-5);
  });

  it('needs a positive price — the denominator is not optional', () => {
    expect(yieldOf(6, null)).toBeNull();
    expect(yieldOf(6, 0)).toBeNull();
    expect(yieldOf(null, 120)).toBeNull();
  });
});

describe('rebase', () => {
  it('indexes both series to 100 at the anchor', () => {
    const out = rebase([{ year: 2023, price: 100, value: 5 }, { year: 2024, price: 150, value: 6 }]);
    expect(out.anchor).toBe(2023);
    expect(out.rows[1].price).toBeCloseTo(150);
    expect(out.rows[1].value).toBeCloseTo(120);   // price ran ahead of the cash
  });

  it('⚠ anchors on the first year BOTH are positive, not the first year shown', () => {
    // Rebasing off a cash-burn year divides by a negative: every later point flips sign and the
    // chart draws a recovery as a collapse.
    const out = rebase([
      { year: 2022, price: 40, value: -2 },
      { year: 2023, price: 50, value: 4 },
      { year: 2024, price: 75, value: 8 },
    ]);
    expect(out.anchor).toBe(2023);
    expect(out.rows[2].value).toBeCloseTo(200);
    // The burn year still plots — below zero, which is what a burn looks like against a base of 100.
    expect(out.rows[0].value).toBeCloseTo(-50);
  });

  it('has no index at all when no year has both positive', () => {
    const out = rebase([{ year: 2024, price: 40, value: -2 }]);
    expect(out.anchor).toBeNull();
    expect(out.rows).toEqual([]);
  });
});

describe('cagrOf', () => {
  it('compounds between the first and last positive observation', () => {
    const pts = [{ year: 2014, price: 100, value: 1 }, { year: 2024, price: 200, value: 2 }];
    const c = cagrOf(pts, (p) => p.price);
    expect(c?.years).toBe(10);
    expect(c?.pct).toBeCloseTo(7.177, 2);       // 2^(1/10) − 1
  });

  it('skips a negative start rather than compounding out of it', () => {
    // 5 Ã· −5 is negative and its 4th root is not a number. Dropping the burn year leaves one
    // positive point, and one point is not a rate — so there is no CAGR, which is the answer.
    const pts = [{ year: 2020, price: 10, value: -5 }, { year: 2024, price: 20, value: 5 }];
    expect(cagrOf(pts, (p) => p.value)).toBeNull();
  });

  it('measures the window it actually spans, not the window on screen', () => {
    const pts = [{ year: 2020, price: 10, value: -5 }, { year: 2022, price: 15, value: 4 },
      { year: 2024, price: 20, value: 9 }];
    const c = cagrOf(pts, (p) => p.value);
    expect(c).toMatchObject({ from: 2022, to: 2024, years: 2 });
  });

  it('is null for a single point — a CAGR off one observation is not a rate', () => {
    expect(cagrOf([{ year: 2024, price: 10, value: 1 }], (p) => p.price)).toBeNull();
  });
});

describe('priceAtYield / cagrBetween — the price-target calculator', () => {
  it('reproduces the worked case', () => {
    // 4.05 of FCF/share demanded at a 5.3% yield -> 76.42, against 200.77 today = -38.3%/yr.
    const target = priceAtYield(4.05, 5.3) as number;
    expect(target).toBeCloseTo(76.42, 2);
    expect(cagrBetween(200.77, target, 2) as number).toBeCloseTo(-0.383, 3);
  });

  it('⚠ a zero or negative yield has no price', () => {
    // 0% divides to infinity — any price is "justified" by no cash flow at all; a negative yield
    // flips the sign and returns a healthy-looking number built on nonsense.
    expect(priceAtYield(4.05, 0)).toBeNull();
    expect(priceAtYield(4.05, -2)).toBeNull();
  });

  it('⚠ a cash-burning forecast has no price either', () => {
    expect(priceAtYield(-1.2, 5.3)).toBeNull();
    expect(priceAtYield(0, 5.3)).toBeNull();
  });

  it('a higher demanded yield means a lower price', () => {
    expect(priceAtYield(4.05, 8) as number).toBeLessThan(priceAtYield(4.05, 5.3) as number);
  });

  it('cagrBetween refuses what it cannot compound', () => {
    expect(cagrBetween(0, 50, 2)).toBeNull();        // no base
    expect(cagrBetween(100, -50, 2)).toBeNull();     // no root of a negative
    expect(cagrBetween(100, 200, 0)).toBeNull();     // no window
  });

  it('is null-safe on every missing input', () => {
    expect(priceAtYield(null, 5)).toBeNull();
    expect(priceAtYield(4, null)).toBeNull();
    expect(cagrBetween(null, 1, 2)).toBeNull();
  });
});

describe('priceTarget — one computation, two readers', () => {
  it('bundles the calculator panel and the chart projection into one answer', () => {
    // The panel prints these and the chart draws the price line out to `forecastPrice`; computing
    // it twice would let the line land somewhere the panel does not say.
    const t = priceTarget(2.61, 200.77, 4.05, 5.3, 2);
    expect(t.currentYield).toBeCloseTo(1.3, 1);
    expect(t.forecastPrice).toBeCloseTo(76.42, 2);
    expect(t.cagr as number).toBeCloseTo(-0.383, 3);
  });

  it('carries nulls through rather than inventing a target', () => {
    const t = priceTarget(2.61, null, 4.05, 5.3, 2);
    expect(t.currentYield).toBeNull();       // no price, no yield
    expect(t.forecastPrice).toBeCloseTo(76.42, 2);   // ...but the target still stands
    expect(t.cagr).toBeNull();               // nothing to measure the return from
  });

  it('has no target when the demanded yield is not positive', () => {
    const t = priceTarget(2.61, 200.77, 4.05, 0, 2);
    expect(t.forecastPrice).toBeNull();
    expect(t.cagr).toBeNull();
  });
});

