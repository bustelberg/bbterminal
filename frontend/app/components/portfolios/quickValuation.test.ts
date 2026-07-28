import { describe, it, expect } from 'vitest';
import {
  cagrBetween, cagrOf, fcfYieldOf, priceAtYield, priceTarget, priceVsFcf, rebase, type MetricRow,
} from './quickValuation';

const PRICE = 'annuals__Per Share Data__Month End Stock Price';
const FCF = 'annuals__Per Share Data__Free Cash Flow per Share';

const m = (metric_code: string, year: number, numeric_value: number | null): MetricRow =>
  ({ metric_code, target_date: `${year}-12-31`, numeric_value });

describe('priceVsFcf', () => {
  it('pairs the two series by fiscal year, oldest first', () => {
    const out = priceVsFcf([m(PRICE, 2023, 100), m(FCF, 2023, 5), m(PRICE, 2024, 120), m(FCF, 2024, 6)]);
    expect(out).toEqual([
      { year: 2023, price: 100, fcf: 5 },
      { year: 2024, price: 120, fcf: 6 },
    ]);
  });

  it('reads every section spelling — one cohort each', () => {
    const out = priceVsFcf([
      m('annuals__per_share_data__Month End Stock Price', 2024, 100),
      m('annuals__per_share_data_array__Free Cash Flow per Share', 2024, 4),
    ]);
    expect(out).toEqual([{ year: 2024, price: 100, fcf: 4 }]);
  });

  it('keeps a year only one series reports — the gap is information', () => {
    // A company that stopped reporting FCF is not a company whose FCF went flat.
    const out = priceVsFcf([m(PRICE, 2023, 100), m(FCF, 2023, 5), m(PRICE, 2024, 120)]);
    expect(out[1]).toEqual({ year: 2024, price: 120, fcf: null });
  });

  it('takes the LAST n fiscal years', () => {
    const rows = [2015, 2016, 2017, 2018].flatMap((y) => [m(PRICE, y, y), m(FCF, y, 1)]);
    expect(priceVsFcf(rows, 2).map((p) => p.year)).toEqual([2017, 2018]);
  });

  it('keeps the later observation when a year-end change reports twice', () => {
    const rows: MetricRow[] = [
      { metric_code: PRICE, target_date: '2024-03-31', numeric_value: 90 },
      { metric_code: PRICE, target_date: '2024-12-31', numeric_value: 110 },
    ];
    expect(priceVsFcf(rows)[0].price).toBe(110);
  });
});

describe('fcfYieldOf', () => {
  it('is FCF per share over the year-end price, as a percent', () => {
    expect(fcfYieldOf(6, 120)).toBeCloseTo(5);
  });

  it('⚠ KEEPS a negative yield — this is where a yield and a multiple part company', () => {
    // −5% reads as "burned cash equal to 5% of the price", which is what happened. The same year
    // as a multiple would be −20x, sorting below every cheap year as if it were the bargain of
    // the decade. The ratio does not invert across zero, so nothing is dropped.
    expect(fcfYieldOf(-2, 40)).toBeCloseTo(-5);
  });

  it('needs a positive price — the denominator is not optional', () => {
    expect(fcfYieldOf(6, null)).toBeNull();
    expect(fcfYieldOf(6, 0)).toBeNull();
    expect(fcfYieldOf(null, 120)).toBeNull();
  });
});

describe('rebase', () => {
  it('indexes both series to 100 at the anchor', () => {
    const out = rebase([{ year: 2023, price: 100, fcf: 5 }, { year: 2024, price: 150, fcf: 6 }]);
    expect(out.anchor).toBe(2023);
    expect(out.rows[1].price).toBeCloseTo(150);
    expect(out.rows[1].fcf).toBeCloseTo(120);   // price ran ahead of the cash
  });

  it('⚠ anchors on the first year BOTH are positive, not the first year shown', () => {
    // Rebasing off a cash-burn year divides by a negative: every later point flips sign and the
    // chart draws a recovery as a collapse.
    const out = rebase([
      { year: 2022, price: 40, fcf: -2 },
      { year: 2023, price: 50, fcf: 4 },
      { year: 2024, price: 75, fcf: 8 },
    ]);
    expect(out.anchor).toBe(2023);
    expect(out.rows[2].fcf).toBeCloseTo(200);
    // The burn year still plots — below zero, which is what a burn looks like against a base of 100.
    expect(out.rows[0].fcf).toBeCloseTo(-50);
  });

  it('has no index at all when no year has both positive', () => {
    const out = rebase([{ year: 2024, price: 40, fcf: -2 }]);
    expect(out.anchor).toBeNull();
    expect(out.rows).toEqual([]);
  });
});

describe('cagrOf', () => {
  it('compounds between the first and last positive observation', () => {
    const pts = [{ year: 2014, price: 100, fcf: 1 }, { year: 2024, price: 200, fcf: 2 }];
    const c = cagrOf(pts, (p) => p.price);
    expect(c?.years).toBe(10);
    expect(c?.pct).toBeCloseTo(7.177, 2);       // 2^(1/10) − 1
  });

  it('skips a negative start rather than compounding out of it', () => {
    // 5 ÷ −5 is negative and its 4th root is not a number. Dropping the burn year leaves one
    // positive point, and one point is not a rate — so there is no CAGR, which is the answer.
    const pts = [{ year: 2020, price: 10, fcf: -5 }, { year: 2024, price: 20, fcf: 5 }];
    expect(cagrOf(pts, (p) => p.fcf)).toBeNull();
  });

  it('measures the window it actually spans, not the window on screen', () => {
    const pts = [{ year: 2020, price: 10, fcf: -5 }, { year: 2022, price: 15, fcf: 4 },
      { year: 2024, price: 20, fcf: 9 }];
    const c = cagrOf(pts, (p) => p.fcf);
    expect(c).toMatchObject({ from: 2022, to: 2024, years: 2 });
  });

  it('is null for a single point — a CAGR off one observation is not a rate', () => {
    expect(cagrOf([{ year: 2024, price: 10, fcf: 1 }], (p) => p.price)).toBeNull();
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
