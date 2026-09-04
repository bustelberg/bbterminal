import { describe, expect, it } from 'vitest';
import rawFixture from './__fixtures__/blendParity.json';
import { buildBlend, type Resp, type Row } from './fundamentalBlend';

/**
 * THE CLIENT HALF OF A CROSS-LANGUAGE PARITY PIN — `backend/tests/test_blend_client_parity.py` is
 * the other, and the two read the SAME fixture.
 *
 * ⚠⚠ NOTHING PINNED THESE TWO AGAINST EACH OTHER UNTIL 2026-09-03, WHICH IS WHY THEY DRIFTED.
 * `blend_series` draws the chart and `buildBlend` reproduces it under the chart, in another
 * language, from another endpoint's payload — the whole design of the drill-down rests on the two
 * agreeing, and `tests/test_blend_stream_parity.py` only pins the two SERVER paths against one
 * another. So the seam with no test on it is the seam that moved: the Fundamental modal was
 * reported disagreeing with itself on ACWI, +10.9%/yr share price on `Graphs` against +10.8% in
 * `Tables`, and +4.5% against +4.6% on revenue.
 *
 * ⚠ A DIFFERENCE THAT SMALL IS THE POINT. Nothing looks wrong on either screen — both lines are
 * smooth, both clear their coverage floors, both drill-downs reconcile to their own line. Only the
 * two numbers side by side say anything, and only to a reader who happens to open both tabs.
 *
 * ⚠ THE EXPECTED SERIES IS NOT A CAPTURED BASELINE. It is worked by hand in the fixture's `_doc`;
 * a baseline snapshotted off one implementation would pin that implementation's bugs as the
 * contract, which on a parity test is the one thing that cannot be allowed.
 */
type Fixture = {
  metric: string;
  metric_code: string;
  periods: string[];
  members: { isin: string; name: string; market_cap_eur: number;
             values: Record<string, number> }[];
  expected_drawn: string[];
  expected_level: Record<string, number>;
  expected_covered_names_pct: Record<string, number>;
  expected_contributors: number;
};
const fx = rawFixture as unknown as Fixture;

/** The `all_constituents=True` payload — EVERY constituent, cap-less ones at weight 0. */
const payload = (): { resp: Resp; rows: Row[] } => {
  const total = fx.members.reduce((a, m) => a + m.market_cap_eur, 0);
  const rows: Row[] = fx.members.map((m) => ({
    isin: m.isin,
    name: m.name,
    // The share of the index this constituent carries — 0 for one with no stored cap, exactly as
    // `portfolio_revenue_matrix` computes it (`100 × weight_by[ci] / total_w`).
    weight_pct: (100 * m.market_cap_eur) / total,
    currency: 'EUR',
    ticker: null,
    exchange: null,
    status: 'ok',
    revenue: m.values,
    market_cap_eur: m.market_cap_eur,
    // ⚠ `{}`, NOT ABSENT, for a constituent with no cap — that is what the endpoint ships
    // (`caps.get(company_id, {})`), and the two are different answers to `wAt`: an empty map means
    // "out of every period's average", an absent one means "one flat weight for every period".
    market_cap_by_period: Object.fromEntries(
      m.market_cap_eur > 0 ? fx.periods.map((p) => [p, m.market_cap_eur]) : [],
    ),
  }));
  return { resp: { years: [...fx.periods], rows, holdings: rows.length }, rows };
};

describe('buildBlend against the server blend', () => {
  it('draws the line the server draws, over the members the server was handed', () => {
    const { resp } = payload();
    const blend = buildBlend(resp, fx.metric);

    // ⚠ THE DRAWN SET FIRST, BECAUSE IT IS WHAT DIVERGED. A refused period is not a missing point:
    // the chain skips it, the next step spans two intervals instead of one, and every level after
    // it is a product over a different partition. Asserting only the endpoint would let a line
    // reach the right 2018 by a route the chart never took.
    expect(Object.keys(blend.level).sort()).toEqual([...fx.expected_drawn]);

    for (const [period, want] of Object.entries(fx.expected_level)) {
      expect(blend.level[period].value).toBeCloseTo(want, 9);
    }
  });

  it('counts the names floor over the members the LINE has, not the rows the TABLE lists', () => {
    const { resp } = payload();
    const blend = buildBlend(resp, fx.metric);

    // ⚠⚠ THE DEFECT ITSELF, IN ONE ASSERTION. `Echo` has no market cap, so it can never carry a
    // weight and is out of every average — but it was still in `parts.length`, the denominator of
    // `coverN[y] / parts.length`. Two reporters out of five reads 40% and is refused; out of the
    // four members the server actually blends it reads 50% and is drawn. `earnings.py`'s
    // `_load_and_expand_members` predicts exactly this and guards the SERVER against it; the guard
    // did not travel with the payload.
    expect(blend.contributors).toBe(fx.expected_contributors);
    for (const [period, want] of Object.entries(fx.expected_covered_names_pct)) {
      expect(blend.coveredNames[period]).toBeCloseTo(want, 9);
    }
  });

  it('leaves a cap-less constituent out of the line entirely', () => {
    const { resp, rows } = payload();
    const blend = buildBlend(resp, fx.metric);
    const echo = rows[rows.length - 1];

    // ⚠ ITS SERIES RUNS 100 -> 800 (+100%/yr), so a leak would be unmissable rather than a rounding
    // difference — the line would be dragged from +9.2%/yr to well past +20%.
    expect(echo.market_cap_eur).toBe(0);
    for (const period of fx.periods) expect(blend.wAt(echo, period)).toBeNull();
    expect(blend.contrib.get(echo)).toBeUndefined();

    // ⚠ AND IT IS STILL A ROW OF THE TABLE. `all_constituents=True` exists so the drill-down can
    // say "in the index, not in the line"; dropping it from the payload's rows would hide the one
    // fact that panel is for. What changed is the DENOMINATOR, not the listing.
    expect(resp.rows).toContain(echo);
  });

  it('is a no-op for a portfolio, whose 0-weight holdings the server does count', () => {
    // ⚠⚠ `market_cap_eur` IS THE INDEX PATH'S OWN FIELD and the subject of `require_market_cap`,
    // so absence has to mean "leave this alone". A book's holding weight is not a market cap, and
    // the two sides already agree about one: `_prepare` drops it as `no_weight` while `total_n`
    // still counts it, which is 2 of 3 here — so a client that dropped the row from `parts` would
    // report 100% where the server reports 66.7%, the same class of divergence pointing the other
    // way.
    const rows: Row[] = [
      { isin: 'A', name: 'A', weight_pct: 60, currency: 'EUR', ticker: null, exchange: null,
        status: 'ok', revenue: { 2023: 100, 2024: 110 } },
      { isin: 'B', name: 'B', weight_pct: 40, currency: 'EUR', ticker: null, exchange: null,
        status: 'ok', revenue: { 2023: 100, 2024: 120 } },
      { isin: 'C', name: 'C', weight_pct: 0, currency: 'EUR', ticker: null, exchange: null,
        status: 'ok', revenue: { 2023: 100, 2024: 100 } },
    ];
    const blend = buildBlend({ years: ['2023', '2024'], rows, holdings: 3 });

    expect(blend.contributors).toBe(3);
    expect(blend.coveredNames['2024']).toBeCloseTo((100 * 2) / 3, 9);
    // (60·0.10 + 40·0.20) ÷ 100 = +14%. C carries no weight, so it is in neither side of the mean.
    expect(blend.level['2024'].value).toBeCloseTo(114, 9);
  });
});
