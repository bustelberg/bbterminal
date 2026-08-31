import { describe, expect, it } from 'vitest';
import { buildBlend, type Blend, type Resp, type Row } from './fundamentalBlend';

/**
 * THE PER-MEMBER DECOMPOSITION OF THE BLENDED LINE'S MOVE — the `Contribution` view of the
 * drill-down matrix, in percentage points.
 *
 * ⚠⚠ THE ONLY PROPERTY THAT MATTERS IS THAT THE COLUMN **ADDS UP TO THE FOOTER**. A pp figure that
 * is merely plausible is worse than no figure at all here, because the whole point of the view is
 * that you can sort it and believe the ranking: the top row is supposed to be the company that moved
 * the line most, and nothing on screen would look wrong if the shares were taken over the wrong
 * denominator — they would still order the same way most of the time, still sum to something near
 * the move, and quietly disagree with the line whenever the membership changed under them.
 *
 * The denominator is the trap, and it has three plausible wrong answers, all of which are on this
 * screen already: the table's own weight column, this period's `denom[y]`, and the coverage total.
 * The right one is Σw over the members that span THE INTERVAL — see `TheDenominatorIsTheInterval`
 * below, where `denom[y]` would halve every contribution while the footer stayed put.
 */

const row = (isin: string, weight: number, revenue: Record<string, number | null>): Row => ({
  isin, name: isin, weight_pct: weight, currency: 'EUR', ticker: isin, exchange: 'XAMS',
  status: 'ok', revenue,
});
const resp = (years: string[], rows: Row[]): Resp => ({ years, rows, holdings: rows.length });
/** Σ of the pp column, exactly as a reader adds it up down the screen. */
const sumPp = (b: Blend, rows: Row[], y: string): number =>
  rows.reduce((a, r) => a + (b.contrib.get(r)?.[y]?.pp ?? 0), 0);

describe('buildBlend contributions', () => {
  it('sums to the line’s own move, which is what makes it a decomposition', () => {
    const rows = [
      row('A', 50, { 2023: 100, 2024: 120 }),   // +20%
      row('B', 30, { 2023: 200, 2024: 180 }),   // −10%
      row('C', 20, { 2023: 50, 2024: 75 }),     // +50%
    ];
    const b = buildBlend(resp(['2023', '2024'], rows));

    // (50·0.20 + 30·−0.10 + 20·0.50) ÷ 100 = +17%
    expect(b.step['2024'].growthPct).toBeCloseTo(17, 10);
    expect(b.contrib.get(rows[0])!['2024'].pp).toBeCloseTo(10, 10);
    expect(b.contrib.get(rows[1])!['2024'].pp).toBeCloseTo(-3, 10);
    expect(b.contrib.get(rows[2])!['2024'].pp).toBeCloseTo(10, 10);
    // The identity, asserted rather than assumed — the same check the footer makes on screen.
    expect(sumPp(b, rows, '2024')).toBeCloseTo(b.step['2024'].growthPct, 10);
    // ⚠ B is the SECOND largest holding and the only detractor: signed order puts it last, which is
    // the finding. Ranking on magnitude would hide it between the two +10pp drivers.
    expect(b.contrib.get(rows[1])!['2024'].pp).toBeLessThan(0);
  });

  it('states both factors, and they multiply out to the pp', () => {
    const rows = [
      row('A', 70, { 2023: 100, 2024: 130 }),
      row('B', 30, { 2023: 100, 2024: 90 }),
    ];
    const b = buildBlend(resp(['2023', '2024'], rows));
    for (const r of rows) {
      const c = b.contrib.get(r)!['2024'];
      // What the cell's tooltip prints: `share × growth = pp`. If this drifts, the tooltip is
      // showing arithmetic that does not reach the number above it.
      // ⚠ Both factors are non-null on the growth path; on the euro-sum path a member with a
      // non-positive base has an exact pp and no factors at all. See `fund_by_period`.
      expect(c.sharePct! * c.growthPct! / 100).toBeCloseTo(c.pp, 10);
    }
    expect(sumPp(b, rows, '2024')).toBeCloseTo(b.step['2024'].growthPct, 10);
  });

  it('takes the share over the INTERVAL’s weight, not the period’s (the denominator trap)', () => {
    const rows = [
      row('A', 50, { 2023: 100, 2024: 120 }),   // spans the interval: +20%
      row('B', 50, { 2024: 200 }),              // first reports IN 2024 — no anchor to move from
    ];
    const b = buildBlend(resp(['2023', '2024'], rows));

    // B is in the period (it carries half the weight of 2024's average) and NOT in the move.
    expect(b.denom['2024']).toBeCloseTo(100, 10);
    expect(b.step['2024'].spanPct).toBeCloseTo(50, 10);
    expect(b.contrib.get(rows[1])?.['2024']).toBeUndefined();

    // The line moved +20%: A is the only member that could be measured over the interval.
    expect(b.step['2024'].growthPct).toBeCloseTo(20, 10);
    // ⚠ THE REGRESSION. Over `denom[y]` (100) A's share would read 50% and its contribution
    // +10.00pp — a column summing to half the move it sits under, with nothing on screen wrong.
    expect(b.contrib.get(rows[0])!['2024'].pp).toBeCloseTo(20, 10);
    expect(sumPp(b, rows, '2024')).toBeCloseTo(b.step['2024'].growthPct, 10);
  });

  it('gives a carried row exactly 0.00pp — a measurement, not an absence', () => {
    const rows = [
      row('A', 25, { 2023: 100, 2024: 110, 2025: 121 }),
      row('B', 25, { 2023: 100, 2024: 110, 2025: 121 }),
      row('C', 25, { 2023: 100, 2024: 110, 2025: 121 }),
      row('D', 25, { 2023: 100, 2024: 110 }),   // no 2025 — the line holds its 2024 figure
    ];
    const b = buildBlend(resp(['2023', '2024', '2025'], rows));

    expect(b.from.D['2025']).toBe('2024');               // it really is carried
    const d = b.contrib.get(rows[3])!['2025'];
    // ⚠ 0, NOT MISSING. D was in the average and did not move; a missing key means "not in this
    // step at all", which the cell renders as a dash. The two must not collapse: three quarters of
    // the basket rose 10% and the fourth stood still, which is why the line moved 7.5% and not 10%.
    expect(d.pp).toBe(0);
    expect(d.growthPct).toBe(0);
    expect(b.step['2025'].growthPct).toBeCloseTo(7.5, 10);
    expect(sumPp(b, rows, '2025')).toBeCloseTo(b.step['2025'].growthPct, 10);
  });

  it('has no step, and no contributions, in the line’s first drawn period', () => {
    const rows = [row('A', 100, { 2023: 100, 2024: 120 })];
    const b = buildBlend(resp(['2023', '2024'], rows));
    // Nothing is measured from the first point, so the column is dashes and the footer is blank —
    // rather than a column of +0.00pp, which would claim every member stood still.
    expect(b.step['2023']).toBeUndefined();
    expect(b.contrib.get(rows[0])?.['2023']).toBeUndefined();
    expect(b.contrib.get(rows[0])!['2024'].pp).toBeCloseTo(20, 10);
  });

  it('never contributes for a member with NO positive period at all', () => {
    const rows = [
      row('A', 50, { 2023: 100, 2024: 120 }),
      row('B', 50, { 2023: -5, 2024: -10 }),    // never positive ⇒ no base to divide by
    ];
    const b = buildBlend(resp(['2023', '2024'], rows));

    expect(b.excluded.has(rows[1])).toBe(true);
    // It contributes nothing anywhere — not 0.00pp, which would read as "held, didn't move".
    expect(b.contrib.get(rows[1])).toBeUndefined();
    // And it is not in the denominator either: A carries the whole move.
    expect(b.contrib.get(rows[0])!['2024'].pp).toBeCloseTo(b.step['2024'].growthPct, 10);
  });

  it('⚠ KEEPS a member whose FIRST period is negative but which recovers (the AMD case)', () => {
    /**
     * ⚠⚠ THIS PINNED THE OPPOSITE UNTIL 2026-08-25, AND THE OLD ASSERTION WAS THE BUG. The client
     * dropped any row whose first REPORTED period was ≤ 0, under a comment claiming it matched
     * `_prepare` — which skips to the first POSITIVE period and keeps the member. So a company
     * whose earliest year happened to be negative was in the CHART (server) and missing from the
     * drill-down that explains it (client): a footer that cannot reach the line above it, and a
     * Contribution column short by that company with nothing on screen wrong.
     */
    const rows = [
      row('A', 50, { 2022: 100, 2023: 100, 2024: 120 }),
      row('B', 50, { 2022: -5, 2023: 10, 2024: 20 }),   // recovers ⇒ rebased on 2023, kept
    ];
    const b = buildBlend(resp(['2022', '2023', '2024'], rows));

    expect(b.excluded.has(rows[1])).toBe(false);
    // Its pre-base period goes with the base — it has nothing at 2022, so it is not in that
    // period's denominator and cannot move the 2022 → 2023 step.
    expect(b.contrib.get(rows[1])?.['2023']).toBeUndefined();
    // …and from its own base onward it is a full member: +100% at half the weight.
    expect(b.contrib.get(rows[1])!['2024'].growthPct).toBeCloseTo(100, 10);
    expect(sumPp(b, rows, '2024')).toBeCloseTo(b.step['2024'].growthPct, 10);
  });
});

/**
 * AN INDEX'S STEP IS WEIGHTED BY THE CAP IT HAD AT THE START OF THAT STEP.
 *
 * ⚠⚠ THE BUG THIS PINS READ +20.21%/yr WHERE THE ANSWER IS +11.14%/yr (2026-08-21). `buildBlend`
 * chains from weighted growth, `g = value(y)/value(anchor) − 1`, and took each member's weight at
 * `y` — the END of the interval. For revenue that is a mild inconsistency; for a PRICE series it is
 * nearly circular, because market cap = price × shares. A constituent that tripled carried ~3× the
 * weight in the very step where it tripled, and one that halved carried half: winners over-weighted
 * in their own winning step, losers under-weighted in theirs, and an index that reads high by
 * construction with no missing data anywhere.
 *
 * Measured on ACWI's `Month End Stock Price`, 1,512 constituents, 2015 → 2025: end-weighted the
 * index ran 100 → 630.2 (+20.21%/yr), anchor-weighted 100 → 287.6 (+11.14%/yr). ACWI really did
 * ~10-11%/yr on price.
 *
 * ⚠ THIS FILE IS THE CLIENT TWIN OF `_fundamental_blend.blend_series` AND EXISTS TO REPRODUCE THE
 * PLOTTED LINE — the `Tables` tab's rates and the drill-down's `Rebased` footer both come from here.
 * Weighted differently from the server it would print figures that disagree with the chart they
 * explain, and both would look entirely reasonable. Pinned on both sides: see
 * `backend/tests/test_blend_step_weight.py`, which asserts the same 100 → 175 panel.
 *
 * ⚠ A PORTFOLIO CANNOT HIT THIS. Without `market_cap_by_period`, `wAt` returns the holding weight
 * for every period, so anchor and end are the same number — which is why the book's own line was
 * right all along and only the benchmark beside it was inflated.
 */
describe('the step is weighted at the anchor, not at the period', () => {
  /** Two constituents, equal at the start and opposite after. The panel's own total market cap goes
   *  200 → 350, so a cap-weighted index that held both returned exactly +75%. There is no other
   *  defensible number, which is what makes this a test rather than a preference. */
  const capped = (isin: string, revenue: Record<string, number>,
    caps: Record<string, number>): Row => ({
    ...row(isin, 50, revenue), market_cap_by_period: caps,
  });
  const WINNER = capped('A', { 2020: 100, 2021: 300 }, { 2020: 100, 2021: 300 });   // ×3
  const LOSER = capped('B', { 2020: 100, 2021: 50 }, { 2020: 100, 2021: 50 });      // ÷2

  it('reproduces the index’s own market-cap move', () => {
    const b = buildBlend(resp(['2020', '2021'], [WINNER, LOSER]));
    expect(b.level['2020'].value).toBeCloseTo(100, 10);
    expect(b.level['2021'].value).toBeCloseTo(175, 10);
    expect(b.step['2021'].growthPct).toBeCloseTo(75, 10);
  });

  it('⚠ the end-weighted answer is the one that was shipped', () => {
    // (300·(+2.00) + 50·(−0.50)) ÷ 350 = +164.3% → level 264.3, against a true 175. Written out so
    // the size of the error lives in the test and not only in a commit message.
    const endWeighted = (300 * 2.0 + 50 * -0.5) / 350;
    expect(100 * (1 + endWeighted)).toBeCloseTo(264.29, 1);
    expect(buildBlend(resp(['2020', '2021'], [WINNER, LOSER])).level['2021'].value)
      .not.toBeCloseTo(264.29, 1);
  });

  it('the contribution column still adds up to the move it decomposes', () => {
    const b = buildBlend(resp(['2020', '2021'], [WINNER, LOSER]));
    expect(b.contrib.get(WINNER)!['2021'].pp).toBeCloseTo(100, 10);
    expect(b.contrib.get(LOSER)!['2021'].pp).toBeCloseTo(-25, 10);
    expect(sumPp(b, [WINNER, LOSER], '2021')).toBeCloseTo(b.step['2021'].growthPct, 10);
  });

  it('⚠ a portfolio is unaffected — no per-period caps, so anchor and end are one number', () => {
    const book = [row('A', 50, { 2020: 100, 2021: 300 }), row('B', 50, { 2020: 100, 2021: 50 })];
    expect(buildBlend(resp(['2020', '2021'], book)).level['2021'].value).toBeCloseTo(175, 10);
  });

  it('⚠ `spanPct` is coverage of THIS period’s weight, in one basis, so it stays a share', () => {
    // ⚠⚠ THE MOVE AND THE COVERAGE ANSWER DIFFERENT QUESTIONS AND NEED DIFFERENT BASES, which is
    // the mistake the anchor-weighting fix originally made here: it divided the anchor-weighted
    // `den` by the anchor's weight sum, giving "how much of the ANCHOR's weight survived" under a
    // tooltip that says "of this period's weight". Both sides are period-`y` weights now, so this
    // is a genuine subset share — it cannot exceed 100%, and the case above (where half the
    // period's weight cannot be measured over the interval) reads 50, not 100.
    expect(buildBlend(resp(['2020', '2021'], [WINNER, LOSER])).step['2021'].spanPct)
      .toBeCloseTo(100, 10);
  });
});

/**
 * THE EURO-SUM CONSTRUCTION — the client twin of `blend_series`'s aggregate branch and
 * `_level_breakdown`'s aggregate decomposition.
 *
 * ⚠⚠ THE TWO CONSTRUCTIONS DISAGREE BY MORE THAN 5pp/yr ON ACWI AND NEITHER LOOKS WRONG ON SCREEN.
 * Averaging per-member growth rates weighted by MARKET CAP gives a company with a big valuation and
 * small cash flow a big vote on cash-flow growth. Growth of a sum weights each member by its share
 * of the total being grown, which is the only weight the question admits. Measured: ACWI revenue
 * ~9.95%/yr averaged against +4.60%/yr summed; FCF/share +19.1% against +7.56%.
 *
 * ⚠ IT IS NOT ABOUT NEGATIVES. Revenue is never negative, so every zero-crossing rule is a no-op on
 * it, and the gap above is entirely the weight.
 */
const fundRow = (isin: string, weight: number, revenue: Record<string, number | null>,
                 fund: Record<string, number>, caps?: Record<string, number>): Row => ({
  ...row(isin, weight, revenue),
  fund_by_period: fund,
  ...(caps ? { market_cap_by_period: caps } : {}),
});

describe('buildBlend — the euro sum', () => {
  // Cap ranking is the REVERSE of the euro ranking, which is the only shape that tells the two
  // constructions apart — and the shape ACWI has.
  //   BIG_CAP  euros 10 → 20 (+100%), cap 900
  //   SMALL    euros 90 → 90 (   0%), cap 100
  // Euro total 100 → 110, so the index returned exactly +10%. Cap-weighting the RATES gives +90%.
  const mk = () => [
    fundRow('BIG', 90, { 2023: 10, 2024: 20 }, { 2023: 10, 2024: 20 },
            { 2023: 900, 2024: 900 }),
    fundRow('SML', 10, { 2023: 90, 2024: 90 }, { 2023: 90, 2024: 90 },
            { 2023: 100, 2024: 100 }),
  ];

  it('moves with the euro total, not with the cap-weighted average of the rates', () => {
    const rows = mk();
    const b = buildBlend(resp(['2023', '2024'], rows));
    expect(b.level['2023'].value).toBeCloseTo(100, 10);
    expect(b.level['2024'].value).toBeCloseTo(110, 10);       // not 190
    expect(b.step['2024'].growthPct).toBeCloseTo(10, 10);
  });

  it('falls back to the growth chain when no row carries euros', () => {
    // ⚠ THE ASSERTION THAT CATCHES A SILENT NON-FIRING. Same rows minus `fund_by_period`: if the
    // aggregate ever stopped running, this and the test above would print the same number and
    // nothing would say which construction had produced it.
    const rows = mk().map(({ fund_by_period: _drop, ...r }) => r as Row);
    const b = buildBlend(resp(['2023', '2024'], rows));
    expect(b.level['2024'].value).toBeCloseTo(190, 10);
  });

  it('shares out the move by share of the EUROS, and the column sums exactly', () => {
    const rows = mk();
    const b = buildBlend(resp(['2023', '2024'], rows));
    const big = b.contrib.get(rows[0])!['2024'];
    // ⚠ THE ENTIRE FINDING IN ONE ASSERTION: BIG holds 90% of the cap and 10% of the euros, and
    // its share of the move is the second.
    expect(big.sharePct).toBeCloseTo(10, 10);
    expect(big.growthPct).toBeCloseTo(100, 10);
    expect(big.pp).toBeCloseTo(10, 10);
    expect(sumPp(b, rows, '2024')).toBeCloseTo(b.step['2024'].growthPct, 10);
  });

  it('keeps a sign-crosser in the sum, with an exact pp and no factors', () => {
    // ⚠⚠ NOBODY IS DROPPED, WHICH THE GROWTH PATH CANNOT MANAGE. `share × growth` needs a positive
    // base; the difference form does not. Factors go null, the pp stays exact, the column sums.
    const rows = [
      fundRow('CRS', 50, { 2023: 12, 2024: 3 }, { 2023: -200, 2024: 300 }),
      fundRow('STD', 50, { 2023: 12, 2024: 12 }, { 2023: 1200, 2024: 1200 }),
    ];
    const b = buildBlend(resp(['2023', '2024'], rows));
    const c = b.contrib.get(rows[0])!['2024'];
    expect(c.growthPct).toBeNull();
    expect(c.sharePct).toBeNull();
    expect(c.pp).toBeCloseTo(50, 10);                          // +500 euros over a 1,000 base
    expect(sumPp(b, rows, '2024')).toBeCloseTo(b.step['2024'].growthPct, 10);
  });

  it('intersects each step, so a member joining is not counted as growth', () => {
    const rows = [
      fundRow('OLD', 50, { 2023: 1, 2024: 1.1 }, { 2023: 100, 2024: 110 }),
      fundRow('NEW', 50, { 2024: 5 }, { 2024: 500 }),
    ];
    const b = buildBlend(resp(['2023', '2024'], rows));
    expect(b.step['2024'].growthPct).toBeCloseTo(10, 10);
  });

  it('nets out a round trip through zero instead of marking the index for ever', () => {
    // The property that makes this construction right for FCF: the growth path floors a member at
    // −100% one year and refuses the ratio the next, so the round trip never closes.
    const rows = [
      fundRow('A', 50, { 2022: 1, 2023: 2, 2024: 1 }, { 2022: 100, 2023: -200, 2024: 100 }),
      fundRow('B', 50, { 2022: 9, 2023: 9, 2024: 9 }, { 2022: 900, 2023: 900, 2024: 900 }),
    ];
    const b = buildBlend(resp(['2022', '2023', '2024'], rows));
    expect(b.level['2023'].value).toBeCloseTo(70, 10);         // 700/1000
    expect(b.level['2024'].value).toBeCloseTo(100, 10);        // back to 1000/1000
  });
});

/**
 * THE POSITIVES-ONLY MEMBER RULE, WHICH THE FOOTER MUST APPLY OR IT EXPLAINS A LINE IT CANNOT
 * REACH.
 *
 * ⚠⚠ AND FOR `eps_nri` IT SPANS THE FORECAST COLUMNS. Eligibility is "positive in every period,
 * actuals AND consensus" (the server's `_positive_only_groups`), because a chart whose solid line
 * continues into a dotted one is ONE line: a company in the first half and out of the second steps
 * the composition exactly at the join, where neither half can show it. The drill-down carries the
 * `…e` columns in the same map as the filed years, so reading the whole row IS the joint rule.
 */
describe('buildBlend positives-only members', () => {
  const rows = () => [
    row('A', 50, { 2023: 1, 2024: 2, '2026e': 4 }),
    row('B', 50, { 2023: 1, 2024: 2, '2026e': -1 }),   // profitable, forecast to lose money
  ];

  it('takes every member when the metric is not in the set', () => {
    const b = buildBlend(resp(['2023', '2024', '2026e'], rows()), 'revenue');
    expect(b.excluded.size).toBe(0);
  });

  it('drops a company on a negative CONSENSUS even though every filed year is positive', () => {
    const r = rows();
    const b = buildBlend(resp(['2023', '2024', '2026e'], r), 'eps_nri');
    expect(b.contrib.get(r[1])).toBeUndefined();
    // ⚠ WITH A REASON. A row the footer drops without one is a blank the reader cannot account
    // for, and every cell on this row looks perfectly fine.
    expect(b.excluded.get(r[1])).toMatch(/negative figure in at least one period/);
    // ⚠ AND THE SURVIVOR CARRIES THE WHOLE LINE — if B were still in the denominator the step
    // would be right by luck here (both grew +100%) and wrong the moment they differ.
    expect(b.contrib.get(r[0])!['2024'].pp).toBeCloseTo(b.step['2024'].growthPct, 10);
  });

  it('names it as withheld BY THE RULE, apart from the rebase’s own drops', () => {
    /**
     * ⚠⚠ THE DISTINCTION THE DRILL-DOWN'S BADGE DEPENDS ON. The `NOT IN LINE` badge was removed on
     * request (2026-08-12) because it announced `_prepare`'s non-positive-BASE drop — mechanical,
     * unactionable, true of a quarter of a book. A member the METRIC'S RULE withheld is a stated
     * policy the reader asked to see. One set for each, or marking the first re-announces the
     * second and undoes that request.
     */
    // ⚠ C IS THE CASE THAT SEPARATES THEM, and it has to be built deliberately: zero clears the
    // rule (only a NEGATIVE fails it) while a series with no positive period at all has no base to
    // rebase on, so C is excluded by the rebase and by nothing else.
    const r = [...rows(), row('C', 50, { 2023: 0, 2024: 0, '2026e': 0 })];
    const b = buildBlend(resp(['2023', '2024', '2026e'], r), 'eps_nri');
    expect(b.excludedByRule.has(r[1])).toBe(true);          // negative consensus — the rule
    expect(b.excluded.has(r[1])).toBe(true);                // …and it carries a reason
    expect(b.excluded.has(r[2])).toBe(true);                // the rebase dropped C…
    expect(b.excludedByRule.has(r[2])).toBe(false);         // …and it must NOT be badged
    expect(b.excludedByRule.has(r[0])).toBe(false);
  });

  it('drops it from the FILED periods too, not only from the forecast ones', () => {
    // ⚠ THE POINT OF THE JOINT RULE: one member set for the whole line, not one per leg.
    const r = rows();
    const b = buildBlend(resp(['2023', '2024', '2026e'], r), 'eps_nri');
    expect(b.contrib.get(r[1])?.['2024']).toBeUndefined();
  });

  it('keeps a company whose consensus is positive', () => {
    const r = [row('A', 50, { 2023: 1, 2024: 2, '2026e': 4 }),
      row('B', 50, { 2023: 1, 2024: 2, '2026e': 3 })];
    const b = buildBlend(resp(['2023', '2024', '2026e'], r), 'eps_nri');
    expect(b.contrib.get(r[1])!['2024'].growthPct).toBeCloseTo(100, 10);
  });

  it('still applies to fcf_ps, whose group is just its own filed years', () => {
    const r = [row('A', 50, { 2023: 1, 2024: 2 }), row('B', 50, { 2023: 1, 2024: -2 })];
    const b = buildBlend(resp(['2023', '2024'], r), 'fcf_ps');
    expect(b.contrib.get(r[1])).toBeUndefined();
  });
});
