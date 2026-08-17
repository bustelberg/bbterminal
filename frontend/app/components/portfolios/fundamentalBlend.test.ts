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
      expect(c.sharePct * c.growthPct / 100).toBeCloseTo(c.pp, 10);
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

  it('never contributes for a member the rebase excluded (the AMD case)', () => {
    const rows = [
      row('A', 50, { 2023: 100, 2024: 120 }),
      row('B', 50, { 2023: -5, 2024: 10 }),     // negative first period ⇒ dropped from the line
    ];
    const b = buildBlend(resp(['2023', '2024'], rows));

    expect(b.excluded.has(rows[1])).toBe(true);
    // It contributes nothing anywhere — not 0.00pp, which would read as "held, didn't move".
    expect(b.contrib.get(rows[1])).toBeUndefined();
    // And it is not in the denominator either: A carries the whole move.
    expect(b.contrib.get(rows[0])!['2024'].pp).toBeCloseTo(b.step['2024'].growthPct, 10);
  });
});
