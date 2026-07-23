import { describe, expect, it } from 'vitest';
import { groupStats, startBasis, type ValuedRow } from './startWeights';

/** Real rows from BUS_Offensief_Dyn (as of 2026-07-22), six largest plus its cash line. */
const ROWS: (ValuedRow & { weight: number })[] = [
  { holding_name: 'ASML Holding', start_value_eur: 57126.80, current_value_eur: 98555.20, weight: 0.08 },
  { holding_name: 'Arista Networks', start_value_eur: 48062.92, current_value_eur: 66066.77, weight: 0.05 },
  { holding_name: 'Nvidia', start_value_eur: 54717.35, current_value_eur: 64502.82, weight: 0.05 },
  { holding_name: 'iShares MSCI Wld Mom Fact ETF EUR', start_value_eur: 59554.30, current_value_eur: 62622.60, weight: 0.05 },
  { holding_name: 'Alphabet - C', start_value_eur: 56285.38, current_value_eur: 62339.83, weight: 0.05 },
  { holding_name: 'Berkshire Hathaway - B', start_value_eur: 60317.50, current_value_eur: 61345.35, weight: 0.05 },
  // ⚠ No opening value: real exposure today, undefined return. Cash is always this.
  { holding_name: 'Effectenrekening', start_value_eur: 0, current_value_eur: 32936.16, weight: 0.03 },
];

/** The Return column: null with no opening value, because there is no return to state — not 0.
 *  (`?? 1` would not save this: the cash line's opening value is a real 0, and 0/0 is NaN.) */
const ret = (r: ValuedRow): number | null => {
  const s = r.start_value_eur ?? 0;
  if (s === 0 || r.current_value_eur == null) return null;
  return ((r.current_value_eur) - s) / Math.abs(s);
};
const weightedSum = (rows: ValuedRow[], w: (r: ValuedRow) => number | null) =>
  rows.reduce((s, r) => {
    const x = ret(r); const y = w(r);
    return x == null || y == null ? s : s + y * x;
  }, 0);

describe('startBasis', () => {
  it('weighting each return by its start weight reproduces the total EXACTLY', () => {
    // The whole reason the column exists. Not "close to" — the same number.
    const b = startBasis(ROWS);
    const weighted = weightedSum(ROWS, b.weightOf);
    expect(weighted).toBeCloseTo(b.totalReturn!, 12);
  });

  it("weighting by today's share does not, and overstates", () => {
    // ⚠ THE BUG A READER HITS WITH ONLY ONE WEIGHT COLUMN ON SCREEN. A holding that rose carries
    // a bigger share of the book TODAY than it held while it was rising, so today's weights tilt
    // toward the winners — here ASML, up +72.5%, at 17.0% of the book at the open and 23.7% now.
    //
    // Both sides are renormalised over the same priced rows: these seven are an EXCERPT, so their
    // raw AIRS weights sum to 0.36 and an un-normalised comparison would just be measuring that.
    // On the whole book it needs no help — measured +11.19% against a true +5.58%.
    const b = startBasis(ROWS);
    const nowSum = b.priced.reduce((s, r) => s + (r.current_value_eur ?? 0), 0);
    const byToday = weightedSum(ROWS, (r) =>
      (b.weightOf(r) == null ? null : (r.current_value_eur ?? 0) / nowSum));
    expect(byToday).toBeGreaterThan(b.totalReturn!);
  });

  it('the start weights sum to 1 over the rows the total spans', () => {
    const b = startBasis(ROWS);
    expect(b.priced.reduce((s, r) => s + (b.weightOf(r) ?? 0), 0)).toBeCloseTo(1, 12);
  });

  it('a holding with no opening value gets null, never 0', () => {
    // ⚠ A 0.00% would read as "held none of the book"; the truth is "was not held yet".
    const b = startBasis(ROWS);
    expect(b.weightOf({ holding_name: 'Effectenrekening', start_value_eur: 0, current_value_eur: 32936.16 }))
      .toBeNull();
    expect(b.priced.map((r) => r.holding_name)).not.toContain('Effectenrekening');
  });

  it('excludes it from the total too, so the identity still closes', () => {
    // Counting cash in would report its entire balance as gain.
    const b = startBasis(ROWS);
    expect(b.nowSum).toBeCloseTo(415432.57, 2);      // the cash 32,936.16 is NOT in here
    expect(b.startSum).toBeCloseTo(336064.25, 2);
    expect(b.totalReturn).toBeCloseTo(0.23617, 4);
  });

  it('a holding priced but not yet marked is excluded from both sides', () => {
    const b = startBasis([...ROWS, { holding_name: 'Unmarked', start_value_eur: 1000, current_value_eur: null }]);
    expect(b.weightOf({ holding_name: 'Unmarked', start_value_eur: 1000 })).toBeNull();
    const weighted = weightedSum(b.priced, b.weightOf);
    expect(weighted).toBeCloseTo(b.totalReturn!, 12);
  });

  it('a book with no opening values at all has no return and no weights', () => {
    const b = startBasis([{ holding_name: 'Cash', start_value_eur: 0, current_value_eur: 10 }]);
    expect(b.totalReturn).toBeNull();
    expect(b.weightOf({ holding_name: 'Cash', start_value_eur: 0 })).toBeNull();
  });
});

describe('groupStats', () => {
  // Two segments over the same real rows: the four equities, then the ETF + the cash line.
  const EQ = ROWS.slice(0, 3).concat(ROWS[4], ROWS[5]);
  const ETF = [ROWS[3], ROWS[6]];
  const basis = startBasis(ROWS);
  const opts = { weightOfRow: (r: (typeof ROWS)[number]) => r.weight, isEtf: () => false };
  const eq = groupStats(EQ, basis, opts);
  const etf = groupStats(ETF, basis, { ...opts, isEtf: (r) => r.holding_name.includes('iShares') });

  it('counts the rows listed under it, not a separate figure', () => {
    expect(eq.holdings).toBe(5);
    expect(etf.holdings).toBe(2);           // incl. the cash line, which is real exposure
  });

  it('the weights are the columns below it, added up', () => {
    expect(eq.startWeightPct! + etf.startWeightPct!).toBeCloseTo(100, 10);
    expect(eq.weightPct).toBeCloseTo(100 * EQ.reduce((s, r) => s + r.weight, 0), 10);
  });

  it('THE SEGMENTS WEIGHT TO THE BOOK — the identity holds one level up too', () => {
    // ⚠ Only true because both come from the same `startBasis` over the same priced rows. This is
    // what makes the group rows a decomposition of the Total rather than figures beside it.
    const weighted = [eq, etf].reduce(
      (s, g) => s + (g.startWeightPct! / 100) * (g.returnPct! / 100), 0);
    expect(weighted).toBeCloseTo(basis.totalReturn!, 12);
  });

  it('a row with no opening value counts in the weight but not in the return', () => {
    // ⚠ Cash is exactly this. Putting it in Σcurrent/Σstart would report its whole balance as gain.
    expect(etf.pricedValueEur).toBeCloseTo(62622.60, 2);        // the ETF only
    expect(etf.valueEur).toBeCloseTo(62622.60 + 32936.16, 2);   // + the cash
    expect(etf.partial).toBe(true);
    expect(eq.partial).toBe(false);
  });

  it('the ETF share is measured over the whole segment value', () => {
    expect(etf.etfPct).toBeCloseTo((100 * 62622.60) / (62622.60 + 32936.16), 6);
    expect(eq.etfPct).toBe(0);
  });

  it('a segment with nothing priced has no return, and says so with null', () => {
    const g = groupStats([ROWS[6]], basis, opts);
    expect(g.returnPct).toBeNull();
    expect(g.startWeightPct).toBe(0);      // it held none of the book at the open — a real 0
    expect(g.weightPct).toBeCloseTo(3, 10);
  });
});

describe('groupStats: the segment return explains itself without using itself', () => {
  const EQ = ROWS.slice(0, 3).concat(ROWS[4], ROWS[5]);
  const ETF = [ROWS[3], ROWS[6]];
  const basis = startBasis(ROWS);
  const opts = { weightOfRow: (r: (typeof ROWS)[number]) => r.weight, isEtf: () => false };

  it('contributionPct IS Σ(row Start wt × row Return) — the two visible columns', () => {
    for (const g of [EQ, ETF]) {
      const s = groupStats(g, basis, opts);
      expect(s.contributionPct! / 100).toBeCloseTo(weightedSum(g, basis.weightOf), 12);
    }
  });

  it('the segment return is that contribution ÷ the segment start weight', () => {
    // ⚠ This is what "renormalised within the segment" means, and it is the whole formula.
    const s = groupStats(EQ, basis, opts);
    expect(s.contributionPct! / s.startWeightPct!).toBeCloseTo(s.returnPct! / 100, 12);
  });

  it('the contributions of the segments add up to the book return', () => {
    const parts = [EQ, ETF].map((g) => groupStats(g, basis, opts).contributionPct!);
    expect(parts.reduce((a, b) => a + b, 0) / 100).toBeCloseTo(basis.totalReturn!, 12);
  });

  it('startValueEur is summed from the rows, NOT reconstructed from the return', () => {
    // ⚠ `pricedValueEur / (1 + returnPct)` derives the input from the output, so it agrees with
    // the answer BY CONSTRUCTION and could never contradict a wrong one. On the real Stocks row
    // it happened to land on the true €945,712 — which is the danger, not the reassurance: a
    // figure that is right by luck and cannot be wrong is not a check.
    const s = groupStats(EQ, basis, opts);
    expect(s.startValueEur).toBeCloseTo(
      EQ.reduce((t, r) => t + (r.start_value_eur ?? 0), 0), 10);
    expect(s.pricedValueEur / s.startValueEur - 1).toBeCloseTo(s.returnPct! / 100, 12);
  });
});
