import { describe, expect, it } from 'vitest';
import {
  aggregateGroups, combineWeighted, groupStats, holdingTotalReturn, startBasis, valueWithIncome,
  weightedReturn, type ValuedRow,
} from './startWeights';

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

describe('groupStats: dividend columns', () => {
  const basis = startBasis(ROWS);
  const base = { weightOfRow: (r: (typeof ROWS)[number]) => r.weight, isEtf: () => false };
  // Only ASML has a journal line; the rest of the group has none.
  const div = (r: ValuedRow) => (r.holding_name === 'ASML Holding' ? 167.70 : null);
  const tax = (r: ValuedRow) => (r.holding_name === 'ASML Holding' ? -25.16 : null);

  it('sums only the rows that have a figure', () => {
    const g = groupStats(ROWS, basis, { ...base, dividendOf: div, dividendTaxOf: tax });
    expect(g.dividendEur).toBeCloseTo(167.70, 2);
    expect(g.dividendTaxEur).toBeCloseTo(-25.16, 2);
  });

  it('is NULL when no row below has a figure, never 0', () => {
    // ⚠ A money column reads 0 as "this group paid nothing". For a book whose Mutaties journal
    // has not been scanned that is a claim we cannot make — the honest answer is a blank.
    const g = groupStats(ROWS, basis, { ...base, dividendOf: () => null, dividendTaxOf: () => null });
    expect(g.dividendEur).toBeNull();
    expect(g.dividendTaxEur).toBeNull();
  });

  it('is 0 when a row HAS a figure and it is zero', () => {
    // A fund that genuinely withheld no tax (a Dutch or bond ETF line) is a real 0, not a blank.
    const g = groupStats(ROWS, basis, { ...base, dividendTaxOf: () => 0 });
    expect(g.dividendTaxEur).toBe(0);
  });

  it('omitting the accessors leaves both null rather than inventing a total', () => {
    const g = groupStats(ROWS, basis, base);
    expect(g.dividendEur).toBeNull();
    expect(g.dividendTaxEur).toBeNull();
  });
});

describe('total return: income in the numerator', () => {
  // Microsoft, real figures: gross 85.15 dividend, -12.77 withholding, so 72.38 net.
  const MSFT = {
    holding_name: 'Microsoft', start_value_eur: 10_000, current_value_eur: 11_000,
    dividend_eur: 85.15, dividend_tax_eur: -12.77,
  };

  it('is (value + NET dividend) / start - 1', () => {
    expect(holdingTotalReturn(MSFT)).toBeCloseTo((11_000 + 72.38) / 10_000 - 1, 12);
  });

  it('⚠ ADDS the tax, because the tax is already negative', () => {
    // Subtracting it — the intuitive reading of "value + dividend - tax" — adds the withholding
    // BACK, overstating by twice the tax. Silently: the result is still a plausible number.
    const wrong = (11_000 + 85.15 - -12.77) / 10_000 - 1;
    expect(holdingTotalReturn(MSFT)).toBeLessThan(wrong);
    expect(wrong - holdingTotalReturn(MSFT)!).toBeCloseTo((2 * 12.77) / 10_000, 12);
  });

  it('a holding with no income returns exactly the price return', () => {
    const plain = { holding_name: 'X', start_value_eur: 100, current_value_eur: 110 };
    expect(holdingTotalReturn(plain)).toBeCloseTo(0.1, 12);
    expect(valueWithIncome(plain)).toBe(110);
  });

  it('no opening value is still undefined, not zero', () => {
    expect(holdingTotalReturn({ holding_name: 'Cash', start_value_eur: 0, current_value_eur: 5 }))
      .toBeNull();
  });

  it('the identity STILL closes with income in it', () => {
    // ⚠ The whole point: the Total became a total return, so every figure built on it did too,
    // and Σ(start wt × return) must still equal it exactly.
    const rows = [MSFT, { holding_name: 'B', start_value_eur: 5_000, current_value_eur: 5_400,
                          dividend_eur: 200, dividend_tax_eur: -30 }];
    const b = startBasis(rows);
    const weighted = rows.reduce((s, r) => s + b.weightOf(r)! * holdingTotalReturn(r)!, 0);
    expect(weighted).toBeCloseTo(b.totalReturn!, 12);
  });

  it('a segment with income is not flagged as partially priced', () => {
    // ⚠ `partial` compares VALUE against VALUE. Comparing it against value+income would flag
    // every income-bearing segment, and the * on the return would stop meaning anything.
    const rows = [MSFT];
    const g = groupStats(rows, startBasis(rows),
      { weightOfRow: () => 1, isEtf: () => false });
    expect(g.partial).toBe(false);
    expect(g.pricedValueEur).toBe(11_000);          // the VALUE, without the income
    expect(g.returnPct!).toBeCloseTo(100 * ((11_000 + 72.38) / 10_000 - 1), 10);
  });
});

describe('aggregateGroups: the Total is the segment rows, summed', () => {
  // Three groups over one book, with income on one of them.
  const ALL = [
    { holding_name: 'ASML', start_value_eur: 10_000, current_value_eur: 12_000, dividend_eur: 100, dividend_tax_eur: -15 },
    { holding_name: 'Nvidia', start_value_eur: 20_000, current_value_eur: 19_000 },
    { holding_name: 'BondETF', start_value_eur: 5_000, current_value_eur: 5_200, dividend_eur: 300, dividend_tax_eur: 0 },
    { holding_name: 'Cash', start_value_eur: 0, current_value_eur: 4_000 },
  ];
  const basis = startBasis(ALL);
  const opts = { weightOfRow: () => 0.25, isEtf: () => false,
    dividendOf: (r: (typeof ALL)[number]) => r.dividend_eur,
    dividendTaxOf: (r: (typeof ALL)[number]) => r.dividend_tax_eur };
  const eq = groupStats([ALL[0], ALL[1]], basis, opts);
  const bond = groupStats([ALL[2]], basis, opts);
  const cash = groupStats([ALL[3]], basis, opts);
  const total = aggregateGroups([eq, bond, cash]);

  it('every money column is the sum of the segment rows', () => {
    expect(total.startEurAll).toBeCloseTo(35_000, 6);
    expect(total.valueEur).toBeCloseTo(40_200, 6);
    expect(total.dividendEur).toBeCloseTo(400, 2);
    expect(total.dividendTaxEur).toBeCloseTo(-15, 2);
    expect(total.holdings).toBe(4);
  });

  it('the start weights sum to 100%', () => {
    expect(total.startWeightPct).toBeCloseTo(100, 6);
  });

  it('the return is the START-WEIGHTED sum of the segment returns', () => {
    const byHand = [eq, bond, cash].reduce(
      (s, g) => s + (g.startWeightPct! / 100) * ((g.returnPct ?? 0) / 100), 0);
    expect(total.returnPct! / 100).toBeCloseTo(byHand, 12);
  });

  it('⚠ and NOT the plain average of them', () => {
    // The segments are wildly different sizes; averaging their returns is a different number.
    const mean = [eq, bond].reduce((s, g) => s + g.returnPct!, 0) / 2;
    expect(total.returnPct).not.toBeCloseTo(mean, 4);
  });

  it('aggregating the segments equals aggregating the holdings directly', () => {
    // ⚠ THE POINT OF THE REFACTOR. These were two code paths that happened to agree; now the
    // Total is defined as the first, and this asserts it still lands on the second.
    expect(total.returnPct! / 100).toBeCloseTo(basis.totalReturn!, 12);
    expect(total.valueEur).toBeCloseTo(
      ALL.reduce((s, r) => s + (r.current_value_eur ?? 0), 0), 6);
  });

  it('an ungrouped block must be included or the book loses those rows', () => {
    // The table renders a trailing block with no header for holdings in no segment. Aggregating
    // only the header-drawing groups would silently drop them.
    const without = aggregateGroups([eq, bond]);
    expect(without.valueEur).toBeLessThan(total.valueEur);
    expect(without.holdings).toBe(3);
  });

  it('a column no segment has stays null rather than becoming 0', () => {
    const bare = { weightOfRow: () => 0.25, isEtf: () => false };
    const t = aggregateGroups([groupStats([ALL[0]], basis, bare), groupStats([ALL[1]], basis, bare)]);
    expect(t.dividendEur).toBeNull();
    expect(t.dividendTaxEur).toBeNull();
  });
});

describe('the model columns', () => {
  const ROWS2 = [
    { holding_name: 'ASML', start_value_eur: 10_000, current_value_eur: 12_000 },
    { holding_name: 'Nvidia', start_value_eur: 20_000, current_value_eur: 19_000 },
    { holding_name: 'NotInModel', start_value_eur: 5_000, current_value_eur: 5_000 },
  ];
  const basis = startBasis(ROWS2);
  const model: Record<string, number> = { ASML: 3.25, Nvidia: 2.93 };
  const opts = {
    weightOfRow: () => 0.3, isEtf: () => false,
    modelOf: (r: (typeof ROWS2)[number]) => model[r.holding_name] ?? null,
    actualOf: (r: (typeof ROWS2)[number]) => (model[r.holding_name] ?? null),
  };

  it('sums only the rows the model names', () => {
    const g = groupStats(ROWS2, basis, opts);
    expect(g.modelPct).toBeCloseTo(6.18, 10);
    expect(g.actualPct).toBeCloseTo(6.18, 10);
  });

  it('is NULL when the model names nothing here, never 0', () => {
    // ⚠ "the strategy wants none of this" and "this book's MODEL report has not been scanned"
    // are different claims, and only one of them is safe to make.
    const g = groupStats(ROWS2, basis, { weightOfRow: () => 0.3, isEtf: () => false });
    expect(g.modelPct).toBeNull();
    expect(g.actualPct).toBeNull();
  });

  it('carries up to the Total exactly, not rounded to cents', () => {
    // ⚠ These are percentages. `sumMoney` would round 3.25 + 2.93 to 2dp per level, and the
    // Total is a sum of sums — the error compounds. Measured on the return, that cost 0.0014pp.
    const a = groupStats([ROWS2[0]], basis, opts);
    const b = groupStats([ROWS2[1]], basis, opts);
    const t = aggregateGroups([a, b]);
    expect(t.modelPct).toBeCloseTo(6.18, 12);
    expect(t.modelPct).toBe(a.modelPct! + b.modelPct!);
  });

  it('a group the model names none of contributes nothing rather than null-ing the total', () => {
    const named = groupStats([ROWS2[0]], basis, opts);
    const unnamed = groupStats([ROWS2[2]], basis, opts);
    expect(unnamed.modelPct).toBeNull();
    expect(aggregateGroups([named, unnamed]).modelPct).toBeCloseTo(3.25, 12);
  });
});

describe('weightedReturn: choosing the weight basis', () => {
  // Two priced holdings plus a cash line that carries weight and has NO return.
  const ROWS3 = [
    { holding_name: 'Winner', start_value_eur: 10_000, current_value_eur: 20_000 },  // +100%
    { holding_name: 'Flat', start_value_eur: 30_000, current_value_eur: 30_000 },    //    0%
    { holding_name: 'Cash', start_value_eur: 0, current_value_eur: 5_000 },          //  none
  ];
  const basis = startBasis(ROWS3);
  const ret = (r: (typeof ROWS3)[number]) => holdingTotalReturn(r);
  // Today's shares: the winner is now 2/3 of the priced book, against 1/4 at the open.
  const todayW: Record<string, number> = { Winner: 0.4, Flat: 0.6, Cash: 0.1 };

  it('on the start basis it reproduces the book return exactly', () => {
    const w = weightedReturn(ROWS3, basis.weightOf, ret);
    expect(w.pct! / 100).toBeCloseTo(basis.totalReturn!, 12);
  });

  it("⚠ another basis is a DIFFERENT number, and higher when it tilts to the winner", () => {
    const now = weightedReturn(ROWS3, (r) => todayW[r.holding_name], ret);
    expect(now.pct).toBeGreaterThan(weightedReturn(ROWS3, basis.weightOf, ret).pct!);
  });

  it('⚠ RENORMALISES — a raw Σ(w × r) understates by whatever the weights miss', () => {
    // The cash line carries 0.1 of weight and no return, so Σw over usable rows is 1.0, not 1.1.
    const w = weightedReturn(ROWS3, (r) => todayW[r.holding_name], ret);
    expect(w.weightSum).toBeCloseTo(1.0, 12);           // cash excluded from the denominator
    const raw = 100 * (0.4 * 1 + 0.6 * 0);              // what an un-renormalised sum would give
    expect(w.pct).toBeCloseTo(raw / 1.0, 10);
    // With weights summing to 0.9 instead, the raw sum would be 10% too small.
    const short = weightedReturn(ROWS3, (r) => todayW[r.holding_name] * 0.9, ret);
    expect(short.pct).toBeCloseTo(w.pct!, 10);          // renormalising makes the scale irrelevant
  });

  it('a row with weight but no return never enters either side', () => {
    const w = weightedReturn(ROWS3, () => 1, ret);
    expect(w.weightSum).toBe(2);                        // the two priced rows, not three
  });

  it('no usable row means no number, not zero', () => {
    expect(weightedReturn([ROWS3[2]], () => 1, ret).pct).toBeNull();
  });

  it('combining groups equals computing over all the rows at once', () => {
    const wOf = (r: (typeof ROWS3)[number]) => todayW[r.holding_name];
    const whole = weightedReturn(ROWS3, wOf, ret);
    const parts = combineWeighted([
      weightedReturn([ROWS3[0]], wOf, ret),
      weightedReturn([ROWS3[1], ROWS3[2]], wOf, ret),
    ]);
    expect(parts.pct).toBeCloseTo(whole.pct!, 12);
  });

  it('a group with no usable row does not drag the combination to null', () => {
    const wOf = (r: (typeof ROWS3)[number]) => todayW[r.holding_name];
    const parts = combineWeighted([
      weightedReturn([ROWS3[0], ROWS3[1]], wOf, ret),
      weightedReturn([ROWS3[2]], wOf, ret),   // cash only — pct is null
    ]);
    expect(parts.pct).not.toBeNull();
  });
});
