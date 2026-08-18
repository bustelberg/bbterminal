/**
 * The START-OF-YEAR basis a book's price return is actually built on.
 *
 * ⚠ THE `Weight` COLUMN IS TODAY'S SHARE, AND WEIGHTING A RETURN BY IT IS WRONG.
 *   A holding that rose carries a bigger share of the book today than it held while it was
 *   rising, so Σ(today's weight × return) systematically overstates. Measured on the real
 *   BUS_Offensief_Dyn snapshot: +11.19% against a book that made +5.58% — exactly double.
 *
 *   The honest weight is the holding's share of the book at the year's OPEN, and it is not a
 *   nicer approximation, it is an IDENTITY:
 *
 *       Σ ( startᵢ/Σstart × (currentᵢ − startᵢ)/startᵢ )  ≡  Σcurrent/Σstart − 1
 *
 *   which is `totalReturn` below, to the digit. That is why both come from this one function:
 *   computing the column and the total separately is how they drift apart.
 *
 * ⚠ NORMALISED OVER THE PRICED ROWS ONLY, AND THAT IS WHAT MAKES IT CLOSE. A holding with no
 *   opening value was not there when the year began — cash is exactly this, and so is anything
 *   bought since. It has real exposure today and an UNDEFINED return, so it is out of both sides
 *   of the identity. Its start weight is `null`, never 0, because a 0.00% in that column would
 *   read as "held none of the book" rather than "was not held".
 */

export type ValuedRow = {
  holding_name: string;
  start_value_eur?: number | null;
  current_value_eur?: number | null;
  /** Gross dividend received over the window (AIRS Mutaties). */
  dividend_eur?: number | null;
  /** Withholding on it — ⚠ NEGATIVE, as AIRS books it. */
  dividend_tax_eur?: number | null;
};

/**
 * What the holding is worth PLUS what it paid out — the numerator of a TOTAL return.
 *
 * ⚠ THE TAX IS ADDED, NOT SUBTRACTED, AND THAT IS NOT A TYPO. `dividend_tax_eur` is already
 * negative (AIRS books the withholding as a debit), so `current + gross + tax` IS
 * `current + net`. Writing the intuitive `- dividend_tax_eur` adds the tax back and overstates
 * every foreign holding's return by twice the withholding — silently, since the result is still
 * a plausible number.
 */
export const valueWithIncome = (r: ValuedRow): number | null =>
  (r.current_value_eur == null
    ? null
    : r.current_value_eur + (r.dividend_eur ?? 0) + (r.dividend_tax_eur ?? 0));

/** The holding's TOTAL return over the window: (value + net income) ÷ opening value − 1.
 *  Null where there is no opening value — undefined, not zero. */
export function holdingTotalReturn(r: ValuedRow): number | null {
  const s = r.start_value_eur ?? 0;
  const v = valueWithIncome(r);
  return s === 0 || v == null ? null : v / s - 1;
}

export type StartBasis<T extends ValuedRow> = {
  /** The rows both the total and the weights span: an opening value AND a current one. */
  priced: T[];
  startSum: number;
  nowSum: number;
  /** Σcurrent ÷ Σstart − 1, or null when nothing has an opening value. */
  totalReturn: number | null;
  /** This row's share of the book at the open, or null when it had no opening value. */
  weightOf: (r: ValuedRow) => number | null;
};

/**
 * One asset-class row (Stocks, Bonds, Alternatives…), computed from the HOLDINGS UNDER IT.
 *
 * ⚠ A GROUP ROW THAT DOES NOT ADD UP FROM ITS OWN ROWS IS A SECOND SOURCE OF TRUTH. The backend
 *   publishes its own per-segment figures, but it computes them over the ISIN-resolution rows,
 *   while the table groups the HOLDINGS rows merged by name — two row sets that can differ, and
 *   when they do the header disagrees with the lines beneath it and nothing on screen says why.
 *   Measured: a Stocks header reading 26 holdings / 84.02% over rows summing to 82.78%.
 *
 *   So every figure here is derived from the same columns the reader can see and add up.
 *
 * ⚠ THE SEGMENTS THEMSELVES WEIGHT TO THE BOOK. `returnPct` uses the same start-weighted
 *   definition as the total, so Σ(segment start weight × segment return) is the book's return —
 *   the identity holds one level up as well. That is only true because both come from
 *   `startBasis`, over the same priced rows.
 */
export type GroupStats = {
  holdings: number;
  /** Share of the BOOK this group was at the year's open. Null when nothing has an opening value. */
  startWeightPct: number | null;
  /** Share of the book today — Σ of the group's own AIRS weights. */
  weightPct: number | null;
  /** Σcurrent ÷ Σstart − 1 over the group's priced rows, as a percent. */
  returnPct: number | null;
  /**
   * What this segment added to the BOOK's return, in percentage points: Σ over its rows of
   * (row Start wt × row Return). Those are two columns on screen, so this is the one figure a
   * reader can check by hand — and the segment's own `returnPct` is just this ÷ `startWeightPct`,
   * which is what "renormalised within the segment" means.
   *
   * The contributions of all the segments add up to the book's return.
   */
  contributionPct: number | null;
  /** Σ Beginwaarde over ALL the group's rows — what the Beginwaarde column below adds up to.
   *  ⚠ NOT `startValueEur`, which spans the PRICED rows only because that is what the return is
   *  computed over. They differ the moment a row has an opening value but no current one, and a
   *  displayed column that does not equal the column beneath it is the bug this file exists to
   *  prevent one level down. */
  startEurAll: number;
  valueEur: number;
  /** ⚠ The group's REAL opening value over the PRICED rows. Never reconstruct it from
   *  `pricedValueEur / (1 + returnPct)` — that derives the inputs from the answer, so it "explains"
   *  a figure with itself and cannot disagree with it even when it is wrong. */
  startValueEur: number;
  pricedValueEur: number;
  /** True when the return spans less of the group than the weight does — see `valueEur` vs priced. */
  partial: boolean;
  /** Share of the group's value held through a fund wrapper. */
  etfPct: number;
  /** Dividend the group received this year, gross EUR. Null when NOT ONE row below has a journal
   *  line — a group we have not scanned must not read as one that paid nothing. */
  dividendEur: number | null;
  /** The withholding on it, negative as AIRS books it. Null on the same rule. */
  dividendTaxEur: number | null;
  /** Σ `Model percentage` — what the strategy says this group should be. Null when no row below
   *  has a model line (this book's MODEL report has not been scanned), never 0: "the model wants
   *  none of this" and "we have not asked" are different claims. */
  modelPct: number | null;
  /** Σ `Werkelijk percentage` — what the model report says the book actually holds. */
  actualPct: number | null;
};

export function groupStats<T extends ValuedRow>(
  group: T[],
  basis: StartBasis<T>,
  opts: {
    weightOfRow: (r: T) => number | null | undefined;
    isEtf: (r: T) => boolean;
    dividendOf?: (r: T) => number | null | undefined;
    dividendTaxOf?: (r: T) => number | null | undefined;
    modelOf?: (r: T) => number | null | undefined;
    actualOf?: (r: T) => number | null | undefined;
  },
): GroupStats {
  // ⚠ Null when nothing below has a figure, 0 only when something does and it sums to zero.
  // A money column reads a 0 as "paid nothing", which is a claim we cannot make about a book
  // whose journal we have not scanned.
  const sumOrNull = (f?: (r: T) => number | null | undefined) => {
    if (!f) return null;
    const vals = group.map(f).filter((v): v is number => v != null);
    return vals.length ? Math.round(vals.reduce((a, b) => a + b, 0) * 100) / 100 : null;
  };
  const sumPct = (f?: (r: T) => number | null | undefined) => {
    if (!f) return null;
    const vals = group.map(f).filter((v): v is number => v != null);
    return vals.length ? vals.reduce((a, b) => a + b, 0) : null;
  };
  const priced = group.filter((r) => basis.weightOf(r) != null);
  const gStart = priced.reduce((s, r) => s + (r.start_value_eur ?? 0), 0);
  // ⚠ TWO SUMS, AND MIXING THEM BREAKS `partial`. `gNow` is the RETURN's numerator (value +
  // income); `gNowRaw` is what the group is WORTH. Comparing a with-income sum against a plain
  // value would flag every income-bearing segment as partially priced.
  const gNow = priced.reduce((s, r) => s + (valueWithIncome(r) ?? 0), 0);
  const gNowRaw = priced.reduce((s, r) => s + (r.current_value_eur ?? 0), 0);
  const valueEur = group.reduce((s, r) => s + (r.current_value_eur ?? 0), 0);
  const weights = group.map((r) => opts.weightOfRow(r)).filter((w): w is number => w != null);
  return {
    holdings: group.length,
    startWeightPct: basis.startSum === 0 ? null : 100 * (gStart / basis.startSum),
    weightPct: weights.length ? 100 * weights.reduce((s, w) => s + w, 0) : null,
    returnPct: gStart !== 0 ? 100 * (gNow / gStart - 1) : null,
    // Σ(row start weight × row return) — literally the two visible columns, multiplied and added.
    contributionPct: basis.startSum === 0 ? null : 100 * ((gNow - gStart) / basis.startSum),
    startEurAll: group.reduce((s, r) => s + (r.start_value_eur ?? 0), 0),
    valueEur,
    startValueEur: gStart,
    pricedValueEur: gNowRaw,
    // A cent of rounding is not a partial segment; the real cases are whole holdings.
    partial: Math.abs(valueEur - gNowRaw) > 1,
    dividendEur: sumOrNull(opts.dividendOf),
    dividendTaxEur: sumOrNull(opts.dividendTaxOf),
    // ⚠ Percentages, so summed EXACTLY — `sumOrNull` rounds to cents, which is right for euros
    // and wrong here (see `aggregateGroups`, where rounding a percentage cost 0.0014pp).
    modelPct: sumPct(opts.modelOf),
    actualPct: sumPct(opts.actualOf),
    etfPct: valueEur
      ? (100 * group.filter(opts.isEtf).reduce((s, r) => s + (r.current_value_eur ?? 0), 0)) / valueEur
      : 0,
  };
}

export function startBasis<T extends ValuedRow>(rows: T[]): StartBasis<T> {
  const priced = rows.filter((r) => (r.start_value_eur ?? 0) !== 0 && r.current_value_eur != null);
  const startSum = priced.reduce((s, r) => s + (r.start_value_eur ?? 0), 0);
  // ⚠ INCOME IS IN THE NUMERATOR. The Total is a TOTAL return now, so every figure built on
  // it (the row returns, the segment returns, the contributions) is one too — and the identity
  // Sigma(start wt x return) == totalReturn still closes, because both sides moved together.
  const nowSum = priced.reduce((s, r) => s + (valueWithIncome(r) ?? 0), 0);
  const names = new Set(priced.map((r) => r.holding_name));
  return {
    priced,
    startSum,
    nowSum,
    totalReturn: startSum !== 0 ? nowSum / startSum - 1 : null,
    weightOf: (r) => (startSum !== 0 && names.has(r.holding_name)
      ? (r.start_value_eur ?? 0) / startSum
      : null),
  };
}

/**
 * The Total row: the SAME aggregation, one level up — over the segment rows rather than the
 * holdings.
 *
 * ⚠ THE POINT IS THAT THERE IS ONLY ONE RULE. Every money column is a plain sum of the column
 * above it, and the return is the start-weighted sum. The Total used to re-derive itself straight
 * from the holdings, which is a second code path that merely happened to agree — and a header
 * that agrees by coincidence starts disagreeing the day either side changes.
 *
 * ⚠ FEED IT EVERY GROUP, INCLUDING THE UNGROUPED ONE. The table renders a trailing block for
 * holdings that belong to no segment (their name is in the holdings payload but not the ISIN one),
 * and that block has no header. Aggregating only the groups that DREW a header would silently drop
 * those rows from the book's totals.
 */
export function aggregateGroups(groups: GroupStats[]): GroupStats {
  // ⚠ TWO SUMS, AND USING THE MONEY ONE ON A PERCENTAGE IS A REAL BUG. `sumMoney` rounds to
  // cents, which is right for euros and WRONG for `contributionPct`/`startWeightPct`: those feed
  // a division, so rounding them first pushes the Total's return off the holdings-level answer.
  // Measured when this was one function: 4.5300% against a true 4.5286%.
  // Null only when NO group had a figure; a real 0 survives.
  const nn = (f: (g: GroupStats) => number | null) =>
    groups.map(f).filter((x): x is number => x != null);
  const sumMoney = (f: (g: GroupStats) => number | null) => {
    const v = nn(f);
    return v.length ? Math.round(v.reduce((a, b) => a + b, 0) * 100) / 100 : null;
  };
  const sumExact = (f: (g: GroupStats) => number | null) => {
    const v = nn(f);
    return v.length ? v.reduce((a, b) => a + b, 0) : null;
  };
  const sum = (f: (g: GroupStats) => number) => groups.reduce((s, g) => s + f(g), 0);
  const startWeightPct = sumExact((g) => g.startWeightPct);
  // ⚠ The return is the START-WEIGHTED sum of the segments' returns, which is exactly the sum of
  // their contributions — each segment's own return times the share of the book it opened with.
  // Averaging the segment returns, or weighting them by today's Weight, is the same error the
  // holdings level already documents, one level up.
  const contributionPct = sumExact((g) => g.contributionPct);
  return {
    holdings: sum((g) => g.holdings),
    startWeightPct,
    weightPct: sumExact((g) => g.weightPct),
    returnPct: contributionPct == null || !startWeightPct
      ? null
      : (contributionPct / startWeightPct) * 100,
    contributionPct,
    startEurAll: sum((g) => g.startEurAll),
    valueEur: sum((g) => g.valueEur),
    startValueEur: sum((g) => g.startValueEur),
    pricedValueEur: sum((g) => g.pricedValueEur),
    partial: groups.some((g) => g.partial),
    etfPct: sum((g) => g.valueEur) ? (100 * sum((g) => g.valueEur * (g.etfPct / 100))) / sum((g) => g.valueEur) : 0,
    dividendEur: sumMoney((g) => g.dividendEur),
    dividendTaxEur: sumMoney((g) => g.dividendTaxEur),
    modelPct: sumExact((g) => g.modelPct),
    actualPct: sumExact((g) => g.actualPct),
  };
}

/** Which column's weights the segment/Total returns are weighted by. */
export type WeightBasis = 'start' | 'now' | 'model' | 'actual';

export const WEIGHT_BASES: { key: WeightBasis; label: string; note: string }[] = [
  { key: 'start', label: 'Start wt',
    note: 'the share of the book each holding opened the year with. The ONLY basis on which the weighted returns are the book’s actual return — an identity, not an approximation.' },
  { key: 'now', label: 'Weight',
    note: 'today’s share (AIRS Weging). A hypothetical: it asks what the year would have returned had you held today’s weights all along, which tilts toward whatever has already risen.' },
  { key: 'model', label: 'Model wt',
    note: 'the strategy’s target weights. A hypothetical: what the model would have returned, rather than what the book did.' },
  { key: 'actual', label: 'Werkelijk',
    note: 'the Model report’s own view of the actual weights. Near-identical to Weight, from a different report on a different date.' },
];

export type WeightedReturn = {
  /** Σ(w × r) ÷ Σw over the rows that have BOTH, as a percent. Null when nothing qualifies. */
  pct: number | null;
  /** Σw over exactly those rows — the denominator, and the share this group carries upward. */
  weightSum: number;
};

/**
 * A weighted return on ANY weight column.
 *
 * ⚠ RENORMALISED, AND THAT IS NOT A REFINEMENT — IT IS WHAT MAKES THE NUMBER MEAN ANYTHING.
 *   None of these columns sums to 100% over the rows that HAVE a return. Measured on
 *   BUS_Neutraal_Dyn: Model wt sums to 98.70% (the model names things the book does not hold),
 *   Weight to 99.99%, and every basis loses the cash line and anything bought during the year,
 *   which carry weight but have no return at all. Σ(w × r) taken raw therefore understates by
 *   whatever the weights happen to miss — silently, since the result is still a plausible percent.
 *
 * ⚠ ONLY `start` IS THE BOOK'S ACTUAL RETURN. On that basis Σ(w × r) is an IDENTITY with
 *   Σcurrent ÷ Σstart − 1. Every other basis answers a DIFFERENT question — "what would this have
 *   returned weighted like that?" — and must be labelled as the hypothetical it is. Weighting by
 *   today's share reads +11.19% on a book that made +5.58%, because a holding that doubled now
 *   carries twice the share it held while it was doubling.
 */
export function weightedReturn<T>(
  rows: T[],
  weightOf: (r: T) => number | null | undefined,
  returnOf: (r: T) => number | null,
): WeightedReturn {
  let wr = 0;
  let w = 0;
  for (const r of rows) {
    const wi = weightOf(r);
    const ri = returnOf(r);
    if (wi == null || ri == null) continue;   // a weight with no return cannot be weighted
    wr += wi * ri;
    w += wi;
  }
  return { pct: w === 0 ? null : (100 * wr) / w, weightSum: w };
}

/** The same figure one level up: the groups' returns, weighted by the share each carries. */
export function combineWeighted(parts: WeightedReturn[]): WeightedReturn {
  const usable = parts.filter((p) => p.pct != null && p.weightSum > 0);
  const w = usable.reduce((s, p) => s + p.weightSum, 0);
  return { pct: w === 0 ? null : usable.reduce((s, p) => s + p.weightSum * p.pct!, 0) / w, weightSum: w };
}
