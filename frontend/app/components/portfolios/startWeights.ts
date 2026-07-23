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
};

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
 * One asset-class row (Stocks, Stock ETF, Bonds…), computed from the HOLDINGS UNDER IT.
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
  valueEur: number;
  /** ⚠ The group's REAL opening value, summed from the rows. Never reconstruct it from
   *  `pricedValueEur / (1 + returnPct)` — that derives the inputs from the answer, so it "explains"
   *  a figure with itself and cannot disagree with it even when it is wrong. */
  startValueEur: number;
  pricedValueEur: number;
  /** True when the return spans less of the group than the weight does — see `valueEur` vs priced. */
  partial: boolean;
  /** Share of the group's value held through a fund wrapper. */
  etfPct: number;
};

export function groupStats<T extends ValuedRow>(
  group: T[],
  basis: StartBasis<T>,
  opts: { weightOfRow: (r: T) => number | null | undefined; isEtf: (r: T) => boolean },
): GroupStats {
  const priced = group.filter((r) => basis.weightOf(r) != null);
  const gStart = priced.reduce((s, r) => s + (r.start_value_eur ?? 0), 0);
  const gNow = priced.reduce((s, r) => s + (r.current_value_eur ?? 0), 0);
  const valueEur = group.reduce((s, r) => s + (r.current_value_eur ?? 0), 0);
  const weights = group.map((r) => opts.weightOfRow(r)).filter((w): w is number => w != null);
  return {
    holdings: group.length,
    startWeightPct: basis.startSum === 0 ? null : 100 * (gStart / basis.startSum),
    weightPct: weights.length ? 100 * weights.reduce((s, w) => s + w, 0) : null,
    returnPct: gStart !== 0 ? 100 * (gNow / gStart - 1) : null,
    // Σ(row start weight × row return) — literally the two visible columns, multiplied and added.
    contributionPct: basis.startSum === 0 ? null : 100 * ((gNow - gStart) / basis.startSum),
    valueEur,
    startValueEur: gStart,
    pricedValueEur: gNow,
    // A cent of rounding is not a partial segment; the real cases are whole holdings.
    partial: Math.abs(valueEur - gNow) > 1,
    etfPct: valueEur
      ? (100 * group.filter(opts.isEtf).reduce((s, r) => s + (r.current_value_eur ?? 0), 0)) / valueEur
      : 0,
  };
}

export function startBasis<T extends ValuedRow>(rows: T[]): StartBasis<T> {
  const priced = rows.filter((r) => (r.start_value_eur ?? 0) !== 0 && r.current_value_eur != null);
  const startSum = priced.reduce((s, r) => s + (r.start_value_eur ?? 0), 0);
  const nowSum = priced.reduce((s, r) => s + (r.current_value_eur ?? 0), 0);
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
