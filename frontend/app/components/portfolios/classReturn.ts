/**
 * A CLASS's return in the Analyse modal's Holdings table — the rows under it, aggregated.
 *
 *  It is the opening-value-weighted aggregate of the SAME `own_return_pct` values printed below:
 *
 *      Σ(Beginwaardeᵢ × AIRS returnᵢ) ÷ Σ Beginwaardeᵢ
 *
 *  The old `Σ Result ÷ Σ Beginwaarde` mixed two different measures. Result includes realised
 * sales and the full journal dividend; AIRS return deliberately excludes realised sales and
 * aligns each dividend to the shares that remain. Consumer Defensive exposed it exactly:
 * L'Oreal, Nestle and P&G were individually right, while their subtotal read 3.28% instead of the
 * weighted 2.74%. Sectors without trims happened to agree, which hid the inconsistent numerator.
 *
 *  A row without an opening value is out of both sides. It was not held when the year opened (or
 * is a cash line), so it has no share of the class's starting money and its result cannot be
 * expressed as a rate on it. `coveredPct` says how much eligible opening value carries an AIRS
 * return, so a pricing gap is visible rather than silently renormalised.
 */

export type ClassReturnRow = {
  /** AIRS's `Beginwaarde`, restated to today's quantity. Null where the row cannot be valued. */
  start_value_eur?: number | null;
  /** The same AIRS return rendered on the position row, in percent. */
  own_return_pct?: number | null;
};

export type ClassReturn = {
  /** The class's return, in %. Null when nothing in it has an opening value — a dash, never a
   *  0.00%, because "no starting money to measure against" and "went nowhere" differ. */
  pct: number | null;
  /** Rows that spoke for it, and rows supplied by the caller. */
  legs: number;
  rows: number;
  missing: number;
  /** Share of eligible opening value carrying an AIRS return, 0–100. */
  coveredPct: number;
  /** Σ(Beginwaarde × AIRS return), so the card can print the division. */
  returnEur: number | null;
  startEur: number | null;
};

const EMPTY: ClassReturn = { pct: null, legs: 0, rows: 0, missing: 0, coveredPct: 0,
  returnEur: null, startEur: null };

/**
 * Opening-value-weighted AIRS return over rows carrying both operands.
 */
export function classWeightedReturn(
  rows: readonly ClassReturnRow[],
  /**
   *  Cash returns 0%, AND THAT IS AN ANSWER, NOT A MISSING ONE. AIRS books no `Beginwaarde` for
   * a cash line, so the rule below finds nothing to divide by and would print a dash — which says
   * "we could not work this out" about the one asset whose return is certain. It earned nothing;
   * a euro is always worth a euro.
   *
   *  And its drag is a fact. This repo prices cash at 0% rather than skipping it everywhere else
   * (`portfolio_math.make_cash_holding`) for the reason recorded there: dropping it scales a
   * 20%-cash portfolio's return up by 25%. A dash invites exactly that — treating cash as an
   * unknown to be ignored — where a 0% states the drag.
   */
  zeroWhenNoOpening = false,
): ClassReturn {
  const eligible = rows.filter((r) => r.start_value_eur != null && r.start_value_eur > 0);
  const priced = eligible.filter((r) => r.own_return_pct != null);
  const totalStart = eligible.reduce((s, r) => s + r.start_value_eur!, 0);
  const startEur = priced.reduce((s, r) => s + r.start_value_eur!, 0);
  if (!priced.length || startEur <= 0) {
    // Cash's instrument return is defined as zero even though AIRS supplies no Beginwaarde. Any
    // credited interest remains visible in the separate Result column.
    if (zeroWhenNoOpening) {
      return { ...EMPTY, pct: 0, rows: rows.length, missing: eligible.length, coveredPct: 100,
        returnEur: 0, startEur: null };
    }
    return { ...EMPTY, rows: rows.length, missing: eligible.length };
  }
  const returnEur = priced.reduce(
    (s, r) => s + r.start_value_eur! * r.own_return_pct! / 100, 0);
  return {
    pct: (returnEur / startEur) * 100,
    legs: priced.length,
    rows: rows.length,
    missing: eligible.length - priced.length,
    coveredPct: totalStart > 0 ? startEur / totalStart * 100 : 100,
    returnEur,
    startEur,
  };
}
