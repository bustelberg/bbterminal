/**
 * A CLASS's return in the Analyse modal's Holdings table — the rows under it, aggregated.
 *
 * ⚠⚠ IT IS THE `Result` COLUMN OVER THE CLASS'S OPENING VALUE, AND NOTHING ELSE. It used to be
 * Σ(start weight × the instrument's own return), which was wrong in TWO independent ways at once
 * and produced a figure a reader could not reconcile with anything on screen. Measured on
 * AITopSelectie 2026-08-05, where the class is 99.91% of the book and the gap was 0.93pp:
 *
 *     43.532%   Σ(weight × own return)   -- what this used to show
 *     44.159%   result ÷ opening value   -- +0.627pp: the realised leg was MISSING
 *     44.462%   result ÷ beginvermogen   -- +0.303pp: a different denominator again
 *
 *   * THE REALISED LEG WAS ABSENT. `own_return_pct` is what the still-held shares did; a position
 *     trimmed during the year banked EUR 6,307 that appeared in the Result column beside it and
 *     in no percentage anywhere. Using the Result column as the numerator fixes that by
 *     construction — the two columns can no longer describe different money.
 *   * AND IT WAS A WEIGHTED AVERAGE OF PER-ROW RATES, which is not the class's return unless every
 *     row shares one denominator. They do not: `own_return_pct` is each instrument's own rate.
 *
 * ⚠ THE DENOMINATOR IS THE CLASS'S OWN `Beginwaarde`, NOT THE BOOK'S OPENING CAPITAL. That is
 * deliberate and it is why this still will not equal the book's return on the total row:
 *   * `Beginwaarde` is RESTATED to today's quantity, so buying more during the year inflates it
 *     (AITopSelectie's rows claim EUR 1,006,881 against a book that opened at EUR 1,000,000);
 *   * a position sold out entirely has no row, so its opening value is missing from the sum
 *     (BUS_Offensief_Dyn's rows claim EUR 1,142,384 against EUR 1,197,811).
 * Those two pull in OPPOSITE directions and neither is an error — restatement is what stops a
 * purchase reading as a gain. The column that ties to the book is `Contribution`, which is on the
 * book's own capital; this one answers "what did this class do", which is a different question and
 * is why both are shown.
 *
 * ⚠ A ROW WITHOUT AN OPENING VALUE IS OUT OF BOTH SIDES. It was not held when the year opened (or
 * is a cash line), so it has no share of the class's starting money and its result cannot be
 * expressed as a rate on it. `coveredPct` says how much of the class's RESULT the figure speaks
 * for, so weight silently leaving the ratio is visible rather than absorbed.
 */

export type ClassReturnRow = {
  /** AIRS's `Beginwaarde`, restated to today's quantity. Null where the row cannot be valued. */
  start_value_eur?: number | null;
  /** unrealised + realised + income, in EUR — the Result column. */
  result_eur?: number | null;
};

export type ClassReturn = {
  /** The class's return, in %. Null when nothing in it has an opening value — a dash, never a
   *  0.00%, because "no starting money to measure against" and "went nowhere" differ. */
  pct: number | null;
  /** Rows that spoke for it, and rows carrying a result at all. */
  legs: number;
  rows: number;
  /** Share of the class's RESULT that the ratio covers, 0–100. Below 100 means some of what the
   *  class made came from rows with no opening value, so the rate understates the money. */
  coveredPct: number;
  /** The euro figures behind it, so the card can print the division rather than assert it. */
  resultEur: number | null;
  startEur: number | null;
};

const EMPTY: ClassReturn = { pct: null, legs: 0, rows: 0, coveredPct: 0,
  resultEur: null, startEur: null };

/**
 * `Σ result ÷ Σ opening value` over the rows that have an opening value.
 *
 * ⚠ THE NUMERATOR IS RESTRICTED TO THE SAME ROWS AS THE DENOMINATOR. Summing every row's result
 * over only the rows that have an opening value would divide one population by another — the
 * figure would exceed the truth by whatever the excluded rows made, and it would still look like
 * a return.
 */
export function classWeightedReturn(
  rows: readonly ClassReturnRow[],
  /**
   * ⚠⚠ CASH RETURNS 0%, AND THAT IS AN ANSWER, NOT A MISSING ONE. AIRS books no `Beginwaarde` for
   * a cash line, so the rule below finds nothing to divide by and would print a dash — which says
   * "we could not work this out" about the one asset whose return is certain. It earned nothing;
   * a euro is always worth a euro.
   *
   * ⚠ AND ITS DRAG IS A FACT. This repo prices cash at 0% rather than skipping it everywhere else
   * (`portfolio_math.make_cash_holding`) for the reason recorded there: dropping it scales a
   * 20%-cash portfolio's return up by 25%. A dash invites exactly that — treating cash as an
   * unknown to be ignored — where a 0% states the drag.
   */
  zeroWhenNoOpening = false,
): ClassReturn {
  const priced = rows.filter((r) => r.start_value_eur != null && r.start_value_eur > 0);
  const startEur = priced.reduce((s, r) => s + r.start_value_eur!, 0);
  const allResult = rows.reduce((s, r) => s + (r.result_eur ?? 0), 0);
  if (!priced.length || startEur <= 0) {
    // ⚠ The income leg still counts. A cash account that was credited interest made real money,
    // and only its PRICE leg is asserted to be zero — so an all-cash class with income is not
    // flatly 0%, it is whatever that income was over... nothing to divide by. Still 0: the rate is
    // undefined and 0 is the honest floor, while the euros stay visible in the Result column.
    if (zeroWhenNoOpening) {
      return { ...EMPTY, pct: 0, rows: rows.length, coveredPct: 100,
        resultEur: allResult || 0, startEur: null };
    }
    return { ...EMPTY, rows: rows.length, resultEur: allResult || null };
  }
  const resultEur = priced.reduce((s, r) => s + (r.result_eur ?? 0), 0);
  return {
    pct: (resultEur / startEur) * 100,
    legs: priced.length,
    rows: rows.length,
    // ⚠ Of the RESULT, not of the weight. The question a reader has is "does this rate describe
    // all the money this class made", and an unpriced row that made nothing costs nothing.
    coveredPct: allResult === 0 ? 100 : Math.abs(resultEur) / Math.abs(allResult) * 100,
    resultEur,
    startEur,
  };
}
