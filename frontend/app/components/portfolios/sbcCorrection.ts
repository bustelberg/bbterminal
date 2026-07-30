/**
 * The tab-level "SBC correction" — ONE definition, consumed by every chart whose numerator is free
 * cash flow.
 *
 * WHY IT IS A TOGGLE AND NOT A DECISION MADE ONCE
 *   Stock-based compensation is a real cost paid in shares. It never leaves the cash flow
 *   statement, so reported FCF flatters any company that pays its people in equity — and the
 *   effect is largest exactly where it matters most (software, biotech). Subtracting it asks "what
 *   would the cash flow be if these people were paid in money?", which is the honest owner's
 *   question. Leaving it asks "what cash actually moved?", which is the honest treasurer's
 *   question. Both are legitimate; which one a reader wants depends on what they are doing, so
 *   they choose — and the charts say which choice is in force.
 *
 * ⚠ DEFAULT ON. The uncorrected figure is the flattering one, and a chart that flatters by default
 * is a chart that misleads by default.
 *
 * ⚠ ONE FUNCTION, FOUR CARDS. FCF-SBC margin, FCF-SBC yield, cash return on capital and
 * FCF / Net Income all take their numerator from here. Two of them already subtracted SBC
 * unconditionally and two never did — which meant the same book could be described as both
 * SBC-corrected and not, on one screen, with nothing saying so.
 */

/**
 * FCF for the numerator, corrected or not.
 *
 * ⚠ A MISSING SBC IS ZERO, NOT UNKNOWN — and that is a deliberate asymmetry with how this codebase
 * treats missing data elsewhere. Most companies genuinely report none; blanking their ratio would
 * empty the chart for the majority in order to be pedantic about the minority. A company that pays
 * no stock compensation and a company we failed to ingest it for are indistinguishable here, and
 * the cost of conflating them is one company reading slightly high, versus losing every company
 * that legitimately reports nothing.
 *
 * ⚠ FCF ITSELF MISSING IS STILL NULL. That is the numerator; without it there is no ratio.
 */
export function correctedFcf(
  fcf: number | null | undefined,
  sbc: number | null | undefined,
  correct: boolean,
): number | null {
  if (fcf == null) return null;
  return correct ? fcf - (sbc ?? 0) : fcf;
}

/** What the numerator is called once the toggle has been applied — so a card's own title states
 *  which figure it is drawing rather than leaving the checkbox as the only clue. */
export const fcfLabel = (correct: boolean) => (correct ? 'FCF-SBC' : 'FCF');
