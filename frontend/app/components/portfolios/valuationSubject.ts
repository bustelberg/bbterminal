/**
 * WHICH COMPANY THE FUNDAMENTAL MODAL IS VALUING, when /research-dashboard has picked two.
 *
 * ⚠⚠ QUICK AND DEEP VALUATION ARE THE ONLY TABS THAT CANNOT DRAW A PAIR. Graphs and Tables put
 * company B on the benchmark line, so both companies are on screen at once and there is nothing to
 * switch between; a reverse DCF and a price-vs-FCF multiple each want ONE share price, ONE share
 * count and one set of saved assumptions, so a second company has nothing to be drawn beside.
 * Before this, picking B changed fourteen charts and left those two tabs answering about A alone,
 * and the only way to value B was to swap the two pickers on the page — which throws the comparison
 * away to get at half of it.
 *
 * ⚠⚠ SO THERE ARE TWO ANSWERS HERE, NOT ONE, AND CONFLATING THEM IS THE BUG THIS MODULE EXISTS TO
 * PREVENT:
 *
 *   * `valued` — what the two valuation tabs READ. It follows the switch on every tab, including
 *     the ones where the switch is not rendered, because both tabs stay MOUNTED once visited: a
 *     `valued` that reverted to A off-tab would silently re-point a hidden panel at the other
 *     company and the reader would come back to a different valuation than they left.
 *   * `shown` — what the HEAD names and what the fundamentals refresh acts on. It follows the
 *     switch ONLY on the two tabs the switch governs, because Graphs and Tables always take A as
 *     their subject with B on the benchmark line.
 *
 * ⚠⚠ THE HEAD HAS TO FOLLOW AT ALL, WHICH IS THE HALF THAT IS EASY TO MISS. Neither valuation tab
 * prints the company anywhere a reader looks first — the name reaches a toast label, one
 * empty-state sentence and the two sub-modals — so the modal's 2xl heading is the ONLY thing on
 * screen saying whose cash flows these are. Left naming A while the panel valued B, every figure
 * under it is attributed to the wrong company by the one line that attributes anything.
 *
 * ⚠ AND THE REFRESH BUTTON FOLLOWS FOR THE REASON `OwnerEarningsModal` already states about it:
 * "a control's scope must match its screen, or it is a trap". A button that refetched A while the
 * reader watched B is exactly the case that rule was written for.
 *
 * Pure, so the asymmetry above is testable — in the component it is four ternaries nothing could
 * see.
 */

/** Which of the pair the valuation tabs are on. `'a'` is the modal's own subject. */
export type ValueSide = 'a' | 'b';

/**
 * A company, in the shape the tabs take it.
 *
 * ⚠ `isin` IS `''` AND `name` IS `null` WHEN ABSENT, never `undefined` — every caller writes
 * `name || isin` for a display label, and `undefined` there is the string "undefined" one optional
 * chain away.
 */
export type ValuationSubject = { isin: string; name: string | null };

/** The tabs the switch governs. ⚠ The KEYS, not the labels — the labels are translated. */
const VALUATION_TABS: ReadonlySet<string> = new Set(['quickval', 'deepval']);

export function valuationSubject({ isin, name, compare, side, tab }: {
  isin?: string;
  name?: string | null;
  /** Company B, when the page has one. */
  compare?: { isin: string; name: string } | null;
  side: ValueSide;
  /** The modal's current tab KEY. */
  tab: string;
}): {
  valued: ValuationSubject;
  shown: ValuationSubject;
  onValuationTab: boolean;
  /** Should the A/B switch render? */
  switchable: boolean;
} {
  const a: ValuationSubject = { isin: isin ?? '', name: name ?? null };
  const onValuationTab = VALUATION_TABS.has(tab);
  /**
   * ⚠ THE `compare &&` GUARD IS WHAT MAKES THE SIDE SAFE TO HOLD IN STATE. With no company B the
   * side collapses to A here, so nothing has to reset it in an effect when B is cleared — and an
   * effect is exactly how a stale B would survive one render and value a company that is no longer
   * on the page.
   */
  const valued: ValuationSubject = compare && side === 'b'
    ? { isin: compare.isin, name: compare.name }
    : a;
  return {
    valued,
    shown: onValuationTab ? valued : a,
    onValuationTab,
    switchable: !!compare && onValuationTab,
  };
}
