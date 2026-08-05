/**
 * A CLASS's return in the Analyse modal's Holdings table — the rows under it, aggregated.
 *
 * ⚠ WEIGHTED AT THE WINDOW'S OPEN, NEVER BY THE `Weight (now)` COLUMN BESIDE IT. The column shows
 *   each holding's CURRENT share of the book, and that share already contains the return: a
 *   holding that doubled carries ~2× the weight it held while it was doubling, so
 *   Σ(now-weight × return) hands the winners a share of the class they never had. It is not a
 *   rounding difference — the same failure measured elsewhere in this codebase read +58.75%
 *   against a true +44.99% (`book_legs`), and +11.19% against +5.58% (`startWeights`). Both were
 *   plausible numbers, which is what makes the bug survive.
 *
 * ⚠ THE WEIGHT IS `weight_pct`, THE OPENING-VALUE SHARE, AND THE OTHER START WEIGHT IN THE PAYLOAD
 *   WOULD GIVE THE SAME ANSWER. `weight_pct` is AIRS's Beginwaarde over the PRICED book;
 *   `weight_start_pct` is the same Beginwaarde over the WHOLE book. Two denominators, and both
 *   cancel here because a class figure renormalises within the class anyway. `weight_pct` is the
 *   one used because (a) it is present whenever the row can be weighed at all, where
 *   `weight_start_pct` is grafted on from the attribution basis and is absent entirely when that
 *   basis cannot be built, and (b) it is the identical weight `SleeveBreakdown`'s Contribution
 *   column uses — so this figure IS the sum of that column, and the modal cannot show the class
 *   two returns a few points apart.
 *
 * ⚠ THE RETURN IS THE INSTRUMENT'S OWN (`own_return_pct`), SO THIS IS THE COLUMN'S OWN AGGREGATE
 *   AND NOT THE BOOK'S VALUE CHANGE. Those are two different measures — the book knows what the
 *   CERTIFICATE did, not what NVIDIA did — and the class header deliberately shows the one built
 *   from the rows printed beneath it. It follows that it need NOT equal the book's own class
 *   return in the allocation legend: where a position is reached through a certificate the two
 *   are answering different questions. Exactness against the book was the alternative, and it
 *   costs every row's number being its wrapper's (measured: 135 stocks, 37 distinct returns).
 *
 * ⚠ A LEG THAT CANNOT BE PRICED LEAVES BOTH SIDES, AND THAT HAS TO BE REPORTED RATHER THAN
 *   ABSORBED. Renormalising over what is left is the right arithmetic and a silent one: the class
 *   then reads as though the missing weight behaved exactly like the rest. `coveredPct` is the
 *   share of the class's opening weight the figure actually speaks for, and the caller shows it.
 */

export type ClassReturnRow = {
  /** Opening-value share of the book. Null where the position had no value at the window's open
   *  (cash, or bought since) — see `notHeldAtOpen`. */
  weight_pct?: number | null;
  /** The instrument's own EUR return over the window, in %. Null where nothing could price it. */
  own_return_pct?: number | null;
};

export type ClassReturn = {
  /** The class's start-weighted return, in %. Null when no row can be weighed AND priced — a
   *  dash, never a 0%, because "nothing to weigh" and "went nowhere" are different facts. */
  pct: number | null;
  /** Rows that spoke for the figure. */
  legs: number;
  /** Rows carrying an opening weight — the denominator's full membership, priced or not. */
  weighed: number;
  /** Share of the class's opening weight the figure speaks for, 0–100. 100 when every weighed row
   *  is priced; below that, weight has silently left the average. */
  coveredPct: number;
  /** Rows with NO opening weight at all: they were not held when the window opened (cash, or
   *  bought since), so they are out of a start-weighted measure by construction — an absence to
   *  name, not a hole to warn about. */
  notHeldAtOpen: number;
};

const EMPTY: ClassReturn = { pct: null, legs: 0, weighed: 0, coveredPct: 0, notHeldAtOpen: 0 };

/**
 * Σ ( wᵢ/Σw × retᵢ ) over the rows that have BOTH an opening weight and a return.
 *
 * ⚠ A NON-POSITIVE OR MISSING WEIGHT IS NOT A ZERO-WEIGHT MEMBER, IT IS A NON-MEMBER. AIRS gives
 * a position bought mid-window no Beginwaarde, so it has real exposure today and an undefined
 * share of the opening book; counting it at 0 would be arithmetically harmless and would then
 * report it as covered, which is the part that is false.
 */
export function classWeightedReturn(rows: readonly ClassReturnRow[]): ClassReturn {
  const weighed = rows.filter((r) => r.weight_pct != null && r.weight_pct > 0);
  if (!weighed.length) return { ...EMPTY, notHeldAtOpen: rows.length };
  const legs = weighed.filter((r) => r.own_return_pct != null);
  const denomAll = weighed.reduce((s, r) => s + r.weight_pct!, 0);
  const denom = legs.reduce((s, r) => s + r.weight_pct!, 0);
  const base = {
    legs: legs.length,
    weighed: weighed.length,
    // ⚠ Against the weighed rows only. A row with no opening weight is not missing from this
    // average, it was never in it — folding the two together would report a book that is 20% cash
    // as 80% covered and send the reader looking for a pricing fault that does not exist.
    coveredPct: denomAll > 0 ? (denom / denomAll) * 100 : 0,
    notHeldAtOpen: rows.length - weighed.length,
  };
  if (!legs.length || denom <= 0) return { ...base, pct: null, coveredPct: 0 };
  return {
    ...base,
    pct: legs.reduce((s, r) => s + (r.weight_pct! / denom) * r.own_return_pct!, 0),
  };
}
