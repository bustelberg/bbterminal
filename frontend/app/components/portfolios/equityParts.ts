/**
 * Splitting the Stocks class into the two things it now contains: OPERATING COMPANIES and FUNDS.
 *
 * ⚠⚠ THE CLASS STOPPED BEING ONE KIND OF THING AND THE TABLE NEVER SAID SO. Until `Equity ETF` was
 * retired (2026-08-18) an equity ETF had its own bucket, so a `Stocks` section was companies and
 * nothing else. Folding the ETFs in was right for the ALLOCATION — a stock ETF is stock exposure,
 * and splitting it out of the class overstated how concentrated the book looked — but it left one
 * list mixing "ASML, 4.2%" with "iShares Core MSCI World, 11.8%", which are not comparable rows: the
 * second is already a thousand of the first. Nothing on screen distinguished them.
 *
 * ⚠ `is_fund` IS THE FLAG, AND IT IS THE SAME ONE EVERY OWNER-EARNINGS GATE USES. The backend sets
 * it per holding for exactly this reason (see `EQUITY_BUCKET`'s ⚠⚠ and the class basket's, which
 * filters `!h.is_fund` so the blender is never handed an instrument with no earnings). Deriving
 * fund-ness here from a name or an ISIN prefix would be a second, worse definition sitting one file
 * away from the real one.
 *
 * ⚠ PURE, AND SEPARATE FROM THE COMPONENT, because the component needs a DOM to render and this
 * repo tests no DOM. The rule worth pinning is not the markup — it is WHEN a division appears at
 * all, and what the share beside it is a share OF.
 */

/** The little a caller needs to be split. Structurally typed, so `BookHolding` satisfies it. */
export type Splittable = { is_fund?: boolean | null; weight_now_pct?: number | null };

export type EquityPart<T> = {
  /** A stable React key, and the reason a part exists at all. `all` = undivided. */
  key: 'all' | 'stocks' | 'funds';
  /** Null on `all` — an undivided class draws no sub-header. */
  label: string | null;
  rows: T[];
  /** This part's share OF THE CLASS, 0-100, or null when the class has no weight to divide. */
  classPct: number | null;
};

/**
 * ⚠ THE LABELS. "Individual stocks" rather than "Stocks", because the section above it is already
 * called Stocks and repeating the word would read as the class restated rather than as half of it.
 * "Stock ETFs" rather than "Funds": every fund in this class is equity exposure — a bond ETF is in
 * Bonds and a commodity one in Alternatives — so the narrower word is the true one, and it is the
 * name the retired bucket used, which is what the reader saw here until recently.
 */
const LABEL = { stocks: 'Individual stocks', funds: 'Stock ETFs' } as const;

/**
 * The Stocks class as its two halves, or as one undivided list.
 *
 * ⚠⚠ IT RETURNS A SINGLE `all` PART UNLESS THE DIVISION IS REAL, and that is most of the value here.
 * A book with no ETFs, or one that is nothing but ETFs, gets no sub-header — a heading over the
 * whole list says a division exists where there is none, and a section labelled "Individual stocks"
 * followed by every row is furniture that has to be read before it can be ignored. Same rule the
 * table already applies to the `No longer held` block, which is absent rather than empty.
 *
 * ⚠ ONLY THE EQUITY CLASS. Every other bucket is either all funds (a bond book is largely ETFs) or
 * has no fund concept at all (Cash), so the split answers nothing there. The caller passes its
 * bucket rather than this guessing, so the one place that knows which class is which stays the one
 * place that decides.
 *
 * ⚠ ORDER IS COMPANIES FIRST. The class is named for them, they are usually the many, and a reader
 * scanning for a specific holding scans that list; the funds are a short tail. It is also the order
 * the two used to appear in when they were separate buckets.
 */
export function equityParts<T extends Splittable>(
  bucket: string, equityBucket: string, rows: T[],
): EquityPart<T>[] {
  const undivided: EquityPart<T>[] = [{ key: 'all', label: null, rows, classPct: null }];
  if (bucket !== equityBucket) return undivided;

  const stocks = rows.filter((r) => !r.is_fund);
  const funds = rows.filter((r) => r.is_fund);
  if (!stocks.length || !funds.length) return undivided;

  // ⚠ THE DENOMINATOR IS THE CLASS, NOT THE BOOK. `weight_now_pct` is a share of the whole
  // portfolio, so summing the funds gives "8% of everything" — true, and not the question a
  // division inside Stocks raises. "38% of Stocks" is. Both halves therefore divide by the class
  // total and the two shares add to 100.
  //
  // ⚠ AND A ZERO TOTAL IS `null`, NEVER 0%. A class whose holdings are all unpriced has no weight
  // to apportion; printing "0% of Stocks" beside a list of real rows claims the book holds none of
  // something it is looking at.
  const w = (rs: T[]) => rs.reduce((a, r) => a + (r.weight_now_pct ?? 0), 0);
  const total = w(rows);
  const pct = (rs: T[]) => (total > 0 ? (100 * w(rs)) / total : null);

  return [
    { key: 'stocks', label: LABEL.stocks, rows: stocks, classPct: pct(stocks) },
    { key: 'funds', label: LABEL.funds, rows: funds, classPct: pct(funds) },
  ];
}
