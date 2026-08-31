/**
 * "36 of 42 companies" — how many holdings a blended line was actually drawn from, and why the
 * others are missing.
 *
 * ⚠⚠ A LINE DRAWN FROM FEWER COMPANIES THAN ITS LABEL NAMES IS INVISIBLE, WHICH IS THE WHOLE
 * HAZARD. Both of this app's blend constructions can quietly leave holdings out, for reasons that
 * have nothing to do with each other:
 *
 *   * `positive_only` — a CHOSEN survivorship filter. `fcf_ps` and `eps_nri` are drawn from the
 *     companies positive in every period (`earnings._POSITIVE_ONLY_METRICS`), which deletes the
 *     cash-burners, the recoveries and every bank whose free cash flow swings on deposit flows.
 *     What remains looks exactly like an index line. ⚠ For a metric with a forecast leg the rule
 *     spans the CONSENSUS too, so a profitable company analysts expect to lose money is out of the
 *     whole line rather than out of half of it. ⚠⚠ IT BELONGS TO A **GROWTH** LINE AND NOWHERE
 *     ELSE: it exists because a year-on-year chain divides a member by itself. The FCF-SBC margin
 *     card had the same filter for an afternoon (2026-08-31) and it was removed — an average of
 *     ratios needs no positives, so there the rule was survivorship with nothing bought for it.
 *   * `aggregate` — an ABSENT INPUT. The euro sum needs a share count and a currency to turn a
 *     per-share filing into euros, and a member without one contributes nothing to the sum. Nobody
 *     decided to exclude it; it simply is not there.
 *
 * ⚠⚠ AND THE SECOND ONE WENT UNCOUNTED FOR MONTHS, WHICH IS WHY THIS MODULE EXISTS RATHER THAN A
 * STRING IN THE CARD. `blend_series` measured it (`fund_members`) and `_blend_rows` threw the
 * number away, so the FCF card said "36 of 42" and the EPS card beside it said nothing at all —
 * and "nothing at all" is exactly what a card with no drops says too. The reader had no way to
 * tell "EPS used everything" from "nobody counted".
 *
 * ⚠ THE PROSE IS PICKED FROM THE SERVER'S `rule`, NEVER FROM THE METRIC NAME. The rules move: the
 * single lever that turned `fcf_ps` from a euro sum into a filtered average was a set membership in
 * `routers/earnings.py`, and a client that re-derived the reason from "is this the FCF card" would
 * be a second copy of that decision — printing a confident wrong explanation of a number that was
 * right, the day the two disagreed.
 */

/** One line's members, as `earnings._blend_rows` reports them. */
export type MemberCount = {
  /** Members the line was drawn from. */
  considered: number;
  /** Holdings that report this metric at all — the same denominator on both constructions. */
  total: number;
  /** Why any are missing. Absent on an older payload; see {@link memberCountHow}. */
  rule?: string;
};

/**
 * The counts for a card, whichever of its metric-code spellings the blend actually saw.
 *
 * ⚠ A CARD KNOWS SEVERAL CODES (`cfg.codes` carries both GuruFocus section spellings), so it takes
 * the first that answered rather than assuming which one this company files under — the same reason
 * `noteFor` exists in `blendNotes`.
 */
export function countFor(
  codes: readonly string[],
  counts?: Record<string, MemberCount>,
): MemberCount | undefined {
  if (!counts) return undefined;
  for (const c of codes) if (counts[c]) return counts[c];
  return undefined;
}

/** Was anything withheld? ⚠ `considered === total` is every other card, every day. */
const withheld = (c?: MemberCount): c is MemberCount =>
  !!c && c.considered < c.total;

export type MemberCountLine = { text: string; rule: string };

/**
 * The count line under a card's title, or `null` when both lines used everything they had.
 *
 * ⚠ BOTH SIDES, SEPARATELY, AND ONLY THE ONES THAT DROPPED ANYTHING. The book and the index are two
 * blends over two sets of companies; one count standing for both would be wrong on whichever it was
 * not, and printing "42 of 42" on the side that withheld nothing is noise on thirteen charts to
 * make one honest.
 *
 * ⚠ THE OWN LINE IS GATED ON `isAgg`. A single company is one member and the count is a tautology.
 */
export function memberCountLine({ own, bench, isAgg, ownLabel, benchLabel }: {
  own?: MemberCount;
  bench?: MemberCount;
  isAgg: boolean;
  ownLabel: string;
  benchLabel?: string | null;
}): MemberCountLine | null {
  const parts: string[] = [];
  if (isAgg && withheld(own)) {
    parts.push(`${ownLabel}: ${own.considered} of ${own.total} companies`);
  }
  if (benchLabel && withheld(bench)) {
    parts.push(`${benchLabel}: ${bench.considered.toLocaleString('en-US')} of `
      + `${bench.total.toLocaleString('en-US')}`);
  }
  if (!parts.length) return null;
  // ⚠ THE RULE IS PER METRIC, NOT PER LINE — `_blend_rows` is the one place a book and an index
  // build their members, so both sides were filtered by the same rule and either may name it. The
  // shown side is preferred so the sentence explains a number that is actually on screen.
  const rule = (isAgg && withheld(own) ? own.rule : undefined) ?? bench?.rule ?? 'all';
  return { text: parts.join(' · '), rule };
}

/** What the ⓘ beside the count says, per `rule`. */
export function memberCountHow(rule: string): string {
  if (rule === 'positive_only') {
    return 'This line is drawn from the companies positive in every period '
      + '— and, where the chart carries an analyst forecast, in every estimated period too, '
      + 'so the solid half of the line and the dotted half are the same companies. A weighted '
      + 'year-on-year growth can then be taken over figures that are all positive.\n\n'
      + 'The cost is survivorship, and it runs one way: cash-burners, recoveries and banks are '
      + 'excluded, and averaging growth rates is upward-biased on top of that. Read it as how the '
      + 'survivors grew.\n\n'
      + 'The excluded companies are still in the per-holding table behind the chart.';
  }
  if (rule === 'aggregate') {
    // ⚠⚠ IT USED TO SAY "it needs a share count", WHICH IS THE WRONG REASON FOR THE ONLY
    // METRIC THAT CAN SHOW THIS. A share count converts a PER-SHARE filing into a company
    // total, and the only aggregated metric left is revenue, which is a total already
    // (`_AGGREGATABLE_TOTAL`, `per_share = false`). Measured on ACWI: 1,511 of 1,511
    // constituents carry euros and the line is still drawn from 1,509 — the two missing are
    // out because they have no MARKET CAP in any period they report, so they are in no
    // period's weighted average. A confident wrong explanation of a right number sends the
    // reader to fix a share count that was never the problem.
    return 'This line adds up what the companies behind it actually earned, in euros. A '
      + 'company counts in a period only where we have both its figure and its market cap '
      + 'for that period, so one with no market-cap history is in none of them.\n\n'
      + 'Nothing was excluded on purpose. The line is not biased by the ones missing — it '
      + 'simply speaks for fewer companies than the chart names, which is why the count is '
      + 'here. They are still in the per-holding table behind the chart.';
  }

  // ⚠ THE HONEST FALLBACK. An older payload carries no `rule`, and inventing one of the two
  // explanations for a count whose cause is unknown is the exact failure this module documents.
  return 'Some companies are not in this line. The per-holding table behind the chart lists every '
    + 'holding, including the ones the chart could not use.';
}
