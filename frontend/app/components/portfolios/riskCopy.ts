'use client';

import { useLang, type Lang } from '../../../lib/i18n';
import { v } from '../../../lib/dynamicValue';

/**
 * The risk panel's copy, in both languages — the seven views behind the Analyse modal's Risk button.
 *
 *  English is the source, dutch is the translation, and a missing dutch string is a compile
 * ERROR. `nl` is typed as `RiskCopy`, so a key added to `en` and forgotten here fails `tsc` rather
 * than falling back to English — a half-translated panel renders as a rendering bug, and nobody
 * reports one of those as an unfinished translation. Same rule `tablesCopy` and `managementCopy`
 * already set.
 *
 *  One surface, complete. `managementCopy`'s own header explains why the guarantee is per surface
 * rather than per page: the Analyse modal family is ~661 visible strings and requiring all of them
 * before any of them would be one change nobody can review. These seven views are a surface — they
 * are either fully Dutch or fully English, never half — and what is still English is listed in
 * `UNTRANSLATED_SURFACES`.
 *
 *  THE ⓘ PROSE IS IN SCOPE HERE, WHICH REVERSES THE EARLIER DECISION (2026-08-21 had it out).
 * These cards are not decoration: they carry the definition, the convention chosen, and the caveat
 * that stops a figure being misread — a Dutch reader who has to switch languages to find out that
 * `ā` was subtracted has not been given the number, only its digits.
 *
 *  Some terms stay english on purpose, and they are the exception rather than laziness. "Active
 * share", "tracking error", "drawdown", "Sharpe", "Sortino", "information ratio" and "HHI" are what
 * a Dutch wealth manager says out loud; "actief aandeel" is not a term, it is a translation of one.
 * Where a real Dutch word exists it is used — volatiliteit, correlatie, concentratie, rendement,
 * gewicht, positie, onderneming.
 *
 *  AIRS'S own field names are not in here, same rule as `managementCopy`: `Beginwaarde`,
 * `Huidige waarde` are the SOURCE's column names and appear identically in both languages.
 */

/**
 * A ⓘ card: headline, where it comes from, over what window, and the caveat.
 *
 *  `how` IS OPTIONAL (2026-08-25). It used to be required, on the reasoning that every figure
 * owes the reader a caveat — but a required field gets filled whether or not there is anything to
 * say, and a card whose How restates its What in different words teaches nothing while making the
 * one card that DOES carry a warning look like more of the same. Where a measure is explained by
 * its own formula and legend, How is left off.
 */
type Card = { what: string; where?: string; when?: string; how?: string };

export type RiskCopy = {
  /** The switch.  SHORT — six of these sit on one row inside a fixed dialog. */
  views: {
    active: string; te: string; corr: string; vol: string; dd: string; conc: string;
  };
  /** The dialog heading per view, rendered as `<title> vs <benchmark>`. */
  titles: {
    active: string; te: string; corr: string; vol: string; dd: string; conc: string;
  };
  basis: { label: string; now: string; start: string };
  subtitle: (portfolio: string, asOf?: string | null) => string;
  close: string;

  common: {
    measured: string;
    daily: string; weekly: string; monthly: string;
    dailyNote: string;
    /**  THE NON-DAILY CADENCES' HOVER. `f` is an operand, not a word — a translation places it. */
    freqNote: (f: number) => string;
    /**  THE PRICE PROVENANCE, one sentence. The field and the vendor are operands — they come
     *  from `lib/provenance` and are names, never translated. */
    pricedFrom: (field: string, vendor: string) => string;
    computing: string;
    /** `x of y priced`, and the sentence that frames every synthetic series. */
    synthetic: (years: number, priced: number, total: number) => string;
    observations: string;
  };

  active: {
    activeShare: string; overlap: string; offBenchmark: string; stocks: string;
    heldOnly: (n: number) => string; everyName: (n: number) => string;
    colCompany: string; colBook: string; colActive: string; notHeld: string;
    otherPositions: string;
    currentWeight: string; startWeight: string; weightHow: string;
    weightWhat: (company: string, book: string) => string;
    /** The footer row.  IT SAYS WHICH SET IT TOTALS — see the  in `ActiveSharePanel`. */
    totalHeld: (n: number) => string;
    totalAll: (n: number) => string;
    totalCard: Card;
    totalCardHeld: Card;
    /**
     *  A sentence, not `20 / 1678`. Two bare numbers over a slash is not a `Where` — it was
     * shortened to dodge a translation and the meaning went with it.
     *
     *  And it names both vendors, because the two sides of this comparison come from different
     * ones — the book from an AIRS scan, the index's weights from yfinance market caps — and a
     * `Where` that counted the rows without saying where they came from answered half its own
     * question.  The labels come from `provenance.sourceLabel`, never typed here: a source is
     * named once in this app, and a hand-written "AIRS" drifts the moment that table is made
     * more precise.
     */
    heldVsIndex: (held: number, members: number,
      bookSrc: string, benchField: string, benchVendor: string) => string;
    offBenchWhere: (off: number, held: number) => string;
    coverage: (pct: string, bench: string) => string;
    unmatched: (n: number, pct: string, names: string) => string;
    /**
     * What each symbol in the active-share formula stands for — see `tipCard`'s `Legend`.
     *
     *  The symbols themselves are not in here. `w_i^{\,p}` is LaTeX, identical in every language,
     * and a translated copy of it is a second place for a superscript to go wrong. Only the prose
     * is translated; the call site owns the notation.
     */
    /**
     *  Both weight rows name their side, and neither says "the book". A legend defining `wᵖ` and
     * `wᵇ` is defining which of two weightings a symbol refers to, so "the book's weight" makes
     * the reader carry the mapping themselves — on a screen that already shows the book's name.
     * The names go in badges (`v`), like every other live value in these cards.
     */
    legend: {
      issuer: string; heldSet: string; allSet: string;
      wp: (bookName: string) => string; wb: (bench: string) => string;
      /**  THE ROW THAT ANSWERS THE QUESTION THE OVERLAP TILE ACTUALLY PROVOKES — why the
       *  benchmark column can sum to more than the overlap. It is the min, and nothing else. */
      min: string;
      /** The condition under the Off-benchmark sum. */
      notInBench: (bench: string) => string;
      stocksNum: string; stocksDen: string;
      absActive: string;
    };
    /**
     * The active-share card's `When` — ONE LINE PER SIDE, each naming what it dates.
     *
     *  It takes already-formatted days (`asOfLine`) AND NULLS, and the null is the whole point:
     * the copy renders "no recorded date" rather than letting a missing stamp read as now. See the
     *  on `asOfLine` — the string this replaced asserted "Today's weights", which is an
     * assumption printed as a fact.
     *
     *  Two lines and no explanation of why there are two. An earlier version spent a sentence
     * saying the book and the index are refreshed by different jobs, so judge for yourself — which
     * is what the two dated lines already say, at four times the length. A `When` is read at a
     * glance; prose in it is prose nobody finishes.  `\n` is a real line break here: the tooltip
     * panel carries `whitespace-pre-line`.
     *
     *  The two parentheticals appear only when they have something to say — the read date only if
     * it differs from the valuation date, the undated count only if it is not zero. That is what
     * keeps the normal case to one date per line without dropping a fact when the fact exists.
     *
     *  Dates are not translated. `2026-08-25` is the same in both languages; only the words are.
     */
    whenWeights: (bookName: string, book: string | null, read: string | null,
      bench: string, caps: string | null, unstamped: number) => string;
    /**
     * The BOOK's date alone — for a figure with no benchmark in it.
     *
     *  The stocks tile is about the book and nothing else, so printing the index's cap range
     * beside it would date a side that does not appear in the number. Two cards, two Whens,
     * because they genuinely measure different things.
     */
    whenBook: (bookName: string, book: string | null, read: string | null) => string;
    cards: { activeShare: Card; overlap: Card; offBenchmark: Card; stocks: Card };
  };

  te: {
    trackingError: string; activeReturn: string;
    benchmarkReturn: (benchmark: string) => string; portfolioReturn: (portfolio: string) => string;
    activeReturnQuestion: (portfolio: string, benchmark: string) => string;
    trackingErrorQuestion: (average: string) => string;
    infoRatio: string; observations: string;
    /**
     * The ā ± TE interval, prepended to the TE card's `how` — see `activeBand`.
     *
     *  The operands arrive pre-formatted, signs and all (`signed2` in `TrackingErrorView`). A
     * translation places numbers, it never renders them: a `toFixed` on this side would be a
     * second rounding convention, and the band's ends would stop matching the worked line above
     * them in the same card.
     */
    /**  `bench` IS NULLABLE because the payload's is — `v()` renders the absence, and an
     *  empty-string fallback at the call site would print "against " with nothing after it. */
    bandReading: (lo: string, hi: string, centre: string,
                  bench: string | null | undefined) => string;
    cards: { te: Card; activeReturn: Card; infoRatio: Card; observations: Card };
    /**
     *  Four cards share `aₜ`, `T` AND `f`, WHICH IS WHY THE LEGEND IS ONE OBJECT AND NOT FOUR.
     * Written per tile the definitions drift on the first edit, and a view where `T` means "paired
     * periods" in one tooltip and "observations" in the next has taught the reader that the symbol
     * is decorative. It moved out of the view unchanged; the English is what was there.
     *
     *  THE "the answer:" PREFIX MARKS THE SYMBOL THE TILE ACTUALLY PRINTS, so a reader scanning a
     * six-row legend can find the one that is the number in front of them. It is part of the
     * translated string, not glued on at the call site, or the Dutch card would read "the answer:
     * het antwoord".
     */
    legend: {
      a: string;
      R: (book: string, bench: string | null | undefined) => string;
      aBar: string;
      T: (n: number | null | undefined) => string;
      f: (n: number | null | undefined) => string;
      te: string; teAnswer: string;
      prod: string;
      Ra: string; IR: string;
    };
    /**  THE FREQUENCY IS AN OPERAND, so the card names the cadence it actually measured.
     *   NULLABLE, matching the payload: `v()` renders the absence, where a `?? ''` at the call
     *  site would print "The T in the formula —  periods" and read as a rendering fault. */
    observationsWhat: (freq: string | null | undefined) => string;
    /** The sentence under the tiles: the book, then the window, then `note`. */
    sleeve: (book: string, from: string, to: string) => string;
    note: string;
  };

  corr: {
    rhoVs: (bench: string) => string; rSquared: string; meanPair: string; activeVol: string;
    betweenPositions: string; legend: string;
    /**  THE COVERAGE SENTENCE, distinct from `thinPairs`: that one explains why a CELL is blank,
     *  this one says how much of the matrix was measurable at all. */
    pairsMeasured: (measured: string, possible: string, minObs: string, holdings: string) => string;
    leastTitle: string; mostTitle: string;
    identityBroken: (pp: string) => string;
    thinPairs: (n: number) => string;
    cards: { rho: Card; rSquared: Card; meanPair: Card; activeVol: Card };
  };

  vol: {
    /**  THE TOOLBAR'S TRAILING NOTE. Every figure in this view is annualised and the cadence
     *  buttons beside it choose the sampling, not the unit — which is the confusion it exists to
     *  head off. */
    shownAnnualised: string;
    rollingCalculation: string;
    rollingPortfolio: string;
    rollingBenchmark: string;
    volatility: string; downside: string; benchVol: (bench: string) => string;
    question: (portfolio: string, benchmark: string) => string;
    worst: (period: string) => string;
    ret: string; sharpe: string; sortino: string;
    periodDay: string; periodWeek: string; periodMonth: string;
    cards: {
      volatility: Card; downside: Card; benchVol: Card; worst: Card;
      ret: Card; sharpe: Card; sortino: Card;
    };
    note: string;
  };

  dd: {
    /** Frequency is accepted for existing callers but omitted from the label. */
    maxDrawdown: (freq: string | null | undefined) => string;
    benchMax: (bench: string | null | undefined) => string;
    today: string;
    /**  THE THRESHOLD IS AN OPERAND — it is configurable, so a hardcoded "5%" would be a label
     *  that silently stops matching the rows under it. */
    episodes: (pct: string) => string;
    provenance: string;
    threeWays: string; threeWaysNote: string;
    worstInFull: string;
    peak: string; trough: string; recovered: string; peakToPeak: string; stillUnderwater: string;
    deepest: string; colDepth: string; colDecline: string; colRecovery: string; open: string;
    unitDays: string; unitWeeks: string; unitMonths: string;
    note: (unit: string) => string;
    cards: { maxDrawdown: Card; benchMax: Card; today: Card; episodes: Card };
  };

  conc: {
    companyWeightExplanation: string;
    effectiveCalculation: string;
    effectiveMeaning: (count: string) => string;
    benchmarkScaleNote: string;
    effective: string; ofIssuers: (n: number) => string;
    question: (portfolio: string, benchmark: string) => string;
    top10: string; ofBook: (pct: string) => string;
    largest: string; benchEffective: (bench: string) => string;
    benchSub: (n: number, top10: string) => string;
    tableTitle: string;
    colIssuer: string; colWeight: string; colCumulative: string;
    coverage: (pct: string, bench: string) => string;
    note: string; unresolved: (n: number) => string;
    cards: { effective: Card; top10: Card; largest: Card; benchEffective: Card };
  };

};

const NL_MONTHS = [
  'januari', 'februari', 'maart', 'april', 'mei', 'juni',
  'juli', 'augustus', 'september', 'oktober', 'november', 'december',
] as const;

/** A stable calendar-date spelling. Parsing as a JavaScript Date would shift the day in some
 * timezones, while this value is an AIRS valuation date with no time of day. */
export function riskLongDate(iso?: string | null): string | null {
  const match = iso?.slice(0, 10).match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const month = NL_MONTHS[Number(match[2]) - 1];
  return month ? `${Number(match[3])} ${month} ${match[1]}` : null;
}

const en: RiskCopy = {
  views: { active: 'Active share', te: 'Tracking error', corr: 'Correlation', vol: 'Volatility',
    dd: 'Drawdown', conc: 'Concentration' },
  titles: { active: 'Active share', te: 'Tracking error', corr: 'Correlation',
    vol: 'Volatility', dd: 'Max drawdown', conc: 'Concentration' },
  basis: { label: 'AIRS weights', now: 'Current', start: 'Start of year' },
  subtitle: () => 'Only stocks considered',
  close: 'close',

  common: {
    measured: 'Measured',
    daily: 'Daily', weekly: 'Weekly', monthly: 'Monthly',
    dailyNote: 'Daily closes are not synchronous — the tracker closes at 16:30 London, a US holding '
      + 'at 21:00 — which lowers the measured covariance.',
    freqNote: (f) => `f = ${f} observations per year.`,
    pricedFrom: (field, vendor) => `Prices from ${v(field)} at ${v(vendor)}.`,
    computing: 'Computing',
    synthetic: (y, p, t) => `The selected stock sleeve at the selected AIRS weights over ${y} years `
      + `(${p} of ${t} priced).`,
    observations: 'Observations',
  },

  active: {
    activeShare: 'Active share', overlap: 'Overlap', offBenchmark: 'Off-benchmark',
    stocks: 'Stocks',
    heldOnly: (n) => `What we hold (${n})`, everyName: (n) => `Every name (${n})`,
    colCompany: 'Company', colBook: 'Book', colActive: 'Active', notHeld: 'not held',
    otherPositions: 'Other portfolio positions',
    currentWeight: 'actual current AIRS portfolio weight', startWeight: 'opening AIRS weight',
    weightHow: 'Normalised stock weight multiplied by the portfolio’s actual stock allocation. The table shows whole-portfolio weights; headline active share and overlap remain normalised.',
    weightWhat: (company, book) => `${v(company)}'s actual weight in the complete ${v(book)} portfolio.`,
    heldVsIndex: (h, m, bookSrc, benchField, benchVendor) =>
      `${v(h)} companies and their weights from ${v(bookSrc)}, against ${v(m)} priced index `
      + `members weighted by ${v(benchField)} from ${v(benchVendor)}.`,
    offBenchWhere: (o, h) => `${v(o)} of the ${v(h)} companies held are not in the index.`,
    totalHeld: (n) => `Total — ${n} held`,
    totalAll: (n) => `Total — all ${n} names`,
    totalCard: {
      what: 'Both columns sum to 100%, so the Active column sums to exactly zero.',
      where: 'The totals use every company displayed in this table.',
      how: 'Each figure is the sum of its column. Active is the portfolio total minus the '
        + 'benchmark total.',
    },
    totalCardHeld: {
      what: 'The held names only, so the Active column does not sum to zero.',
      where: 'The portfolio column uses actual whole-portfolio stock weights. The profile-weighted ACWI '
        + 'column shows the index weights for those same names.',
      how: 'Each total sums the displayed held names. Benchmark-only names are omitted. The signed Active total is stock-basket weight minus ACWI weight for these names; it is neither overlap nor active share.',
    },
    coverage: (pct, b) => `Priced ${pct} of ${b}'s members. The missing weight is redistributed `
      + 'over the rest, so the active share reads slightly low.',
    unmatched: (n, pct, names) => `${n} holding${n === 1 ? '' : 's'} (${pct} of the sleeve) could `
      + `not be matched to a company name and count as fully active: ${names}`,
    legend: {
      //  It says what `i` IS, then gives the case that makes the fold matter. The previous
      // version — "both of its share classes are ONE i, not two" — presupposed two share classes,
      // which almost no company has, and never said that `i` is a term in the sum above.
      issuer: 'one company and one term in the sum. Alphabet A and Alphabet C are folded into a '
        + 'single company',
      heldSet: 'the set of companies held by the portfolio and shown in this table',
      allSet: 'the set of all companies shown in this table',
      wp: (bookName) =>
        `${v(bookName)}'s weight in that company within the stock basket normalised to 100%`,
      wb: (bench) => `${v(bench)}'s own weight in the same company, by market cap`,
      min: 'the smaller of the portfolio and benchmark weights for each company',
      notInBench: (bench) => `read as "over the companies ${v(bench)} does not hold at all" — the `
        + 'sum runs over those alone',
      stocksNum: 'the weight in individual stocks with an ISIN we could match',
      stocksDen: 'the weight in everything the book holds, funds and cash and bonds included',
      absActive: 'the size of each bet regardless of direction. An overweight and an underweight '
        + 'of the same size count the same',
    },
    whenBook: (bookName, book, read) =>
      `${v(bookName)} weights: ${v(book ?? 'no recorded date')}`
      + `${read && read !== book ? ` (read ${v(read)})` : ''}`,
    whenWeights: (bookName, book, read, bench, caps, unstamped) =>
      `${v(bookName)} weights: ${v(book ?? 'no recorded date')}`
      + `${read && read !== book ? ` (read ${v(read)})` : ''}\n`
      + `${v(bench)} weights based on marketcap from yfinance: ${v(caps ?? 'no recorded date')}`
      + `${unstamped > 0 ? ` (${v(unstamped)} undated)` : ''}`,
    cards: {
      activeShare: {
        what: 'How different your stock holdings are from ACWI. 0% means identical company weights; 100% means no overlap.',
      },
      overlap: {
        what: 'The share of the portfolio that overlaps with the benchmark.',
        how: 'Compares your stock holdings with ACWI. We add up the smaller weight for each company. '
          + 'A higher percentage means more overlap; overlap and active share total 100%.',
      },
      offBenchmark: {
        what: 'Weight in companies the index does not hold at all.',
        how: 'Active share mixes two decisions — owning something the index does not, and sizing '
          + 'something it does. This is only the first, so a book can be highly active with this '
          + 'at zero.',
      },
      stocks: {
        what: 'How much of the whole book this comparison covers.',
        where: 'Comparable stocks divided by every position in the complete AIRS book.',
        how: 'The rest stays in the denominator and appears as Other portfolio positions; the '
          + 'individual companies are not renormalised.',
      },
    },
  },

  te: {
    trackingError: 'Tracking error (realised)', activeReturn: 'Active return (ann.)',
    benchmarkReturn: (benchmark) => `${benchmark} return (ann.)`,
    portfolioReturn: (portfolio) => `${portfolio} return (ann.)`,
    activeReturnQuestion: (portfolio, benchmark) => `How would ${portfolio} have performed versus ${benchmark} since 1 January 2021 if it had held this exact portfolio throughout?`,
    trackingErrorQuestion: (average) => `How much would the active return have varied around its average of ${average} a year?`,
    infoRatio: 'Information ratio', observations: 'Observations',
    bandReading: (lo, hi, centre, bench) =>
      `A typical year is roughly between ${v(lo)} and ${v(hi)} versus ${v(bench)}. The range is `
      + `centred on the average active return of ${v(centre)}, not zero. About two years in three `
      + 'may fall within it, so treat it as a guide rather than a promise.',
    cards: {
      te: {
        what: "How much the portfolio's annual return has typically differed from the benchmark.",
        how: 'This is realised tracking error calculated from historical monthly returns. It can '
          + 'differ from a forward-looking estimate based on a covariance matrix.',
      },
      activeReturn: {
        what: "The portfolio's annual return above or below the benchmark.",
        where: 'The same active returns compounded over time, rather than their spread.',
        how: 'This is the return difference, not its volatility. A portfolio can finish level '
          + 'with the benchmark after moving very differently along the way. In that case active '
          + 'return is zero, but tracking error can still be high.',
      },
      infoRatio: {
        what: 'Active return per unit of tracking error.',
        where: 'Active return ÷ tracking error, both annualised.',
        how: 'This shows how much active return the portfolio earned for each unit of tracking '
          + 'error. No value is shown when tracking error is effectively zero because the ratio '
          + 'cannot be calculated.',
      },
      observations: {
        what: 'The T in the formula — the periods both series had.',
        where: 'The INTERSECTION of the two calendars, never a positional pairing.',
        how: 'A Stockholm listing and a London-traded tracker do not share holidays; zipping them '
          + 'offsets the two series from the first mismatch onward and produces a plausible figure '
          + 'measured against the wrong days.',
      },
    },
    legend: {
      a: 'the portfolio return minus the benchmark return in period t',
      R: (book, bench) => `${v(book)}'s and ${v(bench)}'s own returns in that period, both in EUR`,
      aBar: 'the mean active return over the window — the band above is centred on it, not on zero',
      T: (n) => `the number of monthly periods used (${v(n)})`,
      f: (n) => `periods per year (${v(n)}), the annualisation factor`,
      te: 'one standard deviation of the active return, per year',
      teAnswer: 'the answer: one standard deviation of the active return, per year',
      prod: 'multiply the period returns to compound them over time',
      Ra: 'the active return from the tile beside this one, annualised',
      IR: 'the answer: active return per unit of the tracking error taken to earn it',
    },
    observationsWhat: (freq) => `The T in the formula — ${v(freq)} periods both series had.`,
    sleeve: (book, from, to) =>
      `${book}'s stock sleeve at the selected AIRS weights, priced from ${from} to ${to} — `,
    note: "not the book's realised history, so a name bought in March contributes its January "
      + 'return. It is the same portfolio the Active share view describes.',
  },

  corr: {
    rhoVs: (b) => `Correlation vs. ${b} Index`, rSquared: 'R²', meanPair: 'Mean correlation between own positions',
    activeVol: 'Active vol (= TE)',
    betweenPositions: 'Between the positions',
    legend: 'Blue = diverging, amber = moving together. Ordered by weight.',
    leastTitle: 'Least correlated — what actually diversifies',
    mostTitle: 'Most correlated — one bet held twice',
    identityBroken: (pp) => ` σₐ measured and σₐ implied by ρ differ by ${pp}pp — the two series `
      + 'are no longer identical. This is a bug, not a market fact.',
    pairsMeasured: (measured, possible, minObs, holdings) =>
      `${v(measured)} of ${v(possible)} pairs with at least ${v(minObs)} shared returns, across `
      + `${v(holdings)} holdings.`,
    thinPairs: (n) => `A pair with fewer than ${n} overlapping returns is left blank rather than `
      + 'tinted — over ten weeks a correlation is noise with a sign, and a coloured cell looks '
      + 'exactly as authoritative as one measured over five years.',
    cards: {
      rho: {
        what: 'How closely the stock sleeve has moved with the benchmark.',
        how: 'We compare weekly returns in euros of today’s stock holdings with ACWI. '
          + 'Near +1 means they move together, near 0 means little relationship, and near −1 means opposite movements. '
          + 'Bonds and cash are excluded.',
      },
      rSquared: {
        what: "The share of the book's movement the index explains.",
        where: 'Calculated from the correlation above.',
        how: 'Higher values mean the portfolio moved more like the index.',
      },
      meanPair: {
        what: 'The average correlation between holdings.',
        how: 'We compare the weekly returns of every pair of stocks and take the average. Each pair counts equally. Higher values mean the stocks move together more; lower values suggest more diversification.',
      },
      activeVol: {
        what: 'The same tracking error the view beside this one reports.',
        where: 'Built from the same series, so the identity below is exact, not approximate.',
        how: ' THE IDENTITY IS SHOWN, NOT ASSERTED. The figure above is measured directly from '
          + 'the active returns; the worked line rebuilds it from ρ. They agree to floating-point '
          + 'noise, and would visibly diverge if the two views ever stopped reading the same series.',
      },
    },
  },

  vol: {
    shownAnnualised: '· shown annualised',
    rollingCalculation: 'Take the latest 12 monthly returns in EUR, calculate their sample standard deviation (using n − 1 = 11), then multiply by √12 to annualise. Multiply by 100 to display a percentage. Each point on the chart repeats this calculation for its preceding 12 months.',
    rollingPortfolio: 'The portfolio series combines historical instrument returns at the current holding weights. It reconstructs today’s holdings; it is not the client’s realised AIRS return history. Deposits and withdrawals are excluded.',
    rollingBenchmark: 'The benchmark uses historical EUR returns over the same monthly observations, with the benchmark weights shown in the label.',
    volatility: 'Volatility (ann.)', downside: 'Downside deviation',
    benchVol: (b) => `${b} volatility`, worst: (p) => `Worst ${p}`,
    question: (portfolio, benchmark) => `How much would ${portfolio}'s returns have varied compared with ${benchmark}?`,
    ret: 'Return (ann.)', sharpe: 'Sharpe', sortino: 'Sortino',
    periodDay: 'day', periodWeek: 'week', periodMonth: 'month',
    cards: {
      volatility: {
        what: 'The annualised variation in monthly portfolio returns.',
        how: 'This uses instrument returns at the selected portfolio weights. Deposits and '
          + 'withdrawals are not part of the series.',
      },
      downside: {
        what: 'The annualised variation in negative monthly returns.',
        how: 'Positive months count as zero. This is the downside-risk measure used in the '
          + 'Sortino ratio.',
      },
      benchVol: {
        what: "The index's own volatility, on the same periods.",
        where: 'Same function, same series, different input.',
        how: 'This provides a like-for-like comparison with the portfolio.',
      },
      worst: {
        what: 'The single worst period in the window.',
        how: 'This is an actual monthly loss observed in the data, not an annualised estimate.',
      },
      ret: {
        what: "The portfolio's annualised return over the same window.",
        how: 'This return is used in the Sharpe and Sortino ratios shown beside it. It is the '
          + 'portfolio return, not the active return versus the benchmark.',
      },
      sharpe: {
        what: 'Return per unit of total volatility.',
        how: 'Higher values mean more return above the risk-free rate for each unit of volatility.',
      },
      sortino: {
        what: 'Annualised return per unit of downside risk.',
        where: 'Return ÷ downside deviation, both annualised.',
        how: 'No value is shown when no monthly return fell below the target because there is no '
          + 'downside deviation to divide by. That is a valid result, not missing data.',
      },
    },
    note: 'Deposits and withdrawals cannot distort this — it is a weighted basket of instrument '
      + 'returns, not an account value, so there are no flows in the series to strip out. The cost '
      + "is that the weights are today's: this is the volatility of the portfolio as it stands, "
      + 'not the one the client lived through.',
  },

  dd: {
    maxDrawdown: () => 'Max drawdown', benchMax: (b) => `${b} max drawdown`,
    today: 'Today', episodes: (pct) => `Falls over ${pct}%`,
    provenance: "Reconstructed using today's holdings. For the portfolio's actual historical "
      + 'drawdown, use the AIRS returns.',
    threeWays: 'The same drawdown, measured three ways',
    threeWaysNote: 'Coarser cadences cannot see a fall that recovers inside the period, so they '
      + 'read shallower. Daily is the honest basis for this measure.',
    worstInFull: 'Worst drawdown timeline',
    peak: 'Peak', trough: 'Lowest point', recovered: 'Recovered', peakToPeak: 'Total',
    stillUnderwater: 'still underwater',
    deepest: 'Other drawdowns', colDepth: 'Depth', colDecline: 'Fall (days)',
    colRecovery: 'Recovery (days)',
    open: 'open',
    unitDays: 'trading days', unitWeeks: 'weeks', unitMonths: 'months',
    note: (u) => `All durations use ${u}.`,
    cards: {
      maxDrawdown: {
        what: 'The deepest peak-to-trough fall in the window.',
        how: 'The biggest fall from a previous high in a historical simulation of today’s portfolio. '
          + 'We use daily returns and today’s holdings and weights.',
      },
      benchMax: {
        what: "The benchmark's deepest fall over the same dates.",
        where: "The same daily calculation applied to the benchmark's tracker.",
        how: 'The same daily calculation is used, providing a direct comparison with the portfolio.',
      },
      today: {
        what: 'How far below its own high water mark the sleeve sits right now.',
        where: 'The window ended at a new high when this reads 0%.',
        how: ' "Worst ever −31%" and "down 28% right now" are very different conversations, and '
          + 'the second is the one being had.',
      },
      episodes: {
        what: 'The number of separate drawdowns that crossed the threshold shown in the label.',
        where: 'A drawdown begins below a previous peak and ends when that peak is regained.',
        how: 'A drawdown ends only when the previous peak is regained. A partial recovery and '
          + 'later fall remain part of the same episode.',
      },
    },
  },

  conc: {
    companyWeightExplanation: 'This company’s value as a percentage of the entire portfolio. Share classes of the same company are combined.',
    effectiveCalculation: 'Convert each company’s share of the stock basket to a decimal (10% = 0.10). Square these numbers, add them up, then divide 1 by the total. Ten equally weighted stocks give 10 effective positions.',
    effectiveMeaning: (count) => `This result means the stock basket is as concentrated as ${count} equally weighted stocks.`,
    benchmarkScaleNote: 'Using 60% instead of 100% of the same stock index does not change this count: the relative company weights stay the same. Bonds and cash are excluded.',
    effective: 'Effective positions', ofIssuers: (n) => `of ${n} companies held`,
    question: (portfolio, benchmark) => `How concentrated are ${portfolio} and ${benchmark}?`,
    top10: 'Top 10', ofBook: (p) => `${p} of the whole book`,
    largest: 'Largest position', benchEffective: (b) => `${b} effective`,
    benchSub: (n, t) => `of ${n} · top 10 ${t}`,
    tableTitle: "Largest companies, with the index's weight in each",
    colIssuer: 'Company', colWeight: 'Weight', colCumulative: 'Cumulative',
    coverage: (pct, b) => `Priced ${pct} of ${b}'s members — the missing weight redistributes over `
      + 'the rest, so the index reads slightly more concentrated than it is.',
    note: 'Folded onto ISSUERS, not lines — two share classes of one company are a single '
      + 'position, which is what stops the ten largest being decided by an identifier.',
    unresolved: (n) => ` ${n} holding${n === 1 ? '' : 's'} could not be matched to a company name `
      + 'and each counts as its own company.',
    cards: {
      effective: {
        what: 'How many equally-sized positions this book behaves like.',
        how: ' THE BETTER NUMBER, and the reason it leads. A cut at exactly ten is arbitrary — '
          + 'two books with the same C₁₀ can be an even ten-name portfolio and one dominated by '
          + 'its top three. This has no cut-off. Forty names of which five dominate reads far '
          + 'below forty.',
      },
      top10: {
        what: 'The share of the stock sleeve in its ten largest companies.',
        how: ' TWO DENOMINATORS, BOTH TRUE. The headline is of the STOCK SLEEVE, which is what '
          + 'compares across books; the line beneath is of the whole book including cash and '
          + 'funds. A book that is 30% cash really is less concentrated in absolute terms.',
      },
      largest: {
        what: 'The single biggest company, as a share of the sleeve.',
        how: ' A BIG POSITION IS NOT AUTOMATICALLY A BIG BET. Apple at 6% against an index '
          + 'holding 5% is a 1pp bet; the same 6% in a name the index does not hold is a 6pp one. '
          + 'The table below carries both.',
      },
      benchEffective: {
        what: "The index's own effective position count, on the same measure.",
        how: ' A cap-weighted index is far more concentrated than its member count suggests, so '
          + 'this is usually a small fraction of it — which is the honest comparison, not the raw '
          + 'count.',
      },
    },
  },
};

const nl: RiskCopy = {
  views: { active: 'Active share', te: 'Tracking error', corr: 'Correlatie', vol: 'Volatiliteit',
    dd: 'Drawdown', conc: 'Concentratie' },
  titles: { active: 'Active share', te: 'Tracking error', corr: 'Correlatie',
    vol: 'Volatiliteit', dd: 'Maximale drawdown', conc: 'Concentratie' },
  basis: { label: 'AIRS-wegingen', now: 'Actueel', start: 'Begin van het jaar' },
  subtitle: () => 'Only stocks considered',
  close: 'sluiten',

  common: {
    measured: 'Gemeten',
    daily: 'Dagelijks', weekly: 'Wekelijks', monthly: 'Maandelijks',
    dailyNote: 'Dagslotkoersen lopen niet gelijk — de tracker sluit om 16:30 Londen, een Amerikaanse '
      + 'positie om 21:00 — waardoor de gemeten covariantie lager uitvalt.',
    freqNote: (f) => `f = ${f} waarnemingen per jaar.`,
    pricedFrom: (field, vendor) => `Koersen uit ${v(field)} bij ${v(vendor)}.`,
    computing: 'Berekenen',
    synthetic: (y, p, t) => `De gekozen aandelenselectie tegen de gekozen AIRS-wegingen over ${y} jaar `
      + `(${p} van ${t} geprijsd).`,
    observations: 'Waarnemingen',
  },

  active: {
    activeShare: 'Active share', overlap: 'Overlap', offBenchmark: 'Buiten de benchmark',
    stocks: 'Aandelen',
    heldOnly: (n) => `Wat we houden (${n})`, everyName: (n) => `Alle namen (${n})`,
    //  "Portfolio", NIET "Boek" (2026-09-07, op verzoek). Het is de kolomkop van de tabel in
    // Risico, en de ⓘ eronder gebruikt hetzelfde woord — anders benoemt één paneel dezelfde kolom
    // op twee manieren.
    colCompany: 'Onderneming', colBook: 'Portfolio', colActive: 'Actief', notHeld: 'niet gehouden',
    otherPositions: 'Overige portefeuilleposities',
    currentWeight: 'werkelijke actuele AIRS-portefeuilleweging', startWeight: 'AIRS-weging aan het begin',
    weightHow: 'De genormaliseerde aandelenweging maal het werkelijke aandelenbelang. De tabel toont gewichten in de volledige portefeuille; active share en overlap blijven genormaliseerd.',
    weightWhat: (company, book) => `Het werkelijke gewicht van ${v(company)} in de volledige portefeuille ${v(book)}.`,
    heldVsIndex: (h, m, bookSrc, benchField, benchVendor) =>
      `${v(h)} ondernemingen en hun gewichten uit ${v(bookSrc)}, tegenover ${v(m)} geprijsde `
      + `indexleden gewogen naar ${v(benchField)} van ${v(benchVendor)}.`,
    offBenchWhere: (o, h) => `${v(o)} van de ${v(h)} gehouden ondernemingen zitten niet in de index.`,
    totalHeld: (n) => `Totaal — ${n} gehouden`,
    totalAll: (n) => `Totaal — alle ${n} namen`,
    totalCard: {
      what: 'Beide kolommen tellen op tot 100%, dus de kolom Actief telt op tot precies nul.',
      where: 'De totalen gebruiken alle ondernemingen die in deze tabel staan.',
      how: 'Elk getal is de som van zijn kolom. Actief is het portefeuilletotaal min het '
        + 'benchmarktotaal.',
    },
    totalCardHeld: {
      what: 'Alleen de gehouden namen, dus de kolom Actief telt niet op tot nul.',
      where: 'De portefeuille toont werkelijke gewichten in de volledige portefeuille; de profielgewogen ACWI-'
        + 'benchmarkkolom toont wat de index in dezelfde namen houdt.',
      how: 'Elk totaal is de som van de getoonde gehouden namen. Niet-gehouden ACWI-posities ontbreken hier. Actief is het aandelengewicht min het ACWI-gewicht voor deze namen; dit totaal is niet overlap of active share.',
    },
    coverage: (pct, b) => `${pct} van de leden van ${b} geprijsd. Het ontbrekende gewicht wordt over `
      + 'de rest herverdeeld, waardoor de active share iets te laag uitvalt.',
    unmatched: (n, pct, names) => `${n} positie${n === 1 ? '' : 's'} (${pct} van de selectie) kon `
      + `niet aan een ondernemingsnaam worden gekoppeld en telt volledig als actief: ${names}`,
    legend: {
      issuer: 'één onderneming en één term in de som. Alphabet A en Alphabet C worden tot één '
        + 'onderneming samengevoegd',
      heldSet: 'de verzameling ondernemingen die de portefeuille houdt en die in deze tabel staan',
      allSet: 'de verzameling van alle ondernemingen die in deze tabel staan',
      wp: (bookName) => `het gewicht van ${v(bookName)} in die onderneming binnen de aandelen, genormaliseerd naar 100%`,
      wb: (bench) => `het gewicht van ${v(bench)} zelf in diezelfde onderneming, naar marktkapitalisatie`,
      min: 'het kleinste van het portefeuille- en benchmarkgewicht voor elke onderneming',
      notInBench: (bench) => `te lezen als "over de ondernemingen die ${v(bench)} helemaal niet `
        + 'houdt" — de som loopt alleen over die',
      stocksNum: 'het gewicht in individuele aandelen met een ISIN die we konden koppelen',
      stocksDen: 'het gewicht in alles wat het boek houdt, inclusief fondsen, liquiditeiten en obligaties',
      absActive: 'de omvang van elke positie ongeacht de richting. Een over- en een onderweging '
        + 'van dezelfde grootte tellen even zwaar',
    },
    whenBook: (bookName, book, read) =>
      `Gewichten ${v(bookName)}: ${v(book ?? 'geen vastgelegde datum')}`
      + `${read && read !== book ? ` (opgehaald ${v(read)})` : ''}`,
    whenWeights: (bookName, book, read, bench, caps, unstamped) =>
      `Gewichten ${v(bookName)}: ${v(book ?? 'geen vastgelegde datum')}`
      + `${read && read !== book ? ` (opgehaald ${v(read)})` : ''}\n`
      + `Gewichten ${v(bench)} op basis van marktkapitalisatie van yfinance: `
      + `${v(caps ?? 'geen vastgelegde datum')}`
      + `${unstamped > 0 ? ` (${v(unstamped)} zonder datum)` : ''}`,
    cards: {
      activeShare: {
        what: 'Hoeveel uw aandelen afwijken van ACWI. 0% betekent dezelfde ondernemingsgewichten; 100% betekent geen overlap.',
      },
      overlap: {
        what: 'Het deel van de portefeuille dat overlapt met de benchmark.',
        how: 'Vergelijkt uw aandelen met ACWI. Per onderneming tellen we het kleinste gewicht mee. '
          + 'Een hoger percentage betekent meer overlap; overlap en active share tellen op tot 100%.',
      },
      offBenchmark: {
        what: 'Gewicht in ondernemingen die de index helemaal niet houdt.',
        how: 'Active share mengt twee beslissingen — iets bezitten dat de index niet heeft, en iets '
          + 'zwaarder of lichter wegen dat hij wél heeft. Dit is alleen de eerste, dus een boek kan '
          + 'zeer actief zijn terwijl dit nul is.',
      },
      stocks: {
        what: 'Welk deel van het hele boek deze vergelijking beslaat.',
        where: 'Vergelijkbare aandelen gedeeld door alle posities in het volledige AIRS-boek.',
        how: 'De rest blijft in de noemer en staat als Overige portefeuilleposities in de tabel; '
          + 'de individuele ondernemingen worden niet geherwogen.',
      },
    },
  },

  te: {
    trackingError: 'Tracking error (gerealiseerd)', activeReturn: 'Actief rendement (geann.)',
    benchmarkReturn: (benchmark) => `${benchmark}-rendement (geann.)`,
    portfolioReturn: (portfolio) => `${portfolio}-rendement (geann.)`,
    activeReturnQuestion: (portfolio, benchmark) => `Hoe zou ${portfolio} het sinds 1 januari 2021 hebben gedaan ten opzichte van ${benchmark} als het deze exacte portefeuille de hele periode had aangehouden?`,
    trackingErrorQuestion: (average) => `Hoe sterk zou het actieve rendement rond het gemiddelde van ${average} per jaar hebben geschommeld?`,
    infoRatio: 'Information ratio', observations: 'Waarnemingen',
    bandReading: (lo, hi, centre, bench) =>
      `Een doorsnee jaar landt op ā ± TE — tussen ${v(lo)} en ${v(hi)} ten opzichte van ${v(bench)}, met `
      + `als midden het gemiddelde actieve rendement van ${v(centre)} en dus NIET nul. Ongeveer twee `
      + 'op de drie jaren; actieve rendementen hebben dikkere staarten dan een normale verdeling, '
      + 'dus lees het als een orde van grootte en niet als een belofte.  HET MIDDEN IS HET '
      + 'REKENKUNDIG gemiddelde op jaarbasis en ligt daarmee iets boven de geometrische tegel '
      + 'Actief rendement ernaast — het verschil is ruwweg TE²/2, en een ±σ-band is alleen '
      + 'consistent rond het rekenkundige gemiddelde.',
    cards: {
      te: {
        what: 'Hoeveel het rendement van het boek is afgeweken van dat van de benchmark, '
          + 'geannualiseerd.',
        how: ' GEREALISEERD (ex-post), niet de ex-ante voorspelling uit een covariantiematrix — '
          + 'dat zijn verschillende getallen en ze lopen structureel uiteen. ā WORDT afgetrokken en '
          + 'de noemer is T−1 (Bessel); sommige aanbieders doen geen van beide — die versie is '
          + 'symmetrisch rond de benchmark, en groter.',
      },
      activeReturn: {
        what: 'Het jaarlijkse rendement van de portefeuille boven of onder de benchmark.',
        where: 'Dezelfde actieve rendementen, samengesteld over de tijd in plaats van hun spreiding.',
        how: 'Dit is het rendementsverschil, niet de volatiliteit ervan. Een portefeuille kan '
          + 'gelijk eindigen met de benchmark en onderweg toch heel anders bewegen. Dan is het '
          + 'actieve rendement nul, terwijl de tracking error hoog kan zijn.',
      },
      infoRatio: {
        what: 'Actief rendement per eenheid tracking error.',
        where: 'Actief rendement ÷ tracking error, beide geannualiseerd.',
        how: 'Dit toont hoeveel actief rendement de portefeuille behaalde per eenheid tracking '
          + 'error. Er wordt geen waarde getoond als de tracking error vrijwel nul is, omdat de '
          + 'ratio dan niet kan worden berekend.',
      },
      observations: {
        what: 'De T in de formule — de perioden die beide reeksen hadden.',
        where: 'De DOORSNEDE van beide kalenders, nooit een koppeling op positie.',
        how: 'Een notering in Stockholm en een in Londen verhandelde tracker delen geen feestdagen; '
          + 'ze op volgorde koppelen verschuift de reeksen vanaf de eerste afwijking en levert een '
          + 'geloofwaardig cijfer op dat tegen de verkeerde dagen is gemeten.',
      },
    },
    legend: {
      a: 'het portefeuillerendement min het benchmarkrendement in periode t',
      R: (book, bench) => `het eigen rendement van ${v(book)} en van ${v(bench)} in die periode, `
        + 'beide in EUR',
      aBar: 'het gemiddelde actieve rendement over de periode — de band hierboven ligt daaromheen, '
        + 'niet om nul',
      T: (n) => `het aantal gebruikte maandperioden (${v(n)})`,
      f: (n) => `perioden per jaar (${v(n)}), de annualiseringsfactor`,
      te: 'één standaarddeviatie van het actieve rendement, per jaar',
      teAnswer: 'het antwoord: één standaarddeviatie van het actieve rendement, per jaar',
      prod: 'vermenigvuldig de perioderendementen om ze over de tijd samen te stellen',
      Ra: 'het actieve rendement van de tegel hiernaast, geannualiseerd',
      IR: 'het antwoord: actief rendement per eenheid tracking error die daarvoor is genomen',
    },
    observationsWhat: (freq) => `De T in de formule — ${v(freq)} perioden die beide reeksen hadden.`,
    sleeve: (book, from, to) =>
      `De aandelenselectie van ${book} tegen de gekozen AIRS-wegingen, geprijsd van ${from} tot ${to} — `,
    note: 'niet de werkelijke historie van het boek, dus een naam die in maart is gekocht draagt '
      + 'hier zijn januarirendement bij. Het is dezelfde portefeuille die de Active share-weergave '
      + 'beschrijft.',
  },

  corr: {
    rhoVs: (b) => `Correlatie t.o.v. ${b}`, rSquared: 'R²', meanPair: 'Gemiddelde correlatie tussen eigen posities',
    activeVol: 'Actieve volatiliteit (= TE)',
    betweenPositions: 'Tussen de posities',
    legend: 'Blauw = tegengesteld, amber = beweegt mee. Gesorteerd op gewicht.',
    leastTitle: 'Laagst gecorreleerd — wat werkelijk spreidt',
    mostTitle: 'Hoogst gecorreleerd — één positie, twee keer gehouden',
    identityBroken: (pp) => ` De gemeten σₐ en de uit ρ afgeleide σₐ verschillen ${pp}pp — de twee `
      + 'reeksen zijn niet langer identiek. Dit is een fout in de software, geen marktfeit.',
    pairsMeasured: (measured, possible, minObs, holdings) =>
      `${v(measured)} van ${v(possible)} paren met minstens ${v(minObs)} overlappende rendementen, `
      + `over ${v(holdings)} posities.`,
    thinPairs: (n) => `Een paar met minder dan ${n} overlappende rendementen blijft leeg in plaats `
      + 'van gekleurd — over tien weken is een correlatie ruis met een teken, en een gekleurde cel '
      + 'oogt even gezaghebbend als een die over vijf jaar is gemeten.',
    cards: {
      rho: {
        what: 'Hoe nauw de aandelenselectie met de benchmark is meebewogen.',
        how: 'We vergelijken de wekelijkse rendementen in euro’s van de huidige aandelenposities met ACWI. '
          + 'Dicht bij +1 betekent samen bewegen, rond 0 weinig verband en dicht bij −1 tegengesteld bewegen. '
          + 'Obligaties en cash tellen niet mee.',
      },
      rSquared: {
        what: 'Het deel van de beweging van het boek dat de index verklaart.',
        where: 'Berekend uit de correlatie hierboven.',
        how: 'Een hogere waarde betekent dat de portefeuille meer met de index meebewoog.',
      },
      meanPair: {
        what: 'De gemiddelde correlatie tussen posities.',
        how: 'We vergelijken de wekelijkse rendementen van elk paar aandelen en nemen het gemiddelde. Elk paar telt even zwaar. Een hogere waarde betekent meer samen bewegen; een lagere waarde wijst op meer spreiding.',
      },
      activeVol: {
        what: 'Dezelfde tracking error die de weergave hiernaast rapporteert.',
        where: 'Uit dezelfde reeks opgebouwd, dus de identiteit hieronder is exact, niet bij '
          + 'benadering.',
        how: ' DE IDENTITEIT WORDT GETOOND, NIET BEWEERD. Het cijfer hierboven is rechtstreeks uit '
          + 'de actieve rendementen gemeten; de uitgewerkte regel bouwt het opnieuw op uit ρ. Ze '
          + 'komen tot op afrondingsruis overeen, en zouden zichtbaar uiteenlopen als de twee '
          + 'weergaven ooit niet meer dezelfde reeks zouden lezen.',
      },
    },
  },

  vol: {
    shownAnnualised: '· geannualiseerd weergegeven',
    rollingCalculation: 'Neem de laatste 12 maandrendementen in EUR, bereken de steekproefstandaardafwijking (met n − 1 = 11) en vermenigvuldig deze met √12 om te annualiseren. Vermenigvuldig met 100 voor de weergave als percentage. Elk punt in de grafiek herhaalt deze berekening voor de voorgaande 12 maanden.',
    rollingPortfolio: 'De portefeuillereeks combineert historische instrumentrendementen met de huidige positiewegingen. Dit is een reconstructie van de huidige posities, niet de werkelijk behaalde AIRS-rendementsreeks van de cliënt. Stortingen en onttrekkingen zijn uitgesloten.',
    rollingBenchmark: 'De benchmark gebruikt historische EUR-rendementen over dezelfde maandelijkse waarnemingen, met de benchmarkwegingen die in het label staan.',
    volatility: 'Volatiliteit (geann.)', downside: 'Neerwaartse deviatie',
    benchVol: (b) => `Volatiliteit ${b}`, worst: (p) => `Slechtste ${p}`,
    question: (portfolio, benchmark) => `Hoe sterk zouden de rendementen van ${portfolio} hebben geschommeld vergeleken met ${benchmark}?`,
    ret: 'Rendement (geann.)', sharpe: 'Sharpe', sortino: 'Sortino',
    periodDay: 'dag', periodWeek: 'week', periodMonth: 'maand',
    cards: {
      volatility: {
        what: 'De geannualiseerde variatie in maandelijkse portefeuillerendementen.',
        how: 'Dit gebruikt instrumentrendementen tegen de gekozen portefeuillewegingen. '
          + 'Stortingen en onttrekkingen maken geen deel uit van de reeks.',
      },
      downside: {
        what: 'De geannualiseerde variatie in negatieve maandrendementen.',
        how: 'Positieve maanden tellen als nul. Dit is de maatstaf voor neerwaarts risico die in '
          + 'de Sortino-ratio wordt gebruikt.',
      },
      benchVol: {
        what: 'De eigen volatiliteit van de index, over dezelfde perioden.',
        where: 'Dezelfde functie, dezelfde reeks, andere invoer.',
        how: 'Dezelfde dagelijkse berekening maakt een directe vergelijking met de portefeuille mogelijk.',
      },
      worst: {
        what: 'De slechtste afzonderlijke periode in het gemeten venster.',
        how: 'Dit is een werkelijk waargenomen maandverlies, geen geannualiseerde schatting.',
      },
      ret: {
        what: 'Het geannualiseerde portefeuillerendement over hetzelfde venster.',
        how: 'Dit rendement wordt gebruikt in de Sharpe- en Sortino-ratio ernaast. Het is het '
          + 'portefeuillerendement, niet het actieve rendement ten opzichte van de benchmark.',
      },
      sharpe: {
        what: 'Rendement per eenheid totale volatiliteit.',
        how: 'Een hogere waarde betekent meer rendement boven de risicovrije rente per eenheid '
          + 'volatiliteit.',
      },
      sortino: {
        what: 'Geannualiseerd rendement per eenheid neerwaarts risico.',
        where: 'Rendement ÷ neerwaartse deviatie, beide geannualiseerd.',
        how: 'Er wordt geen waarde getoond als geen enkel maandrendement onder de drempel viel, '
          + 'omdat er dan geen neerwaartse deviatie is om door te delen. Dat is een geldig '
          + 'resultaat, geen ontbrekende data.',
      },
    },
    note: 'Stortingen en onttrekkingen kunnen dit niet vertekenen — het is een gewogen mandje van '
      + 'instrumentrendementen, geen rekeningwaarde, dus er zitten geen kasstromen in de reeks om '
      + 'uit te filteren. De prijs daarvan is dat de gewichten die van vandaag zijn: dit is de '
      + 'volatiliteit van de portefeuille zoals die er nú staat, niet die welke de klant heeft '
      + 'meegemaakt.',
  },

  dd: {
    maxDrawdown: () => 'Maximale drawdown', benchMax: (b) => `Maximale drawdown ${b}`,
    today: 'Vandaag', episodes: (pct) => `Dalingen boven ${pct}%`,
    provenance: 'Gereconstrueerd met de huidige posities. Gebruik de AIRS-rendementen voor de '
      + 'werkelijke historische drawdown van de portefeuille.',
    threeWays: 'Dezelfde drawdown, op drie manieren gemeten',
    threeWaysNote: 'Grovere frequenties zien een daling die binnen de periode herstelt niet, dus '
      + 'vallen ze ondieper uit. Dagelijks is de eerlijke basis voor deze maatstaf.',
    worstInFull: 'Tijdlijn van de zwaarste drawdown',
    peak: 'Piek', trough: 'Laagste punt', recovered: 'Hersteld', peakToPeak: 'Totaal',
    stillUnderwater: 'nog niet hersteld',
    deepest: 'Overige drawdowns', colDepth: 'Diepte', colDecline: 'Daling (dagen)',
    colRecovery: 'Herstel (dagen)', open: 'open',
    unitDays: 'handelsdagen', unitWeeks: 'weken', unitMonths: 'maanden',
    note: (u) => `Alle looptijden gebruiken ${u}.`,
    cards: {
      maxDrawdown: {
        what: 'De diepste daling van piek naar dal binnen de periode.',
        how: 'De grootste daling vanaf een eerdere piek in een historische simulatie van de huidige portefeuille. '
          + 'We gebruiken dagrendementen en de huidige posities en gewichten.',
      },
      benchMax: {
        what: 'De diepste daling van de benchmark over dezelfde datums.',
        where: 'Dezelfde dagelijkse berekening toegepast op de tracker van de benchmark.',
        how: 'Dit maakt een directe vergelijking met de portefeuille mogelijk.',
      },
      today: {
        what: 'Hoe ver de selectie op dit moment onder haar eigen hoogste stand staat.',
        where: 'Bij 0% eindigde de periode op een nieuwe hoogste stand.',
        how: ' "Ooit −31%" en "nu 28% onder water" zijn heel verschillende gesprekken, en het '
          + 'tweede is het gesprek dat gevoerd wordt.',
      },
      episodes: {
        what: 'Het aantal afzonderlijke drawdowns dat de grens in het label overschreed.',
        where: 'Een drawdown begint onder een eerdere top en eindigt wanneer die top is hersteld.',
        how: 'Een drawdown eindigt pas wanneer de vorige top is hersteld. Een gedeeltelijk herstel '
          + 'en latere daling blijven deel van dezelfde episode.',
      },
    },
  },

  conc: {
    companyWeightExplanation: 'De waarde van deze onderneming als percentage van de totale portefeuille. Aandelenklassen van dezelfde onderneming worden samengevoegd.',
    effectiveCalculation: 'Zet het aandeel van elke onderneming in het aandelenmandje om naar een decimaal (10% = 0,10). Kwadrateer deze getallen, tel ze op en deel 1 door het totaal. Tien gelijkgewogen aandelen geven 10 effectieve posities.',
    effectiveMeaning: (count) => `Dit betekent dat het aandelenmandje even geconcentreerd is als ${count} gelijkgewogen aandelen.`,
    benchmarkScaleNote: '60% in plaats van 100% van dezelfde aandelenindex verandert dit aantal niet: de onderlinge bedrijfswegingen blijven gelijk. Obligaties en kasgeld tellen niet mee.',
    effective: 'Effectieve posities', ofIssuers: (n) => `van ${n} gehouden ondernemingen`,
    question: (portfolio, benchmark) => `Hoe geconcentreerd zijn ${portfolio} en ${benchmark}?`,
    top10: 'Top 10', ofBook: (p) => `${p} van het hele boek`,
    largest: 'Grootste positie', benchEffective: (b) => `Effectief ${b}`,
    benchSub: (n, t) => `van ${n} · top 10 ${t}`,
    tableTitle: 'Grootste ondernemingen, met het gewicht van de index in elk',
    colIssuer: 'Onderneming', colWeight: 'Gewicht', colCumulative: 'Cumulatief',
    coverage: (pct, b) => `${pct} van de leden van ${b} geprijsd — het ontbrekende gewicht wordt over `
      + 'de rest herverdeeld, waardoor de index iets geconcentreerder oogt dan hij is.',
    note: 'Samengevoegd per EMITTENT, niet per regel — twee aandelenklassen van één onderneming '
      + 'zijn één positie, en dat voorkomt dat de tien grootste door een identificatiecode worden '
      + 'bepaald.',
    unresolved: (n) => ` ${n} positie${n === 1 ? '' : 's'} kon niet aan een ondernemingsnaam worden `
      + 'gekoppeld en telt elk als eigen onderneming.',
    cards: {
      effective: {
        what: 'Naar hoeveel even grote posities dit boek zich gedraagt.',
        how: ' HET BETERE GETAL, en daarom staat het vooraan. Een grens bij precies tien is '
          + 'willekeurig — twee boeken met dezelfde C₁₀ kunnen een gelijkmatige tiennamenportefeuille '
          + 'zijn en een die door zijn top drie wordt gedomineerd. Dit kent geen afkapping. Veertig '
          + 'namen waarvan er vijf domineren komt ver onder veertig uit.',
      },
      top10: {
        what: 'Het deel van de aandelenselectie dat in de tien grootste ondernemingen zit.',
        how: ' TWEE NOEMERS, ALLEBEI WAAR. De kop gaat over de AANDELENSELECTIE, en dat is wat '
          + 'tussen boeken vergelijkbaar is; de regel eronder gaat over het hele boek inclusief '
          + 'liquiditeiten en fondsen. Een boek dat voor 30% uit liquiditeiten bestaat is in '
          + 'absolute zin werkelijk minder geconcentreerd.',
      },
      largest: {
        what: 'De grootste enkele onderneming, als aandeel van de selectie.',
        how: ' EEN GROTE POSITIE IS NIET AUTOMATISCH EEN GROTE KEUZE. Apple op 6% tegenover een '
          + 'index met 5% is een keuze van 1pp; dezelfde 6% in een naam die de index niet houdt is '
          + 'er een van 6pp. De tabel hieronder toont beide.',
      },
      benchEffective: {
        what: 'Het effectieve aantal posities van de index zelf, op dezelfde maatstaf.',
        how: ' Een naar marktkapitalisatie gewogen index is veel geconcentreerder dan zijn aantal '
          + 'leden doet vermoeden, dus dit is meestal een kleine fractie daarvan — en dat is de '
          + 'eerlijke vergelijking, niet het ruwe aantal.',
      },
    },
  },
};

export const RISK_COPY: Record<Lang, RiskCopy> = { en, nl };

/**
 * The Risk panel's copy in the reader's language.
 *
 *  A hook, not a `t('some.key')` LOOKUP — the key path is checked by the compiler this way, where
 * a string key is checked by nobody and fails at runtime as an empty cell. Same reason the tree is
 * nested: `t.vol.cards.downside` reads as the surface it belongs to, so one view cannot borrow
 * another's string by accident.
 */
export function useRiskCopy(): RiskCopy {
  const [lang] = useLang();
  return RISK_COPY[lang];
}
