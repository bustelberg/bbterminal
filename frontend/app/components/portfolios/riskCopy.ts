'use client';

import { useLang, type Lang } from '../../../lib/i18n';
import { v } from '../../../lib/dynamicValue';

/**
 * THE RISK PANEL'S COPY, IN BOTH LANGUAGES — the seven views behind the Analyse modal's Risk button.
 *
 * ⚠⚠ ENGLISH IS THE SOURCE, DUTCH IS THE TRANSLATION, AND A MISSING DUTCH STRING IS A COMPILE
 * ERROR. `nl` is typed as `RiskCopy`, so a key added to `en` and forgotten here fails `tsc` rather
 * than falling back to English — a half-translated panel renders as a rendering bug, and nobody
 * reports one of those as an unfinished translation. Same rule `tablesCopy` and `managementCopy`
 * already set.
 *
 * ⚠ ONE SURFACE, COMPLETE. `managementCopy`'s own header explains why the guarantee is per surface
 * rather than per page: the Analyse modal family is ~661 visible strings and requiring all of them
 * before any of them would be one change nobody can review. These seven views are a surface — they
 * are either fully Dutch or fully English, never half — and what is still English is listed in
 * `UNTRANSLATED_SURFACES`.
 *
 * ⚠⚠ THE ⓘ PROSE IS IN SCOPE HERE, WHICH REVERSES THE EARLIER DECISION (2026-08-21 had it out).
 * These cards are not decoration: they carry the definition, the convention chosen, and the caveat
 * that stops a figure being misread — a Dutch reader who has to switch languages to find out that
 * `ā` was subtracted has not been given the number, only its digits.
 *
 * ⚠ SOME TERMS STAY ENGLISH ON PURPOSE, and they are the exception rather than laziness. "Active
 * share", "tracking error", "drawdown", "Sharpe", "Sortino", "information ratio" and "HHI" are what
 * a Dutch wealth manager says out loud; "actief aandeel" is not a term, it is a translation of one.
 * Where a real Dutch word exists it is used — volatiliteit, correlatie, concentratie, rendement,
 * gewicht, positie, onderneming.
 *
 * ⚠ AIRS'S OWN FIELD NAMES ARE NOT IN HERE, same rule as `managementCopy`: `Beginwaarde`,
 * `Huidige waarde` are the SOURCE's column names and appear identically in both languages.
 */

/**
 * A ⓘ card: headline, where it comes from, over what window, and the caveat.
 *
 * ⚠ `how` IS OPTIONAL (2026-08-25). It used to be required, on the reasoning that every figure
 * owes the reader a caveat — but a required field gets filled whether or not there is anything to
 * say, and a card whose How restates its What in different words teaches nothing while making the
 * one card that DOES carry a warning look like more of the same. Where a measure is explained by
 * its own formula and legend, How is left off.
 */
type Card = { what: string; where?: string; when?: string; how?: string };

export type RiskCopy = {
  /** The switch. ⚠ SHORT — six of these sit on one row inside a fixed dialog. */
  views: {
    active: string; te: string; corr: string; vol: string; dd: string; conc: string;
  };
  /** The dialog heading per view, rendered as `<title> vs <benchmark>`. */
  titles: {
    active: string; te: string; corr: string; vol: string; dd: string; conc: string;
  };
  subtitle: string;
  close: string;

  common: {
    measured: string;
    daily: string; weekly: string; monthly: string;
    dailyNote: string;
    /** ⚠ THE NON-DAILY CADENCES' HOVER. `f` is an operand, not a word — a translation places it. */
    freqNote: (f: number) => string;
    /** ⚠ THE PRICE PROVENANCE, one sentence. The field and the vendor are operands — they come
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
    /** The footer row. ⚠ IT SAYS WHICH SET IT TOTALS — see the ⚠⚠ in `ActiveSharePanel`. */
    totalHeld: (n: number) => string;
    totalAll: (n: number) => string;
    totalCard: Card;
    totalCardHeld: Card;
    /**
     * ⚠ A SENTENCE, NOT `20 / 1678`. Two bare numbers over a slash is not a `Where` — it was
     * shortened to dodge a translation and the meaning went with it.
     *
     * ⚠⚠ AND IT NAMES BOTH VENDORS, because the two sides of this comparison come from different
     * ones — the book from an AIRS scan, the index's weights from yfinance market caps — and a
     * `Where` that counted the rows without saying where they came from answered half its own
     * question. ⚠ The labels come from `provenance.sourceLabel`, never typed here: a source is
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
     * ⚠ THE SYMBOLS THEMSELVES ARE NOT IN HERE. `w_i^{\,p}` is LaTeX, identical in every language,
     * and a translated copy of it is a second place for a superscript to go wrong. Only the prose
     * is translated; the call site owns the notation.
     */
    /**
     * ⚠ BOTH WEIGHT ROWS NAME THEIR SIDE, and neither says "the book". A legend defining `wᵖ` and
     * `wᵇ` is defining which of two weightings a symbol refers to, so "the book's weight" makes
     * the reader carry the mapping themselves — on a screen that already shows the book's name.
     * The names go in badges (`v`), like every other live value in these cards.
     */
    legend: {
      issuer: string; wp: (bookName: string) => string; wb: (bench: string) => string;
      /** ⚠ THE ROW THAT ANSWERS THE QUESTION THE OVERLAP TILE ACTUALLY PROVOKES — why the
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
     * ⚠⚠ IT TAKES ALREADY-FORMATTED DAYS (`asOfLine`) AND NULLS, and the null is the whole point:
     * the copy renders "no recorded date" rather than letting a missing stamp read as now. See the
     * ⚠⚠ on `asOfLine` — the string this replaced asserted "Today's weights", which is an
     * assumption printed as a fact.
     *
     * ⚠⚠ TWO LINES AND NO EXPLANATION OF WHY THERE ARE TWO. An earlier version spent a sentence
     * saying the book and the index are refreshed by different jobs, so judge for yourself — which
     * is what the two dated lines already say, at four times the length. A `When` is read at a
     * glance; prose in it is prose nobody finishes. ⚠ `\n` is a real line break here: the tooltip
     * panel carries `whitespace-pre-line`.
     *
     * ⚠ THE TWO PARENTHETICALS APPEAR ONLY WHEN THEY HAVE SOMETHING TO SAY — the read date only if
     * it differs from the valuation date, the undated count only if it is not zero. That is what
     * keeps the normal case to one date per line without dropping a fact when the fact exists.
     *
     * ⚠ DATES ARE NOT TRANSLATED. `2026-08-25` is the same in both languages; only the words are.
     */
    whenWeights: (bookName: string, book: string | null, read: string | null,
      bench: string, caps: string | null, unstamped: number) => string;
    /**
     * The BOOK's date alone — for a figure with no benchmark in it.
     *
     * ⚠ THE STOCKS TILE IS ABOUT THE BOOK AND NOTHING ELSE, so printing the index's cap range
     * beside it would date a side that does not appear in the number. Two cards, two Whens,
     * because they genuinely measure different things.
     */
    whenBook: (bookName: string, book: string | null, read: string | null) => string;
    cards: { activeShare: Card; overlap: Card; offBenchmark: Card; stocks: Card };
  };

  te: {
    trackingError: string; activeReturn: string; infoRatio: string; observations: string;
    /**
     * The ā ± TE interval, prepended to the TE card's `how` — see `activeBand`.
     *
     * ⚠ THE OPERANDS ARRIVE PRE-FORMATTED, SIGNS AND ALL (`signed2` in `TrackingErrorView`). A
     * translation places numbers, it never renders them: a `toFixed` on this side would be a
     * second rounding convention, and the band's ends would stop matching the worked line above
     * them in the same card.
     */
    /** ⚠ `bench` IS NULLABLE because the payload's is — `v()` renders the absence, and an
     *  empty-string fallback at the call site would print "against " with nothing after it. */
    bandReading: (lo: string, hi: string, centre: string,
                  bench: string | null | undefined) => string;
    cards: { te: Card; activeReturn: Card; infoRatio: Card; observations: Card };
    /**
     * ⚠⚠ FOUR CARDS SHARE `aₜ`, `T` AND `f`, WHICH IS WHY THE LEGEND IS ONE OBJECT AND NOT FOUR.
     * Written per tile the definitions drift on the first edit, and a view where `T` means "paired
     * periods" in one tooltip and "observations" in the next has taught the reader that the symbol
     * is decorative. It moved out of the view unchanged; the English is what was there.
     *
     * ⚠ THE "the answer:" PREFIX MARKS THE SYMBOL THE TILE ACTUALLY PRINTS, so a reader scanning a
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
    /** ⚠ THE FREQUENCY IS AN OPERAND, so the card names the cadence it actually measured.
     *  ⚠ NULLABLE, matching the payload: `v()` renders the absence, where a `?? ''` at the call
     *  site would print "The T in the formula —  periods" and read as a rendering fault. */
    observationsWhat: (freq: string | null | undefined) => string;
    /** The sentence under the tiles: the book, then the window, then `note`. */
    sleeve: (book: string, from: string, to: string) => string;
    note: string;
  };

  corr: {
    rhoVs: (bench: string) => string; rSquared: string; meanPair: string; activeVol: string;
    betweenPositions: string; legend: string;
    /** ⚠ THE COVERAGE SENTENCE, distinct from `thinPairs`: that one explains why a CELL is blank,
     *  this one says how much of the matrix was measurable at all. */
    pairsMeasured: (measured: string, possible: string, minObs: string, holdings: string) => string;
    leastTitle: string; mostTitle: string;
    identityBroken: (pp: string) => string;
    thinPairs: (n: number) => string;
    cards: { rho: Card; rSquared: Card; meanPair: Card; activeVol: Card };
  };

  vol: {
    /** ⚠ THE TOOLBAR'S TRAILING NOTE. Every figure in this view is annualised and the cadence
     *  buttons beside it choose the sampling, not the unit — which is the confusion it exists to
     *  head off. */
    shownAnnualised: string;
    volatility: string; downside: string; benchVol: (bench: string) => string;
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
    /** ⚠ BOTH OPERANDS ARE NULLABLE, matching the payload — `v()` renders the absence, where a
     *  `?? ''` at the call site would print "Max drawdown ()" and read as a rendering fault. */
    maxDrawdown: (freq: string | null | undefined) => string;
    benchMax: (bench: string | null | undefined) => string;
    today: string;
    /** ⚠ THE THRESHOLD IS AN OPERAND — it is configurable, so a hardcoded "5%" would be a label
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
    effective: string; ofIssuers: (n: number) => string;
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

const en: RiskCopy = {
  views: { active: 'Active share', te: 'Tracking error', corr: 'Correlation', vol: 'Volatility',
    dd: 'Drawdown', conc: 'Concentration' },
  titles: { active: 'Active share', te: 'Tracking error', corr: 'Correlation',
    vol: 'Volatility', dd: 'Max drawdown', conc: 'Concentration' },
  subtitle: 'The individual stocks only, taken as 100% of the portfolio.',
  close: 'close',

  common: {
    measured: 'Measured',
    daily: 'Daily', weekly: 'Weekly', monthly: 'Monthly',
    dailyNote: 'Daily closes are not synchronous — the tracker closes at 16:30 London, a US holding '
      + 'at 21:00 — which lowers the measured covariance.',
    freqNote: (f) => `f = ${f} observations per year.`,
    pricedFrom: (field, vendor) => `Prices from ${v(field)} at ${v(vendor)}.`,
    computing: 'Computing…',
    synthetic: (y, p, t) => `Today's stock sleeve at today's weights over ${y} years `
      + `(${p} of ${t} priced).`,
    observations: 'Observations',
  },

  active: {
    activeShare: 'Active share', overlap: 'Overlap', offBenchmark: 'Off-benchmark',
    stocks: 'Stocks',
    heldOnly: (n) => `What we hold (${n})`, everyName: (n) => `Every name (${n})`,
    colCompany: 'Company', colBook: 'Book', colActive: 'Active', notHeld: 'not held',
    heldVsIndex: (h, m, bookSrc, benchField, benchVendor) =>
      `${v(h)} companies and their weights from ${v(bookSrc)}, against ${v(m)} priced index `
      + `members weighted by ${v(benchField)} from ${v(benchVendor)}.`,
    offBenchWhere: (o, h) => `${v(o)} of the ${v(h)} companies held are not in the index.`,
    totalHeld: (n) => `Total — ${n} held`,
    totalAll: (n) => `Total — all ${n} names`,
    totalCard: {
      what: 'Both columns sum to 100%, so the Active column sums to exactly zero.',
      where: '½ · Σ |Active| is the active share in the tile above — the same number, from this '
        + 'table.',
      how: '⚠ THAT ZERO IS THE REASON FOR THE ½. Every overweight has a matching underweight by '
        + 'construction, so without halving it every difference would be counted twice.',
    },
    totalCardHeld: {
      what: 'The held names only, so the Active column does NOT sum to zero.',
      where: 'Book is 100% by construction; the benchmark column is what the index holds in these '
        + 'same names.',
      how: '⚠ THE TOTAL IS THE BOOK\'S WHOLE OVERWEIGHT, and it is carried, name for name, by the '
        + 'index constituents not shown here. Switch to every name to see it cancel. ⚠ ½ Σ |Active| '
        + 'over this subset is NOT the active share — half the sum is missing.',
    },
    coverage: (pct, b) => `Priced ${pct} of ${b}'s members. The missing weight is redistributed `
      + 'over the rest, so the active share reads slightly low.',
    unmatched: (n, pct, names) => `${n} holding${n === 1 ? '' : 's'} (${pct} of the sleeve) could `
      + `not be matched to a company name and count as fully active: ${names}`,
    legend: {
      // ⚠ IT SAYS WHAT `i` IS, then gives the case that makes the fold matter. The previous
      // version — "both of its share classes are ONE i, not two" — presupposed two share classes,
      // which almost no company has, and never said that `i` is a term in the sum above.
      issuer: 'one company, and one term in the sum — Alphabet A and Alphabet C are folded into a '
        + 'single i, not two',
      wp: (bookName) =>
        `${v(bookName)}'s weight in that company, over the stock sleeve renormalised to 100%`,
      wb: (bench) => `${v(bench)}'s own weight in the same company, by market cap`,
      min: 'the SMALLER of the two weights — where we hold less of a company than the index does, '
        + 'only our weight counts, which is why the benchmark column can sum to more than this',
      notInBench: (bench) => `read as "over the companies ${v(bench)} does not hold at all" — the `
        + 'sum runs over those alone',
      stocksNum: 'the weight in individual stocks with an ISIN we could match',
      stocksDen: 'the weight in everything the book holds, funds and cash and bonds included',
      absActive: 'the size of each bet regardless of direction — an overweight and an underweight '
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
        what: 'How much of the stock sleeve differs from the benchmark.',
      },
      overlap: {
        what: 'The share of the sleeve that IS the benchmark.',
        how: 'Exactly 100% − active share, by construction. The two are one number, so nothing '
          + 'is learned by reading both — they are printed together because each is the natural '
          + 'answer to a different question.',
      },
      offBenchmark: {
        what: 'Weight in companies the index does not hold at all.',
        how: 'Active share mixes two decisions — owning something the index does not, and sizing '
          + 'something it does. This is only the first, so a book can be highly active with this '
          + 'at zero.',
      },
      stocks: {
        what: 'How much of the whole book this comparison covers.',
        where: 'Funds, cash and bonds are excluded and the rest renormalised to 100%.',
        how: '⚠ THE FIGURES ABOVE DESCRIBE THIS SLICE, NOT THE BOOK. Leaving cash in at its real '
          + 'weight would count liquidity as an active bet against every index name at once — a '
          + 'defensible measure, but a different one.',
      },
    },
  },

  te: {
    trackingError: 'Tracking error (realised)', activeReturn: 'Active return (ann.)',
    infoRatio: 'Information ratio', observations: 'Observations',
    bandReading: (lo, hi, centre, bench) =>
      `A typical year lands ā ± TE — between ${v(lo)} and ${v(hi)} against ${v(bench)}, centred on the mean `
      + `active return of ${v(centre)} and NOT on zero. About two years in three; active returns are `
      + 'fatter-tailed than normal, so read it as a scale rather than a promise. ⚠ THE CENTRE IS '
      + 'THE ARITHMETIC mean annualised, so it sits a little above the geometric Active return '
      + 'tile beside it — the gap is roughly TE²/2, and a ±σ band is only coherent around the '
      + 'arithmetic one.',
    cards: {
      te: {
        what: "How much the book's return has diverged from the benchmark's, annualised.",
        how: '⚠ REALISED (ex-post), not the ex-ante forecast from a covariance matrix — those are '
          + 'different numbers and routinely disagree. ā IS subtracted and the divisor is T−1 '
          + '(Bessel); some providers do neither — that version is symmetric about the benchmark, '
          + 'and larger.',
      },
      activeReturn: {
        what: 'What the sleeve earned above or below the benchmark, per year.',
        where: 'The same active returns, compounded — not their spread.',
        how: '⚠ THIS IS THE QUANTITY THE TILE BESIDE IT MEASURES THE VOLATILITY OF. They are '
          + 'constantly confused: a book can wander a long way from its index and end up exactly '
          + 'level, which is a large tracking error and no active return.',
      },
      infoRatio: {
        what: 'Active return per unit of tracking error.',
        where: 'Active return ÷ tracking error, both annualised.',
        how: 'Whether the divergence was worth taking. ⚠ A dash means the tracking error is ~0 — '
          + 'there is no risk to divide by, not that the ratio is zero.',
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
      a: 'the active return in period t — what the sleeve did that period, minus what the tracker '
        + 'did',
      R: (book, bench) => `${v(book)}'s and ${v(bench)}'s own returns in that period, both in EUR`,
      aBar: 'the mean active return over the window — the band above is centred on it, not on zero',
      T: (n) => `the number of paired periods (${v(n)} here) — the intersection of the two calendars`,
      f: (n) => `periods per year (${v(n)}), the annualisation factor`,
      te: 'one standard deviation of the active return, per year',
      teAnswer: 'the answer: one standard deviation of the active return, per year',
      prod: 'the periods CHAINED, not averaged — this is what the gap compounded to, which is why '
        + 'it sits slightly below the arithmetic mean the band on the tracking-error tile is '
        + 'centred on',
      Ra: 'the active return from the tile beside this one, annualised',
      IR: 'the answer: active return per unit of the tracking error taken to earn it',
    },
    observationsWhat: (freq) => `The T in the formula — ${v(freq)} periods both series had.`,
    sleeve: (book, from, to) =>
      `${book}'s stock sleeve at its current weights, priced from ${from} to ${to} — `,
    note: "not the book's realised history, so a name bought in March contributes its January "
      + 'return. It is the same portfolio the Active share view describes.',
  },

  corr: {
    rhoVs: (b) => `ρ vs ${b}`, rSquared: 'R²', meanPair: 'Mean ρ between positions',
    activeVol: 'Active vol (= TE)',
    betweenPositions: 'Between the positions',
    legend: 'Blue = diverging, amber = moving together. Ordered by weight.',
    leastTitle: 'Least correlated — what actually diversifies',
    mostTitle: 'Most correlated — one bet held twice',
    identityBroken: (pp) => `⚠ σₐ measured and σₐ implied by ρ differ by ${pp}pp — the two series `
      + 'are no longer identical. This is a bug, not a market fact.',
    pairsMeasured: (measured, possible, minObs, holdings) =>
      `${v(measured)} of ${v(possible)} possible pairs — the ones with at least ${v(minObs)} `
      + `overlapping returns, across ${v(holdings)} holdings.`,
    thinPairs: (n) => `A pair with fewer than ${n} overlapping returns is left blank rather than `
      + 'tinted — over ten weeks a correlation is noise with a sign, and a coloured cell looks '
      + 'exactly as authoritative as one measured over five years.',
    cards: {
      rho: {
        what: 'How closely the stock sleeve has moved with the benchmark.',
        how: '⚠ THIS IS THE OTHER SIDE OF THE TRACKING ERROR, not a separate fact: '
          + 'σₐ² = σₚ² + σᵦ² − 2ρσₚσᵦ. The lower this is, the more the book can diverge.',
      },
      rSquared: {
        what: "The share of the book's movement the index explains.",
        where: 'ρ², nothing more.',
        how: 'ρ = 0.90 and "81% of the movement" are the same fact and land very differently, '
          + 'which is why both are on screen.',
      },
      meanPair: {
        what: 'How alike the holdings are to each other — the diversification check.',
        how: '⚠ UNWEIGHTED ON PURPOSE. It asks whether these NAMES are alike, which is a question '
          + 'about the selection; weighting by position size would answer a different one and make '
          + 'a concentrated book look better diversified.',
      },
      activeVol: {
        what: 'The same tracking error the view beside this one reports.',
        where: 'Built from the same series, so the identity below is exact, not approximate.',
        how: '⚠ THE IDENTITY IS SHOWN, NOT ASSERTED. The figure above is measured directly from '
          + 'the active returns; the worked line rebuilds it from ρ. They agree to floating-point '
          + 'noise, and would visibly diverge if the two views ever stopped reading the same series.',
      },
    },
  },

  vol: {
    shownAnnualised: '· shown annualised',
    volatility: 'Volatility (ann.)', downside: 'Downside deviation',
    benchVol: (b) => `${b} volatility`, worst: (p) => `Worst ${p}`,
    ret: 'Return (ann.)', sharpe: 'Sharpe', sortino: 'Sortino',
    periodDay: 'day', periodWeek: 'week', periodMonth: 'month',
    cards: {
      volatility: {
        what: "How much the sleeve's own return has varied, annualised.",
        how: '⚠⚠ NO CASH FLOWS IN IT, and not because they were chain-linked out — this is a '
          + 'weighted basket of instrument price returns, not an account value, so a deposit or '
          + 'withdrawal is simply not in the series. ⚠ Same σₚ the Correlation view uses in '
          + 'σₐ² = σₚ² + σᵦ² − 2ρσₚσᵦ.',
      },
      downside: {
        what: 'The same spread, counting only the periods that lost money.',
        how: "⚠ SORTINO'S CONVENTION, not the semi-deviation (below-MEAN observations only, "
          + 'divided by how many there are), which reads higher. Both are called "downside '
          + 'deviation"; this is the one the Sortino below is built on. Volatility punishes a good '
          + 'month exactly as hard as a bad one; this does not, which is closer to how the loss is '
          + 'actually experienced.',
      },
      benchVol: {
        what: "The index's own volatility, on the same periods.",
        where: 'Same function, same series, different input.',
        how: '⚠ FOR SCALE, NOT AS A VERDICT. A sleeve more volatile than its index is not by '
          + 'itself worse — that difference is what the Active share and Tracking error views are '
          + 'about.',
      },
      worst: {
        what: 'The single worst period in the window.',
        how: '⚠ NOBODY HAS EVER EXPERIENCED "18% ANNUALISED VOLATILITY". They have experienced the '
          + 'worst week. For a fat-tailed book the two are far apart, which is exactly when σ on '
          + 'its own misleads.',
      },
      ret: {
        what: 'What the sleeve compounded at over the same window.',
        how: 'Here so the two ratios beside it can be checked — a risk number without the return '
          + 'it bought is half a sentence.',
      },
      sharpe: {
        what: 'Return per unit of total volatility.',
        how: '⚠ THE RISK-FREE RATE IS STATED because a Sharpe quoted without it is not comparable '
          + "with anybody else's, and at current rates that is not cosmetic.",
      },
      sortino: {
        what: 'The same ratio, over downside deviation instead of total volatility.',
        where: 'Return ÷ downside deviation, both annualised.',
        how: '⚠ A DASH MEANS NOTHING EVER FELL BELOW THE TARGET — there is no downside to divide '
          + 'by. That is a measurement, not a missing number.',
      },
    },
    note: 'Deposits and withdrawals cannot distort this — it is a weighted basket of instrument '
      + 'returns, not an account value, so there are no flows in the series to strip out. The cost '
      + "is that the weights are today's: this is the volatility of the portfolio as it stands, "
      + 'not the one the client lived through.',
  },

  dd: {
    maxDrawdown: (f) => `Max drawdown (${f})`, benchMax: (b) => `${b} max drawdown`,
    today: 'Today', episodes: (pct) => `Falls over ${pct}%`,
    provenance: "⚠ Reconstructed from today's holdings, not the client's realised experience. "
      + "Names since sold are absent and today's weights were chosen with hindsight, so this reads "
      + "shallower than what was actually lived through. The client's own figure comes from the "
      + 'AIRS returns.',
    threeWays: 'The same drawdown, measured three ways',
    threeWaysNote: 'Coarser cadences cannot see a fall that recovers inside the period, so they '
      + 'read shallower. Daily is the honest basis for this measure.',
    worstInFull: 'The worst one, in full',
    peak: 'Peak', trough: 'Trough', recovered: 'Recovered', peakToPeak: 'Peak to peak',
    stillUnderwater: 'still underwater',
    deepest: 'Deepest falls', colDepth: 'Depth', colDecline: 'Decline', colRecovery: 'Recovery',
    open: 'open',
    unitDays: 'trading days', unitWeeks: 'weeks', unitMonths: 'months',
    note: (u) => `Durations are in ${u} of the selected cadence, not calendar days.`,
    cards: {
      maxDrawdown: {
        what: 'The deepest peak-to-trough fall in the window.',
        how: '⚠ THE CADENCE IS IN THE LABEL because it changes the answer: a fall that recovers '
          + 'inside a week is invisible to a weekly series. See the comparison below.',
      },
      benchMax: {
        what: "The index's own deepest fall, over the same periods.",
        where: 'Same formula, same window, different series.',
        how: "For scale. ⚠ It carries none of this book's survivorship bias — the index kept its "
          + 'fallers — so the gap between the two flatters the book.',
      },
      today: {
        what: 'How far below its own high water mark the sleeve sits right now.',
        where: 'The window ended at a new high when this reads 0%.',
        how: '⚠ "Worst ever −31%" and "down 28% right now" are very different conversations, and '
          + 'the second is the one being had.',
      },
      episodes: {
        what: 'Distinct peak-to-trough episodes in the window.',
        where: 'An episode ends only when the previous high is regained.',
        how: '⚠ ONE NUMBER HIDES WHETHER IT WAS A PATTERN OR AN EVENT. One −30% and four −25%s '
          + 'share a maximum and are not the same risk. ⚠ A 40% fall that bounces 5% and falls '
          + 'further is ONE drawdown, not two — splitting on direction would report shallow dips '
          + 'and no crash.',
      },
    },
  },

  conc: {
    effective: 'Effective positions', ofIssuers: (n) => `of ${n} companies held`,
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
        how: '⚠ THE BETTER NUMBER, and the reason it leads. A cut at exactly ten is arbitrary — '
          + 'two books with the same C₁₀ can be an even ten-name portfolio and one dominated by '
          + 'its top three. This has no cut-off. Forty names of which five dominate reads far '
          + 'below forty.',
      },
      top10: {
        what: 'The share of the stock sleeve in its ten largest companies.',
        how: '⚠⚠ TWO DENOMINATORS, BOTH TRUE. The headline is of the STOCK SLEEVE, which is what '
          + 'compares across books; the line beneath is of the whole book including cash and '
          + 'funds. A book that is 30% cash really is less concentrated in absolute terms.',
      },
      largest: {
        what: 'The single biggest company, as a share of the sleeve.',
        how: '⚠ A BIG POSITION IS NOT AUTOMATICALLY A BIG BET. Apple at 6% against an index '
          + 'holding 5% is a 1pp bet; the same 6% in a name the index does not hold is a 6pp one. '
          + 'The table below carries both.',
      },
      benchEffective: {
        what: "The index's own effective position count, on the same measure.",
        how: '⚠ A cap-weighted index is far more concentrated than its member count suggests, so '
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
  subtitle: 'Alleen de individuele aandelen, genomen als 100% van de portefeuille.',
  close: 'sluiten',

  common: {
    measured: 'Gemeten',
    daily: 'Dagelijks', weekly: 'Wekelijks', monthly: 'Maandelijks',
    dailyNote: 'Dagslotkoersen lopen niet gelijk — de tracker sluit om 16:30 Londen, een Amerikaanse '
      + 'positie om 21:00 — waardoor de gemeten covariantie lager uitvalt.',
    freqNote: (f) => `f = ${f} waarnemingen per jaar.`,
    pricedFrom: (field, vendor) => `Koersen uit ${v(field)} bij ${v(vendor)}.`,
    computing: 'Berekenen…',
    synthetic: (y, p, t) => `De huidige aandelenselectie tegen de huidige gewichten over ${y} jaar `
      + `(${p} van ${t} geprijsd).`,
    observations: 'Waarnemingen',
  },

  active: {
    activeShare: 'Active share', overlap: 'Overlap', offBenchmark: 'Buiten de benchmark',
    stocks: 'Aandelen',
    heldOnly: (n) => `Wat we houden (${n})`, everyName: (n) => `Alle namen (${n})`,
    colCompany: 'Onderneming', colBook: 'Boek', colActive: 'Actief', notHeld: 'niet gehouden',
    heldVsIndex: (h, m, bookSrc, benchField, benchVendor) =>
      `${v(h)} ondernemingen en hun gewichten uit ${v(bookSrc)}, tegenover ${v(m)} geprijsde `
      + `indexleden gewogen naar ${v(benchField)} van ${v(benchVendor)}.`,
    offBenchWhere: (o, h) => `${v(o)} van de ${v(h)} gehouden ondernemingen zitten niet in de index.`,
    totalHeld: (n) => `Totaal — ${n} gehouden`,
    totalAll: (n) => `Totaal — alle ${n} namen`,
    totalCard: {
      what: 'Beide kolommen tellen op tot 100%, dus de kolom Actief telt op tot precies nul.',
      where: '½ · Σ |Actief| is de active share in de tegel hierboven — hetzelfde getal, uit deze '
        + 'tabel.',
      how: '⚠ DIE NUL IS DE REDEN VOOR DE ½. Elke overweging heeft per constructie een even grote '
        + 'onderweging, dus zonder halveren zou elk verschil dubbel worden geteld.',
    },
    totalCardHeld: {
      what: 'Alleen de gehouden namen, dus de kolom Actief telt niet op tot nul.',
      where: 'Boek is per constructie 100%; de benchmarkkolom is wat de index in diezelfde namen '
        + 'houdt.',
      how: '⚠ HET TOTAAL IS DE VOLLEDIGE OVERWEGING VAN HET BOEK, en die wordt naam voor naam '
        + 'gedragen door de indexposities die hier niet staan. Schakel naar alle namen om het te '
        + 'zien wegvallen. ⚠ ½ Σ |Actief| over deze deelverzameling is NIET de active share — de '
        + 'helft van de som ontbreekt.',
    },
    coverage: (pct, b) => `${pct} van de leden van ${b} geprijsd. Het ontbrekende gewicht wordt over `
      + 'de rest herverdeeld, waardoor de active share iets te laag uitvalt.',
    unmatched: (n, pct, names) => `${n} positie${n === 1 ? '' : 's'} (${pct} van de selectie) kon `
      + `niet aan een ondernemingsnaam worden gekoppeld en telt volledig als actief: ${names}`,
    legend: {
      issuer: 'één onderneming, en één term in de som — Alphabet A en Alphabet C worden tot één i '
        + 'samengevoegd, niet twee',
      wp: (bookName) => `het gewicht van ${v(bookName)} in die onderneming, over de tot 100% `
        + 'geherwogen aandelenselectie',
      wb: (bench) => `het gewicht van ${v(bench)} zelf in diezelfde onderneming, naar marktkapitalisatie`,
      min: 'het KLEINSTE van de twee gewichten — houden we minder van een onderneming dan de index, '
        + 'dan telt alleen ons gewicht mee; daarom kan de benchmarkkolom hoger uitkomen dan dit',
      notInBench: (bench) => `te lezen als "over de ondernemingen die ${v(bench)} helemaal niet `
        + 'houdt" — de som loopt alleen over die',
      stocksNum: 'het gewicht in individuele aandelen met een ISIN die we konden koppelen',
      stocksDen: 'het gewicht in alles wat het boek houdt, inclusief fondsen, liquiditeiten en obligaties',
      absActive: 'de omvang van elke positie ongeacht de richting — een over- en een onderweging '
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
        what: 'Hoeveel van de aandelenselectie afwijkt van de benchmark.',
      },
      overlap: {
        what: 'Het deel van de selectie dat de benchmark WEL is.',
        how: 'Per definitie exact 100% − active share. De twee zijn één getal, dus beide lezen '
          + 'levert niets extra op — ze staan samen omdat elk het natuurlijke antwoord is op een '
          + 'andere vraag.',
      },
      offBenchmark: {
        what: 'Gewicht in ondernemingen die de index helemaal niet houdt.',
        how: 'Active share mengt twee beslissingen — iets bezitten dat de index niet heeft, en iets '
          + 'zwaarder of lichter wegen dat hij wél heeft. Dit is alleen de eerste, dus een boek kan '
          + 'zeer actief zijn terwijl dit nul is.',
      },
      stocks: {
        what: 'Welk deel van het hele boek deze vergelijking beslaat.',
        where: 'Fondsen, liquiditeiten en obligaties vallen eruit; de rest wordt geherweegd naar 100%.',
        how: '⚠ DE CIJFERS HIERBOVEN BESCHRIJVEN DIT DEEL, NIET HET BOEK. Liquiditeiten op hun '
          + 'werkelijke gewicht laten staan zou liquiditeit als actieve positie tegen elke '
          + 'indexnaam tegelijk tellen — verdedigbaar, maar een andere maatstaf.',
      },
    },
  },

  te: {
    trackingError: 'Tracking error (gerealiseerd)', activeReturn: 'Actief rendement (geann.)',
    infoRatio: 'Information ratio', observations: 'Waarnemingen',
    bandReading: (lo, hi, centre, bench) =>
      `Een doorsnee jaar landt op ā ± TE — tussen ${v(lo)} en ${v(hi)} ten opzichte van ${v(bench)}, met `
      + `als midden het gemiddelde actieve rendement van ${v(centre)} en dus NIET nul. Ongeveer twee `
      + 'op de drie jaren; actieve rendementen hebben dikkere staarten dan een normale verdeling, '
      + 'dus lees het als een orde van grootte en niet als een belofte. ⚠ HET MIDDEN IS HET '
      + 'REKENKUNDIG gemiddelde op jaarbasis en ligt daarmee iets boven de geometrische tegel '
      + 'Actief rendement ernaast — het verschil is ruwweg TE²/2, en een ±σ-band is alleen '
      + 'consistent rond het rekenkundige gemiddelde.',
    cards: {
      te: {
        what: 'Hoeveel het rendement van het boek is afgeweken van dat van de benchmark, '
          + 'geannualiseerd.',
        how: '⚠ GEREALISEERD (ex-post), niet de ex-ante voorspelling uit een covariantiematrix — '
          + 'dat zijn verschillende getallen en ze lopen structureel uiteen. ā WORDT afgetrokken en '
          + 'de noemer is T−1 (Bessel); sommige aanbieders doen geen van beide — die versie is '
          + 'symmetrisch rond de benchmark, en groter.',
      },
      activeReturn: {
        what: 'Wat de selectie boven of onder de benchmark verdiende, per jaar.',
        where: 'Dezelfde actieve rendementen, samengesteld — niet hun spreiding.',
        how: '⚠ DIT IS DE GROOTHEID WAARVAN DE TEGEL ERNAAST DE VOLATILITEIT MEET. Ze worden '
          + 'voortdurend verward: een boek kan ver van zijn index afdwalen en precies gelijk '
          + 'eindigen — een grote tracking error en geen actief rendement.',
      },
      infoRatio: {
        what: 'Actief rendement per eenheid tracking error.',
        where: 'Actief rendement ÷ tracking error, beide geannualiseerd.',
        how: 'Of de afwijking het waard was. ⚠ Een streepje betekent dat de tracking error ~0 is — '
          + 'er is geen risico om door te delen, niet dat de ratio nul is.',
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
      a: 'het actieve rendement in periode t — wat de selectie die periode deed, min wat de tracker '
        + 'deed',
      R: (book, bench) => `het eigen rendement van ${v(book)} en van ${v(bench)} in die periode, `
        + 'beide in EUR',
      aBar: 'het gemiddelde actieve rendement over het venster — de band hierboven ligt daaromheen, '
        + 'niet om nul',
      T: (n) => `het aantal gepaarde perioden (${v(n)} hier) — de doorsnede van de twee kalenders`,
      f: (n) => `perioden per jaar (${v(n)}), de annualiseringsfactor`,
      te: 'één standaarddeviatie van het actieve rendement, per jaar',
      teAnswer: 'het antwoord: één standaarddeviatie van het actieve rendement, per jaar',
      prod: 'de perioden GEKETEND, niet gemiddeld — dit is waar het verschil naartoe is '
        + 'samengesteld, en daarom ligt het iets onder het rekenkundig gemiddelde waaromheen de '
        + 'band op de tracking-errortegel is gecentreerd',
      Ra: 'het actieve rendement van de tegel hiernaast, geannualiseerd',
      IR: 'het antwoord: actief rendement per eenheid tracking error die daarvoor is genomen',
    },
    observationsWhat: (freq) => `De T in de formule — ${v(freq)} perioden die beide reeksen hadden.`,
    sleeve: (book, from, to) =>
      `De aandelenselectie van ${book} tegen de huidige gewichten, geprijsd van ${from} tot ${to} — `,
    note: 'niet de werkelijke historie van het boek, dus een naam die in maart is gekocht draagt '
      + 'hier zijn januarirendement bij. Het is dezelfde portefeuille die de Active share-weergave '
      + 'beschrijft.',
  },

  corr: {
    rhoVs: (b) => `ρ t.o.v. ${b}`, rSquared: 'R²', meanPair: 'Gemiddelde ρ tussen posities',
    activeVol: 'Actieve volatiliteit (= TE)',
    betweenPositions: 'Tussen de posities',
    legend: 'Blauw = tegengesteld, amber = beweegt mee. Gesorteerd op gewicht.',
    leastTitle: 'Laagst gecorreleerd — wat werkelijk spreidt',
    mostTitle: 'Hoogst gecorreleerd — één positie, twee keer gehouden',
    identityBroken: (pp) => `⚠ De gemeten σₐ en de uit ρ afgeleide σₐ verschillen ${pp}pp — de twee `
      + 'reeksen zijn niet langer identiek. Dit is een fout in de software, geen marktfeit.',
    pairsMeasured: (measured, possible, minObs, holdings) =>
      `${v(measured)} van ${v(possible)} mogelijke paren — die met minstens ${v(minObs)} `
      + `overlappende rendementen, over ${v(holdings)} posities.`,
    thinPairs: (n) => `Een paar met minder dan ${n} overlappende rendementen blijft leeg in plaats `
      + 'van gekleurd — over tien weken is een correlatie ruis met een teken, en een gekleurde cel '
      + 'oogt even gezaghebbend als een die over vijf jaar is gemeten.',
    cards: {
      rho: {
        what: 'Hoe nauw de aandelenselectie met de benchmark is meebewogen.',
        how: '⚠ DIT IS DE ANDERE KANT VAN DE TRACKING ERROR, geen los feit: '
          + 'σₐ² = σₚ² + σᵦ² − 2ρσₚσᵦ. Hoe lager dit is, hoe verder het boek kan afwijken.',
      },
      rSquared: {
        what: 'Het deel van de beweging van het boek dat de index verklaart.',
        where: 'ρ², meer niet.',
        how: 'ρ = 0,90 en "81% van de beweging" zijn hetzelfde feit en komen heel verschillend aan; '
          + 'daarom staan ze allebei op het scherm.',
      },
      meanPair: {
        what: 'Hoezeer de posities op elkaar lijken — de spreidingstoets.',
        how: '⚠ BEWUST ONGEWOGEN. De vraag is of deze NAMEN op elkaar lijken, en dat gaat over de '
          + 'selectie; wegen naar positiegrootte beantwoordt een andere vraag en laat een '
          + 'geconcentreerd boek beter gespreid lijken dan het is.',
      },
      activeVol: {
        what: 'Dezelfde tracking error die de weergave hiernaast rapporteert.',
        where: 'Uit dezelfde reeks opgebouwd, dus de identiteit hieronder is exact, niet bij '
          + 'benadering.',
        how: '⚠ DE IDENTITEIT WORDT GETOOND, NIET BEWEERD. Het cijfer hierboven is rechtstreeks uit '
          + 'de actieve rendementen gemeten; de uitgewerkte regel bouwt het opnieuw op uit ρ. Ze '
          + 'komen tot op afrondingsruis overeen, en zouden zichtbaar uiteenlopen als de twee '
          + 'weergaven ooit niet meer dezelfde reeks zouden lezen.',
      },
    },
  },

  vol: {
    shownAnnualised: '· geannualiseerd weergegeven',
    volatility: 'Volatiliteit (geann.)', downside: 'Neerwaartse deviatie',
    benchVol: (b) => `Volatiliteit ${b}`, worst: (p) => `Slechtste ${p}`,
    ret: 'Rendement (geann.)', sharpe: 'Sharpe', sortino: 'Sortino',
    periodDay: 'dag', periodWeek: 'week', periodMonth: 'maand',
    cards: {
      volatility: {
        what: 'Hoezeer het eigen rendement van de selectie heeft gevarieerd, geannualiseerd.',
        how: '⚠⚠ ER ZITTEN GEEN KASSTROMEN IN, en niet omdat ze eruit zijn geketend — dit is een '
          + 'gewogen mandje van koersrendementen van instrumenten, geen rekeningwaarde, dus een '
          + 'storting of onttrekking zit er eenvoudigweg niet in. ⚠ Dezelfde σₚ die de '
          + 'Correlatie-weergave gebruikt in σₐ² = σₚ² + σᵦ² − 2ρσₚσᵦ.',
      },
      downside: {
        what: 'Dezelfde spreiding, maar alleen over de perioden met verlies.',
        how: '⚠ DE CONVENTIE VAN SORTINO, niet de semi-deviatie (alleen waarnemingen onder het '
          + 'GEMIDDELDE, gedeeld door hun aantal), die hoger uitkomt. Beide heten "neerwaartse '
          + 'deviatie"; dit is degene waarop de Sortino hieronder is gebouwd. Volatiliteit straft '
          + 'een goede maand even hard af als een slechte; dit niet, en dat sluit dichter aan bij '
          + 'hoe het verlies werkelijk wordt ervaren.',
      },
      benchVol: {
        what: 'De eigen volatiliteit van de index, over dezelfde perioden.',
        where: 'Dezelfde functie, dezelfde reeks, andere invoer.',
        how: '⚠ TER VERGELIJKING, NIET ALS OORDEEL. Een selectie die volatieler is dan haar index '
          + 'is daarmee niet slechter — dat verschil is precies waar Active share en Tracking error '
          + 'over gaan.',
      },
      worst: {
        what: 'De slechtste enkele periode binnen het venster.',
        how: '⚠ NIEMAND HEEFT OOIT "18% GEANNUALISEERDE VOLATILITEIT" MEEGEMAAKT. Men heeft de '
          + 'slechtste week meegemaakt. Bij een boek met dikke staarten liggen die twee ver uiteen, '
          + 'en juist dan misleidt σ op zichzelf.',
      },
      ret: {
        what: 'Waartegen de selectie over hetzelfde venster is samengesteld.',
        how: 'Staat hier zodat de twee ratio\'s ernaast te controleren zijn — een risicogetal '
          + 'zonder het rendement dat het opleverde is een halve zin.',
      },
      sharpe: {
        what: 'Rendement per eenheid totale volatiliteit.',
        how: '⚠ DE RISICOVRIJE VOET WORDT VERMELD, want een Sharpe zonder die voet is niet '
          + 'vergelijkbaar met die van anderen, en bij de huidige rente is dat verschil niet '
          + 'cosmetisch.',
      },
      sortino: {
        what: 'Dezelfde ratio, maar over de neerwaartse deviatie in plaats van de totale '
          + 'volatiliteit.',
        where: 'Rendement ÷ neerwaartse deviatie, beide geannualiseerd.',
        how: '⚠ EEN STREEPJE BETEKENT DAT NIETS OOIT ONDER DE DREMPEL IS GEKOMEN — er is geen '
          + 'neerwaarts risico om door te delen. Dat is een meting, geen ontbrekend cijfer.',
      },
    },
    note: 'Stortingen en onttrekkingen kunnen dit niet vertekenen — het is een gewogen mandje van '
      + 'instrumentrendementen, geen rekeningwaarde, dus er zitten geen kasstromen in de reeks om '
      + 'uit te filteren. De prijs daarvan is dat de gewichten die van vandaag zijn: dit is de '
      + 'volatiliteit van de portefeuille zoals die er nú staat, niet die welke de klant heeft '
      + 'meegemaakt.',
  },

  dd: {
    maxDrawdown: (f) => `Maximale drawdown (${f})`, benchMax: (b) => `Maximale drawdown ${b}`,
    today: 'Vandaag', episodes: (pct) => `Dalingen boven ${pct}%`,
    provenance: '⚠ Gereconstrueerd uit de huidige posities, niet de werkelijke ervaring van de '
      + 'klant. Namen die inmiddels zijn verkocht ontbreken en de huidige gewichten zijn met kennis '
      + 'achteraf gekozen, dus dit valt ondieper uit dan wat werkelijk is meegemaakt. Het eigen '
      + 'cijfer van de klant komt uit de AIRS-rendementen.',
    threeWays: 'Dezelfde drawdown, op drie manieren gemeten',
    threeWaysNote: 'Grovere frequenties zien een daling die binnen de periode herstelt niet, dus '
      + 'vallen ze ondieper uit. Dagelijks is de eerlijke basis voor deze maatstaf.',
    worstInFull: 'De zwaarste, volledig',
    peak: 'Piek', trough: 'Dieptepunt', recovered: 'Hersteld', peakToPeak: 'Piek tot piek',
    stillUnderwater: 'nog niet hersteld',
    deepest: 'Zwaarste dalingen', colDepth: 'Diepte', colDecline: 'Daling',
    colRecovery: 'Herstel', open: 'open',
    unitDays: 'handelsdagen', unitWeeks: 'weken', unitMonths: 'maanden',
    note: (u) => `Looptijden zijn in ${u} van de gekozen frequentie, niet in kalenderdagen.`,
    cards: {
      maxDrawdown: {
        what: 'De diepste daling van piek naar dal binnen het venster.',
        how: '⚠ DE FREQUENTIE STAAT IN HET LABEL omdat zij het antwoord verandert: een daling die '
          + 'binnen een week herstelt is onzichtbaar voor een weekreeks. Zie de vergelijking '
          + 'hieronder.',
      },
      benchMax: {
        what: 'De diepste daling van de index zelf, over dezelfde perioden.',
        where: 'Dezelfde formule, hetzelfde venster, een andere reeks.',
        how: 'Ter vergelijking. ⚠ De index draagt geen survivorship bias van dit boek — hij hield '
          + 'zijn dalers — dus het verschil tussen beide vleit het boek.',
      },
      today: {
        what: 'Hoe ver de selectie op dit moment onder haar eigen hoogste stand staat.',
        where: 'Bij 0% eindigde het venster op een nieuwe hoogste stand.',
        how: '⚠ "Ooit −31%" en "nu 28% onder water" zijn heel verschillende gesprekken, en het '
          + 'tweede is het gesprek dat gevoerd wordt.',
      },
      episodes: {
        what: 'Afzonderlijke episodes van piek naar dal binnen het venster.',
        where: 'Een episode eindigt pas wanneer de vorige top weer is bereikt.',
        how: '⚠ ÉÉN GETAL VERBERGT OF HET EEN PATROON WAS OF EEN GEBEURTENIS. Eén −30% en vier '
          + '−25% delen hetzelfde maximum en zijn niet hetzelfde risico. ⚠ Een daling van 40% die '
          + '5% opveert en verder zakt is ÉÉN drawdown, geen twee — splitsen op richting zou losse '
          + 'ondiepe dipjes rapporteren en geen crash.',
      },
    },
  },

  conc: {
    effective: 'Effectieve posities', ofIssuers: (n) => `van ${n} gehouden ondernemingen`,
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
        how: '⚠ HET BETERE GETAL, en daarom staat het vooraan. Een grens bij precies tien is '
          + 'willekeurig — twee boeken met dezelfde C₁₀ kunnen een gelijkmatige tiennamenportefeuille '
          + 'zijn en een die door zijn top drie wordt gedomineerd. Dit kent geen afkapping. Veertig '
          + 'namen waarvan er vijf domineren komt ver onder veertig uit.',
      },
      top10: {
        what: 'Het deel van de aandelenselectie dat in de tien grootste ondernemingen zit.',
        how: '⚠⚠ TWEE NOEMERS, ALLEBEI WAAR. De kop gaat over de AANDELENSELECTIE, en dat is wat '
          + 'tussen boeken vergelijkbaar is; de regel eronder gaat over het hele boek inclusief '
          + 'liquiditeiten en fondsen. Een boek dat voor 30% uit liquiditeiten bestaat is in '
          + 'absolute zin werkelijk minder geconcentreerd.',
      },
      largest: {
        what: 'De grootste enkele onderneming, als aandeel van de selectie.',
        how: '⚠ EEN GROTE POSITIE IS NIET AUTOMATISCH EEN GROTE KEUZE. Apple op 6% tegenover een '
          + 'index met 5% is een keuze van 1pp; dezelfde 6% in een naam die de index niet houdt is '
          + 'er een van 6pp. De tabel hieronder toont beide.',
      },
      benchEffective: {
        what: 'Het effectieve aantal posities van de index zelf, op dezelfde maatstaf.',
        how: '⚠ Een naar marktkapitalisatie gewogen index is veel geconcentreerder dan zijn aantal '
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
 * ⚠ A HOOK, NOT A `t('some.key')` LOOKUP — the key path is checked by the compiler this way, where
 * a string key is checked by nobody and fails at runtime as an empty cell. Same reason the tree is
 * nested: `t.vol.cards.downside` reads as the surface it belongs to, so one view cannot borrow
 * another's string by accident.
 */
export function useRiskCopy(): RiskCopy {
  const [lang] = useLang();
  return RISK_COPY[lang];
}
