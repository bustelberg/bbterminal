'use client';

import { useLang, type Lang } from '../../../lib/i18n';

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
 * gewicht, positie, emittent.
 *
 * ⚠ AIRS'S OWN FIELD NAMES ARE NOT IN HERE, same rule as `managementCopy`: `Beginwaarde`,
 * `Huidige waarde` are the SOURCE's column names and appear identically in both languages.
 */

/** A ⓘ card: headline, where it comes from, over what window, and the caveat. */
type Card = { what: string; where?: string; when?: string; how: string };

export type RiskCopy = {
  /** The switch. ⚠ SHORT — six of these sit on one row inside a fixed dialog. */
  views: {
    active: string; te: string; corr: string; vol: string; dd: string; conc: string; exp: string;
  };
  /** The dialog heading per view, rendered as `<title> vs <benchmark>`. */
  titles: {
    active: string; te: string; corr: string; vol: string; dd: string; conc: string; exp: string;
  };
  subtitle: string;
  close: string;

  common: {
    measured: string;
    daily: string; weekly: string; monthly: string;
    dailyNote: string;
    computing: string;
    /** `x of y priced`, and the sentence that frames every synthetic series. */
    synthetic: (years: number, priced: number, total: number) => string;
    observations: string;
  };

  active: {
    activeShare: string; overlap: string; offBenchmark: string; stocks: string;
    heldOnly: (n: number) => string; everyName: (n: number) => string;
    colCompany: string; colBook: string; colActive: string; notHeld: string;
    coverage: (pct: string, bench: string) => string;
    unmatched: (n: number, pct: string, names: string) => string;
    cards: { activeShare: Card; overlap: Card; offBenchmark: Card; stocks: Card };
  };

  te: {
    trackingError: string; activeReturn: string; infoRatio: string; observations: string;
    cards: { te: Card; activeReturn: Card; infoRatio: Card; observations: Card };
    note: string;
  };

  corr: {
    rhoVs: (bench: string) => string; rSquared: string; meanPair: string; activeVol: string;
    betweenPositions: string; legend: string;
    leastTitle: string; mostTitle: string;
    identityBroken: (pp: string) => string;
    thinPairs: (n: number) => string;
    cards: { rho: Card; rSquared: Card; meanPair: Card; activeVol: Card };
  };

  vol: {
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
    maxDrawdown: (freq: string) => string; benchMax: (bench: string) => string;
    today: string; episodes: string;
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

  exp: {
    sleeve: string; issuers: string; currencies: string; other: string;
    ofBook: (eur: string) => string; weightsOnly: string;
    linesFolded: (lines: number, folded: number) => string; linesOnly: (lines: number) => string;
    largestCcy: (ccy: string, pct: string) => string;
    otherSub: (pct: string) => string;
    unknownCcy: (pct: string) => string;
    currencyTitle: string; positionsTitle: string;
    colIssuer: string; colWeight: string; colValue: string; colCurrency: string;
    lines: (n: number) => string; issuerCount: (n: number) => string;
    note: string;
    cards: { sleeve: Card; issuers: Card; currencies: Card; other: Card };
  };
};

const en: RiskCopy = {
  views: { active: 'Active share', te: 'Tracking error', corr: 'Correlation', vol: 'Volatility',
    dd: 'Drawdown', conc: 'Concentration', exp: 'Positions' },
  titles: { active: 'Active share', te: 'Tracking error', corr: 'Correlation',
    vol: 'Volatility', dd: 'Max drawdown', conc: 'Concentration', exp: 'Effective positions' },
  subtitle: 'The individual stocks only, taken as 100% of the portfolio.',
  close: 'close',

  common: {
    measured: 'Measured',
    daily: 'Daily', weekly: 'Weekly', monthly: 'Monthly',
    dailyNote: 'Daily closes are not synchronous — the tracker closes at 16:30 London, a US holding '
      + 'at 21:00 — which lowers the measured covariance.',
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
    coverage: (pct, b) => `Priced ${pct} of ${b}'s members. The missing weight is redistributed `
      + 'over the rest, so the active share reads slightly low.',
    unmatched: (n, pct, names) => `${n} holding${n === 1 ? '' : 's'} (${pct} of the sleeve) could `
      + `not be matched to a company name and count as fully active: ${names}`,
    cards: {
      activeShare: {
        what: 'How much of the stock sleeve differs from the benchmark.',
        when: "Today's weights — a structural measure, not a return.",
        how: '0% is the index itself; 100% shares no name with it. ⚠ NOT A SCORE — it is the size '
          + 'of the bet, and the only thing it tells you is how far the return CAN diverge, in '
          + 'either direction.',
      },
      overlap: {
        what: 'The share of the sleeve that IS the benchmark.',
        where: 'Σ min(portfolio weight, benchmark weight), over every name on either side.',
        how: 'Exactly 100% − active share, by construction. The two are one number.',
      },
      offBenchmark: {
        what: 'Weight in companies the index does not hold at all.',
        how: 'Active share mixes two decisions — owning something the index does not, and sizing '
          + 'something it does. This is only the first.',
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
    cards: {
      te: {
        what: "How much the book's return has diverged from the benchmark's, annualised.",
        how: '⚠ REALISED (ex-post), not the ex-ante forecast from a covariance matrix — those are '
          + 'different numbers and routinely disagree. ā IS subtracted and the divisor is T−1 '
          + '(Bessel); some providers do neither, which reads higher.',
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
    today: 'Today', episodes: 'Falls over 5%',
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
    effective: 'Effective positions', ofIssuers: (n) => `of ${n} issuers held`,
    top10: 'Top 10', ofBook: (p) => `${p} of the whole book`,
    largest: 'Largest position', benchEffective: (b) => `${b} effective`,
    benchSub: (n, t) => `of ${n} · top 10 ${t}`,
    tableTitle: "Largest issuers, with the index's weight in each",
    colIssuer: 'Issuer', colWeight: 'Weight', colCumulative: 'Cumulative',
    coverage: (pct, b) => `Priced ${pct} of ${b}'s members — the missing weight redistributes over `
      + 'the rest, so the index reads slightly more concentrated than it is.',
    note: 'Folded onto ISSUERS, not lines — two share classes of one company are a single '
      + 'position, which is what stops the ten largest being decided by an identifier.',
    unresolved: (n) => ` ${n} holding${n === 1 ? '' : 's'} could not be matched to a company name `
      + 'and each counts as its own issuer.',
    cards: {
      effective: {
        what: 'How many equally-sized positions this book behaves like.',
        how: '⚠ THE BETTER NUMBER, and the reason it leads. A cut at exactly ten is arbitrary — '
          + 'two books with the same C₁₀ can be an even ten-name portfolio and one dominated by '
          + 'its top three. This has no cut-off. Forty names of which five dominate reads far '
          + 'below forty.',
      },
      top10: {
        what: 'The share of the stock sleeve in its ten largest issuers.',
        how: '⚠⚠ TWO DENOMINATORS, BOTH TRUE. The headline is of the STOCK SLEEVE, which is what '
          + 'compares across books; the line beneath is of the whole book including cash and '
          + 'funds. A book that is 30% cash really is less concentrated in absolute terms.',
      },
      largest: {
        what: 'The single biggest issuer, as a share of the sleeve.',
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

  exp: {
    sleeve: 'Stock sleeve', issuers: 'Issuers', currencies: 'Currencies', other: 'Other',
    ofBook: (e) => `of ${e} in the book`, weightsOnly: 'weights only',
    linesFolded: (l, f) => `${l} lines, ${f} folded`, linesOnly: (l) => `${l} lines`,
    largestCcy: (c, p) => `${c} ${p} largest`,
    otherSub: (p) => `funds, cash, bonds — ${p} of the book`,
    unknownCcy: (p) => `${p} of the sleeve has no currency we could assign. It is reported `
      + 'separately rather than folded into EUR — that default would make the book look more '
      + 'domestic than it is.',
    currencyTitle: 'Currency exposure', positionsTitle: 'Effective position per issuer',
    colIssuer: 'Issuer', colWeight: 'Weight', colValue: 'Value', colCurrency: 'Currency',
    lines: (n) => `${n} lines`, issuerCount: (n) => `${n} issuer${n === 1 ? '' : 's'}`,
    note: "Eᵢ is AIRS's own EUR valuation of the position, not a quantity × price × FX of ours — "
      + "it is the figure on the client's statement, and a second derivation would disagree with "
      + 'it on most rows with no way to say which was right. The weights here are the same ones '
      + 'Active share and Concentration read, folded once.',
    cards: {
      sleeve: {
        what: 'The euros in individual stocks — the sleeve every view in this panel measures.',
        where: "AIRS's own `current_value_eur` per holding, summed. ⚠ NOT a q·P·X of ours: AIRS "
          + "values the book, and that is the number on the client's statement.",
        how: "⚠ TRADE DATE vs SETTLEMENT DATE IS AIRS'S CONVENTION and it exposes no flag saying "
          + 'which it used, so a book with a very recent trade may differ from a trade-date view '
          + "by that trade's value. Stated rather than assumed away.",
      },
      issuers: {
        what: 'Distinct companies held, after folding share classes and dual listings.',
        how: '⚠ THE ONE-LINE ANSWER TO "WHY DOES THIS COUNT DIFFERENTLY FROM THE HOLDINGS TABLE". '
          + 'Alphabet A + Alphabet C is one position. The same fold feeds Active share and '
          + 'Concentration, so all three agree by construction.',
      },
      currencies: {
        what: "How many currencies the sleeve's value actually sits in.",
        where: "The LISTING's currency, from the holding or our grid mapping of the ISIN.",
        how: '⚠ THE EXPOSURE YOU BEAR, not the one the company earns in. Nestlé on SIX is CHF '
          + 'exposure whatever its revenue mix — that is a fact about the position. The economic '
          + 'argument is true and is a different, softer claim.',
      },
      other: {
        what: 'Everything outside the stock sleeve.',
        where: 'Funds, cash, bonds, and any line without a usable ISIN.',
        how: '⚠ EVERY OTHER VIEW IN THIS PANEL EXCLUDES THIS AND RENORMALISES. The figure is here '
          + 'so the renormalisation is never invisible.',
      },
    },
  },
};

const nl: RiskCopy = {
  views: { active: 'Active share', te: 'Tracking error', corr: 'Correlatie', vol: 'Volatiliteit',
    dd: 'Drawdown', conc: 'Concentratie', exp: 'Posities' },
  titles: { active: 'Active share', te: 'Tracking error', corr: 'Correlatie',
    vol: 'Volatiliteit', dd: 'Maximale drawdown', conc: 'Concentratie',
    exp: 'Effectieve posities' },
  subtitle: 'Alleen de individuele aandelen, genomen als 100% van de portefeuille.',
  close: 'sluiten',

  common: {
    measured: 'Gemeten',
    daily: 'Dagelijks', weekly: 'Wekelijks', monthly: 'Maandelijks',
    dailyNote: 'Dagslotkoersen lopen niet gelijk — de tracker sluit om 16:30 Londen, een Amerikaanse '
      + 'positie om 21:00 — waardoor de gemeten covariantie lager uitvalt.',
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
    coverage: (pct, b) => `${pct} van de leden van ${b} geprijsd. Het ontbrekende gewicht wordt over `
      + 'de rest herverdeeld, waardoor de active share iets te laag uitvalt.',
    unmatched: (n, pct, names) => `${n} positie${n === 1 ? '' : 's'} (${pct} van de selectie) kon `
      + `niet aan een ondernemingsnaam worden gekoppeld en telt volledig als actief: ${names}`,
    cards: {
      activeShare: {
        what: 'Hoeveel van de aandelenselectie afwijkt van de benchmark.',
        when: 'De gewichten van vandaag — een structurele maat, geen rendement.',
        how: '0% is de index zelf; 100% deelt er geen enkele naam mee. ⚠ GEEN RAPPORTCIJFER — het '
          + 'is de omvang van de positionering, en het enige wat het zegt is hoe ver het rendement '
          + 'KAN afwijken, in beide richtingen.',
      },
      overlap: {
        what: 'Het deel van de selectie dat de benchmark WEL is.',
        where: 'Σ min(portefeuillegewicht, benchmarkgewicht), over elke naam aan beide zijden.',
        how: 'Per definitie exact 100% − active share. De twee zijn één getal.',
      },
      offBenchmark: {
        what: 'Gewicht in ondernemingen die de index helemaal niet houdt.',
        how: 'Active share mengt twee beslissingen — iets bezitten dat de index niet heeft, en iets '
          + 'zwaarder of lichter wegen dat hij wél heeft. Dit is alleen de eerste.',
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
    cards: {
      te: {
        what: 'Hoeveel het rendement van het boek is afgeweken van dat van de benchmark, '
          + 'geannualiseerd.',
        how: '⚠ GEREALISEERD (ex-post), niet de ex-ante voorspelling uit een covariantiematrix — '
          + 'dat zijn verschillende getallen en ze lopen structureel uiteen. ā WORDT afgetrokken en '
          + 'de noemer is T−1 (Bessel); sommige aanbieders doen geen van beide, wat hoger uitkomt.',
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
    today: 'Vandaag', episodes: 'Dalingen boven 5%',
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
    effective: 'Effectieve posities', ofIssuers: (n) => `van ${n} gehouden emittenten`,
    top10: 'Top 10', ofBook: (p) => `${p} van het hele boek`,
    largest: 'Grootste positie', benchEffective: (b) => `Effectief ${b}`,
    benchSub: (n, t) => `van ${n} · top 10 ${t}`,
    tableTitle: 'Grootste emittenten, met het gewicht van de index in elk',
    colIssuer: 'Emittent', colWeight: 'Gewicht', colCumulative: 'Cumulatief',
    coverage: (pct, b) => `${pct} van de leden van ${b} geprijsd — het ontbrekende gewicht wordt over `
      + 'de rest herverdeeld, waardoor de index iets geconcentreerder oogt dan hij is.',
    note: 'Samengevoegd per EMITTENT, niet per regel — twee aandelenklassen van één onderneming '
      + 'zijn één positie, en dat voorkomt dat de tien grootste door een identificatiecode worden '
      + 'bepaald.',
    unresolved: (n) => ` ${n} positie${n === 1 ? '' : 's'} kon niet aan een ondernemingsnaam worden `
      + 'gekoppeld en telt elk als eigen emittent.',
    cards: {
      effective: {
        what: 'Naar hoeveel even grote posities dit boek zich gedraagt.',
        how: '⚠ HET BETERE GETAL, en daarom staat het vooraan. Een grens bij precies tien is '
          + 'willekeurig — twee boeken met dezelfde C₁₀ kunnen een gelijkmatige tiennamenportefeuille '
          + 'zijn en een die door zijn top drie wordt gedomineerd. Dit kent geen afkapping. Veertig '
          + 'namen waarvan er vijf domineren komt ver onder veertig uit.',
      },
      top10: {
        what: 'Het deel van de aandelenselectie dat in de tien grootste emittenten zit.',
        how: '⚠⚠ TWEE NOEMERS, ALLEBEI WAAR. De kop gaat over de AANDELENSELECTIE, en dat is wat '
          + 'tussen boeken vergelijkbaar is; de regel eronder gaat over het hele boek inclusief '
          + 'liquiditeiten en fondsen. Een boek dat voor 30% uit liquiditeiten bestaat is in '
          + 'absolute zin werkelijk minder geconcentreerd.',
      },
      largest: {
        what: 'De grootste enkele emittent, als aandeel van de selectie.',
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

  exp: {
    sleeve: 'Aandelenselectie', issuers: 'Emittenten', currencies: 'Valuta', other: 'Overig',
    ofBook: (e) => `van ${e} in het boek`, weightsOnly: 'alleen gewichten',
    linesFolded: (l, f) => `${l} regels, ${f} samengevoegd`, linesOnly: (l) => `${l} regels`,
    largestCcy: (c, p) => `${c} ${p} grootste`,
    otherSub: (p) => `fondsen, liquiditeiten, obligaties — ${p} van het boek`,
    unknownCcy: (p) => `Aan ${p} van de selectie konden we geen valuta toekennen. Dat wordt apart `
      + 'gerapporteerd in plaats van bij de euro geteld — die keuze zou het boek binnenlandser doen '
      + 'lijken dan het is.',
    currencyTitle: 'Valuta-exposure', positionsTitle: 'Effectieve positie per emittent',
    colIssuer: 'Emittent', colWeight: 'Gewicht', colValue: 'Waarde', colCurrency: 'Valuta',
    lines: (n) => `${n} regels`, issuerCount: (n) => `${n} emittent${n === 1 ? '' : 'en'}`,
    note: 'Eᵢ is de eigen eurowaardering van AIRS van de positie, geen eigen berekening van stuks × '
      + 'koers × wisselkoers — het is het cijfer op het overzicht van de klant, en een tweede '
      + 'afleiding zou daar op de meeste regels van afwijken zonder dat te zeggen valt welke klopt. '
      + 'De gewichten hier zijn dezelfde die Active share en Concentratie lezen, één keer '
      + 'samengevoegd.',
    cards: {
      sleeve: {
        what: "De euro's in individuele aandelen — de selectie die elke weergave in dit paneel meet.",
        where: 'De eigen `current_value_eur` van AIRS per positie, opgeteld. ⚠ GEEN eigen q·P·X: '
          + 'AIRS waardeert het boek, en dat is het cijfer op het overzicht van de klant.',
        how: '⚠ TRANSACTIEDATUM versus AFWIKKELINGSDATUM IS DE CONVENTIE VAN AIRS, en AIRS geeft '
          + 'nergens aan welke is gebruikt. Een boek met een zeer recente transactie kan daardoor '
          + 'van een transactiedatum-weergave afwijken met de waarde van die transactie. Dit wordt '
          + 'vermeld in plaats van weggeredeneerd.',
      },
      issuers: {
        what: 'Afzonderlijke ondernemingen in bezit, na samenvoeging van aandelenklassen en '
          + 'dubbele noteringen.',
        how: '⚠ HET ANTWOORD IN ÉÉN REGEL OP "WAAROM TELT DIT ANDERS DAN DE POSITIETABEL". Alphabet '
          + 'A + Alphabet C is één positie. Dezelfde samenvoeging voedt Active share en '
          + 'Concentratie, dus alle drie komen per constructie overeen.',
      },
      currencies: {
        what: "In hoeveel valuta de waarde van de selectie feitelijk staat.",
        where: 'De valuta van de NOTERING, uit de positie of uit onze koppeling van de ISIN.',
        how: '⚠ DE EXPOSURE DIE U DRAAGT, niet die waarin de onderneming verdient. Nestlé op SIX is '
          + 'CHF-exposure, ongeacht de omzetverdeling — dat is een feit over de positie. Het '
          + 'economische argument is waar en is een andere, zachtere bewering.',
      },
      other: {
        what: 'Alles buiten de aandelenselectie.',
        where: 'Fondsen, liquiditeiten, obligaties, en elke regel zonder bruikbare ISIN.',
        how: '⚠ ELKE ANDERE WEERGAVE IN DIT PANEEL LAAT DIT WEG EN HERWEEGT. Het cijfer staat hier '
          + 'zodat die herweging nooit onzichtbaar is.',
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
