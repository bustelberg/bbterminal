'use client';

import { useLang, type Lang } from '../../../lib/i18n';
import { v } from '../../../lib/dynamicValue';

/**
 * THE DEEP VALUATION TAB, IN BOTH LANGUAGES — the EGM panel, the Reverse DCF, and the two
 * raw-data modals behind them.
 *
 * ⚠⚠ ENGLISH IS THE SOURCE AND A MISSING DUTCH STRING IS A COMPILE ERROR. `nl` is typed as
 * `DeepValuationCopy`, so a key added to `en` and forgotten here fails `tsc` rather than falling
 * back — a half-translated panel renders as a rendering bug, not as an unfinished translation, and
 * nobody reports it as the latter. Same rule `managementCopy` and `riskCopy` set.
 *
 * ⚠⚠ THE VOCABULARY IS A DUTCH WEALTH MANAGER'S, NOT A DICTIONARY'S. `Reverse DCF`, `WACC`, `P/E`,
 * `CAGR`, `TTM`, `EPS` and `FCF` stay — they are what is said out loud in this business and a
 * translated `omgekeerde contantewaardeberekening` would be more Dutch and less readable. Where a
 * real Dutch word exists it IS used: rendement, groeivoet, disconteringsvoet, eeuwigdurende groei,
 * afschrijvingen, uitkeringsrendement, koers, aandelen.
 *
 * ⚠ NUMBERS AND CURRENCY CODES ARE OPERANDS, NEVER TRANSLATED. Every function here takes its
 * figures pre-formatted from the call site: a `toFixed` on this side would be a second rounding
 * convention, and the two languages would print different numbers for one model.
 *
 * ⚠⚠ THE PANEL SHOWS TWO MODELS, AND THE COPY HAS TO KEEP THEM APART. Everything above
 * `atYourHurdle` answers "at today's price, what do I earn?" — growth, yield and the rerating,
 * all facts about the company. The two rows under that caption answer "what may I pay to earn my
 * hurdle?", which is a fact about the READER. They were one undifferentiated list once, read as
 * rival verdicts, and `Fair value` was demoted to a tooltip to stop that — which left a
 * `Hurdle rate` input driving nothing anybody could see. The caption is what lets both be visible
 * without competing, so a copy edit that drops it re-opens the original bug.
 *
 * ⚠ THE ⓘ CARDS ARE IN SCOPE. They carry the definition, the convention chosen and the caveat that
 * stops a figure being misread — a Dutch reader who must switch languages to learn that growth
 * capex is ADDED back has been given the digits and not the number.
 */

/** A ⓘ card's four prose fields. Any may be absent; `AspectCard` collapses what it is not given. */
type Card = { what?: string; where?: string; when?: string; how?: string };

export type DeepValuationCopy = {
  /**
   * Strings BOTH panels use.
   *
   * ⚠ A SHARED SECTION, NOT A BORROW ACROSS TWO. The nesting exists so a call site cannot reach
   * into another panel's copy by accident, and `t.egm.guruFocus` read from inside the Reverse DCF
   * was exactly that — it happened to be the right sentence, which is what makes the habit hard to
   * see. One vendor sentence used by two panels belongs to neither of them.
   */
  common: {
    guruFocus: (vendor: string) => string;
  };

  /** The EGM panel — the expected-return model. */
  egm: {
    reset: string;
    growthRate: string; exitPE: string; sharePriceNow: string; forwardPE: string;
    hurdleRate: string; dividendYield: string;
    showRawData: string;
    /** The output table's row labels. */
    expectedReturn: string; priceTarget: string; totalPriceMove: string;
    /**
     * The hurdle block — a SECOND MODEL, run on the same inputs and answering the other question.
     *
     * ⚠⚠ THE CAPTION IS NOT DECORATION, IT IS WHAT MAKES THE BLOCK LEGIBLE. `Fair value` used to
     * be a bare row beside the expected return and was demoted to a tooltip for exactly that
     * reason: two verdicts in one undifferentiated list read as rivals, and a reader cannot tell
     * which question each is answering. Under a caption that names the question they stop
     * competing — the panel says what you GET at today's price, then what you may PAY to earn
     * your hurdle.
     */
    atYourHurdle: string;
    maxPE: string; fairValue: string;
    /** The three bridge legs, named where the table prints them. */
    legGrowth: string; legYield: string; legMultiple: string;
    cards: {
      growth: Card; exitPE: Card; price: Card; forwardPE: Card;
      hurdle: Card; dividend: Card;
      expectedReturn: Card; priceTarget: Card; priceMove: Card;
      maxPE: Card; fairValue: Card;
    };
    /** ⚠ A FAIR VALUE WITH NO COMPARISON INVITES THE SUBTRACTION AND ANSWERS NOTHING. The upside
     *  was computed and thrown away for months; it belongs in this card. */
    fairValueVsPrice: (pct: string) => string;
    fairValueNoEps: string;
    /** Operand-bearing lines: the window, the defaults, the analyst chips. */
    everyYearFor: (years: string) => string;
    endOfYear: (years: string) => string;
    perYearOver: (years: string) => string;
    yearsOut: (years: string) => string;
    overYearsNotAnnualised: (years: string) => string;
    houseDefault: (value: string) => string;
    analystsImply: (value: string) => string;
    medianIs: (value: string) => string;
    reratingRuns: (from: string, to: string) => string;
    analystHint: string;
    medianPEHint: string;
    yoursTypedHere: string;
    yoursOrDefault: string;
    whateverMoment: string;
    daysOld: (n: string) => string;

    /** The bridge table's leg labels and the implied-price row. */
    legEarningsGrowth: string; legMultipleWord: string;
    impliedIn: (years: string) => string;
    /** The chips under the measured fields, in both directions — see `onUseHint`. */
    storedCloseBack: string; storedCloseInUse: string; impliedPEHint: string;
    dividendBack: string; dividendInUse: string;
    /** The ⓘ over the price row, which has SIX states because an overridden price has no
     *  provenance at all — see the ⚠⚠ at the call site. */
    priceTyped: string; priceClosingOf: (name: string) => string;
    priceWhereTyped: string; loading: string;
    yahooFinance: (symbol: string) => string;
    noneStored: string; noObservationStored: string;
    clearBoxToGoBack: (price: string) => string;
    rawDataYahoo: string; rawDataNoRefresh: string;
    /** The Refresh control's four states. */
    reReadClose: string; reReadCancel: string; reReading: string; cancelling: string;
    reReadOverridden: string; reReadStale: string;
    /** The forward P/E's own Refresh — a different vendor and transport from the share price's. */
    reReadForwardPE: string; reReadNoCompany: string;
    /** ⚠ THE OVERRIDE STATES. A typed forward P/E HIDES whatever the refresh fetches, exactly as a
     *  typed share price does — the button must say so, and the ⓘ must stop dating a vendor
     *  observation the box is no longer showing. */
    reReadForwardPEOverridden: string;
    forwardPEWhenTyped: string;
    forwardPEHowTyped: (vendor: string, date: string) => string;
    /** What the toast reports. ⚠ THE DATE, NOT "done" — see `refreshForwardPE`. */
    forwardPEMoved: (date: string) => string;
    forwardPEUnchanged: (date: string) => string;
    forwardPENone: string;
    /** The `how` on the three answer rows. */
    growthHow: (dflt: string) => string;
    exitPEHow: (dflt: string) => string;
    forwardPEHow: (years: string) => string;
    forwardPEDisagrees: (implied: string, vendor: string) => string;
    hurdleHow: (dflt: string) => string;
    factorsMultiply: (added: string, compounded: string) => string;
    noForwardPE: string; noCompoundingPath: string;
    priceOnlyFairValue: (fair: string, mult: string) => string;
    priceVsRerating: (implied: string, from: string) => string;
    priceLegOnly: (total: string) => string; noDividend: string;
    /** The symbols the two worked lines define. */
    legend: {
      g: string; y: string; peExit: string; peFwd: string;
      n: (years: string) => string;
      p0: string; p0Row: string; pn: (years: string) => string;
      rerating: string;
      /** ⚠ THE ONLY SYMBOL ON THIS PANEL THAT DESCRIBES THE READER, not the company. */
      h: string; epsFY1: string; maxPE: string;
    };
  };

  /** The Reverse DCF panel. */
  dcf: {
    title: string; subtitle: string;
    lastReported: string; rawData: string;
    impliedGrowth: string; impliedByDiscountRate: string;
    reset: string; showFigures: string; useThisRate: string;
    rowSbc: string; rowCapex: string; rowDA: string; rowGrowthCapex: string;
    rowDiscountRate: string; rowPerpetuityGrowth: string; rowForecastYears: string;
    cards: {
      sbc: Card; capex: Card; da: Card; growthCapex: Card;
      baseNotUsed: Card; solvedAgainst: Card; marketCap: Card;
      discountRate: Card; perpetuityGrowth: Card; forecastYears: Card;
      impliedGrowth: Card;
    };
    fromYearOnwards: (year: string) => string;
    yearsOneTo: (years: string) => string;
    yearsOneToPlain: (years: string) => string;

    /** The toolbar above the inputs. */
    base: string; normalise: string;
    baseTitle: string; normaliseTitle: string;
    nextFY: string; nextFYNone: string;
    /** ⚠ APPENDED TO EVERY CASH-FLOW CARD — one sentence, one place, so the four lines cannot
     *  disagree about which window they share. */
    ttmNote: string; normOff: string; notLikeForLike: string;
    /** The two headline field labels; the currency suffix is added at the call site. */
    freeCashFlow: string; cashFlowValued: string; targetMarketCap: string;
    vsReportedFcf: string; vsNextFyDerived: (fy: string) => string;
    /** The `how` prose on the base row, whose three branches are three different provenances. */
    fcfWhatReported: string; fcfWhatForward: (fy: string) => string;
    fcfHowDirect: string; fcfHowDerived: string; fcfHowReported: string;
    inMillionsIs: (a: string, b: string) => string; inMillions: string;
    nextFiscalYear: string;
    /** The two corrections, each with a "not reported" branch — an absent line is not a zero. */
    sbcHow: string; sbcAbsent: string;
    capexHow: string; daHow: string;
    /** ⚠ TWO PARAGRAPHS, TWO KEYS: the first says WHY it is added back, the second what the
     *  proxy is worth. Joined at the call site so the blank line between them stays one rule. */
    growthCapexAdded: string; growthCapexHow: string; growthCapexAbsent: string;
    /** The valued figure. */
    valuedWhatYours: string; valuedWhat: string;
    valuedWhereYours: string; valuedWhere: string;
    valuedWhenYours: string;
    valuedHowOverridden: string; valuedHowNormOff: string;
    valuedHowAllRan: string; valuedHowPartial: (which: string) => string;
    correctionSbc: string; correctionCapexDep: string;
    /** The base NOT used. */
    baseNotUsedNoFcf: string; baseNotUsedNoConsensus: string; baseNotUsedCompare: string;
    guruFocusLess: (a: string, b: string) => string;
    /** Market cap, discount rate, perpetuity. */
    closeOn: (date: string) => string; sharesOn: (date: string) => string;
    noDatesStored: string; houseDefault: string; noWaccStored: string;
    /** The answer row. */
    impliedHow: string;
    impliedNonPositive: (fcf: string) => string;
    impliedMissing: (what: string) => string;
    impliedRateTooLow: string; impliedNoRate: string;
    /** The analyst comparison rows. */
    analystsPrefix: string;
    analystEps: string; analystOcf: string; analystAvg: string;
    analystEpsTip: string; analystOcfTip: string; analystAvgTip: string;
    /** The symbols the worked lines define. */
    legend: {
      ocfEst: (fy: string) => string; capexFiled: string;
      F: string; Fforward: (fy: string) => string; Ffiled: string;
      S: string; G: string;
      C: string; D: string;
      g: string; r: string; gInf: string; M: string;
      p0: string; N: string;
    };
  };

  /** The "what the model reads" modal behind the Reverse DCF. */
  dcfModal: {
    title: string;
    companyFigures: string; asFiled: string;
    colInput: string; colValue: string; colPeriod: string; colMetricCode: string;
    whatTheyAddUpTo: string;
    marketCap: string; cashFlowCompounded: string; cashFlowOverridden: string;
    solvingAgainst: string; model: string;
    rowSharePrice: string; rowShares: string; rowFcf: string; rowOcfEst: string; rowWacc: string;
    ttmTo: (date: string) => string;
    modelLine: (years: string, discount: string, perpetuity: string) => string;
  };

  /** The "data behind the defaults" modal behind the EGM panel. */
  egmModal: {
    title: string;
    growthSection: string; growthNote: string; noEstimates: string;
    colFiscalPeriod: string; colEpsEstimate: string; from: string; to: string;
    noCagrOnePoint: string; noCagrNonPositive: string; noCagrPrefix: string;
    peSection: string; peNote: string; noPriceHistory: string;
    colFiscalYear: string; colYearEndPrice: string; colEpsNri: string; colPE: string;
    excluded: string; excludedTitle: string;
    medianOfUsable: (n: string) => string;
    yieldSection: string; yieldNote: string; noYieldLine: string;
    colPeriodEnd: string; colCadence: string; colDividendYield: string;
    inUse: string; quarterly: string; annual: string;
    showingMostRecent: (shown: string, total: string) => string;
  };
};

const en: DeepValuationCopy = {
  common: { guruFocus: (vendor) => `GuruFocus, ${v(vendor)}.` },
  egm: {
    reset: 'Put every assumption back to its default',
    growthRate: 'Growth rate', exitPE: 'Exit P/E', sharePriceNow: 'Share price now',
    forwardPE: 'Forward P/E', hurdleRate: 'Hurdle rate', dividendYield: 'Dividend yield',
    showRawData: 'Show the raw data behind these defaults',
    expectedReturn: 'Expected return', priceTarget: 'Price target',
    totalPriceMove: 'Total price move',
    atYourHurdle: 'At your hurdle rate',
    maxPE: 'Max P/E', fairValue: 'Fair value',
    legGrowth: 'Growth', legYield: 'Dividend yield', legMultiple: 'Rerating',
    cards: {
      growth: {
        what: 'Assumed growth in earnings per share.',
        where: 'Yours, or the house default when blank.',
      },
      exitPE: {
        what: 'Assumed P/E when you sell.',
        where: 'Yours, or the house default when blank.',
      },
      price: {
        what: 'The price every return on this panel is measured from.',
        how: 'Blank uses the stored close. Every return on the panel moves with this.',
      },
      forwardPE: {
        what: 'The multiple the rerating leg starts FROM.',
      },
      hurdle: {
        what: 'The return you require.',
        when: 'Per year.',
      },
      dividend: {
        what: 'Assumed dividend yield.',
        how: 'Percent per year. Raw data, no formula.',
      },
      expectedReturn: {
        what: 'Annual TOTAL return these assumptions imply — dividends included.',
        where: 'Computed from the three rows above.',
      },
      priceTarget: {
        what: 'Share price at the end of the window.',
        where: 'Computed from the price and the two assumptions.',
      },
      priceMove: {
        what: 'Total price move over the window.',
        where: 'Computed from the two rows above.',
      },
      maxPE: {
        what: 'The most you can pay today and still earn your hurdle rate.',
        where: 'The exit multiple, discounted back at the hurdle instead of at the market.',
        how: '⚠ A DIFFERENT MODEL FROM THE RETURN ABOVE, on the same assumptions. That one asks '
          + "what today's price earns you; this asks what you may pay. Above the exit multiple "
          + 'when growth and dividends outrun the hurdle.',
      },
      fairValue: {
        what: 'That multiple, on next year’s earnings.',
        where: 'Next year’s consensus EPS × the max P/E beside it.',
      },
    },
    everyYearFor: (years) => `Every year, for ${v(years)} years.`,
    endOfYear: (years) => `End of year ${v(years)}.`,
    perYearOver: (years) => `Per year, over ${v(years)} years.`,
    yearsOut: (years) => `${v(years)} years out.`,
    overYearsNotAnnualised: (years) => `Over ${v(years)} years, not annualised.`,
    houseDefault: (value) => `Default ${v(value)}, a house figure`,
    analystsImply: (value) => `; analysts imply ${v(value)}`,
    medianIs: (value) => `; its 5-year median is ${v(value)}`,
    reratingRuns: (from, to) => ` The rerating leg runs ${v(from)} to ${v(to)}.`,
    analystHint: 'Analysts’ implied growth — the CAGR of the consensus EPS estimates, not a '
      + 'published long-term rate. Click to use it.',
    medianPEHint: 'This company’s own median P/E over the last five years. Click to use it.',
    yoursTypedHere: 'Yours, typed here.',
    yoursOrDefault: 'Yours, or the house default when blank.',
    whateverMoment: 'Whatever moment you mean it to be.',
    daysOld: (n) => `${n} days old`,

    legEarningsGrowth: 'Earnings growth', legMultipleWord: 'Multiple',
    impliedIn: (years) => `Implied in ${years}y`,
    storedCloseBack: 'The stored close. Click to go back to it.',
    storedCloseInUse: 'The stored close — in use.',
    impliedPEHint: 'Price ÷ next-year consensus EPS — the multiple the market is actually paying, '
      + 'computed here rather than read from the vendor. Click to use it.',
    dividendBack: 'The yield GuruFocus reports. Click to go back to it.',
    dividendInUse: 'The yield GuruFocus reports — in use. Click to put it in the box and edit '
      + 'from it.',
    priceTyped: 'The price you typed, not a close.',
    priceClosingOf: (name) => `Closing stock price of ${name}.`,
    priceWhereTyped: 'Yours, typed into the Share price box.',
    loading: 'Loading…',
    yahooFinance: (symbol) => `Yahoo Finance${symbol ? `, ${v(symbol)}` : ''}.`,
    noneStored: 'None stored.',
    noObservationStored: 'No observation stored.',
    clearBoxToGoBack: (price) =>
      `Clear the Share price box to go back to the stored close${price ? `, ${v(price)}` : ''}.`,
    rawDataYahoo: 'Raw data. Refresh fetches the newest closes from Yahoo.',
    rawDataNoRefresh: 'Raw data. Refresh cannot update this one.',
    reReadClose: 'Re-read the stored close', reReadCancel: 'Cancel the re-read',
    reReading: 'Re-reading — press to cancel', cancelling: 'Cancelling…',
    reReadOverridden: 'Re-read the stored close — the Share price box is overriding it, ',
    reReadStale: 'This close is over a week old — re-read it',
    reReadForwardPE: 'Ask GuruFocus for this forward P/E again',
    reReadNoCompany: 'No GuruFocus company for this ISIN, so there is nothing to re-read',
    reReadForwardPEOverridden: 'Ask GuruFocus for this forward P/E again — the box is overriding '
      + 'it, so the new figure appears on that box’s chip',
    forwardPEWhenTyped: 'Whatever moment you mean it to be.',
    forwardPEHowTyped: (vendor, date) => `Clear the box to go back to ${vendor}, which last `
      + `published on ${date}.`,
    forwardPEMoved: (date) => `forward P/E now ${date}`,
    forwardPEUnchanged: (date) => `still ${date} — GuruFocus has nothing newer`,
    forwardPENone: 'GuruFocus returned no forward P/E for this company',
    growthHow: (dflt) => `Percent per year. ${dflt}`,
    exitPEHow: (dflt) => `A multiple, not a percent. ${dflt}`,
    forwardPEHow: (years) => 'Forward, not trailing — it matches the growth rate above, which runs '
      + `from FY1. The multiple leg is (exit ÷ this) ^ (1/${v(years)}) per year.`,
    forwardPEDisagrees: (implied, vendor) =>
      `Price ÷ consensus EPS reads ${v(implied)} against the vendor's ${v(vendor)}. The chip puts `
      + "the market's own figure in.",
    hurdleHow: (dflt) => `Sets the fair value, not the expected return. ${dflt}.`,
    factorsMultiply: (added, compounded) =>
      `The factors MULTIPLY, so the × column ties and the % column does not: ${v(added)} added `
      + `against ${v(compounded)} compounded. The yield lifts this figure but not the implied `
      + 'price below, which is the capital leg alone.',
    noForwardPE: 'No usable forward P/E, so there is nothing to rerate from.',
    noCompoundingPath: 'These assumptions have no compounding path. Check the exit P/E, growth '
      + 'and hurdle.',
    fairValueVsPrice: (pct) => `${v(pct)} against today’s price.`,
    fairValueNoEps: 'No consensus EPS for next year, so there is nothing to apply the multiple to.',
    priceOnlyFairValue: (fair, mult) =>
      `Price only; dividends are in the return below. Fair value at the hurdle is ${v(fair)} `
      + `(${v(mult)}).`,
    priceVsRerating: (implied, from) =>
      ` Price ÷ EPS reads ${v(implied)} against the ${v(from)} the rerating starts from.`,
    priceLegOnly: (total) =>
      `Price leg only. With dividends reinvested the total is ${v(total)}.`,
    noDividend: 'No dividend, so this is the whole return.',
    legend: {
      g: 'assumed EPS growth, per year',
      y: 'assumed dividend yield, applied every year',
      peExit: 'the multiple you assume on sale',
      peFwd: 'the forward P/E it rerates FROM',
      n: (years) => `the forecast horizon, ${v(years)} years`,
      p0: 'the price now',
      p0Row: 'the price now — the row above',
      pn: (years) => `the implied price, ${v(years)} years out`,
      rerating: 'the whole rerating, applied once — not per year',
      h: 'the return you require — your input, not a fact about the company',
      epsFY1: 'next year’s consensus earnings per share',
      maxPE: 'the multiple from the row above',
    },
  },

  dcf: {
    title: 'Reverse DCF',
    subtitle: 'what the price implies, not what the company is worth',
    lastReported: 'Last reported', rawData: 'raw data ↗',
    impliedGrowth: 'Implied FCF growth',
    impliedByDiscountRate: 'Implied growth by discount rate',
    reset: 'Put every input back to its default',
    showFigures: 'Show every company figure this reads, with its source',
    useThisRate: 'Use this rate',
    rowSbc: '− Stock compensation', rowCapex: 'Capital expenditure',
    rowDA: 'Depreciation & amortisation', rowGrowthCapex: '+ Growth capex',
    rowDiscountRate: 'Discount rate', rowPerpetuityGrowth: 'Perpetuity growth',
    rowForecastYears: 'Forecast years',
    cards: {
      sbc: { what: 'Stock-based compensation, subtracted.' },
      capex: { what: 'Capital expenditure, as the magnitude spent.' },
      da: { what: 'Cash-flow depreciation, depletion and amortisation.' },
      growthCapex: {
        what: 'Capital spending above depreciation, added back.',
        where: 'Computed from the two rows above.',
      },
      baseNotUsed: { what: 'The base this panel is NOT using.' },
      solvedAgainst: { what: 'The valuation solved against.' },
      marketCap: {
        where: 'Computed from the price and the share count.',
        how: 'In millions, like the cash flow above. Override it to ask what a different '
          + 'valuation would have to assume.',
      },
      discountRate: {
        what: 'Rate the cash flows are discounted at.',
        how: 'Percent per year; must exceed the perpetuity growth.',
      },
      perpetuityGrowth: {
        what: 'Growth after the forecast years.',
        where: 'House convention.',
        how: 'Percent per year. Raw input, no formula.',
      },
      forecastYears: {
        what: 'Length of the explicit growth phase.',
        how: 'A count of years. Raw input, no formula.',
      },
      impliedGrowth: {
        what: 'FCF growth the market cap already assumes.',
        where: 'Computed here.',
      },
    },
    fromYearOnwards: (year) => `Year ${v(year)} onwards, for ever.`,
    yearsOneTo: (years) => `Years 1 to ${v(years)}, then the perpetuity growth.`,
    yearsOneToPlain: (years) => `Years 1 to ${v(years)}.`,

    base: 'Base', normalise: 'Normalise',
    baseTitle: 'Which cash flow the model grows from.\n\n'
      + "Next FY: the analysts' consensus operating cash flow for the coming fiscal year, less "
      + 'capital expenditure — free cash flow the company has not earned yet.\n'
      + 'Last reported: free cash flow exactly as filed for the most recent fiscal year.\n\n'
      + 'The stock-compensation and growth-capex corrections below run on either.',
    normaliseTitle: 'Value free cash flow net of stock compensation and before growth capex.\n\n'
      + 'SBC is subtracted: it is a real cost that never leaves the cash flow statement.\n'
      + 'Growth capex (capex above depreciation) is ADDED BACK: reported FCF already subtracted '
      + 'it, and it buys the very growth this model is solving for.',
    nextFY: 'Next FY', nextFYNone: 'Next FY (none)',
    ttmNote: '\n\nTrailing twelve months where four quarters exist, the last full fiscal year '
      + 'otherwise — one window for all four cash-flow lines.',
    normOff: '\n\nUntick Normalise to value the reported figure instead.',
    notLikeForLike: '\n\nNot like for like with the implied rate: different metric (free cash '
      + 'flow vs earnings and operating cash flow), different horizon (full forecast plus '
      + 'perpetuity vs 3-5 years), and different basis (total vs per share, so buybacks lift '
      + 'these). A sanity check on the order of magnitude, not an equality.',
    freeCashFlow: 'Free cash flow', cashFlowValued: 'Cash flow valued',
    targetMarketCap: 'Target market cap',
    vsReportedFcf: 'vs reported FCF', vsNextFyDerived: (fy) => `vs ${fy} FCF (derived)`,
    fcfWhatReported: 'Latest reported free cash flow.',
    fcfWhatForward: (fy) => `Consensus free cash flow for ${fy}.`,
    fcfHowDirect: "The analysts' own forecast, read not derived — it nets a forward capex "
      + "estimate, which is what GuruFocus's page shows. Year 1 is their work; every year after "
      + 'it is the rate this panel solves for.',
    fcfHowDerived: 'Derived: no consensus free cash flow is stored for this company, so it is the '
      + 'consensus operating cash flow less the last filed capex. That capex leg largely cancels '
      + 'against the growth-capex row below.',
    fcfHowReported: 'Operating cash flow minus TOTAL capex, which is why the growth-capex row '
      + 'below adds back rather than subtracting.',
    inMillionsIs: (a, b) => ` In millions: ${v(a)} is ${v(b)}.`,
    inMillions: ' In millions.',
    nextFiscalYear: 'Next fiscal year.',
    sbcHow: 'A real cost that never leaves the cash flow statement: added back into operating cash '
      + 'flow as a non-cash charge, so reported free cash flow flatters anyone paying in equity.',
    sbcAbsent: 'Not reported for this company, so nothing is subtracted — an absent line is not a '
      + 'zero.',
    capexHow: 'The first of the two lines the growth-capex row below subtracts.',
    daHow: "The cash-flow line, not the income statement's. GuruFocus files both and they differ; "
      + 'capex is a cash figure, so its maintenance proxy has to be one too.',
    growthCapexAdded: 'Added, not subtracted: the base above already took all capex out. '
      + 'Maintenance capex sustains the business; the excess buys the growth this model solves '
      + 'for, so leaving it in charges the same expansion twice.',
    growthCapexHow: 'Depreciation is a proxy for maintenance capex, weakest for a company building '
      + 'an asset base for the first time. Floored at zero, so under-investment is not read as a '
      + 'windfall.',
    growthCapexAbsent: 'Capex or cash-flow depreciation is not reported for this company, so '
      + 'nothing is added back — an absent line is not a zero.',
    valuedWhatYours: 'The figure the model discounts — YOURS.',
    valuedWhat: 'The figure the model actually discounts.',
    valuedWhereYours: 'Yours, typed here.', valuedWhere: 'Computed from the rows above.',
    valuedWhenYours: 'Whatever period you mean it to be.',
    valuedHowOverridden: 'Typed in, so the corrections above do not apply to it.',
    valuedHowNormOff: 'Normalise is off, so this is the base free cash flow unchanged — stock '
      + 'compensation is not deducted and growth capex is not added back.',
    valuedHowAllRan: 'Every correction ran; the rows above are the whole of it.',
    valuedHowPartial: (which) => `${v(which)} not reported, so that correction did not run — an `
      + 'absent line is not a zero.',
    correctionSbc: 'Stock compensation', correctionCapexDep: 'Capex or depreciation',
    baseNotUsedNoFcf: 'No free cash flow line is ingested for this company.',
    baseNotUsedNoConsensus: 'No consensus operating cash flow, or no capex to net off it, so no '
      + "forward base can be derived. Fewer than a fifth of a broad index's members carry a "
      + 'consensus at all.',
    baseNotUsedCompare: 'For comparison only; nothing here is computed from it. Switch the Base '
      + 'control above to value it instead. A large gap is the year analysts expect beside the '
      + 'year the company had.',
    guruFocusLess: (a, b) => `GuruFocus, ${v(a)} less ${v(b)}.`,
    closeOn: (date) => `close ${v(date)}`, sharesOn: (date) => `shares ${v(date)}`,
    noDatesStored: 'No dates stored.', houseDefault: 'House default.',
    noWaccStored: 'House default — no WACC stored.',
    impliedHow: 'Bisected on demand; there is no closed form for g. Not a valuation — what you '
      + 'would have to believe.',
    impliedNonPositive: (fcf) => `Free cash flow of ${v(fcf)} is at or below zero, so no growth `
      + 'rate works — a fact about the company, not an error.',
    impliedMissing: (what) => `Not enough inputs: no ${v(what)} ingested.`,
    impliedRateTooLow: 'The discount rate must exceed the perpetuity growth — the terminal value '
      + 'divides by the gap.',
    impliedNoRate: 'No rate between −99% and 1000% a year reconciles that market cap with this '
      + 'cash flow.',
    analystsPrefix: 'analysts ',
    analystEps: 'EPS 3-5y', analystOcf: 'OCF/sh 3-5y', analystAvg: 'Avg',
    analystEpsTip: 'Analysts’ 3–5 year EPS growth consensus (GuruFocus “Future 3-5Y EPS Growth '
      + 'Rate Estimate”). A forecast, not a solve.',
    analystOcfTip: 'Analysts’ 3–5 year operating-cash-flow-per-share growth consensus. OCF, not '
      + 'free cash flow: it runs ahead by whatever capex the company spends.',
    analystAvgTip: 'The plain mean of the two rates to the left. Blank unless both are present.'
      + '\n\nIt averages two different metrics — an earnings rate and a cash-flow-per-share rate '
      + '— so it is a rough centre of what analysts expect, not a consensus for any one line.',
    legend: {
      ocfEst: (fy) => `consensus operating cash flow for ${v(fy)}`,
      capexFiled: 'capital expenditure, last filed',
      F: 'the cash flow valued — paid in full in year 1, then grown',
      Fforward: (fy) => `free cash flow for ${v(fy)}`,
      Ffiled: 'free cash flow as filed',
      S: 'stock-based compensation', G: 'growth capex, the row above',
      C: 'capital expenditure, as filed — a negative outflow',
      D: 'cash-flow depreciation, the maintenance-capex proxy',
      g: 'the unknown — the rate this solves for', r: 'the discount rate',
      gInf: 'perpetuity growth, after year n', M: 'the target market cap',
      p0: 'the latest close', N: 'diluted shares outstanding, in millions',
    },
  },

  dcfModal: {
    title: 'Reverse DCF — the data it reads',
    companyFigures: 'Company figures',
    asFiled: 'The latest observation of each line, as filed and unadjusted — except the last '
      + 'estimate row, whose period is in the FUTURE and which is a consensus, not a filing.',
    colInput: 'Input', colValue: 'Value', colPeriod: 'Period', colMetricCode: 'Metric code',
    whatTheyAddUpTo: 'What they add up to',
    marketCap: 'Market cap', cashFlowCompounded: 'Cash flow compounded',
    cashFlowOverridden: 'Cash flow overridden to', solvingAgainst: 'Solving against',
    model: 'Model',
    rowSharePrice: 'Share price', rowShares: 'Shares outstanding (m)',
    rowFcf: 'Free cash flow', rowOcfEst: 'Operating cash flow (next FY, est.)',
    rowWacc: 'WACC (%)',
    ttmTo: (date) => `TTM to ${date}`,
    modelLine: (years, discount, perpetuity) =>
      `${years}y at g, discounted ${discount}, then ${perpetuity} in perpetuity`,
  },

  egmModal: {
    title: 'Assumptions — the data behind the defaults',
    growthSection: 'Growth rate — “analysts”',
    growthNote: 'The growth the consensus EPS estimates imply, first future period to last. '
      + '⚠ Not a published long-term rate — GuruFocus files that as a single number with no date, '
      + 'so it never reaches our database.',
    noEstimates: 'No consensus EPS estimates ingested.',
    colFiscalPeriod: 'Fiscal period', colEpsEstimate: 'EPS estimate',
    from: 'from', to: 'to',
    noCagrPrefix: 'No CAGR: ',
    noCagrOnePoint: 'only one future estimate is ingested.',
    noCagrNonPositive: 'the first or last estimate is not positive, so there is nothing to '
      + 'compound out of.',
    peSection: 'Exit P/E — “5y median P/E”',
    peNote: 'Each year’s closing price over that year’s EPS excluding non-recurring items, then '
      + 'the median. Derived — GuruFocus’s own P/E line isn’t ingested.',
    noPriceHistory: 'No price / EPS history ingested.',
    colFiscalYear: 'Fiscal year', colYearEndPrice: 'Year-end price', colEpsNri: 'EPS w/o NRI',
    colPE: 'P/E',
    excluded: 'excluded',
    excludedTitle: 'Excluded — no positive EPS, so no meaningful multiple.',
    medianOfUsable: (n) => `Median of ${n} usable year(s)`,
    yieldSection: 'Dividend yield — “reported”',
    yieldNote: '⚠ Not an average — the single most recent observation, picked by date across the '
      + 'annual and quarterly rows (both carry the same annualised measure). It is trailing '
      + 'dividends over the price at that period end, so it ages as the price moves.',
    noYieldLine: 'No dividend-yield line ingested — the model assumes a non-payer at 0%.',
    colPeriodEnd: 'Period end', colCadence: 'Cadence', colDividendYield: 'Dividend yield %',
    inUse: '← in use', quarterly: 'quarterly', annual: 'annual',
    showingMostRecent: (shown, total) =>
      `Showing the ${shown} most recent of ${total} observations.`,
  },
};

const nl: DeepValuationCopy = {
  common: { guruFocus: (vendor) => `GuruFocus, ${v(vendor)}.` },
  egm: {
    reset: 'Zet elke aanname terug op de standaardwaarde',
    growthRate: 'Groeivoet', exitPE: 'Exit-K/W', sharePriceNow: 'Koers nu',
    forwardPE: 'Forward K/W', hurdleRate: 'Rendementseis', dividendYield: 'Dividendrendement',
    showRawData: 'Toon de brongegevens achter deze standaardwaarden',
    expectedReturn: 'Verwacht rendement', priceTarget: 'Koersdoel',
    totalPriceMove: 'Totale koersbeweging',
    atYourHurdle: 'Bij uw rendementseis',
    maxPE: 'Max. K/W', fairValue: 'Reële waarde',
    legGrowth: 'Groei', legYield: 'Dividendrendement', legMultiple: 'Herwaardering',
    cards: {
      growth: {
        what: 'Aangenomen groei van de winst per aandeel.',
        where: 'De uwe, of de huisstandaard als het veld leeg is.',
      },
      exitPE: {
        what: 'Aangenomen K/W op het moment van verkoop.',
        where: 'De uwe, of de huisstandaard als het veld leeg is.',
      },
      price: {
        what: 'De koers waarvandaan elk rendement op dit paneel wordt gemeten.',
        how: 'Leeg gebruikt de opgeslagen slotkoers. Elk rendement op het paneel beweegt hiermee '
          + 'mee.',
      },
      forwardPE: {
        what: 'De multiple waar het herwaarderingsdeel VANAF loopt.',
      },
      hurdle: {
        what: 'Het rendement dat u eist.',
        when: 'Per jaar.',
      },
      dividend: {
        what: 'Aangenomen dividendrendement.',
        how: 'Procent per jaar. Brongegeven, geen formule.',
      },
      expectedReturn: {
        what: 'Het jaarlijkse TOTAALrendement dat deze aannames impliceren — dividend inbegrepen.',
        where: 'Berekend uit de drie regels hierboven.',
      },
      priceTarget: {
        what: 'De koers aan het einde van het venster.',
        where: 'Berekend uit de koers en de twee aannames.',
      },
      priceMove: {
        what: 'De totale koersbeweging over het venster.',
        where: 'Berekend uit de twee regels hierboven.',
      },
      maxPE: {
        what: 'Het meeste dat u vandaag kunt betalen en toch uw rendementseis haalt.',
        where: 'De exit-multiple, teruggerekend tegen de rendementseis in plaats van tegen de '
          + 'markt.',
        how: '⚠ EEN ANDER MODEL DAN HET RENDEMENT HIERBOVEN, op dezelfde aannames. Dat vraagt wat '
          + 'de koers van vandaag u oplevert; dit vraagt wat u mag betalen. Hoger dan de '
          + 'exit-multiple wanneer groei en dividend de rendementseis overtreffen.',
      },
      fairValue: {
        what: 'Die multiple, op de winst van volgend jaar.',
        where: 'De consensus-EPS voor volgend jaar × de max. K/W ernaast.',
      },
    },
    everyYearFor: (years) => `Elk jaar, gedurende ${v(years)} jaar.`,
    endOfYear: (years) => `Einde van jaar ${v(years)}.`,
    perYearOver: (years) => `Per jaar, over ${v(years)} jaar.`,
    yearsOut: (years) => `${v(years)} jaar vooruit.`,
    overYearsNotAnnualised: (years) => `Over ${v(years)} jaar, niet geannualiseerd.`,
    houseDefault: (value) => `Standaard ${v(value)}, een huiswaarde`,
    analystsImply: (value) => `; analisten impliceren ${v(value)}`,
    medianIs: (value) => `; de vijfjaarsmediaan is ${v(value)}`,
    reratingRuns: (from, to) => ` Het herwaarderingsdeel loopt van ${v(from)} naar ${v(to)}.`,
    analystHint: 'De door analisten geïmpliceerde groei — de CAGR van de consensus-EPS-ramingen, '
      + 'niet een gepubliceerde langetermijnvoet. Klik om te gebruiken.',
    medianPEHint: 'De eigen mediane K/W van deze onderneming over de afgelopen vijf jaar. Klik om '
      + 'te gebruiken.',
    yoursTypedHere: 'De uwe, hier ingetypt.',
    yoursOrDefault: 'De uwe, of de huisstandaard als het veld leeg is.',
    whateverMoment: 'Welk moment u er ook mee bedoelt.',
    daysOld: (n) => `${n} dagen oud`,

    legEarningsGrowth: 'Winstgroei', legMultipleWord: 'Multiple',
    impliedIn: (years) => `Geïmpliceerd over ${years} jr`,
    storedCloseBack: 'De opgeslagen slotkoers. Klik om terug te gaan.',
    storedCloseInUse: 'De opgeslagen slotkoers — in gebruik.',
    impliedPEHint: 'Koers ÷ consensus-EPS van volgend jaar — de multiple die de markt werkelijk '
      + 'betaalt, hier berekend in plaats van van de leverancier overgenomen. Klik om te gebruiken.',
    dividendBack: 'Het door GuruFocus gerapporteerde rendement. Klik om terug te gaan.',
    dividendInUse: 'Het door GuruFocus gerapporteerde rendement — in gebruik. Klik om het in het '
      + 'veld te zetten en er vanaf te bewerken.',
    priceTyped: 'De koers die u hebt ingetypt, geen slotkoers.',
    priceClosingOf: (name) => `Slotkoers van ${name}.`,
    priceWhereTyped: 'De uwe, ingetypt in het veld Koers nu.',
    loading: 'Laden…',
    yahooFinance: (symbol) => `Yahoo Finance${symbol ? `, ${v(symbol)}` : ''}.`,
    noneStored: 'Niets opgeslagen.',
    noObservationStored: 'Geen waarneming opgeslagen.',
    clearBoxToGoBack: (price) =>
      `Maak het veld Koers nu leeg om terug te gaan naar de opgeslagen slotkoers${price ? `, ${v(price)}` : ''}.`,
    rawDataYahoo: 'Brongegeven. Refresh haalt de nieuwste slotkoersen bij Yahoo op.',
    rawDataNoRefresh: 'Brongegeven. Refresh kan deze niet verversen.',
    reReadClose: 'Lees de opgeslagen slotkoers opnieuw', reReadCancel: 'Annuleer het opnieuw lezen',
    reReading: 'Bezig met opnieuw lezen — druk om te annuleren', cancelling: 'Annuleren…',
    reReadOverridden: 'Lees de opgeslagen slotkoers opnieuw — het veld Koers nu overschrijft hem, ',
    reReadStale: 'Deze slotkoers is ouder dan een week — lees hem opnieuw',
    reReadForwardPE: 'Vraag deze forward K/W opnieuw op bij GuruFocus',
    reReadNoCompany: 'Geen GuruFocus-onderneming voor deze ISIN, dus er valt niets opnieuw te lezen',
    reReadForwardPEOverridden: 'Vraag deze forward K/W opnieuw op bij GuruFocus — het veld '
      + 'overschrijft hem, dus het nieuwe cijfer verschijnt op de chip naast dat veld',
    forwardPEWhenTyped: 'Welk moment u er ook mee bedoelt.',
    forwardPEHowTyped: (vendor, date) => `Maak het veld leeg om terug te gaan naar ${vendor}, die `
      + `voor het laatst op ${date} publiceerde.`,
    forwardPEMoved: (date) => `forward K/W nu ${date}`,
    forwardPEUnchanged: (date) => `nog steeds ${date} — GuruFocus heeft niets nieuwers`,
    forwardPENone: 'GuruFocus gaf geen forward K/W voor deze onderneming',
    growthHow: (dflt) => `Procent per jaar. ${dflt}`,
    exitPEHow: (dflt) => `Een multiple, geen percentage. ${dflt}`,
    forwardPEHow: (years) => 'Forward, niet trailing — hij sluit aan op de groeivoet hierboven, die '
      + `vanaf FY1 loopt. Het multiple-deel is (exit ÷ deze) ^ (1/${v(years)}) per jaar.`,
    forwardPEDisagrees: (implied, vendor) =>
      `Koers ÷ consensus-EPS geeft ${v(implied)} tegenover ${v(vendor)} van de leverancier. De chip `
      + 'zet het eigen cijfer van de markt erin.',
    hurdleHow: (dflt) => `Bepaalt de reële waarde, niet het verwachte rendement. ${dflt}.`,
    factorsMultiply: (added, compounded) =>
      `De factoren VERMENIGVULDIGEN, dus de ×-kolom klopt en de %-kolom niet: ${v(added)} opgeteld `
      + `tegenover ${v(compounded)} samengesteld. Het dividendrendement tilt dit cijfer op maar niet `
      + 'de geïmpliceerde koers hieronder, die alleen het kapitaaldeel is.',
    noForwardPE: 'Geen bruikbare forward K/W, dus er valt niets te herwaarderen.',
    noCompoundingPath: 'Deze aannames hebben geen samenstellingspad. Controleer de exit-K/W, de '
      + 'groei en de rendementseis.',
    fairValueVsPrice: (pct) => `${v(pct)} ten opzichte van de koers van vandaag.`,
    fairValueNoEps: 'Geen consensus-EPS voor volgend jaar, dus er is niets om de multiple op toe '
      + 'te passen.',
    priceOnlyFairValue: (fair, mult) =>
      `Alleen koers; dividend zit in het rendement hieronder. De reële waarde bij de rendementseis `
      + `is ${v(fair)} (${v(mult)}).`,
    priceVsRerating: (implied, from) =>
      ` Koers ÷ EPS geeft ${v(implied)} tegenover de ${v(from)} waar de herwaardering vandaan loopt.`,
    priceLegOnly: (total) =>
      `Alleen het koersdeel. Met herbelegd dividend is het totaal ${v(total)}.`,
    noDividend: 'Geen dividend, dus dit is het volledige rendement.',
    legend: {
      g: 'aangenomen EPS-groei, per jaar',
      y: 'aangenomen dividendrendement, elk jaar toegepast',
      peExit: 'de multiple die u bij verkoop aanneemt',
      peFwd: 'de forward K/W waarvandaan wordt herwaardeerd',
      n: (years) => `de prognosehorizon, ${v(years)} jaar`,
      p0: 'de koers nu',
      p0Row: 'de koers nu — de regel hierboven',
      pn: (years) => `de geïmpliceerde koers, ${v(years)} jaar vooruit`,
      rerating: 'de volledige herwaardering, één keer toegepast — niet per jaar',
      h: 'het rendement dat u eist — uw invoer, geen feit over de onderneming',
      epsFY1: 'de consensus-winst per aandeel voor volgend jaar',
      maxPE: 'de multiple uit de regel hierboven',
    },
  },

  dcf: {
    title: 'Reverse DCF',
    subtitle: 'wat de koers impliceert, niet wat de onderneming waard is',
    lastReported: 'Laatst gerapporteerd', rawData: 'brongegevens ↗',
    impliedGrowth: 'Geïmpliceerde FCF-groei',
    impliedByDiscountRate: 'Geïmpliceerde groei per disconteringsvoet',
    reset: 'Zet elke invoer terug op de standaardwaarde',
    showFigures: 'Toon elk ondernemingscijfer dat dit leest, met de bron',
    useThisRate: 'Gebruik deze voet',
    rowSbc: '− Aandelenbeloning', rowCapex: 'Investeringen',
    rowDA: 'Afschrijvingen', rowGrowthCapex: '+ Groei-investeringen',
    rowDiscountRate: 'Disconteringsvoet', rowPerpetuityGrowth: 'Eeuwigdurende groei',
    rowForecastYears: 'Prognosejaren',
    cards: {
      sbc: { what: 'Aandelengerelateerde beloning, in mindering gebracht.' },
      capex: { what: 'Investeringen, als het uitgegeven bedrag.' },
      da: { what: 'Afschrijvingen en amortisatie uit het kasstroomoverzicht.' },
      growthCapex: {
        what: 'Investeringen boven de afschrijvingen, weer opgeteld.',
        where: 'Berekend uit de twee regels hierboven.',
      },
      baseNotUsed: { what: 'De basis die dit paneel NIET gebruikt.' },
      solvedAgainst: { what: 'De waardering waartegen is opgelost.' },
      marketCap: {
        where: 'Berekend uit de koers en het aantal aandelen.',
        how: 'In miljoenen, net als de kasstroom hierboven. Overschrijf het om te vragen wat een '
          + 'andere waardering zou moeten aannemen.',
      },
      discountRate: {
        what: 'De voet waartegen de kasstromen worden verdisconteerd.',
        how: 'Procent per jaar; moet hoger zijn dan de eeuwigdurende groei.',
      },
      perpetuityGrowth: {
        what: 'De groei na de prognosejaren.',
        where: 'Huisconventie.',
        how: 'Procent per jaar. Ruwe invoer, geen formule.',
      },
      forecastYears: {
        what: 'De lengte van de expliciete groeifase.',
        how: 'Een aantal jaren. Ruwe invoer, geen formule.',
      },
      impliedGrowth: {
        what: 'De FCF-groei die de beurswaarde al aanneemt.',
        where: 'Hier berekend.',
      },
    },
    fromYearOnwards: (year) => `Vanaf jaar ${v(year)}, eeuwigdurend.`,
    yearsOneTo: (years) => `Jaar 1 tot en met ${v(years)}, daarna de eeuwigdurende groei.`,
    yearsOneToPlain: (years) => `Jaar 1 tot en met ${v(years)}.`,

    base: 'Basis', normalise: 'Normaliseren',
    baseTitle: 'Welke kasstroom het model laat groeien.\n\n'
      + 'Volgend boekjaar: de consensus van analisten voor de operationele kasstroom van het '
      + 'komende boekjaar, minus investeringen — vrije kasstroom die de onderneming nog niet heeft '
      + 'verdiend.\n'
      + 'Laatst gerapporteerd: de vrije kasstroom precies zoals gerapporteerd over het meest '
      + 'recente boekjaar.\n\n'
      + 'De correcties voor aandelenbeloning en groei-investeringen hieronder werken op beide.',
    normaliseTitle: 'Waardeer de vrije kasstroom na aftrek van aandelenbeloning en vóór '
      + 'groei-investeringen.\n\n'
      + 'Aandelenbeloning wordt afgetrokken: het is een reële kostenpost die het kasstroomoverzicht '
      + 'nooit verlaat.\n'
      + 'Groei-investeringen (investeringen boven de afschrijvingen) worden WEER OPGETELD: de '
      + 'gerapporteerde vrije kasstroom heeft ze al afgetrokken, en ze kopen precies de groei '
      + 'waarvoor dit model oplost.',
    nextFY: 'Volgend boekjaar', nextFYNone: 'Volgend boekjaar (geen)',
    ttmNote: '\n\nDe laatste twaalf maanden waar vier kwartalen bestaan, anders het laatste '
      + 'volledige boekjaar — één venster voor alle vier de kasstroomregels.',
    normOff: '\n\nVink Normaliseren uit om in plaats daarvan het gerapporteerde cijfer te '
      + 'waarderen.',
    notLikeForLike: '\n\nNiet vergelijkbaar met de geïmpliceerde voet: een andere maatstaf '
      + '(vrije kasstroom tegenover winst en operationele kasstroom), een andere horizon (volledige '
      + 'prognose plus eeuwigdurend tegenover 3-5 jaar) en een andere grondslag (totaal tegenover '
      + 'per aandeel, waardoor inkoop van eigen aandelen deze verhoogt). Een controle op de orde '
      + 'van grootte, geen gelijkheid.',
    freeCashFlow: 'Vrije kasstroom', cashFlowValued: 'Gewaardeerde kasstroom',
    targetMarketCap: 'Doelbeurswaarde',
    vsReportedFcf: 'vs. gerapporteerde FCF',
    vsNextFyDerived: (fy) => `vs. FCF ${fy} (afgeleid)`,
    fcfWhatReported: 'De laatst gerapporteerde vrije kasstroom.',
    fcfWhatForward: (fy) => `Consensus vrije kasstroom voor ${fy}.`,
    fcfHowDirect: 'De eigen prognose van de analisten, gelezen en niet afgeleid — er is al een '
      + 'geraamde toekomstige investering van afgetrokken, wat ook is wat de pagina van GuruFocus '
      + 'toont. Jaar 1 is hun werk; elk jaar daarna is de voet waarvoor dit paneel oplost.',
    fcfHowDerived: 'Afgeleid: er is voor deze onderneming geen consensus voor de vrije kasstroom '
      + 'opgeslagen, dus het is de consensus voor de operationele kasstroom minus de laatst '
      + 'gerapporteerde investeringen. Dat investeringsdeel valt grotendeels weg tegen de regel '
      + 'groei-investeringen hieronder.',
    fcfHowReported: 'Operationele kasstroom minus de TOTALE investeringen, en daarom telt de regel '
      + 'groei-investeringen hieronder weer op in plaats van af te trekken.',
    inMillionsIs: (a, b) => ` In miljoenen: ${v(a)} is ${v(b)}.`,
    inMillions: ' In miljoenen.',
    nextFiscalYear: 'Volgend boekjaar.',
    sbcHow: 'Een reële kostenpost die het kasstroomoverzicht nooit verlaat: hij wordt als '
      + 'niet-kaskost weer bij de operationele kasstroom opgeteld, waardoor de gerapporteerde '
      + 'vrije kasstroom iedereen die in aandelen betaalt gunstiger voorstelt.',
    sbcAbsent: 'Niet gerapporteerd voor deze onderneming, dus er wordt niets afgetrokken — een '
      + 'ontbrekende regel is geen nul.',
    capexHow: 'De eerste van de twee regels die de regel groei-investeringen hieronder aftrekt.',
    daHow: 'De regel uit het kasstroomoverzicht, niet die uit de winst-en-verliesrekening. '
      + 'GuruFocus legt beide vast en ze verschillen; investeringen zijn een kasgetal, dus de '
      + 'benadering voor onderhoudsinvesteringen moet dat ook zijn.',
    growthCapexAdded: 'Opgeteld, niet afgetrokken: de basis hierboven heeft alle investeringen '
      + 'er al uit gehaald. Onderhoudsinvesteringen houden de onderneming in stand; het meerdere '
      + 'koopt de groei waarvoor dit model oplost, dus het erin laten brengt dezelfde uitbreiding '
      + 'twee keer in rekening.',
    growthCapexHow: 'Afschrijvingen zijn een benadering van de onderhoudsinvesteringen, het zwakst '
      + 'voor een onderneming die voor het eerst een activabasis opbouwt. Afgekapt op nul, zodat '
      + 'onderinvestering niet als meevaller wordt gelezen.',
    growthCapexAbsent: 'Investeringen of afschrijvingen uit het kasstroomoverzicht zijn voor deze '
      + 'onderneming niet gerapporteerd, dus er wordt niets weer opgeteld — een ontbrekende regel '
      + 'is geen nul.',
    valuedWhatYours: 'Het cijfer dat het model verdisconteert — het UWE.',
    valuedWhat: 'Het cijfer dat het model werkelijk verdisconteert.',
    valuedWhereYours: 'De uwe, hier ingetypt.',
    valuedWhere: 'Berekend uit de regels hierboven.',
    valuedWhenYours: 'Welke periode u er ook mee bedoelt.',
    valuedHowOverridden: 'Zelf ingetypt, dus de correcties hierboven zijn er niet op toegepast.',
    valuedHowNormOff: 'Normaliseren staat uit, dus dit is de basis-vrije-kasstroom ongewijzigd — '
      + 'aandelenbeloning wordt niet afgetrokken en groei-investeringen worden niet weer opgeteld.',
    valuedHowAllRan: 'Elke correctie is uitgevoerd; de regels hierboven zijn het geheel.',
    valuedHowPartial: (which) => `${v(which)} niet gerapporteerd, dus die correctie is niet `
      + 'uitgevoerd — een ontbrekende regel is geen nul.',
    correctionSbc: 'Aandelenbeloning', correctionCapexDep: 'Investeringen of afschrijvingen',
    baseNotUsedNoFcf: 'Voor deze onderneming is geen regel vrije kasstroom ingelezen.',
    baseNotUsedNoConsensus: 'Geen consensus voor de operationele kasstroom, of geen investeringen '
      + 'om ervan af te trekken, dus er valt geen toekomstige basis af te leiden. Minder dan een '
      + 'vijfde van de leden van een brede index draagt überhaupt een consensus.',
    baseNotUsedCompare: 'Alleen ter vergelijking; hier wordt niets uit berekend. Zet de '
      + 'Basis-keuze hierboven om om hem in plaats daarvan te waarderen. Een groot verschil is het '
      + 'jaar dat analisten verwachten naast het jaar dat de onderneming had.',
    guruFocusLess: (a, b) => `GuruFocus, ${v(a)} minus ${v(b)}.`,
    closeOn: (date) => `slotkoers ${v(date)}`, sharesOn: (date) => `aandelen ${v(date)}`,
    noDatesStored: 'Geen datums opgeslagen.', houseDefault: 'Huisstandaard.',
    noWaccStored: 'Huisstandaard — geen WACC opgeslagen.',
    impliedHow: 'Op verzoek via bisectie bepaald; er is geen gesloten vorm voor g. Geen waardering '
      + '— wat u zou moeten geloven.',
    impliedNonPositive: (fcf) => `Een vrije kasstroom van ${v(fcf)} is nul of negatief, dus geen `
      + 'enkele groeivoet werkt — een feit over de onderneming, geen fout.',
    impliedMissing: (what) => `Te weinig invoer: geen ${v(what)} ingelezen.`,
    impliedRateTooLow: 'De disconteringsvoet moet hoger zijn dan de eeuwigdurende groei — de '
      + 'eindwaarde deelt door het verschil.',
    impliedNoRate: 'Geen enkele voet tussen −99% en 1000% per jaar rijmt die beurswaarde met deze '
      + 'kasstroom.',
    analystsPrefix: 'analisten ',
    analystEps: 'EPS 3-5 jr', analystOcf: 'OCF/aandeel 3-5 jr', analystAvg: 'Gem.',
    analystEpsTip: 'De consensus van analisten voor de EPS-groei over 3–5 jaar (GuruFocus “Future '
      + '3-5Y EPS Growth Rate Estimate”). Een prognose, geen oplossing.',
    analystOcfTip: 'De consensus van analisten voor de groei van de operationele kasstroom per '
      + 'aandeel over 3–5 jaar. OCF, geen vrije kasstroom: hij loopt vooruit met wat de '
      + 'onderneming aan investeringen uitgeeft.',
    analystAvgTip: 'Het gewone gemiddelde van de twee voeten links. Leeg tenzij beide aanwezig '
      + 'zijn.\n\nHet middelt twee verschillende maatstaven — een winstvoet en een '
      + 'kasstroom-per-aandeelvoet — dus het is een ruw midden van wat analisten verwachten, geen '
      + 'consensus voor één afzonderlijke regel.',
    legend: {
      ocfEst: (fy) => `consensus operationele kasstroom voor ${v(fy)}`,
      capexFiled: 'investeringen, laatst gerapporteerd',
      F: 'de gewaardeerde kasstroom — in jaar 1 volledig ontvangen, daarna groeiend',
      Fforward: (fy) => `vrije kasstroom voor ${v(fy)}`,
      Ffiled: 'vrije kasstroom zoals gerapporteerd',
      S: 'aandelengerelateerde beloning', G: 'groei-investeringen, de regel hierboven',
      C: 'investeringen, zoals gerapporteerd — een negatieve uitstroom',
      D: 'afschrijvingen uit het kasstroomoverzicht, de benadering voor onderhoudsinvesteringen',
      g: 'de onbekende — de voet waarvoor dit oplost', r: 'de disconteringsvoet',
      gInf: 'eeuwigdurende groei, na jaar n', M: 'de doelbeurswaarde',
      p0: 'de laatste slotkoers', N: 'uitstaande verwaterde aandelen, in miljoenen',
    },
  },

  dcfModal: {
    title: 'Reverse DCF — de gegevens die het leest',
    companyFigures: 'Ondernemingscijfers',
    asFiled: 'De meest recente waarneming van elke regel, zoals gerapporteerd en ongecorrigeerd — '
      + 'behalve de laatste ramingsregel, waarvan de periode in de TOEKOMST ligt en die een '
      + 'consensus is, geen rapportage.',
    colInput: 'Invoer', colValue: 'Waarde', colPeriod: 'Periode', colMetricCode: 'Metriekcode',
    whatTheyAddUpTo: 'Waar ze bij elkaar op uitkomen',
    marketCap: 'Beurswaarde', cashFlowCompounded: 'Samengestelde kasstroom',
    cashFlowOverridden: 'Kasstroom overschreven naar', solvingAgainst: 'Oplossen tegen',
    model: 'Model',
    rowSharePrice: 'Koers', rowShares: 'Uitstaande aandelen (mln)',
    rowFcf: 'Vrije kasstroom', rowOcfEst: 'Operationele kasstroom (volgend boekjaar, raming)',
    rowWacc: 'WACC (%)',
    ttmTo: (date) => `TTM tot ${date}`,
    modelLine: (years, discount, perpetuity) =>
      `${years} jaar tegen g, verdisconteerd tegen ${discount}, daarna ${perpetuity} eeuwigdurend`,
  },

  egmModal: {
    title: 'Aannames — de gegevens achter de standaardwaarden',
    growthSection: 'Groeivoet — “analisten”',
    growthNote: 'De groei die de consensus-EPS-ramingen impliceren, van de eerste toekomstige '
      + 'periode tot de laatste. ⚠ Geen gepubliceerde langetermijnvoet — GuruFocus legt die vast '
      + 'als één getal zonder datum, waardoor het onze database nooit bereikt.',
    noEstimates: 'Geen consensus-EPS-ramingen ingelezen.',
    colFiscalPeriod: 'Boekperiode', colEpsEstimate: 'EPS-raming',
    from: 'van', to: 'tot',
    noCagrPrefix: 'Geen CAGR: ',
    noCagrOnePoint: 'er is maar één toekomstige raming ingelezen.',
    noCagrNonPositive: 'de eerste of laatste raming is niet positief, dus er valt niets uit samen '
      + 'te stellen.',
    peSection: 'Exit-K/W — “vijfjaarsmediaan K/W”',
    peNote: 'De slotkoers van elk jaar gedeeld door de winst per aandeel van dat jaar exclusief '
      + 'bijzondere posten, en daarvan de mediaan. Afgeleid — de eigen K/W-regel van GuruFocus '
      + 'wordt niet ingelezen.',
    noPriceHistory: 'Geen koers-/EPS-historie ingelezen.',
    colFiscalYear: 'Boekjaar', colYearEndPrice: 'Slotkoers jaareinde',
    colEpsNri: 'EPS excl. bijz. posten', colPE: 'K/W',
    excluded: 'uitgesloten',
    excludedTitle: 'Uitgesloten — geen positieve EPS, dus geen betekenisvolle multiple.',
    medianOfUsable: (n) => `Mediaan van ${n} bruikbaar jaar/jaren`,
    yieldSection: 'Dividendrendement — “gerapporteerd”',
    yieldNote: '⚠ Geen gemiddelde — de enkele meest recente waarneming, gekozen op datum over de '
      + 'jaar- en kwartaalregels heen (beide dragen dezelfde geannualiseerde maatstaf). Het is het '
      + 'dividend over de afgelopen twaalf maanden gedeeld door de koers aan het einde van die '
      + 'periode, dus het veroudert naarmate de koers beweegt.',
    noYieldLine: 'Geen dividendrendementsregel ingelezen — het model gaat uit van een niet-uitkerende '
      + 'onderneming met 0%.',
    colPeriodEnd: 'Einde periode', colCadence: 'Frequentie', colDividendYield: 'Dividendrendement %',
    inUse: '← in gebruik', quarterly: 'per kwartaal', annual: 'per jaar',
    showingMostRecent: (shown, total) =>
      `De ${shown} meest recente van ${total} waarnemingen worden getoond.`,
  },
};

export const DEEP_VALUATION_COPY: Record<Lang, DeepValuationCopy> = { en, nl };

/**
 * The copy for the reader's current language.
 *
 * ⚠ A HOOK, NOT A `t('some.key')` LOOKUP — the key path is checked by the compiler this way, and a
 * string key is checked by nobody and fails at runtime as an empty cell. It is also why the tree is
 * nested: `t.dcf.rowCapex` reads as the surface it belongs to, so a call site cannot borrow another
 * panel's string by accident.
 */
export function useDeepValuationCopy(): DeepValuationCopy {
  const [lang] = useLang();
  return DEEP_VALUATION_COPY[lang];
}
