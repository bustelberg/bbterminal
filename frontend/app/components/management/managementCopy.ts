'use client';

import { useLang, type Lang } from '../../../lib/i18n';

/**
 * THE /management-dashboard COPY, IN BOTH LANGUAGES.
 *
 * ⚠⚠ ENGLISH IS THE SOURCE, DUTCH IS THE TRANSLATION, AND A MISSING DUTCH STRING IS A COMPILE
 * ERROR. `nl` is typed as `ManagementCopy`, so a key added to `en` and forgotten here fails `tsc`
 * rather than falling back — a half-translated panel renders as a rendering bug, not as an
 * unfinished translation, and nobody reports it as the latter. Same rule `tablesCopy` set.
 *
 * ⚠ THE GUARANTEE IS PER SURFACE, WHICH IS WHAT MAKES THIS SHIPPABLE AT ALL. There are ~300 visible
 * strings on this page across ~20 components; requiring all of them before any of them would mean
 * one enormous change nobody can review. Each section below is one surface, complete in both
 * languages the moment it exists — so a panel is either fully Dutch or fully English, never half.
 * The sections still missing are listed at the bottom of this file, deliberately, so "what is left"
 * is a fact in the code rather than something to re-derive by clicking around.
 *
 * ⚠⚠ AIRS'S OWN FIELD NAMES ARE NOT IN HERE AND MUST NOT BE. `Beginwaarde`, `Huidige waarde`,
 * `Werkelijk`, `Asset allocatie`, `Res. YtD` are the column names AirSPMS itself uses. They are
 * already Dutch, they appear identically in the English UI, and that is correct: they are the names
 * of the SOURCE FIELDS, so a reader reconciling this screen against AIRS matches them by eye.
 * "Translating" them to English would break that link in the English UI, and re-translating them in
 * the Dutch one would imply we had renamed something AIRS owns.
 *
 * ⚠ NEITHER ARE THE ⓘ PROVENANCE CARDS (`what`/`where`/`when`/`how`), by decision (2026-08-21): the
 * scope is what a reader sees without hovering. Those are ~480 further strings of multi-sentence
 * prose and are a separate piece of work; they stay English until asked for.
 */

/** One tab of the page's segmented control: the button and the hover that explains it. */
type TabCopy = { label: string; note: string };

export type ManagementCopy = {
  page: {
    title: string;
    tabs: { overview: TabCopy; cross: TabCopy; benchmarks: TabCopy };
  };
  common: {
    loading: string;
    computing: string;
    cancel: string;
    refresh: string;
    refreshAll: string;
    /** The AIRS walk: its verb while running, and the button once there are rows. */
    scanning: string;
    refreshFromAirs: string;
    asOf: string;
  };
  benchmarks: {
    title: string;
    colBenchmark: string;
    colMembers: string;
    colYtdEur: string;
    colYtdLocal: string;
    splitAdjusted: string;
  };
  correlation: {
    title: string;
    noPortfolios: string;
  };
  overview: {
    colClass: string;
    colWeight: string;
    colReturn: string;
    colSector: string;
    colRegion: string;
    colIsin: string;
    colLink: string;
    colFund: string;
    colModelWeight: string;
    colStartWeight: string;
    colDirectResult: string;
    colDivTax: string;
    currentPortfolio: string;
    weightReturnsBy: string;
    autoCalculated: string;
    /** The toolbar's two buttons. The refresh is ONE control with four states, so they live
     *  together — a translated label beside an English "Refreshing…" is the half-done look the
     *  per-surface rule exists to avoid. */
    refreshAll: string;
    refreshing: string;
    scanningModels: string;
    cancelScan: string;
    cancelModelScan: string;
    /** ⚠ OUR name for the bands policy, NOT an AIRS column — see the guard in the test. */
    allocationBands: string;
    allocationBandsHint: string;
    loadingHoldings: string;
    noSnapshot: string;
  };

  /** The stored-models list (`PortfoliosPanel`) — the page's other table. */
  models: {
    loading: string;
    emptyBefore: string;
    scanAirs: string;
    searchPlaceholder: string;
    namePlaceholder: string;
    colFixedDate: string;
    colStartDate: string;
    colAirsBook: string;
    notAnInstrument: string;
    unresolvedSeries: string;
    noSnapshot: string;
    notAPortfolio: string;
    notInGrid: string;
    partialYear: string;
    notComputed: string;
    notCounted: string;
    saveFailed: string;
    positionsSource: string;
    forgetChoice: string;
    fetchedLive: string;
    cashNoAccounts: string;
    cashNoIsin: string;
    viaLinkedModel: string;
    /** Hovers. ⚠ Translated too — the reader hovering is the same reader. */
    hintNoBook: string;
    hintComposition: string;
    hintEffectiveDate: string;
    hintClippedName: string;
    hintYtd: string;
    hintYtdSource: string;
    hintSinceInception: string;
    hintInceptionSource: string;
    hintAnnualised: string;
    hintEmptyFixed: string;
    hintNotAnInstrument: string;
  };

  /** The account's own reconciliation (`AccountTotalReturn`). */
  accountReturn: {
    title: string;
    couldNotLoad: string;
    heldPlusSold: string;
    loadTxFirst: string;
    soldHeld: (sold: number, held: number) => string;
    needsTx: string;
    openTransactions: string;
    /** The clause AFTER the bold control name. ⚠ Its own key, not a
     *  concatenation: Dutch puts the verb somewhere else. */
    needsTxTail: string;
    reload: string;
    reloading: string;
    rowHeld: string;
    rowRealised: string;
    rowIncomeSold: string;
    subPriced: (priced: number, unpriced: number) => string;
    subRealised: (n: number) => string;
    subNone: string;
    totalResult: string;
    totalResultSub: string;
    airsResult: string;
    airsResultSub: string;
    totalYtd: string;
    airsYtd: string;
    closedOut: string;
  };
};

const en: ManagementCopy = {
  page: {
    title: 'Management Dashboard',
    tabs: {
      overview: {
        label: 'Overview',
        note: 'Each portfolio on its own — its holdings, weights and returns.',
      },
      cross: {
        label: 'Cross-portfolio',
        note: 'How the portfolios move together. A pairwise view: no single portfolio has this number.',
      },
      benchmarks: {
        label: 'Benchmarks',
        note: 'The indices the portfolios are measured against, cap-weighted and rebuilt from our own constituents.',
      },
    },
  },
  common: {
    loading: 'Loading…',
    computing: 'Computing…',
    cancel: 'Cancel',
    refresh: 'Refresh',
    refreshAll: 'Refresh all',
    scanning: 'Scanning…',
    refreshFromAirs: 'Refresh from AIRS',
    asOf: 'As of',
  },
  benchmarks: {
    title: 'Benchmarks',
    colBenchmark: 'Benchmark',
    colMembers: 'Members',
    colYtdEur: 'YTD (€)',
    colYtdLocal: 'YTD (local)',
    splitAdjusted: 'Split-adjusted on the fly:',
  },
  correlation: {
    title: 'Portfolio correlations',
    noPortfolios: 'No model portfolio is offered at',
  },
  overview: {
    colClass: 'Class',
    colWeight: 'Weight',
    colReturn: 'Return',
    colSector: 'Sector',
    colRegion: 'Region',
    colIsin: 'ISIN',
    colLink: 'Link',
    colFund: 'Fund',
    colModelWeight: 'Model wt',
    colStartWeight: 'Start wt',
    colDirectResult: 'Direct result',
    colDivTax: 'Div tax',
    currentPortfolio: 'Current portfolio',
    weightReturnsBy: 'Weight returns by',
    autoCalculated: 'Auto (calculated)',
    refreshAll: 'Refresh all',
    refreshing: 'Refreshing…',
    scanningModels: 'Scanning models…',
    cancelScan: 'Cancel scan',
    cancelModelScan: 'Cancel model scan',
    allocationBands: 'Asset allocation',
    allocationBandsHint: 'Per risk profile, the minimum, default and maximum share each asset '
      + 'class may take.',
    loadingHoldings: 'Loading holdings…',
    noSnapshot: 'No holdings snapshot stored.',
  },
  models: {
    loading: 'Loading stored portfolios…',
    emptyBefore: 'Nothing stored yet — hit',
    scanAirs: 'Scan AIRS',
    searchPlaceholder: 'Search name / description…',
    namePlaceholder: 'a name you choose…',
    colFixedDate: 'Fixed date',
    colStartDate: 'Start date',
    colAirsBook: 'AIRS book',
    notAnInstrument: 'This ISIN is not an instrument in our grid — usually an in-house fund, so there is no listing to resolve.',
    unresolvedSeries: 'This ISIN is not an instrument in our grid, so we have no price series for it.',
    noSnapshot: 'no snapshot',
    notAPortfolio: '— not a portfolio —',
    notInGrid: 'not in grid',
    partialYear: 'partial year',
    notComputed: 'Not computed yet.',
    notCounted: 'Not counted yet — the scan is still walking the portfolios.',
    saveFailed: 'Save failed — not stored.',
    positionsSource: 'Positions source',
    forgetChoice: 'Forget this manual choice and fall back to the automatic guess.',
    fetchedLive: 'Fetched live from AirSPMS just now.',
    cashNoAccounts: 'Cash has no accounts to read.',
    cashNoIsin: 'Cash — no ISIN exists for it.',
    viaLinkedModel: 'priced via the linked model portfolio',
    hintNoBook: 'No AIRS book is paired with this model, so there are no book holdings to value.',
    hintComposition: 'Composition of this model — sector, region and currency — beside the '
      + 'benchmark, on one set of groups.',
    hintEffectiveDate: 'The model’s own effective date — when this composition took effect.',
    hintClippedName: 'This value is CLIPPED, not the real portfolio name.',
    hintYtd: 'What this model has returned so far this year, holding its current weights.',
    hintYtdSource: 'asset_price close, EUR via fx_rate',
    hintSinceInception: 'What this model has returned since the day its composition took effect.',
    hintInceptionSource: 'asset_price daily EUR curve',
    hintAnnualised: 'The model’s return restated as a per-year rate, so different ages compare.',
    hintEmptyFixed: 'A fixed model that contains no instruments — genuinely empty, not un-counted.',
    hintNotAnInstrument: 'This ISIN is not an instrument in our grid — usually an in-house fund.',
  },
  accountReturn: {
    title: 'Total return',
    couldNotLoad: 'could not load',
    heldPlusSold: 'held + sold, against the book’s own',
    loadTxFirst: 'load the transactions above first',
    soldHeld: (sold, held) => `${sold} sold · ${held} held`,
    needsTx: 'This book’s transactions have not been fetched, so what it realised on sales is '
      + 'unknown — and the positions it still holds are only part of the year. Open',
    openTransactions: 'Transactions',
    needsTxTail: 'above to load them, then reload here.',
    reload: 'Reload',
    reloading: 'Reloading…',
    rowHeld: 'Positions still held',
    rowRealised: 'Realised on sales this year',
    rowIncomeSold: 'Income from names no longer held',
    subPriced: (priced, unpriced) =>
      `${priced} priced${unpriced ? `, ${unpriced} without an opening value` : ''}`,
    subRealised: (n) => `${n} instrument${n === 1 ? '' : 's'} · AIRS’s own Res. YtD`,
    subNone: 'none',
    totalResult: 'Total result',
    totalResultSub: 'the year, from the positions',
    airsResult: 'AIRS’s own result',
    airsResultSub: 'beleggingsresultaat — the system of record',
    totalYtd: 'Total YTD return',
    airsYtd: 'AIRS’s own YTD',
    closedOut: 'No longer in the positions table — this position was closed out.',
  },
};

/**
 * ⚠ THE TERMS ARE THE ONES AIRS AND THE READER ALREADY USE, not the dictionary's. "Benchmark",
 * "Sector" and "ISIN" are the Dutch words too — a reader of Dutch financial copy expects them, and
 * inventing "ijkpunt" for benchmark would be a translation nobody in this domain writes.
 *
 * ⚠ `Fonds` FOR `Fund`, BUT THE CLASS DIVISION SAYS `Stock ETFs` — see `equityParts`. That label
 * names a kind of instrument the Dutch market also calls an ETF; "aandelenfondsen" would be wider
 * than what the flag means (it is specifically the exchange-traded ones).
 *
 * ⚠ `Rendement` IS RETURN, NOT `Opbrengst`. Opbrengst is proceeds — an amount — where this column
 * is a rate. The two are confusable in exactly the place it matters, a column of percentages.
 */
const nl: ManagementCopy = {
  page: {
    title: 'Managementdashboard',
    tabs: {
      overview: {
        label: 'Overzicht',
        note: 'Elke portefeuille apart — de posities, wegingen en rendementen.',
      },
      cross: {
        label: 'Portefeuille-onderling',
        note: 'Hoe de portefeuilles samen bewegen. Een paarsgewijze weergave: geen enkele portefeuille heeft dit getal op zichzelf.',
      },
      benchmarks: {
        label: 'Benchmarks',
        note: 'De indices waaraan de portefeuilles worden afgemeten, marktkapitalisatie-gewogen en opnieuw opgebouwd uit onze eigen constituenten.',
      },
    },
  },
  common: {
    loading: 'Laden…',
    computing: 'Berekenen…',
    cancel: 'Annuleren',
    refresh: 'Vernieuwen',
    refreshAll: 'Alles vernieuwen',
    scanning: 'Scannen…',
    refreshFromAirs: 'Vernieuwen vanuit AIRS',
    asOf: 'Per',
  },
  benchmarks: {
    title: 'Benchmarks',
    colBenchmark: 'Benchmark',
    colMembers: 'Deelnemingen',
    colYtdEur: 'YTD (€)',
    colYtdLocal: 'YTD (lokaal)',
    splitAdjusted: 'Direct gecorrigeerd voor splitsingen:',
  },
  correlation: {
    title: 'Correlaties tussen portefeuilles',
    noPortfolios: 'Er wordt geen modelportefeuille aangeboden op',
  },
  overview: {
    colClass: 'Categorie',
    colWeight: 'Weging',
    colReturn: 'Rendement',
    colSector: 'Sector',
    colRegion: 'Regio',
    colIsin: 'ISIN',
    colLink: 'Koppeling',
    colFund: 'Fonds',
    colModelWeight: 'Modelweging',
    colStartWeight: 'Beginweging',
    colDirectResult: 'Direct resultaat',
    colDivTax: 'Div. belasting',
    currentPortfolio: 'Huidige portefeuille',
    weightReturnsBy: 'Weeg rendementen op',
    autoCalculated: 'Automatisch (berekend)',
    refreshAll: 'Ververs alles',
    refreshing: 'Verversen…',
    scanningModels: 'Modellen scannen…',
    cancelScan: 'Scan annuleren',
    cancelModelScan: 'Modelscan annuleren',
    allocationBands: 'Asset allocatie',
    allocationBandsHint: 'Per risicoprofiel de minimale, standaard en maximale weging die elke '
      + 'beleggingscategorie mag hebben.',
    loadingHoldings: 'Posities laden…',
    noSnapshot: 'Geen positie-momentopname opgeslagen.',
  },
  models: {
    loading: 'Opgeslagen portefeuilles laden…',
    emptyBefore: 'Nog niets opgeslagen — klik',
    scanAirs: 'Scan AIRS',
    searchPlaceholder: 'Zoek naam / omschrijving…',
    namePlaceholder: 'een naam naar keuze…',
    colFixedDate: 'Vaste datum',
    colStartDate: 'Startdatum',
    colAirsBook: 'AIRS-boek',
    notAnInstrument: 'Deze ISIN is geen instrument in ons overzicht — meestal een eigen fonds, dus er is geen notering om op te lossen.',
    unresolvedSeries: 'Deze ISIN is geen instrument in ons overzicht, dus we hebben er geen koersreeks van.',
    noSnapshot: 'geen momentopname',
    notAPortfolio: '— geen portefeuille —',
    notInGrid: 'niet in het overzicht',
    partialYear: 'deel van het jaar',
    notComputed: 'Nog niet berekend.',
    notCounted: 'Nog niet geteld — de scan loopt de portefeuilles nog langs.',
    saveFailed: 'Opslaan mislukt — niet bewaard.',
    positionsSource: 'Bron van de posities',
    forgetChoice: 'Vergeet deze handmatige keuze en volg weer de automatische.',
    fetchedLive: 'Zojuist live opgehaald uit AirSPMS.',
    cashNoAccounts: 'Liquiditeiten hebben geen rekeningen om te lezen.',
    cashNoIsin: 'Liquiditeiten — daar bestaat geen ISIN voor.',
    viaLinkedModel: 'gewaardeerd via de gekoppelde modelportefeuille',
    hintNoBook: 'Aan dit model is geen AIRS-boek gekoppeld, dus er zijn geen posities om te '
      + 'waarderen.',
    hintComposition: 'De samenstelling van dit model — sector, regio en valuta — naast de '
      + 'benchmark, op één set groepen.',
    hintEffectiveDate: 'De eigen ingangsdatum van het model — vanaf wanneer deze samenstelling '
      + 'geldt.',
    hintClippedName: 'Deze waarde is AFGEKAPT, niet de echte portefeuillenaam.',
    hintYtd: 'Wat dit model dit jaar tot nu toe heeft gerendeerd, met de huidige wegingen.',
    hintYtdSource: 'slotkoers uit asset_price, in EUR via fx_rate',
    hintSinceInception: 'Wat dit model heeft gerendeerd sinds de samenstelling inging.',
    hintInceptionSource: 'dagelijkse EUR-reeks uit asset_price',
    hintAnnualised: 'Het rendement omgerekend naar jaarbasis, zodat verschillende looptijden '
      + 'vergelijkbaar zijn.',
    hintEmptyFixed: 'Een vast model zonder instrumenten — echt leeg, niet ongeteld.',
    hintNotAnInstrument: 'Deze ISIN is geen instrument in ons overzicht — meestal een eigen fonds.',
  },
  accountReturn: {
    title: 'Totaalrendement',
    couldNotLoad: 'kon niet laden',
    heldPlusSold: 'aangehouden + verkocht, tegen dat van het boek',
    loadTxFirst: 'laad eerst de transacties hierboven',
    soldHeld: (sold, held) => `${sold} verkocht · ${held} aangehouden`,
    needsTx: 'De transacties van dit boek zijn niet opgehaald, dus wat er op verkopen is '
      + 'gerealiseerd is onbekend — en de posities die het nog aanhoudt beslaan maar een deel van '
      + 'het jaar. Open',
    openTransactions: 'Transacties',
    needsTxTail: 'hierboven om ze te laden en laad deze daarna opnieuw.',
    reload: 'Opnieuw laden',
    reloading: 'Opnieuw laden…',
    rowHeld: 'Nog aangehouden posities',
    rowRealised: 'Dit jaar gerealiseerd op verkopen',
    rowIncomeSold: 'Opbrengsten uit niet meer aangehouden namen',
    subPriced: (priced, unpriced) =>
      `${priced} gewaardeerd${unpriced ? `, ${unpriced} zonder beginwaarde` : ''}`,
    subRealised: (n) => `${n} instrument${n === 1 ? '' : 'en'} · AIRS’ eigen Res. YtD`,
    subNone: 'geen',
    totalResult: 'Totaalresultaat',
    totalResultSub: 'het jaar, vanuit de posities',
    airsResult: 'Het resultaat van AIRS zelf',
    airsResultSub: 'beleggingsresultaat — het bronsysteem',
    totalYtd: 'Totaalrendement YTD',
    airsYtd: 'YTD volgens AIRS',
    closedOut: 'Staat niet meer in de positietabel — deze positie is volledig verkocht.',
  },
};

export const MANAGEMENT_COPY: Record<Lang, ManagementCopy> = { en, nl };

/**
 * The copy for the reader's current language.
 *
 * ⚠ A HOOK, NOT A `t('some.key')` LOOKUP. The key path is checked by the compiler this way —
 * `t.overview.colWeight` either exists in both languages or does not build — whereas a string key
 * is checked by nobody and fails at runtime as an empty cell. It is also why the tree is nested:
 * `t.benchmarks.title` reads as the surface it belongs to, so a call site cannot borrow another
 * panel's string by accident.
 */
export function useMgmtCopy(): ManagementCopy {
  const [lang] = useLang();
  return MANAGEMENT_COPY[lang];
}

/**
 * ⚠⚠ WHAT IS STILL ENGLISH ON THIS PAGE, kept here so it is a fact in the code rather than
 * something to re-derive by clicking through every panel. Each is a SURFACE: it moves into the type
 * above complete, in both languages, or not at all.
 *
 * ⚠⚠ MEASURED, NOT ESTIMATED (2026-08-22). The Analyse modal family alone is **661 prose strings /
 * ~6,823 words** across ten files — the size of a 25-page document, and roughly 20× the earlier
 * "~26 strings" guess for the modal's chrome. That guess was made by eye and it was wrong by an
 * order of magnitude, which is why the numbers below come from `scripts`-style counting rather than
 * from reading. Sizing a translation by eye is how it gets promised in one sitting.
 *
 *   Risk panel (7 views)       DONE (2026-09-01). `riskCopy.ts` was already complete in both
 *                              languages and NOTHING RENDERED IT: `ActiveSharePanel` was wired and
 *                              the five sibling views (Tracking error, Correlation, Volatility,
 *                              Drawdown, Concentration) still held the English literals. All five
 *                              are wired now. ⚠ It was NOT wiring only, as this note claimed —
 *                              `TrackingErrorView` kept its symbol definitions in a view-local
 *                              `LEGEND` the copy had no section for, and four operand-bearing
 *                              strings (`pairsMeasured`, `shownAnnualised`, `episodes(pct)`,
 *                              `freqNote(f)`, `pricedFrom`) existed on screen and not in the copy.
 *                              A translation sized by reading the copy module misses exactly the
 *                              strings that never reached it.
 *   PortfolioAnalysisModal     primary view + holdings table translated (`analyseCopy`); its
 *                              separately opened child panels remain listed below.
 *   AttributionPanel           97 / 872
 *   BucketDetailPanel          20 / 188
 *   PortfoliosPanel            the /portfolios table this page embeds (~27 strings)
 *   AccountTotalReturn         the account reconciliation rows (~13)
 *   AccountTransactions        the transactions list
 *   HoldingTimingModal         the "why the trading mattered" popup (~12)
 *   AllocationBandsModal       the band editor
 *   the Fundamental modal      Long Equity + Tables (`longEquityCopy`, `tablesCopy`) and DEEP
 *                              VALUATION (2026-09-01, `deepValuationCopy` — the EGM panel, the
 *                              Reverse DCF and both raw-data modals, ⓘ cards included) are done.
 *                              Quick Valuation and the ratio drill-downs are not. ⚠ Deep Valuation
 *                              measured 4 files / 86 visible strings by scan and needed ~170 keys:
 *                              the scan sees JSX text and attributes, not the interpolated `how=`
 *                              prose, which is most of a ⓘ card. Scan to SIZE a batch, read to
 *                              finish one.
 *
 * ⚠⚠ THE ⓘ CARDS ARE NOW IN SCOPE, REVERSING THE BOUNDARY THIS NOTE USED TO DRAW (2026-08-22, on
 * request). They are not decoration: they carry the definition, the convention chosen and the
 * caveat that stops a figure being misread — a Dutch reader who must switch languages to learn
 * that `ā` was subtracted has been given the digits and not the number. `riskCopy` translates them;
 * the surfaces above still owe them.
 *
 * ⚠ TERMS THAT STAY ENGLISH ARE AN EXCEPTION, NOT A SHORTCUT: "active share", "tracking error",
 * "drawdown", "Sharpe", "Sortino", "information ratio", "HHI". These are what a Dutch wealth
 * manager says out loud. Where a real Dutch word exists it is used — volatiliteit, correlatie,
 * concentratie, rendement, gewicht, positie, emittent.
 */
export const UNTRANSLATED_SURFACES = [
  'AttributionPanel', 'BucketDetailPanel',
  'AccountTransactions', 'HoldingTimingModal', 'AllocationBandsModal',
  'QuickValuationTab', 'PriceTargetCalculator',
  'PortfolioOverviewPanel (partly — the holdings table is done)',
] as const;
