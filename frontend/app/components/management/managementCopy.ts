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
    loadingHoldings: string;
    noSnapshot: string;
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
    loadingHoldings: 'Loading holdings…',
    noSnapshot: 'No holdings snapshot stored.',
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
    loadingHoldings: 'Posities laden…',
    noSnapshot: 'Geen positie-momentopname opgeslagen.',
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
 *   Risk panel (7 views)       `riskCopy.ts` — the TABLE IS COMPLETE in both languages, including
 *                              the ⓘ cards. `ActiveSharePanel` is fully wired; the six sibling
 *                              views (Tracking error, Correlation, Volatility, Drawdown,
 *                              Concentration, Positions) still render the English literals and
 *                              need wiring only — no further translation.
 *   PortfolioAnalysisModal     253 strings / 3,342 words. The big one.
 *   AttributionPanel           97 / 872
 *   BucketDetailPanel          20 / 188
 *   PortfoliosPanel            the /portfolios table this page embeds (~27 strings)
 *   AccountTotalReturn         the account reconciliation rows (~13)
 *   AccountTransactions        the transactions list
 *   HoldingTimingModal         the "why the trading mattered" popup (~12)
 *   AllocationBandsModal       the band editor
 *   the Fundamental modal      Long Equity + Tables are done (`longEquityCopy`, `tablesCopy`);
 *                              Quick Valuation, Deep Valuation and the drill-downs are not
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
  'PortfoliosPanel', 'PortfolioAnalysisModal', 'AttributionPanel', 'BucketDetailPanel',
  'AccountTotalReturn', 'AccountTransactions', 'HoldingTimingModal', 'AllocationBandsModal',
  'QuickValuationTab', 'DeepValuationTab',
] as const;
