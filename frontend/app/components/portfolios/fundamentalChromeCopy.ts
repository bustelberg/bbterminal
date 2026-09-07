'use client';

import { useLang, type Lang } from '../../../lib/i18n';

/**
 * The Fundamental modal's own CHROME — the tab row, the refresh button, the SBC checkbox and the
 * period switch. Not the tabs' contents: each of those has its own copy module
 * (`longEquityCopy`, `tablesCopy`, `deepValuationCopy`) or is still untranslated.
 *
 * ⚠⚠ THE TAB LABELS ARE NOT THE TAB KEYS. `Tab` is `'longequity' | 'tables' | 'quickval' |
 * 'deepval'` and stays English for ever: it is this modal's state, `openTab` and every caller pass
 * it, and `LongEquityTab` keys its per-cadence fetches off it. Translating the KEY would remount
 * every card and refetch on a switch that should only repaint text — the same argument
 * `longEquityCopy` makes about `MetricCfg.title`. Only the rendered word changes here.
 *
 * ⚠ `Graphs` IS THE LABEL FOR THE `longequity` KEY (renamed 2026-09-03, on request). The Dutch
 * follows the label, not the key.
 *
 * ⚠ `EN` / `NL` ARE NOT HERE AND MUST NOT BE. They are the language switch's own options and are
 * endonyms — a Dutch reader looks for "NL", not "Nederlands" — so they are the same in every
 * language by design. See `LANG_LABEL` in `lib/i18n`.
 */
export type FundamentalChromeCopy = {
  tabs: { longequity: string; tables: string; quickval: string; deepval: string };
  /** The portfolio-wide refresh, in its four states. */
  refresh: string;
  refreshUniverse: string;
  refreshing: string;
  cancel: string;
  cancelling: string;
  sbc: string;
  sbcTitle: string;
  periods: string;
  annual: string;
  annualNote: string;
  quarterly: string;
  quarterlyNote: string;
};

const EN: FundamentalChromeCopy = {
  tabs: {
    longequity: 'Graphs',
    tables: 'Tables',
    quickval: 'Quick Valuation',
    deepval: 'Deep Valuation',
  },
  refresh: 'Refresh fundamentals',
  refreshUniverse: 'Fetch missing fundamentals',
  refreshing: 'Refreshing…',
  cancel: 'Cancel',
  cancelling: 'Cancelling…',
  sbc: 'SBC correction',
  sbcTitle: 'Subtract stock-based compensation from free cash flow before computing FCF margin, '
    + 'FCF yield, cash return on capital and FCF / Net Income. ⚠ No effect on ROIC, which is '
    + 'GuruFocus’s own published ratio — there is no numerator of ours to adjust.',
  periods: 'Periods',
  annual: 'Annual',
  annualNote: 'One point per fiscal year.',
  quarterly: 'Quarterly',
  quarterlyNote: 'One point per quarter, each the TRAILING TWELVE MONTHS — quarterly frequency '
    + 'with annual scope. Flows sum the last four quarters, balances take the latest, and an '
    + 'already-annualised rate takes their mean. Raw quarters would put a seasonal sawtooth '
    + 'through revenue and every margin built on it.',
};

/**
 * ⚠ TRANSLATED FROM THE ENGLISH ABOVE, never authored here — see the note on `Lang`.
 *
 * ⚠ `Quick Valuation` / `Deep Valuation` KEEP THEIR ENGLISH NAMES. They are what these two screens
 * are called — the terms a wealth manager uses out loud, like `Sharpe` and `drawdown` elsewhere in
 * this app — and a reader who has learned "Deep Valuation" should find it under that name in either
 * language. `Graphs` and `Tables` are ordinary words and do translate.
 */
const NL: FundamentalChromeCopy = {
  tabs: {
    longequity: 'Grafieken',
    tables: 'Tabellen',
    quickval: 'Quick Valuation',
    deepval: 'Deep Valuation',
  },
  refresh: 'Fundamentals verversen',
  refreshUniverse: 'Ontbrekende fundamentals ophalen',
  refreshing: 'Verversen…',
  cancel: 'Annuleren',
  cancelling: 'Annuleren…',
  sbc: 'SBC-correctie',
  sbcTitle: 'Trek aandelenbeloning (SBC) van de vrije kasstroom af vóór het berekenen van '
    + 'FCF-marge, FCF-rendement, cash return op kapitaal en FCF / nettowinst. ⚠ Geen effect op '
    + 'ROIC: dat is GuruFocus’ eigen gepubliceerde ratio — er is geen teller van ons om aan te '
    + 'passen.',
  periods: 'Perioden',
  annual: 'Jaarlijks',
  annualNote: 'Eén punt per boekjaar.',
  quarterly: 'Per kwartaal',
  quarterlyNote: 'Eén punt per kwartaal, telkens over de LAATSTE TWAALF MAANDEN — kwartaalfrequentie '
    + 'met jaarbereik. Stromen tellen de laatste vier kwartalen op, balansposten nemen de nieuwste, '
    + 'en een reeds geannualiseerd percentage neemt het gemiddelde. Ruwe kwartalen zouden een '
    + 'seizoenspatroon door de omzet en elke marge daarop leggen.',
};

export const FUNDAMENTAL_CHROME_COPY: Record<Lang, FundamentalChromeCopy> = { en: EN, nl: NL };

export function useFundamentalChromeCopy(): FundamentalChromeCopy {
  const [lang] = useLang();
  return FUNDAMENTAL_CHROME_COPY[lang];
}
