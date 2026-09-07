'use client';

import { useLang, type Lang } from '../../lib/i18n';

/**
 * The sidebar: its nav labels, the account block at its foot, and the language control between.
 *
 * ⚠⚠ THE NAV IS KEYED BY HREF, AND THE LABEL IS NO LONGER PART OF THE NAV STRUCTURE. `Sidebar.tsx`
 * still owns the ORDER, the sections and the visibility rules — those are layout and permission,
 * neither of which is language — and looks every name up here. A page therefore cannot be called
 * one thing in the nav and another in its own copy module, and `Record<NavKey, string>` makes a
 * missing Dutch label a compile error rather than a blank row.
 *
 * ⚠ SOME LABELS ARE THE SAME WORD IN BOTH LANGUAGES AND THAT IS CORRECT, NOT AN OVERSIGHT:
 * `Backtest`, `Benchmarks`, `API`, `AlphaLab`, `Signal Lab`, `Diversifier`, `Asset Pipeline`. They
 * are product and tool names — what the things are CALLED — and the index families (`ACWI`,
 * `LongEquity`, `Leonteq`, `S&P 500`, `GuruFocus`, `AIRS`) keep their names inside a translated
 * phrase. So the home page's "every description must differ" drift check does NOT apply here.
 */
export type NavKey =
  | '/'
  | '/management-dashboard'
  | '/research-dashboard'
  | '/schedule'
  | '/earnings'
  | '/backtest'
  | '/diversifier'
  | '/universe'
  | '/longequity-universe'
  | '/sp500'
  | '/acwi'
  | '/leonteq'
  | '/fx-rates'
  | '/timezone'
  | '/airs-portfolio'
  | '/request_gurufocus'
  | '/benchmarks'
  | '/isin-compare'
  | '/asset-pipeline'
  | '/alphalab'
  | '/signal-lab'
  | '/fees'
  | '/api'
  | '/network'
  | '/documentation';

export type SidebarCopy = {
  nav: Record<NavKey, string>;
  /** The expand/collapse control's accessible name, built from the section's own label. */
  expandSection: (label: string) => string;
  collapseSection: (label: string) => string;
  language: string;
  languageTitle: string;
  signOut: string;
  deleteAccount: string;
  deleteSure: string;
  deleteConfirm: string;
  deleting: string;
  cancel: string;
};

const EN: SidebarCopy = {
  nav: {
    '/': 'Welcome',
    '/management-dashboard': 'Management Dashboard',
    '/research-dashboard': 'Research Dashboard',
    '/schedule': 'Schedule',
    '/earnings': 'Earnings Dashboard',
    '/backtest': 'Backtest',
    '/diversifier': 'Diversifier',
    '/universe': 'Universe Overview',
    '/longequity-universe': 'LongEquity Universe',
    '/sp500': 'S&P 500 Universe',
    '/acwi': 'ACWI Universe',
    '/leonteq': 'Leonteq Universe',
    '/fx-rates': 'FX Rates',
    '/timezone': 'Trading Hours',
    '/airs-portfolio': 'AIRS Portfolio',
    '/request_gurufocus': 'Request GuruFocus',
    '/benchmarks': 'Benchmarks',
    '/isin-compare': 'ISIN Compare',
    '/asset-pipeline': 'Asset Pipeline',
    '/alphalab': 'AlphaLab',
    '/signal-lab': 'Signal Lab',
    '/fees': 'Fees',
    '/api': 'API',
    '/network': 'Network',
    '/documentation': 'Documentation',
  },
  expandSection: (label) => `Expand ${label}`,
  collapseSection: (label) => `Collapse ${label}`,
  language: 'Language',
  languageTitle: 'The interface language. Stored per browser and shared by every screen. '
    + 'Not every page is translated yet — the home page, the Management Dashboard and the '
    + 'Fundamental modal are the ones that answer today.',
  signOut: 'Sign out',
  deleteAccount: 'Delete account',
  deleteSure: 'Are you sure? This cannot be undone.',
  deleteConfirm: 'Yes, delete',
  deleting: 'Deleting…',
  cancel: 'Cancel',
};

/** ⚠ TRANSLATED FROM THE ENGLISH ABOVE, never authored here — English stays the source language
 *  even though Dutch is now the DEFAULT. See the note on `Lang`. */
const NL: SidebarCopy = {
  nav: {
    '/': 'Welkom',
    '/management-dashboard': 'Managementdashboard',
    '/research-dashboard': 'Researchdashboard',
    '/schedule': 'Planning',
    '/earnings': 'Winstdashboard',
    '/backtest': 'Backtest',
    '/diversifier': 'Diversifier',
    '/universe': 'Universumoverzicht',
    '/longequity-universe': 'LongEquity-universum',
    '/sp500': 'S&P 500-universum',
    '/acwi': 'ACWI-universum',
    '/leonteq': 'Leonteq-universum',
    '/fx-rates': 'Wisselkoersen',
    '/timezone': 'Handelstijden',
    '/airs-portfolio': 'AIRS-portefeuille',
    '/request_gurufocus': 'GuruFocus opvragen',
    '/benchmarks': 'Benchmarks',
    '/isin-compare': 'ISIN vergelijken',
    '/asset-pipeline': 'Asset Pipeline',
    '/alphalab': 'AlphaLab',
    '/signal-lab': 'Signal Lab',
    '/fees': 'Kosten',
    '/api': 'API',
    '/network': 'Netwerk',
    '/documentation': 'Documentatie',
  },
  expandSection: (label) => `${label} uitklappen`,
  collapseSection: (label) => `${label} inklappen`,
  language: 'Taal',
  languageTitle: 'De taal van de interface. Wordt per browser opgeslagen en geldt op elk scherm. '
    + 'Nog niet elke pagina is vertaald — de startpagina, het Managementdashboard en de '
    + 'Fundamental-modal zijn de schermen die nu meegaan.',
  signOut: 'Uitloggen',
  deleteAccount: 'Account verwijderen',
  deleteSure: 'Weet u het zeker? Dit kan niet ongedaan worden gemaakt.',
  deleteConfirm: 'Ja, verwijderen',
  deleting: 'Verwijderen…',
  cancel: 'Annuleren',
};

export const SIDEBAR_COPY: Record<Lang, SidebarCopy> = { en: EN, nl: NL };

export function useSidebarCopy(): SidebarCopy {
  const [lang] = useLang();
  return SIDEBAR_COPY[lang];
}
