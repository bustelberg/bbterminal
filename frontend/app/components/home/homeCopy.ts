'use client';

import { useLang, type Lang } from '../../../lib/i18n';
import { type HomeTileKey } from './homeTileKeys';

// ⚠⚠ NO RE-EXPORT OF `HOME_TILE_ORDER` FROM HERE, DELIBERATELY. Re-exporting it would restore the
// exact bug the split fixes: a server component could import the array through this `'use client'`
// module and get a client-reference proxy again, with nothing in the import path hinting why. Data
// comes from `homeTileKeys.ts` directly, for every caller.

/**
 * The home page's copy, in both languages.
 *
 * ⚠⚠ THE TILE REGISTRY IS KEYED BY HREF, AND THAT IS WHAT STOPS IT DRIFTING. The list used to be an
 * array of {href, label, description} objects inside 'app/page.tsx', which meant adding a page put
 * its English copy in one file and its Dutch copy nowhere. Record<HomeTileKey, …> makes a missing
 * Dutch entry a COMPILE ERROR rather than an empty tile — the same rule 'i18n.ts' states for every
 * translated surface: English is the source, the Dutch follows, and a forgotten string must fail
 * the build instead of silently falling back and looking like a rendering fault.
 *
 * ⚠⚠ THE KEY AND THE ORDER LIVE IN 'homeTileKeys.ts', WHICH HAS NO 'use client'. They were here, and
 * the server component that filters them by role got a client-reference PROXY instead of an array —
 * a runtime TypeError on the home page, invisible to tsc and to vitest. See that file.
 *
 * ⚠ THE HREF IS THE KEY, NOT AN INDEX. A tile's copy cannot end up on the wrong tile, and the route
 * gate's 'isUserAllowedPath' filters the SAME values the page renders.
 */
type Tile = { label: string; description: string };

export type HomeCopy = {
  welcome: string;
  intro: string;
  tiles: Record<HomeTileKey, Tile>;
};

const EN: HomeCopy = {
  welcome: 'Welcome to BBTerminal',
  intro: 'Your one stop shop for insight into Bustelberg portfolios, and in the future also equity '
    + 'research about new companies.',
  tiles: {
    '/management-dashboard': {
      label: 'Management Dashboard',
      description: 'Real-time insight from performance to risk into Bustelberg portfolios.',
    },
    '/earnings': {
      label: 'Earnings Dashboard',
      description: 'Browse per-company earnings metrics pulled from GuruFocus, with quick refresh '
        + 'by source.',
    },
    '/schedule': {
      label: 'Schedule',
      description: 'Scheduled strategies for the MomentumTopSelectie.',
    },
    '/backtest': {
      label: 'Backtest',
      description: 'Test a strategy on a template-managed universe over a date range. Start '
        + 'defaults to the universe’s hard backstop, end defaults to the latest available price '
        + 'data.',
    },
    '/universe': {
      label: 'Universe Overview',
      description: 'Criteria-driven universe screener — apply filters to companies and save '
        + 'labelled, derived universes.',
    },
    '/longequity-universe': {
      label: 'LongEquity Universe',
      description: 'Monthly snapshots of the LongEquity universe, grouped by region and country. '
        + 'Run the ingest pipeline from here.',
    },
    '/sp500': {
      label: 'S&P 500 Universe',
      description: 'Reconstructed S&P 500 memberships over time, with monthly tickers and change '
        + 'history. Freeze a reusable copy here.',
    },
    '/acwi': {
      label: 'ACWI Universe',
      description: 'iShares ACWI holdings and MSCI announcement explorer — review additions, '
        + 'deletions, and net changes.',
    },
    '/leonteq': {
      label: 'Leonteq Universe',
      description: 'Equities Leonteq lists as underlyings for structured products, grouped by '
        + 'sector → industry with GuruFocus links.',
    },
    '/fx-rates': {
      label: 'FX Rates',
      description: 'View FX rate coverage and history, and sync the latest ECB / Yahoo rates into '
        + 'the database.',
    },
    '/airs-portfolio': {
      label: 'AIRS Portfolio',
      description: 'Broker scanner and AIRS Excel upload — parses holdings and computes YTD '
        + 'returns in EUR and local currency.',
    },
    '/request_gurufocus': {
      label: 'Request GuruFocus',
      description: 'Trigger GuruFocus indicator fetches for selected companies and exchanges.',
    },
    '/benchmarks': {
      label: 'Benchmarks',
      description: 'Manage index benchmarks (SPY, ACWI, …) — add tickers, fetch prices, and '
        + 'inspect coverage.',
    },
  },
};

/** ⚠ TRANSLATED FROM THE ENGLISH ABOVE, never authored here — see the note on `Lang`. Product names
 *  (BBTerminal, LongEquity, ACWI, Leonteq, GuruFocus, MomentumTopSelectie) stay untranslated: they
 *  are what the things are called, in either language. */
const NL: HomeCopy = {
  welcome: 'Welkom bij BBTerminal',
  intro: 'Uw one-stop-shop voor inzicht in Bustelberg-portefeuilles, en in de toekomst ook '
    + 'aandelenonderzoek naar nieuwe bedrijven.',
  tiles: {
    '/management-dashboard': {
      label: 'Managementdashboard',
      description: 'Realtime inzicht in Bustelberg-portefeuilles, van rendement tot risico.',
    },
    '/earnings': {
      label: 'Winstdashboard',
      description: 'Blader door winstcijfers per bedrijf uit GuruFocus, met snelle verversing per '
        + 'bron.',
    },
    '/schedule': {
      label: 'Planning',
      description: 'Ingeplande strategieën voor de MomentumTopSelectie.',
    },
    '/backtest': {
      label: 'Backtest',
      description: 'Test een strategie op een sjabloonbeheerd universum over een periode. De '
        + 'startdatum valt terug op de harde ondergrens van het universum, de einddatum op de '
        + 'meest recente koersdata.',
    },
    '/universe': {
      label: 'Universumoverzicht',
      description: 'Universumscreener op criteria — filter bedrijven en sla gelabelde, afgeleide '
        + 'universa op.',
    },
    '/longequity-universe': {
      label: 'LongEquity-universum',
      description: 'Maandelijkse momentopnames van het LongEquity-universum, gegroepeerd per regio '
        + 'en land. Start de ingest-pijplijn hiervandaan.',
    },
    '/sp500': {
      label: 'S&P 500-universum',
      description: 'Gereconstrueerde S&P 500-samenstelling door de tijd, met maandelijkse tickers '
        + 'en wijzigingshistorie. Bevries hier een herbruikbare kopie.',
    },
    '/acwi': {
      label: 'ACWI-universum',
      description: 'iShares ACWI-posities en MSCI-aankondigingen — bekijk toevoegingen, '
        + 'verwijderingen en nettowijzigingen.',
    },
    '/leonteq': {
      label: 'Leonteq-universum',
      description: 'Aandelen die Leonteq noteert als onderliggende waarden voor gestructureerde '
        + 'producten, gegroepeerd per sector → industrie met GuruFocus-links.',
    },
    '/fx-rates': {
      label: 'Wisselkoersen',
      description: 'Bekijk dekking en historie van wisselkoersen en synchroniseer de nieuwste '
        + 'ECB-/Yahoo-koersen naar de database.',
    },
    '/airs-portfolio': {
      label: 'AIRS-portefeuille',
      description: 'Brokerscanner en AIRS-Excelupload — leest posities in en berekent '
        + 'YTD-rendement in euro’s en lokale valuta.',
    },
    '/request_gurufocus': {
      label: 'GuruFocus opvragen',
      description: 'Start GuruFocus-indicatorfetches voor geselecteerde bedrijven en beurzen.',
    },
    '/benchmarks': {
      label: 'Benchmarks',
      description: 'Beheer indexbenchmarks (SPY, ACWI, …) — voeg tickers toe, haal koersen op en '
        + 'bekijk de dekking.',
    },
  },
};

/** ⚠ EXPORTED FOR THE DRIFT TEST, NOT FOR CALL SITES — components use `useHomeCopy()`, which
 *  picks the reader's language. The compiler guarantees each language COVERS `HomeTileKey`; what it
 *  cannot see is the two maps disagreeing with `HOME_TILE_ORDER`, or a Dutch string left as its
 *  English source. `homeCopy.test.ts` reads this to check both. */
export const HOME_COPY: Record<Lang, HomeCopy> = { en: EN, nl: NL };

/** The home page's copy in the reader's language. A hook, not a key lookup — see
 *  `useDeepValuationCopy` for why the path is compiler-checked rather than a string. */
export function useHomeCopy(): HomeCopy {
  const [lang] = useLang();
  return HOME_COPY[lang];
}
