/**
 * The sidebar's copy, and its agreement with the home page's.
 *
 * ⚠ THE COMPILER COVERS COMPLETENESS — `Record<NavKey, string>` will not build with a label missing
 * in either language, and `NavItem = { href: NavKey }` will not build for a nav entry whose href has
 * no label. What it cannot see is the two SURFACES disagreeing: the same page named one thing in the
 * nav and another on its home tile, which is two names for one destination in a single screenshot.
 */
import { describe, expect, it } from 'vitest';

import { SIDEBAR_COPY, type NavKey } from './sidebarCopy';
import { HOME_COPY } from './home/homeCopy';
import { HOME_TILE_ORDER } from './home/homeTileKeys';

const LANGS = ['en', 'nl'] as const;

describe('the nav labels', () => {
  it('cover every language with no empty string', () => {
    for (const lang of LANGS) {
      for (const [href, label] of Object.entries(SIDEBAR_COPY[lang].nav)) {
        expect(label.trim(), `${lang} ${href}`).not.toBe('');
      }
    }
  });

  it('⚠⚠ agree with the home tile for every page that has both', () => {
    // A page named `Planning` in the nav and `Schedule` on its tile is two names for one
    // destination, on one screen. The home tiles are a subset of the nav, so every tile key must
    // resolve here and to the SAME words.
    for (const href of HOME_TILE_ORDER) {
      for (const lang of LANGS) {
        expect(SIDEBAR_COPY[lang].nav[href as NavKey], `${lang} ${href}`)
          .toBe(HOME_COPY[lang].tiles[href].label);
      }
    }
  });

  it('⚠ keeps product and tool names identical in both languages', () => {
    // These are what the things are CALLED — translating them would name something that does not
    // exist in the app. Unlike the home DESCRIPTIONS, a label matching across languages is correct
    // here, so no blanket "must differ" check applies to this map.
    for (const href of ['/backtest', '/benchmarks', '/api', '/alphalab', '/signal-lab',
      '/diversifier', '/asset-pipeline'] as const) {
      expect(SIDEBAR_COPY.nl.nav[href]).toBe(SIDEBAR_COPY.en.nav[href]);
    }
  });

  it('⚠ but DOES translate the ordinary words', () => {
    // The counterweight to the case above: if this list ever matched too, the "translation" would
    // be a copy of the English map and every one of these tests would still pass.
    for (const href of ['/', '/schedule', '/fx-rates', '/fees', '/network',
      '/timezone', '/earnings'] as const) {
      expect(SIDEBAR_COPY.nl.nav[href], `${href} is untranslated`)
        .not.toBe(SIDEBAR_COPY.en.nav[href]);
    }
  });
});

describe('the account block', () => {
  it('⚠ translates the destructive confirmation — the worst place for an English string', () => {
    // It asks for a decision that cannot be undone; a reader who does not read the sentence is
    // being asked to confirm something they have not been told.
    expect(SIDEBAR_COPY.nl.deleteSure).not.toBe(SIDEBAR_COPY.en.deleteSure);
    expect(SIDEBAR_COPY.nl.deleteConfirm).not.toBe(SIDEBAR_COPY.en.deleteConfirm);
    expect(SIDEBAR_COPY.nl.cancel).not.toBe(SIDEBAR_COPY.en.cancel);
    expect(SIDEBAR_COPY.nl.signOut).not.toBe(SIDEBAR_COPY.en.signOut);
    expect(SIDEBAR_COPY.nl.deleteAccount).not.toBe(SIDEBAR_COPY.en.deleteAccount);
  });

  it('builds the section aria-label around the translated name', () => {
    expect(SIDEBAR_COPY.en.expandSection('Universe Overview')).toContain('Universe Overview');
    expect(SIDEBAR_COPY.nl.expandSection('Universumoverzicht')).toContain('Universumoverzicht');
    expect(SIDEBAR_COPY.nl.expandSection('X')).not.toBe(SIDEBAR_COPY.en.expandSection('X'));
  });
});
