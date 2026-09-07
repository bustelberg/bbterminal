/**
 * The home page's copy, pinned in both languages.
 *
 * ⚠ THE COMPILER ALREADY CATCHES A MISSING TILE — `Record<HomeTileKey, Tile>` will not build with
 * one absent. What it cannot see is the two maps disagreeing with `HOME_TILE_ORDER` (a key nobody
 * renders, or a rendered href with no copy), an empty string, or a Dutch entry left as its English
 * source. Those are what this file is for.
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

import { HOME_COPY } from './homeCopy';
import { HOME_TILE_ORDER, type HomeTileKey } from './homeTileKeys';
import { isUserAllowedPath } from '../../../lib/userAllowedPaths';

const ORDER: HomeTileKey[] = [...HOME_TILE_ORDER];
const LANGS = ['en', 'nl'] as const;

describe('HOME_TILE_ORDER', () => {
  it('has no duplicates — a repeated href renders the same tile twice', () => {
    expect(new Set(ORDER).size).toBe(ORDER.length);
  });

  it('⚠ every entry is a plausible route, so no tile is a dead click', () => {
    // A typo'd href renders a tile `isUserAllowedPath` rejects, which an admin still sees and whose
    // click 404s. One leading slash, no trailing one.
    for (const href of ORDER) expect(href).toMatch(/^\/[a-z0-9_-]+$/);
  });

  it('⚠ the user-visible subset is exactly what the route gate allows', () => {
    // The home grid filters this array through the SAME allow-list the route gate uses, so the two
    // cannot drift — that drift is why this filter exists (the page once advertised admin-only
    // pages while hiding /schedule). `/earnings` left the user tier on 2026-09-07.
    expect(ORDER.filter((h) => isUserAllowedPath(h)))
      .toEqual(['/management-dashboard', '/schedule']);
  });
});

describe('⚠⚠ the server/client boundary', () => {
  /**
   * THE ONLY TEST HERE THAT READS SOURCE, AND IT GUARDS A RUNTIME FAILURE NOTHING ELSE CAN SEE.
   *
   * `app/page.tsx` is a SERVER component and imports `HOME_TILE_ORDER` as DATA. If that array ever
   * moves back into a `'use client'` module, the bundler hands the server a client-reference PROXY
   * instead of the values and the home page dies on first render with
   *
   *     TypeError: …HOME_TILE_ORDER.filter is not a function
   *
   * Shipped exactly that way. `tsc` was clean — the types are real either side of the boundary —
   * and every unit test passed, because vitest imports modules directly and never applies the RSC
   * transform. So the suite is structurally blind to it and this assertion is the substitute: the
   * directive's ABSENCE is the whole guarantee.
   */
  it('the data module the server imports has no `use client` directive', () => {
    const src = readFileSync(join(__dirname, 'homeTileKeys.ts'), 'utf8');
    expect(src).not.toMatch(/^\s*['"]use client['"]/);
  });

  it('⚠ and the copy module does NOT re-export it, which would restore the same trap', () => {
    // A re-export from `homeCopy.ts` would let a server component import the array through a
    // `'use client'` module again — same proxy, same TypeError, with nothing in the import path
    // hinting why. Data comes from `homeTileKeys.ts` directly, for every caller.
    const src = readFileSync(join(__dirname, 'homeCopy.ts'), 'utf8');
    expect(src).toMatch(/^\s*['"]use client['"]/);          // it IS a client module…
    expect(src).not.toMatch(/^export\s.*HOME_TILE_ORDER/m); // …so it must not re-export the data
  });
});

describe('the two languages cannot drift', () => {
  it('⚠ both cover exactly the tiles that are rendered — no orphans, no gaps', () => {
    const expected = [...ORDER].sort();
    for (const lang of LANGS) {
      expect(Object.keys(HOME_COPY[lang].tiles).sort()).toEqual(expected);
    }
  });

  it('has no empty string anywhere — a blank tile reads as a rendering fault', () => {
    for (const lang of LANGS) {
      const c = HOME_COPY[lang];
      expect(c.welcome.trim()).not.toBe('');
      expect(c.intro.trim()).not.toBe('');
      for (const href of ORDER) {
        expect(c.tiles[href].label.trim(), `${lang} ${href} label`).not.toBe('');
        expect(c.tiles[href].description.trim(), `${lang} ${href} description`).not.toBe('');
      }
    }
  });

  it('⚠⚠ every DESCRIPTION differs between the languages', () => {
    // The failure this catches is a Dutch entry copy-pasted from the English and never translated:
    // it compiles, it renders, and it looks finished. Descriptions are full sentences, so no two
    // legitimately coincide — unlike LABELS, where `Backtest` and `Benchmarks` are the same word in
    // both languages and are deliberately left alone.
    for (const href of ORDER) {
      expect(HOME_COPY.nl.tiles[href].description, `${href} is untranslated`)
        .not.toBe(HOME_COPY.en.tiles[href].description);
    }
    expect(HOME_COPY.nl.welcome).not.toBe(HOME_COPY.en.welcome);
    expect(HOME_COPY.nl.intro).not.toBe(HOME_COPY.en.intro);
  });

  it('⚠ keeps product names untranslated — they are what the things are CALLED', () => {
    // A translated `MomentumTopSelectie` would name a strategy that does not exist in the app; the
    // same goes for the vendors and the index families.
    expect(HOME_COPY.nl.tiles['/schedule'].description).toContain('MomentumTopSelectie');
    expect(HOME_COPY.nl.tiles['/earnings'].description).toContain('GuruFocus');
    expect(HOME_COPY.nl.tiles['/longequity-universe'].label).toContain('LongEquity');
    expect(HOME_COPY.nl.welcome).toContain('BBTerminal');
  });
});
