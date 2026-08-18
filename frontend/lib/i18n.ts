'use client';

import { useCallback, useSyncExternalStore } from 'react';

/**
 * The app's language choice. English is the default and the source language; Dutch is the
 * translation.
 *
 * ⚠ THE SCOPE IS DELIBERATELY ONE TAB. Only the Fundamental modal's `Tables` tab is translated
 * today, so the switch is rendered only there — see `OwnerEarningsModal`. A language control that
 * appears above a screen it does not translate is worse than none: the reader flips it, nothing
 * moves, and they conclude the feature is broken rather than unfinished.
 *
 * ⚠ ENGLISH IS THE SOURCE, NOT A PEER. Every string is authored in English and translated from
 * there. When copy changes, the English changes first and the Dutch follows — `TablesCopy`'s type
 * makes a forgotten Dutch string a compile error rather than a silent fall-back to English, which
 * would show a half-translated table and look like a rendering bug.
 */
export type Lang = 'en' | 'nl';

/** In display order. The switch is built from this, so adding a language is one entry + its copy. */
export const LANGS = ['en', 'nl'] as const;

/** What the switch prints. Endonyms — a Dutch reader looks for "NL", not "Dutch". */
export const LANG_LABEL: Record<Lang, string> = { en: 'EN', nl: 'NL' };

const KEY = 'bb:lang';

const isLang = (v: unknown): v is Lang => v === 'en' || v === 'nl';

/**
 * ⚠⚠ AN EXTERNAL STORE, NOT `useState` + AN EFFECT THAT READS `localStorage`.
 *
 * The obvious shape — seed the state to `'en'`, then adopt the stored value in a `useEffect` — is
 * wrong twice. It sets state synchronously inside an effect, which React now flags as a cascading
 * render; and it makes every component holding the preference its own copy, so the switch in one
 * open modal would not move the table in another. `useSyncExternalStore` is the primitive for
 * exactly this: one value, read from outside React, with an explicit server snapshot.
 *
 * ⚠ `getServerSnapshot` RETURNS `'en'` AND MUST. These components are `'use client'` but Next still
 * renders them on the server, where `localStorage` does not exist. React uses this snapshot during
 * hydration and re-reads the real one immediately after, which is what keeps the server's HTML and
 * the first client render in agreement — seeding from storage directly makes them disagree and
 * React throws away the subtree to recover.
 *
 * ⚠ THE SNAPSHOT IS CACHED IN `current` BECAUSE `getSnapshot` MUST BE STABLE. React calls it on
 * every render and re-renders if the result differs; hitting `localStorage` each time is both a
 * synchronous disk-backed read in the render path and, on a parse failure, a value that could
 * differ between two calls in the same commit.
 */
let current: Lang | null = null;
const listeners = new Set<() => void>();

function read(): Lang {
  try {
    const stored = window.localStorage.getItem(KEY);
    // ⚠ AN UNKNOWN STORED VALUE FALLS BACK RATHER THAN BEING TRUSTED. `'de'` in this key would
    // otherwise index `COPY` to `undefined` and blank every string in the table.
    return isLang(stored) ? stored : 'en';
  } catch (e) {
    // A blocked or full localStorage is not a reason to fail to render a table.
    console.warn('[bb:i18n] could not read the stored language:', e);
    return 'en';
  }
}

function getSnapshot(): Lang {
  if (current == null) current = read();
  return current;
}

const getServerSnapshot = (): Lang => 'en';

function subscribe(onChange: () => void): () => void {
  listeners.add(onChange);
  // ⚠ `storage` FIRES IN OTHER TABS, NOT THIS ONE — that is the whole point of listening to it.
  // Same-tab changes come through `listeners`, which `setLang` notifies directly.
  const onStorage = (e: StorageEvent) => {
    if (e.key !== KEY) return;
    current = read();
    onChange();
  };
  window.addEventListener('storage', onStorage);
  return () => {
    listeners.delete(onChange);
    window.removeEventListener('storage', onStorage);
  };
}

/** The language preference, persisted per browser and shared by every component that asks. */
export function useLang(): [Lang, (l: Lang) => void] {
  const lang = useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);

  const setLang = useCallback((l: Lang) => {
    current = l;
    try {
      window.localStorage.setItem(KEY, l);
    } catch (e) {
      // ⚠ THE CHOICE STILL TAKES EFFECT FOR THIS SESSION. Failing to persist is a reason not to
      // remember it next time, not a reason to ignore the click that just happened.
      console.warn('[bb:i18n] could not persist the language:', e);
    }
    listeners.forEach((cb) => cb());
  }, []);

  return [lang, setLang];
}
