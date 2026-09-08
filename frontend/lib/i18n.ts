'use client';

import { useCallback, useSyncExternalStore } from 'react';

/**
 * The app's language choice.
 *
 * ⚠⚠ DUTCH IS THE DEFAULT AND ENGLISH IS THE SOURCE — TWO DIFFERENT THINGS, AND CONFLATING THEM IS
 * how a codebase ends up half-authored in each. The readers are Dutch, so an unset preference now
 * resolves to `'nl'` (2026-09-07, on request). Strings are still WRITTEN in English first and
 * translated from there; every copy module keeps its `EN` block as the original and its `NL` block
 * as the translation, and the type is what makes a forgotten translation a compile error. Nothing
 * about the default changes that direction.
 *
 * ⚠⚠ THE FALLBACK AND `getServerSnapshot` MOVE TOGETHER OR NOT AT ALL. They are two halves of one
 * answer: the server renders with the snapshot and React hydrates against it. Change only `read()`
 * and every first paint is English HTML replaced by Dutch on hydration — a visible flash on every
 * load, and React discarding the subtree to recover. Both say `'nl'`.
 *
 * ⚠⚠ THE SWITCH IS GLOBAL SINCE 2026-08-21, AND THAT REVERSED THE RULE THIS NOTE USED TO STATE.
 * It argued that a language control above a screen it does not translate is worse than none — the
 * reader flips it, nothing moves, and they conclude the feature is broken rather than unfinished —
 * so the switch lived inside the Fundamental modal, the only place translated at the time.
 *
 * It now sits in the sidebar, on every page, on request. The argument above was not wrong and the
 * cost is real: pages that are not translated yet do not answer it. What makes it the better trade
 * is that a language is a property of the READER, not of a screen, so a per-screen control is a
 * control the reader has to find again on each one — and this preference is already shared, so a
 * modal opened from anywhere follows it. The mitigation is that the gap is WRITTEN DOWN rather than
 * discovered by pressing: `management/managementCopy.ts` ends with `UNTRANSLATED_SURFACES`, and the
 * switch's own tooltip says not every page answers yet.
 *
 * Translated today: the SIDEBAR end to end — every nav label, the account block and this switch's
 * own tooltip (`sidebarCopy.ts`); the HOME page (heading, intro and every tile —
 * `home/homeCopy.ts`); /management-dashboard's page chrome, Benchmarks, Cross-portfolio and the
 * Overview holdings table; the Fundamental modal's `Long Equity` headings and `Tables`.
 *
 * ⚠ THE SIDEBAR AND THE HOME TILES NAME THE SAME PAGES, and `sidebarCopy.test.ts` pins that they
 * name them IDENTICALLY. Two maps that both label `/schedule` are two chances to call it two
 * things in one screenshot.
 *
 * ⚠ THE HOME PAGE IS THE FIRST SURFACE THAT IS TRANSLATED END TO END, and it is the one the switch
 * is judged by: it is where every reader lands, so an untranslated home page made the control look
 * broken on the very first press. Getting there needed a split — `app/page.tsx` stays a SERVER
 * component (it reads the session and the `view_as` cookie to decide which tiles exist) and hands
 * the hrefs to a client child that looks the copy up. Server-rendered copy cannot follow this
 * preference at all.
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
 * ⚠ `getServerSnapshot` RETURNS THE DEFAULT AND MUST RETURN THE SAME ONE `read()` FALLS BACK TO —
 * `'nl'` since 2026-09-07. These components are `'use client'` but Next still renders them on the
 * server, where `localStorage` does not exist. React uses this snapshot during hydration and
 * re-reads the real one immediately after, which is what keeps the server's HTML and the first
 * client render in agreement — seeding from storage directly makes them disagree and React throws
 * away the subtree to recover.
 *
 * ⚠ A READER WHO HAS CHOSEN `'en'` STILL GETS ONE CORRECTED PAINT, and that is unchanged in kind
 * from before — it was Dutch readers paying it, and it is now the smaller group. There is no way
 * around it without moving the preference into a cookie the server can read.
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
    return isLang(stored) ? stored : 'nl';
  } catch (e) {
    // A blocked or full localStorage is not a reason to fail to render a table.
    console.warn('[bb:i18n] could not read the stored language:', e);
    return 'nl';
  }
}

function getSnapshot(): Lang {
  if (current == null) current = read();
  return current;
}

const getServerSnapshot = (): Lang => 'nl';

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

const OWNER_KEY = 'bb:lang:owner';

/**
 * Bind the stored preference to the reader who chose it, and hand a different reader the DEFAULT.
 *
 * ⚠⚠ THE DEFAULT BEING `'nl'` IS ONLY HALF OF "NEW USERS GET DUTCH", AND THE OTHER HALF IS WHOSE
 * BROWSER THEY ARE IN (2026-09-08, on request). `read()` falls back to `'nl'` only when `bb:lang`
 * is ABSENT, and `setLang` is the only thing that ever writes it — so a stored value is always
 * somebody's deliberate press. On a machine where an admin (or an earlier account) once pressed
 * EN, a brand-new user signs up and reads English, with nothing on screen saying why and no reason
 * to suspect a setting they never touched. A language is a property of the READER, so it has to
 * travel with the account rather than with the browser profile.
 *
 * ⚠ SIGNING OUT FORGETS BOTH, so the next person at this machine starts from the default even
 * before they have an identity — which is the state the login and set-password pages render in.
 *
 * ⚠ A BROWSER THAT PREDATES THIS KEY LOSES ITS CHOICE EXACTLY ONCE: `owner` is absent, the signed-
 * in email is not, so the first claim clears and re-owns. That is deliberate rather than tolerated
 * — the preference cannot be attributed, and the request is that an unattributed reader gets
 * Dutch. Pressing EN again is one click and sticks for good.
 *
 * ⚠ IT NOTIFIES THROUGH THE SAME `listeners` AS `setLang`. Clearing storage without invalidating
 * `current` would leave every mounted component on the previous reader's language until a reload,
 * which is the half-applied state that looks like the switch is broken.
 */
export function claimLangFor(email: string | null): void {
  if (typeof window === 'undefined') return;
  try {
    const owner = window.localStorage.getItem(OWNER_KEY);
    // Same reader as last time — including "still signed out" (both null). Nothing to do, and this
    // is the common path: the Sidebar calls this on every auth-state change.
    if (owner === email) return;
    window.localStorage.removeItem(KEY);
    if (email == null) window.localStorage.removeItem(OWNER_KEY);
    else window.localStorage.setItem(OWNER_KEY, email);
  } catch (e) {
    // ⚠ A BLOCKED STORAGE IS NOT A REASON TO FAIL TO RENDER. Fall through to the notify below so
    // the in-memory snapshot is still consistent with whatever `read()` can see.
    console.warn('[bb:i18n] could not re-own the language preference:', e);
  }
  current = read();
  listeners.forEach((cb) => cb());
}
