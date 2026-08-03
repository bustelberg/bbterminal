/**
 * ⚠ THE ICON HAD FORKED FOUR WAYS, AND NOTHING COULD HAVE CAUGHT IT.
 *
 * `InfoTip`, a second `InfoTip` under `universe/`, `ApiUsageBadge` and `Provenance` each carried
 * their own copy of the class string. They drifted into two visibly different icons — a grey
 * OUTLINED circle at `w-4`/`text-[10px]` and a filled ACCENT circle at `w-3.5`/`text-[9px]` — and
 * both appeared on the same screen, so one affordance read as two controls. No type error, no
 * lint error, no failing test: a duplicated class string is invisible to every tool we run.
 *
 * So the guard is a source scan. It is a fast unit test (reads files, no DOM, no network, no
 * build) and it fails on the ONE thing review cannot reliably catch: a fifth copy.
 */
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

import { INFO_ICON, INFO_ICON_WARN } from './infoIcon';

const ROOTS = ['app', 'lib'];

/** The modules that DEFINE the shared icon and card. Everything else must only import them.
 *  ⚠ Named exactly, never by prefix: a future `infoIconLegacy.tsx` would exempt itself from the
 *  very check it needs to fail. */
const DEFINERS = new Set(['infoIcon.ts', 'infoIcon.test.ts', 'tipCard.tsx', 'tipCard.test.ts']);

function sourceFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    if (entry === 'node_modules' || entry.startsWith('.')) continue;
    const p = join(dir, entry);
    if (statSync(p).isDirectory()) out.push(...sourceFiles(p));
    else if (/\.tsx?$/.test(entry) && !DEFINERS.has(entry)) out.push(p);
  }
  return out;
}

const FILES = ROOTS.flatMap((r) => sourceFiles(join(process.cwd(), r)));

/** Read once, scan many. The three source-scanning checks below used to call `readFileSync` over
 *  the whole tree each — ~500 files × 3 — which made this the slowest file in the suite by an
 *  order of magnitude (650ms of the 1.3s the entire 535-test run spends executing assertions).
 *  Nothing about what is checked changes: same files, same patterns, same offender list. */
const SOURCES: readonly (readonly [path: string, text: string])[] =
  FILES.map((f) => [f, readFileSync(f, 'utf8')] as const);

/** Files whose text satisfies `hit`, reported repo-relative the way the assertions expect. */
const offendersWhere = (hit: (text: string) => boolean): string[] =>
  SOURCES.filter(([, text]) => hit(text)).map(([f]) => f.replace(process.cwd(), ''));

describe('there is exactly one info icon', () => {
  it('finds source files to scan at all', () => {
    // ⚠ Without this, a broken path makes every assertion below pass over an EMPTY list — a
    // green suite that checks nothing, which is worse than a red one.
    expect(FILES.length).toBeGreaterThan(50);
  });

  it.each([
    ['a round bordered "i" (the old outlined style)', /rounded-full border border-neutral-600/],
    ['a hand-rolled accent circle', /rounded-full[^`"']*bg-accent-500\/10/],
  ])('no file hand-rolls %s', (_label, pattern) => {
    expect(offendersWhere((text) => pattern.test(text))).toEqual([]);
  });

  it('no file hand-rolls the tooltip CARD shell either', () => {
    // ⚠ The icon forked four ways; the card it opens forked twice — a designed provenance card
    // beside bare paragraphs. Same gesture, two objects. `lib/tipCard` is the only shell.
    //
    // ⚠ A SUBSTRING, NOT A REGEX. `/min-w-[13rem]/` reads as a CHARACTER CLASS — it matches a
    // single one of 1,3,r,e,m and never the literal token, so the check passed over every file
    // while finding nothing. A vacuous green guard is worse than no guard: it is a claim that
    // something is checked.
    const SHELL = 'space-y-2 min-w-[13rem]';
    expect(offendersWhere((text) => text.includes(SHELL))).toEqual([]);
  });

  it('...and that shell string is the one tipCard actually uses', () => {
    // Pins the guard above to reality: if the shell is restyled, this fails and the guard gets
    // updated with it, instead of silently watching for a string that no longer exists.
    expect(readFileSync(join(process.cwd(), 'lib/tipCard.tsx'), 'utf8'))
      .toContain('space-y-2 min-w-[13rem]');
  });

  it('the two variants share their geometry, differing only in hue', () => {
    // ⚠ A warning state must read as the SAME control in a different state. If the sizes drift,
    // a stale badge becomes a different-looking button.
    const geometry = (s: string) => s.match(/w-3\.5 h-3\.5|text-\[9px\]|rounded-full/g)?.sort();
    expect(geometry(INFO_ICON_WARN)).toEqual(geometry(INFO_ICON));
    expect(INFO_ICON).not.toEqual(INFO_ICON_WARN);
  });

  it.each([
    // ⚠ THE FORK THAT ARRIVES THROUGH INHERITANCE. The icon's content is the literal character
    // `i` — it is TEXT, and text inherits. Each property left unset here was picked by whatever
    // container the icon happened to sit in, and each one produced the same bug in a different
    // place on the SAME page:
    //   font-family     the ⓘ beside a portfolio NAME (`text-fg`) vs beside its position COUNT
    //                   (`font-mono`) — one row, two letterforms, two baselines.
    //   text-transform  the attribution `<thead>` is `uppercase tracking-wide`, so `i` rendered
    //                   as a wide-tracked capital `I` — and the WARN `!`, immune to case, then
    //                   disagreed with its own other state.
    // There is no duplicated class string here for a reviewer or the scan above to catch, which
    // is exactly why these are asserted.
    ['font family', /\bfont-(mono|sans|serif)\b/],
    ['text case', /\b(normal-case|uppercase|lowercase|capitalize)\b/],
    ['letter spacing', /\btracking-\w+\b/],
  ])('pins its own %s — an unset property is inherited, not shared', (_label, pattern) => {
    for (const cls of [INFO_ICON, INFO_ICON_WARN]) {
      expect(cls).toMatch(pattern);
    }
    // ...and BOTH variants must pin the SAME value, or the warning state becomes another control.
    const value = (s: string) => s.match(pattern)![0];
    expect(value(INFO_ICON_WARN)).toBe(value(INFO_ICON));
  });

  it('carries no margin — spacing belongs to the call site', () => {
    // Folding `ml-1` in here would force every caller to accept it or override it, and an
    // overridden shared class is how the next fork starts.
    for (const cls of [INFO_ICON, INFO_ICON_WARN]) {
      expect(cls).not.toMatch(/\bm[lrtbxy]?-\d/);
    }
  });
});
