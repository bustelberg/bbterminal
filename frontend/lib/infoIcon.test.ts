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
    const offenders = FILES.filter((f) => pattern.test(readFileSync(f, 'utf8')))
      .map((f) => f.replace(process.cwd(), ''));
    expect(offenders).toEqual([]);
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
    const offenders = FILES.filter((f) => readFileSync(f, 'utf8').includes(SHELL))
      .map((f) => f.replace(process.cwd(), ''));
    expect(offenders).toEqual([]);
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

  it('carries no margin — spacing belongs to the call site', () => {
    // Folding `ml-1` in here would force every caller to accept it or override it, and an
    // overridden shared class is how the next fork starts.
    for (const cls of [INFO_ICON, INFO_ICON_WARN]) {
      expect(cls).not.toMatch(/\bm[lrtbxy]?-\d/);
    }
  });
});
