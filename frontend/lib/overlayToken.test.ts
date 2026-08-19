import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

/**
 * `--color-overlay` is CHARCOAL INK (#131922). It is only ever a hover wash because every call site
 * applies it at low alpha — `hover:bg-overlay/5`, `/[0.02]`, `/[0.04]`. Used bare it is very nearly
 * black, and the text on top of it does not change colour, so a hover turns dark ink on a dark
 * fill: invisible.
 *
 * ⚠⚠ THIS IS NOT HYPOTHETICAL. The /research-dashboard company picker shipped with
 * `hover:bg-overlay` on its suggestion rows and the highlighted row was unreadable. 198 uses across
 * the app carried an alpha; that one did not, and nothing failed — not tsc, not eslint, not a
 * single test. A design token whose entire meaning lives in a modifier needs a check that the
 * modifier is there.
 *
 * ⚠ IT READS THE SOURCE, WHICH IS WHY IT CAN CATCH THIS AT ALL. There is no rendered pixel to
 * assert on in a unit test, and the repo bans anything that boots a browser. Reading files is
 * milliseconds and needs no DB, no network and no build.
 */

const ROOTS = ['app', 'lib'];

function walk(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    const p = join(dir, entry);
    if (entry === 'node_modules' || entry.startsWith('.')) continue;
    if (statSync(p).isDirectory()) walk(p, out);
    else if (/\.(tsx?|css)$/.test(entry)) out.push(p);
  }
  return out;
}

/**
 * A `bg-overlay` / `text-overlay` / `border-overlay` NOT followed by `/<alpha>`.
 *
 * ⚠ The `(?![\w-])` is what makes this precise rather than noisy: without it the pattern also
 * matches `bg-overlay-something` and every longer token that merely starts the same way.
 */
const BARE = /\b(?:bg|text|border|from|to|via)-overlay(?![\w-])(?!\/)/;

describe('the overlay token is never used at full opacity', () => {
  const files = ROOTS.flatMap((r) => walk(r));

  it('finds source to check at all', () => {
    // ⚠ A guard on the guard. If the walk breaks, every assertion below passes vacuously and the
    // rule silently stops being enforced — which is worse than not having it.
    expect(files.length).toBeGreaterThan(100);
  });

  it('no component applies it bare', () => {
    const offenders: string[] = [];
    for (const f of files) {
      // globals.css DEFINES the token and is allowed to name it; components CONSUME it.
      if (f.endsWith('globals.css')) continue;
      // ⚠ AND THIS FILE, which has to quote the broken form both in its prose and in the negative
      // test below. Excluding it by NAME rather than excluding every `*.test.*`: a component's own
      // test has no more business writing a bare overlay than the component does, and a blanket
      // exemption is how the rule quietly stops covering half the tree.
      if (f.endsWith('overlayToken.test.ts')) continue;
      readFileSync(f, 'utf8').split('\n').forEach((line, i) => {
        if (BARE.test(line)) offenders.push(`${f}:${i + 1}  ${line.trim().slice(0, 100)}`);
      });
    }
    expect(offenders, `overlay used at full opacity — it is charcoal ink (#131922), so the text on `
      + `top of it becomes unreadable. Add an alpha, e.g. hover:bg-overlay/[0.04].\n`
      + offenders.join('\n')).toEqual([]);
  });

  it('the pattern actually fires — otherwise this suite proves nothing', () => {
    // ⚠ A NEGATIVE TEST FOR A NEGATIVE ASSERTION. "No offenders" is also what a broken regex
    // returns, and that is exactly how a rule like this rots into decoration.
    expect(BARE.test('className="hover:bg-overlay transition-colors"')).toBe(true);
    expect(BARE.test('className="bg-overlay"')).toBe(true);
    expect(BARE.test('hover:bg-overlay/[0.04]')).toBe(false);
    expect(BARE.test('hover:bg-overlay/5')).toBe(false);
    expect(BARE.test('bg-overlay-ish')).toBe(false);
  });
});
