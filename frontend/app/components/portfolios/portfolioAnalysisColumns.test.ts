/**
 * EVERY ROW OF THE ANALYSE MODAL'S HOLDINGS TABLE HAS THE SAME NUMBER OF LEADING CELLS.
 *
 * ⚠⚠ THIS READS THE SOURCE, WHICH IS UNUSUAL AND IS THE ONLY OPTION HERE. The table lives inside a
 * 3,000-line component that needs a DOM to render, and this repo tests no DOM (see CLAUDE.md's
 * unit-tests-only rule). The invariant is nevertheless a pure, checkable property of the FILE: the
 * money columns are gated (`{show('opening') && …}`), so each row's leading block is the run of
 * cells before its first gate, and every one of them must match the header. Same technique as
 * `lib/overlayToken.test.ts`, and for the same reason — a real defect that no tool was watching.
 *
 * ⚠⚠ THE DEFECT IT WAS WRITTEN FOR (2026-08-21): the `No longer held` DETAIL rows carried five
 * leading cells where the header has eight — Momentum, 5y vol and Beta were simply absent. Every
 * figure from Beginwaarde rightward therefore sat THREE columns left of its own title: the opening
 * value under "Momentum", the result under "Beginwaarde", the contribution under "Money-weighted".
 * Plausible numbers, wrong columns, on the one block of rows nobody reconciles — and the file
 * already carried TWO warnings about exactly this hazard ("COLUMN COUNT IS ELEVEN and is counted by
 * hand in FOUR places… every figure below shifts a cell right, silently" and "an empty cell still
 * OCCUPIES the column"). Both were right, both were read, and neither was enforced.
 *
 * ⚠ COMMENTS ARE STRIPPED FIRST. The held row's own note says "the cap lives on a wrapper, not on
 * the `<td>`" — prose naming a tag, which counted as a ninth column on the first run of this check
 * and would have sent the fix at a row that was already correct.
 *
 * ⚠ `colSpan` COUNTS AS ITS SPAN. The class label and the sold-position name both span the three
 * text columns; counting them as one cell each would report four rows short instead of one.
 *
 * Pure — reads a file, no DOM, no network, no render.
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

/** ⚠ RELATIVE TO THE FRONTEND ROOT, which is vitest's cwd — the same convention
 *  `lib/overlayToken.test.ts` uses (`walk('app')`). `__dirname` is not defined under ESM. */
const FILE = join('app', 'components', 'portfolios', 'PortfolioAnalysisModal.tsx');

/** The gate that opens the money block. Everything before it in a row is the leading block. */
const GATE = "{show('opening')";

const source = (): string => {
  const raw = readFileSync(FILE, 'utf8');
  return raw
    .replace(/\{\/\*[\s\S]*?\*\/\}/g, '')     // JSX comments — see the ⚠ above
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/^[ \t]*\/\/.*$/gm, '');
};

/** Cells in one row's leading block, counting a `colSpan={n}` as n. */
function leadingCells(block: string): number {
  let n = 0;
  for (const m of block.matchAll(/<t[dh]\b([^>]*)>/g)) {
    const span = /colSpan=\{(\d+)\}/.exec(m[1] ?? '');
    n += span ? Number(span[1]) : 1;
  }
  return n;
}

/** `{line: leadingCells}` for every row in the table that opens a money block. */
function rows(): { line: number; cells: number }[] {
  const s = source();
  const out: { line: number; cells: number }[] = [];
  for (let i = s.indexOf(GATE); i >= 0; i = s.indexOf(GATE, i + 1)) {
    const tr = s.lastIndexOf('<tr', i);
    out.push({
      line: s.slice(0, tr).split('\n').length,
      cells: leadingCells(s.slice(tr, i)),
    });
  }
  return out;
}

describe('the Analyse modal holdings table', () => {
  it('finds every hand-written row, not just the header', () => {
    // ⚠ THE COUNT OF ROWS IS ITSELF PART OF THE CHECK. If the parse silently matched only the
    // header, the assertion below would pass over one row and prove nothing. Six: the thead, the
    // class group row, the held row, the sold group row, the sold detail row, the grand total.
    expect(rows().length).toBe(6);
  });

  it('⚠⚠ gives every row the SAME leading cells as the header, or its figures sit under the wrong titles', () => {
    const found = rows();
    const header = found[0].cells;
    // Eight: # · Name · Via · Sector · Momentum · 5y vol · Beta · Weight (now).
    expect(header).toBe(8);
    // Reported as {line: cells} so a failure names the row to go and look at, not just a count.
    const offenders = found.filter((r) => r.cells !== header);
    expect(offenders.map((r) => `line ${r.line}: ${r.cells} cells`)).toEqual([]);
  });

  it('counts a colSpan as its span, or four of the six rows would look short', () => {
    // The guard on the guard: three rows span the text columns with `colSpan={3}`, and a checker
    // that counted them as one cell would report a misalignment in rows that are correct — which
    // is the failure mode that gets a check deleted rather than fixed.
    expect(leadingCells('<td /><td colSpan={3}><td /><td /><td /><td>')).toBe(8);
    expect(leadingCells('<td /><td /><td /><td /><td /><td /><td /><td />')).toBe(8);
  });
});

/**
 * EVERY GATED MONEY COLUMN APPEARS IN EVERY ROW OF THE TABLE — the same invariant one column to
 * the right of the one above, and the one the leading-block check cannot see.
 *
 * ⚠⚠ A COLUMN ADDED TO FIVE ROWS OUT OF SIX SHIFTS EVERY FIGURE AFTER IT, in exactly the rows it
 * was forgotten in, and only while the reader has that group ticked. The header, the class
 * subtotal, the held row, the `No longer held` subtotal, the sold detail row and the grand total
 * each render the money block by hand; the file's own ⚠ has said "counted by hand in SIX places"
 * since 2026-08-21, and the leading-block test below it only guards what comes BEFORE the block.
 *
 * ⚠ IT COUNTS `show('k')` OCCURRENCES rather than parsing the rows, because that is the whole
 * claim: six renders per column, no more and no less. A column that legitimately has nothing to
 * show in a row still renders `<td />` — an empty cell occupies the column, a missing one does not.
 *
 * Pure — reads a file, no DOM.
 */
describe('every money column is rendered in every row', () => {
  it('six renders each, from the header to the grand total', () => {
    const s = source();
    const keys = [...new Set([...s.matchAll(/show\('([a-z]+)'\)/g)].map((m) => m[1]))];
    // ⚠ THE SET IS READ OFF THE FILE, not listed here — a new column joins this check by existing.
    expect(keys.length).toBeGreaterThanOrEqual(7);
    // ⚠⚠ `String.raw`, OR THE BACKSLASHES VANISH AND THE TEST PASSES ON NOTHING. In a plain
    // template literal `\(` is just `(`, so the pattern becomes a capture group, matches no line
    // in the file, and every count comes out 0 — all equal, assertion green, invariant unchecked.
    const counts = Object.fromEntries(
      keys.map((k) => [k, [...s.matchAll(new RegExp(String.raw`show\('${k}'\)`, 'g'))].length]));
    const rows = Math.max(...Object.values(counts));
    // ⚠ AND A FLOOR ON THE COUNT ITSELF, for the same reason: six is the number of rows that render
    // the block, so anything less means this test stopped finding them rather than that they agree.
    expect(rows).toBeGreaterThanOrEqual(6);
    expect(counts, `every column must render in all ${rows} rows`).toEqual(
      Object.fromEntries(keys.map((k) => [k, rows])));
  });
});
