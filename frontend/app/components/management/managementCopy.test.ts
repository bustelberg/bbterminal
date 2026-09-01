import { describe, expect, it } from 'vitest';

import { LANGS, type Lang } from '../../../lib/i18n';
import { MANAGEMENT_COPY, type ManagementCopy } from './managementCopy';

/**
 * The /management-dashboard copy, in both languages.
 *
 * ⚠⚠ THE TYPE ALREADY CATCHES A MISSING KEY — `nl` is declared as `ManagementCopy`, so a string
 * added to `en` and forgotten fails `tsc`. What the compiler CANNOT catch is the two failures that
 * actually ship: a key that exists in Dutch and is still the English word, and a key that exists
 * and is empty. Both render as a screen that looks translated and is not, which is the state a
 * reader has no way to report except as "it did not work".
 *
 * ⚠ WALKED RECURSIVELY, NOT LISTED. A hand-written list of paths is a second declaration of the
 * shape, and the one thing certain about it is that it will fall behind the first — the copy tree
 * is nested precisely so it can grow a section at a time.
 *
 * Pure — no DOM, no network.
 */

/** Every leaf string in one language's tree, as `path -> value`. */
function leaves(node: unknown, path = '', out: Record<string, string> = {}): Record<string, string> {
  if (typeof node === 'string') {
    out[path] = node;
    return out;
  }
  if (node && typeof node === 'object') {
    for (const [k, v] of Object.entries(node)) leaves(v, path ? `${path}.${k}` : k, out);
  }
  return out;
}

const EN = leaves(MANAGEMENT_COPY.en);
const NL = leaves(MANAGEMENT_COPY.nl);

describe('both languages are complete', () => {
  it.each(LANGS)('%s has a non-empty string at every path', (lang: Lang) => {
    const entries = Object.entries(leaves(MANAGEMENT_COPY[lang]));
    expect(entries.length).toBeGreaterThan(0);
    expect(entries.filter(([, v]) => v.trim() === '').map(([k]) => k)).toEqual([]);
  });

  it('the two trees have exactly the same shape', () => {
    // Belt and braces over the type: an `as` or a widened literal anywhere in the chain would let
    // a key diverge, and the symptom is one blank cell rather than a build failure.
    expect(Object.keys(NL).sort()).toEqual(Object.keys(EN).sort());
  });
});

describe('⚠ the Dutch is actually Dutch', () => {
  /**
   * ⚠ THE EXCEPTIONS ARE NAMED, NOT INFERRED. These read identically in both languages ON PURPOSE
   * and a check that flagged them would be turned off within a week:
   *
   *   · `Benchmark`, `Sector`, `ISIN` — the Dutch words too. A reader of Dutch financial copy
   *     expects them; "ijkpunt" for benchmark is a translation nobody in this domain writes.
   *   · `YTD (€)` — an abbreviation and a currency symbol, with nothing to translate.
   *   · `Benchmarks` (the tab and the panel heading) — same word, same reason.
   *   · `Scan AIRS` — a verb Dutch borrows unchanged, plus the vendor's own name. "AIRS
   *     doorzoeken" reads as a description of the button rather than as its label, and the
   *     button beside it ("Vernieuwen vanuit AIRS") IS translated, so the pair is not lazy.
   */
  const SAME_BY_DESIGN = new Set([
    'page.tabs.benchmarks.label', 'benchmarks.title', 'benchmarks.colBenchmark',
    'benchmarks.colYtdEur', 'overview.colSector', 'overview.colIsin', 'models.scanAirs',
  ]);

  it('translates every string that is not a term Dutch borrows', () => {
    const untranslated = Object.keys(EN).filter((k) => EN[k] === NL[k] && !SAME_BY_DESIGN.has(k));
    expect(untranslated).toEqual([]);
  });

  it('the named exceptions really are identical, so the list cannot rot', () => {
    // If one of these is ever genuinely translated, this fails and the exception gets removed —
    // rather than sitting in the set for ever, silently excusing a key it no longer describes.
    for (const k of SAME_BY_DESIGN) expect(NL[k], k).toBe(EN[k]);
  });
});

describe("⚠ AIRS's own field names are not in the copy tree", () => {
  it('carries no AirSPMS column name', () => {
    // `Beginwaarde`, `Huidige waarde`, `Werkelijk`, `Asset allocatie` are the SOURCE system's
    // labels. They are already Dutch, they appear identically in the English UI, and that is
    // correct — a reader reconciling this screen against AIRS matches them by eye. Pulling one in
    // here would invite "translating" it in English (breaking the link) or renaming it in Dutch
    // (implying we renamed a field AIRS owns).
    // ⚠⚠ IT GUARDS THE KEYS THAT LABEL AIRS **COLUMNS**, not every string that coincides with
    // one. `overview.allocationBands` is OUR name for the bands policy — the button opens our own
    // min/default/max table per risk profile — and its Dutch is legitimately "Asset allocatie",
    // the same two words AIRS uses for a different thing. Checked over every value, that
    // collision was indistinguishable from importing a source column, which is the thing this
    // actually forbids. Scoped, not relaxed: a `col*` key still may not carry an AIRS name.
    const airs = ['Beginwaarde', 'Huidige waarde', 'Werkelijk', 'Asset allocatie', 'Res. YtD'];
    const columnKeys = Object.keys(EN).filter((k) => /\.col[A-Z]/.test(k));
    expect(columnKeys.length, 'no column keys found — the filter has drifted').toBeGreaterThan(8);
    const all = columnKeys.flatMap((k) => [EN[k], NL[k]]);
    for (const name of airs) {
      expect(all.filter((v) => v === name), name).toEqual([]);
    }
  });
});

describe('the type is the contract', () => {
  it('a section can be added without touching this test', () => {
    // The walk is the point: this asserts on the SHAPE, so a new panel's copy is covered the
    // moment it exists. Named here so the next person adding one knows they need not extend it.
    const en: ManagementCopy = MANAGEMENT_COPY.en;
    expect(Object.keys(en)).toContain('page');
    expect(Object.keys(en).length).toBeGreaterThanOrEqual(5);
  });
});
