/**
 * ONE HOUSE STYLE FOR EVERY ⓘ ON /management-dashboard.
 *
 * ⚠⚠ THE MODEL IS THE ACTIVE SHARE CARD, named as such on 2026-08-31: a one-line `what`, a `where`
 * whose live figures are BADGED (`v()`), a `when` that dates both sides, the maths TYPESET through
 * `worked` + `legend`, and nothing else. Four short fields and an equation. The rules below are
 * that card, written down:
 *
 *   1. NO `⚠` IN A TOOLTIP. The warning blocks are how this codebase talks to itself; a reader
 *      hovering a figure wants the figure explained, not the incident that shaped the code. The
 *      reasoning belongs in the source, where it already is.
 *   2. NO UNICODE MATHS. `Σ(w × x) ÷ Σw` in the UI font is a row of glyphs that resembles an
 *      expression — a summation with no limits, `Σ` at the advance width of a comma. Maths goes
 *      through `worked`, which is KaTeX.
 *   3. SHORT. A field is a sentence, not a paragraph. Past `MAX_FIELD` it stops being read, which
 *      makes the caveat inside it worse than useless.
 *
 * ⚠⚠ `UNCONVERTED` IS A RATCHET, NOT A LIST OF EXCEPTIONS. Every file in it is one nobody has
 * rewritten yet; the rule is that the list only ever gets shorter. A new file is covered the moment
 * it exists, which is the half that stops this being a one-off tidy-up that decays.
 *
 * Pure — reads source, no DOM. Same technique as `overlayToken.test.ts` and
 * `portfolioAnalysisColumns.test.ts`, and for the same reason: the property is real, checkable, and
 * nothing else was watching it.
 */
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

/** The tooltip fields. `text` is `InfoTip`'s plain-prose prop; the rest are `AspectCard`'s. */
const FIELDS = ['what', 'where', 'when', 'how', 'hint', 'note', 'text'];

/** Longest a single field may be. ⚠ Two lines of a 22rem card, which is what a reader takes in
 *  before deciding to stop reading. */
const MAX_FIELD = 240;

/** Glyphs that mean somebody wrote maths as text. ⚠ `−` (minus) and `→` are punctuation in a
 *  sentence and are deliberately absent; these four only ever appear in a pseudo-formula. */
const UNICODE_MATHS = ['÷', '×', 'Σ', '√'];

/**
 * Files whose ⓘ copy predates the rule. ⚠ THE LIST SHRINKS AND NEVER GROWS — adding a name here to
 * make a new tooltip pass is the one edit this file exists to prevent.
 */
const UNCONVERTED = new Set([
  'AccountTotalReturn.tsx',
  'AttributionPanel.tsx',
  'CagrTable.tsx',
  'ConcentrationView.tsx',
  'CorrelationView.tsx',
  'DeepValuationTab.tsx',
  'DrawdownView.tsx',
  'MetricGrowthCard.tsx',
  'MultipleHistoryChart.tsx',
  'PortfolioAnalysisModal.tsx',
  'PriceTargetCalculator.tsx',
  'QuickValuationTab.tsx',
  'ReverseDcfPanel.tsx',
  'TablesTab.tsx',
  'TrackingErrorView.tsx',
  'VolatilityView.tsx',
]);

const DIR = join('app', 'components', 'portfolios');

/** Every `.tsx` under the portfolios tree that is not itself a test. */
function componentFiles(): string[] {
  return readdirSync(DIR)
    .filter((f) => f.endsWith('.tsx') && !f.includes('.test.'))
    .filter((f) => statSync(join(DIR, f)).isFile());
}

/**
 * The STRING LITERALS a tooltip field is built from, one entry per field occurrence.
 *
 * ⚠ IT READS THE LITERALS, NOT THE EXPRESSION. A field is routinely a concatenation, a ternary or a
 * call into a copy module; what matters is the prose that reaches the card, so every quoted chunk
 * between the field and the next attribute is joined. A field whose text lives in a copy module
 * contributes nothing here — that module is covered by its own tests.
 */
function fields(source: string): { field: string; body: string }[] {
  const out: { field: string; body: string }[] = [];
  const re = new RegExp(`\\b(${FIELDS.join('|')})=\\{?`, 'g');
  for (const m of source.matchAll(re)) {
    const start = (m.index ?? 0) + m[0].length;
    // To the next attribute or the end of the element — crude and sufficient: the only thing read
    // out of the slice is its quoted chunks.
    const rest = source.slice(start, start + 4000);
    const end = rest.search(/\n\s{2,}[a-zA-Z]+=|\/>|\}\s*\/>/);
    const slice = rest.slice(0, end === -1 ? rest.length : end);
    const chunks = [...slice.matchAll(/'((?:[^'\\]|\\.)*)'|"((?:[^"\\]|\\.)*)"|`([^`]*)`/g)]
      .map((c) => c[1] ?? c[2] ?? c[3] ?? '');
    if (chunks.length) out.push({ field: m[1], body: chunks.join(' ') });
  }
  return out;
}

describe('every ⓘ on the dashboard follows the Active Share card', () => {
  const converted = componentFiles().filter((f) => !UNCONVERTED.has(f));

  it('covers a real set of files, so a green run means something', () => {
    // ⚠ THE GUARD ON THE GUARD. A scanner that silently matched nothing would pass every rule
    // below; this is what says it is actually reading tooltips.
    const total = converted.reduce(
      (n, f) => n + fields(readFileSync(join(DIR, f), 'utf8')).length, 0);
    expect(converted.length).toBeGreaterThan(20);
    expect(total).toBeGreaterThan(80);
  });

  it.each([
    ['carries no ⚠ — the reasoning belongs in the source', (b: string) => b.includes('⚠')],
    ['writes no maths as text — that is what `worked` is for',
      (b: string) => UNICODE_MATHS.some((g) => b.includes(g))],
    ['stays short enough to be read', (b: string) => b.length > MAX_FIELD],
  ])('%s', (_label, offends: (b: string) => boolean) => {
    const bad: string[] = [];
    for (const f of converted) {
      for (const { field, body } of fields(readFileSync(join(DIR, f), 'utf8'))) {
        if (offends(body)) bad.push(`${f} · ${field}= ${body.slice(0, 90)}…`);
      }
    }
    expect(bad, 'see the rules at the top of this file').toEqual([]);
  });

  it('⚠ the ratchet only turns one way', () => {
    // Every name in `UNCONVERTED` must still exist and must still need converting — a file that
    // has been cleaned up and left in the list makes the list a lie, and a file that has been
    // renamed or deleted makes it dead weight nobody will read.
    const present = new Set(componentFiles());
    for (const f of UNCONVERTED) {
      expect(present.has(f), `${f} is in UNCONVERTED but no longer exists`).toBe(true);
      const bodies = fields(readFileSync(join(DIR, f), 'utf8')).map((x) => x.body);
      const stillBad = bodies.some((b) => b.includes('⚠')
        || UNICODE_MATHS.some((g) => b.includes(g)) || b.length > MAX_FIELD);
      expect(stillBad, `${f} is clean — take it out of UNCONVERTED`).toBe(true);
    }
  });
});
