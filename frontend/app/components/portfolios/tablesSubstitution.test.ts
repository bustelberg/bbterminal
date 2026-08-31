/**
 * ⚠⚠ THESE TESTS READ THE STRING AND REDO THE ARITHMETIC, WHICH IS THE ONLY THING WORTH ASSERTING
 * HERE. Comparing the output to a hard-coded expected string would pin the wording and prove
 * nothing about the claim — and the claim is the entire feature: a reader who does the printed
 * division must land on the printed answer. So every case parses the operands back OUT of the
 * rendered text and checks them, exactly as somebody with a calculator would.
 *
 * ⚠ THE BUG THIS WAS WRITTEN AFTER: at one decimal the coverage row rendered
 * `100 ÷ 1.2% = 84.7×`, and 100 ÷ 1.2 is 83.3. Nothing about that output looked wrong; it was only
 * wrong if you checked it, which is precisely what a worked example invites the reader to do.
 */
import { describe, expect, it } from 'vitest';
import { meanSub, rateSub } from './tablesSubstitution';
import { subDigits, subNum } from './workedFormula';

/**
 * The addends and the divisor out of `\dfrac{a + b + c}{n} = m\%`.
 *
 * ⚠ LaTeX, NOT UNICODE (the builders were typeset 2026-08-22). `workedMean` emits a real `\dfrac`
 * with an escaped `\%`; it used to emit `(a + b + c) ÷ n = m%`. Only the SPELLING moved — every
 * assertion below still reads the operands back out and redoes the arithmetic, which is the whole
 * point of the file. ⚠ The backslashes are DOUBLED: `\d` in a regex is a digit class, so matching
 * a literal `\dfrac` needs `\\dfrac`. Written singly the regex quietly matches `dfrac` — a pattern
 * that compiles, never matches, and reports as "not a mean line".
 */
function parseMean(line: string) {
  const m = /\\dfrac\{([^}]*)\}\{(\d+)\} = (-?[\d.]+)\\%/.exec(line);
  if (!m) throw new Error(`not a mean line: ${line}`);
  return {
    addends: m[1].split(' + ').map(Number),
    n: Number(m[2]),
    printed: Number(m[3]),
    /** The raw text of each addend — precision is part of what is under test. */
    texts: m[1].split(' + '),
  };
}

/**
 * ⚠⚠ THE LINES ARE JOINED BY A **LaTeX** BREAK, NOT A NEWLINE (2026-08-31). These builders emit one
 * typeset expression that the ⓘ hands to KaTeX whole; a newline-separated string was what made the
 * Tables tooltips print `\\left(\\dfrac{...}` at the reader, backslashes and all, once the
 * expressions themselves were typeset. Splitting on it here is how these tests still read one line
 * at a time.
 */
const LINE = ' \\\\[4pt] ';

const COVER = (burden: number) => (burden > 0 ? 100 / burden : null);
const seriesOf = (from: number, vals: (number | null)[]) =>
  new Map(vals.map((v, i) => [from + i, v]));

describe('meanSub', () => {
  it('prints operands that average to the printed mean', () => {
    const m = seriesOf(2021, [55.4, 54.1, 53.3, 56.6, 57.5]);
    const out = meanSub(m, 2025, 5);
    const [head, line] = out.split(LINE);
    expect(head).toBe(String.raw`\text{Book, 2021-2025}`);

    const { addends, n, printed } = parseMean(line);
    expect(addends).toEqual([55.4, 54.1, 53.3, 56.6, 57.5]);
    expect(n).toBe(5);
    // The reader's own arithmetic, on what is actually on screen — at whatever precision is
    // printed, DERIVED rather than hard-coded. The claim is that the line reconciles, not that it
    // uses any given number of decimals; hard-coding one is what broke this when the floor moved
    // to two (2026-08-22).
    const dp = (String(printed).split('.')[1] ?? '').length;
    const reader = addends.reduce((a, b) => a + b, 0) / n;
    expect(Number(reader.toFixed(dp))).toBe(printed);
  });

  it('reconciles the coverage inversion at the printed precision', () => {
    // ⚠ REAL SHAPE: a burden near 1% is where one decimal loses the digit the division needs.
    const burdens = [0, 2.15, 1.84, 1.12, 0.93, 1.41, 0.88, 0.76, 1.05, 1.66];
    const m = seriesOf(2016, burdens);
    const [head, meanLine, coverLine] = meanSub(m, 2025, 10, COVER).split(LINE);
    expect(head).toBe(String.raw`\text{Book, 2016-2025}`);

    const { addends, n, printed } = parseMean(meanLine);
    expect(n).toBe(10);
    expect(Number((addends.reduce((a, b) => a + b, 0) / n).toFixed(2))).toBe(printed);

    // ⚠ `\\div`, `\\%` and `\\times` — the same line, typeset. The arithmetic asserted
    // below is unchanged; only the spelling of the operators moved.
    const cov = /^100 \\div (-?[\d.]+)\\% = (-?[\d.]+)\\times$/.exec(coverLine);
    expect(cov).not.toBeNull();
    // The operand on the coverage line IS the mean printed on the line above it.
    expect(Number(cov![1])).toBe(printed);
    // And dividing it, as printed, gives the figure claimed — to the digit shown.
    expect((100 / Number(cov![1])).toFixed(1)).toBe(cov![2]);
  });

  it('widens precision for small operands and keeps one precision per list', () => {
    // A near-zero burden: at one decimal every addend would collapse to 0.1 and the mean with it.
    const m = seriesOf(2021, [0.11, 0.09, 0.14, 0.08, 0.12]);
    const { texts, addends, n, printed } = parseMean(meanSub(m, 2025, 5, COVER)
      .split(LINE)[0]);
    expect(new Set(texts.map((t) => t.split('.')[1]?.length ?? 0)).size).toBe(1);
    expect(Number((addends.reduce((a, b) => a + b, 0) / n).toFixed(3))).toBe(printed);
    expect(printed).toBeGreaterThan(0);
  });

  it('does not hand the array index to the digits parameter', () => {
    // ⚠ REGRESSION. `vals.map(subNum)` passes (value, index) — so the first addend rendered at 0
    // decimals, the second at 1, and a reader adding them up got a different mean than the one
    // printed beside them. The list must be uniform whatever its length.
    const m = seriesOf(2016, [1.11, 2.22, 3.33, 4.44, 5.55, 6.66, 7.77, 8.88, 9.99, 1.01]);
    const { texts } = parseMean(meanSub(m, 2025, 10));
    expect(texts.every((t) => t.split('.')[1]?.length === texts[0].split('.')[1]?.length))
      .toBe(true);
  });

  it('lists only the years it actually had, and names that span', () => {
    // ⚠ A SHORT WINDOW MUST NOT PAD. The `n of 10` badge already says the window is short; a list
    // of ten addends under it would contradict the badge in the same tooltip.
    const m = seriesOf(2016, [null, null, 12, 14, null, 16, 18, 20, 22, 24]);
    const [head, line] = meanSub(m, 2025, 10).split(LINE);
    expect(head).toBe(String.raw`\text{Book, 2018-2025}`);
    const { addends, n } = parseMean(line);
    expect(addends).toEqual([12, 14, 16, 18, 20, 22, 24]);
    expect(n).toBe(7);
  });

  it('refuses rather than guessing when there is nothing to work through', () => {
    expect(meanSub(seriesOf(2021, [1, 2]), null, 5)).toBe('');
    expect(meanSub(new Map(), 2025, 5)).toBe('');
    // An all-zero burden has no coverage — the mean line still stands, the inversion does not.
    const flat = meanSub(seriesOf(2021, [0, 0, 0]), 2023, 5, COVER);
    expect(flat.split(LINE)).toHaveLength(1);
  });
});

describe('rateSub', () => {
  /** A `Cagr` exactly as `lineCagr`/`endpointCagr` build one — rate AND the pair it came from. */
  const rate = (fromValue: number, toValue: number, from: string, to: string, years: number) =>
    ({ pct: ((toValue / fromValue) ** (1 / years) - 1) * 100,
      from, to, years, fromValue, toValue });

  it('prints endpoints that compound to the printed rate', () => {
    const got = rate(100, 606.34, '2015', '2025', 10);
    const [head, expr] = rateSub(got).split(LINE);
    expect(head).toBe(String.raw`\text{Book}`);

    // ⚠ LaTeX, NOT UNICODE (2026-08-22). `workedCagr` is typeset by KaTeX now — the endpoints ride
    // as SUBSCRIPTS on the values they belong to rather than in `[brackets]`, and the division is
    // a real `\dfrac` rather than a `÷` glyph given the advance width of a comma. This regex is
    // the same assertion in the new spelling; the RULE it pins (the later period is the numerator,
    // and the printed pair compounds to the printed rate) has not changed.
    const m = /^\\left\(\\dfrac\{(-?[\d.]+)_\{\\,(\d+)\}\}\{(-?[\d.]+)_\{\\,(\d+)\}\}\\right\)\^\{1\/(\d+)\} - 1 = ([+-][\d.]+)\\%$/
      .exec(expr);
    expect(m).not.toBeNull();
    const [, to, toP, from, fromP, years, shown] = m!;
    // ⚠ THE ENDPOINTS ARE LABELLED WITH THEIR PERIODS, which is what makes this line readable
    // without a caption — and the later one has to be the numerator.
    expect([toP, fromP]).toEqual(['2025', '2015']);
    const reader = ((Number(to) / Number(from)) ** (1 / Number(years)) - 1) * 100;
    expect(Math.abs(reader - Number(shown))).toBeLessThan(0.1);
  });

  it('keeps enough digits on a small base to reconcile', () => {
    const got = rate(2.4913, 8.137, '2020', '2025', 5);
    const expr = rateSub(got);
    const nums = [...expr.matchAll(/(-?[\d.]+)_\{\\,/g)].map((x) => Number(x[1]));
    const reader = ((nums[0] / nums[1]) ** (1 / 5) - 1) * 100;
    expect(Math.abs(reader - got.pct)).toBeLessThan(0.1);
  });

  it('refuses a rate it cannot show the endpoints of', () => {
    expect(rateSub(null)).toBe('');
    expect(rateSub({ pct: null, reason: 'no line' })).toBe('');
    // ⚠ A NON-POSITIVE BASE. Every producer refuses one, so this cannot arrive from the app — but
    // `(606 ÷ -3) ^ …` printed beside a positive rate would be a worked example of something
    // impossible, so the guard is asserted rather than assumed.
    expect(rateSub({ pct: 10, from: '2015', to: '2025', years: 10,
      fromValue: -3, toValue: 606 })).toBe('');
  });
});

describe('subNum', () => {
  it('scales precision to magnitude', () => {
    // ⚠ TWO IS THE FLOOR since 2026-08-22 — every figure on the risk views prints two decimals, so
    // a worked line showing `55.4` beside a tile reading `55.40` invites the reader to check
    // whether they are the same number. See `workedFormula.subDigits`.
    expect(subDigits(55.4)).toBe(2);
    expect(subDigits(1.18)).toBe(2);
    expect(subDigits(1200)).toBe(0);       // reads as an integer at that magnitude
    expect(subDigits(0.108)).toBe(3);      // about to be a denominator
    expect(subNum(1.18)).toBe('1.18');
    expect(subNum(1.18, 1)).toBe('1.2');
  });

  it('treats a negative like its magnitude', () => {
    expect(subDigits(-0.5)).toBe(subDigits(0.5));
    expect(subNum(-1.234)).toBe('-1.23');
  });
});
