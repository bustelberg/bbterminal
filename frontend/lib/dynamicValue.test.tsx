/**
 * ⚠⚠ TEXT MUST SURVIVE EVERY PATH THROUGH THE SPLITTER. The badging itself is cosmetic — if a
 * value renders unbadged nobody is misled. Losing a run of text is not cosmetic: a tooltip that
 * silently drops the second half of a sentence still looks like a finished tooltip, which is the
 * same shape of failure as the unescaped `%` that truncated a formula at its first percentage.
 */
import { describe, expect, it } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

import DynamicText, { hasValues, splitValues, v } from './dynamicValue';

/** ⚠ RESTATED HERE RATHER THAN EXPORTED. Exporting the marks would invite a caller to build one
 *  by hand; duplicating them in the test pins the codepoints, which is the point — they were
 *  U+0001/U+0002 and painted tofu boxes wherever a marked string bypassed the renderer. */
const MARK_OPEN = String.fromCharCode(0x2060);
const MARK_CLOSE = String.fromCharCode(0x2061);

/** Everything the reader would see, tags stripped — the only claim worth asserting on markup. */
const shown = (node: React.ReactElement) =>
  renderToStaticMarkup(node).replace(/<[^>]*>/g, '');

describe('v', () => {
  it('marks a value so the splitter can find it', () => {
    expect(splitValues(`held ${v(44)} names`)).toEqual([
      { text: 'held ', dynamic: false },
      { text: '44', dynamic: true },
      { text: ' names', dynamic: false },
    ]);
  });

  it('takes a number, a string, or nothing at all', () => {
    expect(splitValues(v('ACWI'))).toEqual([{ text: 'ACWI', dynamic: true }]);
    expect(splitValues(v(0))).toEqual([{ text: '0', dynamic: true }]);
    // ⚠ `v(null)` IS A REAL CASE — a date that was never recorded, formatted away upstream — and
    // an empty badge is a floating grey rectangle that reads as a rendering fault.
    expect(splitValues(v(null))).toEqual([]);
    expect(splitValues(v(undefined))).toEqual([]);
    expect(splitValues(v(''))).toEqual([]);
  });

  it('⚠ strips marks out of the value itself', () => {
    // ⚠⚠ A VALUE IS DATA — a portfolio name somebody typed, a label from a vendor. One carrying a
    // sentinel would close its badge early and swallow the rest of the sentence into the next one.
    const hostile = `${MARK_CLOSE}evil${MARK_OPEN}`;
    expect(splitValues(`a ${v(hostile)} b`)).toEqual([
      { text: 'a ', dynamic: false },
      { text: 'evil', dynamic: true },
      { text: ' b', dynamic: false },
    ]);
  });
});

describe('splitValues', () => {
  it('leaves unmarked prose entirely alone', () => {
    expect(splitValues('nothing dynamic here')).toEqual([
      { text: 'nothing dynamic here', dynamic: false },
    ]);
    expect(splitValues('')).toEqual([]);
  });

  it('handles several values and back-to-back ones', () => {
    expect(splitValues(`${v('a')}${v('b')} tail`)).toEqual([
      { text: 'a', dynamic: true },
      { text: 'b', dynamic: true },
      { text: ' tail', dynamic: false },
    ]);
  });

  it('⚠ keeps every character on an unbalanced mark', () => {
    // ⚠⚠ THE FAILURE THIS GUARDS. A naive pairing turns a stray open mark into a badge that runs
    // to the end of the string — or drops the tail entirely. Here the mark is discarded and the
    // text is not: at worst a value renders unbadged, which nobody is harmed by.
    const stray = `head ${MARK_OPEN}tail never closed`;
    expect(splitValues(stray).map((p) => p.text).join('')).toBe('head tail never closed');
    expect(splitValues(stray).every((p) => !p.dynamic)).toBe(true);
  });

  it('never loses text, whatever the marks do', () => {
    for (const s of [`${MARK_CLOSE}orphan close`, `${MARK_OPEN}${MARK_OPEN}double open${MARK_CLOSE}`,
      `a${MARK_CLOSE}b${MARK_OPEN}c`, MARK_OPEN, MARK_CLOSE]) {
      const joined = splitValues(s).map((p) => p.text).join('');
      // Every non-mark character is still there, in order.
      expect(joined).toBe(s.split(MARK_OPEN).join('').split(MARK_CLOSE).join(''));
    }
  });
});

describe('hasValues', () => {
  it('is the cheap check that lets unmarked copy skip the splitter', () => {
    expect(hasValues('plain')).toBe(false);
    expect(hasValues(`marked ${v(1)}`)).toBe(true);
  });
});

describe('DynamicText', () => {
  it('renders the same words either way', () => {
    // ⚠ THE READER'S SENTENCE IS UNCHANGED BY BADGING — only its typography. A conversion that
    // altered the text would be a copy change disguised as a styling one.
    expect(shown(<DynamicText text={`held ${v(44)} of ${v(1678)} names`} />))
      .toBe('held 44 of 1678 names');
    expect(shown(<DynamicText text="held 44 of 1678 names" />))
      .toBe('held 44 of 1678 names');
  });

  it('wraps only the marked runs', () => {
    const html = renderToStaticMarkup(<DynamicText text={`held ${v(44)} names`} />);
    expect(html.match(/<span/g)?.length).toBe(1);
    expect(html).toContain('>44</span>');
  });

  it('emits no markup at all for unmarked prose', () => {
    // ⚠ WHAT MAKES ADOPTION INCREMENTAL: a string nobody has converted renders byte-for-byte as
    // it did before this component existed.
    expect(renderToStaticMarkup(<DynamicText text="held 44 names" />)).toBe('held 44 names');
  });
});
