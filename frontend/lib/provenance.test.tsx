/**
 * ⚠ A COLUMN HEADER CANNOT BE OUT OF DATE.
 *
 * `Provenance` turns its chip amber — an `!` instead of an `i` — when the source it names is ≥2
 * trading days behind. That is the right signal on a VALUE: staleness is a property of an
 * observation, and the reader who needs to know is the one reading the number.
 *
 * It is meaningless on a HEADING. A header says where a column's numbers come from and how they
 * are computed; both are true whatever date the data carries. Measured on the attribution table:
 * `w_B` and `R_B` sat in the header row wearing a staleness warning, because the call site handed
 * them the benchmark's `asOf` — so the label was flagged out of date while being exactly as
 * current as a label can be, and the actual per-value staleness had no distinct signal left.
 *
 * The guard therefore lives inside `Provenance` (`!column && asOf`), not at the ~90 call sites
 * that would each have to remember it.
 */
import { renderToStaticMarkup } from 'react-dom/server';

import { describe, expect, it } from 'vitest';

import { INFO_ICON_WARN } from './infoIcon';
import { Provenance } from './provenance';
import { snapshotFreshness } from './snapshotAge';

/** Years back, so it is stale under any definition of "trading days behind" — the test must not
 *  depend on which day it runs. */
const LONG_AGO = '2020-01-02';

describe('a column-header ⓘ can never go stale', () => {
  it('the stale signal genuinely fires on a VALUE (or the test below proves nothing)', () => {
    // ⚠ Without this, `column` could be suppressing an alarm that never sounded in the first
    // place, and the assertion underneath would pass over a component that is simply never amber.
    expect(snapshotFreshness(LONG_AGO)?.tone).toBe('stale');

    const html = renderToStaticMarkup(
      <Provenance source="benchmark" asOf={LONG_AGO} kind="copied" note="a value" />,
    );
    expect(html).toContain(INFO_ICON_WARN);
    expect(html).toMatch(/>!</);
  });

  it('...and is suppressed on the same date when the ⓘ describes a COLUMN', () => {
    const html = renderToStaticMarkup(
      <Provenance source="benchmark" asOf={LONG_AGO} column kind="copied" note="a column" />,
    );
    expect(html).not.toContain(INFO_ICON_WARN);
    expect(html).not.toMatch(/>!</);
    expect(html).toMatch(/>i</);
  });

});

// NOT ASSERTED HERE: the card's When line ("per value — each cell carries its own date"). The
// card body is rendered only while the tip is open (`InfoTip` gates it on hover/pin state), so a
// static render contains the chip and nothing else — reaching it would need an event-driven
// render, which is the browser-test machinery this repo does not keep. The chip's colour and
// glyph ARE the at-a-glance signal, and those are what the two tests above pin.
