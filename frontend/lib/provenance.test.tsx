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
import { Provenance, ProvenanceFetchedAt } from './provenance';
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

/**
 * ⚠⚠ AMBER MEANS "YOU CAN FIX THIS", NOT "THIS IS OLD".
 *
 * Measured 2026-08-17, immediately after a full "Refresh all": 27 of 45 account rows wore the `!`,
 * and 23 of them had been read that same afternoon — AIRS had simply not valued those books since
 * (its newest valuation anywhere was 2026-08-15, and many books stop at 08-11/12). Not one of the
 * 23 could be cleared by any action on the page, which is how an alarm becomes furniture and takes
 * the four rows that WERE ours to fix down with it.
 *
 * The date and its age stay in the card either way. Only the colour moves.
 */
describe('the amber chip is reserved for the lag we own', () => {
  /** Two trading days is the same threshold `snapshotFreshness` uses; a fixed recent date would go
   *  stale as the test aged, so it is computed from today. */
  const today = new Date().toISOString().slice(0, 10);

  it('stays quiet when we read the source today and the SOURCE is what is behind', () => {
    const html = renderToStaticMarkup(
      <Provenance source="airs_att" asOf={LONG_AGO} fetchedAt={`${today}T13:15:00Z`}
        kind="copied" note="a value" />,
    );
    expect(html).not.toContain(INFO_ICON_WARN);
    expect(html).toMatch(/>i</);
  });

  it('still fires when OUR copy is the stale side', () => {
    const html = renderToStaticMarkup(
      <Provenance source="airs_att" asOf={LONG_AGO} fetchedAt={`${LONG_AGO}T13:15:00Z`}
        kind="copied" note="a value" />,
    );
    expect(html).toContain(INFO_ICON_WARN);
    expect(html).toMatch(/>!</);
  });

  it('⚠ still fires when we do NOT KNOW when we last read it', () => {
    /** The AMD incident is why: a 4-day-old cached value read EUR 114,587 / +142% against
     *  AIRS-live's EUR 107,086 / +126%, with nothing on the page saying it was from a past scan.
     *  Silence on an unknown fetch would hide exactly that, so absence keeps the warning. */
    const html = renderToStaticMarkup(
      <Provenance source="airs_att" asOf={LONG_AGO} kind="copied" note="a value" />,
    );
    expect(html).toContain(INFO_ICON_WARN);
  });
});

describe('a subtree can supply the fetch time once, for all of its icons', () => {
  const today = new Date().toISOString().slice(0, 10);

  it('an icon with no fetchedAt of its own inherits the provider’s', () => {
    /** The expanded account panel renders 40 of these against one book — see
     *  `ProvenanceFetchedAt`. Without the provider each one would go amber on AIRS's lag. */
    const html = renderToStaticMarkup(
      <ProvenanceFetchedAt at={`${today}T13:15:00Z`}>
        <Provenance source="airs_volk" asOf={LONG_AGO} kind="formula" note="a value" />
      </ProvenanceFetchedAt>,
    );
    expect(html).not.toContain(INFO_ICON_WARN);
  });

  it('an icon with its OWN older fetchedAt is not silenced by the provider', () => {
    /** The prop wins, so a nested exception stays possible — one number in the subtree may come
     *  from a read we know to be older than the account's. */
    const html = renderToStaticMarkup(
      <ProvenanceFetchedAt at={`${today}T13:15:00Z`}>
        <Provenance source="airs_volk" asOf={LONG_AGO} fetchedAt={`${LONG_AGO}T09:00:00Z`}
          kind="formula" note="a value" />
      </ProvenanceFetchedAt>,
    );
    expect(html).toContain(INFO_ICON_WARN);
  });

  it('outside a provider nothing changes', () => {
    const html = renderToStaticMarkup(
      <Provenance source="airs_volk" asOf={LONG_AGO} kind="formula" note="a value" />,
    );
    expect(html).toContain(INFO_ICON_WARN);
  });

  it('⚠ A NESTED PROVIDER RESETS IT — a different source object does NOT inherit', () => {
    /**
     * ⚠⚠ `PortfolioAnalysisModal` DEPENDS ON THIS. It wraps its whole subtree in the book's fetch
     * time so every badge describing that book agrees with the row that opened it — but the
     * Fundamental and Holding-timing modals render INSIDE that box (for event-propagation
     * reasons, a layout fact and not a claim about their data) and describe a COMPANY, not the
     * book. Inheriting the book's "we read it at ..." would de-amber a fundamental that really is
     * ours to refresh, which is the hazard `ProvenanceFetchedAt`'s own note warns about: one
     * object's fetch time silencing another object's staleness.
     *
     * `at={undefined}` is the reset, and it must behave exactly like no provider at all.
     */
    const html = renderToStaticMarkup(
      <ProvenanceFetchedAt at={`${today}T13:15:00Z`}>
        <ProvenanceFetchedAt at={undefined}>
          <Provenance source="airs_volk" asOf={LONG_AGO} kind="formula" note="a value" />
        </ProvenanceFetchedAt>
      </ProvenanceFetchedAt>,
    );
    expect(html).toContain(INFO_ICON_WARN);
  });
});

// NOT ASSERTED HERE: the card's When line ("per value — each cell carries its own date"). The
// card body is rendered only while the tip is open (`InfoTip` gates it on hover/pin state), so a
// static render contains the chip and nothing else — reaching it would need an event-driven
// render, which is the browser-test machinery this repo does not keep. The chip's colour and
// glyph ARE the at-a-glance signal, and those are what the two tests above pin.
