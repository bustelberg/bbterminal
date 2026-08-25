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

import { INFO_ICON, INFO_ICON_WARN } from './infoIcon';
import {
  Provenance, ProvenanceFetchedAt, provenanceFreshness, sourceField, sourceLabel, sourceVendor,
} from './provenance';
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
     *  `ProvenanceFetchedAt`. Without the provider each one would go amber on AIRS's lag.
     *
     *  ⚠⚠ IT ASSERTS THE **AGED** STATE, NOT MERELY "NOT AMBER", AND THAT WEAKNESS IS WHAT LET
     *  THE BUG THROUGH. `not.toContain(INFO_ICON_WARN)` also passes when the icon is the ordinary
     *  BLUE one — which is precisely what shipped: a card reading blue on the outside and
     *  "⚠ 3 trading days old" on the inside. A negative assertion about one of three states says
     *  nothing about which of the other two you got. */
    const html = renderToStaticMarkup(
      <ProvenanceFetchedAt at={`${today}T13:15:00Z`}>
        <Provenance source="airs_volk" asOf={LONG_AGO} kind="formula" note="a value" />
      </ProvenanceFetchedAt>,
    );
    expect(html).toContain(INFO_ICON);
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

/**
 * ⚠⚠ THE ⓘ AND ITS CARD MUST ALWAYS SAY THE SAME THING (2026-08-18).
 *
 * Reported: an account row whose ⓘ was BLUE while the card it opened read "2026-08-13 · 3 trading
 * days old" in AMBER. Two answers to one question, one hover apart. The cause was two
 * computations: the icon read `lagOwner(asOf, fetchedAt)`, the card's When pill read
 * `snapshotFreshness(asOf)` ALONE. Both are now `provenanceFreshness`, which is why these tests
 * assert the icon and the pill together rather than either on its own.
 *
 * ⚠⚠ AND "CURRENT" MEANS OUR COPY MATCHES THE SOURCE. If we fetched today and the source has
 * published nothing since, the figure IS current — it is the whole of what is knowable. A middle
 * state for "old, but not ours to fix" was tried and removed the same day: either the data is
 * current or it is not, and what date the source stamped on its own valuation is a fact about the
 * source, stated plainly in the When line rather than coloured as a fault.
 */
describe('the icon and its card carry ONE verdict', () => {
  const today = new Date().toISOString().slice(0, 10);
  const yesterday = new Date(Date.now() - 864e5).toISOString().slice(0, 10);

  /** The card is rendered only while the tip is open, so the pill is asserted through
   *  `provenanceFreshness` directly — the same function the icon reads. */
  it('⚠⚠ REVERSED (2026-08-19) — READ YESTERDAY IS NOW AMBER. This asserted BLUE for "read '
     + 'yesterday, valued days ago", on the old ≥2-trading-day threshold. The rule is now: not '
     + 'read TODAY is outdated. Amber stays actionable either way — a Refresh sets `fetched_at` to '
     + 'now and clears it — which is the property the 2026-08-17 incident was about', () => {
    const html = renderToStaticMarkup(
      <Provenance source="airs_att" asOf={LONG_AGO} fetchedAt={`${yesterday}T13:00:00Z`}
        kind="copied" note="a value" />,
    );
    expect(html).toContain(INFO_ICON_WARN);
    // ...and the card agrees, because it is the same call.
    expect(provenanceFreshness(LONG_AGO, `${yesterday}T13:00:00Z`).stale).toBe(true);
  });

  it('⚠⚠ THE SOURCE’S OWN AGE IS NO LONGER REPORTED AT ALL (2026-08-19). This asserted the '
     + 'label said "the source’s latest" for a book read today but valued long ago. Where we '
     + 'know when WE read it, that is the whole answer — the valuation age answers a question '
     + 'nobody on this page is asking, and printing both put two clocks in one row with the '
     + 'actionable one hidden', () => {
    const today = new Date().toISOString().slice(0, 10);
    const f = provenanceFreshness(LONG_AGO, `${today}T13:00:00Z`);
    expect(f.stale).toBe(false);
    expect(f.label).toBe('read today');
    expect(f.label).not.toContain('source');
    // ⚠ And nothing about trading-day ages either — that phrasing was the source's clock.
    expect(f.label).not.toMatch(/old|trading day/);
  });

  it('⚠ WHEN SHOWS OUR READ DATE, not the valuation date, wherever we know it', () => {
    const html = renderToStaticMarkup(
      <Provenance source="airs_volk" asOf={LONG_AGO} fetchedAt={`${yesterday}T13:00:00Z`}
        kind="copied" note="a value" />,
    );
    // The card only renders inside the open tip, so assert the verdict the row is built from.
    expect(provenanceFreshness(LONG_AGO, `${yesterday}T13:00:00Z`).label)
      .toMatch(/not read today|read \d+ trading day/);
    expect(html).toContain(INFO_ICON_WARN);
  });

  it('⚠ UNCHANGED WHERE THE FETCH TIME IS UNKNOWN. Most call sites pass no `fetchedAt`; treating '
     + '"cannot tell" as "not today" would turn the whole app amber at once', () => {
    expect(provenanceFreshness(LONG_AGO, undefined).stale).toBe(true);    // stale source, as before
    const today = new Date().toISOString().slice(0, 10);
    expect(provenanceFreshness(today, undefined).stale).toBe(false);      // fresh source, as before
  });

  it('OUR lag is amber in both places', () => {
    const html = renderToStaticMarkup(
      <Provenance source="airs_att" asOf={LONG_AGO} fetchedAt={`${LONG_AGO}T09:00:00Z`}
        kind="copied" note="a value" />,
    );
    expect(html).toContain(INFO_ICON_WARN);
    expect(provenanceFreshness(LONG_AGO, `${LONG_AGO}T09:00:00Z`).stale).toBe(true);
  });

  it('⚠ UNKNOWN WHOSE LAG STAYS AMBER — most call sites pass no fetchedAt', () => {
    // Silence there would hide the incident this signal exists for: a 4-day-old cached value
    // reading EUR 114,587 / +142% against AIRS-live's EUR 107,086 / +126%.
    expect(provenanceFreshness(LONG_AGO, undefined).stale).toBe(true);
    expect(renderToStaticMarkup(
      <Provenance source="airs_att" asOf={LONG_AGO} kind="copied" note="a value" />,
    )).toContain(INFO_ICON_WARN);
  });

  it('a genuinely current figure is blue with no pill to contradict it', () => {
    const f = provenanceFreshness(today, `${today}T09:00:00Z`);
    expect(f.stale).toBe(false);
    expect(renderToStaticMarkup(
      <Provenance source="airs_att" asOf={today} fetchedAt={`${today}T09:00:00Z`}
        kind="copied" note="a value" />,
    )).toContain(INFO_ICON);
  });

  it('a column header is never stale in either place', () => {
    expect(provenanceFreshness(LONG_AGO, undefined, true).stale).toBe(false);
    expect(renderToStaticMarkup(
      <Provenance source="airs_att" asOf={LONG_AGO} column kind="copied" note="a value" />,
    )).not.toContain(INFO_ICON_WARN);
  });
});

/**
 * ⚠ THE PROSE AND THE BADGE MUST NAME A SOURCE THE SAME WAY. `sourceLabel` exists so a card that
 * says where a figure came from in a sentence reads out of the same table the badge beside it
 * renders from — the alternative is a hand-typed "AIRS" that drifts the moment a label here is
 * made more precise.
 */
describe('sourceLabel', () => {
  it('gives the badge’s own words for a source', () => {
    expect(sourceLabel('airs_model')).toBe('AIRS Model-portefeuille');
    expect(sourceLabel('airs_volk')).toBe('AIRS Vermogensoverzicht (VOLK)');
  });

  it('⚠ keeps the benchmark’s three inputs apart', () => {
    // ⚠⚠ AN INDEX HAS A PRICE SOURCE, A WEIGHT SOURCE AND AN ETF SOURCE, and they are genuinely
    // different data: closes give it a return, market caps give it its weights, and the ETF is a
    // separate series again (ACWI YTD reads 11.83% rebuilt vs 14.67% from the fund). Active share
    // reads the CAPS and never touches a close, so collapsing any two of these labels would print
    // a series the figure does not use — the exact mislabel `benchmark_etf` was split out for.
    const [close, caps, etf] = (['benchmark', 'benchmark_caps', 'benchmark_etf'] as const)
      .map(sourceLabel);
    expect(new Set([close, caps, etf]).size).toBe(3);
    expect(caps).toContain('market cap');
    expect(close).not.toContain('market cap');
  });
});

/**
 * ⚠⚠ THE PARTS AND THE LABEL CANNOT DISAGREE, because the label is BUILT from the parts. This is
 * the invariant that makes it safe for prose to badge "market cap" and "yfinance" separately: the
 * day `benchmark_caps` stops being yfinance, the sentence changes with the registry instead of
 * quietly keeping a hand-typed vendor name.
 */
describe('sourceVendor / sourceField', () => {
  it('splits a label into the two facts a sentence needs apart', () => {
    expect(sourceField('benchmark_caps')).toBe('market cap');
    expect(sourceVendor('benchmark_caps')).toBe('yfinance');
  });

  it('⚠ the field carries no qualifier', () => {
    // ⚠ "(benchmark constituents)" disambiguates a label standing alone; inside a sentence that has
    // already said "priced index members" it is the same fact twice.
    expect(sourceField('benchmark_caps')).not.toContain('(');
    expect(sourceLabel('benchmark_caps')).toContain('(benchmark constituents)');
  });

  it('composes back into the label for every source', () => {
    for (const k of ['airs_volk', 'airs_att', 'airs_model', 'yfinance', 'fx',
      'benchmark', 'benchmark_etf', 'benchmark_caps', 'derived'] as const) {
      const label = sourceLabel(k);
      expect(label).toContain(sourceField(k));
      if (sourceVendor(k)) expect(label).toContain(sourceVendor(k));
      // ⚠ NO LEADING SPACE ON THE VENDORLESS ONE. `derived` has no third party to name, and a
      // label that starts with a space is the kind of defect nobody reports and everybody sees.
      expect(label).toBe(label.trim());
    }
  });
});
