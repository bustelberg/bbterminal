/**
 * Per-value data provenance: a small ⓘ badge whose popover FULLY explains one number —
 * WHERE it came from (source + field), WHEN (as-of + a freshness pill), and HOW it was derived.
 *
 * /portfolios blends several sources at different as-of times — AIRS's own scans (model
 * composition, VOLK book values, ATT returns) and our yfinance/FX reconstruction — and the same
 * displayed number can come from either world. A reader cannot tell a 4-day-stale AIRS value from a
 * live one, an AIRS return from a yfinance one, or a price return from a flow-aware one, by looking
 * at the digits. This attaches that whole answer to the value.
 *
 * The badge turns amber when the source is genuinely stale (≥2 trading days), and the popover's
 * pill states the freshness. Rendered via {@link InfoTip} so it appears instantly (the native
 * `title=` sits for ~1-2s) and can't be clipped by an overflow ancestor.
 */
import { createContext, useContext } from 'react';
import InfoTip from '../app/components/InfoTip';
import { INFO_ICON, INFO_ICON_WARN } from './infoIcon';
import { Field, Legend, TipCard, Worked, type FormulaSymbol } from './tipCard';
import { ValueBadge } from './dynamicValue';
import { BADGE_NEUTRAL, BADGE_PILL, BADGE_WARN } from './badgeChrome';
import { trimStop } from './provenanceText';
import { businessDaysBehind, fetchedToday, lagOwner, snapshotFreshness } from './snapshotAge';

export type SourceKey =
  | 'airs_volk'      // AIRS Vermogensoverzicht — the book's own EUR position values
  | 'airs_att'       // AIRS Rendementen — the account's flow-aware return (cumulatief_rendement)
  | 'airs_model'     // AIRS Model-portefeuille scan — composition (ISIN, fund, weight, sector)
  | 'yfinance'       // our yfinance daily closes (asset_price)
  | 'fx'             // ECB/Yahoo FX rate (fx_rate)
  | 'benchmark'      // yfinance close, benchmark constituents (the reconstructed index)
  | 'benchmark_etf'  // GuruFocus close for the index ETF itself (ACWI, SPY)
  | 'benchmark_caps' // yfinance market cap per constituent — the index's WEIGHTS, not its prices
  | 'derived';       // computed from the above (no single source of its own)

/**
 * ⚠ THE LABEL IS THE WHOLE ANSWER — there is no second "vendor" line, and there must not be one
 * again. Every source used to carry one, and in all seven cases it restated the label it sat
 * next to: "AIRS Vermogensoverzicht (VOLK) · AIRS scan", "yfinance daily close · Yahoo",
 * "ECB / Yahoo FX rate · ECB". It read as a second, more precise fact and was not one, so the
 * card spent a line teaching the reader nothing. Name the source once, in terms someone can go
 * and look up.
 */
/**
 * ⚠⚠ STORED AS PARTS, COMPOSED INTO THE LABEL — never the other way round. Prose sometimes needs
 * the halves separately ("weighted by market cap from yfinance"), and the tempting shortcut is to
 * type those two words at the call site beside a registry that already knows them. That is the
 * drift this table exists to stop: the day `benchmark_caps` becomes something other than yfinance,
 * the label changes here and the sentence keeps saying yfinance. So the parts are the source of
 * truth and {@link sourceLabel} builds the display string from them.
 *
 * ⚠ THIS IS NOT THE "SECOND VENDOR LINE" THE ⚠ ABOVE FORBIDS. That one printed the vendor AGAIN
 * next to a label already naming it — the same fact twice. These are the label's own components,
 * exposed so a sentence can use one of them instead of the whole thing.
 */
const SOURCE: Record<SourceKey, { vendor: string; field: string; qualifier?: string }> = {
  airs_volk: { vendor: 'AIRS', field: 'Vermogensoverzicht', qualifier: 'VOLK' },
  airs_att: { vendor: 'AIRS', field: 'Rendementen', qualifier: 'ATT' },
  airs_model: { vendor: 'AIRS', field: 'Model-portefeuille' },
  yfinance: { vendor: 'yfinance', field: 'daily close' },
  fx: { vendor: 'ECB / Yahoo', field: 'FX rate' },
  benchmark: { vendor: 'yfinance', field: 'close', qualifier: 'benchmark constituents' },
  // ⚠ A KEY OF ITS OWN, NOT `benchmark`. Since 2026-08-19 a benchmark figure can come from either
  // of two places — the index ETF's own price series (GuruFocus) or the constituent rebuild
  // (yfinance) — and they differ by ~2.8pp on ACWI YTD. Reusing `benchmark` would print
  // "yfinance close" over a GuruFocus number, which is precisely the mislabel this whole component
  // exists to make impossible.
  benchmark_etf: { vendor: 'GuruFocus', field: 'daily close', qualifier: 'index ETF' },
  // ⚠ A KEY OF ITS OWN FOR THE SAME REASON `benchmark_etf` IS. A cap-weighted index has two
  // separate yfinance inputs — the daily CLOSES that give it a return, and the per-constituent
  // MARKET CAPS that give it its weights (`_benchmark_refresh._caps`, refreshed by its own job and
  // stamped with its own `market_cap_checked_at`). Active share reads the caps and never touches a
  // close, so labelling it "yfinance close" would name a series the figure does not use.
  benchmark_caps: { vendor: 'yfinance', field: 'market cap', qualifier: 'benchmark constituents' },
  // ⚠ THE ONE SOURCE WITH NO VENDOR, because there is no third party to name — the composer drops
  // the empty half rather than printing a leading space.
  derived: { vendor: '', field: 'Computed on our side' },
};

/**
 * The canonical name of one source, for prose outside a provenance badge.
 *
 * ⚠ SO A CARD THAT NAMES A SOURCE IN A SENTENCE USES THE SAME WORDS AS THE BADGE BESIDE IT. The
 * alternative is a hand-typed "AIRS" or "Yahoo" in copy, which drifts the moment a label here is
 * made more precise — and the whole point of this table is that a source is named once.
 */
export function sourceLabel(key: SourceKey): string {
  const s = SOURCE[key];
  const head = [s.vendor, s.field].filter(Boolean).join(' ');
  return s.qualifier ? `${head} (${s.qualifier})` : head;
}

/**
 * WHO supplies it — `yfinance`, `AIRS`, `GuruFocus`.
 *
 * ⚠ FOR A SENTENCE THAT NEEDS THE HALVES APART: "weighted by market cap from yfinance" badges two
 * different facts, and squeezing the whole label into one badge makes "yfinance market cap
 * (benchmark constituents)" look like a single opaque token rather than a field and its vendor.
 * ⚠ Empty for `derived` — check before writing "from …" around it.
 */
export const sourceVendor = (key: SourceKey): string => SOURCE[key].vendor;

/**
 * WHAT is read — `market cap`, `daily close`, `Vermogensoverzicht`.
 *
 * ⚠ WITHOUT THE QUALIFIER. "(benchmark constituents)" disambiguates a label standing on its own;
 * inside a sentence that has already said "priced index members" it is the same fact twice.
 */
export const sourceField = (key: SourceKey): string => SOURCE[key].field;

/** The as-of freshness as a small coloured pill: green "current", neutral "1 trading day old",
 *  amber for genuinely stale. The one element in the card that carries colour on purpose. */
/**
 * THE ONE FRESHNESS VERDICT — is what we hold current, or is it not?
 *
 * ⚠⚠ IT IS ONE FUNCTION BECAUSE THE ICON AND THE CARD MUST NEVER DISAGREE. They were computed
 * apart: the icon from `lagOwner(asOf, fetchedAt)` and the card's pill from `snapshotFreshness
 * (asOf)` alone. So an account read yesterday, valued by AIRS three trading days ago, rendered a
 * BLUE icon over a card whose When pill said "⚠ 3 trading days old" in amber — two answers to one
 * question, one hover apart. Anything that colours a provenance signal reads this.
 *
 * ⚠⚠ CURRENT MEANS OUR COPY MATCHES THE SOURCE, AND THAT IS THE WHOLE DEFINITION. If we fetched
 * today and the source has published nothing since, the figure IS current: it is the entirety of
 * what is knowable. An earlier model called that case "old, but not ours to fix" and gave it a
 * third colour — a distinction no reader asked for. What date the source chose to stamp on its
 * valuation is a fact ABOUT THE SOURCE; the When line states it plainly and it is not a defect in
 * what we hold.
 *
 * ⚠ STALE WHEN WE CANNOT TELL. Most call sites pass no `fetchedAt`, and silence there would hide
 * the incident this signal exists for — a 4-day-old cached value reading €114,587 / +142% against
 * AIRS-live's €107,086 / +126%, with nothing on the page saying it came from a past scan.
 *
 * ⚠ A COLUMN HEADER IS NEVER STALE. It describes a column; staleness is a property of one
 * observation. The guard lives here rather than at ~90 call sites.
 */
export function provenanceFreshness(
  asOf: string | null | undefined, fetchedAt: string | null | undefined, column?: boolean,
): { stale: boolean; label: string } {
  if (column || !asOf) return { stale: false, label: '' };
  const f = snapshotFreshness(asOf);

  /**
   * ⚠⚠ NOT FETCHED TODAY IS OUTDATED (2026-08-19). Where we KNOW when we last read the source,
   * that is the whole verdict: today is current, anything older is amber.
   *
   * This tightens the old threshold (≥2 trading days) rather than replacing the principle. Amber
   * still means "you can fix this": a Refresh sets `fetched_at` to now and clears it, which is
   * exactly the property the 2026-08-17 incident was about — firing amber on AIRS's own valuation
   * lag produced 27 alarms of which 23 could not be cleared by anything on the page, and an alarm
   * you cannot clear is one readers learn to scroll past. Our own read age is always ours to fix,
   * so tightening it does not reintroduce that.
   *
   * ⚠ THE SOURCE'S OWN LAG STILL IS NOT A FAULT. Read today, the row is current even if AIRS last
   * valued the book a week ago — the card says so in `Whose lag`, and the pill keeps the date.
   */
  /**
   * ⚠⚠ WHERE WE KNOW WHEN WE READ IT, THAT IS THE WHOLE ANSWER — the source's own valuation age is
   * not reported at all (2026-08-19). It used to read "6 trading days old — the source's latest",
   * which answers a question nobody on this page is asking: what matters is whether OUR copy is
   * current, and the only thing that makes it current is having read it today.
   *
   * That also removes the last place the two lags could be confused. `stale` is now exactly
   * "not read today", and the pill says so in the same words.
   */
  if (fetchedAt) {
    if (fetchedToday(fetchedAt)) return { stale: false, label: 'read today' };
    const days = businessDaysBehind(fetchedAt.slice(0, 10));
    return { stale: true,
      label: days <= 0 ? 'not read today'
        : `read ${days} trading day${days === 1 ? '' : 's'} ago` };
  }

  // ⚠ UNCHANGED WHERE THE FETCH TIME IS UNKNOWN, and that is deliberate rather than an omission.
  // Most call sites pass no `fetchedAt`; treating "cannot tell" as "not today" would turn the
  // whole app amber at once, which is the furniture failure above in its purest form. Silence
  // there still falls back to the source date, exactly as before.
  if (f?.tone !== 'stale') return { stale: false, label: f?.label ?? '' };
  return { stale: true, label: f.label };
}

function FreshnessPill({ stale, label }: { stale: boolean; label: string }) {
  if (!label) return null;
  // ⚠ THE SAME TWO COLOURS THE ICON HAS, DRIVEN BY THE SAME BOOLEAN. A third tone here is how the
  // pill and the badge came to disagree in the first place.
  // ⚠ AND THE CHROME NOW COMES FROM `badgeChrome`, shared with the live-value badge — same tint,
  // border and radius, so the two objects in this card read as one convention.
  const cls = stale ? BADGE_WARN : `${BADGE_NEUTRAL} text-fg-muted`;
  return <span className={`${BADGE_PILL} ${cls}`}>{label}</span>;
}

/**
 * HOW a number came to exist — and there are only ever TWO honest answers:
 *   'copied'  — read straight out of the source above, unchanged, at the When date. Nothing was
 *               computed on our side; the digits are exactly what the vendor reported.
 *   'formula' — WE computed it here, a formula over data (whose inputs come from the Source above).
 *               `how` carries the formula itself (e.g. "Now ÷ Start − 1 = …").
 * Every value on the page is one or the other; the card states which so a reader never has to guess
 * whether a number was reported or derived. (`how` alone, with no kind, is the legacy free-text.)
 */
export type ProvKind = 'copied' | 'formula';

/**
 * The designed popover body: What · Source (WHERE) · When · How (copied vs formula).
 *
 * ⚠ `what` LEADS WHEN IT IS GIVEN, AND THAT IS THE POINT. Source/When/How all answer questions
 * ABOUT a number the reader has already identified. They are useless — worse, they look like an
 * answer — to someone who does not yet know what the number IS. So a card carrying `what` puts it
 * in the headline and demotes the source to a field beneath it; the reader learns what they are
 * looking at, then where it came from.
 *
 * Without `what` the card renders exactly as before (Source in the headline). That is not a fork:
 * it is the same shell with a different field promoted, and it keeps every call site that has not
 * been given a `what` yet rendering correctly rather than showing an empty heading.
 */
function ProvenanceCard({ source, asOf, fetchedAt, note, how, kind, column, what, fresh,
  worked, legend }: {
  source: SourceKey; asOf?: string | null; fetchedAt?: string | null;
  note?: string; how?: string; kind?: ProvKind;
  worked?: string; legend?: readonly FormulaSymbol[];
  column?: boolean; what?: string;
  /** The one verdict — see `provenanceFreshness`. Passed so the card cannot differ from the icon. */
  fresh: { stale: boolean; label: string };
}) {
  // ⚠ THE COMPOSED LABEL, not the parts — this card names the source once and in full, which is
  // what `sourceLabel` builds. The halves exist for prose that needs them apart; see `sourceField`.
  const s = { label: sourceLabel(source) };
  // ⚠ THE VERDICT IS PASSED IN, NOT RECOMPUTED. See `provenanceFreshness`: this card recomputing
  // it from `asOf` alone is precisely how it came to contradict the icon that opened it.
  const f = fresh;
  /**
   * ⚠⚠ WHOSE LAG IS IT. `asOf` is the day AIRS VALUED the book; `fetchedAt` is the moment we last
   * READ it. Only the second is ours to fix, and the card used to show only the first — so an old
   * valuation read as our staleness and the badge sent the reader to a Refresh button that cannot
   * publish a valuation AIRS has not made. Measured after a full refresh: 31 accounts re-scanned,
   * newest valuation available anywhere 2026-08-15, twenty of them still dated 2026-08-11/12.
   *
   * Silent when `fetchedAt` is absent — most Provenance call sites have no such fact, and inventing
   * a verdict for them would be worse than the omission this fixes.
   */
  // ⚠ SUPPRESSED WHEN WE READ IT TODAY. `lagOwner` still answers "source" there, and its text is
  // the "this is simply the newest valuation AIRS has" prose — which is exactly the source-lag
  // explanation this card no longer trades in. A row read today is current; there is nothing to
  // explain and nothing to act on.
  const lag = lagOwner(asOf, fetchedAt);
  const whoseLag = lag?.side === 'ours' ? lag : null;
  return (
    // The shared shell — identical chrome to every other tooltip; only the FIELDS differ.
    <TipCard label={what ? 'What' : 'Where'} title={what ?? s.label}
      subtitle={what ? undefined : note}>
      <>
        {what && (
          <Field label="Where">
            <span className="text-fg-soft">
              {s.label}
              {note && <span className="text-fg-muted"> — {note}</span>}
            </span>
          </Field>
        )}
        <Field label="When">
          {/* ⚠⚠ WHEN IS WHEN **WE** READ IT, wherever we know that. It used to be the source's
              valuation date with a freshness pill about the source — two different clocks in one
              row, and the one the reader can act on was the one not shown. `asOf` remains the
              answer only where no fetch time exists (yfinance closes, FX rates, computed values),
              which is most call sites and is unchanged. */}
          {column
            ? <span className="text-fg-muted">per value — each cell carries its own date</span>
            : fetchedAt
              ? (
                <span className="flex items-center gap-1.5 flex-wrap">
                  <ValueBadge>{fetchedAt.slice(0, 10)}</ValueBadge>
                  <FreshnessPill stale={f.stale} label={f.label} />
                </span>
              )
              : asOf
                ? (
                  <span className="flex items-center gap-1.5 flex-wrap">
                    <ValueBadge>{asOf}</ValueBadge>
                    <FreshnessPill stale={f.stale} label={f.label} />
                  </span>
                )
                : <span className="text-fg-muted">no dated source (a structural / computed value)</span>}
        </Field>
        {/* ⚠ ONLY WHEN THE BADGE IS AMBER AND WE KNOW BOTH DATES. A "we read this today" line under
            a fresh row is noise; under an amber one it is the difference between an action and a
            dead end. See `whoseLag`. */}
        {whoseLag && (
          <Field label="Whose lag">
            <span className="text-fg-soft leading-relaxed">{whoseLag.text}</span>
          </Field>
        )}
        {/* ⚠⚠ THE MATHS IS TYPESET HERE TOO, NOT ONLY IN `AspectCard` (2026-09-01). The house
            style says every ⓘ on this dashboard states its arithmetic through `worked` + `legend`
            — and half the dashboard's cards are PROVENANCE cards, which had no way to. So the
            files still on `tooltipStyle`'s `UNCONVERTED` ratchet were writing formulas as prose
            with Unicode operators (`Σ(wᵢ × rᵢ) ÷ Σwᵢ`), which is the exact thing that rule exists
            to forbid — they had no alternative. Same two components as `AspectCard`, so the two
            card families cannot drift into two ways of setting one formula.
            ⚠ THE THREE ARE ONE GROUP — prose, the maths it describes, and the key to that maths.
            Spaced as siblings the legend sits as far from its formula as from the sentence. */}
        {(kind || how || worked || legend?.length) && (
          <span className="block space-y-1.5">
          <Field label="How">
            <span className="text-fg-soft leading-relaxed">
              {kind === 'copied'
                /* ⚠⚠ IT MAY CARRY A CAVEAT, AND IT COULD NOT BEFORE. This branch ignored `how`
                   entirely, on the reasoning that a copied figure has no arithmetic to explain —
                   true, and it left nowhere to say the things a reader still has to know about a
                   number we did not compute: that AIRS's `cumulatief_rendement` is flow-aware and
                   includes income, that a deposit therefore does not flatter it. The only way to
                   surface that was to mis-tag the field as a `formula`, which is what the Return
                   tile did: "A formula on the data: AIRS's own cumulatief_rendement…" over a figure
                   read straight off the source. The tag now tells the truth and the caveat still
                   fits. */
                ? <>Copied straight from {s.label}{asOf ? ` (${asOf})` : ''}, as reported — not
                  computed here{how ? <>. <span className="text-fg whitespace-pre-wrap">{trimStop(how)}</span></> : null}.</>
                : kind === 'formula'
                  /* ⚠ `whitespace-pre-wrap` SO A FORMULA CAN BREATHE. The best formula cards state
                     the rule and then the same rule with this row's own numbers under it — and a
                     blank line between the two is what makes them read as one thing said twice
                     rather than as one long sentence. In a plain span every run of whitespace
                     collapses to one character, so the separation has to be permitted here; it
                     changes nothing for the single-line cards, which contain no runs to preserve. */
                  ? <>A formula on the data{how ? <>: <span className="text-fg whitespace-pre-wrap">{trimStop(how)}</span></> : <> we compute here</>}.</>
                  : how}
            </span>
          </Field>
          {worked ? <Worked text={worked} /> : null}
          {legend?.length ? <Legend items={legend} /> : null}
          </span>
        )}
      </>
    </TipCard>
  );
}

/** A small ⓘ badge beside a number; hover for the WHERE / WHEN / HOW card. The badge is a subtle
 *  accent chip normally and an amber "!" chip when the source is stale (≥2 trading days behind), so
 *  a stale number reads as stale at a glance — no bare glyphs.
 *
 *  `note` — the specific field/line at the source ("cumulatief_rendement", "Beginwaarde").
 *  `kind` — 'copied' (read from the source, unchanged) or 'formula' (computed here); the only two
 *           ways a number arrives. `how` carries the formula when kind is 'formula'.
 *
 *  ⚠ `column` — THIS ⓘ DESCRIBES A COLUMN, NOT A VALUE, AND A COLUMN CANNOT BE OUT OF DATE.
 *  A header is a label: it says where a column's numbers come from and how they are computed,
 *  both of which are true whatever date the data carries. Handing it an `asOf` made the amber
 *  "!" fire on the HEADING — measured on the attribution table, where `w_B` and `R_B` sat there
 *  wearing a staleness warning while the label itself was as current as it would ever be. Worse,
 *  it is the wrong alarm in the wrong place: staleness is a property of an observation, so the
 *  reader who needs it is the one reading a NUMBER, and every value carries its own chip that
 *  can still go amber. So `column` suppresses the warn state outright and the When field says
 *  where the date actually lives. It is not a styling opt-out — it is the statement that this
 *  icon has no observation behind it. */
/**
 * A `fetchedAt` for a whole subtree — WHEN WE LAST READ THE THING EVERY NUMBER UNDER IT DESCRIBES.
 *
 * ⚠⚠ A CONTEXT AND NOT A PROP, FOR ONE REASON: the expanded account panel renders **40** ⓘ icons
 * and every one of them carries the SAME account's `as_of`. Threading a second date through all of
 * them is forty chances to forget one — and a forgotten one is not a visible bug, it is a single
 * icon that stays amber while its neighbours went quiet, which reads as "this particular number is
 * stale" and is the most misleading outcome available.
 *
 * ⚠ IT MUST WRAP EXACTLY ONE SOURCE-OBJECT'S SUBTREE. The provider says "everything in here was
 * read at this moment"; wrapping two accounts, or a page, would hand one book's fetch time to
 * another's numbers and quietly de-amber a row that really is our lag. An explicit `fetchedAt` prop
 * always wins, so a nested exception stays possible.
 */
const FetchedAtContext = createContext<string | null | undefined>(undefined);

export function ProvenanceFetchedAt({ at, children }: {
  at?: string | null; children: React.ReactNode;
}) {
  return <FetchedAtContext.Provider value={at}>{children}</FetchedAtContext.Provider>;
}

export function Provenance({ source, asOf, fetchedAt, note, how, kind, column = false, what,
  worked, legend }: {
  source: SourceKey; asOf?: string | null; note?: string; how?: string; kind?: ProvKind;
  /** The formula, then the same formula with this row's numbers in it. See `Worked`. */
  worked?: string;
  /** What each symbol in `worked` stands for. */
  legend?: readonly FormulaSymbol[];
  /** When WE last read the source, if the caller knows it. Turns an amber badge from a dead end
   *  into an answer — see `whoseLag` in the card. Optional everywhere. */
  fetchedAt?: string | null;
  column?: boolean;
  /** WHAT this number is, in one plain sentence — "Your share of the model held in Industrials."
   *  Answered FIRST, because Source/When/How are all questions about a number the reader has
   *  already identified, and none of them helps someone who cannot tell what they are looking at. */
  what?: string;
}) {
  // ⚠ `!column &&` FIRST. A column header must never reach the stale branch, whatever it was
  // handed — the guard belongs here, not at ~90 call sites that each have to remember it.
  /**
   * ⚠⚠ AMBER MEANS "YOU CAN FIX THIS", NOT "THIS IS OLD" — and firing it on AIRS's own lag made it
   * furniture. Measured 2026-08-17, straight after a full "Refresh all": 27 of 45 account rows wore
   * the `!`, and 23 of them had been read that same afternoon — AIRS had simply not valued those
   * books since. Not one of the 23 could be cleared by any action available on the page, and an
   * alarm that cannot be cleared is one a reader learns to scroll past, which then costs them the
   * four rows that WERE ours to fix.
   *
   * So `source`-side lag demotes the icon to the ordinary `i`: the date and its age are still in the
   * card, one hover away, with `Whose lag` saying why refreshing cannot move it. Amber is reserved
   * for the lag we own — and for the case where we do NOT KNOW whose it is (`fetchedAt` absent, most
   * call sites), because silence there would hide the AMD incident this badge was built for: a
   * 4-day-old cached value read €114,587 / +142% against AIRS-live's €107,086 / +126%.
   */
  // ⚠ A DATE OF ITS OWN WINS; ANYTHING ELSE INHERITS THE SUBTREE'S — see `ProvenanceFetchedAt`.
  // `??` treats an explicit `null` exactly like an absent prop, and that is deliberate: `null` here
  // means "this call site does not know when we read it", which is precisely when the provider's
  // answer — about the same account — is the better one.
  const ctxFetchedAt = useContext(FetchedAtContext);
  const fetched = fetchedAt ?? ctxFetchedAt;
  // ⚠⚠ COMPUTED ONCE AND HANDED TO BOTH — the icon below and the card's When pill. Two
  // computations of "is this current" in one component is what produced a blue ⓘ over an amber
  // "3 trading days old".
  const fresh = provenanceFreshness(asOf, fetched, column);
  return (
    <InfoTip content={<ProvenanceCard source={source} asOf={asOf} fetchedAt={fetched} note={note}
      worked={worked} legend={legend}
      how={how} kind={kind} column={column} what={what} fresh={fresh} />}>
      <span
        className={`ml-1 ${fresh.stale ? INFO_ICON_WARN : INFO_ICON}`}
        // ⚠ THE STATE IS IN THE LABEL, NOT ONLY IN THE HUE. A colour is the whole signal for a
        // sighted reader and none of it for anyone else.
        aria-label={`data source and formula${fresh.stale ? ' — not current; refresh to update' : ''}`}
      >
        {fresh.stale ? '!' : 'i'}
      </span>
    </InfoTip>
  );
}
