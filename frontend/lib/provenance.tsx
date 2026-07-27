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
import InfoTip from '../app/components/InfoTip';
import { INFO_ICON, INFO_ICON_WARN } from './infoIcon';
import { Field, TipCard } from './tipCard';
import { trimStop } from './provenanceText';
import { snapshotFreshness, type SnapshotTone } from './snapshotAge';

export type SourceKey =
  | 'airs_volk'      // AIRS Vermogensoverzicht — the book's own EUR position values
  | 'airs_att'       // AIRS Rendementen — the account's flow-aware return (cumulatief_rendement)
  | 'airs_model'     // AIRS Model-portefeuille scan — composition (ISIN, fund, weight, sector)
  | 'yfinance'       // our yfinance daily closes (asset_price)
  | 'fx'             // ECB/Yahoo FX rate (fx_rate)
  | 'benchmark'      // yfinance close, benchmark constituents
  | 'derived';       // computed from the above (no single source of its own)

/**
 * ⚠ THE LABEL IS THE WHOLE ANSWER — there is no second "vendor" line, and there must not be one
 * again. Every source used to carry one, and in all seven cases it restated the label it sat
 * next to: "AIRS Vermogensoverzicht (VOLK) · AIRS scan", "yfinance daily close · Yahoo",
 * "ECB / Yahoo FX rate · ECB". It read as a second, more precise fact and was not one, so the
 * card spent a line teaching the reader nothing. Name the source once, in terms someone can go
 * and look up.
 */
const SOURCE: Record<SourceKey, { label: string }> = {
  airs_volk: { label: 'AIRS Vermogensoverzicht (VOLK)' },
  airs_att: { label: 'AIRS Rendementen (ATT)' },
  airs_model: { label: 'AIRS Model-portefeuille' },
  yfinance: { label: 'yfinance daily close' },
  fx: { label: 'ECB / Yahoo FX rate' },
  benchmark: { label: 'yfinance close (benchmark)' },
  derived: { label: 'Computed on our side' },
};

/** The as-of freshness as a small coloured pill: green "current", neutral "1 trading day old",
 *  amber for genuinely stale. The one element in the card that carries colour on purpose. */
function FreshnessPill({ tone, label }: { tone: SnapshotTone; label: string }) {
  const cls =
    tone === 'stale' ? 'bg-warn-500/15 text-warn-600 border-warn-500/40'
      : tone === 'ok' ? 'bg-neutral-500/10 text-fg-muted border-neutral-700/40'
        : 'bg-pos-500/15 text-pos-600 border-pos-500/40';
  return (
    <span className={`px-1.5 py-px rounded-full text-[9px] font-medium border whitespace-nowrap ${cls}`}>
      {tone === 'fresh' ? 'current' : label}
    </span>
  );
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
function ProvenanceCard({ source, asOf, note, how, kind, column, what }: {
  source: SourceKey; asOf?: string | null; note?: string; how?: string; kind?: ProvKind;
  column?: boolean; what?: string;
}) {
  const s = SOURCE[source];
  const f = asOf ? snapshotFreshness(asOf) : null;
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
          {column
            ? <span className="text-fg-muted">per value — each cell carries its own date</span>
            : asOf
              ? (
                <span className="flex items-center gap-1.5 flex-wrap">
                  <span className="font-mono text-fg">{asOf}</span>
                  {f && <FreshnessPill tone={f.tone} label={f.label} />}
                </span>
              )
              : <span className="text-fg-muted">no dated source (a structural / computed value)</span>}
        </Field>
        {(kind || how) && (
          <Field label="How">
            <span className="text-fg-soft leading-relaxed">
              {kind === 'copied'
                ? <>Copied straight from {s.label}{asOf ? ` (${asOf})` : ''}, as reported — not computed here.</>
                : kind === 'formula'
                  ? <>A formula on the data{how ? <>: <span className="text-fg">{trimStop(how)}</span></> : <> we compute here</>}.</>
                  : how}
            </span>
          </Field>
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
export function Provenance({ source, asOf, note, how, kind, column = false, what }: {
  source: SourceKey; asOf?: string | null; note?: string; how?: string; kind?: ProvKind;
  column?: boolean;
  /** WHAT this number is, in one plain sentence — "Your share of the model held in Industrials."
   *  Answered FIRST, because Source/When/How are all questions about a number the reader has
   *  already identified, and none of them helps someone who cannot tell what they are looking at. */
  what?: string;
}) {
  // ⚠ `!column &&` FIRST. A column header must never reach the stale branch, whatever it was
  // handed — the guard belongs here, not at ~90 call sites that each have to remember it.
  const stale = !column && asOf ? snapshotFreshness(asOf)?.tone === 'stale' : false;
  return (
    <InfoTip content={<ProvenanceCard source={source} asOf={asOf} note={note} how={how}
      kind={kind} column={column} what={what} />}>
      <span
        className={`ml-1 ${stale ? INFO_ICON_WARN : INFO_ICON}`}
        aria-label="data source and formula"
      >
        {stale ? '!' : 'i'}
      </span>
    </InfoTip>
  );
}
