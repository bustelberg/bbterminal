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
import { snapshotFreshness, type SnapshotTone } from './snapshotAge';

export type SourceKey =
  | 'airs_volk'      // AIRS Vermogensoverzicht — the book's own EUR position values
  | 'airs_att'       // AIRS Rendementen — the account's flow-aware return (cumulatief_rendement)
  | 'airs_model'     // AIRS Model-portefeuille scan — composition (ISIN, fund, weight, sector)
  | 'yfinance'       // our yfinance daily closes (asset_price)
  | 'fx'             // ECB/Yahoo FX rate (fx_rate)
  | 'benchmark'      // yfinance close, benchmark constituents
  | 'derived';       // computed from the above (no single source of its own)

const SOURCE: Record<SourceKey, { label: string; vendor: string }> = {
  airs_volk: { label: 'AIRS Vermogensoverzicht (VOLK)', vendor: 'AIRS scan' },
  airs_att: { label: 'AIRS Rendementen (ATT)', vendor: 'AIRS scan' },
  airs_model: { label: 'AIRS Model-portefeuille', vendor: 'AIRS scan' },
  yfinance: { label: 'yfinance daily close', vendor: 'Yahoo' },
  fx: { label: 'ECB / Yahoo FX rate', vendor: 'ECB' },
  benchmark: { label: 'yfinance close (benchmark)', vendor: 'Yahoo' },
  derived: { label: 'Computed on our side', vendor: '' },
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

/** A short label column ("WHEN", "HOW") beside its value — the card's two-column grid. */
function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-baseline gap-2">
      <span className="text-[9px] uppercase tracking-wider text-fg-faint w-9 shrink-0 pt-px">{label}</span>
      <span className="min-w-0 text-[11px]">{children}</span>
    </div>
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

/** The designed popover body: Source (WHERE + field) · When · How (copied vs formula). */
function ProvenanceCard({ source, asOf, note, how, kind }: {
  source: SourceKey; asOf?: string | null; note?: string; how?: string; kind?: ProvKind;
}) {
  const s = SOURCE[source];
  const f = asOf ? snapshotFreshness(asOf) : null;
  return (
    <div className="space-y-2 min-w-[13rem]">
      <div>
        <div className="flex items-center gap-1.5 mb-0.5">
          <span className="text-[9px] uppercase tracking-wider text-fg-faint">Source</span>
          {s.vendor && <span className="text-[9px] text-fg-faint">· {s.vendor}</span>}
        </div>
        <div className="text-fg-strong font-semibold leading-tight">{s.label}</div>
        {note && <div className="text-fg-muted text-[11px] mt-0.5">{note}</div>}
      </div>
      <div className="border-t border-neutral-800/40 pt-2 space-y-1.5">
        <Field label="When">
          {asOf
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
                  ? <>A formula on the data{how ? <>: <span className="text-fg">{how}</span></> : <> we compute here</>}.</>
                  : how}
            </span>
          </Field>
        )}
      </div>
    </div>
  );
}

/** A small ⓘ badge beside a number; hover for the WHERE / WHEN / HOW card. The badge is a subtle
 *  accent chip normally and an amber "!" chip when the source is stale (≥2 trading days behind), so
 *  a stale number reads as stale at a glance — no bare glyphs.
 *
 *  `note` — the specific field/line at the source ("cumulatief_rendement", "Beginwaarde").
 *  `kind` — 'copied' (read from the source, unchanged) or 'formula' (computed here); the only two
 *           ways a number arrives. `how` carries the formula when kind is 'formula'. */
export function Provenance({ source, asOf, note, how, kind }: {
  source: SourceKey; asOf?: string | null; note?: string; how?: string; kind?: ProvKind;
}) {
  const stale = asOf ? snapshotFreshness(asOf)?.tone === 'stale' : false;
  return (
    <InfoTip content={<ProvenanceCard source={source} asOf={asOf} note={note} how={how} kind={kind} />}>
      <span
        className={`ml-1 inline-flex items-center justify-center w-3.5 h-3.5 rounded-full align-middle
          text-[9px] font-semibold leading-none cursor-help transition-colors ${
          stale
            ? 'bg-warn-500/20 text-warn-600 hover:bg-warn-500/30'
            : 'bg-accent-500/10 text-accent-500 hover:bg-accent-500/20'}`}
        aria-label="data source and formula"
      >
        {stale ? '!' : 'i'}
      </span>
    </InfoTip>
  );
}
