'use client';

import { Provenance } from '../../../lib/provenance';
import type { ModelPortfolioAnalysis } from '../../../lib/types/api';

type Realised = NonNullable<ModelPortfolioAnalysis['realised']>;

/**
 * WHAT THE BOOK SOLD THIS YEAR — the half of the year the Holdings table above cannot show.
 *
 * ⚠ EVERY FIGURE ABOVE THIS BLOCK IS BUILT FROM POSITIONS STILL HELD. A name sold in March has no
 * row, so its result is invisible: measured on BUS_Offensief_Dyn, EUR -28,656 — **22.5% of the
 * year's movement** — and on a book whose biggest loser was sold, the surviving rows describe a
 * year that did not happen.
 *
 * ⚠ ONE DENOMINATOR, AND IT IS THE BOOK'S OWN OPENING CAPITAL. The Holdings table weights each
 * position by its share of the PRICED HELD book — right for a class return, and unable to carry a
 * sold position at all, because that position is not in the denominator. On `beginvermogen` the
 * three legs add to the book's YTD exactly (measured: 5.826708% against AIRS's 5.826704%).
 *
 * ⚠⚠ THERE IS NO WEIGHT COLUMN HERE, AND ITS ABSENCE IS THE STATEMENT. A sold parcel's opening
 * value is NOT recoverable: `proceeds − Res. YtD` yields its cost basis, which for a parcel bought
 * in February is capital that did not exist on 1 January — feeding it in made the opening-capital
 * gap worse (EUR 55,427 → EUR 377,776), and partial sells make it unrecoverable in principle since
 * AIRS restates `Beginwaarde` to the CURRENT quantity. A contribution needs no weight; an
 * allocation effect does. That is exactly why these legs are absent from the Sector / Region /
 * Currency bars and from Brinson, and why those views instead report how much of the year they
 * cannot see.
 */
export default function RealisedPanel({ r, asOf }: { r: Realised; asOf?: string | null }) {
  // ⚠ ABSENT IS NOT EMPTY. No pairing, no cached transactions, or a sheet we could not read — each
  // has its own reason, and an empty list presented as an answer would say "sold nothing", which
  // on the book this was measured against would hide EUR 28,656 of realised loss.
  if (!r.available) {
    return (
      <div className="bg-card border border-neutral-800/40 rounded-xl px-4 py-3">
        <h4 className="text-xs font-medium text-fg-strong">Sold this year</h4>
        <p className="text-[11px] text-warn-500 mt-1">{r.note}</p>
      </div>
    );
  }

  const legs = r.legs ?? [];
  return (
    <div className="bg-card border border-neutral-800/40 rounded-xl overflow-hidden">
      <div className="flex items-center justify-between gap-2 px-4 py-2.5 border-b border-neutral-800/40">
        <h4 className="text-xs font-medium text-fg-strong">
          Sold this year
          <Provenance source="airs_volk" asOf={asOf} kind="formula" column
            what={'What this book realised on sales this year — the part of the year the Holdings '
              + 'table above cannot show, because a sold position has no row left.'}
            how={'AIRS’s own Res. YtD per sale, from the Transacties report, aggregated per '
              + 'instrument and divided by the book’s opening capital. ⚠ Res. YtD, never '
              + 'proceeds − cost: the two differ by whatever part of a gain was made in an earlier '
              + 'year, and on this book that difference is 8 percentage points and a sign.'} />
        </h4>
        <span className="text-[10px] font-mono text-fg-faint">
          {legs.length} instrument{legs.length === 1 ? '' : 's'}
          {' · '}{legs.filter((l) => l.closed_out).length} closed out
        </span>
      </div>

      {/* The three legs on ONE denominator, adding to the book's own year. */}
      <div className="px-4 py-2 border-b border-neutral-800/40 grid gap-1 text-[11px]">
        <Leg label="Positions still held" pct={r.held_pct} eur={r.held_eur} />
        <Leg label="Realised on sales" pct={r.realised_pct} eur={r.realised_eur} />
        <Leg label="Income from names no longer held" pct={r.sold_income_pct} eur={r.sold_income_eur} />
        <div className="flex items-center gap-2 pt-1 mt-0.5 border-t border-neutral-800/40 font-semibold">
          <span className="text-fg-strong">The book’s year</span>
          <span className="ml-auto font-mono tabular-nums">
            <span className={tone(r.total_pct)}>{pct(r.total_pct)}</span>
            <span className="ml-2 text-fg-faint font-normal">
              AIRS: <span className={tone(r.book_ytd_pct)}>{pct(r.book_ytd_pct)}</span>
            </span>
          </span>
        </div>
        {/* ⚠ THE CHECK IS SHOWN EVERY TIME, INCLUDING WHEN IT PASSES. And `reconciles == null` is
            UNKNOWN, not failed: the holdings snapshot and the ATT report are separate downloads
            and land a day apart, which on a €1.4m book is tens of thousands of euros of market
            movement — accusing the arithmetic there would send a reader hunting a position that
            is not missing. */}
        <div className="text-[10px] pt-0.5">
          {r.reconciles === true ? (
            <span className="text-pos-400">
              ✓ reconciles with AIRS’s own result to €{Math.abs(r.residual_eur ?? 0).toFixed(2)}
            </span>
          ) : (
            <span className={r.reconciles === false ? 'text-neg-400' : 'text-warn-500'}>
              {r.reconciles === false ? '⚠' : 'ⓘ'} {eur(r.residual_eur)} apart
              {r.residual_reason ? ` — ${r.residual_reason}` : ''}
            </span>
          )}
        </div>
        {/* ⚠⚠ On a book with deposits or withdrawals the percentages are withheld entirely: a
            result over an opening capital is not a return there, and three contributions that do
            not add to the figure they claim to decompose each look reasonable alone. */}
        {r.comparable === false && (
          <p className="text-[10px] text-warn-500">
            Money was paid into or out of this book this year, so a result over its opening capital
            is not a return — the euro amounts stand, the percentages are withheld, and AIRS’s own
            flow-aware figure is the one to read.
          </p>
        )}
      </div>

      {!!legs.length && (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="text-[10px] uppercase tracking-wide text-fg-faint">
              <tr className="border-b border-neutral-800/40">
                <th className="text-left pl-4 py-2 font-medium">Sold</th>
                <th className="text-right py-2 font-medium w-32">Realised (€)</th>
                <th className="text-right pr-4 py-2 font-medium w-32">
                  Contribution
                  <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                    what="What this sale added to, or took off, the book’s year."
                    how={'Its realised result divided by the book’s opening capital — the SAME '
                      + 'denominator the two legs above use, so all of them add to the book’s YTD. '
                      + '⚠ It is not a weight, and there is deliberately no weight column: a sold '
                      + 'position’s share of the book when the year opened cannot be recovered '
                      + 'from AIRS’s data, which is also why these names are absent from the '
                      + 'Sector / Region / Currency bars and from the attribution table.'} />
                </th>
              </tr>
            </thead>
            <tbody>
              {legs.map((l) => (
                <tr key={l.fonds ?? ''} className="border-b border-neutral-800/[0.15] last:border-0 hover:bg-overlay/[0.03] transition-colors">
                  <td className="py-1.5 pl-4 text-fg">
                    {l.fonds ?? '—'}
                    {/* ⚠ A SALE IS A REALISATION, NOT A CLOSURE — most of these are still held,
                        trimmed. Only absence from the Holdings table means closed. */}
                    {l.closed_out && (
                      <span className="ml-2 px-1.5 py-0.5 rounded-md bg-overlay/5 text-[9px] text-fg-muted"
                        title="No longer in the Holdings table above — this position was closed out.">
                        closed
                      </span>
                    )}
                    {!!l.prior_year_eur && (
                      <span className="ml-2 text-[9px] text-warn-500"
                        title={`${eur(l.prior_year_eur)} of this result was earned in earlier years and is correctly NOT in this year's figure. Using proceeds − cost instead of AIRS's Res. YtD would wrongly include it.`}>
                        {eur(l.prior_year_eur)} prior yr
                      </span>
                    )}
                    {l.first && (
                      <span className="ml-2 text-[9px] text-fg-faint">
                        {l.first === l.last ? l.first : `${l.first} → ${l.last}`}
                      </span>
                    )}
                  </td>
                  <td className={`py-1.5 text-right font-mono tabular-nums ${tone(l.realised_ytd_eur)}`}>
                    {eur(l.realised_ytd_eur)}
                  </td>
                  <td className={`py-1.5 pr-4 text-right font-mono tabular-nums ${tone(l.contribution_pct)}`}>
                    {ppt(l.contribution_pct)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function Leg({ label, pct: p, eur: e }: { label: string; pct?: number | null; eur?: number | null }) {
  return (
    <div className="flex items-center gap-2">
      <span className="text-fg-soft">{label}</span>
      <span className="ml-auto font-mono tabular-nums text-fg-muted">{eur(e)}</span>
      <span className={`w-20 text-right font-mono tabular-nums ${tone(p)}`}>{ppt(p)}</span>
    </div>
  );
}

/** ⚠ A dash, never a 0 — "we could not compute this" and "it came to nothing" are different. */
const eur = (v?: number | null) =>
  (v == null ? '—' : `€${v.toLocaleString('en-US', { maximumFractionDigits: 2, minimumFractionDigits: 2 })}`);
const pct = (v?: number | null) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`);
/** ⚠ pp, not %. It is a share OF the book's return; printing "+0.70%" beside the book's "+5.83%"
 *  reads as a second, rival return. */
const ppt = (v?: number | null) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}pp`);
const tone = (v?: number | null) =>
  (v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400');
