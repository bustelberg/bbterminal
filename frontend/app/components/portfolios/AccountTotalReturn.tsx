'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import type { AirsAccountReconciliation } from '../../../lib/types/api';
import { useMgmtCopy } from '../management/managementCopy';

/**
 * THE YEAR, BUILT FROM THE POSITIONS — held AND sold — and set against the book's own figure.
 *
 * ⚠ THE TWO NUMBERS ALREADY ON THIS SCREEN DISAGREE, AND NOTHING SAID WHY. The positions table
 * reports a start-weighted return over what the book still HOLDS; the account row reports AIRS's
 * own `cumulatief_rendement`. Measured 2026-08-05 across 39 accounts, **23 disagree by more than
 * 1pp** — AITopSelectie +37.84% against +38.73%, BUS_FTS_BEPOFF_DYN by +3.27pp. Both are correct
 * answers to different questions, which is precisely the pair a reader cannot arbitrate.
 *
 * The missing piece is what was SOLD. Adding it closes the year exactly:
 *
 *     held 380,986.94  +  realised 6,306.85  +  income from names no longer held 0.00
 *       =  387,293.79   against the book's own 387,293.75   ->  residual EUR 0.04
 *
 * and 387,293.79 / 1,000,000 = 38.7294%, against AIRS's own 38.729375%.
 *
 * ⚠ THE RESIDUAL IS THE PRODUCT, NOT A FOOTNOTE. A total assembled from three legs and never set
 * against the book's own is an assertion; set against it, it is a reconciliation. It is shown
 * every time, including when it is four cents.
 *
 * ⚠ EUROS ADD, PERCENTAGES DO NOT. Every line of the waterfall is a euro amount measured on the
 * same basis; the two percentages are shown at the top and never subtracted into each other.
 * `gap_pp` is in POINTS for the same reason.
 */
export default function AccountTotalReturn({ portefeuille }: { portefeuille: string }) {
  // ⚠ THE COPY MODULE, NOT LITERALS — see `managementCopy`. A missing Dutch string is a
  // compile error there, which is what keeps this panel from rendering half-translated.
  const t = useMgmtCopy().accountReturn;
  const [open, setOpen] = useState(false);
  const [d, setD] = useState<AirsAccountReconciliation | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setErr(null);
    try {
      const r = await apiFetch(`${API_URL}/api/airs/accounts/`
        + `${encodeURIComponent(portefeuille)}/return-reconciliation`);
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      const j: AirsAccountReconciliation = await r.json();
      console.warn(`[AIRS total return] ${portefeuille}`, j);
      setD(j);
    } catch (e) {
      console.warn(`[AIRS total return] ${portefeuille} failed`, e);
      setErr('Could not load the reconciliation — see the console for the full error.');
    } finally {
      setLoading(false);
    }
  }, [portefeuille]);

  // ⚠ NULL IS NOT ZERO. No Transacties sheet cached means the realised leg is UNKNOWN, so there is
  // no total to show — and showing the held-only figure as "the total" would understate the year
  // by exactly the amount nobody had looked up.
  const needsTx = !!d && d.realised_ytd_eur == null && !d.realised_note;

  // ⚠ AN INCOMPLETE ANSWER IS RE-FETCHED ON RE-OPEN, AND WITHOUT THIS THE PANEL CONTRADICTED
  // ITSELF. It told the reader to load the Transactions above "then re-open this" — and re-opening
  // did nothing, because the first answer was already in state and the effect only fired when
  // there was none. Measured on Bustelberg Offensief: the transactions were sitting in the cache,
  // fully readable (+3.94%, reconciling to EUR 0.05), while this panel kept insisting they had not
  // been fetched. An instruction that does not work is worse than no instruction.
  //
  // ⚠ ONLY ON THE OPEN TRANSITION, or this loops: `load` replaces `d`, which re-runs the effect,
  // which would find it still incomplete and fetch again for ever. `wasOpen` makes the re-fetch a
  // one-shot per open. And only when INCOMPLETE — a finished reconciliation is not re-fetched just
  // because the reader collapsed and expanded it.
  const wasOpen = useRef(false);
  useEffect(() => {
    const justOpened = open && !wasOpen.current;
    wasOpen.current = open;
    if (!open || loading) return;
    if (!d && !err) { void load(); return; }
    if (justOpened && (needsTx || err)) void load();
  }, [open, d, loading, err, needsTx, load]);

  return (
    <div className="space-y-2">
      <button type="button" onClick={() => setOpen((v) => !v)} aria-expanded={open}
        className="w-full flex items-center gap-2 text-left text-[12px] px-2 py-1.5 rounded-lg border border-neutral-800/40 bg-card hover:bg-overlay/5 transition-colors">
        <span className={`text-[9px] text-fg-faint transition-transform ${open ? 'rotate-90' : ''}`}>▶</span>
        <span className="font-medium text-fg-strong">{t.title}</span>
        <span className="text-fg-faint">
          {loading ? 'loading…'
            : err && !d ? <span className="text-neg-400">{t.couldNotLoad}</span>
              : !d ? t.heldPlusSold
                : needsTx ? t.loadTxFirst
                  : t.soldHeld(d.realised_names ?? 0, d.open_priced ?? 0)}
        </span>
        {d && (
          <span className="ml-auto flex items-center gap-3 font-mono">
            {d.total_return_pct != null && (
              <span className={tone(d.total_return_pct)}>{pct(d.total_return_pct)}</span>
            )}
            {/* The verdict, in one glyph. A reconciliation whose result you have to compute
                yourself is not one. */}
            {d.reconciles != null && (
              <span className={d.reconciles ? 'text-pos-400' : 'text-warn-500'}
                title={d.reconciles
                  ? `Reconciles with AIRS's own figure to €${Math.abs(d.residual_vs_book_eur ?? 0).toFixed(2)}.`
                  : `€${(d.residual_vs_book_eur ?? 0).toFixed(2)} of the book's result is not explained by its positions.`}>
                {d.reconciles ? '✓' : '⚠'}
              </span>
            )}
          </span>
        )}
      </button>

      {open && (
        <div className="space-y-2">
          {loading && !d && <p className="text-[12px] text-fg-subtle px-1">Loading…</p>}
          {err && <p className="text-[12px] text-neg-400 px-1">{err}</p>}
          {needsTx && (
            <div className="flex items-start gap-2 px-1">
              <p className="text-[12px] text-warn-500">
                {t.needsTx}
                <strong> {t.openTransactions}</strong>{' '}{t.needsTxTail}
              </p>
              {/* ⚠ A CONTROL, NOT JUST AN INSTRUCTION. Re-opening now re-fetches too, but a reader
                  who has just loaded the transactions in the panel above should not have to
                  discover that by collapsing this one. */}
              <button type="button" disabled={loading} onClick={() => void load()}
                className="shrink-0 px-2 py-1 rounded-md border border-neutral-800/40 text-[11px] text-fg-subtle hover:bg-overlay/5 disabled:opacity-50 transition-colors">
                {loading ? t.reloading : t.reload}
              </button>
            </div>
          )}
          {d?.realised_note && <p className="text-[12px] text-warn-500 px-1">{d.realised_note}</p>}

          {d && !needsTx && (
            <div className="rounded-lg border border-neutral-800/40 overflow-hidden">
              <table className="w-full text-xs">
                <tbody className="divide-y divide-neutral-800/20">
                  <Row label={t.rowHeld} sub={t.subPriced(d.open_priced ?? 0, d.open_unpriced ?? 0)}
                    eur={d.open_result_eur} />
                  <Row label={t.rowRealised} sub={t.subRealised(d.realised_names ?? 0)}
                    eur={d.realised_ytd_eur} />
                  <Row label={t.rowIncomeSold}
                    sub={d.sold_funds?.length ? d.sold_funds.join(', ') : t.subNone}
                    eur={d.sold_income_eur} />
                  <tr className="bg-overlay/[0.04] font-semibold">
                    <td className="px-3 py-2 text-fg-strong">
                      {t.totalResult}
                      <span className="ml-2 font-normal text-[11px] text-fg-faint">
                        {t.totalResultSub}
                      </span>
                    </td>
                    <td className={`px-3 py-2 text-right font-mono tabular-nums ${tone(d.total_result_eur)}`}>
                      {eur(d.total_result_eur)}
                    </td>
                  </tr>
                  <tr className="border-t border-neutral-800/40">
                    <td className="px-3 py-2 text-fg-soft">
                      {t.airsResult}
                      <span className="ml-2 text-[11px] text-fg-faint">
                        {t.airsResultSub}
                      </span>
                    </td>
                    <td className={`px-3 py-2 text-right font-mono tabular-nums ${tone(d.book_result_eur)}`}>
                      {eur(d.book_result_eur)}
                    </td>
                  </tr>
                  {/* ⚠ ALWAYS SHOWN, even at four cents. The check is the product. */}
                  <tr>
                    <td className="px-3 py-2 text-fg-muted">
                      Residual
                      <span className="ml-2 text-[11px] text-fg-faint">
                        {d.reconciles ? 'rounding' : 'NOT explained by the positions'}
                      </span>
                    </td>
                    <td className={`px-3 py-2 text-right font-mono tabular-nums ${d.reconciles ? 'text-fg-faint' : 'text-warn-500'}`}>
                      {eur(d.residual_vs_book_eur)}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          )}

          {d && !needsTx && (
            <div className="grid gap-2 sm:grid-cols-2 text-[12px]">
              <Stat label={t.totalYtd} value={d.total_return_pct != null ? pct(d.total_return_pct) : '—'}
                tone={d.total_return_pct != null ? tone(d.total_return_pct) : 'text-fg-faint'}
                note={d.return_basis === 'opening_capital'
                  ? `total result over the year's opening capital ${eur(d.book_start_eur)}`
                  /* ⚠⚠ A RESULT OVER AN OPENING CAPITAL IS ONLY A RETURN WHEN NOTHING WAS PAID IN
                     OR OUT. Refused rather than fudged — AIRS's own figure is flow-aware. */
                  : d.return_basis === 'flows'
                    ? `refused: ${eur(d.deposits_eur)} in / ${eur(d.withdrawals_eur)} out this year — read AIRS’s flow-aware figure instead`
                    : 'not available'} />
              <Stat label={t.airsYtd} value={d.book_return_pct != null ? pct(d.book_return_pct) : '—'}
                tone={d.book_return_pct != null ? tone(d.book_return_pct) : 'text-fg-faint'}
                note={`cumulatief_rendement, flow-aware${d.months ? `, ${d.months} month${d.months === 1 ? '' : 's'}` : ''}`} />
            </div>
          )}

          {/* ⚠ NAMED, NOT ASSUMED HARMLESS. A transaction type nothing interprets is either a
              corporate action carrying no money or something new that belongs in the total, and
              only a visible count can ever tell the two apart. */}
          {d && Object.keys(d.unknown_transaction_types ?? {}).length > 0 && (
            <p className="text-[11px] text-warn-500 px-1">
              {Object.entries(d.unknown_transaction_types ?? {}).map(([t, n]) => `${n}× “${t}”`).join(', ')}
              {' '}transaction row(s) are not interpreted and carry no money on the sheets measured
              so far — excluded from every total above, and counted here so a future one that does
              carry a value cannot slip in unnoticed.
            </p>
          )}

          {!!d?.realised?.length && (
            <div className="overflow-x-auto rounded-lg border border-neutral-800/40">
              <table className="w-full text-xs whitespace-nowrap">
                <thead className="bg-card [&_th]:bg-card">
                  <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                    <th className="px-3 py-1.5 font-medium text-left">Sold</th>
                    <th className="px-3 py-1.5 font-medium text-right">Sales</th>
                    <th className="px-3 py-1.5 font-medium text-right">Quantity</th>
                    <th className="px-3 py-1.5 font-medium text-right">Proceeds (€)</th>
                    <th className="px-3 py-1.5 font-medium text-right">Cost (€)</th>
                    <th className="px-3 py-1.5 font-medium text-right"
                      title="AIRS's own Res. YtD — the part of the realised result that belongs to THIS year. Deliberately not proceeds − cost, which would include a gain made in an earlier year.">
                      Realised YTD (€)
                    </th>
                    <th className="px-3 py-1.5 font-medium text-left">Period</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-neutral-800/20">
                  {(d.realised ?? []).map((l) => (
                    <tr key={l.fonds} className="hover:bg-overlay/[0.02]">
                      <td className="px-3 py-1.5 text-fg-soft">
                        {l.fonds}
                        {/* ⚠ A SALE IS A REALISATION, NOT A CLOSURE. Most of these names are still
                            held — they were trimmed. Only a name absent from the positions table
                            is genuinely out, and that is what this badge means. */}
                        {l.closed_out && (
                          <span className="ml-2 px-1.5 py-0.5 rounded-md bg-overlay/5 text-[10px] text-fg-muted"
                            title={t.closedOut}>
                            closed
                          </span>
                        )}
                        {/* ⚠ THE WHOLE REASON `Res. YtD` IS USED RATHER THAN proceeds − cost. */}
                        {!!l.prior_year_eur && (
                          <span className="ml-2 text-[10px] text-warn-500"
                            title={`${eur(l.prior_year_eur)} of this gain was made in earlier years and is correctly NOT in this year's total.`}>
                            {eur(l.prior_year_eur)} prior yr
                          </span>
                        )}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono tabular-nums text-fg-muted">{l.sales}</td>
                      <td className="px-3 py-1.5 text-right font-mono tabular-nums text-fg-muted">{num(l.quantity)}</td>
                      <td className="px-3 py-1.5 text-right font-mono tabular-nums text-fg">{eur(l.proceeds_eur)}</td>
                      <td className="px-3 py-1.5 text-right font-mono tabular-nums text-fg-muted">{eur(l.cost_eur)}</td>
                      <td className={`px-3 py-1.5 text-right font-mono tabular-nums ${tone(l.realised_ytd_eur)}`}>
                        {eur(l.realised_ytd_eur)}
                      </td>
                      <td className="px-3 py-1.5 text-fg-faint text-[11px]">
                        {l.first === l.last ? l.first : `${l.first} → ${l.last}`}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function Row({ label, sub, eur: v }: { label: string; sub?: string; eur?: number | null }) {
  return (
    <tr>
      <td className="px-3 py-1.5 text-fg-soft">
        {label}
        {sub && <span className="ml-2 text-[11px] text-fg-faint">{sub}</span>}
      </td>
      <td className={`px-3 py-1.5 text-right font-mono tabular-nums ${tone(v)}`}>{eur(v)}</td>
    </tr>
  );
}

function Stat({ label, value, note, tone: t }: {
  label: string; value: string; note: string; tone: string;
}) {
  return (
    <div className="rounded-lg border border-neutral-800/40 bg-card px-3 py-2">
      <div className="text-[11px] uppercase tracking-wide text-fg-faint">{label}</div>
      <div className={`font-mono text-lg ${t}`}>{value}</div>
      <div className="text-[11px] text-fg-faint mt-0.5">{note}</div>
    </div>
  );
}

/** ⚠ A DASH, NEVER A €0. "We could not compute this" and "it came to nothing" are different
 *  facts, and on a reconciliation the second is a claim. */
const eur = (v?: number | null) =>
  (v == null ? '—' : `€${v.toLocaleString('en-US', { maximumFractionDigits: 2, minimumFractionDigits: 2 })}`);
const num = (v?: number | null) =>
  (v == null ? '—' : v.toLocaleString('en-US', { maximumFractionDigits: 6 }));
const pct = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
const tone = (v?: number | null) =>
  (v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400');
