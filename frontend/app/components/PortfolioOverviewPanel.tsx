'use client';

import { Fragment, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import PortfolioAnalysisModal from './portfolios/PortfolioAnalysisModal';
import type {
  AirsAccountDetail, AirsAccountIsins, AirsHoldingSegment, AirsPortfolioOverview,
} from '../../lib/types/api';

/**
 * The one table: a portfolio, by the name you gave it, on AIRS's own numbers.
 *
 * A portfolio lives in AIRS as TWO rows that share nothing but a strategy — the Fixed one
 * (`_FX`/`_AFS`: weights, ISINs, your nickname, and nothing AIRS will value) and the Dynamic one
 * (`_DYN`: the real book — quantities, EUR values, returns, and NO ISIN). Measured: 58 Fixed with
 * a composition, 31 valued Dynamic, overlap ZERO. Neither is the portfolio. The pair is.
 *
 * So: the NAME is the Fixed side's; every NUMBER is the Dynamic side's, because AIRS is the
 * system of record for what a book made and we are not. Expanding a row shows the holdings —
 * ISIN and fund name from the Fixed side, everything else AIRS's own.
 *
 * ⚠ 27 OF 28 PAIRINGS ARE AN UNCONFIRMED GUESS, AND THE ROW MUST SAY SO. This is not a small
 *   doubt: the risk variants of a strategy hold the SAME instruments (BUS_FTS_Bepoff/DEF/NEU_AFS
 *   share 27 of 27 ISINs), so a mis-pairing files a real book's money under another strategy's
 *   name and NOTHING else on the row looks wrong. Confirm them in Dynamic → Fixed.
 */

const pct = (v: number | null | undefined) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
const eur = (v: number | null | undefined) =>
  v == null ? '—' : `€${Math.round(v).toLocaleString('en-US')}`;
const tone = (v: number | null | undefined) =>
  v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400';

export default function PortfolioOverviewPanel() {
  const [rows, setRows] = useState<AirsPortfolioOverview[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [open, setOpen] = useState<string | null>(null);
  const [detail, setDetail] = useState<Record<string, AirsAccountDetail>>({});
  const [isins, setIsins] = useState<Record<string, AirsAccountIsins>>({});
  const [onlyLinked, setOnlyLinked] = useState(true);
  // The Fixed portfolio to analyse. Its id, not the row's — the modal describes the strategy.
  const [analyse, setAnalyse] = useState<{ id: number; name: string } | null>(null);

  useEffect(() => {
    let dead = false;
    void (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/airs/portfolios/overview`);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const b = (await r.json()) as AirsPortfolioOverview[];
        if (!dead) setRows(b);
      } catch (e) {
        if (!dead) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { dead = true; };
  }, []);

  const expand = async (p: string) => {
    setOpen(open === p ? null : p);
    if (detail[p] || open === p) return;
    // Fetched together: a holding briefly showing its value without its identity, or worse with
    // the wrong one, is not an improvement over showing neither.
    const [h, i] = await Promise.all([
      apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(p)}/holdings`),
      apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(p)}/isins`),
    ]);
    if (!h.ok) return;
    // Awaited BEFORE the updaters: a setState callback is not async, so `await` inside one
    // stores the Promise itself and the row renders `[object Promise]`-shaped nothing.
    const holdings = (await h.json()) as AirsAccountDetail;
    const resolved = i.ok ? ((await i.json()) as AirsAccountIsins) : null;
    setDetail((d) => ({ ...d, [p]: holdings }));
    if (resolved) setIsins((m) => ({ ...m, [p]: resolved }));
  };

  const view = (rows ?? []).filter((r) => (onlyLinked ? !!r.fixed_name : true));
  const linked = (rows ?? []).filter((r) => !!r.fixed_name).length;
  const unconfirmed = (rows ?? []).filter((r) => r.link_source === 'guess').length;

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div className="flex items-baseline justify-between gap-4 flex-wrap">
        <div>
          <h3 className="text-sm font-semibold text-fg-strong">
            Portfolios{rows ? ` · ${view.length}` : ''}
          </h3>
          <p className="text-[11px] text-fg-faint mt-0.5 max-w-3xl">
            Named from the Fixed portfolio; figures from AIRS, year to date. Expand a row for
            holdings.
          </p>
        </div>
        {rows && (
          <label className="flex items-center gap-1.5 text-xs text-fg-subtle cursor-pointer whitespace-nowrap">
            <input type="checkbox" checked={onlyLinked}
              onChange={(e) => setOnlyLinked(e.target.checked)} />
            {/* An unlinked book is a real book — the benchmarks and tests. Hidden by default
                because it has no nickname and no ISINs, not because it is not a portfolio. */}
            Linked only ({linked} of {rows.length})
          </label>
        )}
      </div>

      {unconfirmed > 0 && rows && (
        // Terse on screen, but the stake stays reachable on hover: a wrong pairing files a
        // book's money under another strategy's name, and the risk variants hold the same
        // instruments, so nothing else on the row would look wrong.
        <p className="text-[11px] text-warn-400 bg-warn-500/10 border border-warn-500/20 rounded-lg px-3 py-1.5"
          title="A wrong pairing names a portfolio after a strategy it does not run. The risk variants of a strategy hold the same instruments, so no other column would reveal it.">
          {unconfirmed} pairing{unconfirmed === 1 ? '' : 's'}{' '}unconfirmed. Confirm in
          Dynamic&nbsp;→&nbsp;Fixed.
        </p>
      )}

      {!rows && !err && <p className="text-xs text-fg-subtle">Loading…</p>}
      {err && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{err}</div>
      )}

      {rows && (
        <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[70vh]">
          <table className="w-full text-xs">
            <thead className="bg-card sticky top-0 z-10">
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                <th className="px-3 py-1.5 font-medium text-left">Name</th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="Positions in the Fixed portfolio — the ISINs this pairing can reach. Blank = not linked to one.">
                  ISINs
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's own cumulatief_rendement for the year — every month compounded, flow-aware. Never end ÷ begin.">
                  YTD
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's rendement from its newest row — the LATEST MONTH, a different window from the year. Not a rival YTD.">
                  Last month
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's koersresultaat for the year — price gains, income excluded.">
                  Price result
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's opbrengsten for the year — dividends and coupons.">
                  Income
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's beleggingsresultaat for the year. Ties exactly to the value change: end − begin − deposits + withdrawals.">
                  Invest. result
                </th>
                <th className="px-3 py-1.5 font-medium text-right">Value</th>
                <th className="px-3 py-1.5 font-medium text-right">Pos.</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {view.map((r) => {
                const isOpen = open === r.dynamic_portefeuille;
                return (
                  <Fragment key={r.dynamic_portefeuille}>
                    <tr onClick={() => void expand(r.dynamic_portefeuille)}
                      className="hover:bg-accent-500/10 transition-colors cursor-pointer">
                      <td className="px-3 py-1.5 text-fg whitespace-nowrap">
                        <span className="text-fg-faint mr-1.5">{isOpen ? '▾' : '▸'}</span>
                        {r.name}
                        {r.link_source === 'guess' && (
                          <span className="text-warn-400 ml-1"
                            title={`Unconfirmed: this book is paired with ${r.fixed_name} by a name match nobody has approved (${r.link_reason ?? ''}). The name above is that pairing's.`}>
                            ⚠
                          </span>
                        )}
                        {/* AIRS's own codes, kept visible but quiet: the nickname is what you
                            read, the codes are what you search AirSPMS for. */}
                        <span className="text-fg-faint font-mono text-[10px] ml-2">
                          {r.dynamic_portefeuille}{r.fixed_name ? ` · ${r.fixed_name}` : ''}
                        </span>
                        {/* Analyse describes the FIXED portfolio (its composition + attribution),
                            which is why it needs `fixed_portfolio_id` and an unlinked row cannot
                            offer it. stopPropagation so it does not also toggle the row. */}
                        {r.fixed_portfolio_id != null && (
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              setAnalyse({ id: r.fixed_portfolio_id!, name: r.name });
                            }}
                            className="ml-2 text-[10px] px-1.5 py-0.5 rounded border border-neutral-800/40 text-fg-subtle hover:bg-overlay/5 hover:text-fg align-middle"
                          >
                            Analyse
                          </button>
                        )}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">{r.isins ?? '—'}</td>
                      <td className={`px-3 py-1.5 text-right font-mono font-semibold ${tone(r.ytd_pct)}`}>
                        {pct(r.ytd_pct)}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-faint">{pct(r.latest_month_pct)}</td>
                      <td className={`px-3 py-1.5 text-right font-mono ${tone(r.price_result_eur)}`}>
                        {eur(r.price_result_eur)}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">{eur(r.income_eur)}</td>
                      <td className={`px-3 py-1.5 text-right font-mono ${tone(r.investment_result_eur)}`}
                        title={r.reconciles === false
                          ? `⚠ Does not reconcile: off by ${eur(r.residual_eur)} against the book's own value change. A month is probably missing from our copy, so this total is short.`
                          : undefined}>
                        {eur(r.investment_result_eur)}
                        {r.reconciles === false && <span className="text-warn-400 ml-1">⚠</span>}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-soft">{eur(r.end_value_eur)}</td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">{r.holdings ?? '—'}</td>
                    </tr>
                    {isOpen && (
                      <tr>
                        <td colSpan={9} className="px-3 py-3 bg-inset">
                          <Holdings d={detail[r.dynamic_portefeuille]} i={isins[r.dynamic_portefeuille]} />
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      {analyse && (
        <PortfolioAnalysisModal id={analyse.id} name={analyse.name}
          onClose={() => setAnalyse(null)} />
      )}
    </section>
  );
}

/**
 * ⚠ `unpriced` IS NOT A PASS — the name matched and NOTHING checked it, which for a fund is
 * exactly where the share-class trap lives (IE00BNDS1P30 vs IE00BNDS1Q47: both "Vanguard ESG
 * Global Corporate Bond UCITS ETF EUR Hedged", Acc and Inc, €4.79 vs €3.99, compounding
 * differently). It must not look like `ok`.
 */
function IsinCell({ r }: { r: NonNullable<AirsAccountIsins['rows']>[number] | undefined }) {
  if (!r?.isin) return <span className="text-fg-faint">—</span>;
  const mismatch = r.verdict === 'price_mismatch';
  const unpriced = r.verdict === 'unpriced';
  return (
    <span className="font-mono whitespace-nowrap">
      <span className={mismatch ? 'text-neg-400' : unpriced ? 'text-fg-muted' : 'text-fg-soft'}>
        {r.isin}
      </span>
      {mismatch && (
        <span className="text-neg-400 ml-1" title={`⚠ The price says this is NOT the same instrument. This holding implies €${r.implied_price_eur}/unit; ${r.isin} last closed at €${r.our_price_eur} (${r.our_instrument ?? 'our instrument'}) — a ratio of ${r.price_ratio}. Either the Fixed portfolio carries the wrong ISIN, or the book holds a different share class than it specifies.`}>⚠</span>
      )}
      {unpriced && (
        <span className="text-fg-faint ml-1" title="Matched on the name only — we hold no price series for this instrument, so nothing confirms it. Not the same as a pass.">?</span>
      )}
    </span>
  );
}

/**
 * An asset class, and what it returned. AIRS's own `Beleggingscategorie` — not our inference.
 *
 * ⚠ THE RETURN AND THE WEIGHT DO NOT COVER THE SAME HOLDINGS. A holding with no opening value
 *   has an undefined return but real exposure, so it counts in the weight and not in the return.
 *   Cash is exactly this, and so is a short (Nestle India, -3,504 shares). Where they differ the
 *   header says how much the return spans, rather than quietly averaging over a smaller basket.
 *
 * ⚠ ETFs ARE COUNTED, NEVER BUCKETED. An equity ETF is Equity and a bond ETF is Bonds — that is
 *   AIRS's classification and it is the right one: 10 of the 11 bond ISINs are ETFs, so an "ETF"
 *   bucket would empty Bonds and make a defensive book read as holding almost none.
 */
function SegmentHeader({ s }: { s: AirsHoldingSegment }) {
  const etfPct = s.value_eur && s.etf_value_eur ? (100 * s.etf_value_eur) / s.value_eur : 0;
  const partial = s.value_eur != null && s.priced_value_eur != null
    && Math.abs(s.value_eur - s.priced_value_eur) > 1;
  return (
    <tr className="bg-overlay/[0.03] border-t border-neutral-800/40">
      <td className="px-3 py-1 font-semibold text-fg-strong">
        {s.asset_class}
        <span className="text-fg-faint font-normal ml-2">
          {s.holdings} holding{s.holdings === 1 ? '' : 's'}
          {etfPct >= 0.5 && (
            <span title={`${eur(s.etf_value_eur)} of this segment is held via ETFs. An equity ETF is Equity and a bond ETF is Bonds — the wrapper does not change the exposure.`}>
              {' · '}{etfPct.toFixed(0)}% via ETFs
            </span>
          )}
        </span>
      </td>
      {/* ⚠ ONE CELL PER COLUMN, and the money cells sit under the columns they sum. The table is
          Fund · ISIN · Qty · Ccy · Weight · Start · Now · Gain · Return — nine. An extra blank
          here shifts every figure one column right, which is silent: a weight renders perfectly
          well under "Start (€)". */}
      <td />{/* ISIN */}
      <td />{/* Qty */}
      <td />{/* Ccy */}
      <td className="px-3 py-1 text-right font-mono text-fg-subtle">
        {s.weight_pct == null ? '—' : `${s.weight_pct.toFixed(2)}%`}
      </td>
      <td className="px-3 py-1 text-right font-mono text-fg-subtle">{eur(s.start_value_eur)}</td>
      <td className="px-3 py-1 text-right font-mono text-fg">{eur(s.value_eur)}</td>
      <td className={`px-3 py-1 text-right font-mono ${tone(s.gain_eur)}`}>{eur(s.gain_eur)}</td>
      <td className={`px-3 py-1 text-right font-mono font-semibold ${tone(s.return_pct)}`}
        title={partial
          ? `Price return over ${eur(s.priced_value_eur)} of this segment's ${eur(s.value_eur)}. The rest has no opening value — it was not held when the year opened — so its return is undefined, not zero.`
          : 'Price return: the segment valued at the year\'s open (restated to today\'s quantities) against today. No income, not flow-aware — so the segments do not sum to the portfolio\'s figure.'}>
        {s.return_pct == null ? '—' : pct(s.return_pct)}
        {partial && s.return_pct != null && <span className="text-warn-400 ml-1">*</span>}
      </td>
    </tr>
  );
}

function Holdings({ d, i }: { d?: AirsAccountDetail; i?: AirsAccountIsins }) {
  if (!d) return <p className="text-[11px] text-fg-subtle">Loading holdings…</p>;
  if (!d.rows?.length) return <p className="text-[11px] text-fg-subtle">No holdings snapshot stored.</p>;
  const byName = new Map((i?.rows ?? []).map((r) => [r.holding_name, r]));
  const mismatches = (i?.rows ?? []).filter((r) => r.verdict === 'price_mismatch').length;

  // Grouped by AIRS's asset class, in the backend's order (Cash and Unclassified last — they are
  // not classes anyone allocates to, they are what is left). A holding whose class we do not know
  // still renders: it falls in the trailing ungrouped block rather than vanishing from a table
  // that is supposed to account for the whole book.
  const all = d.rows ?? [];
  const segs = i?.segments ?? [];
  const classOf = (name: string) => byName.get(name)?.asset_class ?? null;
  const ordered: [AirsHoldingSegment | null, typeof all][] = segs.length
    ? segs
      .map((s) => [s, all.filter((r) => classOf(r.holding_name) === s.asset_class)] as
        [AirsHoldingSegment, typeof all])
      .filter(([, g]) => g.length)
    : [[null, all]];
  const grouped = new Set(ordered.flatMap(([, g]) => g.map((r) => r.holding_name)));
  const rest = all.filter((r) => !grouped.has(r.holding_name));
  if (rest.length) ordered.push([null, rest]);
  return (
    <div className="space-y-2">
      {/* ⚠ Stated BEFORE the numbers — a reader coming from a weights table will try to add
          these up and conclude something is broken. One line; the reasoning is on hover. */}
      <p className="text-[10px] text-fg-faint leading-relaxed"
        title="Price returns: AIRS restates each opening value to the current quantity, so a purchase is not a gain. The portfolio's own figure is flow-aware and includes income, which no price return carries — so these do not sum to it.">
        ISIN and fund name from the Fixed portfolio; other columns from AIRS. Price returns — they
        do <strong>not</strong>{' '}sum to the portfolio&apos;s{' '}
        <span className={`font-mono ${tone(d.ytd_pct)}`}>{pct(d.ytd_pct)}</span>, which is
        flow-aware and includes <span className="font-mono">{eur(d.income_eur)}</span>{' '}income.
        {mismatches > 0 && (
          <span className="text-neg-400">{' '}{mismatches} ISIN{mismatches === 1 ? '' : 's'}{' '}
            {mismatches === 1 ? 'disagrees' : 'disagree'} with the price.</span>
        )}
        {i?.unmatched_model_positions && i.unmatched_model_positions.length > 0 && (
          <span className="text-fg-muted"
            title="Held by the Fixed portfolio but not by this book — implementation drift.">
            {' '}Not held here:{' '}
            {i.unmatched_model_positions.map((u) => u.fonds).join(', ')}.
          </span>
        )}
      </p>
      <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[50vh]">
        <table className="w-full text-xs">
          <thead className="bg-card sticky top-0">
            <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
              <th className="px-3 py-1.5 font-medium text-left">Fund</th>
              <th className="px-3 py-1.5 font-medium text-left"
                title="From the Fixed portfolio, then price-checked against that instrument's own close. ⚠ = the price disagrees; ? = no series, so nothing confirms the name.">
                ISIN
              </th>
              <th className="px-3 py-1.5 font-medium text-right">Qty</th>
              <th className="px-3 py-1.5 font-medium text-left">Ccy</th>
              <th className="px-3 py-1.5 font-medium text-right">Weight</th>
              <th className="px-3 py-1.5 font-medium text-right" title="Beginwaarde lopend jaar EUR — restated to today's quantity.">Start (€)</th>
              <th className="px-3 py-1.5 font-medium text-right" title="Huidige waarde EUR.">Now (€)</th>
              <th className="px-3 py-1.5 font-medium text-right">Gain (€)</th>
              <th className="px-3 py-1.5 font-medium text-right">Return</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {ordered.map(([seg, group]) => (
              <Fragment key={seg?.asset_class ?? 'x'}>
                {seg && <SegmentHeader s={seg} />}
                {group.map((r, n) => (
              <tr key={`${r.holding_name}-${n}`} className="hover:bg-overlay/[0.02]">
                <td className="px-3 py-1.5 text-fg-soft pl-6">{r.holding_name}</td>
                <td className="px-3 py-1.5"><IsinCell r={byName.get(r.holding_name)} /></td>
                <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                  {r.quantity != null ? r.quantity.toLocaleString('en-US') : '—'}
                </td>
                <td className="px-3 py-1.5 font-mono text-fg-muted">{r.currency || '—'}</td>
                <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                  {r.weight != null ? `${(r.weight * 100).toFixed(2)}%` : '—'}
                </td>
                <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">{eur(r.start_value_eur)}</td>
                <td className="px-3 py-1.5 text-right font-mono text-fg">{eur(r.current_value_eur)}</td>
                <td className={`px-3 py-1.5 text-right font-mono ${tone(r.ytd_return_eur)}`}>{eur(r.ytd_return_eur)}</td>
                {/* ⚠ A dash, never 0%. No opening value = the return is UNDEFINED, not flat. */}
                <td className={`px-3 py-1.5 text-right font-mono ${tone(r.ytd_return_pct)}`}
                  title={r.ytd_return_pct == null
                    ? 'No opening value — not held when the year opened (or a cash line). Its return is undefined, not zero.'
                    : undefined}>
                  {r.ytd_return_pct == null ? '—' : pct(r.ytd_return_pct * 100)}
                </td>
              </tr>
                ))}
              </Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
