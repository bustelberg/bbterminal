'use client';

import { Fragment, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import type { AirsAccount, AirsAccountDetail, AirsAccountIsins } from '../../lib/types/api';

/**
 * The AIRS ACCOUNTS — what the books actually made, on AIRS's own numbers.
 *
 * WHY THIS SITS BESIDE THE MODEL TABLE RATHER THAN REPLACING IT
 *   A model portfolio is a COMPOSITION — weights, no holdings — so AIRS has nothing to value and
 *   publishes no Vermogensoverzicht for one. Measured: 58 models with a composition, 39 accounts
 *   with AIRS values, overlap ZERO. Two different questions:
 *     the model    "would this strategy work?"   -> yfinance; nothing else can price a set of weights
 *     the account  "what did this book make?"    -> AIRS knows, and it is the system of record
 *   The gap between them is implementation drift, timing and fees.
 *
 * ⚠ EVERY MONEY FIGURE HERE IS THE YEAR'S, SUMMED ACROSS AIRS'S MONTHLY ROWS.
 *   One ATT row is one MONTH. This panel used to render the freshest row, so it showed
 *   AITopSelectie's JULY price result of -130,063 as the year's, next to a +42% YTD — wrong sign,
 *   a third of the size. The year is +420,225. `_airs_accounts._year_perf` does the assembly.
 *
 * ⚠ `Last month` IS NOT A RIVAL YTD.
 *   It was once shown as "value ratio ⚠ THE WRONG NUMBER", on the theory that deposits inflated
 *   it. Measured 2026-07-17: AITopSelectie has ZERO deposits in every month of 2026 and its two
 *   figures still differ by 50pp — flows were never the cause, the month-vs-year read was. Both
 *   numbers are AIRS's own and both are right, of different windows.
 *
 * ⚠ THE POSITIONS DO NOT SUM TO THE ACCOUNT'S RETURN, AND THAT IS CORRECT.
 *   Each position is a PRICE return; the account's figure is flow-aware and also carries income.
 *   The model table above has the opposite property (its holdings weight exactly to its total), so
 *   a reader arriving from there will expect these to tie. The panel says so out loud.
 */

const pct = (v: number | null | undefined, dp = 2) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(dp)}%`;
const eur = (v: number | null | undefined) =>
  v == null ? '—' : `€${Math.round(v).toLocaleString('en-US')}`;
const tone = (v: number | null | undefined) =>
  v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400';

export default function AirsAccountsPanel() {
  const [rows, setRows] = useState<AirsAccount[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [open, setOpen] = useState<string | null>(null);
  const [detail, setDetail] = useState<Record<string, AirsAccountDetail>>({});
  const [isinMap, setIsinMap] = useState<Record<string, AirsAccountIsins>>({});

  useEffect(() => {
    let dead = false;
    void (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/airs/accounts`);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const b = (await r.json()) as AirsAccount[];
        if (!dead) setRows(b);
      } catch (e) {
        if (!dead) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { dead = true; };
  }, []);

  const expand = async (name: string) => {
    setOpen(open === name ? null : name);
    if (detail[name] || open === name) return;
    // Two sources, one table. The holdings are AIRS's (money, no ISIN); the ISINs come from the
    // model this account runs, joined row-by-row and price-checked. Fetched together so a row
    // never renders its ISIN a beat after its value — a holding briefly showing someone else's
    // identity is worse than showing none.
    const [hRes, iRes] = await Promise.all([
      apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(name)}/holdings`),
      apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(name)}/isins`),
    ]);
    if (!hRes.ok) return;
    const body = (await hRes.json()) as AirsAccountDetail;
    const isins = iRes.ok ? ((await iRes.json()) as AirsAccountIsins) : null;
    setDetail((d) => ({ ...d, [name]: body }));
    if (isins) setIsinMap((m) => ({ ...m, [name]: isins }));
  };

  const asOf = rows?.find((r) => r.as_of)?.as_of ?? rows?.[0]?.periode;

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div>
        <h3 className="text-sm font-semibold text-fg-strong">
          {/* "Dynamic" is AIRS's own word: these are the `normaal` portfolios, whose names carry
              the `_DYN` suffix — the live books. Their counterpart is the Fixed table above. */}
          AIRS Dynamic Portfolio&apos;s{rows ? ` · ${rows.length}` : ''}
        </h3>
        <p className="text-[11px] text-fg-faint mt-0.5"
          title="The live books, valued by AIRS. A Fixed portfolio is a composition of weights, which AIRS has nothing to value — the two do not overlap.">
          AIRS EUR values, for the year.
          {asOf && <> As of <span className="font-mono text-fg-subtle">{asOf}</span>.</>}
        </p>
      </div>

      {!rows && !err && <p className="text-xs text-fg-subtle">Loading…</p>}
      {err && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{err}</div>
      )}

      {rows && rows.length > 0 && (
        <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[60vh]">
          <table className="w-full text-xs">
            <thead className="bg-card sticky top-0 z-10">
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                <th className="px-3 py-1.5 font-medium text-left">Account</th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's own cumulatief_rendement — the year, being every month's return compounded together. Flow-aware, because AIRS knows the deposits and withdrawals. Never end ÷ begin.">
                  YTD (€)
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's rendement from its newest row — the LATEST MONTH's return, not the year's. It is not a rival YTD and not a worse one: it is a fact about a different window. AITopSelectie reads −5.85% here against +46.12% for the year, and both are correct.">
                  Last month
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's koersresultaat, summed across every month of the year — the price gains, separated from income. This is the number a 'what did the positions do' question is really after.">
                  Price result
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's opbrengsten for the year — dividends and coupons. No price return contains this, which is one reason the positions below never sum to the YTD.">
                  Income
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's beleggingsresultaat for the year — what the investing made, in euros. It is price result + income + accrued interest, so it will not always equal the two columns to its left added together (AIRS also nets costs into it). It ties to the value change exactly: end − begin − deposits + withdrawals.">
                  Invest. result
                </th>
                <th className="px-3 py-1.5 font-medium text-right">Value</th>
                <th className="px-3 py-1.5 font-medium text-right">Pos.</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {rows.map((a) => {
                const isOpen = open === a.portefeuille;
                return (
                  <Fragment key={a.portefeuille}>
                    <tr onClick={() => void expand(a.portefeuille)}
                      className="hover:bg-accent-500/10 transition-colors cursor-pointer">
                      <td className="px-3 py-1.5 font-mono text-fg whitespace-nowrap">
                        <span className="text-fg-faint mr-1.5">{isOpen ? '▾' : '▸'}</span>
                        {a.portefeuille}
                      </td>
                      <td className={`px-3 py-1.5 text-right font-mono font-semibold ${tone(a.ytd_pct)}`}>
                        {pct(a.ytd_pct)}
                      </td>
                      {/* The latest month — muted because it is the smaller window, not because
                          it is suspect. It answers "how is it going right now". */}
                      <td className={`px-3 py-1.5 text-right font-mono text-fg-faint`}
                        title={a.months
                          ? `The most recent of ${a.months} monthly rows AIRS reports for this year. The YTD beside it is all ${a.months} compounded.`
                          : undefined}>
                        {pct(a.latest_month_pct)}
                      </td>
                      <td className={`px-3 py-1.5 text-right font-mono ${tone(a.price_result_eur)}`}>
                        {eur(a.price_result_eur)}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">{eur(a.income_eur)}</td>
                      {/* ⚠ A total that does not tie to the value change is not a total. AIRS's own
                          identity is end − begin − deposits + withdrawals == beleggingsresultaat;
                          when it fails we are missing a month, and the figure is short by exactly
                          that month while still looking like a year. Say so rather than print it
                          plain. */}
                      <td className={`px-3 py-1.5 text-right font-mono ${tone(a.investment_result_eur)}`}
                        title={a.reconciles === false
                          ? `⚠ Does not reconcile: off by ${eur(a.residual_eur)} against the account's own value change. A month is probably missing from our copy, so this total is short.`
                          : 'Ties exactly to the value change: end − begin − deposits + withdrawals.'}>
                        {eur(a.investment_result_eur)}
                        {a.reconciles === false && <span className="text-warn-400 ml-1">⚠</span>}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-soft">{eur(a.end_value_eur)}</td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">{a.holdings ?? '—'}</td>
                    </tr>
                    {isOpen && (
                      <tr>
                        <td colSpan={8} className="px-3 py-3 bg-inset">
                          <AccountPositions d={detail[a.portefeuille]} i={isinMap[a.portefeuille]} />
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
    </section>
  );
}

/**
 * The ISIN we believe a holding is, and how much to believe it.
 *
 * ⚠ `unpriced` IS NOT A PASS. The name matched and NOTHING checked it — which for a fund is
 *   exactly where the share-class trap lives (IE00BNDS1P30 vs IE00BNDS1Q47: both "Vanguard ESG
 *   Global Corporate Bond UCITS ETF EUR Hedged", Acc and Inc, and they compound differently).
 *   It must not look like `ok`.
 */
function IsinCell({ r }: { r: NonNullable<AirsAccountIsins['rows']>[number] | undefined }) {
  if (!r?.isin) {
    return (
      <span className="text-fg-faint" title={r ? 'No model position matched this holding.' : undefined}>
        —
      </span>
    );
  }
  const mismatch = r.verdict === 'price_mismatch';
  const unpriced = r.verdict === 'unpriced';
  return (
    <span className="font-mono whitespace-nowrap">
      <span className={mismatch ? 'text-neg-400' : unpriced ? 'text-fg-muted' : 'text-fg-soft'}>
        {r.isin}
      </span>
      {mismatch && (
        <span className="text-neg-400 ml-1" title={`⚠ The price says this is NOT the same instrument. This holding implies €${r.implied_price_eur}/unit; ${r.isin} last closed at €${r.our_price_eur} (${r.our_instrument ?? 'our instrument'}) — a ratio of ${r.price_ratio}. Either AIRS's model carries the wrong ISIN, or the book holds a different share class than the model specifies. Both are worth knowing; neither is guessed at here.`}>
          ⚠
        </span>
      )}
      {unpriced && (
        <span className="text-fg-faint ml-1" title="Matched on the name only — we hold no price series for this instrument, so nothing confirms it. Not the same as a pass.">
          ?
        </span>
      )}
    </span>
  );
}

function AccountPositions({ d, i }: { d?: AirsAccountDetail; i?: AirsAccountIsins }) {
  if (!d) return <p className="text-[11px] text-fg-subtle">Loading positions…</p>;
  if (!d.rows?.length) {
    return <p className="text-[11px] text-fg-subtle">No position snapshot stored.</p>;
  }
  // Keyed by AIRS's own fund name — the only thing both sides share. The resolver deduped its
  // rows (AIRS bills one instrument on several lines), so several rows may map to one entry;
  // that is correct, they ARE one instrument.
  const byName = new Map((i?.rows ?? []).map((r) => [r.holding_name, r]));
  const mismatches = (i?.rows ?? []).filter((r) => r.verdict === 'price_mismatch');
  return (
    <div className="space-y-2">
      {i?.model_name && (
        <p className="text-[10px] text-fg-faint leading-relaxed"
          title="Each ISIN is checked against that instrument's own close: a name cannot distinguish two share classes of one fund.">
          ISINs from{' '}
          <span className="font-mono text-fg-subtle">{i.model_name}</span>, price-checked.
          {i.model_source === 'guess' && (
            <span className="text-warn-400" title="This Dynamic↔Fixed pairing is an unconfirmed name match. Confirm it in the Dynamic → Fixed table below.">
              {' '}Pairing unconfirmed.
            </span>
          )}
          {mismatches.length > 0 && (
            <span className="text-neg-400">
              {' '}{mismatches.length} disagree{mismatches.length === 1 ? 's' : ''} with the price.
            </span>
          )}
          {i.unmatched_model_positions && i.unmatched_model_positions.length > 0 && (
            <span className="text-fg-muted"
              title="Held by the Fixed portfolio but not by this book — implementation drift.">
              {' '}Not held here:{' '}
              {i.unmatched_model_positions.map((u) => u.fonds).join(', ')}.
            </span>
          )}
        </p>
      )}
      {/* ⚠ Said BEFORE the numbers, not after. The Fixed table weights exactly to its total; a
          reader arriving from there will try to add these up and conclude something is broken. */}
      <p className="text-[10px] text-fg-faint leading-relaxed"
        title="Price returns: AIRS restates each position's opening value to its current quantity, so a purchase is not a gain. The account's figure is flow-aware and includes income, which no price return carries — so these do not sum to it.">
        Price returns — they do <strong>not</strong>{' '}sum to the account&apos;s{' '}
        <span className={`font-mono ${tone(d.ytd_pct)}`}>{pct(d.ytd_pct)}</span>, which is
        flow-aware and includes <span className="font-mono">{eur(d.income_eur)}</span>{' '}income.
      </p>
      <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[50vh]">
        <table className="w-full text-xs">
          <thead className="bg-card sticky top-0">
            <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
              <th className="px-3 py-1.5 font-medium text-left">Fund</th>
              <th className="px-3 py-1.5 font-medium text-left"
                title="The ISIN this holding is, taken from the Fixed portfolio this Dynamic one runs, then price-checked against that instrument's own close. ⚠ = the price disagrees; ? = we have no series, so nothing confirms the name.">
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
            {d.rows.map((r, i) => (
              <tr key={`${r.holding_name}-${i}`} className="hover:bg-overlay/[0.02]">
                <td className="px-3 py-1.5 text-fg-soft">{r.holding_name}</td>
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
                <td className={`px-3 py-1.5 text-right font-mono ${tone(r.ytd_return_eur)}`}>
                  {eur(r.ytd_return_eur)}
                </td>
                {/* ⚠ A dash, never 0%. A position not held at the year's open (or a cash line) has
                    no opening value — its return is UNDEFINED, and "0.00%" would be a claim. */}
                <td className={`px-3 py-1.5 text-right font-mono ${tone(r.ytd_return_pct)}`}
                  title={r.ytd_return_pct == null
                    ? 'No opening value — this was not held when the year opened (or it is a cash line). Its return is undefined, not zero.'
                    : ''}>
                  {r.ytd_return_pct == null ? '—' : pct(r.ytd_return_pct * 100)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
