'use client';

import { Fragment, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import type { AirsAccount, AirsAccountDetail } from '../../lib/types/api';

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
 * ⚠ THE RETURN IS AIRS'S OWN `cumulatief_rendement`, NEVER end/begin.
 *   A value ratio is a return only when nothing was deposited or withdrawn — and these are real
 *   accounts. AIRS publishes both; they disagree by >1pp in 31 of 38, and on AITopSelectie OFF DYN
 *   the ratio reads -5.85% on a book that made +46.12%. The ratio is shown here ONLY beside the
 *   right number, so nobody recomputes it by hand later and trusts it.
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
    const r = await apiFetch(
      `${API_URL}/api/airs/accounts/${encodeURIComponent(name)}/holdings`);
    if (!r.ok) return;
    const body = (await r.json()) as AirsAccountDetail;
    setDetail((d) => ({ ...d, [name]: body }));
  };

  const asOf = rows?.find((r) => r.as_of)?.as_of ?? rows?.[0]?.periode;

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div>
        <h3 className="text-sm font-semibold text-fg-strong">
          AIRS accounts{rows ? ` · ${rows.length}` : ''}
        </h3>
        <p className="text-[11px] text-fg-faint mt-0.5">
          What the books actually made, on AIRS&apos;s own EUR values — not our price data.
          {asOf && <> As of <span className="font-mono text-fg-subtle">{asOf}</span>.</>}
          {' '}A different object from the model portfolios above: a model is a composition of
          weights, which AIRS has nothing to value.
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
                  title="AIRS's own cumulative return (cumulatief_rendement) — flow-aware, because AIRS knows the deposits and withdrawals. This is NOT end value ÷ begin value; see the next column.">
                  YTD (€)
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="⚠ THE WRONG NUMBER, shown on purpose. This is end value ÷ begin value − 1 — the figure you get by comparing the portfolio's worth on 31 December against today. It is a return ONLY if nothing was paid in or out, and these are real accounts. Measured: it disagrees with the real return by more than a point in 31 of 38 accounts, and on AITopSelectie OFF DYN it reads −5.85% on a book that made +46.12%. It is here so nobody recomputes it by hand and trusts it.">
                  value ratio ⚠
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's koersresultaat — the price gains, separated from income. This is the number a 'what did the positions do' question is really after.">
                  Price result
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's opbrengsten — dividends and coupons. No price return contains this, which is one reason the positions below never sum to the YTD.">
                  Income
                </th>
                <th className="px-3 py-1.5 font-medium text-right">Value</th>
                <th className="px-3 py-1.5 font-medium text-right">Pos.</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {rows.map((a) => {
                const isOpen = open === a.portefeuille;
                const gap = (a.ytd_pct ?? 0) - (a.value_ratio_pct ?? 0);
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
                      {/* The wrong number, muted — present as a warning, never as an alternative. */}
                      <td className="px-3 py-1.5 text-right font-mono text-fg-faint"
                        title={Math.abs(gap) > 1
                          ? `Off by ${gap >= 0 ? '+' : ''}${gap.toFixed(2)}pp. The difference is money paid in or out — this ratio counts a deposit as a gain.`
                          : 'Close to the real return here, which only means little money moved in or out this year. It is not a method that works.'}>
                        {pct(a.value_ratio_pct)}
                        {Math.abs(gap) > 1 && <span className="text-warn-400 ml-1">⚠</span>}
                      </td>
                      <td className={`px-3 py-1.5 text-right font-mono ${tone(a.price_result_eur)}`}>
                        {eur(a.price_result_eur)}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">{eur(a.income_eur)}</td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-soft">{eur(a.end_value_eur)}</td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">{a.holdings ?? '—'}</td>
                    </tr>
                    {isOpen && (
                      <tr>
                        <td colSpan={7} className="px-3 py-3 bg-inset">
                          <AccountPositions d={detail[a.portefeuille]} />
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

function AccountPositions({ d }: { d?: AirsAccountDetail }) {
  if (!d) return <p className="text-[11px] text-fg-subtle">Loading positions…</p>;
  if (!d.rows?.length) {
    return <p className="text-[11px] text-fg-subtle">We hold no position snapshot for this account.</p>;
  }
  return (
    <div className="space-y-2">
      {/* ⚠ Said BEFORE the numbers, not after. The model table above weights exactly to its total;
          a reader arriving from there will try to add these up and conclude something is broken. */}
      <p className="text-[10px] text-fg-faint leading-relaxed">
        These are <strong>price</strong> returns — AIRS restates each position&apos;s opening value
        to its current quantity, so a purchase does not show up as a gain. They do{' '}
        <strong>not</strong> add up to the account&apos;s{' '}
        <span className={`font-mono ${tone(d.ytd_pct)}`}>{pct(d.ytd_pct)}</span>, and that is
        correct: the account&apos;s figure is flow-aware and also contains{' '}
        <span className="font-mono">{eur(d.income_eur)}</span> of income, which no price return
        carries.
      </p>
      <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[50vh]">
        <table className="w-full text-xs">
          <thead className="bg-card sticky top-0">
            <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
              <th className="px-3 py-1.5 font-medium text-left">Fund</th>
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
