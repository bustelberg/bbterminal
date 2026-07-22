'use client';

import { Fragment, useCallback, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { Provenance } from '../../lib/provenance';
import PortfolioAnalysisModal from './portfolios/PortfolioAnalysisModal';
import OwnerEarningsModal from './portfolios/OwnerEarningsModal';
import { type Basket } from './portfolios/PerformanceModal';
import { allocColor, bucketLabel, BUCKET_ORDER } from './portfolios/allocationColors';

/** What an Analyse/Fundamental modal is opened for: one instrument (isin), a group (basket), or a
 *  whole portfolio (portfolioId, resolved to a basket server-side). */
type ModalTarget = { name: string; isin?: string; basket?: Basket; portfolioId?: number };
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

/** A clean reload glyph (inline SVG, not the `↻` character) — the standard two-arrow refresh,
 *  spinning while a refresh is running. */
function RefreshIcon({ spinning, size = 14 }: { spinning?: boolean; size?: number }) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill="none" stroke="currentColor"
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"
      className={spinning ? 'animate-spin' : ''}>
      <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" />
      <path d="M21 3v5h-5" />
      <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" />
      <path d="M3 21v-5h5" />
    </svg>
  );
}

export default function PortfolioOverviewPanel() {
  const [rows, setRows] = useState<AirsPortfolioOverview[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [open, setOpen] = useState<string | null>(null);
  const [detail, setDetail] = useState<Record<string, AirsAccountDetail>>({});
  const [isins, setIsins] = useState<Record<string, AirsAccountIsins>>({});
  const [onlyLinked, setOnlyLinked] = useState(true);
  // The Fixed portfolio to analyse. Its id, not the row's — the modal describes the strategy.
  const [analyse, setAnalyse] = useState<{ id: number; name: string } | null>(null);
  // The whole portfolio's Fundamental (blended owner earnings + price steadiness), by its id.
  const [pfFund, setPfFund] = useState<{ id: number; name: string } | null>(null);
  // Refresh state: the fleet job is running; a status/error line; which single rows are re-scanning.
  const [refreshingAll, setRefreshingAll] = useState(false);
  const [refreshMsg, setRefreshMsg] = useState<{ text: string; kind: 'info' | 'error' | 'ok' } | null>(null);
  const [refreshingRows, setRefreshingRows] = useState<Set<string>>(new Set());

  const loadOverview = useCallback(async () => {
    try {
      const r = await apiFetch(`${API_URL}/api/airs/portfolios/overview`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      setRows((await r.json()) as AirsPortfolioOverview[]);
      setErr(null);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => { void loadOverview(); }, [loadOverview]);

  // Re-scan ONE portfolio's AIRS reports (Rendement + Vermogensoverzicht) and reload its figures.
  // Drops the cached holdings so a re-expand pulls the fresh snapshot too.
  const refreshOne = async (portefeuille: string) => {
    setRefreshingRows((s) => new Set(s).add(portefeuille));
    setRefreshMsg(null);
    try {
      const r = await apiFetch(
        `${API_URL}/api/airs/portfolios/${encodeURIComponent(portefeuille)}/refresh`,
        { method: 'POST' });
      const b = (await r.json().catch(() => null)) as
        { status?: string; errors?: string[]; as_of?: string; holdings_rows?: number } | null;
      if (!r.ok) {
        setRefreshMsg({ text: `${portefeuille}: refresh failed — HTTP ${r.status}. Is the backend running with AIRS credentials?`, kind: 'error' });
      } else if (b?.status === 'busy') {
        setRefreshMsg({ text: 'A full refresh is already running — try again in a moment.', kind: 'info' });
      } else if (b?.status === 'error' || b?.errors?.length) {
        setRefreshMsg({ text: `${portefeuille}: AIRS scan failed — ${b?.errors?.join('; ') || 'unknown error'}`, kind: 'error' });
      } else {
        setRefreshMsg({ text: `${portefeuille}: refreshed — ${b?.holdings_rows ?? 0} holdings as of ${b?.as_of ?? 'today'}.`, kind: 'ok' });
      }
      setDetail((d) => { const n = { ...d }; delete n[portefeuille]; return n; });
      setIsins((m) => { const n = { ...m }; delete n[portefeuille]; return n; });
      await loadOverview();
    } catch (e) {
      setRefreshMsg({ text: `${portefeuille}: ${e instanceof Error ? e.message : String(e)}`, kind: 'error' });
    } finally {
      setRefreshingRows((s) => { const n = new Set(s); n.delete(portefeuille); return n; });
    }
  };

  // Re-scan EVERY live portfolio (the background fleet job), polling its status until it finishes,
  // then reloading. Minutes, not seconds — hence the progress line and a disabled button.
  const refreshAll = async () => {
    if (refreshingAll) return;
    setRefreshingAll(true);
    setRefreshMsg({ text: 'Starting full refresh…', kind: 'info' });
    try {
      const started = await apiFetch(`${API_URL}/api/airs/vermogen/refresh`, { method: 'POST' });
      if (!started.ok) {
        setRefreshMsg({ text: `Refresh failed — HTTP ${started.status}. Is the backend running with AIRS credentials?`, kind: 'error' });
        return;
      }
      for (;;) {
        await new Promise((res) => setTimeout(res, 2500));
        const s = await apiFetch(`${API_URL}/api/airs/vermogen/status`);
        const st = (await s.json().catch(() => null)) as
          { running?: boolean; message?: string; errors?: string[] } | null;
        if (!st?.running) {
          setRefreshMsg({ text: st?.message ?? 'Refresh complete.', kind: st?.errors?.length ? 'error' : 'ok' });
          break;
        }
        if (st?.message) setRefreshMsg({ text: st.message, kind: 'info' });
      }
      setDetail({});
      setIsins({});
      await loadOverview();
    } catch (e) {
      setRefreshMsg({ text: e instanceof Error ? e.message : String(e), kind: 'error' });
    } finally {
      setRefreshingAll(false);
    }
  };

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

  // Re-fetch just one account's ISIN resolution (after a manual Class override), so the row
  // re-groups under its new bucket without collapsing/re-opening the whole holdings table.
  const refreshIsins = useCallback(async (p: string) => {
    const i = await apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(p)}/isins`);
    if (!i.ok) return;
    const resolved = (await i.json()) as AirsAccountIsins;
    setIsins((m) => ({ ...m, [p]: resolved }));
  }, []);

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
        <div className="flex items-center gap-3 flex-wrap">
          {/* Re-scan EVERY portfolio's AIRS Rendement + Vermogensoverzicht. Minutes — a background
              job with a live progress line; the button disables while it runs. */}
          <button type="button" onClick={() => void refreshAll()} disabled={refreshingAll}
            title="Re-scan every portfolio's AIRS Rendement + Vermogensoverzicht now (takes a few minutes)."
            className="inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-lg border border-neutral-700 text-fg-subtle hover:text-accent-300 hover:border-accent-500/50 transition-colors disabled:opacity-50 disabled:cursor-wait">
            <RefreshIcon spinning={refreshingAll} size={12} />
            {refreshingAll ? 'Refreshing…' : 'Refresh all'}
          </button>
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
      </div>

      {/* Refresh outcome — LOUD, so a failed AIRS scan (session expired, backend down, no
          credentials) is never mistaken for "nothing happened". Green when a snapshot was actually
          written, red with the AIRS error otherwise. */}
      {refreshMsg && (
        <div className={`text-[11px] rounded-lg px-3 py-1.5 border ${
          refreshMsg.kind === 'error' ? 'text-neg-300 bg-neg-500/10 border-neg-500/20'
            : refreshMsg.kind === 'ok' ? 'text-pos-300 bg-pos-500/10 border-pos-500/20'
              : 'text-fg-subtle bg-overlay/[0.03] border-neutral-800/40'}`}>
          {refreshMsg.text}
        </div>
      )}

      {unconfirmed > 0 && rows && (
        // Terse on screen, but the stake stays reachable on hover: a wrong pairing files a
        // book's money under another strategy's name, and the risk variants hold the same
        // instruments, so nothing else on the row would look wrong.
        <p className="text-[11px] text-warn-400 bg-warn-500/10 border border-warn-500/20 rounded-lg px-3 py-1.5"
          title="A wrong pairing names a portfolio after a strategy it does not run. The risk variants of a strategy hold the same instruments, so no other column would reveal it.">
          {unconfirmed} pairing{unconfirmed === 1 ? '' : 's'}{' '}unconfirmed — a name match
          nobody has approved yet.
        </p>
      )}

      {!rows && !err && <p className="text-xs text-fg-subtle">Loading…</p>}
      {err && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{err}</div>
      )}

      {rows && (
        <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[70vh]">
          <table className="w-full text-xs whitespace-nowrap">
            <thead className="bg-card sticky top-0 z-10 [&_th]:bg-card">
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                <th className="px-3 py-1.5 font-medium text-left" />{/* Analyse */}
                <th className="px-3 py-1.5 font-medium text-left">Name</th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="Positions in the Fixed portfolio — the ISINs this pairing can reach. Blank = not linked to one.">
                  ISINs
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's own cumulatief_rendement for the year — each month's investment return compounded. It accounts for deposits and withdrawals, so it is not just (end value ÷ start value − 1).">
                  YTD
                </th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's rendement from its newest row — the current (latest) month, a different window from the year. Not a rival YTD.">
                  Current month
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {view.map((r) => {
                const isOpen = open === r.dynamic_portefeuille;
                return (
                  <Fragment key={r.dynamic_portefeuille}>
                    <tr onClick={() => void expand(r.dynamic_portefeuille)}
                      className="hover:bg-accent-500/10 transition-colors cursor-pointer">
                      {/* Analyse, leftmost. Describes the FIXED portfolio (composition +
                          attribution), which is why it needs `fixed_portfolio_id` and an unlinked
                          row cannot offer it. stopPropagation so it does not also toggle the row. */}
                      <td className="px-3 py-1.5 whitespace-nowrap">
                        <div className="flex items-stretch gap-1">
                          {r.fixed_portfolio_id != null && (
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                setAnalyse({ id: r.fixed_portfolio_id!, name: r.name });
                              }}
                              className="inline-flex items-center text-[10px] px-1.5 py-0.5 rounded border border-neutral-800/40 text-fg-subtle hover:bg-overlay/5 hover:text-fg"
                            >
                              Analyse
                            </button>
                          )}
                          {r.fixed_portfolio_id != null && (
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                setPfFund({ id: r.fixed_portfolio_id!, name: r.name });
                              }}
                              title="Fundamental — the whole portfolio's blended owner earnings & price steadiness"
                              className="inline-flex items-center text-[10px] px-1.5 py-0.5 rounded border border-neutral-800/40 text-fg-subtle hover:bg-overlay/5 hover:text-fg"
                            >
                              Fundamental
                            </button>
                          )}
                          {/* Re-scan just this portfolio (a few seconds). stopPropagation so it does
                              not also toggle the row's holdings. `items-stretch` on the wrapper keeps
                              this exactly the height of the Analyse button beside it. */}
                          <button
                            onClick={(e) => { e.stopPropagation(); void refreshOne(r.dynamic_portefeuille); }}
                            disabled={refreshingRows.has(r.dynamic_portefeuille)}
                            title="Re-scan this portfolio's AIRS Rendement + Vermogensoverzicht now."
                            aria-label="Refresh this portfolio"
                            className="inline-flex items-center justify-center px-1.5 py-0.5 rounded border border-neutral-800/40 text-fg-subtle hover:bg-overlay/5 hover:text-accent-300 disabled:opacity-50 disabled:cursor-wait"
                          >
                            <RefreshIcon spinning={refreshingRows.has(r.dynamic_portefeuille)} size={12} />
                          </button>
                        </div>
                      </td>
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
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                        {r.isins ?? '—'}
                        {r.isins != null && (
                          <Provenance source="airs_model" kind="formula" note="position count"
                            how="a count of the positions in the paired Fixed portfolio" />
                        )}
                      </td>
                      <td className={`px-3 py-1.5 text-right font-mono font-semibold ${tone(r.ytd_pct)}`}>
                        {pct(r.ytd_pct)}
                        <Provenance source="airs_att" asOf={r.as_of} kind="copied"
                          note="cumulatief_rendement — AIRS's own compounded year, net of deposit/withdrawal timing" />
                      </td>
                      <td className={`px-3 py-1.5 text-right font-mono ${tone(r.latest_month_pct)}`}>
                        {pct(r.latest_month_pct)}
                        <Provenance source="airs_att" asOf={r.as_of} kind="copied"
                          note="rendement — AIRS's return for the most recent month" />
                      </td>
                    </tr>
                    {isOpen && (
                      <tr>
                        <td colSpan={5} className="px-3 py-3 bg-inset">
                          <Holdings d={detail[r.dynamic_portefeuille]} i={isins[r.dynamic_portefeuille]}
                            portefeuille={r.dynamic_portefeuille} onOverride={refreshIsins} />
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
      {pfFund && (
        <OwnerEarningsModal portfolioId={pfFund.id} name={pfFund.name}
          onClose={() => setPfFund(null)} />
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
/** The smart asset-class label with its palette dot — shares the allocation bar's colours so the
 *  Class column and the bar read as one system. `—` when the row has no bucket (unresolved).
 *
 *  For an ISIN-bearing row it is EDITABLE: an overlaid `<select>` lets a user pin the Class (or
 *  pick "Auto" to revert to the calculated one). The choice is persisted per ISIN and beats the
 *  calculation forever; an overridden badge wears a ring on its dot. Cash (no ISIN) is read-only. */
function BucketBadge({ bucket, isin, overridden, onOverride }: {
  bucket?: string | null; isin?: string | null; overridden?: boolean | null;
  onOverride?: (isin: string, bucket: string | null) => void | Promise<void>;
}) {
  const [saving, setSaving] = useState(false);
  if (!bucket) return <span className="text-fg-faint">—</span>;

  const dot = (
    <span className="w-2 h-2 rounded-sm inline-block shrink-0"
      style={{ backgroundColor: allocColor(bucket), boxShadow: overridden ? '0 0 0 1.5px var(--color-bg-page), 0 0 0 2.5px currentColor' : undefined }} />
  );
  // Read-only for cash / unresolved rows (no ISIN to pin).
  if (!isin || !onOverride) {
    return (
      <span className="inline-flex items-center gap-1.5 whitespace-nowrap">
        {dot}<span className="text-fg-soft">{bucketLabel(bucket)}</span>
      </span>
    );
  }
  return (
    <span className={`relative inline-flex items-center gap-1.5 whitespace-nowrap rounded px-1 -mx-1 ${saving ? 'opacity-50' : 'hover:bg-overlay/5'}`}
      title={overridden ? 'Class manually set — pick “Auto” to revert to the calculated class.' : 'Auto-classified — click to override the Class.'}>
      {dot}
      <span className="text-fg-soft">{bucketLabel(bucket)}</span>
      {overridden && <span className="text-accent-400 text-[9px] leading-none">✎</span>}
      {/* The picker overlays the whole cell, invisible, so the badge stays the visible affordance. */}
      <select
        aria-label="Set Class"
        value={overridden ? bucket : ''}
        disabled={saving}
        onClick={(e) => e.stopPropagation()}
        onChange={async (e) => {
          const v = e.target.value || null;   // '' = Auto (clear the override)
          setSaving(true);
          try { await onOverride(isin, v); } finally { setSaving(false); }
        }}
        className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
      >
        <option value="">Auto (calculated)</option>
        {BUCKET_ORDER.map((b) => <option key={b} value={b}>{bucketLabel(b)}</option>)}
      </select>
    </span>
  );
}

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
function SegmentHeader({ s, asOf, holdings, onAnalyse, onFundamental }: {
  s: AirsHoldingSegment; asOf?: string | null;
  holdings: { isin: string; weight: number }[];
  onAnalyse: (v: ModalTarget) => void;
  onFundamental: (v: ModalTarget) => void;
}) {
  const etfPct = s.value_eur && s.etf_value_eur ? (100 * s.etf_value_eur) / s.value_eur : 0;
  const partial = s.value_eur != null && s.priced_value_eur != null
    && Math.abs(s.value_eur - s.priced_value_eur) > 1;
  const label = bucketLabel(s.asset_class) || 'Group';
  const target: ModalTarget = { name: label, basket: { holdings, label } };
  const cls = 'text-[10px] px-1.5 py-0.5 rounded border border-neutral-800/40 text-fg-subtle hover:bg-overlay/5 hover:text-accent-300 whitespace-nowrap';
  return (
    <tr className="bg-overlay/[0.03] border-t border-neutral-800/40">
      <td className="px-2 py-1">
        {holdings.length > 0 && (
          <div className="flex items-center gap-1">
            <button type="button" onClick={() => onAnalyse(target)} className={cls}
              title={`Analyse the ${label} sleeve — composition, return vs benchmark, returns/risk`}>Analyse</button>
            <button type="button" onClick={() => onFundamental(target)} className={cls}
              title={`Fundamentals of the ${label} sleeve — blended owner earnings & price steadiness`}>Fundamental</button>
          </div>
        )}
      </td>
      <td className="px-3 py-1 font-semibold text-fg-strong">
        {label}
        <span className="text-fg-faint font-normal ml-2">
          {s.holdings} holding{s.holdings === 1 ? '' : 's'}
          <Provenance source="airs_volk" asOf={asOf} kind="formula" note="holdings in this segment"
            how="a count of the segment's holdings" />
          {etfPct >= 0.5 && (
            <span title={`${eur(s.etf_value_eur)} of this segment is held via ETFs. An equity ETF is Equity and a bond ETF is Bonds — the wrapper does not change the exposure.`}>
              {' · '}{etfPct.toFixed(0)}% via ETFs
              <Provenance source="airs_volk" asOf={asOf} kind="formula" note="ETF share of the segment"
                how="ETF value ÷ segment value (both EUR)" />
            </span>
          )}
        </span>
      </td>
      {/* ⚠ ONE CELL PER COLUMN. The table is [Fundamental] · Fund · ISIN · Class · Sector · Country
          · Region · Ccy · Weight · Return — ten (the leading Fundamental cell is emitted above). An
          extra blank here shifts every figure one column right, which is silent: a weight renders
          perfectly well under "Ccy". */}
      <td />{/* ISIN */}
      <td />{/* Class */}
      <td />{/* Sector */}
      <td />{/* Country */}
      <td />{/* Region */}
      <td />{/* Ccy */}
      <td className="px-3 py-1 text-right font-mono text-fg-subtle">
        {s.weight_pct == null ? '—' : `${s.weight_pct.toFixed(2)}%`}
        <Provenance source="airs_volk" asOf={asOf} kind="formula" note="segment weight"
          how="segment value ÷ book total (summed from the AIRS VOLK position values)" />
      </td>
      <td className={`px-3 py-1 text-right font-mono font-semibold ${tone(s.return_pct)}`}
        title={partial
          ? `Weighted-average return of this segment's priced holdings (${eur(s.priced_value_eur)} of ${eur(s.value_eur)}). The rest has no opening value — not held when the year opened — so its return is undefined, not zero.`
          : 'The weighted average of the holdings\' returns, each weighted by its current value (Weight). Price return only — no income, not flow-aware.'}>
        {s.return_pct == null ? '—' : pct(s.return_pct)}
        {partial && s.return_pct != null && <span className="text-warn-400 ml-1">*</span>}
        <Provenance source="airs_volk" asOf={asOf} kind="formula" note="segment weighted return"
          how="Σ(weight × return) ÷ Σweight over the segment's priced holdings (weight = current value)." />
      </td>
    </tr>
  );
}

/** Leading-cell triggers for the per-instrument fundamentals + risk views. Only an instrument with
 *  an ISIN can be looked up — cash and unresolved lines have none, so they get a blank cell rather
 *  than dead buttons. */
function FundamentalCell({ isin, name, onAnalyse, onFundamental }: {
  isin?: string | null; name?: string | null;
  onAnalyse: (v: ModalTarget) => void;
  onFundamental: (v: ModalTarget) => void;
}) {
  if (!isin) return <td className="px-2 py-1.5" />;
  const nm = name ?? isin;
  // A single stock is a portfolio-of-one — the same Analyse view (composition, return, risk) as a
  // group, just concentrated 100% in one name.
  const analyseTarget: ModalTarget = { name: nm, basket: { holdings: [{ isin, weight: 1, name: nm }], label: nm } };
  const cls = 'text-[10px] px-1.5 py-0.5 rounded border border-neutral-800/40 text-fg-subtle hover:bg-overlay/5 hover:text-accent-300 whitespace-nowrap';
  return (
    <td className="px-2 py-1.5">
      <div className="flex items-center gap-1">
        <button type="button" onClick={() => onAnalyse(analyseTarget)} className={cls}
          title="Analyse — composition, return vs benchmark, and returns/risk over 2/4/8 years">
          Analyse
        </button>
        <button type="button" onClick={() => onFundamental({ name: nm, isin })} className={cls}
          title="Fundamental — is this company fundamentally good? (owner earnings + price steadiness)">
          Fundamental
        </button>
      </div>
    </td>
  );
}

function Holdings({ d, i, portefeuille, onOverride }: {
  d?: AirsAccountDetail; i?: AirsAccountIsins;
  portefeuille?: string; onOverride?: (p: string) => void | Promise<void>;
}) {
  const [fund, setFund] = useState<ModalTarget | null>(null);
  const [perf, setPerf] = useState<ModalTarget | null>(null);
  // Persist a manual Class pin (or clear it → Auto), then re-fetch this account so the row
  // re-groups under its new bucket.
  const setBucket = useCallback(async (isin: string, bucket: string | null) => {
    await apiFetch(`${API_URL}/api/airs/asset-bucket-override`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ isin, bucket }),
    });
    if (portefeuille && onOverride) await onOverride(portefeuille);
  }, [portefeuille, onOverride]);
  if (!d) return <p className="text-[11px] text-fg-subtle">Loading holdings…</p>;
  if (!d.rows?.length) return <p className="text-[11px] text-fg-subtle">No holdings snapshot stored.</p>;
  const byName = new Map((i?.rows ?? []).map((r) => [r.holding_name, r]));
  const mismatches = (i?.rows ?? []).filter((r) => r.verdict === 'price_mismatch').length;

  // Grouped by the CALCULATED Class (the `bucket`, incl. manual overrides), in the backend's order
  // (Cash and Unclassified last — they are what is left). A holding whose class we do not know
  // still renders: it falls in the trailing ungrouped block rather than vanishing from a table
  // that is supposed to account for the whole book.
  // ⚠ AIRS bills one instrument on SEVERAL lines — BUS_Neutraal lists "6,5% Rabobank Certificaten
  // 14-perp." at 1.64% AND 0.01%. The ISIN/segment side already dedupes (resolve_account_isins), so
  // merge by name here too, summing weight + values, or the same holding shows as two rows. The
  // return % is identical for two lines of one instrument (same price move), so keep the first's.
  const merged = new Map<string, NonNullable<AirsAccountDetail['rows']>[number]>();
  for (const r of d.rows ?? []) {
    const cur = merged.get(r.holding_name);
    if (!cur) { merged.set(r.holding_name, { ...r }); continue; }
    const add = (a?: number | null, b?: number | null) => (a == null && b == null ? a : (a ?? 0) + (b ?? 0));
    cur.weight = add(cur.weight, r.weight);
    cur.quantity = add(cur.quantity, r.quantity);
    cur.current_value_eur = add(cur.current_value_eur, r.current_value_eur);
    cur.start_value_eur = add(cur.start_value_eur, r.start_value_eur);
    cur.ytd_return_eur = add(cur.ytd_return_eur, r.ytd_return_eur);
    cur.fund_result_eur = add(cur.fund_result_eur, r.fund_result_eur);
    cur.fx_result_eur = add(cur.fx_result_eur, r.fx_result_eur);
  }
  const all = [...merged.values()];
  const segs = i?.segments ?? [];
  const classOf = (name: string) => byName.get(name)?.bucket ?? null;
  const ordered: [AirsHoldingSegment | null, typeof all][] = segs.length
    ? segs
      .map((s) => [s, all.filter((r) => classOf(r.holding_name) === s.asset_class)] as
        [AirsHoldingSegment, typeof all])
      .filter(([, g]) => g.length)
    : [[null, all]];
  const grouped = new Set(ordered.flatMap(([, g]) => g.map((r) => r.holding_name)));
  const rest = all.filter((r) => !grouped.has(r.holding_name));
  if (rest.length) ordered.push([null, rest]);
  // The top TOTAL row: all weights summed (≈100%), and the portfolio return as the WEIGHTED
  // AVERAGE of the individual positions' returns — Σ(weightᵢ · returnᵢ) / Σweightᵢ, using the
  // displayed Weight and per-row return, so the total is exactly what the rows below say (and the
  // same basis each bucket uses). A row with no opening value (undefined return) drops out.
  const totalWeight = all.reduce((s, r) => s + (r.weight ?? 0), 0);
  const retRows = all.filter((r) => r.ytd_return_pct != null && r.weight != null);
  const wSum = retRows.reduce((s, r) => s + (r.weight ?? 0), 0);
  const totalReturn = wSum !== 0
    ? retRows.reduce((s, r) => s + (r.weight ?? 0) * (r.ytd_return_pct ?? 0), 0) / wSum
    : null;
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
          <thead className="bg-card sticky top-0 z-20 [&_th]:bg-card">
            <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
              <th className="px-2 py-1.5 font-medium text-left" />{/* Fundamental */}
              <th className="px-3 py-1.5 font-medium text-left">Fund</th>
              <th className="px-3 py-1.5 font-medium text-left"
                title="From the Fixed portfolio, then price-checked against that instrument's own close. ⚠ = the price disagrees; ? = no series, so nothing confirms the name.">
                ISIN
              </th>
              <th className="px-3 py-1.5 font-medium text-left"
                title="Smart asset-class label — Stocks · Stock ETF · Bonds · Alternatives · Cash · Unclassified (genuinely unsure). AIRS's own class first, then the instrument's grid data and name.">
                Class
              </th>
              <th className="px-3 py-1.5 font-medium text-left" title="The instrument's own yfinance sector. A fund is opaque, so it reads “—”.">Sector</th>
              <th className="px-3 py-1.5 font-medium text-left">Country</th>
              <th className="px-3 py-1.5 font-medium text-left" title="MSCI region from the instrument's yfinance geo. ⚠ For an ETF this describes its listing, not what it holds.">Region</th>
              <th className="px-3 py-1.5 font-medium text-left">Ccy</th>
              <th className="px-3 py-1.5 font-medium text-right">Weight</th>
              <th className="px-3 py-1.5 font-medium text-right">Return</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {/* TOTAL — all weights summed and the value-weighted price return, at the top. */}
            <tr className="bg-overlay/[0.04] font-semibold border-b border-neutral-800/40">
              <td className="px-2 py-1.5" />{/* Fundamental */}
              <td className="px-3 py-1.5 text-fg-strong" colSpan={7}>
                Total · {all.length} holding{all.length === 1 ? '' : 's'}
              </td>
              <td className="px-3 py-1.5 text-right font-mono text-fg-strong">{(totalWeight * 100).toFixed(2)}%</td>
              <td className={`px-3 py-1.5 text-right font-mono ${totalReturn == null ? 'text-fg-faint' : tone(totalReturn)}`}
                title="Weighted average of the positions' returns — Σ(weight × return) ÷ Σweight over holdings with an opening value (the same basis each bucket uses). Price return only — not flow-aware.">
                {totalReturn == null ? '—' : pct(totalReturn * 100)}
              </td>
            </tr>
            {ordered.map(([seg, group]) => {
              const groupHoldings = group
                .map((r) => ({ isin: byName.get(r.holding_name)?.isin, weight: r.weight ?? 0, name: r.holding_name }))
                .filter((h): h is { isin: string; weight: number; name: string } => !!h.isin);
              return (
              <Fragment key={seg?.asset_class ?? 'x'}>
                {seg && <SegmentHeader s={seg} asOf={d.as_of} holdings={groupHoldings}
                  onAnalyse={setPerf} onFundamental={setFund} />}
                {group.map((r, n) => (
              <tr key={`${r.holding_name}-${n}`} className="hover:bg-overlay/[0.02]">
                <FundamentalCell isin={byName.get(r.holding_name)?.isin} name={r.holding_name} onAnalyse={setPerf} onFundamental={setFund} />
                <td className="px-3 py-1.5 text-fg-soft pl-6">{r.holding_name}</td>
                <td className="px-3 py-1.5"><IsinCell r={byName.get(r.holding_name)} /></td>
                <td className="px-3 py-1.5"><BucketBadge bucket={byName.get(r.holding_name)?.bucket}
                  isin={byName.get(r.holding_name)?.isin} overridden={byName.get(r.holding_name)?.bucket_overridden}
                  onOverride={setBucket} /></td>
                <td className="px-3 py-1.5 text-fg-subtle">{byName.get(r.holding_name)?.sector || '—'}</td>
                <td className="px-3 py-1.5 text-fg-subtle">{byName.get(r.holding_name)?.country || '—'}</td>
                <td className="px-3 py-1.5 text-fg-subtle">{byName.get(r.holding_name)?.region || '—'}</td>
                <td className="px-3 py-1.5 font-mono text-fg-muted">{r.currency || '—'}</td>
                <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                  {r.weight != null ? `${(r.weight * 100).toFixed(2)}%` : '—'}
                  {r.weight != null && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="copied"
                      note="Weging — AIRS's own position weight" />
                  )}
                </td>
                {/* ⚠ A dash, never 0%. No opening value = the return is UNDEFINED, not flat. */}
                <td className={`px-3 py-1.5 text-right font-mono ${tone(r.ytd_return_pct)}`}
                  title={r.ytd_return_pct == null
                    ? 'No opening value — not held when the year opened (or a cash line). Its return is undefined, not zero.'
                    : undefined}>
                  {r.ytd_return_pct == null ? '—' : pct(r.ytd_return_pct * 100)}
                  {r.ytd_return_pct != null && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="formula" note="Resultaat (price return)"
                      how={`Now ÷ Start − 1 = ${eur(r.current_value_eur)} ÷ ${eur(r.start_value_eur)} − 1 = ${pct(r.ytd_return_pct * 100)}`} />
                  )}
                </td>
              </tr>
                ))}
              </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
      {fund && (
        <OwnerEarningsModal isin={fund.isin} basket={fund.basket} portfolioId={fund.portfolioId}
          name={fund.name} onClose={() => setFund(null)} />
      )}
      {perf?.basket && (
        <PortfolioAnalysisModal name={perf.name} basket={perf.basket} onClose={() => setPerf(null)} />
      )}
    </div>
  );
}
