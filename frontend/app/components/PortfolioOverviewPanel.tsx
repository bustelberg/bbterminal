'use client';

import { Fragment, useCallback, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { dialog } from '../../lib/dialog';
import { Provenance } from '../../lib/provenance';
import PortfolioAnalysisModal from './portfolios/PortfolioAnalysisModal';
import OwnerEarningsModal from './portfolios/OwnerEarningsModal';
import { type Basket } from './portfolios/PerformanceModal';
import { allocColor, bucketLabel, BUCKET_ORDER } from './portfolios/allocationColors';
import { groupStats, startBasis, type GroupStats } from './portfolios/startWeights';

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

  // Fetch ONE account's holdings + ISIN resolution into the caches. Split out of `expand` because
  // a refresh has to be able to re-fetch a row that is ALREADY open: clearing the caches without
  // re-fetching left the open row on "Loading holdings…" for ever, since nothing re-requests until
  // the next click — the row had to be collapsed and re-expanded by hand to recover.
  const loadDetail = useCallback(async (p: string) => {
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
  }, []);

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
      // ⚠ Dropping the cache is only half of it. An OPEN row re-renders straight into
      // "Loading holdings…" and stays there, because only a click re-requests — so re-fetch here.
      if (open === portefeuille) await loadDetail(portefeuille);
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
      if (open) await loadDetail(open);   // same trap as refreshOne — an open row must re-fetch
    } catch (e) {
      setRefreshMsg({ text: e instanceof Error ? e.message : String(e), kind: 'error' });
    } finally {
      setRefreshingAll(false);
    }
  };

  const expand = async (p: string) => {
    setOpen(open === p ? null : p);
    if (detail[p] || open === p) return;
    await loadDetail(p);
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
                        {/* The name is the FIXED side's, reached through a pairing — so its card
                            states the pairing, which for 27 of 28 rows is an unapproved guess. */}
                        <Provenance source="airs_model" kind={r.fixed_name ? 'formula' : 'copied'}
                          note={r.fixed_name ? 'name — from the Fixed portfolio this book is paired with' : 'name — the AIRS book itself; no Fixed portfolio paired'}
                          how={r.fixed_name
                            ? `${r.dynamic_portefeuille} paired with ${r.fixed_name}${
                              r.link_source === 'guess'
                                ? ` by a name match nobody has approved (${r.link_reason ?? 'name match'}) — ⚠ the risk variants of a strategy hold the same instruments, so no other column would reveal a wrong pairing.`
                                : ' by a confirmed link.'}`
                            : undefined} />
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

/** HOW an ISIN came to be on this row — the name match that proposed it AND the price check that
 *  did or did not confirm it. `verdict` is the field that matters; `name_score` alone is not a pass
 *  (a fund's Acc/Inc share classes have near-identical names and different ISINs). */
function isinHow(r: NonNullable<AirsAccountIsins['rows']>[number]): string {
  // The book states its own ISIN (AIRS's `ISIN-code`, since 2026-07-23) — nothing is inferred, so
  // the card says so plainly. The price check here is no longer testing a pairing; it tests OUR
  // price series for that instrument, which is a check we could not make before.
  if (r.isin_source === 'book') {
    const checked = r.verdict === 'ok'
      ? `Our own price series agrees: €${r.implied_price_eur}/unit implied here vs €${r.our_price_eur} (ratio ${r.price_ratio}).`
      : r.verdict === 'price_mismatch'
        ? `⚠ Our own price series DISAGREES: €${r.implied_price_eur}/unit implied here vs €${r.our_price_eur} for ${r.our_instrument ?? 'our instrument'} (ratio ${r.price_ratio}). The identity is AIRS's, so this points at OUR listing for it, not at the match.`
        : 'We hold no price series for it, so nothing cross-checks our side.';
    return `read straight off the holding — AIRS's own ISIN-code column. No name matching was involved. ${checked}`;
  }
  if (r.isin_overridden) {
    // ⚠ A pin is an IDENTITY, not a verification — so the card leads with who decided it and
    // still reports what the price said, exactly as on a matched row.
    const checked = r.verdict === 'ok'
      ? `The price then CONFIRMED it independently: €${r.implied_price_eur}/unit implied here vs €${r.our_price_eur} for that ISIN (ratio ${r.price_ratio}).`
      : r.verdict === 'price_mismatch'
        ? `⚠ The price CONTRADICTS it: €${r.implied_price_eur}/unit implied here vs €${r.our_price_eur} (ratio ${r.price_ratio}). Check the ISIN.`
        : 'We hold no price series for it, so nothing confirms it.';
    return `set by hand${r.isin_override_note ? ` — ${r.isin_override_note}` : ''}. The Fixed portfolio has no position for this holding, so no match could produce an ISIN. ${checked}`;
  }
  if (r.verdict === 'unmatched') {
    return `the Fixed portfolio has no position for this holding. The only one left was “${
      r.rejected_fonds ?? '—'}” (${r.rejected_isin ?? '—'}), and both the name (score ${
      r.name_score}) and the price say that is a different instrument, so it was refused. The stored model is out of date: re-scan the Fixed portfolio.`;
  }
  const matched = `matched to the Fixed portfolio's “${r.model_fonds ?? '—'}” by name${
    r.name_score != null ? ` (score ${r.name_score})` : ''}`;
  if (r.verdict === 'ok') {
    return `${matched}, then confirmed on price: €${r.implied_price_eur}/unit implied here vs €${r.our_price_eur} for that ISIN (ratio ${r.price_ratio}).`;
  }
  if (r.verdict === 'price_mismatch') {
    return `${matched}, then CONTRADICTED on price: €${r.implied_price_eur}/unit implied here vs €${r.our_price_eur} for ${r.our_instrument ?? 'that ISIN'} (ratio ${r.price_ratio}).`;
  }
  return `${matched}. We hold no price series for it, so NOTHING confirms the match — not the same as a pass.`;
}

function IsinCell({ r, onPin }: {
  r: NonNullable<AirsAccountIsins['rows']>[number] | undefined;
  /** Supply/clear this holding's ISIN by hand. Absent = read-only (a cash line has nothing to pin). */
  onPin?: (holdingName: string, current?: string | null) => void | Promise<void>;
}) {
  // ⚠ NOT a bare dash. A blank in this column reads as "this line has no ISIN", i.e. cash — and
  // this is the opposite: a real instrument the model has no position for. Say which, and make it
  // the affordance for fixing it, since no re-match can ever find an ISIN that is not in the data.
  if (r?.verdict === 'unmatched') {
    return (
      <button type="button" disabled={!onPin}
        onClick={(e) => { e.stopPropagation(); void onPin?.(r.holding_name, null); }}
        title={`The Fixed portfolio has no position for this holding. The only one left was “${r.rejected_fonds ?? '—'}” (${r.rejected_isin ?? '—'}), which the name and the price both say is a different instrument, so it was refused rather than published as this holding's ISIN. Re-scan the Fixed portfolio, or click to set the ISIN by hand.`}
        className="text-warn-500 whitespace-nowrap underline decoration-dotted underline-offset-2 hover:text-warn-400 disabled:no-underline">
        no model position
      </button>
    );
  }
  if (!r?.isin) return <span className="text-fg-faint">—</span>;
  const mismatch = r.verdict === 'price_mismatch';
  const unpriced = r.verdict === 'unpriced';
  return (
    <span className="font-mono whitespace-nowrap">
      <span className={mismatch ? 'text-neg-400' : unpriced ? 'text-fg-muted' : 'text-fg-soft'}>
        {r.isin}
      </span>
      {/* A pinned identity must never read as a match. Same ✎ the Class override wears. */}
      {r.isin_overridden && (
        <button type="button" disabled={!onPin}
          onClick={(e) => { e.stopPropagation(); void onPin?.(r.holding_name, r.isin); }}
          title="ISIN set by hand — the Fixed portfolio has no position for this holding. Click to change it, or clear it to go back to matching."
          className="text-accent-400 text-[9px] leading-none ml-1 align-middle hover:text-accent-300">
          ✎
        </button>
      )}
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
function SegmentHeader({ s, asOf, holdings, stats, onAnalyse, onFundamental }: {
  s: AirsHoldingSegment; asOf?: string | null;
  holdings: { isin: string; weight: number }[];
  /** ⚠ EVERY FIGURE ON THIS ROW COMES FROM THE HOLDINGS UNDER IT (`groupStats`), not from the
   *  backend's own per-segment numbers — it computes those over a different row set, and a header
   *  that disagrees with the lines beneath it is a second source of truth with no way to tell
   *  which is right. `s` is used only for the label. */
  stats: GroupStats;
  onAnalyse: (v: ModalTarget) => void;
  onFundamental: (v: ModalTarget) => void;
}) {
  const { etfPct, partial } = stats;
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
          {stats.holdings} holding{stats.holdings === 1 ? '' : 's'}
          <Provenance source="airs_volk" asOf={asOf} kind="formula" note="holdings in this segment"
            how="a count of the rows listed below this header — not a separate figure." />
          {etfPct >= 0.5 && (
            <span title={`${eur(stats.valueEur * (etfPct / 100))} of this segment is held via ETFs. An equity ETF is Equity and a bond ETF is Bonds — the wrapper does not change the exposure.`}>
              {' · '}{etfPct.toFixed(0)}% via ETFs
              <Provenance source="airs_volk" asOf={asOf} kind="formula" note="ETF share of the segment"
                how="the value of the fund-wrapped rows below ÷ this segment's total value (both EUR)." />
            </span>
          )}
        </span>
      </td>
      {/* ⚠ ONE CELL PER COLUMN. The table is [Fundamental] · Fund · ISIN · Class · Sector · Country
          · Region · Ccy · Start wt · Weight · Return — eleven (the leading Fundamental cell is
          emitted above). An extra blank here shifts every figure one column right, which is
          silent: a weight renders perfectly well under "Ccy". */}
      <td />{/* ISIN */}
      <td />{/* Class */}
      <td />{/* Sector */}
      <td />{/* Country */}
      <td />{/* Region */}
      <td />{/* Ccy */}
      <td className="px-3 py-1 text-right font-mono text-fg-subtle">
        {stats.startWeightPct == null ? '—' : `${stats.startWeightPct.toFixed(2)}%`}
        {stats.startWeightPct != null && (
          <Provenance source="airs_volk" asOf={asOf} kind="formula" note="segment start weight"
            how={`the Start wt column of the ${stats.holdings} row${stats.holdings === 1 ? '' : 's'} below, added up. Equivalently this segment's opening value ÷ the book's. Weighting each segment's return by it gives the book's return.`} />
        )}
      </td>
      <td className="px-3 py-1 text-right font-mono text-fg-subtle">
        {stats.weightPct == null ? '—' : `${stats.weightPct.toFixed(2)}%`}
        {stats.weightPct != null && (
          <Provenance source="airs_volk" asOf={asOf} kind="formula" note="segment weight, as of today"
            how={`the Weight column of the ${stats.holdings} row${stats.holdings === 1 ? '' : 's'} below, added up (AIRS's own Weging per position).`} />
        )}
      </td>
      <td className={`px-3 py-1 text-right font-mono font-semibold ${tone(stats.returnPct)}`}
        title={partial
          ? `Start-weighted value change of this segment's priced holdings (${eur(stats.pricedValueEur)} of ${eur(stats.valueEur)}). The rest has no opening value — not held when the year opened — so its return is undefined, not zero.`
          : 'The start-weighted value change — Σ current ÷ Σ start − 1, each holding weighted by its OPENING value. Price return only — no income, not flow-aware.'}>
        {stats.returnPct == null ? '—' : pct(stats.returnPct)}
        {partial && stats.returnPct != null && <span className="text-warn-400 ml-1">*</span>}
        {/* ONE formula, in the two columns the reader can see. `contributionPct` is Σ(Start wt ×
            Return) over the rows below; dividing by the segment's own Start wt turns that
            book-level figure into the segment's own return. */}
        <Provenance source="airs_volk" asOf={asOf} kind="formula" note="segment return"
          how={stats.returnPct == null || stats.contributionPct == null || !stats.startWeightPct
            ? 'no holding here has an opening value, so this segment has no return to state.'
            // ⚠ pp, not %. The contribution is a share OF THE BOOK's return; printing it "+5.46%"
            // beside the segment's own "+6.60%" reads as two rival returns.
            : `Σ (each row's Start wt × its Return) = ${stats.contributionPct >= 0 ? '+' : ''}${stats.contributionPct.toFixed(2)}pp, which is what this segment added to the book's return. As the segment's OWN return that is ÷ its ${stats.startWeightPct.toFixed(2)}% Start wt = ${pct(stats.returnPct)}.${partial ? ' Priced rows only — the rest had no opening value, so their return is undefined, not zero.' : ''}`} />
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
  // Supply this holding's ISIN by hand. The ONLY route when the model has no position for it —
  // no matching can find an ISIN that is not in the data. Keyed by name, so it fixes every book
  // holding the same instrument at once; an empty answer clears the pin.
  const pinIsin = useCallback(async (holdingName: string, current?: string | null) => {
    const v = await dialog.prompt(
      `The Fixed portfolio has no position for “${holdingName}”, so its ISIN has to be supplied by hand. It is still price-checked afterwards, and applies to every portfolio holding this instrument. Leave empty to clear.`,
      { title: 'Set ISIN', defaultValue: current ?? '', placeholder: 'e.g. IE000OEF25S1' });
    if (v == null) return;                       // cancelled — not the same as cleared
    const res = await apiFetch(`${API_URL}/api/airs/holding-isin-override`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ holding_name: holdingName, isin: v.trim() || null }),
    });
    if (!res.ok) {
      const b = (await res.json().catch(() => null)) as { detail?: string } | null;
      await dialog.alert(b?.detail || `Could not save the ISIN (HTTP ${res.status}).`);
      return;
    }
    if (portefeuille && onOverride) await onOverride(portefeuille);
  }, [portefeuille, onOverride]);
  if (!d) return <p className="text-[11px] text-fg-subtle">Loading holdings…</p>;
  if (!d.rows?.length) return <p className="text-[11px] text-fg-subtle">No holdings snapshot stored.</p>;
  const byName = new Map((i?.rows ?? []).map((r) => [r.holding_name, r]));
  const mismatches = (i?.rows ?? []).filter((r) => r.verdict === 'price_mismatch').length;
  // How many identities are AIRS's OWN (exact) vs recovered by matching a fund name. Stated once
  // per table rather than badged on every row: during the changeover it is the whole table that
  // is one or the other, and it is what says whether a book still needs re-scanning.
  const named = (i?.rows ?? []).filter((r) => r.isin_source === 'model').length;
  const exact = (i?.rows ?? []).filter((r) => r.isin_source === 'book').length;

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
  // The top TOTAL row: all weights summed (≈100%), and the portfolio's START-WEIGHTED value change
  // — Σcurrent ÷ Σstart − 1, equivalently each position's return weighted by its OPENING value.
  // ⚠ NOT Σ(displayed-weight × return): the Weight column is today's value share, and weighting by
  // it lets a big winner (up +148%, now 3× its share) dominate — that read +56.11% on a book whose
  // true return was +41.98% (≈ the +43.08% flow-aware book figure). Start-weighting is the honest
  // number and the one that lines up with `cumulatief_rendement`. Undefined-return rows drop out.
  const totalWeight = all.reduce((s, r) => s + (r.weight ?? 0), 0);
  // The Total AND the Start-wt column come from ONE call: Σ(start weight × return) is
  // Σ(current−start) ÷ Σstart identically, and computing the two separately is exactly how a
  // column and the figure it is supposed to explain drift apart. See `startWeights.ts`.
  const basis = startBasis(all);
  const { priced: pricedRows, startSum, nowSum, totalReturn, weightOf: startWeight } = basis;
  return (
    <div className="space-y-2">
      {/* ⚠ Stated BEFORE the numbers — a reader coming from a weights table will try to add
          these up and conclude something is broken. One line; the reasoning is on hover. */}
      <p className="text-[10px] text-fg-faint leading-relaxed"
        title="Price returns: AIRS restates each opening value to the current quantity, so a purchase is not a gain. The portfolio's own figure is flow-aware and includes income, which no price return carries, so these do not sum to it.">
        {exact > 0 && named === 0
          ? 'ISINs are AIRS’s own, per holding. '
          : exact > 0
            ? `${exact} ISIN${exact === 1 ? '' : 's'} from AIRS, ${named} matched by name. `
            : 'ISIN and fund name from the Fixed portfolio. '}
        Other columns from AIRS. Weight the Return column by{' '}
        <strong>Start wt</strong>, not Weight, to reach the Total. Price returns do not sum to the
        portfolio&apos;s{' '}
        <span className={`font-mono ${tone(d.ytd_pct)}`}>{pct(d.ytd_pct)}</span>, which is
        flow-aware and includes <span className="font-mono">{eur(d.income_eur)}</span>{' '}of income.
        {mismatches > 0 && (
          <span className="text-neg-400">{' '}{mismatches} ISIN{mismatches === 1 ? '' : 's'}{' '}
            {mismatches === 1 ? 'disagrees' : 'disagree'} with the price.</span>
        )}
        {i?.unmatched_model_positions && i.unmatched_model_positions.length > 0 && (
          <span className="text-fg-muted"
            title="Held by the Fixed portfolio but not by this book: implementation drift.">
            {' '}Not held:{' '}
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
                title="AIRS's own ISIN-code where the book carries one (exact), else matched by name to a Fixed portfolio position, else pinned by hand. Always price-checked against that instrument's own close. ⚠ = the price disagrees; ? = no series, so nothing cross-checks it.">
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
              <th className="px-3 py-1.5 font-medium text-right"
                title="Share of the book at the START of the year (Beginwaarde ÷ total Beginwaarde). This is the weight the Return column belongs to: weighting each return by it reproduces the Total exactly. “—” = no opening value, so the holding was not there when the year began.">
                Start wt
              </th>
              <th className="px-3 py-1.5 font-medium text-right"
                title="AIRS's own Weging — today's share of the book. It answers what you hold NOW; it is NOT the weight behind the Return column, because a holding that rose carries a bigger share today than it held while it was rising.">
                Weight
              </th>
              <th className="px-3 py-1.5 font-medium text-right">Return</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {/* TOTAL — all weights summed and the value-weighted price return, at the top. */}
            <tr className="bg-overlay/[0.04] font-semibold border-b border-neutral-800/40">
              <td className="px-2 py-1.5" />{/* Fundamental */}
              <td className="px-3 py-1.5 text-fg-strong" colSpan={7}>
                Total · {all.length} holding{all.length === 1 ? '' : 's'}
                <Provenance source="airs_volk" asOf={d.as_of} kind="formula" note="holdings in the book"
                  how="a count of the AIRS positions, merged where AIRS bills one instrument on several lines." />
              </td>
              <td className="px-3 py-1.5 text-right font-mono text-fg-strong">
                {startSum === 0 ? '—' : '100.00%'}
                <Provenance source="airs_volk" asOf={d.as_of} kind="formula" note="total start weight"
                  how="100% by construction — it is each priced holding's opening value over their sum. The point is the column below it: those shares times the Return column give the Total return exactly." />
              </td>
              <td className="px-3 py-1.5 text-right font-mono text-fg-strong">
                {(totalWeight * 100).toFixed(2)}%
                <Provenance source="airs_volk" asOf={d.as_of} kind="formula" note="total weight"
                  how="Σ of the positions' own AIRS weights (Weging)." />
              </td>
              <td className={`px-3 py-1.5 text-right font-mono ${totalReturn == null ? 'text-fg-faint' : tone(totalReturn)}`}
                title="Start-weighted value change — Σ current ÷ Σ start − 1 over holdings with an opening value (each position's return weighted by its OPENING value, the same basis each bucket uses). Price return only — not flow-aware, so it is close to but not exactly the book's cumulatief_rendement.">
                {totalReturn == null ? '—' : pct(totalReturn * 100)}
                {/* Same formula as a segment row, minus the renormalising step: the Start wt
                    column already sums to 100% here, so there is nothing to divide by. */}
                <Provenance source="airs_volk" asOf={d.as_of} kind="formula" note="start-weighted price return"
                  how={totalReturn == null
                    ? 'no holding has an opening value, so the book has no price return to compute.'
                    : `Σ (each row's Start wt × its Return) over the ${pricedRows.length} holding${pricedRows.length === 1 ? '' : 's'} with an opening value — the Start wt column sums to 100%, so nothing is renormalised. Equivalently ${eur(nowSum)} ÷ ${eur(startSum)} − 1 = ${pct(totalReturn * 100)}. Price return only — not flow-aware, so it is close to but NOT the book's cumulatief_rendement.`} />
              </td>
            </tr>
            {ordered.map(([seg, group]) => {
              const groupHoldings = group
                .map((r) => ({ isin: byName.get(r.holding_name)?.isin, weight: r.weight ?? 0, name: r.holding_name }))
                .filter((h): h is { isin: string; weight: number; name: string } => !!h.isin);
              return (
              <Fragment key={seg?.asset_class ?? 'x'}>
                {seg && <SegmentHeader s={seg} asOf={d.as_of} holdings={groupHoldings}
                  stats={groupStats(group, basis, {
                    weightOfRow: (r) => r.weight,
                    isEtf: (r) => !!byName.get(r.holding_name)?.is_etf,
                  })}
                  onAnalyse={setPerf} onFundamental={setFund} />}
                {group.map((r, n) => {
                  const g = byName.get(r.holding_name);
                  return (
              <tr key={`${r.holding_name}-${n}`} className="hover:bg-overlay/[0.02]">
                <FundamentalCell isin={g?.isin} name={r.holding_name} onAnalyse={setPerf} onFundamental={setFund} />
                <td className="px-3 py-1.5 text-fg-soft pl-6">
                  {r.holding_name}
                  <Provenance source="airs_volk" asOf={d.as_of} kind="copied"
                    note="Fonds — the position's own name in the AIRS book" />
                </td>
                <td className="px-3 py-1.5">
                  <IsinCell r={g} onPin={pinIsin} />
                  {/* ⚠ The ISIN is the one column NOT read off a source — it is INFERRED (a name
                      match, then a price check), so its card carries both steps. */}
                  {g && (g.isin || g.verdict === 'unmatched') && (
                    <Provenance
                      source={g.isin_source === 'book' ? 'airs_volk'
                        : g.isin_overridden ? 'derived' : 'airs_model'}
                      asOf={g.isin_source === 'book' ? d.as_of : undefined}
                      kind={g.isin_source === 'book' ? 'copied' : 'formula'}
                      note={g.isin_source === 'book'
                        ? "ISIN-code — the holding's own ISIN in the AIRS book"
                        : g.isin_overridden
                          ? 'ISIN — pinned by hand, then price-checked'
                          : g.verdict === 'unmatched'
                            ? 'ISIN — refused; no Fixed portfolio position matches this holding'
                            : 'ISIN — matched by NAME to a Fixed portfolio position'}
                      how={isinHow(g)} />
                  )}
                </td>
                {/* Provenance sits OUTSIDE the badge: BucketBadge overlays an invisible `<select>`
                    across its whole span, which would swallow the hover. */}
                <td className="px-3 py-1.5">
                  <BucketBadge bucket={g?.bucket} isin={g?.isin}
                    overridden={g?.bucket_overridden} onOverride={setBucket} />
                  {g?.bucket && (
                    <Provenance source="derived" kind="formula"
                      note={g.bucket_overridden ? 'Class — manually pinned' : 'Class — the smart asset-class label'}
                      how={g.bucket_overridden
                        ? 'a manual override pinned to this ISIN, which beats the calculated class for good.'
                        : `AIRS's own Beleggingscategorie${g.categorie ? ` (“${g.categorie}”)` : ''}, then refined by the instrument's grid data (fund/ETF, asset class, name).`} />
                  )}
                </td>
                <td className="px-3 py-1.5 text-fg-subtle">
                  {g?.sector || '—'}
                  {g?.sector && (
                    <Provenance source="yfinance" kind="copied"
                      note="sector — the instrument's own sector in asset_grid, joined by ISIN" />
                  )}
                </td>
                {/* ⚠ `country` coalesces domicile over LISTING, so a US name priced on a thin German
                    line can read "Germany". The card says so rather than the column lying quietly. */}
                <td className="px-3 py-1.5 text-fg-subtle">
                  {g?.country || '—'}
                  {g?.country && (
                    <Provenance source="yfinance" kind="formula" note="country — where the issuer is domiciled"
                      how="Yahoo's assetProfile.country, falling back to the LISTING's country when Yahoo reports no domicile — for a thin foreign line that fallback names the venue, not the issuer." />
                  )}
                </td>
                <td className="px-3 py-1.5 text-fg-subtle">
                  {g?.region || '—'}
                  {g?.region && (
                    <Provenance source="yfinance" kind="formula" note="region — the MSCI ACWI region"
                      how="derived from the resolved country above. ⚠ For an ETF it describes the fund's own listing, not what the fund holds." />
                  )}
                </td>
                <td className="px-3 py-1.5 font-mono text-fg-muted">
                  {r.currency || '—'}
                  {r.currency && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="copied"
                      note="Valuta — the currency AIRS books this position in" />
                  )}
                </td>
                {/* ⚠ A dash, never 0.00%. No opening value means the holding was NOT THERE when
                    the year began, which is why it has no return either — not that it held none
                    of the book. */}
                <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                  {(() => { const sw = startWeight(r); return sw == null ? '—' : `${(sw * 100).toFixed(2)}%`; })()}
                  {startWeight(r) != null && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="formula"
                      note="start weight — the share of the book this holding was at the year's open"
                      how={`Beginwaarde ÷ the book's total Beginwaarde = ${eur(r.start_value_eur)} ÷ ${eur(startSum)}. This is the weight the Return beside it belongs to.`} />
                  )}
                </td>
                <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                  {r.weight != null ? `${(r.weight * 100).toFixed(2)}%` : '—'}
                  {r.weight != null && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="copied"
                      note="Weging — AIRS's own position weight, as of today" />
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
                  );
                })}
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
