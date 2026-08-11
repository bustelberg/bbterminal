'use client';

import { Fragment, useCallback, useMemo, useState } from 'react';
import LoadingDots from '../LoadingDots';
import CellInfoTip from '../momentum/CellInfoTip';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { useApiData } from '../../../lib/hooks/useApiData';
import { useBenchmarkCurrencyMap, useBenchmarkIsinMap, useBenchmarks, useCompanyExchangeMap, useCompanyIsinMap } from '../../../lib/hooks/apiData';
import { fmtSleevePct, parsePct, stockSleevePct, validateSleeves, type SleeveEtfDraft } from './sleeveMath';
import { displayExchange, EXCHANGE_NAMES, fmtPct, fmtPrice, guruFocusUrl } from '../momentum/utils';
import TableDownloadButton from '../TableDownloadButton';
// ⚠ THE SAME MODAL THE DAILY-HOLDINGS TABLE OPENS, not a second one. It reads
// `POST /api/momentum/signal-breakdown` — one endpoint, one renderer, so "why
// this was picked" cannot have two answers.
import BreakdownModal, { type BreakdownTarget } from '../momentum/BreakdownModal';
import { PriceRefreshPanel, useStockRefresh } from './priceRefresh';
import type { Column } from '../../../lib/tableExport';
import type { Holding } from '../../../lib/stores/momentum';
import type { components } from '../../../lib/api-types';

/** The reprice endpoint's payload — see `ReloadPrices`. */
type ReloadResult = components['schemas']['RepriceResult'];

type SnapshotResponse = {
  snapshot_id: number;
  as_of_date: string;
  latest_price_date: string | null;
  // The engine's authoritative weighted EUR return of this basket since entry.
  // The card DISPLAYS this — it does not recompute a portfolio return.
  period_return_pct: number | null;
  holdings: Holding[];
  // The config the picks were MADE with (universe, signal + category weights).
  // ⚠ The snapshot's own, not the strategy's current one: the strategy config is
  // editable (cash %, ETF sleeves) and the "why was this picked" screen has to
  // explain the decision as it was taken, not as the settings look today.
  config?: Record<string, unknown> | null;
};

/** Hover info icon showing a value's "as of" date (the trading date the
 * price/rate in this cell reflects). Renders nothing without a date. */
function AsOfTip({ date }: { date: string | null }) {
  if (!date) return null;
  return (
    <CellInfoTip>
      <div className="text-fg-muted">As of</div>
      <div className="font-mono text-fg">{date}</div>
    </CellInfoTip>
  );
}

/**
 * Reload this strategy's PRICES — start and end, local and converted.
 *
 * ⚠ IT DOES NOT RE-SELECT, AND THAT IS THE WHOLE DISTINCTION. Re-running the selection for a
 * past date is "Force re-rebalance" on the pipeline card, and it is not a repair: `metric_data`
 * is not append-only in `target_date` (vendors publish late closes stamped with their true
 * earlier date), so a past basket cannot be reproduced from the live database and re-deciding it
 * would silently rewrite what the strategy held. This reloads the marks on the holdings that are
 * already there.
 *
 * ⚠ AND IT RUNS THE NIGHTLY TICK'S OWN FUNCTION, not a second implementation. What the button
 * buys is timing — the correction lands now rather than at 05:00 UTC.
 *
 * The detail goes to the console (which holdings moved, and in which fields); the chip says only
 * how many, because "did it change anything?" is the one thing you need at a glance.
 */
function ReloadPrices({ strategyId, canEdit, onDone }: {
  strategyId?: number;
  canEdit?: boolean;
  onDone?: () => void | Promise<void>;
}) {
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  if (!canEdit || strategyId == null) return null;

  const run = async () => {
    setBusy(true);
    setMsg(null);
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies/${strategyId}/reprice`,
        { method: 'POST' });
      const b = (await r.json().catch(() => null)) as ReloadResult | null;
      if (!r.ok || !b) {
        console.warn('[reprice] failed', r.status, b);
        setMsg(`failed (HTTP ${r.status})`);
        return;
      }
      const moved = (b.holdings ?? []).filter((h) => (h.changed ?? []).length);
      console.groupCollapsed(
        `[reprice] strategy ${strategyId} — ${b.changed_holdings ?? 0} of `
        + `${(b.holdings ?? []).length} holding(s) changed`);
      if (moved.length) {
        console.table(moved.map((h) => ({
          ticker: h.ticker, etf: h.is_etf,
          'start (local)': h.entry_price_local, 'start (€)': h.entry_price_eur,
          'end (local)': h.exit_price_local, 'end (€)': h.exit_price_eur,
          'return %': h.forward_return_pct, changed: (h.changed ?? []).join(', '),
        })));
      } else {
        console.log(b.note ?? 'every price was already current');
      }
      console.log('full payload', b);
      console.groupEnd();
      setMsg(b.changed_holdings ? `${b.changed_holdings} updated` : 'already current');
      await onDone?.();
    } catch (e) {
      console.warn('[reprice] threw', e);
      setMsg('failed — see the console');
    } finally {
      setBusy(false);
    }
  };

  return (
    <span className="flex items-baseline gap-1.5">
      <button type="button" onClick={() => void run()} disabled={busy}
        title="Reload this portfolio's prices — start and end, local and converted — from their sources. Does NOT re-pick the holdings; that is Force re-rebalance. Runs the same function the nightly tick does, so the correction lands now instead of at 05:00 UTC."
        className="text-[12px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-muted hover:bg-overlay/5 disabled:opacity-50">
        {busy ? 'Reloading…' : 'Reload prices'}
      </button>
      {msg && <span className="text-[11px] text-fg-faint">{msg}</span>}
    </span>
  );
}

/** Sleeve control — the portfolio's cash % and ETF overlay, set by hand; the
 * stock picks take whatever is left, at the RELATIVE weights the underlying
 * strategy chose (the backend renormalizes them, so editing twice can't
 * compound). Admins get the editor; read-only users get a static chip.
 *
 * The percentages here are ABSOLUTE — shares of the whole portfolio, i.e. the
 * weights the holdings table below actually shows. */
function SleeveControl({ strategyId, cashPct, etfSleeves, canEdit, onChanged }: {
  strategyId?: number;
  cashPct: number;                                        // 0..100, absolute
  etfSleeves: { benchmarkId: number; pct: number }[];     // absolute, as held
  canEdit: boolean;
  onChanged?: () => void | Promise<void>;
}) {
  const { data: benchmarks } = useBenchmarks();
  const [open, setOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [cash, setCash] = useState<string>(fmtSleevePct(cashPct));
  const [etfs, setEtfs] = useState<SleeveEtfDraft[]>([]);

  const nameFor = useCallback((id: number) => {
    const b = (benchmarks ?? []).find((x) => x.benchmark_id === id);
    return b ? (b.ticker || b.name || `#${id}`) : `#${id}`;
  }, [benchmarks]);

  // Open = adopt the CURRENT book as the draft. Deriving it from what's held
  // (rather than from the saved config) means the editor always opens on the
  // numbers in the table beneath it.
  const openEditor = () => {
    setCash(fmtSleevePct(cashPct));
    setEtfs(etfSleeves.map((e) => ({ benchmarkId: e.benchmarkId, weightPct: fmtSleevePct(e.pct) })));
    setErr(null);
    setOpen(true);
  };

  const stocksPct = stockSleevePct(parsePct(cash), etfs);
  const invalid = validateSleeves(parsePct(cash), etfs);

  const save = async () => {
    if (invalid || strategyId == null) return;
    setSaving(true);
    setErr(null);
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies/${strategyId}/sleeves`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          cash_pct: parsePct(cash) / 100,
          etfs: etfs.map((e) => ({ benchmark_id: e.benchmarkId, weight_pct: parsePct(e.weightPct) })),
        }),
      });
      if (!r.ok) {
        // Full diagnostic to the console; one short line in the UI.
        const detail = await r.json().catch(() => null);
        console.warn('[sleeves] save failed', r.status, detail);
        setErr(typeof detail?.detail === 'string' ? detail.detail : `Save failed (HTTP ${r.status})`);
        return;
      }
      setOpen(false);
      await onChanged?.();
    } catch (e) {
      console.warn('[sleeves] save failed', e);
      setErr('Save failed — see the console.');
    } finally {
      setSaving(false);
    }
  };

  const summary = [
    cashPct > 0 ? `Cash ${fmtSleevePct(cashPct)}%` : null,
    ...etfSleeves.map((e) => `${nameFor(e.benchmarkId)} ${fmtSleevePct(e.pct)}%`),
  ].filter(Boolean).join(' · ');

  if (!canEdit || strategyId == null) {
    if (!summary) return null;
    return (
      <span className="text-xs" title="Cash + ETF sleeves (the stock picks take the rest)">
        <span className="font-mono text-fg-soft">{summary}</span>
      </span>
    );
  }

  return (
    <span className="relative flex items-center gap-2 text-xs">
      {summary && <span className="font-mono text-fg-soft">{summary}</span>}
      <button
        type="button" onClick={() => (open ? setOpen(false) : openEditor())}
        title="Set the cash % and ETF sleeves by hand — the stock picks take the rest, re-weighted from the strategy's own selection"
        className="text-[12px] px-2 py-0.5 rounded border border-neutral-700 text-fg-soft hover:bg-overlay/5 transition-colors"
      >
        {open ? 'Close' : summary ? 'Edit sleeves' : 'Add cash / ETFs'}
      </button>

      {open && (
        <div className="absolute left-0 top-6 z-20 w-[26rem] bg-popover border border-neutral-800/60 rounded-xl p-3 shadow-lg">
          <div className="flex items-center justify-between mb-2">
            <span className="text-fg-strong font-semibold">Sleeves</span>
            <span className="text-fg-faint">% of the whole portfolio</span>
          </div>

          <div className="flex items-center gap-2 mb-2">
            <span className="w-28 text-fg-subtle">Cash</span>
            <input
              type="number" min={0} max={100} step={0.5} value={cash}
              onChange={(e) => setCash(e.target.value)} disabled={saving}
              className="w-20 bg-page border border-neutral-700 rounded px-1.5 py-0.5 text-right font-mono text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 disabled:opacity-50"
            />
            <span className="text-fg-faint">%</span>
          </div>

          {etfs.map((e, i) => (
            <div key={i} className="flex items-center gap-2 mb-2">
              <select
                value={e.benchmarkId ?? ''} disabled={saving}
                onChange={(ev) => setEtfs(etfs.map((x, j) => (
                  j === i ? { ...x, benchmarkId: ev.target.value ? Number(ev.target.value) : null } : x
                )))}
                className="w-28 flex-1 bg-page border border-neutral-700 rounded px-1.5 py-0.5 text-fg focus:border-accent-500 disabled:opacity-50"
              >
                <option value="">Pick an ETF…</option>
                {(benchmarks ?? []).map((b) => (
                  <option key={b.benchmark_id} value={b.benchmark_id}>
                    {b.ticker ? `${b.ticker} — ` : ''}{b.name}
                  </option>
                ))}
              </select>
              <input
                type="number" min={0} max={100} step={0.5} value={e.weightPct}
                onChange={(ev) => setEtfs(etfs.map((x, j) => (j === i ? { ...x, weightPct: ev.target.value } : x)))}
                disabled={saving}
                className="w-20 bg-page border border-neutral-700 rounded px-1.5 py-0.5 text-right font-mono text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 disabled:opacity-50"
              />
              <span className="text-fg-faint">%</span>
              <button
                type="button" onClick={() => setEtfs(etfs.filter((_, j) => j !== i))} disabled={saving}
                title="Remove this ETF" className="text-fg-faint hover:text-neg-400 px-1"
              >
                ✕
              </button>
            </div>
          ))}

          <button
            type="button" disabled={saving}
            onClick={() => setEtfs([...etfs, { benchmarkId: null, weightPct: '0' }])}
            className="text-[12px] text-accent-400 hover:underline mb-2"
          >
            + Add ETF
          </button>

          <div className="flex items-center gap-2 border-t border-neutral-800/40 pt-2 mb-2">
            <span className="w-28 text-fg-subtle">Stock picks</span>
            <span className={`w-20 text-right font-mono ${stocksPct < 0 ? 'text-neg-400' : 'text-fg'}`}>
              {fmtSleevePct(stocksPct)}
            </span>
            <span className="text-fg-faint">%</span>
            <span className="text-fg-faint ml-1">— the strategy&apos;s own weights, re-scaled</span>
          </div>

          {(invalid || err) && (
            <div className="text-neg-400 mb-2">{err ?? invalid}</div>
          )}

          <div className="flex items-center justify-between">
            <span className="text-fg-faint">Restates the open period + re-prices.</span>
            <span className="flex gap-2">
              <button
                type="button" onClick={() => setOpen(false)} disabled={saving}
                className="text-[12px] px-2 py-0.5 rounded border border-neutral-700 text-fg-soft hover:bg-overlay/5"
              >
                Cancel
              </button>
              <button
                type="button" onClick={() => void save()} disabled={saving || !!invalid}
                className="text-[12px] px-2 py-0.5 rounded bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
              >
                {saving ? 'Saving…' : 'Save'}
              </button>
            </span>
          </div>
        </div>
      )}
    </span>
  );
}

/** The strategy's CURRENT live portfolio — the latest snapshot's holdings,
 * shown front-and-centre when a scheduled strategy is expanded. Surfaces what
 * you hold, target vs current (drifted) weight, the entry/latest prices in
 * local + EUR with the FX rate, the return, and the ISIN. Always sorted by
 * current weight descending. Renders nothing when there's no snapshot yet. */
export default function CurrentPortfolioCard({
  snapshotId, strategyId, canEditCash = false, staleCompanyIds, onCashChanged,
}: {
  snapshotId: number | null;
  strategyId?: number;
  /** Admin (non-read-only) may set the cash allocation. */
  canEditCash?: boolean;
  /** Company ids the LIVE staleness check (from /runs `stale_prices`) flags as
   * lagging the basket's freshest close — the same set the monthly-returns
   * heatmap warns on. These rows always get the refresh action, even when the
   * persisted snapshot's frozen dates don't yet mark them stale. */
  staleCompanyIds?: number[];
  /** Called after a successful cash change so the parent reloads the detail
   * (the re-price creates a NEW snapshot the card then re-reads). */
  onCashChanged?: () => void | Promise<void>;
}) {
  const { data: snap, loading, error } = useApiData<SnapshotResponse>(
    snapshotId != null ? `/api/momentum/current-picks/${snapshotId}` : null,
  );
  // Per-stock refresh (admin only): fetch one holding's price from GuruFocus
  // now and show the request/response inline. Reloading the detail after a
  // success surfaces the freshly-loaded close (+ re-priced basket).
  // "Why was this picked" — the same modal the Daily-holdings table opens, from
  // the same endpoint. Null = closed.
  const [breakdown, setBreakdown] = useState<BreakdownTarget | null>(null);
  const { refreshing, results: refreshResults, refresh, clear: clearRefresh } = useStockRefresh(onCashChanged);
  const refreshOne = useCallback((companyId: number) => refresh(companyId, strategyId), [refresh, strategyId]);
  const staleSet = useMemo(() => new Set(staleCompanyIds ?? []), [staleCompanyIds]);

  const isinByCompany = useCompanyIsinMap();
  const isinByBenchmark = useBenchmarkIsinMap();   // keyed by -benchmark_id (the holding's company_id)
  const ccyByBenchmark = useBenchmarkCurrencyMap(); // ETF currency from the LIVE benchmark, keyed by -benchmark_id
  const exchangeByCompany = useCompanyExchangeMap();

  // Resolve a holding's currency: an ETF (negative company_id) takes the LIVE
  // benchmark currency (so a currency set after scheduling is respected now);
  // a stock uses its own stored currency.
  const resolveCcy = useCallback(
    (h: Holding): string => (
      ((h.company_id ?? 0) < 0 ? ccyByBenchmark.get(h.company_id) : undefined)
      ?? h.currency ?? ''
    ).toUpperCase(),
    [ccyByBenchmark],
  );

  // PURE DISPLAY — the card recomputes NOTHING. Every return is the engine's
  // single source of truth: per-row Return = `forward_return_pct`, the Total =
  // the snapshot's `period_return_pct` (= Σ weight·forward, computed once by the
  // re-pricer in EUR at each date's FX). Start/End (€) just SHOW the stored EUR
  // marks; the FX→€ columns are those marks ÷ the local price (a displayed rate,
  // not a conversion the card performs). A holding with no stored EUR yet (an
  // ETF in a snapshot taken before the EUR re-pricer ran) shows a blank €cell —
  // cosmetic, and it self-heals on the next price-update — but the Return + Total
  // stay correct + consistent because they read the engine, not the prices.
  const rows = useMemo(() => {
    const holdings = snap?.holdings ?? [];
    const derived = holdings.map((h) => {
      const isEtf = (h.company_id ?? 0) < 0;
      const ccy = resolveCcy(h);
      const isEur = ccy === '' || ccy === 'EUR';
      const entryDate = h.entry_date ? String(h.entry_date).slice(0, 10) : null;
      const endDate = h.exit_date
        ? String(h.exit_date).slice(0, 10)
        : (snap?.latest_price_date ? String(snap.latest_price_date).slice(0, 10) : null);
      // Stored EUR marks only (treat 0 as "not stored"); EUR sleeves pass the
      // local price through at 1:1.
      const startEur = (h.entry_price_eur != null && h.entry_price_eur > 0)
        ? h.entry_price_eur : (isEur ? (h.entry_price_local ?? null) : null);
      const endEur = (h.exit_price_eur != null && h.exit_price_eur > 0)
        ? h.exit_price_eur : (isEur ? (h.exit_price_local ?? null) : null);
      const startFx = startEur != null && h.entry_price_local ? startEur / h.entry_price_local : (isEur ? 1 : null);
      const endFx = endEur != null && h.exit_price_local ? endEur / h.exit_price_local : (isEur ? 1 : null);
      // The per-holding return is the engine's stored value — never recomputed.
      const eurReturn = h.forward_return_pct ?? null;
      const weight = h.weight ?? 0;
      return { h, isEtf, ccy, isEur, entryDate, endDate, startEur, endEur, startFx, endFx, eurReturn, weight, target: weight * 100 };
    });
    // Drifted (current) weights from the engine's per-holding return.
    const totalFactor = derived.reduce((a, d) => a + d.weight * (1 + (d.eurReturn ?? 0) / 100), 0) || 1;
    return derived
      .map((d) => ({ ...d, current: (d.weight * (1 + (d.eurReturn ?? 0) / 100) / totalFactor) * 100 }))
      .sort((a, b) => b.current - a.current);
  }, [snap, resolveCcy]);

  // The Total is the engine's `period_return_pct` verbatim — the single source
  // of truth the /schedule header MTD also reads, so they can't disagree. Only
  // if it's somehow absent do we fall back to the weighted per-holding returns
  // (which equal it by construction).
  const totalReturn = useMemo(() => {
    if (snap?.period_return_pct != null) return snap.period_return_pct;
    let wsum = 0;
    let rsum = 0;
    for (const d of rows) {
      if (d.eurReturn != null) { rsum += d.eurReturn * d.weight; wsum += d.weight; }
    }
    return wsum > 0 ? rsum / wsum : null;
  }, [snap, rows]);

  // Export columns — mirror the on-screen table (same already-sorted `rows`),
  // so a CSV/XLSX download matches exactly what the user sees.
  type Row = (typeof rows)[number];
  const exportColumns: Column<Row>[] = useMemo(() => [
    { key: 'ticker', header: 'Ticker', accessor: (d) => d.h.ticker ?? '' },
    { key: 'exchange', header: 'Exchange', accessor: (d) => displayExchange(exchangeByCompany.get(d.h.company_id) ?? (d.isEtf ? 'ETF' : ''), d.h.ticker) },
    { key: 'isin', header: 'ISIN', accessor: (d) => d.h.isin ?? isinByCompany.get(d.h.company_id) ?? isinByBenchmark.get(d.h.company_id) ?? '' },
    { key: 'company', header: 'Company', accessor: (d) => d.h.company_name ?? '' },
    { key: 'sector', header: 'Sector', accessor: (d) => d.h.sector ?? '' },
    { key: 'currency', header: 'Currency', accessor: (d) => d.ccy },
    { key: 'target', header: 'Target %', accessor: (d) => d.target },
    { key: 'current', header: 'Current %', accessor: (d) => d.current },
    { key: 'start_loc', header: 'Start (local)', accessor: (d) => d.h.entry_price_local ?? null },
    { key: 'start_as_of', header: 'Start as of', accessor: (d) => d.entryDate },
    { key: 'end_loc', header: 'End (local)', accessor: (d) => d.h.exit_price_local ?? null },
    { key: 'end_as_of', header: 'End as of', accessor: (d) => d.endDate },
    { key: 'start_fx_eur', header: 'Start FX→€', accessor: (d) => d.startFx },
    { key: 'end_fx_eur', header: 'End FX→€', accessor: (d) => d.endFx },
    { key: 'start_eur', header: 'Start (€)', accessor: (d) => d.startEur },
    { key: 'end_eur', header: 'End (€)', accessor: (d) => d.endEur },
    { key: 'return_eur', header: 'Return (€) %', accessor: (d) => d.eurReturn },
  ], [isinByCompany, isinByBenchmark, exchangeByCompany]);

  if (snapshotId == null) return null;
  if (loading) {
    return (
      <div className="bg-card border border-neutral-800/40 rounded-lg px-4 py-3">
        <LoadingDots label="Loading current portfolio" />
      </div>
    );
  }
  if (error) {
    return (
      <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3 text-xs text-neg-300">
        Couldn&apos;t load current portfolio: {error}
      </div>
    );
  }
  if (!snap || rows.length === 0) return null;

  // Reference for "out of date": the portfolio's freshest close. A holding
  // whose latest close (End date) lags this is stale (GuruFocus publish lag /
  // an illiquid name) and gets flagged orange in the As-of column.
  const referenceDate = snap.latest_price_date ? String(snap.latest_price_date).slice(0, 10) : null;
  // The sleeves AS HELD, straight off the snapshot: cash is the cash holding's
  // weight, each ETF is its own (negative company_id = -benchmark_id). Read from
  // the holdings rather than the config so the editor always opens on the
  // numbers in the table below it.
  const currentCashPct = ((snap.holdings ?? []).find((h) => h.is_cash)?.weight ?? 0) * 100;
  const currentEtfSleeves = (snap.holdings ?? [])
    .filter((h) => !h.is_cash && (h.company_id ?? 0) < 0)
    .map((h) => ({ benchmarkId: -(h.company_id ?? 0), pct: (h.weight ?? 0) * 100 }));
  // "Held since" = the earliest holding entry date — the actual price date the
  // return is measured FROM ("…since it was entered"), which is the close before
  // the rebalance day (e.g. Friday 05-29 for a first-Monday 06-01 rebalance).
  // NOT the snapshot's as_of_date, which can carry a stale/exit date.
  const heldSince = (snap.holdings ?? [])
    .map((h) => (h.entry_date ? String(h.entry_date).slice(0, 10) : null))
    .filter((d): d is string => !!d)
    .sort()[0] ?? String(snap.as_of_date).slice(0, 10);

  return (
    <div className="bg-card border border-neutral-800/40 rounded-xl p-5">
      <div className="flex items-baseline justify-between mb-3 flex-wrap gap-2">
        <div className="flex items-baseline gap-3 flex-wrap">
          <h3 className="text-sm font-semibold text-fg-strong">Current portfolio</h3>
          <SleeveControl
            strategyId={strategyId}
            cashPct={currentCashPct}
            etfSleeves={currentEtfSleeves}
            canEdit={canEditCash}
            onChanged={onCashChanged}
          />
          <ReloadPrices strategyId={strategyId} canEdit={canEditCash} onDone={onCashChanged} />
          {totalReturn != null && (
            <span className="text-sm" title="Weighted EUR return of the held portfolio since it was entered">
              <span className="text-fg-subtle text-xs">Total (€) </span>
              <span className={`font-mono font-semibold ${totalReturn >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
                {totalReturn >= 0 ? '+' : ''}{totalReturn.toFixed(2)}%
              </span>
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-fg-subtle font-mono">
            {rows.length} holdings · held since {heldSince}
            {snap.latest_price_date && <> · as of {String(snap.latest_price_date).slice(0, 10)}</>}
          </span>
          <TableDownloadButton
            rows={rows}
            columns={exportColumns}
            filename="current-portfolio"
            confirmNoun="holdings"
            title="Download the current portfolio as CSV / XLSX"
          />
        </div>
      </div>

      <div className="overflow-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-fg-subtle text-xs border-b border-neutral-800/60">
              <th className="text-left font-medium py-2 pr-2">Ticker</th>
              <th className="text-left font-medium py-2 px-2">ISIN</th>
              <th className="text-left font-medium py-2 px-2">Company</th>
              <th className="text-left font-medium py-2 px-2">Sector</th>
              <th className="text-right font-medium py-2 px-2" title="Target weight set at the rebalance">Target</th>
              <th className="text-right font-medium py-2 px-2" title="Weight today after price drift, renormalized to 100%">Current</th>
              <th className="text-right font-medium py-2 px-2 border-l border-neutral-800/40" title="Entry price in local trading currency">Start (local)</th>
              <th className="text-right font-medium py-2 px-2" title="Entry date the Start (local) price reflects">As of</th>
              <th className="text-right font-medium py-2 px-2" title="Latest close in local trading currency">End (local)</th>
              <th className="text-right font-medium py-2 px-2" title="Latest-close date the End (local) price reflects. Orange when it lags the portfolio's freshest close (stale price).">As of</th>
              <th className="text-right font-medium py-2 px-2 border-l border-neutral-800/40" title="FX rate locked in at entry: EUR per 1 unit of the local currency (1.00 for EUR)">Start FX→€</th>
              <th className="text-right font-medium py-2 px-2" title="Entry date the Start FX→€ rate reflects">As of</th>
              <th className="text-right font-medium py-2 px-2" title="FX rate at the latest close: EUR per 1 unit of the local currency (the engine's exit EUR ÷ local)">End FX→€</th>
              <th className="text-right font-medium py-2 px-2" title="Date the End FX→€ rate reflects. Orange when it lags the portfolio's freshest close (stale).">As of</th>
              <th className="text-right font-medium py-2 px-2 border-l border-neutral-800/40" title="Engine's EUR entry mark (converted at the entry-date FX)">Start (€)</th>
              <th className="text-right font-medium py-2 px-2" title="Engine's EUR exit mark (converted at the close-date FX)">End (€)</th>
              <th className="text-right font-medium py-2 pl-2 border-l border-neutral-800/40" title="The engine's per-holding EUR return since entry (forward_return_pct) — shown verbatim, not recomputed">Return (€)</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const { h, target, current, isEtf, ccy, entryDate, endDate, startEur, endEur, startFx, endFx, eurReturn } = row;
              // Cash sleeve — a plain row: weights + a flat 0% return, no
              // ticker/ISIN/price/date cells (17 columns, "—" for the rest).
              if (h.is_cash) {
                const dash = 'py-2 px-2 text-right font-mono whitespace-nowrap text-fg-faint';
                return (
                  <tr key="cash" className="border-b border-neutral-800/30 bg-overlay/[0.02]">
                    <td className="py-2 pr-2 font-mono whitespace-nowrap text-fg-soft">
                      Cash
                      <span className="ml-1.5 text-[10px] uppercase tracking-wide px-1 py-0.5 rounded bg-neutral-500/15 text-fg-muted border border-neutral-500/30">CASH</span>
                    </td>
                    <td className="py-2 px-2 text-fg-faint">—</td>
                    <td className="py-2 px-2 text-fg-soft">Cash</td>
                    <td className="py-2 px-2 text-fg-subtle">—</td>
                    <td className="py-2 px-2 text-right font-mono text-fg-muted">{target.toFixed(1)}%</td>
                    <td className="py-2 px-2 text-right font-mono text-fg-strong">{current.toFixed(1)}%</td>
                    <td className={`${dash} border-l border-neutral-800/40`}>—</td>
                    <td className={dash}>—</td>
                    <td className={dash}>—</td>
                    <td className={dash}>—</td>
                    <td className={`${dash} border-l border-neutral-800/40`}>—</td>
                    <td className={dash}>—</td>
                    <td className={dash}>—</td>
                    <td className={dash}>—</td>
                    <td className={`${dash} border-l border-neutral-800/40`}>—</td>
                    <td className={dash}>—</td>
                    <td className="py-2 pl-2 text-right font-mono border-l border-neutral-800/40 text-fg-faint">0.00%</td>
                  </tr>
                );
              }
              const exchRaw = exchangeByCompany.get(h.company_id) ?? '';
              const exch = displayExchange(exchRaw, h.ticker);
              const isin = h.isin ?? isinByCompany.get(h.company_id) ?? isinByBenchmark.get(h.company_id) ?? '';
              const href = guruFocusUrl(h.ticker, exchRaw);
              // Each date-dependent value (Start/End local, Start/End FX) carries
              // its own As-of cell. Entry cells show the entry date (historical —
              // never "stale"); End cells show the close date, orange when this
              // holding lags the portfolio's freshest close.
              const staleEnd = !!(endDate && referenceDate && endDate < referenceDate);
              // Admins can force-refresh ANY holding from here — a stock from
              // metric_data, an ETF overlay from benchmark_price. Stale rows —
              // flagged by the LIVE check (matches the heatmap warning) or the
              // snapshot's own lagging close — show the ↻ always, in warning
              // colour; healthy rows show it subtly on hover.
              const canRefresh = canEditCash;
              const isStale = staleSet.has(h.company_id) || staleEnd;
              const detail = refreshResults.get(h.company_id);
              const entryAsOfCell = () => (
                <td className="py-2 px-2 text-right font-mono whitespace-nowrap text-fg-subtle" title="Entry date this value reflects">
                  {entryDate ?? '—'}
                </td>
              );
              const endAsOfCell = () => (
                <td
                  className={`py-2 px-2 text-right font-mono whitespace-nowrap ${staleEnd ? 'text-warn-400' : 'text-fg-subtle'}`}
                  title={staleEnd ? `Stale — this holding's latest close (${endDate}) lags the portfolio's freshest close (${referenceDate})` : 'Latest close date this holding reflects'}
                >
                  {endDate ?? '—'}
                </td>
              );
              return (
                <Fragment key={`${h.side ?? 'long'}-${h.company_id}`}>
                <tr className="group border-b border-neutral-800/30 hover:bg-overlay/[0.02]">
                  <td className="py-2 pr-2 font-mono whitespace-nowrap">
                    <a href={href} target="_blank" rel="noopener noreferrer" className="text-accent-400 hover:text-accent-300 hover:underline">{h.ticker}</a>
                    {exch && <span className="ml-1 text-[11px] text-fg-subtle" title={EXCHANGE_NAMES[exch.toUpperCase()] ?? exch}>({exch})</span>}
                    {isEtf && <span className="ml-1.5 text-[10px] uppercase tracking-wide px-1 py-0.5 rounded bg-accent-500/15 text-accent-300 border border-accent-500/30">ETF</span>}
                    {canRefresh && (
                      <button
                        type="button"
                        onClick={() => void refreshOne(h.company_id)}
                        disabled={refreshing.has(h.company_id)}
                        title={isStale
                          ? "Stale price — refresh this stock from GuruFocus now (shows the request + response)"
                          : "Refresh this stock's price from GuruFocus now (shows the request + response)"}
                        className={`ml-1.5 text-[12px] disabled:opacity-40 transition-opacity ${
                          isStale
                            ? 'text-warn-400 hover:text-warn-300'
                            : 'text-fg-faint hover:text-accent-300 opacity-0 group-hover:opacity-100 focus:opacity-100'
                        }`}
                      >
                        {refreshing.has(h.company_id) ? '…' : '↻'}
                      </button>
                    )}
                  </td>
                  <td className="py-2 px-2 font-mono text-fg-muted whitespace-nowrap">{isin || '—'}</td>
                  {/* The name opens the arithmetic behind the pick. ⚠ ONLY for a
                      real company: an ETF sleeve and the cash row were never
                      SELECTED by the engine — they were set by hand — so there is
                      no signal breakdown to show and a clickable name there would
                      promise an explanation that cannot exist. The GuruFocus link
                      is unaffected; it lives on the Ticker cell to the left. */}
                  <td className="py-2 px-2 truncate max-w-[220px]">
                    {(h.company_id ?? 0) > 0 && !h.is_cash ? (
                      <button
                        type="button"
                        onClick={() => setBreakdown({
                          companyId: h.company_id,
                          date: snap.as_of_date,
                          name: h.company_name ?? '',
                          ticker: h.ticker ?? null,
                        })}
                        title={`Why was ${h.company_name} picked? — signals, normalisation and the category blend as of ${snap.as_of_date}`}
                        className="text-fg-soft hover:text-accent-300 hover:underline text-left truncate max-w-full"
                      >
                        {h.company_name}
                      </button>
                    ) : (
                      <span className="text-fg-soft">{h.company_name}</span>
                    )}
                  </td>
                  <td className="py-2 px-2 text-fg-subtle">{h.sector}</td>
                  <td className="py-2 px-2 text-right font-mono text-fg-muted">{target.toFixed(1)}%</td>
                  <td className="py-2 px-2 text-right font-mono text-fg-strong">{current.toFixed(1)}%</td>
                  <td className="py-2 px-2 text-right font-mono text-fg-muted whitespace-nowrap border-l border-neutral-800/40">
                    {fmtPrice(h.entry_price_local)}{ccy && <span className="text-fg-faint text-[11px] ml-1">{ccy}</span>}
                  </td>
                  {entryAsOfCell()}
                  <td className="py-2 px-2 text-right font-mono text-fg-muted whitespace-nowrap">{fmtPrice(h.exit_price_local)}</td>
                  {endAsOfCell()}
                  <td className="py-2 px-2 text-right font-mono text-fg-subtle whitespace-nowrap border-l border-neutral-800/40" title={startFx != null && ccy ? `1 ${ccy} = ${startFx.toFixed(4)} EUR (at entry)` : 'No entry FX (EUR / not converted)'}>
                    {startFx != null ? startFx.toFixed(4) : '—'}
                  </td>
                  {entryAsOfCell()}
                  <td className="py-2 px-2 text-right font-mono text-fg-subtle whitespace-nowrap" title={endFx != null && ccy ? `1 ${ccy} = ${endFx.toFixed(4)} EUR (latest)` : 'No FX rate available'}>
                    {endFx != null ? endFx.toFixed(4) : '—'}
                  </td>
                  {endAsOfCell()}
                  <td className="py-2 px-2 text-right font-mono text-fg-muted whitespace-nowrap border-l border-neutral-800/40">{fmtPrice(startEur)}<AsOfTip date={entryDate} /></td>
                  <td className="py-2 px-2 text-right font-mono text-fg whitespace-nowrap">{fmtPrice(endEur)}</td>
                  <td className={`py-2 pl-2 text-right font-mono border-l border-neutral-800/40 ${eurReturn != null ? (eurReturn >= 0 ? 'text-pos-400' : 'text-neg-400') : 'text-fg-faint'}`}>
                    {fmtPct(eurReturn)}
                  </td>
                </tr>
                {detail && (
                  <tr className="border-b border-neutral-800/30 bg-inset/40">
                    <td colSpan={17} className="px-3 py-2">
                      <PriceRefreshPanel
                        result={detail}
                        onClose={() => clearRefresh(h.company_id)}
                      />
                    </td>
                  </tr>
                )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-fg-subtle mt-3 leading-relaxed">
        Sorted by current weight. <span className="font-medium">Target</span> is the last-rebalance weight; <span className="font-medium">Current</span> is the drifted weight, renormalized to 100%. <span className="font-medium">Start/End</span> are entry and latest-close prices in local currency. <span className="font-medium">Return (€)</span> and <span className="font-medium">Total</span> are the engine&apos;s figures (per-holding and weighted portfolio return), matching the headline MTD. <span className="font-medium">Start/End (€)</span>{' '}are the engine&apos;s EUR marks — &quot;—&quot; until priced (ETFs self-heal on the next price update).
        {canEditCash && <> A stale (<span className="text-warn-400">orange</span>) close date shows a <span className="text-warn-400">↻</span> to refresh that holding from GuruFocus, with the request and response inline.</>}
        {' '}Click a <span className="font-medium">company name</span> for the arithmetic behind its selection.
      </p>
      {breakdown && (
        <BreakdownModal
          target={breakdown}
          // The snapshot's own config — the universe + weights the pick was made
          // with. `{}` only when a legacy snapshot stored none; the endpoint then
          // falls back to its own defaults rather than failing.
          config={(snap.config ?? {}) as Record<string, unknown>}
          onClose={() => setBreakdown(null)}
        />
      )}
    </div>
  );
}
