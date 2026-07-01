'use client';

import { useCallback, useMemo, useState } from 'react';
import LoadingDots from '../LoadingDots';
import CellInfoTip from '../momentum/CellInfoTip';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { useApiData } from '../../../lib/hooks/useApiData';
import { useBenchmarkCurrencyMap, useBenchmarkIsinMap, useCompanyExchangeMap, useCompanyIsinMap } from '../../../lib/hooks/apiData';
import { displayExchange, EXCHANGE_NAMES, fmtPct, fmtPrice, guruFocusUrl } from '../momentum/utils';
import TableDownloadButton from '../TableDownloadButton';
import type { Column } from '../../../lib/tableExport';
import type { Holding } from '../../../lib/stores/momentum';

type SnapshotResponse = {
  snapshot_id: number;
  as_of_date: string;
  latest_price_date: string | null;
  // The engine's authoritative weighted EUR return of this basket since entry.
  // The card DISPLAYS this — it does not recompute a portfolio return.
  period_return_pct: number | null;
  holdings: Holding[];
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

/** Cash-allocation control. Admins set the portfolio's cash % (0–100), which
 * scales every other holding's weight and re-prices the strategy; read-only
 * users see a static chip (or nothing when there's no cash). */
function CashControl({ strategyId, currentPct, canEdit, onChanged }: {
  strategyId?: number;
  currentPct: number;   // 0..100
  canEdit: boolean;
  onChanged?: () => void | Promise<void>;
}) {
  const [value, setValue] = useState<string>(String(Math.round(currentPct)));
  const [saving, setSaving] = useState(false);

  if (!canEdit || strategyId == null) {
    if (currentPct <= 0) return null;
    return (
      <span className="text-xs" title="Cash allocation (scales the other holdings)">
        <span className="text-fg-subtle">Cash </span>
        <span className="font-mono text-fg-soft">{currentPct.toFixed(0)}%</span>
      </span>
    );
  }

  const save = async () => {
    const pct = Math.min(100, Math.max(0, Number(value) || 0));
    setSaving(true);
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies/${strategyId}/cash`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cash_pct: pct / 100 }),
      });
      if (r.ok) await onChanged?.();
    } finally {
      setSaving(false);
    }
  };

  const dirty = (Number(value) || 0) !== Math.round(currentPct);
  return (
    <span className="flex items-center gap-1 text-xs" title="Set cash % — scales every other holding's weight by (100−cash)% and re-prices the strategy">
      <span className="text-fg-subtle">Cash</span>
      <input
        type="number" min={0} max={100} step={1} value={value}
        onChange={(e) => setValue(e.target.value)} disabled={saving}
        className="w-14 bg-page border border-neutral-700 rounded px-1.5 py-0.5 text-right font-mono text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 disabled:opacity-50"
      />
      <span className="text-fg-faint">%</span>
      <button
        type="button" onClick={() => void save()} disabled={saving || !dirty}
        className="text-[11px] px-2 py-0.5 rounded bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
      >
        {saving ? 'Saving…' : 'Set'}
      </button>
    </span>
  );
}

/** The strategy's CURRENT live portfolio — the latest snapshot's holdings,
 * shown front-and-centre when a scheduled strategy is expanded. Surfaces what
 * you hold, target vs current (drifted) weight, the entry/latest prices in
 * local + EUR with the FX rate, the return, and the ISIN. Always sorted by
 * current weight descending. Renders nothing when there's no snapshot yet. */
export default function CurrentPortfolioCard({
  snapshotId, strategyId, canEditCash = false, onCashChanged,
}: {
  snapshotId: number | null;
  strategyId?: number;
  /** Admin (non-read-only) may set the cash allocation. */
  canEditCash?: boolean;
  /** Called after a successful cash change so the parent reloads the detail
   * (the re-price creates a NEW snapshot the card then re-reads). */
  onCashChanged?: () => void | Promise<void>;
}) {
  const { data: snap, loading, error } = useApiData<SnapshotResponse>(
    snapshotId != null ? `/api/momentum/current-picks/${snapshotId}` : null,
  );
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
  // Current cash % = the cash holding's weight (0 when none).
  const currentCashPct = ((snap.holdings ?? []).find((h) => h.is_cash)?.weight ?? 0) * 100;
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
          <CashControl strategyId={strategyId} currentPct={currentCashPct} canEdit={canEditCash} onChanged={onCashChanged} />
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
                      <span className="ml-1.5 text-[9px] uppercase tracking-wide px-1 py-0.5 rounded bg-neutral-500/15 text-fg-muted border border-neutral-500/30">CASH</span>
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
                <tr key={`${h.side ?? 'long'}-${h.company_id}`} className="border-b border-neutral-800/30 hover:bg-overlay/[0.02]">
                  <td className="py-2 pr-2 font-mono whitespace-nowrap">
                    <a href={href} target="_blank" rel="noopener noreferrer" className="text-accent-400 hover:text-accent-300 hover:underline">{h.ticker}</a>
                    {exch && <span className="ml-1 text-[10px] text-fg-subtle" title={EXCHANGE_NAMES[exch.toUpperCase()] ?? exch}>({exch})</span>}
                    {isEtf && <span className="ml-1.5 text-[9px] uppercase tracking-wide px-1 py-0.5 rounded bg-accent-500/15 text-accent-300 border border-accent-500/30">ETF</span>}
                  </td>
                  <td className="py-2 px-2 font-mono text-fg-muted whitespace-nowrap">{isin || '—'}</td>
                  <td className="py-2 px-2 truncate max-w-[220px]">
                    <a href={href} target="_blank" rel="noopener noreferrer" className="text-fg-soft hover:text-accent-300 hover:underline">{h.company_name}</a>
                  </td>
                  <td className="py-2 px-2 text-fg-subtle">{h.sector}</td>
                  <td className="py-2 px-2 text-right font-mono text-fg-muted">{target.toFixed(1)}%</td>
                  <td className="py-2 px-2 text-right font-mono text-fg-strong">{current.toFixed(1)}%</td>
                  <td className="py-2 px-2 text-right font-mono text-fg-muted whitespace-nowrap border-l border-neutral-800/40">
                    {fmtPrice(h.entry_price_local)}{ccy && <span className="text-fg-faint text-[10px] ml-1">{ccy}</span>}
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
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-fg-subtle mt-3 leading-relaxed">
        Sorted by current weight. <span className="font-medium">Target</span> is the weight set at the last rebalance; <span className="font-medium">Current</span> is where it has drifted to as prices moved (renormalized to 100%).
        <span className="font-medium"> Start/End</span> are the entry and latest-close prices in local currency.
<span className="font-medium"> Return (€)</span> and the <span className="font-medium">Total</span> are the engine&apos;s figures shown verbatim (the per-holding return and the snapshot&apos;s weighted EUR return) — the same source the headline MTD reads, so they always agree. <span className="font-medium">Start/End (€)</span> show the engine&apos;s EUR marks; &quot;—&quot; until a holding has been EUR-priced (an ETF self-heals on the next price-update).
      </p>
    </div>
  );
}
