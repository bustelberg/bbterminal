'use client';

import { useCallback, useMemo } from 'react';
import LoadingDots from '../LoadingDots';
import CellInfoTip from '../momentum/CellInfoTip';
import { useApiData } from '../../../lib/hooks/useApiData';
import { useBenchmarkCurrencyMap, useBenchmarkIsinMap, useCompanyExchangeMap, useCompanyIsinMap, useFxRateMap } from '../../../lib/hooks/apiData';
import { useFxConverters } from '../../../lib/hooks/useFxToEur';
import { displayExchange, EXCHANGE_NAMES, fmtPct, fmtPrice, guruFocusUrl } from '../momentum/utils';
import type { Holding } from '../../../lib/stores/momentum';

type SnapshotResponse = {
  snapshot_id: number;
  as_of_date: string;
  latest_price_date: string | null;
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

/** The strategy's CURRENT live portfolio — the latest snapshot's holdings,
 * shown front-and-centre when a scheduled strategy is expanded. Surfaces what
 * you hold, target vs current (drifted) weight, the entry/latest prices in
 * local + EUR with the FX rate, the return, and the ISIN. Always sorted by
 * current weight descending. Renders nothing when there's no snapshot yet. */
export default function CurrentPortfolioCard({ snapshotId }: { snapshotId: number | null }) {
  const { data: snap, loading, error } = useApiData<SnapshotResponse>(
    snapshotId != null ? `/api/momentum/current-picks/${snapshotId}` : null,
  );
  const isinByCompany = useCompanyIsinMap();
  const isinByBenchmark = useBenchmarkIsinMap();   // keyed by -benchmark_id (the holding's company_id)
  const ccyByBenchmark = useBenchmarkCurrencyMap(); // ETF currency from the LIVE benchmark, keyed by -benchmark_id
  const exchangeByCompany = useCompanyExchangeMap();
  const fxRate = useFxRateMap();                    // currency → units per EUR (latest; updates daily)

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
  // FX history converters for the ETF currencies (entry-date conversion of the
  // ETF's native price — its stored EUR is unconverted when the currency was
  // set after scheduling). Stocks keep the engine's reliable stored entry EUR.
  const etfCurrencies = useMemo(
    () => (snap?.holdings ?? []).filter((h) => (h.company_id ?? 0) < 0).map(resolveCcy),
    [snap, resolveCcy],
  );
  const fxConverters = useFxConverters(etfCurrencies);

  const rows = useMemo(() => {
    const holdings = snap?.holdings ?? [];
    // Drifted (current) weights: each starts at its target and grows by its
    // own return over the period, renormalized so the book sums to 100%.
    const factors = holdings.map((h) => (h.weight ?? 0) * (1 + (h.forward_return_pct ?? 0) / 100));
    const total = factors.reduce((a, b) => a + b, 0) || 1;
    return holdings
      .map((h, i) => ({
        h,
        target: (h.weight ?? 0) * 100,
        current: (factors[i] / total) * 100,
      }))
      // Always sorted by current (drifted) weight, descending.
      .sort((a, b) => b.current - a.current);
  }, [snap]);

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

  return (
    <div className="bg-card border border-neutral-800/40 rounded-xl p-5">
      <div className="flex items-baseline justify-between mb-3 flex-wrap gap-2">
        <h3 className="text-sm font-semibold text-fg-strong">Current portfolio</h3>
        <span className="text-xs text-fg-subtle font-mono">
          {rows.length} holdings · held since {String(snap.as_of_date).slice(0, 10)}
          {snap.latest_price_date && <> · as of {String(snap.latest_price_date).slice(0, 10)}</>}
        </span>
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
              <th className="text-right font-medium py-2 px-2 border-l border-neutral-800/40" title="Entry price in local trading currency">Start (loc)</th>
              <th className="text-right font-medium py-2 px-2" title="Latest close in local trading currency">End (loc)</th>
              <th className="text-right font-medium py-2 px-2 border-l border-neutral-800/40" title="FX rate locked in at entry: EUR per 1 unit of the local currency (1.00 for EUR)">Start FX→€</th>
              <th className="text-right font-medium py-2 px-2" title="Latest FX rate (EUR per 1 unit of the local currency) — refreshed daily from the FX sync">End FX→€</th>
              <th className="text-right font-medium py-2 px-2 border-l border-neutral-800/40" title="Entry price converted to EUR at the entry rate">Start (€)</th>
              <th className="text-right font-medium py-2 px-2" title="Latest close converted to EUR at today's rate (updates daily)">End (€)</th>
              <th className="text-right font-medium py-2 pl-2 border-l border-neutral-800/40" title="Return since this position was entered">Return</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(({ h, target, current }) => {
              const exchRaw = exchangeByCompany.get(h.company_id) ?? '';
              const exch = displayExchange(exchRaw, h.ticker);
              const isin = isinByCompany.get(h.company_id) ?? isinByBenchmark.get(h.company_id) ?? '';
              const href = guruFocusUrl(h.ticker, exchRaw);
              const isEtf = (h.company_id ?? 0) < 0;
              const ccyU = resolveCcy(h);
              const ccy = ccyU;
              const isEur = ccyU === '' || ccyU === 'EUR';
              // As-of dates: Start columns reflect the entry trading day; End
              // columns reflect the latest priced day (snapshot-wide fallback).
              const entryDate = h.entry_date ? String(h.entry_date).slice(0, 10) : null;
              const endDate = h.exit_date
                ? String(h.exit_date).slice(0, 10)
                : (snap.latest_price_date ? String(snap.latest_price_date).slice(0, 10) : null);
              // Start (€): stocks keep the engine's stored entry EUR (reliable);
              // an ETF converts its native entry price at the entry-date rate
              // (its stored EUR is unconverted when the currency was set late),
              // falling back to the stored value until FX history loads.
              let startEur: number | null;
              if (isEur) {
                startEur = h.entry_price_local ?? h.entry_price_eur ?? null;
              } else if (isEtf) {
                const conv = fxConverters.get(ccyU);
                if (conv && h.entry_price_local != null && entryDate) {
                  startEur = conv(h.entry_price_local, entryDate);
                } else {
                  startEur = h.entry_price_eur ?? null;
                }
              } else {
                startEur = h.entry_price_eur ?? null;
              }
              // Start FX = EUR per 1 local unit, from that entry conversion.
              const startFx = startEur != null && h.entry_price_local
                ? startEur / h.entry_price_local
                : (isEur ? 1 : null);
              // End FX = the latest stored rate (updates daily); End (€) marked
              // to market at it, else the snapshot's stored EUR exit.
              const endRate = fxRate.get(ccyU || 'EUR');
              const endFx = isEur ? 1 : (endRate ? 1 / endRate : null);
              const endEur = (h.exit_price_local != null && endFx != null)
                ? h.exit_price_local * endFx
                : h.exit_price_eur;
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
                    {fmtPrice(h.entry_price_local)}{ccy && <span className="text-fg-faint text-[10px] ml-1">{ccy}</span>}<AsOfTip date={entryDate} />
                  </td>
                  <td className="py-2 px-2 text-right font-mono text-fg-muted whitespace-nowrap">{fmtPrice(h.exit_price_local)}<AsOfTip date={endDate} /></td>
                  <td className="py-2 px-2 text-right font-mono text-fg-subtle whitespace-nowrap border-l border-neutral-800/40" title={startFx != null && ccy ? `1 ${ccy} = ${startFx.toFixed(4)} EUR (at entry)` : 'No entry FX (EUR / not converted)'}>
                    {startFx != null ? startFx.toFixed(4) : '—'}<AsOfTip date={entryDate} />
                  </td>
                  <td className="py-2 px-2 text-right font-mono text-fg-subtle whitespace-nowrap" title={endFx != null && ccy ? `1 ${ccy} = ${endFx.toFixed(4)} EUR (latest)` : 'No FX rate available'}>
                    {endFx != null ? endFx.toFixed(4) : '—'}<AsOfTip date={endDate} />
                  </td>
                  <td className="py-2 px-2 text-right font-mono text-fg-muted whitespace-nowrap border-l border-neutral-800/40">{fmtPrice(startEur)}<AsOfTip date={entryDate} /></td>
                  <td className="py-2 px-2 text-right font-mono text-fg whitespace-nowrap">{fmtPrice(endEur)}<AsOfTip date={endDate} /></td>
                  <td className={`py-2 pl-2 text-right font-mono border-l border-neutral-800/40 ${h.forward_return_pct != null ? (h.forward_return_pct >= 0 ? 'text-pos-400' : 'text-neg-400') : 'text-fg-faint'}`}>
                    {fmtPct(h.forward_return_pct)}
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
        <span className="font-medium"> Start FX→€</span> is the rate locked in at entry; <span className="font-medium">End FX→€</span> is today&apos;s rate (EUR per unit, refreshed daily from the FX sync), so <span className="font-medium">End (€)</span> is marked to market at the current rate. 1.00 for EUR sleeves; &quot;—&quot; when no rate is available.
      </p>
    </div>
  );
}
