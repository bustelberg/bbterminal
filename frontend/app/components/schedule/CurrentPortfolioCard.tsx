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
  const fxRate = useFxRateMap();                    // currency → units per EUR (latest)

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
  // Entry-date FX converters for the ETF currencies — only used as a FALLBACK
  // when an ETF holding has no engine-stored EUR yet (a snapshot taken before
  // the EUR-aware price-update re-priced it). Stocks + re-priced ETFs use the
  // engine's stored EUR directly.
  const etfCurrencies = useMemo(
    () => (snap?.holdings ?? []).filter((h) => (h.company_id ?? 0) < 0).map(resolveCcy),
    [snap, resolveCcy],
  );
  const fxConverters = useFxConverters(etfCurrencies);

  // Per-holding derived values, all in EUR. The EUR marks + per-holding return
  // come STRAIGHT from the engine's stored values (`entry_price_eur`,
  // `exit_price_eur`, `forward_return_pct`) — the price-update re-pricer
  // converts to EUR at each date's FX, so these are the authoritative figures
  // and the Total matches the engine's `period_return_pct`. The ONE exception:
  // an ETF overlay in a snapshot taken before that re-pricer ran has no stored
  // EUR (entry/exit_price_eur null/0) — rather than show blank FX/€ columns we
  // convert its benchmark price client-side (entry-date FX for start, latest FX
  // for end). That fallback self-heals to the exact stored value on the next
  // price-update.
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
      // Engine-stored EUR first (treat 0 as "not stored"); then EUR passthrough;
      // then the ETF client-side FX fallback.
      let startEur = (h.entry_price_eur != null && h.entry_price_eur > 0) ? h.entry_price_eur : null;
      let endEur = (h.exit_price_eur != null && h.exit_price_eur > 0) ? h.exit_price_eur : null;
      if (startEur == null) {
        if (isEur) startEur = h.entry_price_local ?? null;
        else if (isEtf) {
          const conv = fxConverters.get(ccy);
          startEur = (conv && h.entry_price_local != null && entryDate) ? conv(h.entry_price_local, entryDate) : null;
        }
      }
      if (endEur == null) {
        if (isEur) endEur = h.exit_price_local ?? null;
        else if (isEtf) {
          const endRate = fxRate.get(ccy);  // units per EUR → EUR = local / rate
          endEur = (endRate && h.exit_price_local != null) ? h.exit_price_local / endRate : null;
        }
      }
      const startFx = startEur != null && h.entry_price_local ? startEur / h.entry_price_local : (isEur ? 1 : null);
      const endFx = endEur != null && h.exit_price_local ? endEur / h.exit_price_local : (isEur ? 1 : null);
      // Return derived from the EUR marks shown (so the row is internally
      // consistent); falls back to the engine's stored return only if EUR can't
      // be resolved. For stocks + re-priced ETFs the EUR marks ARE the engine's,
      // so this equals `forward_return_pct` and the Total equals the engine.
      const eurReturn = (startEur != null && endEur != null && startEur > 0)
        ? (endEur / startEur - 1) * 100
        : (h.forward_return_pct ?? null);
      const weight = h.weight ?? 0;
      return { h, isEtf, ccy, isEur, entryDate, endDate, startEur, endEur, startFx, endFx, eurReturn, weight, target: weight * 100 };
    });
    // Drifted (current) weights from the EUR return, renormalized to 100%.
    const totalFactor = derived.reduce((a, d) => a + d.weight * (1 + (d.eurReturn ?? 0) / 100), 0) || 1;
    return derived
      .map((d) => ({ ...d, current: (d.weight * (1 + (d.eurReturn ?? 0) / 100) / totalFactor) * 100 }))
      .sort((a, b) => b.current - a.current);
  }, [snap, resolveCcy, fxConverters, fxRate]);

  // Total portfolio performance since entry: each holding's EUR return weighted
  // by its target weight (same EUR basis as the strategy-row MTD header).
  const totalReturn = useMemo(() => {
    let wsum = 0;
    let rsum = 0;
    for (const d of rows) {
      if (d.eurReturn != null) { rsum += d.eurReturn * d.weight; wsum += d.weight; }
    }
    return wsum > 0 ? rsum / wsum : null;
  }, [rows]);

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
        <div className="flex items-baseline gap-3 flex-wrap">
          <h3 className="text-sm font-semibold text-fg-strong">Current portfolio</h3>
          {totalReturn != null && (
            <span className="text-sm" title="Weighted EUR return of the held portfolio since it was entered">
              <span className="text-fg-subtle text-xs">Total (€) </span>
              <span className={`font-mono font-semibold ${totalReturn >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
                {totalReturn >= 0 ? '+' : ''}{totalReturn.toFixed(2)}%
              </span>
            </span>
          )}
        </div>
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
              <th className="text-right font-medium py-2 pl-2 border-l border-neutral-800/40" title="EUR return since this position was entered (local price move × FX move)">Return (€)</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const { h, target, current, isEtf, ccy, entryDate, endDate, startEur, endEur, startFx, endFx, eurReturn } = row;
              const exchRaw = exchangeByCompany.get(h.company_id) ?? '';
              const exch = displayExchange(exchRaw, h.ticker);
              const isin = isinByCompany.get(h.company_id) ?? isinByBenchmark.get(h.company_id) ?? '';
              const href = guruFocusUrl(h.ticker, exchRaw);
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
        <span className="font-medium"> Start FX→€</span> is the rate locked in at entry; <span className="font-medium">End FX→€</span> is the rate at the latest close (EUR per unit), so <span className="font-medium">End (€)</span> is the engine&apos;s EUR mark at that date — the same EUR basis as the strategy&apos;s headline return. 1.00 for EUR sleeves; &quot;—&quot; when no rate is available.
      </p>
    </div>
  );
}
