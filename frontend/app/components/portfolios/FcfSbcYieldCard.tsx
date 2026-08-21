'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { useLang } from '../../../lib/i18n';
import { chartTitle } from './longEquityCopy';
import { pairedSpan, RatioStats } from './CardStats';
import { LegendItem } from './ChartLegend';
import { type Target } from './HoldingsRevenueModal';
import { fcfLabel } from './sbcCorrection';
import FcfSbcYieldInputsModal from './FcfSbcYieldInputsModal';
import { fcfSbcYieldByYear, type FcfSbcYieldInputs } from './fcfSbcYieldData';
import DailyToggle from './DailyToggle';
import { paddedDomain, xToMonth, xToPeriod } from './marginData';
import { periodAxis } from '../../../lib/chartAxis';
import { benchNote, benchmarkFirst, mergeSeries, useBenchInputs, withBench, type BenchTarget } from './benchSeries';

/**
 * FCF-SBC yield card: (Free Cash Flow − Stock-Based Compensation) ÷ Market Cap per fiscal year, on
 * a LINEAR % axis (a ratio, not a compounding series — no log / exponential trend). The cash yield
 * a buyer earns at that year's price, net of non-cash stock comp. Higher = cheaper for the cash it
 * throws off. Click through to the three base lines per company.
 *
 * ⚠ THE RATIO IS DERIVED HERE from the raw lines (`fcfSbcYieldByYear`), so the line, the tiles and
 * the drill-down are one computation. Aggregation is a weight-weighted average of per-company
 * yields — currency-safe, unlike summing mixed-currency amounts. Mirrors {@link ./MarginCard}.
 */

export default function FcfSbcYieldCard({ holdingsTarget, holdingsName, sbcCorrection = true, benchTarget }: {
  holdingsTarget: Target; holdingsName?: string | null;
  /** Tab-level toggle. ⚠ This card USED to subtract SBC unconditionally. */
  sbcCorrection?: boolean;
  /** The index drawn beside the book — same endpoint, same helper. See `benchSeries`. */
  benchTarget?: BenchTarget | null;
}) {
  // ⚠ READ FROM THE STORE, NOT DRILLED THROUGH `LongEquityTab` AS A PROP — see the same
  // note on the sibling cards. `useLang` is an external store (`lib/i18n.ts`).
  const [lang] = useLang();
  const [data, setData] = useState<FcfSbcYieldInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);
  /** Per-card daily override — see {@link ./DailyToggle} for why it is not on the tab control. */
  const [daily, setDaily] = useState(false);
  const target = useMemo(
    () => (daily ? { ...holdingsTarget, cadence: 'daily' } : holdingsTarget),
    [holdingsTarget, daily]);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/fcf-sbc-yield-inputs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(target),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as FcfSbcYieldInputs);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [target]);

  const yieldByYr = useMemo(
    () => fcfSbcYieldByYear(data?.rows ?? [], sbcCorrection), [data, sbcCorrection]);

  const [benchData, benchErr] = useBenchInputs<FcfSbcYieldInputs>('fcf-sbc-yield-inputs', benchTarget);
  const benchByYr = useMemo(
    () => (benchData ? fcfSbcYieldByYear(benchData.rows, sbcCorrection) : null), [benchData, sbcCorrection]);

  const note = benchNote(benchTarget, benchData, benchErr, benchByYr);

  const chartData = useMemo(
    () => mergeSeries(yieldByYr, benchByYr, 'yld'), [yieldByYr, benchByYr]);

  const own = holdingsName ?? `${fcfLabel(sbcCorrection)} yield`;
  /**
   * The book's figures and the benchmark's, over the ONE window both lines cover — see
   * `CardStats`/`sharedSpan`. ⚠ COMPUTED ONCE: `own.avg` is BOTH the tile and the dashed average
   * line on the chart below, so the card cannot plot a mean it does not print.
   */
  const stats = useMemo(() => pairedSpan(yieldByYr, benchByYr), [yieldByYr, benchByYr]);
  const avg = stats.own.avg;
  const pct = (v: number | null) => (v == null ? '—' : `${v.toFixed(1)}%`);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <div className="flex items-baseline justify-between gap-2">
        <h4 className="text-base font-semibold text-fg-strong">{chartTitle(lang, 'fcfYield', sbcCorrection)}</h4>
        <DailyToggle on={daily} onChange={setDaily}
          note={'Daily: FCF − SBC stays flat between fiscal periods while the market cap moves '
            + 'every trading day — rebuilt as the day’s close × shares outstanding, since '
            + 'GuruFocus publishes a market cap only per fiscal period. Off, it follows the tab.'} />
      </div>

      {data == null && !err ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : err ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
      ) : yieldByYr.size === 0 ? (
        <p className="text-[12px] text-fg-faint py-16 text-center">No FCF / market-cap figures ingested to compute a yield.</p>
      ) : (
        <>
          <RatioStats stats={stats} benchLabel={benchTarget?.label} fmt={pct}
            avgInfo={<InfoTip content={<AspectCard
              what="Average (FCF − SBC) ÷ Market Cap over the years shown."
              where="Computed here — the yield per year, weight-averaged across holdings."
              when="The years on the chart."
              how="The cash a buyer earns per euro of price, after removing non-cash stock comp from FCF. Higher = cheaper for the cash it generates." />} />} />

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis {...periodAxis(daily ? xToMonth : xToPeriod)} />
                <YAxis domain={paddedDomain(withBench(yieldByYr.values(), benchByYr))} tick={{ fontSize: 12, fill: chartTheme.axisTick }} width={48}
                  tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }} itemSorter={benchmarkFirst}
                  formatter={(v, n) => [`${typeof v === 'number' ? v.toFixed(1) : '—'}%`, n === 'bench' ? (benchTarget?.label ?? 'Benchmark') : own]} />
                <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
                {avg != null && <ReferenceLine y={avg} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />}
                {/* ⚠ NO DOTS ON A DAILY SERIES — 2,700 markers is a solid band, not a line. */}
                <Line dataKey="yld" name="yld" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={daily ? false : { r: 2.5 }} connectNulls />
                {benchByYr && <Line dataKey="bench" name="bench" type="monotone" stroke={chartTheme.pos} strokeWidth={2} dot={{ r: 2 }} connectNulls />}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <LegendItem color={chartTheme.accent} label={own} />
              {avg != null && <LegendItem color={chartTheme.accent} stroke="dashed"
                label={`${own} average`} />}
              {benchByYr && <LegendItem color={chartTheme.pos} label={benchTarget?.label} />}
              {note && (
                <span className="text-fg-faint" title="An overlay that simply does not appear is indistinguishable from an index that matches this book exactly. Full detail is in the console.">
                  {note}
                </span>
              )}
            </div>
          </div>
        </>
      )}

      {showInputs && (
        <FcfSbcYieldInputsModal target={target} portfolioName={holdingsName}
          benchTarget={benchTarget} benchLabel={benchTarget?.label ?? null}
          onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
