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
import { Stat } from './MetricGrowthCard';
import { type Target } from './HoldingsRevenueModal';
import { fcfLabel } from './sbcCorrection';
import FcfSbcYieldInputsModal from './FcfSbcYieldInputsModal';
import { fcfSbcYieldByYear, type FcfSbcYieldInputs } from './fcfSbcYieldData';
import { meanOf, paddedDomain } from './marginData';

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

export default function FcfSbcYieldCard({ holdingsTarget, holdingsName, sbcCorrection = true }: {
  holdingsTarget: Target; holdingsName?: string | null;
  /** Tab-level toggle. ⚠ This card USED to subtract SBC unconditionally. */
  sbcCorrection?: boolean;
}) {
  const [data, setData] = useState<FcfSbcYieldInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/fcf-sbc-yield-inputs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(holdingsTarget),
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
  }, [holdingsTarget]);

  const yieldByYr = useMemo(
    () => fcfSbcYieldByYear(data?.rows ?? [], sbcCorrection), [data, sbcCorrection]);

  const chartData = useMemo(() => (
    [...yieldByYr.keys()].sort((a, b) => a - b).map((year) => ({ year, yld: yieldByYr.get(year) ?? null }))
  ), [yieldByYr]);

  const avg = meanOf([...yieldByYr.values()]);
  const latestYear = Math.max(-Infinity, ...yieldByYr.keys());
  const latest = Number.isFinite(latestYear) ? yieldByYr.get(latestYear) ?? null : null;
  const pct = (v: number | null) => (v == null ? '—' : `${v.toFixed(1)}%`);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <h4 className="text-base font-semibold text-fg-strong">{fcfLabel(sbcCorrection)} yield</h4>

      {data == null && !err ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : err ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
      ) : yieldByYr.size === 0 ? (
        <p className="text-[11px] text-fg-faint py-16 text-center">No FCF / market-cap figures ingested to compute a yield.</p>
      ) : (
        <>
          <div className="flex flex-wrap gap-2">
            <Stat label="Avg" value={pct(avg)} color={chartTheme.accent}
              info={<InfoTip content={<AspectCard
                what="Average (FCF − SBC) ÷ Market Cap over the years shown."
                where="Computed here — the yield per year, weight-averaged across holdings."
                when="The years on the chart."
                how="The cash a buyer earns per euro of price, after removing non-cash stock comp from FCF. Higher = cheaper for the cash it generates." />} />} />
            <Stat label="Latest" value={pct(latest)} color={chartTheme.accent} />
          </div>

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis dataKey="year" tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
                <YAxis domain={paddedDomain([...yieldByYr.values()])} tick={{ fontSize: 11, fill: chartTheme.axisTick }} width={48}
                  tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
                  formatter={(v) => [`${typeof v === 'number' ? v.toFixed(1) : '—'}%`, `${fcfLabel(sbcCorrection)} yield`]} />
                <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
                {avg != null && <ReferenceLine y={avg} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />}
                <Line dataKey="yld" name="yld" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accent }} />{fcfLabel(sbcCorrection)} yield (avg dashed)</span>
            </div>
          </div>
        </>
      )}

      {showInputs && (
        <FcfSbcYieldInputsModal target={holdingsTarget} portfolioName={holdingsName} onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
