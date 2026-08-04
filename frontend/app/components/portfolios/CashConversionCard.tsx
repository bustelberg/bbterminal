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
import CashConversionInputsModal from './CashConversionInputsModal';
import { cashConversionByYear, type CashConversionInputs } from './cashConversionData';
import { meanOf, paddedDomain , xToPeriod } from './marginData';

/**
 * Cash-conversion card: Free Cash Flow ÷ Net Income per fiscal year, on a LINEAR % axis. Whether
 * the reported profit turns into money — profit you cannot bank is an opinion about revenue
 * recognition. Click through to the two base lines per company.
 *
 * ⚠ 100% IS NOT THE CEILING. Depreciation ahead of capex converts more cash than the accounts book
 * as profit, so a durable reading above 100% is a compliment (ASML 2025: 11,027.3 / 9,609.4 =
 * 114.8%; Apple 88.2%). The reference line sits at 100 because that is the break-even, not the max.
 *
 * ⚠ A LOSS HAS NO CONVERSION. Net income ≤ 0 is a hole, never a negative percentage: a loss-maker
 * with positive cash flow would read as burning cash, and two companies could show −80% for
 * opposite reasons. A negative FCF against POSITIVE earnings IS kept — that is the finding.
 *
 * ⚠ SCOPE-MISMATCHED BY CONSTRUCTION, deliberately: FCF is whole-company cash while Net Income is
 * the SHAREHOLDERS' line, so a group with large minorities reads high (Mitsui: 34,378 vs 46,910).
 * The alternative would mismatch EPS and every other card on this tab — see `_METRIC_CODES`.
 *
 * ⚠ THE RATIO IS DERIVED HERE from the raw lines (`cashConversionByYear`), so the line, the tiles and
 * the drill-down are one computation. Aggregation is a weight-weighted average of per-company
 * ratios — currency-safe, unlike summing mixed-currency amounts. Mirrors {@link ./SbcOcfCard}.
 */

export default function CashConversionCard({ holdingsTarget, holdingsName, sbcCorrection = true }: {
  holdingsTarget: Target; holdingsName?: string | null;
  /** Tab-level toggle — see `sbcCorrection`. */
  sbcCorrection?: boolean;
}) {
  const [data, setData] = useState<CashConversionInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/cash-conversion-inputs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(holdingsTarget),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as CashConversionInputs);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [holdingsTarget]);

  const marginByYr = useMemo(
    () => cashConversionByYear(data?.rows ?? [], sbcCorrection), [data, sbcCorrection]);

  const chartData = useMemo(() => (
    [...marginByYr.keys()].sort((a, b) => a - b).map((year) => ({ year, margin: marginByYr.get(year) ?? null }))
  ), [marginByYr]);

  const avg = meanOf([...marginByYr.values()]);
  const latestYear = Math.max(-Infinity, ...marginByYr.keys());
  const latest = Number.isFinite(latestYear) ? marginByYr.get(latestYear) ?? null : null;
  const pct = (v: number | null) => (v == null ? '—' : `${v.toFixed(1)}%`);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <h4 className="text-base font-semibold text-fg-strong">
        {sbcCorrection ? '(FCF − SBC) / Net Income' : 'FCF / Net Income'}
      </h4>

      {data == null && !err ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : err ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
      ) : marginByYr.size === 0 ? (
        <p className="text-[11px] text-fg-faint py-16 text-center">No FCF / net-income figures ingested to compute a conversion.</p>
      ) : (
        <>
          <div className="flex flex-wrap gap-2">
            <Stat label="Avg" value={pct(avg)} color={chartTheme.accent}
              info={<InfoTip content={<AspectCard
                what="Average FCF ÷ Net Income over the years shown — how much of the reported profit turned into cash."
                where="Computed here — Free Cash Flow ÷ Net Income per year, weight-averaged across holdings. ⚠ FCF is whole-company cash while Net Income is the SHAREHOLDERS' line, so a group with large minorities reads high."
                when="The years on the chart."
                how="⚠ 100% IS BREAK-EVEN, NOT A CEILING — above it the business converts more cash than it books as profit (depreciation ahead of capex), which is a compliment. Persistently below it means the earnings are not turning into money. A LOSS has no conversion at all, so that year is a hole rather than a negative percentage." />} />} />
            <Stat label="Latest" value={pct(latest)} color={chartTheme.accent} />
          </div>

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis dataKey="year" tickFormatter={xToPeriod} tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
                <YAxis domain={paddedDomain([...marginByYr.values()])} tick={{ fontSize: 11, fill: chartTheme.axisTick }} width={48}
                  tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
                  formatter={(v) => [`${typeof v === 'number' ? v.toFixed(1) : '—'}%`, 'FCF / Net Income']} />
                <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
                {/* ⚠ 100 IS THE MEANINGFUL LINE ON THIS CHART, NOT 0. Crossing it is the event —
                    profit converting to cash or not — whereas 0 only matters in the rare year FCF
                    goes negative. Drawn in recessive grey: it is a reference, not a series. */}
                <ReferenceLine y={100} stroke={chartTheme.axisTick} strokeDasharray="2 4" strokeOpacity={0.5} />
                {avg != null && <ReferenceLine y={avg} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />}
                <Line dataKey="margin" name="margin" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5"
                title="The faint dotted line is 100% — profit converting fully into cash. Above it is better, not an error.">
                <span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accent }} />
                FCF / Net Income (avg dashed · 100% dotted)
              </span>
            </div>
          </div>
        </>
      )}

      {showInputs && (
        <CashConversionInputsModal target={holdingsTarget} portfolioName={holdingsName} onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
