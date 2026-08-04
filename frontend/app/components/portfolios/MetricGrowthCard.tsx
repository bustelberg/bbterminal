'use client';

import { useMemo, useState } from 'react';
import {
  CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { logLinearFit } from '../../../lib/trendFit';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import HoldingsRevenueModal, { type Target } from './HoldingsRevenueModal';
import HoldingsIngestPanel from './HoldingsIngestPanel';
import { noteFor, reportingLine, whyNoLine, type BlendNote } from './blendNotes';
import { paddedLogDomain , xToPeriod } from './marginData';

/**
 * One "Long Equity" growth card: a metric per fiscal year on a LOG axis with an exponential-trend
 * overlay, its R²/CAGR, and a click-through to the per-holding table. A repeatable unit — Revenue
 * and FCF/share are two instances of it; more slot into the grid the same way.
 *
 * ⚠ R² IS COMPUTED FROM THE POINTS ON SCREEN (`logLinearFit`), so the headline can't disagree with
 * the plotted line. The company series is EXTRACTED from `data.metrics` (fetched once by the tab),
 * matching either section spelling.
 */

export type MetricCfg = {
  title: string;                 // 'Revenue' | 'FCF / share' | 'ROIC'
  noun: string;                  // 'revenue' | 'FCF/share' | 'ROIC' — for labels/messages
  codes: string[];               // company-metric codes (both section spellings)
  benchmarkMetric: string;       // the `metric` param for the holdings drill-down endpoint
  unit: 'millions' | 'per_share' | 'percent' | 'shares';   // 'shares' = a count, no currency
  // 'growth' = a compounding series on a LOG axis with an exponential trend + R²/CAGR;
  // 'ratio'  = a % on a LINEAR axis with an average line (a ratio doesn't compound).
  kind?: 'growth' | 'ratio';
};

type MetricRow = { metric_code: string; target_date: string; numeric_value: number | null };

export function Stat({ label, value, tone, color, info }: {
  label: string; value: string; tone?: string; color?: string; info?: React.ReactNode;
}) {
  // `color` (a chart hex) ties the tile to its line — a coloured left bar + matching value ink.
  return (
    <div className="rounded-lg border border-neutral-800/40 bg-inset px-3 py-2 min-w-[6.5rem]"
      style={color ? { borderLeft: `3px solid ${color}` } : undefined}>
      <div className="flex items-center gap-1 text-[11px] uppercase tracking-wide text-fg-muted">{label}{info}</div>
      <div className={`font-mono text-xl font-semibold leading-tight ${color ? '' : (tone ?? 'text-fg-strong')}`}
        style={color ? { color } : undefined}>{value}</div>
    </div>
  );
}

export default function MetricGrowthCard({
  cfg, metrics, isAgg, currency, holdingsTarget, holdingsName, ingestIsin, onIngested,
  blendNotes, onReloadMetrics, cadence = 'annual',
}: {
  cfg: MetricCfg;
  /** 'annual' = one point per fiscal year. 'quarterly' = one TRAILING-TWELVE-MONTH point per
   *  quarter — quarterly frequency, annual scope, so it is comparable with the annual line and
   *  free of the seasonality raw quarters would carry. */
  cadence?: 'annual' | 'quarterly';
  metrics: MetricRow[] | null;   // null = still loading (the tab's company fetch)
  isAgg: boolean;
  currency?: string | null;
  holdingsTarget: Target;
  holdingsName?: string | null;  // the portfolio/company the drill-down is for
  // Portfolio only: why a metric the holdings DO carry produced no blended line. Absent for a
  // metric nobody reports — that one really is "not ingested". See `blendNotes`.
  blendNotes?: Record<string, BlendNote>;
  // Reload just the metrics fetch (the four growth cards share it), NOT the whole tab — the
  // derived cards each refetch their own inputs on a re-key and none of that is needed here.
  onReloadMetrics?: () => void;
  // Single-company only: its ISIN + a callback to reload the tab's metrics after an ingest, so an
  // empty card can fetch this company's financials from GuruFocus (which brings every growth line
  // at once — revenue and shares are the same 192-company set). A portfolio ingests per-row in the
  // drill-down instead, so this is left undefined there.
  ingestIsin?: string;
  onIngested?: () => void;
}) {
  const [showHoldings, setShowHoldings] = useState(false);
  const [busy, setBusy] = useState(false);
  const [outcome, setOutcome] = useState<string | null>(null);
  const isRatio = cfg.kind === 'ratio';

  // A GuruFocus ingest outcome → a plain sentence. Every non-success is a real answer (see the
  // backend's `classify_fetch_outcome`), so it's stated, never swallowed.
  const explain = (status: string | undefined, detail: string | undefined, http: number) => {
    switch (status) {
      case 'unsubscribed': return 'Unsubscribed — this listing’s exchange is outside our GuruFocus subscription.';
      case 'no_data': return 'No data — GuruFocus has no fundamentals for this listing.';
      case 'not_equity': return 'Not an equity — fundamentals don’t apply (bond / fund / derivative).';
      case 'not_found': return 'Not found — couldn’t resolve this ISIN to a GuruFocus listing.';
      case 'error': return detail || 'Fetch failed.';
      default: return detail || status || `HTTP ${http}`;
    }
  };

  const ingest = async () => {
    if (!ingestIsin) return;
    setBusy(true); setOutcome(null);
    try {
      const r = await apiFetch(`${API_URL}/api/earnings/fundamental-coverage/ingest`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ isin: ingestIsin, name: holdingsName ?? undefined }),
      });
      const j = (await r.json().catch(() => null)) as { status?: string; detail?: string } | null;
      if (r.ok && j?.status === 'ingested') {
        // Financials landed (revenue AND shares come together) → reload the tab; the chart appears.
        onIngested?.();
        return;
      }
      // Anything else is a stated reason, and we do NOT reload — nothing changed, so keep it visible.
      setOutcome(explain(j?.status, j?.detail, r.status));
    } catch (e) {
      setOutcome(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  /**
   * ⚠ THE X UNIT IS ALWAYS A YEAR — WHOLE ON ANNUAL, FRACTIONAL ON QUARTERLY — AND THAT IS WHAT
   * KEEPS THE CAGR A **C-A-GR**. `logLinearFit` regresses ln(value) on this axis, so its slope is
   * "per x unit". Bucketing quarterly points 0,1,2,3… would make the slope per QUARTER and the
   * card would print a quarterly growth rate under a label that says annual — a number ~4x too
   * small, entirely plausible, and wrong on every one of the three growth cards at once.
   *
   * A TTM point dated 2026-03-31 sits at 2026.25, so four of them span exactly 1.0 on the axis and
   * the fitted slope is per year by construction. R² is unaffected (it is scale-free).
   */
  const points = useMemo(() => {
    const rows = metrics ?? [];
    const codes = new Set(cfg.codes);
    const byX = new Map<number, { date: string; value: number }>();
    for (const m of rows) {
      if (!codes.has(m.metric_code) || m.numeric_value == null) continue;
      const d = String(m.target_date);
      const y = parseInt(d.slice(0, 4), 10);
      if (y < 2015) continue;   // charts start from 2015, like the holdings/margin views
      // Annual rows keep one point per calendar year (the latest); TTM rows are already one per
      // quarter, so each gets its own x and nothing collapses.
      const x = cadence === 'quarterly'
        ? y + (Math.ceil(parseInt(d.slice(5, 7), 10) / 3) - 1) / 4
        : y;
      const cur = byX.get(x);
      if (!cur || d > cur.date) byX.set(x, { date: d, value: m.numeric_value });
    }
    return [...byX.entries()].map(([year, v]) => ({ year, value: v.value })).sort((a, b) => a.year - b.year);
  }, [metrics, cfg, cadence]);

  // Present only when the blend saw this metric and still drew nothing — the one case where
  // "not ingested" would be false.
  const blendNote = noteFor(blendNotes, cfg.codes);

  const fit = useMemo(() => logLinearFit(points), [points]);            // growth only
  const avg = points.length ? points.reduce((a, p) => a + p.value, 0) / points.length : null;  // ratio only
  const latest = points.length ? points[points.length - 1].value : null;

  const chartData = useMemo(() => {
    const trendByYear = new Map(fit.trend.map((t) => [t.year, t.value]));
    return points.map((p) => ({
      year: p.year,
      // A log axis can't plot ≤ 0; a linear ratio can (ROIC can be negative), so keep it.
      value: isRatio ? p.value : (p.value > 0 ? p.value : null),
      trend: isRatio ? null : (trendByYear.get(p.year) ?? null),
    }));
  }, [points, fit, isRatio]);

  // Log axis: pad the domain (multiplicatively) so the min/max points + trend endpoints don't clip.
  const logDomain = useMemo(() =>
    paddedLogDomain(chartData.flatMap((d) => [d.value, d.trend]).filter((v): v is number => v != null)),
  [chartData]);

  const fmt = (v: number | null | undefined) => {
    if (v == null) return '—';
    if (cfg.unit === 'percent') return `${v.toFixed(1)}%`;
    if (isAgg) return v.toFixed(1);                      // blended growth index
    if (cfg.unit === 'per_share') return v.toFixed(2);
    // 'millions' (currency) and 'shares' (a count) both scale B/T/M; only the prefix differs.
    const a = Math.abs(v);
    if (a >= 1e6) return `${(v / 1e6).toFixed(2)}T`;
    if (a >= 1e3) return `${(v / 1e3).toFixed(1)}B`;
    return `${v.toFixed(0)}M`;
  };
  // A share count carries no currency, so no ccy prefix on 'shares' (nor on a % ratio).
  const ccy = !isAgg && currency && cfg.unit !== 'percent' && cfg.unit !== 'shares' ? `${currency} ` : '';
  const cagr = (v: number | null) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <h4 className="text-base font-semibold text-fg-strong">{cfg.title}</h4>

      {metrics == null ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : points.length === 0 && isAgg && blendNote ? (
        // ⚠ THE HOLDINGS HAVE IT AND THE BLEND COULD NOT DRAW IT. Offering "fetch financials" here
        // would send the reader to spend GuruFocus quota on data that is already in the database.
        // State the fact and the reason, and open the per-holding table instead.
        <div className="py-16 flex flex-col items-center gap-2 text-center px-2">
          <p className="text-[11px] text-fg-soft">{reportingLine(blendNote, cfg.noun)}.</p>
          <p className="text-[11px] text-fg-faint">No portfolio line: {whyNoLine(blendNote)}</p>
          <button type="button" onClick={() => setShowHoldings(true)}
            className="text-xs px-3 py-1 rounded-lg border border-neutral-700 text-fg-soft hover:bg-overlay/5">
            See per holding
          </button>
        </div>
      ) : points.length === 0 && isAgg ? (
        // A portfolio has no single ISIN to fetch — it has N of them, each with its own outcome.
        // The panel runs them one at a time and REPORTS per holding; it stays on screen until the
        // reader chooses "Reload", so a partial success can't hide the holdings that came back
        // empty. Reload is metrics-only: the derived cards' inputs are untouched by this run.
        <HoldingsIngestPanel target={holdingsTarget} metric={cfg.benchmarkMetric}
          noun={cfg.noun} onIngested={onReloadMetrics ?? onIngested} />
      ) : points.length === 0 ? (
        <div className="py-16 flex flex-col items-center gap-3 text-center px-4">
          <p className="text-[11px] text-fg-faint">No {cfg.noun} ingested for this company.</p>
          {ingestIsin && (busy ? (
            <span className="text-xs text-fg-subtle">Fetching from GuruFocus…</span>
          ) : outcome ? (
            <>
              <p className="text-xs text-warn-300 max-w-[28ch]">{outcome}</p>
              <button type="button" onClick={ingest}
                className="text-[11px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-soft hover:bg-overlay/5">
                Try again
              </button>
            </>
          ) : (
            <button type="button" onClick={ingest}
              title="Fetch this company's financials from GuruFocus."
              className="text-xs px-3 py-1 rounded-lg border border-accent-600/40 text-accent-400 hover:bg-overlay/5">
              Fetch financials from GuruFocus
            </button>
          ))}
        </div>
      ) : (
        <>
          <div className="flex flex-wrap gap-2">
            {isRatio ? (
              <>
                <Stat label="Avg" value={fmt(avg)} color={chartTheme.accent}
                  info={<InfoTip content={<AspectCard
                    what={`Average ${cfg.noun} over the years shown.`}
                    where="Computed here from the points below." when={`${points.length} year(s).`}
                    how="A simple mean — a ratio doesn't compound, so there's no growth rate." />} />} />
                <Stat label="Latest" value={fmt(latest)} color={chartTheme.accent} />
              </>
            ) : (
              <>
                <Stat label="R²" value={fit.r2 == null ? '—' : fit.r2.toFixed(2)} color={chartTheme.accent}
                  info={<InfoTip content={<AspectCard
                    what={`How tightly ${cfg.noun} hugs a constant-growth line (0–1).`}
                    where="Computed here — a log-linear regression on the points below."
                    when={`Over the ${fit.n} year(s) shown.`}
                    how={`R² of ln(${cfg.noun}) vs year. 1.0 = perfectly steady compounding; low = lumpy or cyclical.`} />} />} />
                <Stat label="CAGR" value={cagr(fit.cagr)} color={chartTheme.accent}
                  info={<InfoTip content={<AspectCard
                    what="The compound annual growth rate of the fitted trend."
                    where="Computed here from the same fit." when={`Over the ${fit.n} year(s) shown.`}
                    how="e^(slope) − 1 of the log-linear regression." />} />} />
              </>
            )}
          </div>

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowHoldings(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis dataKey="year" tickFormatter={xToPeriod} tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
                {isRatio ? (
                  <YAxis tick={{ fontSize: 11, fill: chartTheme.axisTick }} width={48}
                    tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
                ) : (
                  // Log scale: an exponential (constant-%) growth trend draws as a straight line.
                  <YAxis scale="log" domain={logDomain ?? ['dataMin', 'dataMax']} allowDataOverflow
                    tick={{ fontSize: 11, fill: chartTheme.axisTick }} tickFormatter={(v: number) => fmt(v)} width={60} />
                )}
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
                  formatter={(v, name) => [`${ccy}${fmt(typeof v === 'number' ? v : null)}`, name === 'trend' ? 'Trend' : cfg.title]} />
                {isRatio && <ReferenceLine y={0} stroke={chartTheme.zeroLine} />}
                {isRatio && avg != null && <ReferenceLine y={avg} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />}
                <Line dataKey="value" name="value" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                {!isRatio && <Line dataKey="trend" name="trend" type="monotone" stroke={chartTheme.warn} strokeWidth={2} strokeDasharray="5 3" dot={false} connectNulls />}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accent }} />{cfg.title}{isRatio ? ' (avg dashed)' : ''}</span>
              {!isRatio && (
                <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.warn }} />Trend (R² {fit.r2 == null ? '—' : fit.r2.toFixed(2)})</span>
              )}
            </div>
          </div>
        </>
      )}

      {showHoldings && (
        <HoldingsRevenueModal target={holdingsTarget} metric={cfg.benchmarkMetric} unit={cfg.unit}
          noun={cfg.noun} portfolioName={holdingsName} onClose={() => setShowHoldings(false)} />
      )}
    </div>
  );
}
