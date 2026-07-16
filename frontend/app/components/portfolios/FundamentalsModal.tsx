'use client';

import { useEffect, useState } from 'react';
import {
  Area, CartesianGrid, ComposedChart, Legend, Line, ReferenceArea, ReferenceLine,
  ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import type { FundamentalsResponse, FundamentalSeries, QualityMetric } from '../../../lib/types/api';

/**
 * Is this company fundamentally sound — and are we paying a sensible price for it?
 *
 * FOUR CHARTS, ONE CALL. Each answers a different question, and each misleads without the others:
 *
 *   1. PRICE vs FAIR VALUE   what we pay, against five independent methods
 *   2. YIELD                 what a euro of price BUYS — cash thrown off, and cash handed back
 *   3. ROIC vs WACC          whether the business earns more than its capital costs
 *   4. SAFETY                whether cheap is cheap for a reason
 *
 * ⚠ THE PRICE LINE IS OUR OWN DAILY YFINANCE CLOSE, IN EUR — never GuruFocus's. /portfolios prices
 * everything from `asset_price`; a second vendor here would compare two price universes on a page
 * whose whole claim is that its numbers are comparable. The fair values are GuruFocus's, converted
 * to the SAME EUR — GF denominates them in its own listing's currency, and its listing need not be
 * the one we price. See `_asset_fundamentals`.
 *
 * ⚠ EVERY SERIES SAYS WHAT IT DROPPED. A loss year has no PE; a period with no FX rate cannot be
 * converted. Measured on Apple: the fair values lose 13 of 40 periods to thin FX history and
 * interest coverage 16. A line that quietly skips its bad years is a line about the good ones.
 */

const fmtEur = (v: number | null | undefined) =>
  v == null ? '—' : `€${v.toLocaleString('en-US', { maximumFractionDigits: 2 })}`;
const fmtPct = (v: number | null | undefined) =>
  v == null ? '—' : `${v >= 0 ? '' : ''}${v.toFixed(2)}%`;

/** One series as `{t, v}` on a NUMERIC time axis, for charts whose series have different cadences.
 *
 * ⚠ WHY NOT ONE MERGED FRAME (which is what `frame()` below does, correctly, for charts 2-4).
 * Chart 1 puts a DAILY price (≈3,000 points) beside an ANNUAL band (12). Merged on date, each band
 * series is 12 values among 3,000 rows — and `connectNulls` is then the only reason the band draws
 * as a line at all rather than 12 lonely dots. That makes `connectNulls` load-bearing for the
 * cadence gap, so it cannot also be used to express a REAL break. Per-series data separates the
 * two: each line carries only its own dates, and a null means something.
 *
 * `breakOnNonPositive` maps a value <= 0 to null. A log axis cannot plot it, and bridging it would
 * draw a confident line straight through the years a method said the company had no value —
 * Peter Lynch needs positive earnings growth; EPV <= 0 says the business earns nothing. Measured,
 * a quarter of the band's in-window points are <= 0 (Tesla 33/60, AMD 21/60). The line BREAKS
 * there, and the caption counts them.
 */
function tSeries(points: { date: string; value: number }[], breakOnNonPositive = false) {
  return points.map((p) => ({
    t: Date.parse(p.date),
    v: breakOnNonPositive && p.value <= 0 ? null : p.value,
  }));
}

/** Merge several dated series into one recharts frame keyed by date. */
function frame(series: { label: string; points: { date: string; value: number }[] }[]) {
  const by: Record<string, Record<string, number | string>> = {};
  for (const s of series) {
    for (const p of s.points) {
      by[p.date] = by[p.date] ?? { date: p.date };
      by[p.date][s.label] = p.value;
    }
  }
  return Object.values(by).sort((a, b) => String(a.date).localeCompare(String(b.date)));
}

/** "27 of 40 periods" — shown wherever a series is short of its blob's history. */
function Gap({ s }: { s: FundamentalSeries }) {
  if (!s.dropped) return null;
  return (
    <span className="text-warn-400" title={`${s.label} has no value for ${s.dropped} of the ${s.period_count} periods in this company's history — GuruFocus published none (a loss year has no PE, an unlevered year no interest coverage), or we had no FX rate that far back. The line skips them; it does not interpolate.`}>
      {` ${s.points?.length ?? 0}/${s.period_count}`}
    </span>
  );
}

/** The quality verdict: four numbers, read in two seconds.
 *
 * ⚠ FOUR NUMBERS AND NOT ONE SCORE, DELIBERATELY. A composite 0-100 would hide the disagreement
 * between them, and the disagreement IS the finding — Intel reads a passable +3.1pp spread while
 * its ROIC fell 17 points; a single score averages the melting moat away. (GuruFocus sells a GF
 * Score; outsourcing the judgement is the thing being avoided.)
 *
 * ⚠ "NOT MEASURED" IS NOT "BAD". A bank has no ROIC and no gross margin AT ALL — structurally
 * absent, not empty — and its cash conversion tracks its loan book rather than its collections.
 * All four are therefore inapplicable to one, which the strip SAYS rather than rendering four
 * failures.
 */
function QualityStrip({ metrics }: { metrics: QualityMetric[] }) {
  const measured = metrics.filter((m) => m.status === 'ok' || m.status === 'fail');
  if (metrics.length && !measured.length) {
    return (
      <div className="bg-inset border border-neutral-800/40 rounded-lg px-3 py-2 text-[11px] text-fg-subtle">
        <strong className="text-fg">These four cannot judge this company.</strong> Every one of them
        is built on ROIC, gross margin or cash conversion, and a bank has none of the three in a
        comparable sense — lending IS the business, so its capital is its product and its operating
        cash flow tracks its loan book. It needs its own measures (net interest margin, efficiency
        ratio, cost of risk), not these.
      </div>
    );
  }
  const tone = (s?: string) =>
    s === 'ok' ? 'text-pos-400 border-pos-500/25 bg-pos-500/[0.07]'
      : s === 'fail' ? 'text-neg-400 border-neg-500/25 bg-neg-500/[0.07]'
        : 'text-fg-faint border-neutral-800/40';
  const fmt = (m: QualityMetric) => {
    if (m.status === 'n_a') return 'n/a';
    if (m.value == null) return '—';
    return m.unit === 'x' ? `${m.value.toFixed(2)}×`
      : `${m.value >= 0 ? '+' : ''}${m.value.toFixed(1)}pp`;
  };
  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
      {metrics.map((m) => (
        <div key={m.key} title={m.note ?? ''}
          className={`rounded-lg border px-3 py-2 ${tone(m.status)}`}>
          <div className="text-[9px] uppercase tracking-wide opacity-70">{m.label}</div>
          <div className="font-mono text-base font-semibold leading-tight">{fmt(m)}</div>
          <div className="text-[9px] opacity-60">
            {m.status === 'n_a' ? 'not applicable'
              : m.status === 'unknown' ? `only ${m.periods} periods`
                : `${m.periods} periods`}
          </div>
        </div>
      ))}
    </div>
  );
}

function Panel({ title, sub, children }: {
  title: string; sub: React.ReactNode; children: React.ReactNode;
}) {
  return (
    <div className="bg-card border border-neutral-800/40 rounded-xl p-4 min-w-0">
      <h4 className="text-xs font-semibold text-fg-strong">{title}</h4>
      <p className="text-[10px] text-fg-faint mt-0.5 mb-2 leading-relaxed">{sub}</p>
      <div className="h-56">
        <ResponsiveContainer width="100%" height="100%">{children as never}</ResponsiveContainer>
      </div>
    </div>
  );
}

const axis = { stroke: chartTheme.axisTick, fontSize: 9 };
const grid = { stroke: chartTheme.grid, strokeDasharray: '3 3' };
const tip = chartTheme.tooltip;

export default function FundamentalsModal({ isin, fonds, onClose }: {
  isin: string; fonds: string; onClose: () => void;
}) {
  const [d, setD] = useState<FundamentalsResponse | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let dead = false;
    void (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/fundamentals/isin/${isin}`);
        if (!r.ok) {
          const body = await r.json().catch(() => ({}));
          throw new Error(body?.detail ?? `HTTP ${r.status}`);
        }
        const b = (await r.json()) as FundamentalsResponse;
        if (!dead) setD(b);
      } catch (e) {
        if (!dead) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { dead = true; };
  }, [isin]);

  const fair = d?.fair_values_eur ?? [];
  const priceLine = tSeries(d?.price_eur ?? []);
  const bandLines = fair.map((s) => ({ s, data: tSeries(s.points ?? [], true) }));
  const nonPositive = fair.reduce((n, s) => n + (s.non_positive ?? 0), 0);
  const bandDropped = fair.reduce((n, s) => n + (s.dropped ?? 0), 0);

  return (
    <div className="fixed inset-0 z-50 bg-scrim/60 flex items-start justify-center overflow-auto p-6"
      onClick={onClose}>
      {/* A real dialog role: it is what a screen reader needs, and it is the only stable handle on
          this thing from the outside — `locator('div').filter({hasText})` resolves to the
          innermost match, which is the header, not the modal. */}
      <div role="dialog" aria-modal="true" aria-label={`Fundamentals for ${fonds}`}
        className="bg-page rounded-xl border border-neutral-800/40 w-full max-w-6xl my-4"
        onClick={(e) => e.stopPropagation()}>
        <div className="flex items-start justify-between gap-3 px-5 py-4 border-b border-neutral-800/40">
          <div className="min-w-0">
            <h3 className="text-base font-mono font-semibold text-fg-strong">{fonds}</h3>
            <p className="text-[11px] text-fg-subtle mt-0.5">
              Fundamentally sound? · <span className="font-mono">{isin}</span>
              {d && <> · <span className="font-mono">{d.symbol}</span> ·{' '}
                <span className="font-mono">{d.period_count}</span> periods</>}
            </p>
          </div>
          <button onClick={onClose} className="text-fg-faint hover:text-fg text-lg leading-none">✕</button>
        </div>

        {!d && !err && <p className="px-5 py-8 text-xs text-fg-subtle">Reading the accounts…</p>}
        {err && (
          <div className="m-5 bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">
            {err}
          </div>
        )}

        {d && (
          <div className="p-5 space-y-3">
            {/* ⚠ A non-home listing's history has HOLES — the band would be drawn across them. */}
            {!d.is_home && (
              <div className="bg-warn-500/10 border border-warn-500/20 rounded-lg px-3 py-2 text-[11px] text-warn-300">
                <strong>{d.symbol}</strong> is not this ISIN&apos;s home listing. GuruFocus&apos;s
                history on a secondary listing has gaps (Apple: 91 records on Nasdaq, 63 on Zurich
                with a five-year hole), so treat every line here as indicative.
              </div>
            )}

            {/* The verdict, above the evidence. The charts answer "are we paying too much";
                these four answer "is it worth owning at any price", which is the question that
                comes first — and the one the charts below cannot be read without. */}
            <QualityStrip metrics={d.quality ?? []} />

            <div className="grid gap-3 lg:grid-cols-2">
              {/* ── 1 ─────────────────────────────────────────────────────────────────────── */}
              <Panel title="Price vs fair value"
                sub={<>What we pay against five independent methods. The band&apos;s WIDTH is the
                  signal: converging means the value is knowable, fanning apart means it is not —
                  which a single fair-value number would hide. Price is our own daily yfinance
                  close in EUR; the methods are GuruFocus, converted to the same EUR.{' '}
                  <span title="The gap between price and fair value is a RATIO — 'twice its worth' should look the same at €17 as at €278. Only a log axis draws equal ratios as equal distances; on a linear one this company's first decade would be a pixel at the bottom (prices here range 4.5x to 353x over the window).">
                    Log scale, because the gap is a ratio.</span>
                  {/* ⚠ TWO DIFFERENT ABSENCES, AND THEY ARE NOT THE SAME SENTENCE. `dropped` = the
                      method published nothing for that period; `non_positive` = it published a
                      value and the value is ≤ 0, which is an ANSWER ("no value") a log axis cannot
                      draw. Collapsing them would report a loss-making decade as missing data. */}
                  {bandDropped > 0 && (
                    <span className="text-warn-400"> The methods cover{' '}
                      {fair[0]?.points?.length ?? 0}/{fair[0]?.period_count ?? 0} periods —
                      GuruFocus published none for the rest.</span>
                  )}
                  {nonPositive > 0 && (
                    <span className="text-warn-400"> {nonPositive} of the band&apos;s points are
                      ≤ 0 — a method with no value for a loss-making year (Peter Lynch needs
                      positive earnings growth; Earnings Power ≤ 0 says the business earns
                      nothing). Those lines BREAK rather than bridge the years.</span>
                  )}</>}>
                {/* ⚠ PER-SERIES DATA ON A NUMERIC TIME AXIS, not one merged frame. The price is
                    DAILY and the band ANNUAL; merged on date, `connectNulls` would be the only
                    thing drawing the band as a line at all — which would make it load-bearing for
                    the CADENCE gap and therefore unusable to express a real break. Apart, a null
                    means exactly one thing: this method had no value here. */}
                <ComposedChart>
                  <CartesianGrid {...grid} />
                  <XAxis dataKey="t" type="number" scale="time" domain={['dataMin', 'dataMax']}
                    {...axis} minTickGap={40}
                    tickFormatter={(t: number) => String(new Date(t).getFullYear())} />
                  {/* Log: equal ratios, equal distances. `dataMin`/`dataMax` because 'auto' pads
                      toward 0, which a log axis cannot reach. */}
                  <YAxis scale="log" domain={['dataMin', 'dataMax']} allowDataOverflow
                    {...axis} tickFormatter={(v: number) => `€${v >= 100 ? v.toFixed(0) : v.toFixed(1)}`} />
                  <Tooltip {...tip}
                    labelFormatter={(t) => new Date(Number(t)).toISOString().slice(0, 10)}
                    formatter={(v) => fmtEur(typeof v === 'number' ? v : null)} />
                  <Legend wrapperStyle={{ fontSize: 9 }} />
                  {bandLines.map(({ s, data }, i) => (
                    <Line key={s.label} data={data} type="monotone" dataKey="v" name={s.label}
                      dot={false} strokeWidth={1} strokeDasharray="4 3" connectNulls={false}
                      stroke={chartTheme.series[i % chartTheme.series.length]} />
                  ))}
                  {/* Drawn last = on top. The price is the subject; the methods are the context. */}
                  <Line data={priceLine} type="monotone" dataKey="v" name="Price" dot={false}
                    strokeWidth={2} stroke={chartTheme.accentStrong} />
                </ComposedChart>
              </Panel>

              {/* ── 2 ─────────────────────────────────────────────────────────────────────── */}
              <Panel title="What a euro of price buys"
                sub={<>A yield, unlike a multiple, is comparable across names. FCF yield is what the
                  business throws off; Greenblatt&apos;s is EBIT/EV, so leverage cannot flatter it;
                  shareholder yield is what actually reaches us — dividends, buybacks, debt paid
                  down. {(d.yields ?? []).map((s) => <span key={s.label}>{s.label}<Gap s={s} />{' '}</span>)}</>}>
                <ComposedChart data={frame((d.yields ?? []).map((s) => ({
                  label: s.label, points: (s.points ?? []) as { date: string; value: number }[],
                })))}>
                  <CartesianGrid {...grid} />
                  <XAxis dataKey="date" {...axis} minTickGap={40} />
                  <YAxis {...axis} tickFormatter={(v: number) => `${v}%`} />
                  <Tooltip {...tip} formatter={(v) => fmtPct(typeof v === "number" ? v : null)} />
                  <Legend wrapperStyle={{ fontSize: 9 }} />
                  <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
                  {(d.yields ?? []).map((s, i) => (
                    <Line key={s.label} type="monotone" dataKey={s.label} dot={false}
                      strokeWidth={1.5} connectNulls
                      stroke={chartTheme.series[i % chartTheme.series.length]} />
                  ))}
                </ComposedChart>
              </Panel>

              {/* ── 3 ─────────────────────────────────────────────────────────────────────── */}
              <Panel title="ROIC vs WACC — is the business worth owning"
                sub={!d.has_roic
                  ? <span className="text-warn-400">This company&apos;s industry template
                    ({d.template ?? 'unknown'}) has no meaningful ROIC — a bank&apos;s capital IS
                    its product. That is an answer about the template, not a gap.</span>
                  : <>The spread is the whole point: a business earning BELOW its cost of capital
                    destroys value however cheap it looks, and one earning far above it deserves
                    its multiple. This is what makes the other three charts readable.</>}>
                <ComposedChart data={frame((d.returns ?? []).map((s) => ({
                  label: s.label, points: (s.points ?? []) as { date: string; value: number }[],
                })))}>
                  <CartesianGrid {...grid} />
                  <XAxis dataKey="date" {...axis} minTickGap={40} />
                  <YAxis {...axis} tickFormatter={(v: number) => `${v}%`} />
                  <Tooltip {...tip} formatter={(v) => fmtPct(typeof v === "number" ? v : null)} />
                  <Legend wrapperStyle={{ fontSize: 9 }} />
                  <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
                  <Area type="monotone" dataKey="ROIC" stroke={chartTheme.pos} strokeWidth={2}
                    fill={chartTheme.pos} fillOpacity={0.12} connectNulls />
                  <Line type="monotone" dataKey="WACC" dot={false} strokeWidth={1.5}
                    strokeDasharray="4 3" stroke={chartTheme.neg} connectNulls />
                </ComposedChart>
              </Panel>

              {/* ── 4 ─────────────────────────────────────────────────────────────────────── */}
              <Panel title="Is cheap, cheap for a reason?"
                sub={<>The value-trap screen. Piotroski F ≥ 7 is a strengthening business; Altman Z
                  below 1.8 is distress; Beneish M above −1.78 flags earnings that may be managed.
                  Plotted on one axis because the SHAPE matters more than the levels — a score
                  falling for years says more than today&apos;s number.
                  {(d.safety ?? []).map((s) => <span key={s.label}>{' '}{s.label}<Gap s={s} /></span>)}</>}>
                <ComposedChart data={frame((d.safety ?? [])
                  .filter((s) => s.label !== 'Interest coverage')      // its scale dwarfs the rest
                  .map((s) => ({
                    label: s.label, points: (s.points ?? []) as { date: string; value: number }[],
                  })))}>
                  <CartesianGrid {...grid} />
                  <XAxis dataKey="date" {...axis} minTickGap={40} />
                  <YAxis {...axis} />
                  <Tooltip {...tip} />
                  <Legend wrapperStyle={{ fontSize: 9 }} />
                  {/* Altman's distress zone, in ink rather than prose. */}
                  <ReferenceArea y1={-5} y2={1.8} fill={chartTheme.neg} fillOpacity={0.06} />
                  <ReferenceLine y={-1.78} stroke={chartTheme.warn} strokeDasharray="2 2" />
                  {(d.safety ?? []).filter((s) => s.label !== 'Interest coverage').map((s, i) => (
                    <Line key={s.label} type="monotone" dataKey={s.label} dot={false}
                      strokeWidth={1.5} connectNulls
                      stroke={chartTheme.series[i % chartTheme.series.length]} />
                  ))}
                </ComposedChart>
              </Panel>
            </div>

            <p className="text-[10px] text-fg-faint leading-relaxed">
              Price is <strong>yfinance daily close, in EUR</strong> — the same series every model
              portfolio on this page is priced from. The fair values, yields, returns and scores are
              GuruFocus&apos;s, off one already-cached blob ({d.fetched ? 'fetched now' : 'no API call'}
              ). Fair values are converted to EUR at each period&apos;s own rate, because GuruFocus
              denominates them in <span className="font-mono">{d.symbol}</span>&apos;s trading
              currency ({d.currency}) and our price is{' '}
              <span className="font-mono">{d.yahoo_symbol}</span> ({d.price_currency}) — two
              listings of one share class, and their gap would otherwise read as mispricing.
              {' '}⚠ These are <strong>restated</strong> figures: a 2019 ROIC here is what 2019 looks
              like now, not what anyone could see in 2019.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
