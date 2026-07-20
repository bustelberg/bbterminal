'use client';

import { useEffect, useState } from 'react';
import {
  Bar, BarChart, CartesianGrid, LabelList, Legend, ResponsiveContainer, Tooltip,
  XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { Provenance } from '../../../lib/provenance';
import type { ModelPortfolioAnalysis } from '../../../lib/types/api';
import AttributionPanel from './AttributionPanel';
import BucketDetailPanel from './BucketDetailPanel';
import PerformanceTable, { type PerfWindow } from './PerformanceTable';
import { type Basket } from './PerformanceModal';

type RiskWindows = { windows?: PerfWindow[] };

/**
 * A model portfolio's composition — sector / region / currency — beside the SP500 benchmark's.
 *
 * TWO SERIES, TWO HUES, VALIDATED. The obvious pair (accent blue + `compare` violet, the app's
 * standard A/B) FAILS colourblind separation: ΔE 4.9 under deuteranopia, i.e. one colour to a
 * deuteranope. Blue + amber scores 103. That is not a judgement call and it was not eyeballed —
 * `dataviz/scripts/validate_palette.js` computes it. The amber sits a hair under 3:1 contrast on
 * white, which obliges relief, so every bar carries a DIRECT VALUE LABEL (in ink, never in the
 * series colour — text wears text tokens).
 *
 * ⚠ THE "Fund (not looked through)" BAR IS THE HONEST ONE. We hold no constituent data for an
 * ETF, and its listing tells you nothing about its contents: 24 of the 26 held ETFs have a
 * "sector" of literally `etf` or `Equity`; an Amsterdam-listed MSCI World ETF is not European
 * exposure; quoted in EUR it still holds mostly USD assets. So funds are bucketed, not
 * decomposed. A portfolio that is 40% ETF shows a 40% bar meaning "we cannot see inside this" —
 * which is true, and far better than a confident, invented split.
 */
const SERIES = {
  portfolio: chartTheme.accent,   // #3b82c9 — the thing of interest
  benchmark: chartTheme.warn,     // #c0891a — CVD-separated from it (ΔE 103), not violet (4.9)
};

const AXIS_LABEL: Record<string, string> = {
  sector: 'Sector',
  region: 'Region',
  currency: 'Currency',
};

const AXIS_NOTE: Record<string, string> = {
  sector: 'Equities by their own sector; every fund in one bucket (we hold no look-through).',
  region: "Where the issuer IS — domicile, else its ISIN's country. Never the venue we price it on.",
  currency: "The company's own reporting currency — NOT the venue we happen to price it on.",
};

/** A bar's own value, direct-labelled. Empty for a zero — a "0%" label on an absent bucket is
 *  ink spent saying nothing, and it crowds the ones that matter. */
const pct = (v: unknown) => {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? `${n.toFixed(0)}%` : '';
};

type Axis = NonNullable<ModelPortfolioAnalysis['axes']>[number];
type Row = Axis['rows'][number];

/** The headline band: return + excess (both sources) and the risk of the ride (yfinance only), so
 *  the "did it earn its excess, at what risk?" cross-read is answered before scrolling. */
function Scorecard({ returns, benchmark, source, longest }: {
  returns?: ModelPortfolioAnalysis['returns'];
  benchmark: string;
  source: 'model' | 'book';
  longest: PerfWindow | null;
}) {
  const r = returns;
  const sp = (v: number | null | undefined) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`);
  const tone = (v: number | null | undefined) => (v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400');
  const rTone = (v: number | null | undefined) => (v == null ? 'text-fg-faint' : v >= 1 ? 'text-pos-400' : v >= 0.5 ? 'text-warn-300' : 'text-fg');
  const yr = longest?.years ?? 8;
  return (
    <div className="flex flex-wrap items-stretch gap-2 mb-4">
      <Chip label="Return (YTD)" value={sp(r?.portfolio_ytd_pct)} valueClass={tone(r?.portfolio_ytd_pct)} />
      <Chip label={`vs ${benchmark} return`} value={sp(r?.benchmark_ytd_pct)} valueClass={tone(r?.benchmark_ytd_pct)} />
      <Chip label="Excess" value={sp(r?.ytd_excess_pct)} valueClass={tone(r?.ytd_excess_pct)} />
      <div className="w-px bg-neutral-800/40 self-stretch mx-1" />
      <Chip label={`Sharpe (${yr}Y)`} value={longest?.sharpe?.toFixed(2) ?? '—'}
        valueClass={rTone(longest?.sharpe)} hint={source !== 'model' ? 'yfinance source only' : undefined} />
      <Chip label={`Max DD (${yr}Y)`} value={sp(longest?.max_drawdown_pct)}
        valueClass={longest ? 'text-neg-400' : 'text-fg-faint'} />
      <Chip label={`Vol (${yr}Y)`} value={sp(longest?.ann_vol_pct)}
        valueClass={longest ? 'text-fg' : 'text-fg-faint'} />
    </div>
  );
}

function Chip({ label, value, valueClass, hint }: {
  label: string; value: string; valueClass: string; hint?: string;
}) {
  return (
    <div className="bg-elevated border border-neutral-800/40 rounded-lg px-3 py-1.5 min-w-[6rem]" title={hint}>
      <div className="text-[9px] uppercase tracking-wide text-fg-faint">{label}</div>
      <div className={`text-sm font-mono font-semibold ${valueClass}`}>{value}</div>
    </div>
  );
}

/** A return, coloured by sign. `—` when it could not be measured — never a 0. */
function Num({ v }: { v?: number | null }) {
  if (v == null) {
    return <span className="text-fg-faint" title="Not measurable — see the coverage note.">—</span>;
  }
  return (
    <span className={v >= 0 ? 'text-pos-400' : 'text-neg-400'}>
      {v >= 0 ? '+' : ''}{v.toFixed(2)}%
    </span>
  );
}

/** One window: the model, the index over THAT SAME window, and the excess between them.
 *
 *  Clickable — an excess return is a fact, not an explanation, and the row is where a reader
 *  asks "why". It opens the Brinson attribution for THIS window. */
function ReturnRow({ label, from, port, bench, excess, source, benchmark,
  portfolioAsOf, benchmarkAsOf, onClick, active }: {
  label: string; from?: string | null;
  port?: number | null; bench?: number | null; excess?: number | null;
  source: 'model' | 'book'; benchmark: string;
  portfolioAsOf?: string | null; benchmarkAsOf?: string | null;
  onClick: () => void; active: boolean;
}) {
  const fmt = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
  return (
    <tr onClick={onClick}
      className={`border-t border-neutral-800/20 cursor-pointer transition-colors ${
        active ? 'bg-accent-500/10' : 'hover:bg-accent-500/5'}`}
      title="Why? — decompose this excess into sector allocation vs stock selection">
      <td className="py-2 pr-3">
        <div className="text-fg">{label}</div>
        {/* "Since when" is half of what a return means — and it is NOT 1 January for half of
            these models. Never leave it to be assumed. */}
        <div className="text-[10px] text-fg-faint font-mono">from {from ?? '—'}</div>
      </td>
      <td className="py-2 px-3 text-right font-mono">
        <Num v={port} />
        {port != null && (
          <Provenance
            source={source === 'book' ? 'airs_att' : 'yfinance'}
            asOf={portfolioAsOf}
            note={source === 'book' ? 'cumulatief_rendement (flow-aware, incl. income)' : 'asset_price close × FX'}
            how={source === 'book'
              ? "AIRS's own book return over the window — monthly returns compounded, flow-aware, income included."
              : 'Buy-and-hold EUR return of the composition (yfinance close × FX), weighted by its percentages.'} />
        )}
      </td>
      <td className="py-2 px-3 text-right font-mono">
        <Num v={bench} />
        {bench != null && (
          <Provenance source="benchmark" asOf={benchmarkAsOf}
            note={`${benchmark} — cap-weighted index`}
            how={`Cap-weighted EUR return of the ${benchmark} constituents over the same window (start-of-window weights).`} />
        )}
      </td>
      <td className="py-2 pl-3 text-right font-mono font-semibold">
        <Num v={excess} />
        {excess != null && port != null && bench != null && (
          <Provenance source="derived" note="portfolio − benchmark"
            how={`${fmt(port)} − ${fmt(bench)} = ${fmt(excess)}`} />
        )}
      </td>
    </tr>
  );
}

/** EUR return, portfolio vs benchmark, over TWO windows.
 *
 * Six numbers and a comparison — a STAT TILE, not a chart. A bar chart of two returns is a chart
 * built to have something to look at.
 *
 * ⚠ THE BENCHMARK IS PRICED OVER THE MODEL'S OWN WINDOWS, NOT THE CALENDAR YEAR. A model's "YTD"
 * opens at max(1 Jan, its inception), and 27 of the 56 models are younger than the year. Setting
 * a 9-day portfolio return beside the index's full-year return and calling the gap
 * out-performance would be nonsense that reads exactly like a finding. Every row states the date
 * it starts from, and `ytd_is_since` says so out loud when the two windows coincide.
 */
function ReturnsTile({ r, benchmark, source, why, onWhy }: {
  r: NonNullable<ModelPortfolioAnalysis['returns']>;
  benchmark: string;
  source: 'model' | 'book';
  why: 'ytd' | 'since' | null;
  onWhy: (w: 'ytd' | 'since' | null) => void;
}) {
  const isBook = source === 'book';
  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-4">
      <h4 className="text-sm font-semibold text-fg-strong">Return (€)</h4>
      <p className="text-[11px] text-fg-faint mt-0.5 mb-1">
        {isBook
          ? `Portfolio = AIRS's own book return (cumulatief_rendement, flow-aware and incl. income) over the calendar year; the benchmark is priced from yfinance over the same window.`
          : `The index is priced over the model’s OWN windows, not the calendar year — otherwise the gap is not out-performance, it is two different periods.`}
      </p>
      {isBook && r.book_available === false && (
        <p className="text-[11px] text-warn-300 mb-1">
          {'⚠ No AIRS book is paired with this model, so there is no book return to show. '}
          {'Switch Source back to Model, or pair a book on /portfolios.'}
        </p>
      )}
      <table className="w-full text-xs">
        <thead>
          <tr className="text-fg-faint text-[10px] uppercase tracking-wide">
            <th className="py-1 pr-3 text-left font-medium">Window</th>
            <th className="py-1 px-3 text-right font-medium">Portfolio</th>
            <th className="py-1 px-3 text-right font-medium">{benchmark}</th>
            <th className="py-1 pl-3 text-right font-medium">Excess</th>
          </tr>
        </thead>
        <tbody>
          <ReturnRow label={isBook ? 'Year to date' : 'YTD'} from={r.ytd_from}
            port={r.portfolio_ytd_pct} bench={r.benchmark_ytd_pct} excess={r.ytd_excess_pct}
            source={source} benchmark={benchmark}
            portfolioAsOf={r.portfolio_as_of} benchmarkAsOf={r.benchmark_as_of}
            active={why === 'ytd'} onClick={() => onWhy(why === 'ytd' ? null : 'ytd')} />
          {isBook ? (
            /* AIRS keeps no composition history for the book, so 'since inception' has no book
               equivalent — say so rather than show a blank that reads like a missing number. */
            <tr className="border-t border-neutral-800/20">
              <td className="py-2 pr-3 text-fg-faint">Since inception</td>
              <td colSpan={3} className="py-2 pl-3 text-[11px] text-fg-faint">
                Not available from AIRS — the book has no composition history.
              </td>
            </tr>
          ) : (
            <ReturnRow label="Since inception" from={r.since_from} port={r.portfolio_since_pct}
              bench={r.benchmark_since_pct} excess={r.since_excess_pct}
              source={source} benchmark={benchmark}
              portfolioAsOf={r.portfolio_as_of} benchmarkAsOf={r.benchmark_as_of}
              active={why === 'since'} onClick={() => onWhy(why === 'since' ? null : 'since')} />
          )}
        </tbody>
      </table>
      {!isBook && r.ytd_is_since && (
        <p className="text-[11px] text-warn-300 mt-2">
          {'⚠ This model is younger than the year, so its YTD window IS its since-inception '}
          {'window — the two rows are the same number by construction, not a coincidence.'}
        </p>
      )}
      <BookVsStrategy r={r} />
    </section>
  );
}

/**
 * The STRATEGY (this modal, priced from yfinance) beside the BOOK (the row that opened it, valued
 * by AIRS). Their difference is implementation drift, timing and fees — the reason the Fixed and
 * Dynamic tables exist as two things, and the one number nothing else on the page shows.
 *
 * ⚠ THE GAP IS ONLY SHOWN WHEN THE WINDOWS MATCH. The book is always the calendar year; 9 of 28
 *   models have a partial-year YTD. Where they differ (`book_comparable === false`), the two
 *   numbers still appear — but the subtraction does not, and the reason is stated. A gap across
 *   two windows is not drift, and it would read exactly like one.
 */
function BookVsStrategy({ r }: { r: NonNullable<ModelPortfolioAnalysis['returns']> }) {
  if (!r.book_portefeuille) return null;   // an unlinked model has no book to compare against
  const pct = (v: number | null | undefined) =>
    v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
  const tone = (v: number | null | undefined) =>
    v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400';
  return (
    <div className="mt-3 pt-3 border-t border-neutral-800/40">
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-[11px] uppercase tracking-wide text-fg-faint">Book vs strategy (YTD)</span>
        {r.book_comparable && r.book_gap_pct != null && (
          <span className={`text-xs font-mono font-semibold ${tone(r.book_gap_pct)}`}
            title="Strategy return minus the book's. Positive = the weights out-performed the book AIRS actually holds — implementation drift, timing and fees.">
            {pct(r.book_gap_pct)} drift
          </span>
        )}
      </div>
      <div className="grid grid-cols-2 gap-2 mt-1.5 text-xs">
        <div className="bg-inset rounded-lg px-3 py-2">
          <div className="text-[10px] text-fg-faint">Strategy (our prices)</div>
          {/* Always the yfinance model YTD — pinned server-side, so this stays the strategy number
              even when Source=Book makes the primary column above the AIRS book. */}
          <div className={`font-mono ${tone(r.strategy_ytd_pct ?? r.portfolio_ytd_pct)}`}>
            {pct(r.strategy_ytd_pct ?? r.portfolio_ytd_pct)}
            <Provenance source="yfinance" asOf={r.benchmark_as_of} note="asset_price close × FX"
              how="The yfinance reconstruction of the strategy's YTD — buy-and-hold of the composition's designed weights." />
          </div>
        </div>
        <div className="bg-inset rounded-lg px-3 py-2">
          <div className="text-[10px] text-fg-faint" title={`AIRS's own return for ${r.book_portefeuille}.`}>
            Book (AIRS)
          </div>
          <div className={`font-mono ${tone(r.book_ytd_pct)}`}>
            {pct(r.book_ytd_pct)}
            <Provenance source="airs_att" asOf={r.book_as_of} note="cumulatief_rendement"
              how="AIRS's own return for the paired book over the year — flow-aware, includes income." />
          </div>
        </div>
      </div>
      {!r.book_comparable && r.book_reason && (
        <p className="text-[11px] text-warn-300 mt-1.5">⚠ {r.book_reason}</p>
      )}
    </div>
  );
}

function Chart({ axis, rows, benchmark, onBucket, selected }: {
  axis: string;
  rows: Row[];
  benchmark: string;
  onBucket: (axis: string, bucket: string) => void;
  selected: string | null;
}) {
  // Recharts sizes a category axis by its slots, so the height has to grow with the buckets or
  // the labels collide — a chart that renders on top of itself is not a chart.
  const height = Math.max(160, rows.length * 34 + 44);

  return (
    <section className={`bg-card border rounded-xl p-4 ${
      selected ? 'border-accent-500/40' : 'border-neutral-800/40'}`}>
      <h4 className="text-sm font-semibold text-fg-strong">{AXIS_LABEL[axis] ?? axis}</h4>
      <p className="text-[11px] text-fg-faint mt-0.5">{AXIS_NOTE[axis]}</p>
      <p className="text-[10px] text-accent-400/80 mb-2">Click a bar for the holdings behind it.</p>
      {/* Numeric height, not "100%": recharts' ResponsiveContainer starts at {-1,-1} and only
          measures on the next frame, so height="100%" reads -1 on the first paint and warns
          "width(-1) and height(-1)". An explicit numeric height is >0 from the first render
          (the same reason every other chart here passes a number) and skips the warning. */}
      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={rows} layout="vertical" barGap={2}
          margin={{ top: 4, right: 46, bottom: 4, left: 8 }}>
          <CartesianGrid stroke={chartTheme.grid} horizontal={false} />
          <XAxis type="number" unit="%" tick={{ fill: chartTheme.axisTick, fontSize: 11 }}
            stroke={chartTheme.zeroLine} />
          <YAxis type="category" dataKey="bucket" width={148}
            tick={{ fill: chartTheme.axisTick, fontSize: 11 }} stroke={chartTheme.zeroLine} />
          <Tooltip
            {...chartTheme.tooltip}
            formatter={(v: unknown, name: unknown) => [`${Number(v).toFixed(1)}%`, String(name)]}
            // The TILT is why the two are side by side at all — put it in the tooltip rather
            // than making the reader subtract two bars by eye.
            labelFormatter={(label: unknown) => {
              const key = String(label);
              const r = rows.find((x: Row) => x.bucket === key);
              const d = r?.diff_pct ?? 0;
              return `${key}  ·  tilt ${d >= 0 ? '+' : ''}${d.toFixed(1)}pp`;
            }}
          />
          <Legend wrapperStyle={{ fontSize: 11, color: chartTheme.axisLabel }} />
          <Bar dataKey="portfolio_pct" name="Portfolio" fill={SERIES.portfolio}
            radius={[0, 4, 4, 0]} barSize={11} cursor="pointer"
            onClick={(_entry: unknown, index: number) => onBucket(axis, rows[index].bucket)}>
            {/* Direct labels: the relief the amber's sub-3:1 contrast obliges, and useful
                regardless. Ink, not the series colour. */}
            <LabelList dataKey="portfolio_pct" position="right" formatter={pct}
              style={{ fill: chartTheme.axisLabel, fontSize: 10 }} />
          </Bar>
          <Bar dataKey="benchmark_pct" name={benchmark} fill={SERIES.benchmark}
            radius={[0, 4, 4, 0]} barSize={11} cursor="pointer"
            onClick={(_entry: unknown, index: number) => onBucket(axis, rows[index].bucket)}>
            <LabelList dataKey="benchmark_pct" position="right" formatter={pct}
              style={{ fill: chartTheme.axisLabel, fontSize: 10 }} />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </section>
  );
}

/** The benchmarks a model can be measured against. All are rebuilt from OUR constituents, OUR
 *  yfinance prices and OUR FX — the same world the portfolio is priced in. A benchmark drawn from
 *  a different price vendor would compare two price universes and call the difference alpha.
 *
 *  AEX is the one that CAPS: 25 names, and uncapped ASML is 37.5% of it (the real index caps a
 *  constituent at 15% at each review, for exactly that reason). The cap is applied server-side in
 *  `_benchmark_index.INDEX_CAP_PCT` — not here, and not per-caller. */
const BENCHMARKS = ['SP500', 'ACWI', 'AEX'] as const;

export default function PortfolioAnalysisModal({ id, name, basket, onClose }: {
  id?: number; name: string; basket?: Basket; onClose: () => void;
}) {
  // A basket (a single stock, a group) is treated as a portfolio-of-N: same view, but yfinance-only
  // (no AIRS book) and no id-based drill-downs (attribution / bucket detail are portfolio-only).
  const isBasket = !!basket;
  const reqKey = isBasket ? basket!.holdings.map((h) => `${h.isin}:${h.weight}`).join(',') : `id:${id}`;
  const [benchmark, setBenchmark] = useState<string>('SP500');
  // Where the PORTFOLIO numbers come from. 'model' = the yfinance reconstruction (nominal weights
  // + reconstructed returns); 'book' = the paired AIRS book's ACTUAL holdings, EUR weights and
  // returns (cumulatief_rendement + the VOLK per-holding results). ONE toggle drives both the
  // composition weighting (`weight_by`) and the return source (`source`) — the benchmark and the
  // sector/region/currency vocabulary stay yfinance either way, so the two remain comparable.
  // Defaults to 'book' — the paired AIRS book (its actual holdings, weights and returns) is what
  // you usually want when you open Analyse; flip to 'model' for the yfinance strategy reconstruction.
  const [source, setSource] = useState<'model' | 'book'>(isBasket ? 'model' : 'book');
  // Which window's excess the reader asked "why" about. Null = not asked.
  const [why, setWhy] = useState<'ytd' | 'since' | null>(null);
  // Which composition bar the reader clicked, to drill into its holdings. {axis, bucket}.
  const [bucket, setBucket] = useState<{ axis: string; bucket: string } | null>(null);
  const [data, setData] = useState<ModelPortfolioAnalysis | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Risk windows are daily-yfinance only (AIRS keeps no daily history), so they exist only in the
  // Model source — fetched when it's selected, and gated behind a hint under Book.
  const [risk, setRisk] = useState<RiskWindows | null>(null);
  const [riskError, setRiskError] = useState<string | null>(null);
  // The benchmark's own risk windows (its investable ETF), shown beside the subject's in the table.
  const [benchRisk, setBenchRisk] = useState<RiskWindows | null>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  // No state reset here — clearing it synchronously inside the effect cascades a render. The
  // benchmark picker clears it in its own handler, which is an event, not a render.
  // No state reset here — clearing it synchronously inside the effect cascades a render. The
  // benchmark picker clears it in its own handler, which is an event, not a render.
  const basketBody = basket
    ? { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ holdings: basket.holdings, label: basket.label }) }
    : undefined;

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const r = isBasket
          ? await apiFetch(`${API_URL}/api/airs/basket/analysis?benchmark=${benchmark}`, basketBody)
          : await apiFetch(`${API_URL}/api/airs/model-portfolios/${id}/analysis`
            + `?benchmark=${benchmark}&weight_by=${source}&source=${source}`);
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as ModelPortfolioAnalysis);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reqKey, benchmark, source]);

  useEffect(() => {
    if (!isBasket && source !== 'model') return;   // a portfolio's book has no daily history
    let cancelled = false;
    void (async () => {
      setRisk(null); setRiskError(null);
      try {
        const r = isBasket
          ? await apiFetch(`${API_URL}/api/asset-pipeline/basket/performance`, basketBody)
          : await apiFetch(`${API_URL}/api/airs/model-portfolios/${id}/risk-windows`);
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setRiskError(b?.detail ?? `HTTP ${r.status}`); return; }
        setRisk(b as RiskWindows);
      } catch (e) {
        if (!cancelled) setRiskError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reqKey, source]);

  useEffect(() => {
    if (!isBasket && source !== 'model') return;   // risk table only shows on the yfinance basis
    let cancelled = false;
    void (async () => {
      setBenchRisk(null);
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/benchmark-risk/${encodeURIComponent(benchmark)}`);
        const b = await r.json().catch(() => null);
        if (cancelled || !r.ok) return;
        setBenchRisk(b as RiskWindows);
      } catch { /* benchmark risk is a nice-to-have; ignore failures */ }
    })();
    return () => { cancelled = true; };
  }, [benchmark, source, isBasket]);

  const partial = data && (data.covered_pct ?? 100) < 99.5;
  // Risk is yfinance — always available for a basket; for a portfolio only under the Model source.
  const riskWindows = (isBasket || source === 'model') ? (risk?.windows ?? []) : [];
  const riskAsOf = riskWindows.find((w) => w.to_date)?.to_date ?? null;
  // Longest available window (8Y → 4Y → 2Y) drives the scorecard's risk chips.
  const longest = [...riskWindows].reverse().find((w) => w.available) ?? null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-scrim/60"
      onClick={onClose} role="dialog" aria-modal="true">
      {/* The modal auto-sizes to what's open: charts only (narrow), the bucket detail (wider),
          or the full Brinson table (widest). One detail at a time — see the shared dock below. */}
      <div className={`bg-page border border-neutral-800/40 rounded-xl shadow-xl w-[96vw] max-h-[92vh] overflow-auto p-5 transition-[max-width] duration-200 ${
        why ? 'max-w-[1780px]' : bucket ? 'max-w-[1460px]' : 'max-w-[1080px]'}`}
        onClick={(e) => e.stopPropagation()}>
        <div className="flex items-start justify-between gap-3 mb-4">
          <div className="min-w-0">
            <h3 className="text-base font-mono font-semibold text-fg-strong">{name}</h3>
            <p className="text-[11px] text-fg-subtle mt-0.5">
              Composition vs <span className="font-mono">{data?.benchmark ?? 'SP500'}</span>
              {data && <> · <span className="font-mono">{data.holdings}</span> holdings ·{' '}
                <span className="font-mono">{data.benchmark_members}</span>{' '}index members
                {data.as_of && <> · as of <span className="font-mono">{data.as_of}</span></>}</>}
              {source === 'book' && (
                <span className="text-accent-400"
                  title="Portfolio numbers (weights + returns) come from the paired AIRS book, not the yfinance model reconstruction. The benchmark is still yfinance.">
                  {' '}· AIRS book
                </span>
              )}
            </p>
            {data?.weight_note && (
              <p className="text-[11px] text-warn-300 mt-0.5">⚠ {data.weight_note}</p>
            )}
          </div>
          <div className="flex items-center gap-2 shrink-0">
            {/* Where the PORTFOLIO numbers come from. Model = the yfinance reconstruction
                (nominal weights + reconstructed returns). Book (AIRS) = the paired AIRS book's
                actual holdings, EUR weights and returns. Only the portfolio side moves — the
                benchmark and the sector / region / currency vocabulary stay yfinance, because the
                benchmark cannot be priced any other way. */}
            {!isBasket && (
              <label className="flex items-center gap-1.5 text-[11px] text-fg-muted"
                title="Source: Model = the yfinance reconstruction (nominal % weights, reconstructed returns). Book (AIRS) = the paired AIRS book's ACTUAL holdings, EUR weights and returns (cumulatief_rendement over the calendar year). Classification and benchmark are yfinance either way.">
                Source
                <select value={source} aria-label="Source"
                  onChange={(e) => { setData(null); setError(null); setBucket(null); setWhy(null); setSource(e.target.value as 'model' | 'book'); }}
                  className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-[11px] text-fg focus:border-accent-500 w-[6.5rem]">
                  <option value="model">Model</option>
                  <option value="book">Book (AIRS)</option>
                </select>
              </label>
            )}
            <label className="flex items-center gap-1.5 text-[11px] text-fg-muted">
              Benchmark
              <select value={benchmark} aria-label="Benchmark"
                onChange={(e) => { setData(null); setError(null); setBucket(null); setBenchmark(e.target.value); }}
                className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-[11px] font-mono text-fg focus:border-accent-500 w-[6.5rem]">
                {BENCHMARKS.map((b) => <option key={b} value={b}>{b}</option>)}
              </select>
            </label>
            <button type="button" onClick={onClose}
              className="text-xs px-3 py-1.5 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 hover:border-accent-500/50 transition-colors">
              Close
            </button>
          </div>
        </div>

        {error && (
          <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">
            {error}
          </div>
        )}
        {!data && !error && <p className="text-xs text-fg-subtle">Loading composition…</p>}

        {data && (
          <>
            {/* Coverage is never assumed. A composition renormalised over a fraction of the
                model is the same invention the returns refuse to make. */}
            {partial && (
              <p className="text-[11px] text-warn-300 mb-3">
                {'⚠ Only '}
                <span className="font-mono">{(data.covered_pct ?? 0).toFixed(0)}%</span>
                {' of this model’s weight can be classified — the rest (structured products, '}
                {'in-house funds) has no instrument record, and the bars are renormalised over '}
                {'what remains.'}
              </p>
            )}
            {/* ⚠ How much of the INDEX we could price. ACWI's missing names go a whole country at
                a time, and a cap-weighted index renormalised over the rest does not LOSE that
                weight — it redistributes it into everything else. The reader is told, not left
                to assume 100%. */}
            {(data.benchmark_coverage_pct ?? 100) < 97 && (
              <p className="text-[11px] text-warn-300 mb-3">
                {'⚠ This index is rebuilt from '}
                <span className="font-mono">{data.benchmark_priced}</span>
                {' of its '}
                <span className="font-mono">{data.benchmark_universe_members}</span>
                {' constituents ('}
                <span className="font-mono">
                  {(data.benchmark_coverage_pct ?? 0).toFixed(0)}%
                </span>
                {`) — the rest have no price series yet. The weights are renormalised over what `}
                {'remains, so the missing names are not dropped, they are redistributed into the '}
                {'others. Treat the tilts as indicative.'}
              </p>
            )}
            {/* The wrong-listing bug, surfaced rather than absorbed. It is why the currency axis
                reads the company's own reporting currency and not the venue we price it on. */}
            {(data.benchmark_foreign_listings ?? 0) > 0 && (
              <p className="text-[11px] text-fg-faint mb-3">
                <span className="font-mono">{data.benchmark_foreign_listings}</span>
                {' of the index’s '}
                <span className="font-mono">{data.benchmark_members}</span>
                {' members are priced on a foreign venue in our grid (Corning on Stuttgart, '}
                {'Ciena on Xetra…). That does not move these bars — currency is the company’s '}
                {'own — but it does mean the benchmark’s price series is drawn off those '}
                {'listings.'}
              </p>
            )}

            {/* The headline, always visible: the one number from each lens (return, excess, and —
                when the yfinance source is on — the risk of the ride) so "did it earn its excess,
                and at what risk?" is answered before scrolling. */}
            <Scorecard returns={data.returns} benchmark={data.benchmark ?? 'SP500'}
              source={source} longest={longest} />

            {/* Main content on the left, the bucket click-through docked on the RIGHT when open —
                so a new click swaps the panel in place rather than pushing it below the fold. */}
            <div className="flex flex-col lg:flex-row gap-4 items-start">
              <div className="min-w-0 w-full lg:flex-1 grid gap-4 lg:grid-cols-2">
                {data.returns && (
                  <ReturnsTile r={data.returns} benchmark={data.benchmark ?? 'SP500'}
                    source={source}
                    why={why} onWhy={(w) => { if (isBasket) return; setBucket(null); setWhy(w); }} />
                )}
                {(data.axes ?? []).map((a) => (
                  <Chart key={a.axis} axis={a.axis} rows={a.rows}
                    benchmark={data.benchmark ?? 'SP500'}
                    onBucket={(axis, b) => { if (isBasket) return; setWhy(null); setBucket(
                      (prev) => prev && prev.axis === axis && prev.bucket === b ? null : { axis, bucket: b }); }}
                    selected={bucket?.axis === a.axis ? bucket.bucket : null} />
                ))}
              </div>
              {/* ONE detail dock, shared by the "why" Brinson attribution (from a return row) and
                  the per-bucket breakdown (from a bar). They are MUTUALLY EXCLUSIVE — opening one
                  clears the other — so a new drill-down replaces whatever occupied this space, and
                  the modal above widens to fit whichever is up. */}
              {(why || bucket) && (
                <div className={`w-full lg:shrink-0 lg:sticky lg:top-2 self-stretch lg:self-start lg:max-h-[82vh] overflow-auto ${
                  why ? 'lg:w-[840px]' : 'lg:w-[480px]'}`}>
                  {why ? (
                    <AttributionPanel id={id ?? 0} benchmark={data.benchmark ?? 'SP500'} window={why}
                      source={source} portfolioAsOf={data.returns?.portfolio_as_of}
                      benchmarkAsOf={data.returns?.benchmark_as_of}
                      onClose={() => setWhy(null)} />
                  ) : bucket ? (
                    <BucketDetailPanel id={id ?? 0} benchmark={data.benchmark ?? 'SP500'}
                      axis={bucket.axis} bucket={bucket.bucket} onClose={() => setBucket(null)} />
                  ) : null}
                </div>
              )}
            </div>

            {/* Risk over time — the third lens. Daily-yfinance only, so it lives behind the Model
                source; under Book it names why (AIRS has no daily history) rather than showing a
                blank. The whole portfolio priced as one value-weighted EUR basket. */}
            <div className="mt-5 border-t border-neutral-800/40 pt-4">
              <div className="flex items-baseline gap-2 mb-2">
                <h4 className="text-sm font-semibold text-fg-strong">Risk over time</h4>
                <span className="text-[11px] text-fg-faint">
                  {isBasket ? 'value-weighted' : 'whole portfolio'} · daily yfinance · 2/4/8-year windows
                </span>
              </div>
              {!isBasket && source !== 'model' ? (
                <p className="text-[11px] text-fg-muted bg-inset border border-neutral-800/40 rounded-lg px-3 py-2">
                  Risk metrics need a daily price history, which the AIRS book doesn’t keep (it holds
                  only a few snapshot dates). Switch <strong>Source</strong> to <strong>Model</strong>{' '}
                  (yfinance) above to price the holdings daily and fill these in.
                </p>
              ) : riskError ? (
                <p className="text-[11px] text-fg-muted bg-inset border border-neutral-800/40 rounded-lg px-3 py-2">
                  No risk metrics: {riskError}
                </p>
              ) : !risk ? (
                <p className="text-[11px] text-fg-subtle py-4">Loading risk…</p>
              ) : (
                <PerformanceTable windows={riskWindows} asOf={riskAsOf}
                  benchWindows={benchRisk?.windows} benchLabel={data.benchmark ?? benchmark}
                  subjectLabel={name} />
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
