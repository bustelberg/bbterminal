'use client';

import { useEffect, useState } from 'react';
import {
  Bar, BarChart, CartesianGrid, LabelList, Legend, ResponsiveContainer, Tooltip,
  XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import type { ModelPortfolioAnalysis } from '../../../lib/types/api';
import AttributionPanel from './AttributionPanel';

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
function ReturnRow({ label, from, port, bench, excess, onClick, active }: {
  label: string; from?: string | null;
  port?: number | null; bench?: number | null; excess?: number | null;
  onClick: () => void; active: boolean;
}) {
  return (
    <tr onClick={onClick}
      className={`border-t border-neutral-800/20 cursor-pointer transition-colors ${
        active ? 'bg-accent-500/10' : 'hover:bg-accent-500/5'}`}
      title="Why? — decompose this excess into sector allocation vs stock selection">
      <td className="py-2 pr-3">
        <div className="text-fg flex items-center gap-1.5">
          {label}
          <span className={`text-[10px] ${active ? 'text-accent-400' : 'text-fg-faint'}`}>
            {active ? '▾ why' : '▸ why'}
          </span>
        </div>
        {/* "Since when" is half of what a return means — and it is NOT 1 January for half of
            these models. Never leave it to be assumed. */}
        <div className="text-[10px] text-fg-faint font-mono">from {from ?? '—'}</div>
      </td>
      <td className="py-2 px-3 text-right font-mono"><Num v={port} /></td>
      <td className="py-2 px-3 text-right font-mono"><Num v={bench} /></td>
      <td className="py-2 pl-3 text-right font-mono font-semibold"><Num v={excess} /></td>
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
function ReturnsTile({ r, benchmark, why, onWhy }: {
  r: NonNullable<ModelPortfolioAnalysis['returns']>;
  benchmark: string;
  why: 'ytd' | 'since' | null;
  onWhy: (w: 'ytd' | 'since' | null) => void;
}) {
  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-4">
      <h4 className="text-sm font-semibold text-fg-strong">Return (€)</h4>
      <p className="text-[11px] text-fg-faint mt-0.5 mb-1">
        {`The index is priced over the model’s OWN windows, not the calendar year — otherwise the gap is not out-performance, it is two different periods.`}
      </p>
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
          <ReturnRow label="YTD" from={r.ytd_from} port={r.portfolio_ytd_pct}
            bench={r.benchmark_ytd_pct} excess={r.ytd_excess_pct}
            active={why === 'ytd'} onClick={() => onWhy(why === 'ytd' ? null : 'ytd')} />
          <ReturnRow label="Since inception" from={r.since_from} port={r.portfolio_since_pct}
            bench={r.benchmark_since_pct} excess={r.since_excess_pct}
            active={why === 'since'} onClick={() => onWhy(why === 'since' ? null : 'since')} />
        </tbody>
      </table>
      {r.ytd_is_since && (
        <p className="text-[11px] text-warn-300 mt-2">
          {'⚠ This model is younger than the year, so its YTD window IS its since-inception '}
          {'window — the two rows are the same number by construction, not a coincidence.'}
        </p>
      )}
    </section>
  );
}

function Chart({ axis, rows, benchmark }: {
  axis: string;
  rows: Row[];
  benchmark: string;
}) {
  // Recharts sizes a category axis by its slots, so the height has to grow with the buckets or
  // the labels collide — a chart that renders on top of itself is not a chart.
  const height = Math.max(160, rows.length * 34 + 44);

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-4">
      <h4 className="text-sm font-semibold text-fg-strong">{AXIS_LABEL[axis] ?? axis}</h4>
      <p className="text-[11px] text-fg-faint mt-0.5 mb-2">{AXIS_NOTE[axis]}</p>
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
            radius={[0, 4, 4, 0]} barSize={11}>
            {/* Direct labels: the relief the amber's sub-3:1 contrast obliges, and useful
                regardless. Ink, not the series colour. */}
            <LabelList dataKey="portfolio_pct" position="right" formatter={pct}
              style={{ fill: chartTheme.axisLabel, fontSize: 10 }} />
          </Bar>
          <Bar dataKey="benchmark_pct" name={benchmark} fill={SERIES.benchmark}
            radius={[0, 4, 4, 0]} barSize={11}>
            <LabelList dataKey="benchmark_pct" position="right" formatter={pct}
              style={{ fill: chartTheme.axisLabel, fontSize: 10 }} />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </section>
  );
}

/** The benchmarks a model can be measured against. Both are rebuilt from OUR constituents, OUR
 *  yfinance prices and OUR FX — the same world the portfolio is priced in. A benchmark drawn from
 *  a different price vendor would compare two price universes and call the difference alpha. */
const BENCHMARKS = ['SP500', 'ACWI'] as const;

export default function PortfolioAnalysisModal({ id, name, onClose }: {
  id: number; name: string; onClose: () => void;
}) {
  const [benchmark, setBenchmark] = useState<string>('SP500');
  // Which window's excess the reader asked "why" about. Null = not asked.
  const [why, setWhy] = useState<'ytd' | 'since' | null>(null);
  const [data, setData] = useState<ModelPortfolioAnalysis | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  // No state reset here — clearing it synchronously inside the effect cascades a render. The
  // benchmark picker clears it in its own handler, which is an event, not a render.
  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/model-portfolios/${id}/analysis?benchmark=${benchmark}`);
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as ModelPortfolioAnalysis);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
  }, [id, benchmark]);

  const partial = data && (data.covered_pct ?? 100) < 99.5;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-scrim/60"
      onClick={onClose} role="dialog" aria-modal="true">
      <div className="bg-page border border-neutral-800/40 rounded-xl shadow-xl w-[96vw] max-w-[1100px] max-h-[92vh] overflow-auto p-5"
        onClick={(e) => e.stopPropagation()}>
        <div className="flex items-start justify-between gap-3 mb-4">
          <div className="min-w-0">
            <h3 className="text-base font-mono font-semibold text-fg-strong">{name}</h3>
            <p className="text-[11px] text-fg-subtle mt-0.5">
              Composition vs <span className="font-mono">{data?.benchmark ?? 'SP500'}</span>
              {data && <> · <span className="font-mono">{data.holdings}</span> holdings ·{' '}
                <span className="font-mono">{data.benchmark_members}</span> index members
                {data.as_of && <> · as of <span className="font-mono">{data.as_of}</span></>}</>}
            </p>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <label className="flex items-center gap-1.5 text-[11px] text-fg-muted">
              Benchmark
              <select value={benchmark}
                onChange={(e) => { setData(null); setError(null); setBenchmark(e.target.value); }}
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

            <div className="grid gap-4 lg:grid-cols-2">
              {data.returns && (
                <ReturnsTile r={data.returns} benchmark={data.benchmark ?? 'SP500'}
                  why={why} onWhy={setWhy} />
              )}
              {/* The "why" — Brinson, for the window whose row was clicked. Spans both columns:
                  it is a nine-column table, not a tile. */}
              {why && (
                <AttributionPanel id={id} benchmark={data.benchmark ?? 'SP500'} window={why}
                  onClose={() => setWhy(null)} />
              )}
              {(data.axes ?? []).map((a) => (
                <Chart key={a.axis} axis={a.axis} rows={a.rows}
                  benchmark={data.benchmark ?? 'SP500'} />
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
