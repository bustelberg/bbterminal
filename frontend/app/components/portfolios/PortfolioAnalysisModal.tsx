'use client';

import { useEffect, useState } from 'react';
import { Cell, Pie, PieChart, ResponsiveContainer } from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { allocColor, bucketLabel } from './allocationColors';
import type { ModelPortfolioAnalysis } from '../../../lib/types/api';
import AttributionPanel from './AttributionPanel';
import BucketDetailPanel from './BucketDetailPanel';
import { type Basket } from './PerformanceModal';

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
 * ⚠ FUNDS FOLD INTO "Unclassified" — THE HONEST BUCKET. We hold no constituent data for an
 * ETF, and its listing tells you nothing about its contents: 24 of the 26 held ETFs have a
 * "sector" of literally `etf` or `Equity`; an Amsterdam-listed MSCI World ETF is not European
 * exposure; quoted in EUR it still holds mostly USD assets. So funds are bucketed, not
 * decomposed. A portfolio that is 40% ETF shows a 40% Unclassified bar meaning "we cannot see
 * inside this" — which is true, and far better than a confident, invented split.
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
  sector: 'Equity holdings by sector — bonds, cash and funds are left out (they have no equity sector).',
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

/** The headline band: return + excess (both sources), so the "did it earn its excess?" read is
 *  answered before scrolling. */
function Scorecard({ returns, benchmark, onAttribution, attributionActive }: {
  returns?: ModelPortfolioAnalysis['returns'];
  benchmark: string;
  onAttribution?: () => void;      // launches the YTD Brinson attribution; omitted for a basket
  attributionActive?: boolean;
}) {
  const r = returns;
  const sp = (v: number | null | undefined) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`);
  // The excess is a DIFFERENCE of two returns, so it is in percentage POINTS (pp), not percent —
  // and it equals the attribution "Total", which is also pp.
  const spp = (v: number | null | undefined) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}pp`);
  const tone = (v: number | null | undefined) => (v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400');
  return (
    <div className="grid grid-cols-2 gap-2 self-center">
      <Chip label="Return (YTD)" value={sp(r?.portfolio_ytd_pct)} valueClass={tone(r?.portfolio_ytd_pct)} />
      <Chip label={`vs ${benchmark} return`} value={sp(r?.benchmark_ytd_pct)} valueClass={tone(r?.benchmark_ytd_pct)} />
      <Chip label="Excess" value={spp(r?.ytd_excess_pct)} valueClass={tone(r?.ytd_excess_pct)} />
      {onAttribution && (
        <button type="button" onClick={onAttribution}
          title="Why? — break the excess into allocation vs selection (Brinson-Fachler attribution)."
          className={`rounded-lg border px-3 py-1.5 min-w-[6rem] text-xs font-medium transition-colors flex items-center justify-center ${
            attributionActive
              ? 'bg-accent-600 text-white border-transparent'
              : 'bg-elevated border-neutral-800/40 text-fg-muted hover:text-accent-300 hover:border-accent-500/50'}`}>
          Attribution
        </button>
      )}
    </div>
  );
}

type AllocSlice = { bucket: string; pct: number; return_pct?: number | null };
const RADIAN = Math.PI / 180;
const fmtRet = (v?: number | null) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`);
const retTone = (v?: number | null) => (v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400');

/** A donut of the portfolio's own asset-class split — the % INSIDE each slice, and a legend beside
 *  it naming each class and its YTD return. Slices/legend are clickable to filter the charts to that
 *  class. Palette (no red/green, CVD-validated) shared with the /portfolios "Class" column. */
function AllocationPie({ slices, selected, onSelect }: {
  slices: AllocSlice[];
  selected?: string | null;
  onSelect?: (bucket: string | null) => void;
}) {
  const ordered = [...slices].sort((a, b) => b.pct - a.pct);
  const toggle = onSelect ? (b: string) => onSelect(selected === b ? null : b) : undefined;

  // The % inside each slice — skip the slivers, where a label would collide with its neighbours.
  const label = (p: { cx?: number; cy?: number; midAngle?: number; innerRadius?: number;
    outerRadius?: number; index?: number }) => {
    const { cx = 0, cy = 0, midAngle = 0, innerRadius = 0, outerRadius = 0, index = 0 } = p;
    const s = ordered[index];
    if (!s || s.pct < 6) return null;
    const r = innerRadius + (outerRadius - innerRadius) * 0.55;
    const x = cx + r * Math.cos(-midAngle * RADIAN);
    const y = cy + r * Math.sin(-midAngle * RADIAN);
    // pointer-events: none so a click on the % label passes THROUGH to the slice underneath —
    // otherwise the label swallows the click and that part of the pie is dead.
    return (
      <text x={x} y={y} fill="#fff" textAnchor="middle" dominantBaseline="central"
        fontSize={11} fontWeight={600} style={{ pointerEvents: 'none' }}>{`${s.pct.toFixed(0)}%`}</text>
    );
  };

  return (
    <div className="shrink-0">
      <div className="flex items-center gap-2 mb-1">
        <span className="text-[10px] uppercase tracking-wide text-fg-faint">Allocation</span>
        {onSelect && (selected
          ? <button type="button" onClick={() => onSelect(null)}
              className="text-[10px] text-accent-400 hover:text-accent-300">
              filtering to {bucketLabel(selected)} — show all ✕
            </button>
          : <span className="text-[10px] text-fg-faint">click a class to filter the charts</span>)}
      </div>
      <div className="flex items-center gap-6">
        {/* Recharts gives each sector a tabindex; clicking focuses it and the browser draws a
            square focus outline. Suppress it on every element inside the chart. */}
        <div style={{ width: 240, height: 240 }} className="shrink-0 [&_*:focus]:outline-none [&_*:focus-visible]:outline-none">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie data={ordered} dataKey="pct" nameKey="bucket" cx="50%" cy="50%"
                innerRadius={60} outerRadius={116} paddingAngle={1.5} labelLine={false}
                label={label} isAnimationActive={false}
                onClick={toggle ? (_d: unknown, index: number) => toggle(ordered[index].bucket) : undefined}>
                {ordered.map((s) => (
                  <Cell key={s.bucket} fill={allocColor(s.bucket)}
                    opacity={selected && selected !== s.bucket ? 0.3 : 1}
                    stroke={selected === s.bucket ? chartTheme.axisLabel : 'transparent'}
                    strokeWidth={selected === s.bucket ? 2 : 0}
                    cursor={toggle ? 'pointer' : undefined} />
                ))}
              </Pie>
            </PieChart>
          </ResponsiveContainer>
        </div>
        {/* Legend: class name · weight · YTD return; click to filter the charts. */}
        <div className="flex flex-col gap-1 min-w-0">
          {ordered.map((s) => {
            const active = selected === s.bucket;
            const cls = `flex items-center gap-2 text-[11px] text-left whitespace-nowrap transition-opacity ${
              toggle ? 'cursor-pointer' : ''} ${selected && !active ? 'opacity-40' : ''}`;
            const inner = (
              <>
                <span className="w-2.5 h-2.5 rounded-sm inline-block shrink-0" style={{ backgroundColor: allocColor(s.bucket) }} />
                <span className={active ? 'text-fg-strong font-medium' : 'text-fg-muted'}>{bucketLabel(s.bucket)}</span>
                <span className="font-mono text-fg-soft">{s.pct.toFixed(1)}%</span>
                <span className="text-fg-faint">·</span>
                <span className={`font-mono ${retTone(s.return_pct)}`}>{fmtRet(s.return_pct)}</span>
              </>
            );
            return toggle
              ? <button type="button" key={s.bucket} onClick={() => toggle(s.bucket)} className={cls}>{inner}</button>
              : <span key={s.bucket} className={cls}>{inner}</span>;
          })}
        </div>
      </div>
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

function Chart({ axis, rows, benchmark, onBucket, selected }: {
  axis: string;
  rows: Row[];
  benchmark: string;
  onBucket: (axis: string, bucket: string) => void;
  selected: string | null;
}) {
  // Sector is an EQUITY-only view; a non-equity selection leaves it with no portfolio side, so say
  // so rather than draw the benchmark's sectors beside an empty portfolio.
  const sectorEmpty = axis === 'sector' && rows.every((r) => (r.portfolio_pct ?? 0) === 0);
  // Largest share first — a reader scans a ranked list, not the server's order.
  const sorted = [...rows].sort((a, b) =>
    (b.portfolio_pct ?? 0) - (a.portfolio_pct ?? 0) || (b.benchmark_pct ?? 0) - (a.benchmark_pct ?? 0));

  return (
    <section className={`bg-card border rounded-xl p-4 ${
      selected ? 'border-accent-500/40' : 'border-neutral-800/40'}`}>
      <h4 className="text-sm font-semibold text-fg-strong">{AXIS_LABEL[axis] ?? axis}</h4>
      <p className="text-[11px] text-fg-faint mt-0.5">{AXIS_NOTE[axis]}</p>
      {sectorEmpty ? (
        <p className="text-[11px] text-fg-subtle py-8 text-center">
          Nothing to show — the current selection holds no equity, and sector is an equity-only view.
        </p>
      ) : (<>
      {/* Legend (two series ⇒ mandatory — identity is never colour-alone): a filled bar is the
          model, a tick is the benchmark. Blue + amber is the CVD-separated pair (ΔE 103) — see
          the file header; text wears text tokens, the swatches carry the colour. */}
      <div className="chart-legend flex items-center gap-4 text-[10px] text-fg-faint mt-2 mb-2">
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3.5 h-2 rounded-sm" style={{ background: SERIES.portfolio }} />
          Portfolio
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-[3px] h-3 rounded-sm" style={{ background: SERIES.benchmark }} />
          {benchmark}
        </span>
      </div>
      {/* Inline bullet rows: label on ONE line (truncated, never wrapped), a filled bar for the
          model with the benchmark drawn as a reference tick over it, and both values in-line at the
          end (ink, not the series colour). Compact, ranked, and every bit of text on its own row. */}
      <div className="flex flex-col gap-0.5">
        {sorted.map((r) => {
          const p = Math.max(0, r.portfolio_pct ?? 0);
          const b = Math.max(0, r.benchmark_pct ?? 0);
          const tilt = r.diff_pct ?? (p - b);
          const active = r.bucket === selected;
          return (
            <button type="button" key={r.bucket}
              onClick={() => onBucket(axis, r.bucket)}
              title={`${r.bucket}  ·  Portfolio ${p.toFixed(1)}%  vs  ${benchmark} ${b.toFixed(1)}%  ·  tilt ${tilt >= 0 ? '+' : ''}${tilt.toFixed(1)}pp`}
              className={`group flex items-center gap-2.5 rounded-md -mx-1.5 px-1.5 py-1 text-left transition-colors ${
                active ? 'bg-accent-500/10' : 'hover:bg-overlay/[0.03]'}`}>
              <span className={`w-[6.5rem] shrink-0 truncate text-[11px] ${
                active ? 'font-medium text-fg-strong' : 'text-fg-muted'}`}>{r.bucket}</span>
              {/* Fixed 0–100% scale — a bar's length IS its share of the sleeve, not its rank
                  against the biggest bucket. */}
              <span className="relative h-[18px] flex-1 rounded bg-inset">
                {p > 0 && (
                  <span className="absolute inset-y-[3px] left-0 rounded"
                    style={{ width: `${Math.min(100, p)}%`, minWidth: 3, background: SERIES.portfolio }} />
                )}
                {b > 0 && (
                  <span className="absolute inset-y-0 w-[3px] rounded-sm"
                    style={{ left: `calc(${Math.min(100, b)}% - 1.5px)`, background: SERIES.benchmark }} />
                )}
              </span>
              {/* Colour-coded to the series they belong to (portfolio blue / benchmark amber), so
                  the value ties to its bar without reading the legend. */}
              <span className="w-9 shrink-0 text-right font-mono text-[11px]" style={{ color: SERIES.portfolio }}>{pct(p)}</span>
              <span className="w-9 shrink-0 text-right font-mono text-[10px]" style={{ color: SERIES.benchmark }}>{pct(b)}</span>
            </button>
          );
        })}
      </div>
      </>)}
    </section>
  );
}

type BookHolding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];

/** The headline tile for a NON-EQUITY sleeve: its own return + weight, no benchmark. Replaces the
 *  Return/vs-SP500/Excess/Attribution scorecard, none of which makes sense off the equity book. */
function SleeveTile({ bucket, slices }: { bucket: string; slices?: AllocSlice[] }) {
  const s = (slices ?? []).find((x) => x.bucket === bucket);
  return (
    <div className="bg-elevated border border-neutral-800/40 rounded-lg px-4 py-3 min-w-[10rem] flex flex-col justify-center">
      <div className={`text-2xl font-mono font-semibold ${retTone(s?.return_pct)}`}>{fmtRet(s?.return_pct)}</div>
      <div className="text-[10px] text-fg-faint">YTD (€)</div>
    </div>
  );
}

/** What to show for a bond / fund / cash / alternatives sleeve, where sector-vs-SP500 says nothing:
 *  (1) a CONTRIBUTION breakdown — each holding's weight × return, which sums to the sleeve return —
 *  and (2) a CURRENCY-exposure chart (no benchmark). Both computed client-side from the book detail,
 *  so switching sleeves is instant. */
type SleeveSortKey = 'name' | 'weight' | 'return' | 'contrib';

function SleeveBreakdown({ holdings, bucket }: { holdings: BookHolding[]; bucket: string }) {
  // Sortable table — click a header to toggle direction. Default: weight, largest first.
  const [sortKey, setSortKey] = useState<SleeveSortKey>('weight');
  const [dir, setDir] = useState<'asc' | 'desc'>('desc');

  const rows = holdings.filter((h) => h.bucket === bucket);
  const totalW = rows.reduce((s, h) => s + (h.weight_pct ?? 0), 0) || 1;
  // Renormalise each holding's (opening-value) weight WITHIN the sleeve, then contribution =
  // weight × return (in pp of the sleeve return). Σ contribution == the sleeve figure exactly.
  const items = rows.map((h) => {
    const w = (h.weight_pct ?? 0) / totalW * 100;
    return { name: h.name, weight: w, ret: h.return_pct,
      contrib: h.return_pct != null ? (w / 100) * h.return_pct : null };
  });
  type Item = (typeof items)[number];
  const val = (it: Item): number | string | null =>
    sortKey === 'name' ? (it.name ?? '').toLowerCase()
      : sortKey === 'weight' ? it.weight
        : sortKey === 'return' ? (it.ret ?? null)
          : it.contrib;
  const sorted = [...items].sort((a, b) => {
    const av = val(a); const bv = val(b);
    if (sortKey === 'name') {
      const cmp = String(av).localeCompare(String(bv));
      return dir === 'asc' ? cmp : -cmp;
    }
    if (av == null && bv == null) return 0;
    if (av == null) return 1;      // undefined return/contrib always sorts last
    if (bv == null) return -1;
    return dir === 'asc' ? (av as number) - (bv as number) : (bv as number) - (av as number);
  });
  const click = (k: SleeveSortKey) => {
    if (k === sortKey) { setDir((d) => (d === 'asc' ? 'desc' : 'asc')); return; }
    setSortKey(k);
    setDir(k === 'name' ? 'asc' : 'desc');
  };
  const caret = (k: SleeveSortKey) => (sortKey === k ? (dir === 'asc' ? ' ▲' : ' ▼') : '');
  const th = 'py-1 font-medium cursor-pointer select-none whitespace-nowrap hover:text-fg-soft';

  const ccyMap = new Map<string, number>();
  rows.forEach((h) => {
    const c = h.currency ?? 'Unknown';
    ccyMap.set(c, (ccyMap.get(c) ?? 0) + (h.weight_pct ?? 0) / totalW * 100);
  });
  const ccy = [...ccyMap.entries()].map(([c, p]) => ({ ccy: c, pct: p })).sort((a, b) => b.pct - a.pct);

  if (!rows.length) return (
    <p className="text-[11px] text-fg-subtle py-8 text-center">
      No priced holdings in the {bucketLabel(bucket)} sleeve.
    </p>
  );

  return (
    <div className="grid gap-4 lg:grid-cols-3">
      {/* Contribution breakdown — the always-honest "what's in here and what drove it". */}
      <section className="bg-card border border-neutral-800/40 rounded-xl p-4 lg:col-span-2">
        <h4 className="text-sm font-semibold text-fg-strong">Performance</h4>
        <div className="mt-3 overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead>
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                <th className={`${th} pr-2 text-left`} onClick={() => click('name')}>Name{caret('name')}</th>
                <th className={`${th} px-2 text-right`} onClick={() => click('weight')}>Weight{caret('weight')}</th>
                <th className={`${th} px-2 text-right`} onClick={() => click('return')}>Return{caret('return')}</th>
                <th className={`${th} pl-2 text-right`} onClick={() => click('contrib')}>Contribution{caret('contrib')}</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((it, i) => {
                const c = it.contrib ?? 0;
                return (
                  <tr key={i} className="border-t border-neutral-800/20">
                    <td className="py-1 pr-2 text-fg-soft" title={it.name ?? ''}>{it.name ?? '—'}</td>
                    <td className="py-1 px-2 text-right font-mono text-fg">{it.weight.toFixed(1)}%</td>
                    <td className={`py-1 px-2 text-right font-mono ${retTone(it.ret)}`}>{fmtRet(it.ret)}</td>
                    <td className={`py-1 pl-2 text-right font-mono ${c >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
                      {c >= 0 ? '+' : ''}{c.toFixed(2)}pp
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>
      {/* Currency exposure — the one compositional cut that survives off the equity book. */}
      <section className="bg-card border border-neutral-800/40 rounded-xl p-4">
        <h4 className="text-sm font-semibold text-fg-strong">Currency</h4>
        <div className="mt-3 flex flex-col gap-1.5">
          {ccy.map((c) => (
            <div key={c.ccy} className="flex items-center gap-2 text-[11px]">
              <span className="w-12 shrink-0 font-mono text-fg-muted">{c.ccy}</span>
              {/* Fixed 0–100% scale — a bar's length IS the currency's share of the sleeve. */}
              <span className="relative flex-1 h-[16px] rounded bg-inset">
                <span className="absolute inset-y-[2px] left-0 rounded"
                  style={{ width: `${Math.min(100, c.pct)}%`, background: SERIES.portfolio }} />
              </span>
              <span className="w-10 shrink-0 text-right font-mono text-fg-soft">{c.pct.toFixed(0)}%</span>
            </div>
          ))}
        </div>
      </section>
    </div>
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
  // Where the PORTFOLIO numbers come from — FIXED, no toggle. A model portfolio uses the paired
  // AIRS book (its ACTUAL holdings, EUR weights and returns); a basket has no book, so it uses the
  // yfinance reconstruction. AIRS is the primary source, yfinance the fallback where we can price.
  // Drives both the composition weighting (`weight_by`) and the return source (`source`); the
  // benchmark and the sector/region/currency vocabulary stay yfinance either way.
  const source: 'model' | 'book' = isBasket ? 'model' : 'book';
  // Which window's excess the reader asked "why" about. Null = not asked.
  const [why, setWhy] = useState<'ytd' | 'since' | null>(null);
  // Which composition bar the reader clicked, to drill into its holdings. {axis, bucket}.
  const [bucket, setBucket] = useState<{ axis: string; bucket: string } | null>(null);
  // Which allocation class the reader picked, to break down. Null = NOTHING selected — the whole
  // portfolio, where the modal shows the book's return vs the benchmark and prompts the reader to
  // click a class. Selecting a class replaces that with the class's OWN return + its breakdown.
  const [assetFilter, setAssetFilter] = useState<string | null>(null);
  const [data, setData] = useState<ModelPortfolioAnalysis | null>(null);
  const [error, setError] = useState<string | null>(null);

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
            + `?benchmark=${benchmark}&weight_by=${source}&source=${source}`
            + (assetFilter ? `&bucket=${encodeURIComponent(assetFilter)}` : ''));
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
  }, [reqKey, benchmark, source, assetFilter]);

  // The selected class (null = nothing selected = the whole portfolio). A basket is never a
  // portfolio-of-classes, so it stays on the whole-basket view.
  const selected = !isBasket ? assetFilter : null;
  // A specific NON-EQUITY class → its contribution + currency (sector-vs-SP500 says nothing there);
  // 'Equity' (Stocks) keeps the sector / benchmark composition view.
  const sleeve = selected && selected !== 'Equity' ? selected : null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-scrim/60"
      onClick={onClose} role="dialog" aria-modal="true">
      {/* Fixed at 80% of the viewport width AND height; the charts span the full width and the
          drill-down dock opens full-width below them. */}
      <div className="bg-page border border-neutral-800/40 rounded-xl shadow-xl w-[80vw] h-[80vh] overflow-auto p-5"
        onClick={(e) => e.stopPropagation()}>
        <div className="flex items-start justify-between gap-3 mb-4">
          <div className="min-w-0">
            <h3 className="text-base font-mono font-semibold text-fg-strong">{name}</h3>
            {data?.weight_note && (
              <p className="text-[11px] text-warn-300 mt-0.5">⚠ {data.weight_note}</p>
            )}
          </div>
          <div className="flex items-center gap-2 shrink-0">
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
            {/* Top: the allocation pie (the class selector) beside — when NOTHING is selected — the
                whole-portfolio return / vs-benchmark / excess scorecard, or, when a class IS
                selected, ONLY that class's own return (+ Attribution for Stocks). LEFT-aligned so
                the pie holds its position and only the right-hand content changes with the
                selection. The pie is empty for an ad-hoc basket. */}
            <div className="flex items-center justify-start gap-8 flex-wrap mb-4 pl-8 lg:pl-20">
              {data.allocation && data.allocation.length > 0 && (
                <AllocationPie slices={data.allocation} selected={assetFilter}
                  onSelect={isBasket ? undefined : (b) => { setWhy(null); setBucket(null); setAssetFilter(b); }} />
              )}
              {selected
                ? (
                  <div className="self-center flex items-stretch gap-3">
                    <SleeveTile bucket={selected} slices={data.allocation} />
                    {/* Attribution (Brinson) is an EQUITY analysis — offered ONLY on the Stocks
                        sleeve, never on a bond/cash/fund sleeve or the whole-portfolio view. Same
                        box size as the return tile (items-stretch + centred label). */}
                    {selected === 'Equity' && (
                      <button type="button"
                        onClick={() => { setBucket(null); setWhy(why === 'ytd' ? null : 'ytd'); }}
                        title="Why? — break the excess into allocation vs selection (Brinson-Fachler attribution)."
                        className={`rounded-lg border px-4 flex items-center justify-center text-xs font-medium transition-colors ${
                          why === 'ytd'
                            ? 'bg-accent-600 text-white border-transparent'
                            : 'bg-elevated border-neutral-800/40 text-fg-muted hover:text-accent-300 hover:border-accent-500/50'}`}>
                        Attribution
                      </button>
                    )}
                  </div>
                )
                : <Scorecard returns={data.returns} benchmark={data.benchmark ?? 'SP500'} />}
            </div>
            {/* ⚠ How much of the INDEX we could price — shown whenever a benchmark number is on
                screen (the whole-portfolio scorecard, or the Stocks charts). ACWI's missing names
                go a whole country at a time, and a cap-weighted index renormalised over the rest
                does not LOSE that weight — it redistributes it. Stated, never assumed to be 100%. */}
            {!sleeve && (data.benchmark_coverage_pct ?? 100) < 97 && (
              <p className="text-[11px] text-warn-300 mb-3">
                {'⚠ This index is rebuilt from '}
                <span className="font-mono">{data.benchmark_priced}</span>
                {' of its '}
                <span className="font-mono">{data.benchmark_universe_members}</span>
                {' constituents ('}
                <span className="font-mono">{(data.benchmark_coverage_pct ?? 0).toFixed(0)}%</span>
                {`) — the rest have no price series yet. The weights are renormalised over what `}
                {'remains, so the missing names are not dropped, they are redistributed into the '}
                {'others. Treat the tilts as indicative.'}
              </p>
            )}
            {selected == null ? (
              /* NOTHING selected → prompt the reader to pick a class. */
              <div className="py-14 text-center">
                <p className="text-sm text-fg-muted">Select a class in the chart above to break it down.</p>
                <p className="text-[11px] text-fg-faint mt-1">
                  Its holdings, contribution and currency — or, for stocks, sector &amp; region
                  versus {data.benchmark ?? 'SP500'}.
                </p>
              </div>
            ) : sleeve ? (
              /* NON-EQUITY class: its holdings' contribution + a currency chart, no benchmark. */
              <SleeveBreakdown holdings={data.book_holdings ?? []} bucket={sleeve} />
            ) : (
              /* STOCKS: the sector / region / currency composition versus the benchmark, with the
                 Brinson attribution ("why", from the Attribution button) or a per-bucket drill-down
                 (from a bar) docked full-width below — mutually exclusive. */
              <>
                <div className="grid gap-4 lg:grid-cols-3">
                  {(data.axes ?? []).map((a) => (
                    <Chart key={a.axis} axis={a.axis} rows={a.rows}
                      benchmark={data.benchmark ?? 'SP500'}
                      onBucket={(axis, b) => { if (isBasket) return; setWhy(null); setBucket(
                        (prev) => prev && prev.axis === axis && prev.bucket === b ? null : { axis, bucket: b }); }}
                      selected={bucket?.axis === a.axis ? bucket.bucket : null} />
                  ))}
                </div>
                {(why || bucket) && (
                  <div className="mt-4">
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
              </>
            )}
          </>
        )}
      </div>
    </div>
  );
}
