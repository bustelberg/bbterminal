'use client';

import { useEffect, useState } from 'react';
import { Cell, Pie, PieChart, ResponsiveContainer } from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { formatPct, visibleBuckets } from './composition';
import { allocColor, bucketLabel } from './allocationColors';
import { Provenance } from '../../../lib/provenance';
import type { ModelPortfolioAnalysis } from '../../../lib/types/api';
import AttributionPanel from './AttributionPanel';
import BucketDetailPanel from './BucketDetailPanel';
import CompositionDataModal from './CompositionDataModal';
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

/** ⚠ THE BASIS CHANGED (2026-07-31) AND SO DID THESE. The bars are weighted by each position's
 *  value when the window OPENED, over the holdings that can be attributed — the same weights the
 *  Attribution table shows, so a bar equals its own Brinson row. They are no longer "what we hold
 *  now": a stock bought mid-window has no start value and is absent. The `Data` button states the
 *  denominator and names everything the basis leaves out. */
const AXIS_NOTE: Record<string, string> = {
  sector: 'Start-of-window weights. Cash, funds and unpriced holdings have no sector.',
  region: "The issuer's domicile, else its ISIN country. Not the listing venue.",
  currency: 'The reporting currency of the company. Not the listing currency.',
};

/** A bar's own value, direct-labelled.
 *
 * ⚠ IT COMES FROM `composition.ts`, WHICH ALSO DECIDES WHICH BUCKETS ARE SHOWN. A local formatter
 * is how the filter broke once already: this rendered at `toFixed(0)` while the filter assumed one
 * decimal, so a 0.2% bucket printed "0%" and survived a rule written to remove it. Same constant,
 * both jobs. */
const pct = formatPct;

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

type AllocSlice = {
  bucket: string; pct: number; return_pct?: number | null;
  /** Individual holdings in this class, counted AFTER the certificates are looked through. */
  holdings?: number;
};
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
                {/* ⚠ HOW MANY NAMES CARRY THAT WEIGHT. Counted after the certificates are looked
                    through, so a slice reads as the companies actually behind it rather than the
                    lines AIRS stores. "66% in one bond ETF" and "66% across sixty names" draw an
                    identical wedge and are not the same portfolio; the count is the only thing on
                    the legend that separates them. */}
                {(s.holdings ?? 0) > 0 && (
                  <span className="text-fg-faint tabular-nums"
                    title={`${s.holdings} individual holding${s.holdings === 1 ? '' : 's'} in ${bucketLabel(s.bucket)}`}>
                    ({s.holdings})
                  </span>
                )}
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

function Chart({ axis, rows, basis, positions, attributablePct, unpricedPct, excluded, benchmark,
  name, onBucket, selected }: {
  axis: string;
  rows: Row[];
  /** The denominator in words, and how many positions it spans — from the server, per axis. */
  basis?: string | null;
  positions?: number | null;
  /** How much of the book has a bucket on this axis. The remainder is mostly funds/bonds/cash. */
  attributablePct?: number | null;
  /** The part of that remainder which is a genuine hole — held, but unpriceable. */
  unpricedPct?: number | null;
  excluded?: Axis['excluded'];
  benchmark: string;
  name: string;
  onBucket: (axis: string, bucket: string) => void;
  selected: string | null;
}) {
  // ⚠ A HEADER BUTTON, NOT A CLICK ON THE CHART BODY. The bars are ALREADY a click target — they
  // open the per-bucket attribution panel — so making the surface around them open a second thing
  // would put two different drill-downs a few pixels apart.
  const [showData, setShowData] = useState(false);
  // Sector is an EQUITY-only view; a non-equity selection leaves it with no portfolio side, so say
  // so rather than draw the benchmark's sectors beside an empty portfolio.
  const sectorEmpty = axis === 'sector' && rows.every((r) => (r.portfolio_pct ?? 0) === 0);
  // Largest share first — a reader scans a ranked list, not the server's order.
  // ⚠ Filtered to buckets with weight on AT LEAST ONE side, never "where the portfolio holds
  // something": a bucket the book does not own but the benchmark does is an unowned region/sector,
  // which is a finding, not an empty row. See `composition.ts`.
  const sorted = visibleBuckets([...rows].sort((a, b) =>
    (b.portfolio_pct ?? 0) - (a.portfolio_pct ?? 0) || (b.benchmark_pct ?? 0) - (a.benchmark_pct ?? 0)));

  return (
    <section className={`bg-card border rounded-xl p-4 ${
      selected ? 'border-accent-500/40' : 'border-neutral-800/40'}`}>
      <div className="flex items-baseline gap-2">
        <h4 className="text-sm font-semibold text-fg-strong">{AXIS_LABEL[axis] ?? axis}</h4>
        <button type="button" onClick={() => setShowData(true)}
          title="Show every holding behind these bars, at the weight each bar counted it at — and what the percentages are a share of."
          className="ml-auto text-[10px] px-1.5 py-0.5 rounded-md border border-neutral-800/40 text-fg-faint hover:text-accent-300 hover:border-accent-500/50 transition-colors">
          Data
        </button>
      </div>
      <p className="text-[11px] text-fg-faint mt-0.5">{AXIS_NOTE[axis]}</p>
      {/* ⚠ ONLY THE UNPRICED HOLDINGS GET A WARNING, AND THIS IS THE WHOLE DISTINCTION. A fund, a
          bond and a cash line have no sector by definition — they are not Stocks in our own
          classification and have their own slice of the allocation chart, so counting them as
          weight this chart "cannot handle" turned a perfectly ordinary 13% in ETFs into what
          looked like a defect. An unpriced STOCK is the real hole: it is missing from a bucket
          that should contain it, which makes that bucket read low. */}
      {(unpricedPct ?? 0) > 0.005 && (
        <p className="text-[11px] text-warn-300 mt-0.5"
          title="Real holdings, in real buckets, that we have no price series for. They are absent from the bars, so the buckets they belong to read lower than they are. Open Data for the names.">
          ⚠ {unpricedPct!.toFixed(1)}% held but unpriceable — missing from these bars
        </p>
      )}
      {/* Stated, not warned: how much of the book has a {axis} at all. */}
      {attributablePct != null && attributablePct < 99.95 && (
        <p className="text-[11px] text-fg-faint mt-0.5"
          title="The rest is funds, bonds and cash, which have no sector — see the allocation chart above. Open Data for the names.">
          {attributablePct.toFixed(0)}% of the book has a {AXIS_LABEL[axis]?.toLowerCase() ?? axis}
        </p>
      )}
      {showData && (
        <CompositionDataModal axis={axis} rows={rows} basis={basis} positions={positions}
          attributablePct={attributablePct} unpricedPct={unpricedPct} excluded={excluded}
          benchmark={benchmark} name={name} onClose={() => setShowData(false)} />
      )}
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

/** THE WHOLE PORTFOLIO, one row per instrument, grouped by asset class — what the reader sees
 *  before picking a class. Every long position is here, counted AFTER the certificates are looked
 *  through, so a model that stores twelve lines shows the 172 instruments it actually holds.
 *
 *  ⚠ WEIGHT IS `weight_now_pct`, NOT `weight_pct`. The two answer different questions and only one
 *  of them belongs beside a chart: `weight_now_pct` shares the allocation chart's denominator, so
 *  each class subtotal here EQUALS its slice to the decimal. `weight_pct` is the opening-value
 *  weight the per-class contribution view needs (a contribution must be weighted by what was held
 *  when the window opened, not by what a winner has grown into) and
 *  is measured over the priced book only — showing it here would put a table and the chart directly
 *  above it a few points apart, which reads as a bug in both.
 *
 *  ⚠ RETURN IS `own_return_pct`, NOT `return_pct`, FOR THE SAME REASON IN REVERSE. `return_pct` is
 *  the book's value change, and the book knows what the CERTIFICATE did, not what NVIDIA did —
 *  splitting it hands all 135 stocks their wrapper's number (NVIDIA read +0.08% against its own
 *  +2.82%). `own_return_pct` prices each instrument off its own EUR series. It follows that the
 *  rows do NOT sum to a class return, so no class return is shown in this table; that figure is a
 *  different measure and it lives in the chart legend, where it is the only one on offer.
 *
 *  NO CLASS RETURN IN THE GROUP HEADER. Two returns for one class, a few points apart and both
 *  correct, is exactly the pair a reader cannot arbitrate. */
type HoldingSortKey = 'name' | 'weight' | 'weightStart' | 'return';

/** Weights and returns to TWO decimals, always — including the trailing zero. A 0.4% and a 0.44%
 *  position round to the same "0.4%", and in a 172-row table that is where the small holdings live.
 *  Fixed precision also keeps the column optically aligned in a mono font. */
const num2 = (v: number) => v.toFixed(2);
const ret2 = (v?: number | null) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`);

function PortfolioHoldings({ holdings, slices, asOf }: {
  holdings: BookHolding[]; slices?: AllocSlice[]; asOf?: string | null;
}) {
  const [sortKey, setSortKey] = useState<HoldingSortKey>('weight');
  const [dir, setDir] = useState<'asc' | 'desc'>('desc');

  if (!holdings.length) return (
    <p className="text-[11px] text-fg-subtle py-8 text-center">No positions to show for this portfolio.</p>
  );

  // Classes in the chart's own order, so the eye moves between them without re-reading.
  const order = (slices ?? []).map((s) => s.bucket);
  const groups = [...new Set([...order, ...holdings.map((h) => h.bucket)])]
    .map((bucket) => ({
      bucket,
      slice: (slices ?? []).find((s) => s.bucket === bucket),
      rows: holdings.filter((h) => h.bucket === bucket),
    }))
    .filter((g) => g.rows.length > 0);

  const cmp = (a: BookHolding, b: BookHolding) => {
    if (sortKey === 'name') {
      const c = (a.name ?? '').localeCompare(b.name ?? '');
      return dir === 'asc' ? c : -c;
    }
    const pick = (h: BookHolding) => (sortKey === 'weight' ? (h.weight_now_pct ?? 0)
      : sortKey === 'weightStart' ? h.weight_start_pct : h.own_return_pct);
    const av = pick(a);
    const bv = pick(b);
    if (av == null && bv == null) return 0;
    if (av == null) return 1;          // an unpriced holding always sorts last, both directions
    if (bv == null) return -1;
    return dir === 'asc' ? av - bv : bv - av;
  };
  const click = (k: HoldingSortKey) => {
    if (k === sortKey) { setDir((d) => (d === 'asc' ? 'desc' : 'asc')); return; }
    setSortKey(k);
    setDir(k === 'name' ? 'asc' : 'desc');
  };
  const caret = (k: HoldingSortKey) => (
    <span className={`ml-0.5 ${sortKey === k ? 'text-accent-400' : 'text-transparent'}`}>
      {sortKey === k && dir === 'asc' ? '▲' : '▼'}
    </span>
  );
  const th = 'py-2 font-medium cursor-pointer select-none whitespace-nowrap hover:text-fg-soft transition-colors';
  const anchor = holdings.find((h) => h.own_return_from)?.own_return_from;

  return (
    <div className="bg-card border border-neutral-800/40 rounded-xl overflow-hidden">
      <div className="flex items-center justify-between gap-2 px-4 py-2.5 border-b border-neutral-800/40">
        <h4 className="text-xs font-medium text-fg-strong">
          Holdings
          <Provenance source="airs_volk" asOf={asOf} kind="formula" column
            what={'Every instrument the portfolio holds, grouped by asset class. A position held '
              + 'through a certificate is listed as the instruments behind it, not as the '
              + 'certificate.'}
            how={'One row per ISIN. An instrument reached through more than one strategy is a '
              + 'single row with the weights added, and every strategy it came through is named '
              + 'in the Via column.'} />
        </h4>
        <span className="text-[10px] font-mono text-fg-faint">
          {holdings.length} positions · {groups.length} classes
        </span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="text-[10px] uppercase tracking-wide text-fg-faint bg-card sticky top-0 z-10">
            <tr className="border-b border-neutral-800/40">
              <th className="text-right w-10 pl-4 pr-2 py-2 font-medium">#</th>
              <th className={`text-left ${th}`} onClick={() => click('name')}>Name{caret('name')}</th>
              <th className="text-left py-2 font-medium w-32">ISIN</th>
              <th className="text-left py-2 font-medium">
                Via
                <Provenance source="airs_model" asOf={asOf} kind="copied" column
                  what={'The strategies this instrument is held through — the model portfolios '
                    + 'whose certificates were looked through to reach it.'}
                  how={'Empty means the position is held directly. More than one is normal: the '
                    + 'same stock can be reached through several certificates, and the weights '
                    + 'shown are the sum of all of them.'} />
              </th>
              {/* ⚠ THE COLUMN THAT RECONCILES THIS TABLE WITH THE SECTOR CHARTS. Those bars are
                  weighted at the window's OPEN; this table was current-value only, so dividing a
                  7.02% here by the Stocks slice and expecting the chart's 5.75% never worked —
                  and the gap looks exactly like a bug. Both numbers now sit side by side, and the
                  difference between them IS what the position did. */}
              <th className={`text-right w-24 ${th}`} onClick={() => click('weightStart')}>
                Weight (start){caret('weightStart')}
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what={'The share of the portfolio held in this instrument when the window '
                    + 'opened — the same weight the Sector / Region / Currency charts use.'}
                  how={'The position’s Beginwaarde ÷ the portfolio’s total Beginwaarde. Taken '
                    + 'from the very rows those charts are built from, not recomputed. A bar on '
                    + 'those charts is this number divided by the share of the book that has a '
                    + 'bucket at all, since a bar excludes funds, cash and unpriced holdings. '
                    + '0.00% means the position was bought after the window opened.'} />
              </th>
              <th className={`text-right w-24 ${th}`} onClick={() => click('weight')}>
                Weight (now){caret('weight')}
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what={'The share of the portfolio held in this instrument, right now.'}
                  how={'The position’s current EUR value ÷ the portfolio’s total current EUR '
                    + 'value. A position reached through a certificate takes the certificate’s '
                    + 'EUR value split by the strategy’s own percentages, so each class subtotal '
                    + 'equals its share of the chart above.'} />
              </th>
              <th className={`text-right w-28 pr-4 ${th}`} onClick={() => click('return')}>
                Return{caret('return')}
                <Provenance source="yfinance" asOf={asOf} kind="formula" column
                  what={'What this instrument itself returned in euros over the window, '
                    + 'independent of how much of it the portfolio holds.'}
                  how={'Its closing price now ÷ its closing price on '
                    + `${anchor ?? 'the window’s opening date'}, minus 1, both converted to euros `
                    + 'at that date’s rate, so the figure carries the currency leg. The window '
                    + 'opens on 1 January or on the composition’s effective date, whichever is '
                    + 'later. This is the instrument’s own return, not the portfolio’s share of '
                    + 'it — the rows do not add up to a class return.'} />
              </th>
            </tr>
          </thead>
          {groups.map((g) => (
            <tbody key={g.bucket}>
              <tr className="bg-inset border-y border-neutral-800/40">
                <td className="pl-4" />
                <td className="py-2 text-[11px] font-medium text-fg-strong" colSpan={3}>
                  <span className="inline-block w-2.5 h-2.5 rounded-sm mr-2 align-middle"
                    style={{ background: allocColor(g.bucket) }} />
                  {bucketLabel(g.bucket)}
                  <span className="ml-2 px-1.5 py-0.5 rounded-md bg-overlay/5 text-[10px] font-normal text-fg-muted">
                    {g.rows.length}
                  </span>
                </td>
                {/* The class at the window's open — the denominator a reader needs to check a bar
                    against. Summed from the rows, since the pie carries only the current figure. */}
                <td className="py-2 text-right font-mono text-[11px] font-semibold text-fg-muted">
                  {num2(g.rows.reduce((s, h) => s + (h.weight_start_pct ?? 0), 0))}%
                </td>
                <td className="py-2 text-right font-mono text-[11px] font-semibold text-fg-strong">
                  {num2(g.slice?.pct ?? g.rows.reduce((s, h) => s + (h.weight_now_pct ?? 0), 0))}%
                </td>
                {/* Deliberately blank. A class return here would be the book's value change —
                    a different measure from the instrument returns beneath it, and putting the
                    two in one column is how a reader ends up trusting neither. */}
                <td className="pr-4" />
              </tr>
              {[...g.rows].sort(cmp).map((h, i) => (
                <tr key={h.isin ?? `${g.bucket}-${h.name ?? i}`}
                  className="border-b border-neutral-800/[0.15] last:border-0 hover:bg-overlay/[0.03] transition-colors">
                  <td className="py-1.5 pl-4 pr-2 text-right font-mono text-[10px] text-fg-faint tabular-nums">{i + 1}</td>
                  <td className="py-1.5 pr-3 text-fg truncate max-w-0" title={h.name ?? undefined}>{h.name ?? '—'}</td>
                  <td className="py-1.5 pr-3 font-mono text-[10px] text-fg-faint">{h.isin ?? '—'}</td>
                  <td className="py-1.5 pr-3">
                    <ViaChips names={h.via_names ?? []} />
                  </td>
                  {/* ⚠ A BLANK IS NOT A ZERO HERE. `null` = no ISIN to join on (cash); `0.00%` =
                      a real fact, the position was bought after the window opened and therefore
                      carries no weight on ANY of the composition charts. */}
                  <td className="py-1.5 text-right font-mono text-fg-muted tabular-nums"
                    title={h.weight_start_pct === 0
                      ? 'Bought after the window opened — no start weight, so it is absent from the Sector / Region / Currency charts.'
                      : undefined}>
                    {h.weight_start_pct == null ? '—' : `${num2(h.weight_start_pct)}%`}
                  </td>
                  <td className="py-1.5 text-right font-mono text-fg tabular-nums">
                    {num2(h.weight_now_pct ?? 0)}%
                  </td>
                  {/* An unpriced position shows a dash, never 0% — "we could not price this over
                      the window" and "it did not move" are different facts and a 0 states the
                      wrong one. An interpolated opening mark is flagged per value, because that
                      one IS a property of the number rather than of the column. */}
                  <td className={`py-1.5 pr-4 text-right font-mono tabular-nums ${retTone(h.own_return_pct)}`}
                    title={h.own_return_pct == null ? 'No price series over this window' : undefined}>
                    {ret2(h.own_return_pct)}
                    {h.own_return_estimated && (
                      <span className="ml-1 text-warn-400" title="Opening price interpolated — no close near the window's start">≈</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          ))}
        </table>
      </div>
    </div>
  );
}

/** The strategies a holding is reached through. Two named, the rest counted — a third long model
 *  name pushes the numeric columns off the row, and the full list is one hover away. */
function ViaChips({ names }: { names: string[] }) {
  if (!names.length) return <span className="text-[10px] text-fg-faint">direct</span>;
  const shown = names.slice(0, 2);
  return (
    <span className="flex flex-wrap items-center gap-1" title={names.join(' · ')}>
      {shown.map((n) => (
        <span key={n}
          className="px-1.5 py-0.5 rounded-md bg-accent-500/10 text-accent-400 text-[10px] whitespace-nowrap max-w-[11rem] truncate">
          {n}
        </span>
      ))}
      {names.length > shown.length && (
        <span className="text-[10px] text-fg-faint">+{names.length - shown.length}</span>
      )}
    </span>
  );
}

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

/** What to show for a bond / fund / cash / alternatives class, where sector-vs-SP500 says nothing:
 *  (1) a CONTRIBUTION breakdown — each holding's weight × its own return — and (2) a CURRENCY
 *  exposure chart (no benchmark). Both computed client-side from the book detail, so switching
 *  classes is instant. */
type SleeveSortKey = 'name' | 'weight' | 'return' | 'contrib';

function SleeveBreakdown({ holdings, bucket }: { holdings: BookHolding[]; bucket: string }) {
  // Sortable table — click a header to toggle direction. Default: weight, largest first.
  const [sortKey, setSortKey] = useState<SleeveSortKey>('weight');
  const [dir, setDir] = useState<'asc' | 'desc'>('desc');

  // ⚠ PRICED POSITIONS ONLY. `weight_pct` is null where we could not price the holding over the
  // window, and a row with no return contributes nothing to the breakdown — so listing it at a
  // `?? 0` weight would add a 0.0% line that reads as a tiny holding rather than an unpriced one.
  const rows = holdings.filter((h) => h.bucket === bucket && h.weight_pct != null);
  const totalW = rows.reduce((s, h) => s + (h.weight_pct ?? 0), 0) || 1;
  // Renormalise each holding's (opening-value) weight WITHIN the class, then contribution =
  // weight × return, in pp of the class return.
  //
  // ⚠ THE RETURN IS THE INSTRUMENT'S OWN (`own_return_pct`), NOT THE BOOK SPLIT. The book's
  // per-leg return is the CERTIFICATE's return stamped on everything behind it — every stock in
  // one strategy showing the same figure — which made Σ contribution land on the class number
  // exactly while telling the reader nothing true about any single holding. Exact and
  // uninformative is the worse trade: the sum is now approximate and each row is real.
  const items = rows.map((h) => {
    const w = (h.weight_pct ?? 0) / totalW * 100;
    return { name: h.name, weight: w, ret: h.own_return_pct,
      contrib: h.own_return_pct != null ? (w / 100) * h.own_return_pct : null };
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
      No priced holdings in {bucketLabel(bucket)}.
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
                    <td className="py-1 px-2 text-right font-mono text-fg">{it.weight.toFixed(2)}%</td>
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
            {/* ⚠ NO LOOK-THROUGH BANNER HERE. These charts ARE drawn through the certificates —
                the composition is the stocks behind them, not the lines AIRS stores — and the
                payload still reports `looked_through_pct` / `opaque_pct` / `looked_through` for
                anyone reading the API. It is simply not announced on screen. */}
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
              /* NOTHING selected → the whole portfolio, one row per instrument, grouped by class.
                 A prompt to click something used to sit here; it told the reader what to do next
                 and nothing about what they hold. Picking a class still narrows this to that
                 class's own breakdown. */
              <PortfolioHoldings holdings={data.book_holdings ?? []} slices={data.allocation}
                asOf={data.as_of} />
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
                      basis={a.basis} positions={a.positions} name={name}
                      attributablePct={a.attributable_pct} unpricedPct={a.unpriced_pct}
                      excluded={a.excluded}
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
                        axis={bucket.axis} bucket={bucket.bucket} source={source}
                        onClose={() => setBucket(null)} />
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
