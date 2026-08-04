'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { formatPct, visibleBuckets } from './composition';
import { allocColor, bucketLabel } from './allocationColors';
import { Provenance } from '../../../lib/provenance';
import { trace, traceError } from '../../../lib/debugTrace';
import type { ModelPortfolioAnalysis } from '../../../lib/types/api';
import AttributionPanel from './AttributionPanel';
import BucketDetailPanel from './BucketDetailPanel';
import CompositionDataModal from './CompositionDataModal';
import { type Basket } from './types';

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

  // ⚠ THE DIFFERENCE OF THE TWO NUMBERS ON SCREEN, NOT THE SERVER'S UNROUNDED ONE — because the
  // row is now written as an equation, and an equation that does not hold is worse than three
  // separate figures. The server's `ytd_excess_pct` IS `portfolio − benchmark`, but at two
  // decimals the rounded difference and the difference of the roundings disagree by 0.01pp often
  // enough to notice (3.945 − 11.684 = −7.739 → "−7.74pp", beside a printed 3.95 and 11.68 whose
  // difference is −7.73). Rounding each operand first makes the line true as displayed; the
  // discrepancy it absorbs is never more than 0.01pp.
  const r2 = (v: number | null | undefined) => (v == null ? null : Math.round(v * 100) / 100);
  const p = r2(r?.portfolio_ytd_pct);
  const b = r2(r?.benchmark_ytd_pct);
  const excess = p == null || b == null ? r?.ytd_excess_pct : p - b;
  // Centred on the chips (the row is `items-center`), so the operators hold the middle of the band
  // rather than hanging off one edge of it.
  const op = 'text-base font-mono text-fg-faint shrink-0';
  return (
    <div className="flex items-center gap-2 self-center flex-wrap">
      <Chip label="Return (YTD)" value={sp(r?.portfolio_ytd_pct)} valueClass={tone(r?.portfolio_ytd_pct)} />
      <span className={op} aria-hidden>−</span>
      <Chip label={`vs ${benchmark} return`} value={sp(r?.benchmark_ytd_pct)} valueClass={tone(r?.benchmark_ytd_pct)} />
      <span className={op} aria-hidden>=</span>
      <Chip label="Excess" value={spp(excess)} valueClass={tone(excess)}
        hint="The portfolio's return minus the benchmark's, in percentage POINTS — the two figures to the left, subtracted." />
      {onAttribution && (
        <button type="button" onClick={onAttribution}
          title="Why? — break the excess into allocation vs selection (Brinson-Fachler attribution)."
          className={`ml-1 cursor-pointer rounded-lg border px-3 py-1.5 min-w-[6rem] text-xs font-medium transition-colors flex items-center justify-center ${
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
/** A return, TWO decimals, always — the same precision the holdings table and the scorecard use.
 *  One decimal was the odd one out: the legend said a class made +7.4% while the rows inside it
 *  were quoted to the hundredth, so the two could not be tied together by eye. `—` is not a zero:
 *  it is a class we could not price (cash has no return to state). */
const fmtRet = (v?: number | null) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`);
const retTone = (v?: number | null) => (v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400');

/** The portfolio's own asset-class split, as ranked horizontal bars — one row per class, each
 *  clickable to filter the charts below. Palette (no red/green, CVD-validated) shared with the
 *  /portfolios "Class" column, so a class wears one colour across the whole app.
 *
 *  ⚠ BARS, NOT A DONUT (2026-08-04). Every job this thing does is a job a pie is bad at:
 *    * COMPARING classes — an angle is the hardest encoding to judge; a common baseline is the
 *      easiest. Stock ETF 10.92% against Alternatives 1.02% is one glance here and a squint there.
 *    * SHOWING THE SMALL ONES — Unclassified is 0.12%, a slice 0.4° wide. As a row it still has a
 *      label, a value, a return and a hit target, which is what it needs to be *clickable*.
 *    * LABELLING — the donut could only fit a `%` inside slices over 6%, so half the classes were
 *      labelled and half were not, and the legend beside it had to repeat every number anyway.
 *  It also drops the only recharts import in this modal: the whole thing is now flex + divs.
 *
 *  ⚠ THE SCALE IS FIXED 0–100%, NOT SCALED TO THE BIGGEST CLASS — same rule as the composition
 *  bars below. A bar's LENGTH is its share of the portfolio; stretching Equity's 85% to full width
 *  would make every other class read bigger than it is, which is the one thing a part-to-whole
 *  chart must not do. The cost is that a 0.12% bar is a sliver, so it carries a minimum width and
 *  its number is printed beside it. */
/** The axis's ticks, and the gridlines inside every bar — ONE list, so a tick can never sit where
 *  no gridline is. Quarters: enough to read a length against, few enough to stay recessive. */
const AXIS_TICKS = [0, 25, 50, 75, 100];

type Band = NonNullable<ModelPortfolioAnalysis['bands']>[number];

function AllocationBars({ slices, selected, onSelect, variant, bands }: {
  slices: AllocSlice[];
  selected?: string | null;
  onSelect?: (bucket: string | null) => void;
  /** The risk profile AIRS's own name says this model is offered at, or null for the products
   *  that are not offered at one. */
  variant?: string | null;
  /** The policy for that profile — the band each class is SUPPOSED to sit in. */
  bands?: Band[];
}) {
  const ordered = [...slices].sort((a, b) => b.pct - a.pct);
  const toggle = onSelect ? (b: string) => onSelect(selected === b ? null : b) : undefined;
  const bandOf = new Map((bands ?? []).map((b) => [b.bucket, b]));
  /** Held outside the permitted range — the finding this whole overlay exists to surface. Only a
   *  bound that is actually SET can be breached: an unset max is not a max of 100. */
  const breach = (s: AllocSlice) => {
    const b = bandOf.get(s.bucket);
    if (!b) return null;
    if (b.min_pct != null && s.pct < b.min_pct) return `below the ${b.min_pct}% minimum`;
    if (b.max_pct != null && s.pct > b.max_pct) return `above the ${b.max_pct}% maximum`;
    return null;
  };

  return (
    // ⚠ WIDTH IS SET HERE SO THE BARS GET IT. Every other column is fixed, so the track is the
    // remainder — widening the block is the only way to lengthen the bars, and the axis above them
    // is laid out from the same fixed columns so the two cannot drift apart.
    <div className="shrink-0 w-[41rem] max-w-full">
      <div className="flex items-center gap-2 mb-1.5 flex-wrap">
        <span className="text-[10px] uppercase tracking-wide text-fg-faint">Allocation</span>
        {/* The profile read off AIRS's own portfolio name — the same classifier the correlation
            matrix filters by. Shown because the bands below are only meaningful once the reader
            knows WHICH policy is being drawn. */}
        {variant && (
          <span className="text-[10px] px-1.5 py-0.5 rounded-md bg-overlay/5 text-fg-muted"
            title={`Read from this model's own AIRS name. The bands drawn over the bars are the ${variant} allocation policy.`}>
            {variant}
          </span>
        )}
        {onSelect && (selected
          ? <button type="button" onClick={() => onSelect(null)}
              className="cursor-pointer text-[10px] text-accent-400 hover:text-accent-300">
              filtering to {bucketLabel(selected)} — show all ✕
            </button>
          : <span className="text-[10px] text-fg-faint">click a class to filter the charts</span>)}
      </div>
      {/* ⚠ A SECOND THING IS ON THE CHART NOW, SO IT IS NAMED. The bar is what the portfolio holds;
          the bracket is what the policy permits. Without this line the caps read as gridlines and
          the target tick as noise. Only rendered when there IS a policy — the products with no
          risk profile draw no bands and get no legend for them. */}
      {bandOf.size > 0 && (
        <div className="flex items-center gap-3.5 text-[10px] text-fg-faint mb-1.5">
          {/* The swatches are the marks themselves at row scale — a legend drawn differently from
              the thing it names is one more thing to map. */}
          <span className="flex items-center gap-1.5">
            <span className="relative inline-block w-5 h-3 rounded-sm bg-neutral-500/[0.14]">
              <span className="absolute inset-y-0 left-0 w-0.5 bg-neutral-500/70" />
              <span className="absolute inset-y-0 right-0 w-0.5 bg-neutral-500/70" />
            </span>
            {variant} band
          </span>
          <span className="flex items-center gap-1.5">
            <span className="inline-block w-[3px] h-3 rounded-sm bg-neutral-800/85" />
            target
          </span>
          <span className="flex items-center gap-1.5">
            <span className="inline-block w-0.5 h-3 bg-warn-500" />
            outside the band
          </span>
        </div>
      )}
      {/* ⚠ THE AXIS SPELLS OUT THE SCALE THE BARS ARE ALREADY ON — 0–100% of the portfolio, fixed,
          never stretched to the biggest class. Without it the reader has only the printed values to
          tell a full-width bar from an 85% one, which is exactly the check a bar chart is supposed
          to make free. Its spacers mirror the row's fixed columns EXACTLY (same widths, same gaps,
          same padding), so the 0 and the 100 sit on the ends of the track beneath them; change a
          column width in one place and it must change in the other. */}
      <div className="flex items-end gap-2.5 -mx-1.5 px-1.5 mb-1 select-none" aria-hidden>
        <span className="w-[6.5rem] shrink-0" />
        <span className="relative flex-1 h-3.5">
          {AXIS_TICKS.map((t) => (
            <span key={t} className="absolute bottom-0 flex flex-col items-center"
              style={{ left: `${t}%`, transform: t === 0 ? 'none' : t === 100 ? 'translateX(-100%)' : 'translateX(-50%)' }}>
              <span className="text-[9px] text-fg-faint tabular-nums leading-none mb-0.5">{t}</span>
              <span className="w-px h-1 bg-neutral-700/60" />
            </span>
          ))}
        </span>
        <span className="w-12 shrink-0 text-[9px] text-fg-faint text-right leading-none">%</span>
        <span className="w-7 shrink-0" />
        <span className="w-14 shrink-0 text-[9px] text-fg-faint text-right leading-none">YTD</span>
      </div>
      {/* ⚠ NO LEGEND. One series, and every bar is directly labelled with the class it belongs to —
          a legend box would map colours to names the row already prints. (The composition charts
          below DO carry one: two series there, so identity cannot be colour-alone.) */}
      <div className="flex flex-col gap-0.5">
        {ordered.map((s) => {
          const active = selected === s.bucket;
          const Row = toggle ? 'button' : 'div';
          return (
            <Row key={s.bucket} {...(toggle ? { type: 'button' as const, onClick: () => toggle(s.bucket) } : {})}
              title={`${bucketLabel(s.bucket)} — ${s.pct.toFixed(2)}% of the portfolio`
                + `${s.holdings ? `, ${s.holdings} holding${s.holdings === 1 ? '' : 's'}` : ''}`
                + `${s.return_pct == null ? '' : `, ${fmtRet(s.return_pct)} YTD`}`
                + ((): string => {
                  const b = bandOf.get(s.bucket);
                  if (!b) return '';
                  const range = `${b.min_pct ?? '—'}% to ${b.max_pct ?? '—'}%`
                    + (b.default_pct == null ? '' : `, target ${b.default_pct}%`);
                  return `\n${variant} policy: ${range}`
                    + (breach(s) ? `\n⚠ held ${breach(s)}` : '');
                })()}
              // ⚠ THE CURSOR IS CONDITIONAL BECAUSE THE CLICK IS. These rows render as a plain
              // div for an ad-hoc basket (no `onSelect`), and a pointer over something that does
              // nothing is a worse lie than no pointer over something that does.
              className={`group flex items-center gap-2.5 rounded-md -mx-1.5 px-1.5 py-1 text-left w-full transition-colors ${
                toggle ? 'cursor-pointer' : ''} ${
                active ? 'bg-accent-500/10' : 'hover:bg-overlay/[0.03]'} ${
                selected && !active ? 'opacity-45' : ''}`}>
              <span className={`w-[6.5rem] shrink-0 truncate text-[11px] ${
                active ? 'font-medium text-fg-strong' : 'text-fg-muted'}`}>{bucketLabel(s.bucket)}</span>
              {/* ⚠ THIS IS A BULLET CHART, AND ITS ONE RULE IS THAT THE MEASURE IS THINNER THAN
                  THE RANGE. The band sits BEHIND the bar at full height while the bar is a slimmer
                  ribbon down the middle, so the band's top and bottom edges stay visible straight
                  THROUGH the bar — no wash over the colour, nothing hidden, and the overlap reads
                  as "this much of the permitted range is used". The first version drew the band on
                  top in a 6% overlay, which darkened the class colour where they met and vanished
                  where they didn't: two different readings of one annotation. */}
              <span className="relative h-[18px] flex-1 rounded bg-inset overflow-hidden">
                {/* Recessive gridlines at the axis's own ticks. The ends are the track's own
                    edges, so only the interior ticks are drawn. */}
                {AXIS_TICKS.filter((t) => t > 0 && t < 100).map((t) => (
                  <span key={t} className="absolute inset-y-0 w-px bg-neutral-700/30"
                    style={{ left: `${t}%` }} />
                ))}
                {(() => {
                  const b = bandOf.get(s.bucket);
                  if (!b) return null;
                  const lo = b.min_pct ?? 0;
                  const hi = b.max_pct ?? 100;
                  const bad = breach(s);
                  // The breached bound goes amber — the mark points at WHICH limit was crossed,
                  // which the amber value at the end of the row cannot say. The bar itself keeps
                  // its class colour: colour follows the entity, never its status.
                  const cap = (mine: 'min' | 'max') =>
                    (bad?.includes(mine === 'min' ? 'minimum' : 'maximum')
                      ? 'bg-warn-500' : 'bg-neutral-500/70');
                  return (
                    <>
                      <span className="absolute inset-y-0 bg-neutral-500/[0.14] pointer-events-none"
                        style={{ left: `${lo}%`, width: `${Math.max(0, hi - lo)}%` }} />
                      {b.min_pct != null && (
                        <span className={`absolute inset-y-0 w-0.5 pointer-events-none ${cap('min')}`}
                          style={{ left: `${b.min_pct}%` }} />
                      )}
                      {b.max_pct != null && (
                        <span className={`absolute inset-y-0 w-0.5 pointer-events-none ${cap('max')}`}
                          style={{ left: `calc(${b.max_pct}% - 2px)` }} />
                      )}
                    </>
                  );
                })()}
                {/* The measure: a slim ribbon, centred, so the band shows above and below it. */}
                <span className="absolute inset-y-[5px] left-0 rounded-sm"
                  style={{ width: `${Math.min(100, s.pct)}%`, minWidth: 3,
                    background: allocColor(s.bucket) }} />
                {/* The target, LAST so it crosses the bar — a target hidden under the measure is
                    the one comparison this chart exists to make. Full height and dark, so it can
                    never be mistaken for the lighter band caps beside it. */}
                {bandOf.get(s.bucket)?.default_pct != null && (
                  <span className="absolute inset-y-0 w-[3px] rounded-sm bg-neutral-800/85 pointer-events-none"
                    style={{ left: `calc(${bandOf.get(s.bucket)!.default_pct}% - 1.5px)` }} />
                )}
              </span>
              {/* Direct value label, in INK — text wears text tokens; the bar beside it carries the
                  colour. TWO decimals, matching the class subtotals in the holdings table below:
                  the same number printed at two precisions reads as two measurements. ⚠ Not
                  `pct`/`formatPct`, which is bound to the composition-bar filter's threshold. */}
              {/* ⚠ A BREACH IS SAID, NOT ONLY DRAWN. Reading it off the geometry means noticing a
                  bar's end sits past a grey cap — true, and easy to miss on the row you scroll by.
                  The value goes amber and the ⚠ names the bound it crossed. Amber, not red: a
                  weight outside its band is a thing to look at, not a fault. */}
              <span className={`w-12 shrink-0 text-right font-mono text-[11px] tabular-nums ${
                breach(s) ? 'text-warn-500 font-semibold' : 'text-fg-soft'}`}>
                {s.pct.toFixed(2)}%
              </span>
              {/* ⚠ HOW MANY NAMES CARRY THAT WEIGHT. Counted after the certificates are looked
                  through, so a bar reads as the companies actually behind it rather than the lines
                  AIRS stores. "66% in one bond ETF" and "66% across sixty names" draw an identical
                  bar and are not the same portfolio; the count is the only thing here that
                  separates them. */}
              <span className="w-7 shrink-0 text-right text-[10px] text-fg-faint tabular-nums">
                {(s.holdings ?? 0) > 0 ? `(${s.holdings})` : ''}
              </span>
              <span className={`w-14 shrink-0 text-right font-mono text-[11px] tabular-nums ${retTone(s.return_pct)}`}>
                {fmtRet(s.return_pct)}
              </span>
            </Row>
          );
        })}
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

function Chart({ axis, rows, basis, positions, unpricedPct, excluded, benchmark,
  name, onBucket, selected }: {
  axis: string;
  rows: Row[];
  /** The denominator in words, and how many positions it spans — from the server, per axis. */
  basis?: string | null;
  positions?: number | null;
  /** The weight held but unpriceable — a genuine hole in the bars, unlike funds/cash.
   *  ⚠ `attributable_pct` is deliberately NOT read here: a coverage figure phrased as an absence
   *  ("87% of the book has a sector") is heard as a data-quality problem with the stocks, when the
   *  remainder is funds and cash. The line below names the holdings instead. */
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
  // Weight the chart legitimately leaves out. ⚠ Only the not-a-bucket kind — an unpriced holding
  // is a different fact with its own warning above, and adding the two would put a real gap and a
  // definitional one behind one number.
  const excludedWeight = (excluded ?? [])
    .filter((e) => e.reason !== 'unpriced')
    .reduce((s, e) => s + (e.weight_pct ?? 0), 0);
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
          className="ml-auto cursor-pointer text-[10px] px-1.5 py-0.5 rounded-md border border-neutral-800/40 text-fg-faint hover:text-accent-300 hover:border-accent-500/50 transition-colors">
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
      {/* ⚠ NAME WHAT THE REMAINDER *IS*, NEVER WHAT IT LACKS. This read "87% of the book has a
          sector", which is true of the book and reads — under a Stocks-only chart — as a claim
          that 13% of the STOCKS are unclassified. They were not: they were five ETFs and a cash
          line. A percentage phrased as an absence gets heard as a data-quality problem, so the
          line now says which holdings they are and why they are legitimately absent. */}
      {excludedWeight > 0.005 && (
        <p className="text-[11px] text-fg-faint mt-0.5"
          title="Funds, bonds and cash have no sector of their own — they are their own slices of the allocation chart above. Open Data for the names.">
          Excludes {excludedWeight.toFixed(1)}% in funds, bonds and cash — no{' '}
          {AXIS_LABEL[axis]?.toLowerCase() ?? axis} to place
        </p>
      )}
      {showData && (
        <CompositionDataModal axis={axis} rows={rows} basis={basis} positions={positions}
          unpricedPct={unpricedPct} excluded={excluded}
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
              className={`group flex cursor-pointer items-center gap-2.5 rounded-md -mx-1.5 px-1.5 py-1 text-left transition-colors ${
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
 *  ⚠ THERE IS DELIBERATELY NO "WEIGHT (START)" COLUMN (removed 2026-08-04, on request). It was the
 *  bridge between this table and the Sector / Region / Currency bars, which are weighted at the
 *  window's OPEN and over the holdings that HAVE a bucket — so the ONE weight left here does not
 *  divide into a bar, and nothing on screen now claims it does. `weight_start_pct` is still in the
 *  payload and still what those bars are built from; the Sector column is what makes a row findable
 *  behind a bar instead. If the two ever need reconciling again, bring the column back rather than
 *  dividing `weight_now_pct` by a slice — that was measured wrong (ASML 7.02% now, 5.75% on the bar).
 *
 *  ⚠ RETURN IS `own_return_pct`, NOT `return_pct`, FOR THE SAME REASON IN REVERSE. `return_pct` is
 *  the book's value change, and the book knows what the CERTIFICATE did, not what NVIDIA did —
 *  splitting it hands all 135 stocks their wrapper's number (NVIDIA read +0.08% against its own
 *  +2.82%). It follows that the rows do NOT sum to a class return, so no class return is shown in
 *  this table; that figure is a different measure and it lives in the chart legend, where it is
 *  the only one on offer.
 *
 *  ⚠ `own_return_pct` IS AIRS'S OWN FIGURE WHEREVER **ANY** AIRS BOOK HAS ONE — the identical
 *  number that book's expanded row shows (Beginwaarde → Huidige waarde plus net dividend). EVERY
 *  ROUTE INTO THE POSITION IS VALUED BY THE BOOK THAT HOLDS IT, and the figure is their blend,
 *  weighted by what each held when the window OPENED:
 *    * this book's own shares — for a split row, taken PRE-EXPANSION, because the merged row's
 *      values are contaminated by the certificate's proportional split. Any `via` tag used to veto
 *      this outright, which is how MasterCard, 96% of it held outright, was priced off a listing;
 *    * the book BEHIND each certificate, for the part that arrives wrapped (20 of the 23 legs here
 *      exist only there);
 *    * our yfinance series, only where NO AIRS book values the row at all.
 *  Measured: MasterCard = 95.90% × +2.14% (this book) + 4.10% × +17.62% (StarTopSelectie's) =
 *  +2.77%. Either leg alone misrepresents it — the first ignores a leg that nearly tripled the
 *  book's rate on the name, the second describes 4% of the position with the rest invisible.
 *  A route with no return leaves BOTH sides of that average, so the answer is the return of the
 *  legs we can value rather than one silently diluted toward zero by a leg we cannot.
 *
 *  ⚠ AN AIRS FIGURE IS A POSITION RESULT, NOT A PRICE RETURN, AND RUNG 3 MAKES THAT VISIBLE.
 *  AIRS's Beginwaarde is the year-open value OR the PURCHASE value for a position opened during
 *  the year, so the same instrument can differ sharply between two books: MasterCard is +2.14% in
 *  BUS_Offensief_Dyn (held since January) and +17.62% in StarTopSelectie's (bought later). Both
 *  AIRS, both right, different questions — so `own_return_book` names the source and the cell
 *  marks any row that came from another book. The `Via` column cannot stand in for that marker:
 *  a row can be reached through a certificate and STILL be valued here.
 *
 *  NO CLASS RETURN IN THE GROUP HEADER. Two returns for one class, a few points apart and both
 *  correct, is exactly the pair a reader cannot arbitrate. */
type HoldingSortKey = 'name' | 'sector' | 'weight' | 'return';

/** Weights and returns to TWO decimals, always — including the trailing zero. A 0.4% and a 0.44%
 *  position round to the same "0.4%", and in a 172-row table that is where the small holdings live.
 *  Fixed precision also keeps the column optically aligned in a mono font. */
const num2 = (v: number) => v.toFixed(2);

/** The sector to PRINT, '' where there is none to print. The server sends the sector chart's own
 *  bucket, so a fund and an unclassifiable stock both arrive as "Unclassified" — a word that reads
 *  in a table cell as a sector rather than as its absence, and one an alphabetical sort would file
 *  under U among the real ones. Rendered as an em dash, sorted last. Cash keeps its name: it is a
 *  bucket the charts actually draw. */
const sectorLabel = (s?: string | null) => (!s || s === 'Unclassified' ? '' : s);

/** A euro amount for a provenance card — whole euros. The dividend inside a return is context for
 *  the number above it, not a figure anyone reconciles to the cent. */
const eur0 = (v: number) =>
  `€${v.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;

/** The routes that actually spoke for a holding's Return — those with both a return and an opening
 *  value. Fewer than two means the figure is one book's and needs no arithmetic shown. */
const blendLegs = (h: BookHolding) =>
  (h.sources ?? []).filter((s) => s.blend_weight_pct != null && s.return_pct != null);

/** The division behind a single-route Return, in the valuing book's own euros:
 *  `(€68,769 + €0) ÷ €58,669 − 1`. A percentage with no numerator or denominator on screen cannot
 *  be checked against the book it claims to come from — and being checkable against that book is
 *  the whole reason it is preferred over our price series. Null when the book sent no figures. */
function bookMath(h: BookHolding): string | null {
  const s = (h.sources ?? []).find((x) => x.blend_weight_pct != null);
  if (!s?.book_start_value_eur || s.book_current_value_eur == null) return null;
  // Brackets only when there is something to bracket — "(€68,769) ÷ …" reads as a formula with a
  // term missing.
  const now = s.book_income_eur
    ? `(${eur0(s.book_current_value_eur)} + ${eur0(s.book_income_eur)} net dividend)`
    : eur0(s.book_current_value_eur);
  return `${now} ÷ ${eur0(s.book_start_value_eur)} − 1`;
}

/** The arithmetic behind a blended Return, in the reader's own numbers — each leg carrying its
 *  opening euros and the book that valued it, so the whole derivation is ONE line:
 *
 *    held directly 95.90% × +2.14% (€49,557 at the open, BUS_Offensief_Dyn)
 *      + StarTopSelectie Offensief 4.10% × +17.62% (€2,119, StarTopSelectie OFF DYN) = +2.77%
 *
 *  ⚠ THE WEIGHTS ARE SHARES OF THE POSITION'S OPENING VALUE, not of the book — those are in the
 *  Via column and add to the Weight. Two denominators for two questions, and the card says which
 *  it is using rather than leaving a reader to assume they match. */
function blendHow(h: BookHolding): string | null {
  const legs = blendLegs(h);
  if (legs.length < 2) return null;
  const parts = legs.map((s, i) =>
    `${s.label ?? 'held directly'} ${num2(s.blend_weight_pct!)}% × ${fmtRet(s.return_pct)}`
    + ` (${eur0(s.start_value_eur ?? 0)}${i === 0 ? ' at the open' : ''}, ${s.book})`);
  return `${parts.join(' + ')} = ${fmtRet(h.own_return_pct)}`;
}

function PortfolioHoldings({ holdings, slices, asOf, note, bookName }: {
  holdings: BookHolding[]; slices?: AllocSlice[]; asOf?: string | null;
  /** WHY the table is empty, from the server (`book_note`) — three different faults used to
   *  render as one sentence, next to a portfolios list that visibly has rows. */
  note?: string | null;
  /** THIS book's own account name. A Return whose `own_return_book` differs came from the book
   *  behind a certificate, and this is the only thing that tells the two apart. */
  bookName?: string | null;
}) {
  const [sortKey, setSortKey] = useState<HoldingSortKey>('weight');
  const [dir, setDir] = useState<'asc' | 'desc'>('desc');

  // ⚠ AN EMPTY TABLE MUST NAME ITS OWN CAUSE. "No positions to show for this portfolio" was
  // shown for three unrelated faults — unpaired model, book never scanned, opened as a basket —
  // and it was read, correctly, as the modal being broken: the portfolios list right behind it
  // shows the rows, because THAT view reads the account directly and needs no pairing.
  if (!holdings.length) return (
    <div className="py-8 px-6 text-center space-y-1">
      <p className="text-[11px] text-fg-subtle">No valued positions to show here.</p>
      {note && <p className="text-[11px] text-fg-faint max-w-xl mx-auto">{note}</p>}
    </div>
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
    // The two text columns sort as text. ⚠ An unclassified row sorts LAST either way, like an
    // unpriced one below — "we could not classify this" is an absence, not a sector beginning with U.
    if (sortKey === 'name' || sortKey === 'sector') {
      const key = (h: BookHolding) => (sortKey === 'name' ? (h.name ?? '') : sectorLabel(h.sector));
      const av = key(a);
      const bv = key(b);
      if (!av && !bv) return 0;
      if (!av) return 1;
      if (!bv) return -1;
      const c = av.localeCompare(bv);
      return dir === 'asc' ? c : -c;
    }
    const pick = (h: BookHolding) => (sortKey === 'weight' ? (h.weight_now_pct ?? 0)
      : h.own_return_pct);
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
    setDir(k === 'name' || k === 'sector' ? 'asc' : 'desc');
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
                <Provenance source="airs_model" asOf={asOf} kind="formula" column
                  what={'How the portfolio got into this instrument — its own shares, a strategy '
                    + 'whose certificate was looked through to reach it, or both.'}
                  how={'“direct” means the book holds it itself. Where there is more than one way '
                    + 'in, each is sized: the percentages are shares of the whole book and add up '
                    + 'to this row’s Weight. MasterCard, for example, is 4.06% held outright and '
                    + '0.16% through the Star certificate — chipped only with the strategy name it '
                    + 'read as a position the book does not own. Hover for the euro amounts and '
                    + 'each route’s share of the position itself.'} />
              </th>
              {/* ⚠ THE SECTOR CHART'S OWN BUCKET, WHICH IS WHY IT IS WORTH A COLUMN — sorting by
                  it lists the rows behind a bar, in the bar's own vocabulary. A raw
                  `asset_grid.sector` here would say "Financial Services" under a bar saying
                  "Financials" and read as two different exposures. */}
              <th className={`text-left w-36 ${th}`} onClick={() => click('sector')}>
                Sector{caret('sector')}
                <Provenance source="yfinance" asOf={asOf} kind="formula" column
                  what={'The sector this instrument is counted in on the Sector chart above.'}
                  how={'Yahoo’s sector for the ISIN, canonicalised so one sector has one name '
                    + '(“Financial Services” and “Financials” are the same bucket). A dash means '
                    + 'there is no sector to show: a fund, whose listing says nothing about what '
                    + 'it holds, or a holding the grid cannot classify — both are the '
                    + 'Unclassified share of the chart, not a missing lookup.'} />
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
                {/* colSpan 4: Name · ISIN · Via · Sector — every text column, so the class label
                    runs to the first number. */}
                <td className="py-2 text-[11px] font-medium text-fg-strong" colSpan={4}>
                  <span className="inline-block w-2.5 h-2.5 rounded-sm mr-2 align-middle"
                    style={{ background: allocColor(g.bucket) }} />
                  {bucketLabel(g.bucket)}
                  <span className="ml-2 px-1.5 py-0.5 rounded-md bg-overlay/5 text-[10px] font-normal text-fg-muted">
                    {g.rows.length}
                  </span>
                </td>
                <td className="py-2 text-right font-mono text-[11px] font-semibold text-fg-strong whitespace-nowrap">
                  {num2(g.slice?.pct ?? g.rows.reduce((s, h) => s + (h.weight_now_pct ?? 0), 0))}%
                  <Provenance source="airs_volk" asOf={asOf} kind="formula"
                    what={`${g.bucket}'s share of the book TODAY.`}
                    note={g.slice ? 'the allocation chart’s own figure — cash included'
                      : 'summed from the rows below'}
                    how={'Read from the same figure the Allocation bars above are drawn from, so '
                      + 'the two cannot disagree. It counts every class INCLUDING cash, which '
                      + 'is why the classes sum to 100% here. ⚠ The Sector / Region / Currency '
                      + 'bars are weighted at the window’s OPEN and over the holdings that HAVE a '
                      + 'bucket, so dividing a figure here by a class and expecting a bar will '
                      + 'not work — the difference between the two is what the positions did.'} />
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
                    <ViaChips names={h.via_names ?? []} sources={h.sources} />
                  </td>
                  {/* ⚠ A DASH IS AN ANSWER, NOT A MISSING LOOKUP — a fund has no sector to show
                      (its listing says nothing about what it holds) and neither has a holding the
                      grid cannot classify. Both are the chart's Unclassified share, and printing
                      that word in a cell would read as a sector of that name. */}
                  {/* Not `truncate max-w-0` — the Name column already carries that, and a second
                      zero-width column in an auto-layout table fights it for the slack. Sector
                      names are short and known; they get to stay on one line. */}
                  <td className="py-1.5 pr-3 text-fg-muted whitespace-nowrap"
                    title={sectorLabel(h.sector) || 'No sector — a fund, or not classifiable'}>
                    {sectorLabel(h.sector) || <span className="text-fg-faint">—</span>}
                  </td>
                  <td className="py-1.5 text-right font-mono text-fg tabular-nums whitespace-nowrap">
                    {num2(h.weight_now_pct ?? 0)}%
                    <Provenance source="airs_volk" asOf={asOf} kind="formula"
                      what={`${h.name ?? 'This holding'}'s share of the book TODAY.`}
                      note="Huidige waarde ÷ the book’s total Huidige waarde"
                      how={'AIRS’s own current valuation as a share of the book — what the '
                        + 'Allocation bars are drawn from, and the right answer to “how much of my '
                        + 'money is in this now”. ⚠ It is NOT the weight to multiply the Return '
                        + 'by: it already contains the return (a winner has grown into a larger '
                        + 'share), so the product double-counts. Use the Start weight for that.'} />
                  </td>
                  {/* An unpriced position shows a dash, never 0% — "we could not price this over
                      the window" and "it did not move" are different facts and a 0 states the
                      wrong one. An interpolated opening mark is flagged per value, because that
                      one IS a property of the number rather than of the column. */}
                  <td className={`py-1.5 pr-4 text-right font-mono tabular-nums whitespace-nowrap ${retTone(h.own_return_pct)}`}>
                    {fmtRet(h.own_return_pct)}
                    {/* ⚠ THE FALLBACK MARKS ITSELF. Most rows are AIRS's own number now, so a
                        yfinance one sitting silently among them is the pair a reader cannot
                        arbitrate — same rule as the ≈ beside an interpolated mark. */}
                    {h.own_return_pct != null && h.own_return_source === 'yfinance' && (
                      <span className="ml-1 text-fg-faint"
                        title="yfinance, not AIRS — no AIRS book values this row (its wrapped model has no paired account, or it has no opening value anywhere), so there is no book figure to show.">ƒ</span>
                    )}
                    {/* ⚠ AND SO DOES THE OTHER BOOK. Two AIRS figures in one column measured on
                        two different books is the same unarbitrable pair as two vendors — and
                        here the gap is large, because AIRS's Beginwaarde is the year-open value OR
                        the PURCHASE value for a position opened during the year. MasterCard is
                        +2.14% in this book and +17.62% in StarTopSelectie's. The `Via` column
                        cannot stand in for this marker: a row can be reached through a certificate
                        AND still be valued here (MasterCard is 96.2% held outright), so Via is
                        present on rows whose figure is this book's own. */}
                    {/* ⚠ NO MARKER FOR "ANOTHER BOOK VALUED THIS", AND NONE FOR A BLEND. The Via
                        column already names the strategy on the same row and sizes it, so a glyph
                        here would be a second, vaguer telling of something already on screen — and
                        the ⓘ carries the arithmetic. The ƒ above stays, because "no AIRS book
                        values this at all" is NOT visible anywhere else on the row. */}
                    {h.own_return_estimated && (
                      <span className="ml-1 text-warn-400" title="Opening price interpolated — no close near the window's start">≈</span>
                    )}
                    {/* ⚠ THE CARD'S *SOURCE* CHANGES PER ROW, WHICH IS THE WHOLE REASON IT IS ON
                        THE CELL. Most rows are AIRS's own valuation; a row inside a certificate
                        (or bought mid-window) has no AIRS return of its own and is priced off our
                        yfinance series instead. Same column, same font, two vendors — a header
                        card could only state one of them, and would be wrong for the other. */}
                    <Provenance
                      source={h.own_return_source === 'yfinance' ? 'yfinance' : 'airs_volk'}
                      /* ⚠ THE ROW'S OWN DATE, NOT THE TABLE'S. An AIRS row is as-of the book
                         snapshot; a look-through row is as-of that instrument's last close,
                         which is a different date and can trail it by weeks. One clock for both
                         would be wrong for one of them, every time. */
                      asOf={h.own_return_as_of ?? (h.own_return_source === 'yfinance' ? undefined : asOf)}
                      kind="formula"
                      what={h.own_return_pct == null
                        ? `${h.name ?? 'This holding'} could not be priced over this window.`
                        : `What ${h.name ?? 'this holding'} returned since ${h.own_return_from ?? 'the window opened'}, in EUR.`}
                      note={h.own_return_pct == null
                        ? 'no valuation at one end of the window — a dash, never a 0%, because “could not price” and “did not move” are different facts'
                        : h.own_return_source === 'yfinance'
                          ? 'our own EUR close series — no AIRS book values this row'
                          : blendHow(h)
                            ? `${blendLegs(h).length} routes in, each valued by the book that holds it, weighted by opening value`
                            /* The division in the valuing book's own euros — same line whether
                               that is this book or the one behind a certificate, so the two read
                               as one measure taken twice rather than two different measures. */
                            : `${h.own_return_book && h.own_return_book !== bookName
                              ? `${h.own_return_book}: ` : ''}${bookMath(h)
                              ?? (h.own_income_eur
                                ? `(Huidige waarde + ${eur0(h.own_income_eur)} net dividend) ÷ Beginwaarde − 1`
                                : 'Huidige waarde ÷ Beginwaarde − 1')}`}
                      how={blendHow(h)
                        ? (`${blendHow(h)} — every route in, valued by the book that holds it. `
                          + '⚠ Those percentages are shares of THIS POSITION’s opening value, not '
                          + 'of the book: the book shares are the Via column and add up to the '
                          + 'Weight. Opening value, because a leg that rose carries a bigger share '
                          + 'of the position today than it held while it was rising. ⚠ Two books '
                          + 'can differ sharply on one instrument — AIRS’s Beginwaarde is the '
                          + 'year-open value, or the PURCHASE value for a position opened during '
                          + 'the year.')
                        : h.own_return_source === 'yfinance'
                        ? ('No AIRS book values this row — the certificate it sits in wraps a '
                          + 'strategy with no paired account, or it has no opening value anywhere. '
                          + 'AIRS values the WRAPPER, not what is inside it, and splitting the '
                          + 'certificate’s value change across its holdings would hand every one '
                          + 'of them the wrapper’s number (NVIDIA once read +0.08% against its own '
                          + '+2.82%). So it is priced off the instrument’s own EUR series, at the '
                          + 'same anchor, with the same split adjustment and per-date FX.')
                        : h.own_return_book && h.own_return_book !== bookName
                          ? (`Held inside a certificate wrapping ${h.own_return_book}, so that book `
                            + `holds the shares and values them: ${bookMath(h) ?? 'Huidige waarde ÷ Beginwaarde − 1'}`
                            + `. Expand ${h.own_return_book} on the portfolios list for the same `
                            + 'number. ⚠ A POSITION result, so it carries that book’s purchase date '
                            + '— Beginwaarde is the year-open value, or the PURCHASE value if the '
                            + 'position was opened during the year, so two books can read '
                            + 'differently on one instrument and both be right.')
                          : ('AIRS’s own valuation at both ends of the window, plus any dividend it '
                            + 'paid, net of withholding. This is the IDENTICAL number the Return '
                            + 'column shows when you expand this portfolio on the portfolios list — '
                            + 'same formula, same income journal, so the two cannot disagree.')} />
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

/** WHERE the position came from — and, when there is more than one way in, HOW MUCH came each way.
 *
 *  ⚠ THE PERCENTAGES APPEAR ONLY ON A SPLIT ROW, AND THAT IS THE POINT. Names alone cannot tell a
 *  position held entirely through a certificate from one that is 96% the book's own shares:
 *  MasterCard is €50,489 held outright against €1,991 through Star, and chipped only "Star" it
 *  read as a holding the book does not own. On a single-route row the split would restate the
 *  Weight column beside it, so it is left off — 49 of this book's 52 rows say just "direct".
 *
 *  The percentages are shares of the BOOK and add up to that row's Weight. The share of the ROW
 *  ("3.8% of this position") answers a different question and ties to nothing else on screen, so
 *  it rides in the tooltip. */
function ViaChips({ names, sources }: { names: string[]; sources?: BookHolding['sources'] }) {
  const routes = sources ?? [];
  const rowTotal = routes.reduce((s, r) => s + (r.weight_now_pct ?? 0), 0);
  const title = routes.length
    ? routes.map((r) => `${r.label ?? 'Held directly'}: ${r.weight_now_pct.toFixed(2)}% of the book`
      + ` · €${Math.round(r.value_eur).toLocaleString('en-US')}`
      + (rowTotal > 0 ? ` · ${(100 * r.weight_now_pct / rowTotal).toFixed(1)}% of this position` : ''))
      .join('\n')
    : names.join(' · ');

  // More than one route in: name each and size it.
  if (routes.length > 1) {
    return (
      <span className="flex flex-wrap items-center gap-1" title={title}>
        {routes.map((r) => (
          <span key={r.label ?? '__direct'}
            className={`px-1.5 py-0.5 rounded-md text-[10px] whitespace-nowrap flex items-baseline gap-1 ${
              r.label ? 'bg-accent-500/10 text-accent-400' : 'bg-overlay/5 text-fg-muted'}`}>
            <span className="max-w-[9rem] truncate">{r.label ?? 'direct'}</span>
            <span className="font-mono opacity-80">{num2(r.weight_now_pct)}%</span>
          </span>
        ))}
      </span>
    );
  }

  if (!names.length) return <span className="text-[10px] text-fg-faint" title={title || undefined}>direct</span>;
  const shown = names.slice(0, 2);
  return (
    <span className="flex flex-wrap items-center gap-1" title={title}>
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
        // ⚠ WHERE THE WAIT WENT. `apiFetch` already logs the round-trip total, but this endpoint
        // is one request covering eight different loads — so a 5-second "Loading composition…"
        // told you only that it was slow, never which load. The server now reports per phase and
        // this prints it, the same way the AIRS expand has always done.
        const t = (b as ModelPortfolioAnalysis)?.timings_ms;
        if (t && Object.keys(t).length) {
          const total = Object.values(t).reduce((s, v) => s + (v ?? 0), 0);
          trace('analyse', `server phases (ms) — ${total} total`, t);
        }
        setData(b as ModelPortfolioAnalysis);
      } catch (e) {
        traceError('analyse', 'the composition could not be loaded', e);
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
                className="cursor-pointer bg-page border border-neutral-700 rounded-lg px-2 py-1 text-[11px] font-mono text-fg focus:border-accent-500 w-[6.5rem]">
                {BENCHMARKS.map((b) => <option key={b} value={b}>{b}</option>)}
              </select>
            </label>
            <button type="button" onClick={onClose}
              className="cursor-pointer text-xs px-3 py-1.5 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 hover:border-accent-500/50 transition-colors">
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
            {/* Top: the allocation bars (the class selector) beside — when NOTHING is selected —
                the whole-portfolio return / vs-benchmark / excess scorecard, or, when a class IS
                selected, ONLY that class's own return (+ Attribution for Stocks). LEFT-aligned so
                the bars hold their position and only the right-hand content changes with the
                selection. Empty for an ad-hoc basket. */}
            <div className="flex items-center justify-start gap-8 flex-wrap mb-4 pl-8 lg:pl-20">
              {data.allocation && data.allocation.length > 0 && (
                <AllocationBars slices={data.allocation} selected={assetFilter}
                  variant={data.variant} bands={data.bands}
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
                        className={`cursor-pointer rounded-lg border px-4 flex items-center justify-center text-xs font-medium transition-colors ${
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
                note={data.book_note} bookName={data.book_portefeuille}
                /* ⚠ THE BOOK SNAPSHOT, NOT `data.as_of`. That field is the model COMPOSITION's
                   effective date (2025-12-30 for AITopSelectie) — a true fact about the weights
                   the model declares, and the wrong clock for figures the BOOK values, which are
                   as-of 2026-08-01. Stamped with it, the modal called the row's own +111.74%
                   216 days old while the row called it 2. */
                asOf={data.holdings_as_of ?? data.as_of} />
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
                      unpricedPct={a.unpriced_pct} excluded={a.excluded}
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
