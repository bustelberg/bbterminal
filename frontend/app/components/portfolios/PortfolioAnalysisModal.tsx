'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { formatPct, visibleBuckets } from './composition';
import { allocColor, bucketLabel, CASH_BUCKET, EQUITY_BUCKET } from './allocationColors';
import { classWeightedReturn } from './classReturn';
import { Provenance } from '../../../lib/provenance';
import { trace, traceError } from '../../../lib/debugTrace';
import type { ModelPortfolioAnalysis } from '../../../lib/types/api';
import AttributionPanel from './AttributionPanel';
import BucketDetailPanel from './BucketDetailPanel';
import CompositionDataModal from './CompositionDataModal';
import OwnerEarningsModal from './OwnerEarningsModal';
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

/** ⚠ DERIVED FROM THE PAYLOAD, NOT HAND-WRITTEN. It used to be its own literal — `{bucket, pct,
 *  return_pct, holdings}` — which silently went stale the moment the server grew a field: adding
 *  `contribution_pct` compiled fine on the backend and failed here with "does not exist on type
 *  AllocSlice", which is the good outcome; the bad one is a field that exists on both sides and
 *  quietly is not the same shape. Same pattern `BookHolding` below already uses. */
type AllocSlice = NonNullable<ModelPortfolioAnalysis['allocation']>[number];
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

function AllocationBars({ slices, selected, onSelect, variant, bands, soldContribution }: {
  slices: AllocSlice[];
  /** ⚠ The year's contribution from positions SOLD OUT during it. They have no asset class, so no
   *  bar can carry them — without this the slices are a set of parts that misses its own total. */
  soldContribution?: number | null;
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
      {/* ⚠ A SECOND THING IS ON THE CHART, SO IT IS NAMED. The bar is what the portfolio holds;
          the three stripes are what the policy permits. Without this line they read as gridlines
          and the target tick as noise. Only rendered when there IS a policy — the products with
          no risk profile draw no stripes and get no legend for them. */}
      {bandOf.size > 0 && (
        <div className="flex items-center gap-3.5 text-[10px] text-fg-faint mb-1.5">
          {/* ⚠ ONE ENTRY PER STRIPE, AND EACH SWATCH IS THAT STRIPE AT ROW SCALE — a legend drawn
              differently from the thing it names is one more thing to map.

              ⚠ MINIMAL AND MAXIMAL SHARE A SWATCH ON PURPOSE. They are the same kind of mark, told
              apart by POSITION (left bound, right bound). Neither ever changes colour — see the
              bounds themselves below.

              ⚠ THERE IS NO BREACH ENTRY (there was one: "outside the band"). Nothing on the chart
              is amber any more; a weight outside its band is reported on the row's percentage,
              which goes amber with the crossed bound named in its tooltip. */}
          <span className="flex items-center gap-1.5">
            <span className="inline-block w-0.5 h-3 bg-neutral-500/70" />
            minimal
          </span>
          <span className="flex items-center gap-1.5">
            <span className="inline-block w-[3px] h-3 rounded-sm bg-neutral-800/85" />
            target
          </span>
          <span className="flex items-center gap-1.5">
            <span className="inline-block w-0.5 h-3 bg-neutral-500/70" />
            maximal
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
                  THE RANGE. The policy marks run the FULL height of the track while the bar is a
                  slimmer ribbon down the middle, so a stripe stays visible straight THROUGH the
                  bar — nothing is hidden and nothing is washed over. Two earlier versions are
                  worth not repeating: a 6% overlay ON TOP of the bar, which darkened the class
                  colour where they met and vanished where they didn't (two readings of one
                  annotation), and a translucent min→max block behind it, which was a second grey
                  wash under every row for a span the two outer stripes already delimit. */}
              {/* ⚠ THE TRACK GROWS WITH THE BAR, IN PROPORTION. The measure is inset from this
                  height, so thickening the ribbon alone would eat the margin the band's top and
                  bottom edges live in and break the bullet-chart rule above it. 36 / inset-10
                  keeps the ribbon at exactly half the track, as 18 / inset-5 did. */}
              <span className="relative h-[36px] flex-1 rounded bg-inset overflow-hidden">
                {/* Recessive gridlines at the axis's own ticks. The ends are the track's own
                    edges, so only the interior ticks are drawn. */}
                {AXIS_TICKS.filter((t) => t > 0 && t < 100).map((t) => (
                  <span key={t} className="absolute inset-y-0 w-px bg-neutral-700/30"
                    style={{ left: `${t}%` }} />
                ))}
                {/* ⚠ THE POLICY IS THREE STRIPES — min, target, max — AND NO FILL. The band used to
                    be drawn as a translucent block spanning min→max with caps at its edges; at one
                    stripe per bound the same information reads at a glance and nothing is shaded
                    over the class colour. What the fill added was the SPAN as an area, which the
                    two outer stripes already delimit; what it cost was a second grey wash behind
                    every bar. (The target stripe is drawn after the measure, further down, because
                    it has to cross it.) */}
                {/* ⚠ THE BOUNDS ARE ALWAYS GREY — they never recolour on a breach. A limit is a
                    fixed property of the policy; it does not change because today's weight sits
                    the wrong side of it. Tinting it amber made the CHART report the exception
                    twice (the bar visibly ends past the stripe already) and made the mark look
                    like a different mark. The breach is said where a fact about the holding
                    belongs: the row's percentage, in amber, with the bound named in its tooltip. */}
                {(() => {
                  const b = bandOf.get(s.bucket);
                  if (!b) return null;
                  return (
                    <>
                      {b.min_pct != null && (
                        <span className="absolute inset-y-0 w-0.5 pointer-events-none bg-neutral-500/70"
                          style={{ left: `${b.min_pct}%` }} />
                      )}
                      {b.max_pct != null && (
                        <span className="absolute inset-y-0 w-0.5 pointer-events-none bg-neutral-500/70"
                          style={{ left: `calc(${b.max_pct}% - 2px)` }} />
                      )}
                    </>
                  );
                })()}
                {/* The measure: a slim ribbon, centred, so the stripes read above and below it. */}
                <span className="absolute inset-y-[10px] left-0 rounded-sm"
                  style={{ width: `${Math.min(100, s.pct)}%`, minWidth: 3,
                    background: allocColor(s.bucket) }} />
                {/* The target — the third stripe, and LAST so it crosses the bar: a target hidden
                    under the measure is the one comparison this chart exists to make. Darker and a
                    pixel wider than the two bounds, so the three are never confused. */}
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
              {/* ⚠ POINTS, NOT PERCENT, AND THE FIGURE CHANGED WITH THE UNIT. This showed the
                  class's RETURN (its Result over its own opening value) — a rate, which cannot
                  wear "pp" because pp means points OF something. Relabelling alone would have been
                  a lie; the number is now the class's CONTRIBUTION, on the book's own opening
                  capital, which is the thing that legitimately adds. Its own return is still in
                  the Holdings table below, in the column labelled Return. */}
              <span className={`w-16 shrink-0 text-right font-mono text-[11px] tabular-nums ${retTone(s.contribution_pct)}`}
                title={`${bucketLabel(s.bucket)} added ${ppt(s.contribution_pct)} to the book’s year.`
                  + ` Its own return was ${fmtRet(s.return_pct)} — a rate on its own opening value,`
                  + ' which is why the two differ and why only this one adds up.'}>
                {ppt(s.contribution_pct)}
              </span>
            </Row>
          );
        })}
        {/* ⚠⚠ THE PART NO BAR CAN CARRY. A position sold out during the year has no asset class —
            no sector, no ISIN, no current weight — so it appears in no slice above. Without this
            line the bars are a set of parts that silently misses its total: measured on
            BUS_Offensief_Dyn they come to +8.211pp against a book that made +5.827%, and the
            missing -2.384pp is eight names it no longer holds. Printed only when there IS a
            remainder, so a book that sold nothing does not carry a permanent 0.00pp footnote. */}
        {soldContribution != null && Math.abs(soldContribution) >= 0.005 && (
          <div className="flex items-center gap-2 pt-1 mt-1 border-t border-neutral-800/40 text-[10px]">
            <span className="text-fg-faint">Sold during the year — no longer a holding</span>
            <span className={`ml-auto font-mono tabular-nums ${retTone(soldContribution)}`}>
              {ppt(soldContribution)}
            </span>
          </div>
        )}
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

/** HOW MUCH OF THE YEAR THE WEIGHT-BASED VIEWS CANNOT SEE — the Sector / Region / Currency bars
 *  and the Brinson table beneath them.
 *
 *  ⚠ SHOWN ONLY WHEN IT IS MATERIAL (≥1% of the year's movement). A permanent banner on every
 *  portfolio is furniture; a reader stops reading it, and it is then worth nothing on the book
 *  where it matters. Below 1pp the omission cannot change a sector's verdict.
 *
 *  ⚠ THE SHARE IS OF THE ABSOLUTE movement, not the net. A realised −28,656 against a held
 *  +75,164 is not "negative coverage" — the question is how much happened outside these charts,
 *  and a loss counts as much as a gain. */
function RealisedCoverageNote({ r }: { r?: ModelPortfolioAnalysis['realised'] }) {
  const share = r?.available ? r.realised_share_of_result_pct : null;
  if (share == null || share < 1) return null;
  return (
    <p className="text-[11px] text-warn-500 mb-3">
      ⚠ <span className="font-mono">{share.toFixed(0)}%</span>
      {' of this book’s year happened in positions it has since SOLD, and these charts cannot show '
        + 'them: they are weighted by what each position was worth when the year opened, and a sold '
        + 'position’s opening weight cannot be recovered from AIRS’s data. The bars and the '
        + 'attribution below therefore describe only what is still held — a sector traded out of '
        + 'entirely reads as one that was never owned. The sold names and what each contributed are '
        + 'itemised under Holdings.'}
    </p>
  );
}

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
 *  +2.82%).
 *
 *  ⚠ THE CLASS ROW AGGREGATES THAT COLUMN, WEIGHTED AT THE WINDOW'S OPEN (added 2026-08-05, on
 *  request) — `classReturn.ts`. It is emphatically NOT weighted by the `Weight (now)` column
 *  beside it: that share already contains the return, so the product hands a winner a share of
 *  the class it never held (measured elsewhere on this data at +58.75% against a true +44.99%).
 *  The weight is `weight_pct`, the opening-value share, which is also what SleeveBreakdown's
 *  Contribution column renormalises — so the class figure IS the sum of that column and the two
 *  views cannot show one class two returns.
 *
 *  ⚠ IT NEED NOT EQUAL THE BOOK'S CLASS RETURN IN THE ALLOCATION LEGEND, and that is the honest
 *  outcome rather than a defect. The legend's figure is the book's own value change, which for a
 *  looked-through position is the CERTIFICATE's; this one is built from the instrument returns
 *  actually printed underneath it. Two measures, and each is shown where its own rows are — the
 *  card on the cell says which one this is.
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
 *  ⚠ AND IT REPORTS WHAT IT COULD NOT WEIGH. An unpriceable leg leaves both the numerator and the
 *  denominator, so the class reads as though that weight behaved exactly like the rest — the same
 *  silent renormalisation the coverage floors elsewhere exist to stop. `coveredPct` is on the
 *  cell's card and short coverage is marked in amber, never absorbed. */
type HoldingSortKey = 'name' | 'sector' | 'weight' | 'return' | 'contribution';

/**
 * THE MONEY COLUMNS, GROUPED BY THE ANSWER THEY BUILD UP TO.
 *
 * ⚠ A COLUMN ON ITS OWN IS A NUMBER; A GROUP IS AN ARGUMENT. Every figure this table derives is
 * the end of a short chain, and a reader who wants to check one wants the whole chain, not one
 * cell of it. Picking columns individually meant assembling that chain by hand and getting it
 * wrong — turning on Return's denominator without its numerator, say.
 *
 *     Return             Result ÷ Beginwaarde
 *     On money invested  Result ÷ Avg capital invested
 *     Contribution       Result ÷ the book's opening capital
 *
 * ⚠ ALL THREE SHARE `Result`, WHICH IS WHY SELECTION IS STORED AS GROUPS AND THE COLUMNS ARE
 * DERIVED AS THEIR UNION. Storing columns instead would mean deciding what happens to `Result`
 * when one of two groups that both need it is switched off — a question with no good answer, and
 * one this shape never has to ask.
 *
 * ⚠ NOTHING IS ON BY DEFAULT. The table opens at six columns, which fits a screen.
 */
const COLUMN_GROUPS = [
  {
    key: 'return',
    label: 'How the Return is built',
    hint: '(Value now − Beginwaarde) + Realised + Income = Result, ÷ Beginwaarde',
    // ⚠ `Return` itself is NOT here: it is always on. This group supplies the chain BEHIND it, so
    // ticking it puts the whole derivation on screen beside the answer already showing.
    cols: ['opening', 'valuenow', 'unrealised', 'realised', 'income', 'result'],
  },
  {
    key: 'onmoney',
    label: 'How On money invested is built',
    hint: 'Result ÷ Avg capital invested',
    cols: ['result', 'avgcapital', 'moneyweighted'],
  },
  {
    key: 'contribution',
    label: 'How the Contribution is built',
    hint: 'Result ÷ the book’s opening capital',
    cols: ['result', 'contribution'],
  },
] as const;

type ColumnGroup = (typeof COLUMN_GROUPS)[number]['key'];
type MoneyCol = (typeof COLUMN_GROUPS)[number]['cols'][number];
const GROUPS_KEY = 'bb.analyse.holdings.columnGroups';

/**
 * The saved choice, or nothing.
 *
 * ⚠ READ DURING THE FIRST RENDER, WHICH IS SAFE *HERE* AND NOT IN GENERAL. Touching
 * `localStorage` in a lazy initialiser is the classic hydration bug — the server renders one
 * thing and the client another. It cannot happen in this table: it is rendered from
 * `{data && …}` after a client-side fetch inside a modal the user opened, so the server never
 * produces it and there is no first paint to mismatch. An effect instead would mean a setState
 * inside an effect, which is the cascading render the lint rule objects to.
 *
 * ⚠ THE KEY CHANGED WITH THE SHAPE. It used to store COLUMN keys; storing group keys under the
 * same name would read an old list as a set of unknown groups. A new key lets the old value be
 * ignored rather than misread — the reader's choice resets once, which is the cheap failure.
 */
function readSavedGroups(): Set<ColumnGroup> {
  if (typeof window === 'undefined') return new Set();
  try {
    const raw = localStorage.getItem(GROUPS_KEY);
    if (!raw) return new Set();
    const saved = JSON.parse(raw) as unknown;
    if (!Array.isArray(saved)) return new Set();
    const valid = COLUMN_GROUPS.map((g) => g.key) as readonly string[];
    return new Set(saved.filter((k): k is ColumnGroup => typeof k === 'string' && valid.includes(k)));
  } catch (e) {
    console.warn('[analyse] could not read the saved column choice', e);
    return new Set();
  }
}

/** Which groups are shown, and the columns that follow from them. */
function useColumnGroups() {
  const [groups, setGroups] = useState<Set<ColumnGroup>>(readSavedGroups);
  const toggle = (k: ColumnGroup) => setGroups((prev) => {
    const next = new Set(prev);
    if (next.has(k)) next.delete(k); else next.add(k);
    try { localStorage.setItem(GROUPS_KEY, JSON.stringify([...next])); } catch { /* private mode */ }
    return next;
  });
  // ⚠ THE UNION, RECOMPUTED — never stored. A column belongs to as many groups as need it, and
  // `Result` belongs to all three; deriving means switching one group off can never take a column
  // another group still depends on.
  const cols = new Set<MoneyCol>(
    COLUMN_GROUPS.filter((g) => groups.has(g.key)).flatMap((g) => g.cols as readonly MoneyCol[]));
  return { groups, toggle, cols };
}

/** The +/− control over those groups. ⚠ Closed by a full-screen click catcher rather than a
 *  document listener: this lives inside a modal that already stops propagation in places, and a
 *  listener the modal swallows leaves a panel nothing can dismiss. */
function ColumnPicker({ groups, toggle }: {
  groups: Set<ColumnGroup>; toggle: (k: ColumnGroup) => void;
}) {
  const [open, setOpen] = useState(false);
  return (
    <span className="relative inline-flex">
      <button type="button" onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        title="Show the columns behind each figure"
        className={`cursor-pointer text-[10px] leading-none px-1.5 py-1 rounded border transition-colors ${
          open ? 'border-accent-500/50 text-accent-300 bg-overlay/5'
            : 'border-neutral-800/40 text-fg-subtle hover:text-accent-300 hover:bg-overlay/5'}`}>
        + columns
      </button>
      {open && (
        <>
          <span className="fixed inset-0 z-30" onClick={() => setOpen(false)} />
          <span className="absolute right-0 top-full mt-1 z-40 w-[20rem] rounded-lg border border-neutral-800/40 bg-popover shadow-xl p-1.5 flex flex-col gap-0.5">
            {COLUMN_GROUPS.map((g) => (
              <label key={g.key}
                className="flex items-start gap-2 px-1.5 py-1.5 rounded hover:bg-overlay/5 cursor-pointer">
                <input type="checkbox" checked={groups.has(g.key)} onChange={() => toggle(g.key)}
                  className="accent-accent-600 mt-0.5" />
                <span className="flex flex-col gap-0.5 min-w-0">
                  <span className="text-[11px] text-fg-soft">{g.label}</span>
                  {/* ⚠ THE CHAIN ITSELF, not a description of it. It is what the reader is about
                      to put on screen, and it says in one line why these columns come together. */}
                  <span className="text-[10px] font-mono text-fg-faint">{g.hint}</span>
                </span>
              </label>
            ))}
          </span>
        </>
      )}
    </span>
  );
}

/** The four euro/point columns, summed. ⚠ A SUM OF NULLS IS NULL, NOT ZERO: a class in which
 *  nothing could be valued has an undefined result, and a €0 subtotal would say it broke even. */
function sumResults(rows: BookHolding[]) {
  const add = (pick: (h: BookHolding) => number | null | undefined) => {
    const vals = rows.map(pick).filter((v): v is number => v != null);
    return vals.length ? vals.reduce((s, v) => s + v, 0) : null;
  };
  // ⚠ NUMERATOR AND DENOMINATOR OVER THE SAME ROWS. Only the rows that HAVE an average invested
  // capital may contribute their result to this ratio — summing every row's result over the
  // capital of some of them would divide one population by another and overstate by whatever the
  // excluded rows made. On a book with certificates that is 22 of 52 rows, so it is not a rounding
  // concern. Null when nothing in the group has flows we can see.
  const priced = rows.filter((h) => (h.avg_capital_eur ?? 0) > 0);
  const cap = priced.reduce((s, h) => s + h.avg_capital_eur!, 0);
  return {
    opening: add((h) => h.start_value_eur),
    valuenow: add((h) => h.current_value_eur),
    // ⚠ Over the rows that HAVE one — a leg inside a certificate has no flows, so it contributes
    // nothing here and its result is correspondingly excluded from `mwr` below.
    avgcapital: cap || null,
    unrealised: add((h) => h.unrealised_eur),
    realised: add((h) => h.realised_result_eur),
    income: add((h) => h.income_eur),
    result: add((h) => h.result_eur),
    contribution: add((h) => h.contribution_pct),
    mwr: cap > 0 ? priced.reduce((s, h) => s + (h.result_eur ?? 0), 0) / cap * 100 : null,
    /** How much of the group this ratio speaks for — the rest is legs inside certificates, which
     *  have no flows of their own. */
    mwrRows: priced.length,
  };
}

/** ⚠ NULL, NOT ZERO, WHEN THERE IS NOTHING TO ADD — the same rule as `sumResults`. */
const sum = (vals: (number | null | undefined)[]) => {
  const v = vals.filter((x): x is number => x != null);
  return v.length ? v.reduce((s, x) => s + x, 0) : null;
};
/** Two subtotals, either of which may be "nothing to add". ⚠ `null + 5` must be 5, not null: a
 *  book with no sold positions still has a grand total. */
const add2 = (a: number | null, b: number | null) =>
  (a == null && b == null ? null : (a ?? 0) + (b ?? 0));

/** Whole euros with a sign, or a dash. ⚠ Whole euros because these are result columns read across
 *  a 52-row table — cents there are noise that costs column width and buys nothing. */
const eur0n = (v?: number | null) =>
  (v == null ? '—'
    : `${v < 0 ? '−' : ''}€${Math.abs(v).toLocaleString('en-US', { maximumFractionDigits: 0 })}`);

/** ⚠ pp, not %. A share OF the book's return; "+2.87%" beside the book's "+5.83%" reads as a
 *  second, rival return rather than as a part of it. */
const ppt = (v?: number | null) =>
  (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}pp`);

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

/** A Fundamental trigger — the same control the /portfolios table carries, for one instrument or
 *  for a whole class as a value-weighted basket.
 *
 *  ⚠ ONLY WHERE THERE IS SOMETHING TO LOOK UP. Owner earnings are per-COMPANY: cash has no ISIN,
 *  an unresolved line has none either, and a class whose members are all unresolved yields an
 *  empty basket. A button that opens a modal saying "nothing to show" is worse than no button —
 *  it reads as a broken feature rather than an absent one. */
function FundamentalButton({ onOpen, title, className = '' }: {
  onOpen: () => void; title: string; className?: string;
}) {
  return (
    <button type="button"
      onClick={(e) => { e.stopPropagation(); onOpen(); }}
      title={title}
      className={`cursor-pointer text-[10px] px-1.5 py-0.5 rounded border border-neutral-800/40 text-fg-subtle hover:bg-overlay/5 hover:text-accent-300 whitespace-nowrap transition-colors ${className}`}>
      Fundamental
    </button>
  );
}

function PortfolioHoldings({ holdings, slices, asOf, note, bookName, realised, onFundamental }: {
  holdings: BookHolding[]; slices?: AllocSlice[]; asOf?: string | null;
  /** ⚠ THE POSITIONS THAT NO LONGER HAVE A ROW — sold out entirely during the year. They are the
   *  reason this table could not add up before: measured, 8 names and −2.38pp of one book's year,
   *  invisible because a closed position has nothing left to list. Rendered as their own group,
   *  because they have no asset class, no ISIN and no current weight — only a result. */
  realised?: ModelPortfolioAnalysis['realised'];
  /** Opens the owner-earnings modal for one instrument or a whole class. */
  onFundamental: (t: { name: string; isin?: string; basket?: Basket }) => void;
  /** WHY the table is empty, from the server (`book_note`) — three different faults used to
   *  render as one sentence, next to a portfolios list that visibly has rows. */
  note?: string | null;
  /** THIS book's own account name. A Return whose `own_return_book` differs came from the book
   *  behind a certificate, and this is the only thing that tells the two apart. */
  bookName?: string | null;
}) {
  const [sortKey, setSortKey] = useState<HoldingSortKey>('weight');
  const [dir, setDir] = useState<'asc' | 'desc'>('desc');
  // ⚠ ONE PREDICATE, USED IN ALL SIX ROW SHAPES (thead, class row, holding row, sold header, sold
  // row, total). The file already warns that the column count is counted by hand in several
  // places; making them CONDITIONAL multiplies that risk, so every gate is a bare `show(<key>)`
  // and nothing else — the same NINE keys must appear in every shape, or a figure renders under
  // the wrong heading. Two invariants worth re-checking after any edit here, both mechanical:
  //   * each shape gates the same nine keys and carries six always-on cells;
  //   * every key gated below is reachable from at least one entry in `COLUMN_GROUPS`, or the
  //     column exists and nothing can ever switch it on.
  const { groups: pickedGroups, toggle, cols } = useColumnGroups();
  const show = (k: MoneyCol) => cols.has(k);

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
    .map((bucket) => {
      const rows = holdings.filter((h) => h.bucket === bucket);
      return {
        bucket,
        slice: (slices ?? []).find((s) => s.bucket === bucket),
        rows,
        // The class's own return: the Return column below, weighted by what each position was
        // worth when the window OPENED. Never by the Weight (now) column — see `classReturn.ts`.
        // ⚠ Cash has no `Beginwaarde` to divide by, and its return is nonetheless known exactly:
        // zero. See the flag's own note — a dash there says "unknown" about the one asset whose
        // return is certain, and hides its drag.
        ret: classWeightedReturn(rows, bucket === CASH_BUCKET),
        // ⚠ PLAIN SUMS, and they are allowed to be plain BECAUSE they are euros. A euro column
        // adds; that is the whole reason the result breakdown is in euros and the weight-based
        // arguments elsewhere in this file do not apply to it.
        sum: sumResults(rows),
        // The class as a value-weighted basket, for the Fundamental button on its header. ISIN-
        // bearing rows only: owner earnings are per-company, and cash has no company.
        basket: {
          label: bucketLabel(bucket),
          holdings: rows
            .filter((h) => h.isin)
            .map((h) => ({ isin: h.isin!, weight: h.weight_now_pct ?? 0, name: h.name ?? undefined })),
        } satisfies Basket,
      };
    })
    .filter((g) => g.rows.length > 0);

  // ⚠ ONLY THE CLOSED-OUT ONES. A name that was TRIMMED still has a holdings row, and its realised
  // result is already grafted onto that row — listing it here too would count it twice and the
  // total would stop tying. Measured on BUS_Offensief: of 13 traded names, 5 are trims (on their
  // own rows) and 8 are gone (here). Every orphan being closed-out is what makes the split exact.
  const sold = (realised?.available ? realised.positions ?? [] : []).filter((p) => p.closed_out);
  const soldCap = sum(sold.map((p) => p.avg_capital_eur));
  const soldSum = {
    // ⚠ `opening_eur`, NOT a Beginwaarde — a sold-out position has no holdings row and therefore
    // no restated opening value. This is its value at the year's open reconstructed from the sale
    // (`proceeds − Res. YtD`, scaled to the shares actually held then). Same quantity in spirit,
    // different provenance, which is why the ⓘ on the column says so.
    opening: sum(sold.map((p) => p.opening_eur)),
    // ⚠ No "value now": it is gone. A 0 there would read as a holding that fell to nothing.
    realised: sum(sold.map((p) => p.realised_result_eur)),
    income: sum(sold.map((p) => p.income_eur)),
    result: sum(sold.map((p) => p.result_eur)),
    contribution: sum(sold.map((p) => p.contribution_pct)),
    // A closed-out position DOES have an average invested capital — it was held for part of the
    // year, and Modified Dietz weights it by exactly that part.
    mwr: soldCap ? (sum(sold.map((p) => p.result_eur)) ?? 0) / soldCap * 100 : null,
  };
  const heldSum = sumResults(holdings);
  const heldCap = holdings.reduce((s, h) => s + (h.avg_capital_eur ?? 0), 0);
  const grand = {
    opening: add2(heldSum.opening, soldSum.opening),
    valuenow: heldSum.valuenow,
    avgcapital: add2(heldSum.avgcapital, soldCap),
    unrealised: heldSum.unrealised,
    realised: add2(heldSum.realised, soldSum.realised),
    income: add2(heldSum.income, soldSum.income),
    result: add2(heldSum.result, soldSum.result),
    contribution: add2(heldSum.contribution, soldSum.contribution),
    // ⚠ Again both sides over the same rows: the held legs we can see flows for, plus the sold
    // ones. A leg inside a certificate is in NEITHER, which is why this is not the book's own
    // money-weighted return and is not labelled as one.
    mwr: (heldCap + (soldCap ?? 0)) > 0
      ? (holdings.reduce((s, h) => s + ((h.avg_capital_eur ?? 0) > 0 ? (h.result_eur ?? 0) : 0), 0)
        + (sum(sold.map((p) => p.result_eur)) ?? 0)) / (heldCap + (soldCap ?? 0)) * 100
      : null,
  };
  // Within a cent of a point. The measured case lands at exactly 0.0000pp; the tolerance is for
  // float noise across ~60 additions, not for a missing leg.
  const reconciled = grand.contribution != null && realised?.book_ytd_pct != null
    && Math.abs(grand.contribution - realised.book_ytd_pct) < 0.01;

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
      : sortKey === 'contribution' ? h.contribution_pct
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
            how={`one row per ISIN, after the certificates are looked through

${holdings.length} rows, ${holdings.filter((h) => (h.via_names ?? []).length).length} of them reached through a certificate`} />
        </h4>
        <span className="flex items-center gap-2">
          <span className="text-[10px] font-mono text-fg-faint">
            {holdings.length} positions · {groups.length} classes
          </span>
          <ColumnPicker groups={pickedGroups} toggle={toggle} />
        </span>
      </div>
      {/* ⚠⚠ `overflow-auto` + a HEIGHT, because `sticky` needs a scrollport with room to scroll.
          This was `overflow-x-auto` with a `sticky top-0` thead, and the sticky was DEAD: setting
          `overflow-x` forces `overflow-y` to `auto` as well, so this div became a scroll container
          in both axes — and `position: sticky` sticks to the NEAREST scrolling ancestor, which was
          this box, exactly as tall as its own content. The header had nothing to travel against.
          The modal body (`h-[80vh] overflow-auto`) is the real scrollport, and this wrapper stood
          between the two.
          ⚠ THE FIX IS NOT TO DROP `overflow-x`. Twelve columns are ~81rem wide and overflow the
          modal on any ordinary screen; without a horizontal container that scroll moves to the
          modal body, dragging every other section sideways with it. So the table gets its own
          viewport instead — one box that scrolls both ways, with the header pinned inside it. */}
      <div className="overflow-auto max-h-[55vh]">
        <table className="w-full text-xs">
          {/* ⚠ `[&_th]:bg-card` IS LOAD-BEARING, not belt-and-braces. A background on `<thead>`
              alone does not paint reliably under `border-collapse`, so the group rows (`bg-inset`)
              scroll THROUGH the header and the two sets of text overlap. The cells carry it. */}
          <thead className="text-[10px] uppercase tracking-wide text-fg-faint bg-card [&_th]:bg-card sticky top-0 z-20">
            <tr className="border-b border-neutral-800/40">
              <th className="text-right w-10 pl-4 pr-2 py-2 font-medium">#</th>
              {/* ⚠ A FLOOR IS REQUIRED HERE BECAUSE THE CELL BELOW IS `max-w-0`. That is what lets
                  a long instrument name truncate instead of stretching the table — but it also
                  makes Name the column an auto-layout table takes slack FROM first, and with
                  twelve columns there was none left: on a book whose Via column carries certificate
                  chips, Name collapsed to a single letter per row. `min-w` is the only thing
                  standing between "truncates gracefully" and "shows nothing". */}
              <th className={`text-left min-w-[13rem] ${th}`} onClick={() => click('name')}>Name{caret('name')}</th>
              {/* ⚠ CAPPED. The chips truncate INDIVIDUALLY (max-w-[9rem] each) but the column
                  itself had no bound, so a row with three routes in was free to demand 30rem —
                  taken straight out of Name. Bounded here, the chips wrap within the column
                  instead of eating the table. */}
              <th className="text-left w-40 max-w-[10rem] py-2 font-medium">
                Via
                <Provenance source="airs_model" asOf={asOf} kind="formula" column
                  what={'How the portfolio got into this instrument — its own shares, a strategy '
                    + 'whose certificate was looked through to reach it, or both.'}
                  how={`each route in, as a share of the whole book — they sum to that row’s Weight

${holdings.filter((h) => (h.via_names ?? []).length).length} of ${holdings.length} rows arrive through a certificate, the rest are held outright`} />
              </th>
              {/* ⚠ THE SECTOR CHART'S OWN BUCKET, WHICH IS WHY IT IS WORTH A COLUMN — sorting by
                  it lists the rows behind a bar, in the bar's own vocabulary. A raw
                  `asset_grid.sector` here would say "Financial Services" under a bar saying
                  "Financials" and read as two different exposures. */}
              <th className={`text-left w-36 ${th}`} onClick={() => click('sector')}>
                Sector{caret('sector')}
                <Provenance source="yfinance" asOf={asOf} kind="formula" column
                  what={'The sector this instrument is counted in on the Sector chart above.'}
                  how={`Yahoo’s sector for the ISIN, canonicalised so one sector has one name

${new Set(holdings.map((h) => h.sector).filter((s) => s && s !== 'Unclassified')).size} sectors across ${holdings.filter((h) => sectorLabel(h.sector)).length} rows; ${holdings.filter((h) => !sectorLabel(h.sector)).length} have none (a fund, or unclassifiable)`} />
              </th>
              <th className={`text-right w-24 ${th}`} onClick={() => click('weight')}>
                Weight (now){caret('weight')}
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what={'The share of the portfolio held in this instrument, right now.'}
                  how={`Huidige waarde ÷ the book’s total Huidige waarde

the ${holdings.length} rows below sum to ${eur0n(grand.valuenow)} = 100.00%`} />
              </th>
              {/* ⚠ THE THREE COMPONENTS, THEN THEIR SUM — the whole point of merging the ledger
                  into this table. A reader who wants to know what a position MADE should not have
                  to reconcile a return against a weight; these add up on screen.
                  ⚠ COLUMN COUNT IS ELEVEN and is counted by hand in FOUR places (this thead,
                  the group row, the body row, the total row). Add one here and forget another and
                  every figure below shifts a cell right, silently — a contribution renders
                  perfectly well under "Return". */}
{show('opening') && (
              <th className="text-right w-32 py-2 font-medium">
                Beginwaarde (1 Jan)
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what="What this position was worth at the start of the year, on AIRS’s own basis."
                  how={`Σ (quantity held today × its price on 1 January)

= ${eur0n(grand.opening)}`} />
              </th>
)}
{show('valuenow') && (
              <th className="text-right w-28 py-2 font-medium">
                Value now
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what="What this position is worth today."
                  how={`Σ (AIRS’s current valuation)

= ${eur0n(grand.valuenow)}`} />
              </th>
)}
{show('avgcapital') && (
              <th className="text-right w-32 py-2 font-medium">
                Avg capital invested
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what="The money actually tied up in this position over the year."
                  how={`Σ (value at the open + each flow × the share of the year still to run)

= ${eur0n(grand.avgcapital)}`} />
              </th>
)}
{show('unrealised') && (
              <th className="text-right w-28 py-2 font-medium">
                Unrealised
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what="What this position has gained or lost while the book has held it — on paper, not banked."
                  note="value now − value when the year opened"
                  how={`Value now − Beginwaarde

${eur0n(grand.valuenow)} − ${eur0n(grand.opening)} = ${eur0n(grand.unrealised)}`} />
              </th>
)}
{show('realised') && (
              <th className="text-right w-28 py-2 font-medium">
                Realised
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what="What was banked by actually selling — this year’s part of it."
                  note="AIRS’s Res. YtD, summed over the year’s sales"
                  how={`Σ Res. YtD over the year’s sales

= ${eur0n(grand.realised)}`} />
              </th>
)}
{show('income') && (
              <th className="text-right w-24 py-2 font-medium">
                Income
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what="The dividends and coupons this position paid the book this year."
                  note="net — gross less withholding tax"
                  how={`Σ (dividend + withholding tax)

= ${eur0n(grand.income)}`} />
              </th>
)}
{show('result') && (
              <th className="text-right w-28 py-2 font-medium">
                Result
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what="What the book actually made on this position this year, in euros."
                  note="unrealised + realised + income"
                  how={`Unrealised + Realised + Income

${eur0n(grand.unrealised)} + ${eur0n(grand.realised)} + ${eur0n(grand.income)} = ${eur0n(grand.result)}`} />
              </th>
)}
{show('contribution') && (
              <th className={`text-right w-28 ${th}`} onClick={() => click('contribution')}>
                Contribution{caret('contribution')}
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what="What this position added to, or took off, the book’s return for the year."
                  note="result ÷ the book’s opening capital"
                  how={`Result ÷ the book’s opening capital

${eur0n(grand.result)} ÷ ${eur0n(realised?.basis_eur)} = ${ppt(grand.contribution)}`} />
              </th>
)}
{show('moneyweighted') && (
              <th className="text-right w-32 py-2 font-medium">
                On money invested
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what="What this position returned on the money actually put into it."
                  note="result ÷ average invested capital"
                  how={`Result ÷ Avg capital invested

${eur0n(grand.result)} ÷ ${eur0n(grand.avgcapital)} = ${fmtRet(grand.mwr)}`} />
              </th>
)}
              <th className={`text-right w-28 pr-4 ${th}`} onClick={() => click('return')}>
                Return{caret('return')}
                {/* ⚠ `airs_volk`, NOT `yfinance`. This header claimed yfinance while the rows
                    beneath it are AIRS's own valuation — each row's card names its actual source
                    correctly, so the column header disagreed with almost every cell under it. The
                    yfinance path is the marked FALLBACK (ƒ), not the basis.
                    ⚠ AND IT DESCRIBED THE CLASS ROW'S OLD ARITHMETIC. It said the rows "average
                    into" the class figure weighted by opening value — true when the class return
                    was Σ(weight × return), and false since it became Σ result ÷ Σ opening value.
                    A header that explains a formula the table no longer uses is worse than one
                    that explains nothing. */}
                <Provenance source="airs_volk" asOf={asOf} kind="formula" column
                  what={'What this instrument itself returned in euros since '
                    + `${anchor ?? 'the year opened'} — independent of how much of it the book `
                    + 'holds.'}
                  how={`(Huidige waarde + net income) ÷ Beginwaarde − 1, per row

a row marked ƒ is priced off our own EUR series instead; the class rows below divide their own Result by their own Beginwaarde`} />
              </th>
            </tr>
          </thead>
          {groups.map((g) => {
            // ⚠ THE TERM THAT RECONCILES Contribution WITH Return, and it exists nowhere else on
            // the row. `contribution = return × this`. Null when the book has no opening capital
            // to divide by — in which case neither figure is on a footing to be explained.
            const openingShare = (realised?.basis_eur && g.ret.startEur)
              ? g.ret.startEur / realised.basis_eur * 100 : null;
            return (
            <tbody key={g.bucket}>
              <tr className="bg-inset border-y border-neutral-800/40">
                <td className="pl-4" />
                {/* colSpan 3: Name · Via · Sector — every text column, so the class label
                    runs to the first number. */}
                <td className="py-2 font-medium text-fg-strong" colSpan={3}>
                  <span className="inline-block w-2.5 h-2.5 rounded-sm mr-2 align-middle"
                    style={{ background: allocColor(g.bucket) }} />
                  {bucketLabel(g.bucket)}
                  <span className="ml-2 px-1.5 py-0.5 rounded-md bg-overlay/5 text-[10px] font-normal text-fg-muted">
                    {g.rows.length}
                  </span>
                  {/* ⚠ WEIGHTED BY WHAT IS BEING BLENDED, over the members that CAN be blended.
                      The basket takes each member's `weight_now_pct` — the same figure this row's
                      subtotal is summed from — and only the rows with an ISIN, because owner
                      earnings are per-company and cash has none. Sending the whole class would
                      hand the blender a weight it cannot attribute to anything.
                      ⚠⚠ AND ONLY ON STOCKS. An ISIN is not enough: an ETF has one and is not a
                      company (this app deliberately does not look through funds, so there is
                      nothing behind it to measure), Alternatives is crypto and commodities with no
                      earnings at all, and a bond is a claim on a company rather than a share of
                      it. The button used to appear on all of them and opened a modal with nothing
                      in it — which reads as a broken feature rather than an absent one, the exact
                      thing `FundamentalButton`'s own docstring says not to do. */}
                  {g.bucket === EQUITY_BUCKET && g.basket.holdings.length > 0 && (
                    <FundamentalButton className="ml-2 align-middle"
                      title={`Blended owner earnings and price steadiness across the ${g.basket.holdings.length} priced name${g.basket.holdings.length === 1 ? '' : 's'} in ${bucketLabel(g.bucket)}, weighted by what the book holds today.`}
                      onOpen={() => onFundamental({ name: g.basket.label, basket: g.basket })} />
                  )}
                </td>
                <td className="py-2 text-right font-mono font-semibold text-fg-strong whitespace-nowrap">
                  {num2(g.slice?.pct ?? g.rows.reduce((s, h) => s + (h.weight_now_pct ?? 0), 0))}%
                  <Provenance source="airs_volk" asOf={asOf} kind="formula"
                    what={`${g.bucket}'s share of the book TODAY.`}
                    note={g.slice ? 'the allocation chart’s own figure — cash included'
                      : 'summed from the rows below'}
                    how={`Σ Huidige waarde of this class ÷ the book’s total

${num2(g.slice?.pct ?? g.rows.reduce((s, h) => s + (h.weight_now_pct ?? 0), 0))}% — the same figure the Allocation bar above is drawn from`} />
                </td>
                {/* The class's own four euro columns, summed — so a reader can see which CLASS
                    made the money, not only which position. */}
                {show('opening') && <td className={`py-2 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted`}>{eur0n(g.sum.opening)}</td>}
                {show('valuenow') && <td className={`py-2 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted`}>{eur0n(g.sum.valuenow)}</td>}
                {show('avgcapital') && <td className={`py-2 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted`}>{eur0n(g.sum.avgcapital)}</td>}
                {show('unrealised') && <td className={`py-2 text-right font-mono tabular-nums whitespace-nowrap ${retTone(g.sum.unrealised)}`}>{eur0n(g.sum.unrealised)}</td>}
                {show('realised') && <td className={`py-2 text-right font-mono tabular-nums whitespace-nowrap ${retTone(g.sum.realised)}`}>{eur0n(g.sum.realised)}</td>}
                {show('income') && <td className={`py-2 text-right font-mono tabular-nums whitespace-nowrap ${retTone(g.sum.income)}`}>{eur0n(g.sum.income)}</td>}
                {show('result') && <td className={`py-2 text-right font-mono font-semibold tabular-nums whitespace-nowrap ${retTone(g.sum.result)}`}>{eur0n(g.sum.result)}</td>}
                {show('contribution') && (
                <td className={`py-2 text-right font-mono font-semibold tabular-nums whitespace-nowrap ${retTone(g.sum.contribution)}`}>
                  {ppt(g.sum.contribution)}
                  {/* ⚠ THE PAIR A READER CANNOT ARBITRATE UNLESS IT IS EXPLAINED, and on a class
                      that is nearly the whole book the two sit a fraction of a point apart and
                      look like one of them is wrong. They share a NUMERATOR and differ only in
                      what they divide by — so the card prints both divisions, side by side. */}
                  <Provenance source="airs_volk" asOf={asOf} kind="formula"
                    what={`What ${bucketLabel(g.bucket)} added to, or took off, the book’s return for the year.`}
                    note={`${eur0n(g.sum.result)} ÷ the book’s opening capital`}
                    /* ⚠ THE MULTIPLICATION IS THE WHOLE EXPLANATION, so it goes on screen rather
                       than in prose. Contribution and Return differ by exactly one term — the
                       class's share of the book's OPENING capital — and that term is nowhere else
                       in the table: the Weight column is today's share (85.38% where the opening
                       share is 82.98%), which is why nobody could reconstruct it. Verified on
                       every class of both measured books, to the third decimal. */
                    how={`Return × this class’s share of the book’s opening capital

${fmtRet(g.ret.pct)} × ${openingShare == null ? '—' : num2(openingShare) + '%'} (${eur0n(g.ret.startEur)} of ${eur0n(realised?.basis_eur)}) = ${ppt(g.sum.contribution)}`} />
                </td>
                )}
                {show('moneyweighted') && <td className={`py-2 text-right font-mono font-semibold tabular-nums whitespace-nowrap ${retTone(g.sum.mwr)}`} title={g.sum.mwrRows < g.rows.length ? `Over the ${g.sum.mwrRows} of ${g.rows.length} holdings the book buys and sells itself — the rest sit inside certificates and have no flows of their own.` : undefined}>{fmtRet(g.sum.mwr)}</td>}
                {/* ⚠ THE COLUMN BELOW, AGGREGATED — NOT THE BOOK'S VALUE CHANGE, and not the
                    Weight (now) column times the returns. A dash where nothing in the class had
                    both an opening weight and a return; a 0.00% there would claim the class went
                    nowhere. */}
                <td className={`py-2 pr-4 text-right font-mono font-semibold tabular-nums whitespace-nowrap ${retTone(g.ret.pct)}`}>
                  {fmtRet(g.ret.pct)}
                  {/* ⚠ MARKED WHEN THE RATE DOES NOT DESCRIBE ALL THE MONEY. A row with no opening
                      value is out of both sides of the division — right, and invisible unless it
                      is said, because the money it made is still in the Result column beside it.
                      0.5pp of slack absorbs float noise without hiding a real gap. */}
                  {g.ret.pct != null && g.ret.coveredPct < 99.5 && (
                    <span className="ml-1 text-warn-400"
                      title={`This rate covers ${num2(g.ret.coveredPct)}% of what ${bucketLabel(g.bucket)} made — ${g.ret.rows - g.ret.legs} position(s) had no value when the year opened (bought since, or a cash line), so there is nothing to measure their result against. Their euros are still in the Result column.`}>⚠</span>
                  )}
                  <Provenance source="airs_volk" asOf={asOf} kind="formula"
                    what={g.ret.pct == null
                      ? `Nothing in ${bucketLabel(g.bucket)} was held when the year opened, so it has no return to measure.`
                      : `What ${bucketLabel(g.bucket)} made this year, against what it was worth when the year opened.`}
                    note={g.ret.pct == null
                      ? 'a dash, never a 0% — “no starting money to measure against” and “went nowhere” are different facts'
                      : `${eur0n(g.ret.resultEur)} ÷ ${eur0n(g.ret.startEur)}`
                        + (g.ret.coveredPct < 99.5
                          ? ` · covers ${num2(g.ret.coveredPct)}% of what this class made`
                          : '')}
                    /* ⚠ THE DIFFERENCE FROM THE BOOK'S OWN RETURN IS NAMED HERE, because a reader
                       who spots a class at 99.9% of the book returning a point less than the book
                       will otherwise go looking for it in the cash line — where it is not. */
                    /* ⚠ THE LONG VERSION IS IN THE CODE, NOT ON THE CARD (shortened 2026-08-05,
                       on request). What a reader needs at the cell is: what it divides, that it
                       does not tie to the book, and which column does. The reasoning behind that
                       — measured on AITopSelectie — is that three percentages on this row sit on
                       three different bases: Weight is the class's share TODAY (99.91%), this
                       Return divides by the RESTATED opening value (100.69% of the book), and the
                       class's TRUE share on 1 January was 4.03%, because the book held 96% cash
                       and deployed it on 5 January. On those true weights it composes exactly:
                       4.03% × 1102.77% + 95.97% × 0% = 44.4624%, AIRS's own figure. Which is
                       precisely why none of the three may be multiplied by another. */
                    how={`Result ÷ what this class was worth when the year opened

${eur0n(g.ret.resultEur)} ÷ ${eur0n(g.ret.startEur)} = ${fmtRet(g.ret.pct)}`} />
                </td>
              </tr>
              {[...g.rows].sort(cmp).map((h, i) => (
                <tr key={h.isin ?? `${g.bucket}-${h.name ?? i}`}
                  className="group border-b border-neutral-800/[0.15] last:border-0 hover:bg-overlay/[0.03] transition-colors">
                  <td className="py-1.5 pl-4 pr-2 text-right font-mono text-[10px] text-fg-faint tabular-nums">{i + 1}</td>
                  {/* ⚠ IN THE NAME CELL, NOT A NEW COLUMN. The header's colSpans are counted by
                      hand across four places in this table (group row, thead, body, tfoot); a
                      fourteenth column here shifts every figure one cell right, silently — a
                      weight renders perfectly well under "Ccy". The button rides with the name it
                      belongs to and appears on hover so 52 rows are not 52 buttons at rest. */}
                  <td className="py-1.5 pr-3 text-fg max-w-0" title={h.name ?? undefined}>
                    <span className="flex items-center gap-1.5 min-w-0">
                      <span className="truncate">{h.name ?? '—'}</span>
                      {/* ⚠ SAME GATE AS THE CLASS ROW, and it has to be here too or the rule is
                          half-applied: an ETF row carries an ISIN, so `h.isin &&` alone put a
                          Fundamental button on every fund, bond and commodity in the table. Owner
                          earnings are a property of an operating COMPANY; nothing else has them. */}
                      {h.isin && h.bucket === EQUITY_BUCKET && (
                        <FundamentalButton
                          className="opacity-0 group-hover:opacity-100 focus:opacity-100 shrink-0"
                          title={`Fundamental — is ${h.name ?? h.isin} fundamentally good? (owner earnings + price steadiness)`}
                          onOpen={() => onFundamental({ name: h.name ?? h.isin!, isin: h.isin! })} />
                      )}
                    </span>
                  </td>
                  {/* ⚠ The cap lives on a wrapper, not on the `<td>`: a second `max-w-0` column
                      fights the Name cell for the slack (the Sector comment below records the same
                      trap). A fixed max-width simply bounds it, and `ViaChips` already wraps. */}
                  <td className="py-1.5 pr-3">
                    <div className="max-w-[10rem]">
                      <ViaChips names={h.via_names ?? []} sources={h.sources} />
                    </div>
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
                      how={`Huidige waarde ÷ the book’s total Huidige waarde\n\n`
                        + `${eur0n(h.current_value_eur)} ÷ ${eur0n(grand.valuenow)} = `
                        + `${num2(h.weight_now_pct ?? 0)}%`} />
                  </td>
                  {/* ⚠ EVERY ONE A DASH WHERE THERE IS NOTHING, NEVER A €0. "Nothing was sold" and
                      "the sale broke even" are different facts, and on a money column the second
                      is a claim. */}
                  {show('opening') && <td className={`py-1.5 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted`}>{eur0n(h.start_value_eur)}</td>}
                  {show('valuenow') && <td className={`py-1.5 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted`}>{eur0n(h.current_value_eur)}</td>}
                  {show('avgcapital') && <td className={`py-1.5 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted`}>{eur0n(h.avg_capital_eur)}</td>}
                  {show('unrealised') && <td className={`py-1.5 text-right font-mono tabular-nums whitespace-nowrap ${retTone(h.unrealised_eur)}`}>{eur0n(h.unrealised_eur)}</td>}
                  {show('realised') && <td className={`py-1.5 text-right font-mono tabular-nums whitespace-nowrap ${retTone(h.realised_result_eur)}`}>{eur0n(h.realised_result_eur)}</td>}
                  {show('income') && <td className={`py-1.5 text-right font-mono tabular-nums whitespace-nowrap ${retTone(h.income_eur)}`}>{eur0n(h.income_eur)}</td>}
                  {show('result') && <td className={`py-1.5 text-right font-mono font-semibold tabular-nums whitespace-nowrap ${retTone(h.result_eur)}`}>{eur0n(h.result_eur)}</td>}
                  {show('contribution') && <td className={`py-1.5 text-right font-mono tabular-nums whitespace-nowrap ${retTone(h.contribution_pct)}`}>{ppt(h.contribution_pct)}</td>}
                  {show('moneyweighted') && <td className={`py-1.5 text-right font-mono tabular-nums whitespace-nowrap ${retTone(h.money_weighted_return_pct)}`} title={h.money_weighted_return_pct != null ? undefined : h.capital_unknown ? 'Shares were DEPOSITED into this position during the year (AIRS books it Tt = D, Deponering — a split, a bonus issue or a transfer in). Its trade quantities and its holding quantity are then on different bases, so the capital it tied up cannot be worked out. Its euro figures are unaffected.' : 'Held through a certificate — AIRS trades the wrapper, so this position has no purchases of its own to measure a return on.'}>{fmtRet(h.money_weighted_return_pct)}</td>}
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
                        ? `Σ (each route’s share of this position × that route’s own return)

${blendHow(h)}`
                        : h.own_return_source === 'yfinance'
                          ? `our own EUR close series — no AIRS book values this row

${fmtRet(h.own_return_pct)} since ${h.own_return_from ?? 'the year opened'}`
                          : `(Huidige waarde + net income) ÷ Beginwaarde − 1${h.own_return_book && h.own_return_book !== bookName ? `, as valued by ${h.own_return_book}` : ''}

${bookMath(h) ?? (h.own_income_eur ? `(Huidige waarde + ${eur0(h.own_income_eur)} net dividend) ÷ Beginwaarde − 1` : 'Huidige waarde ÷ Beginwaarde − 1')} = ${fmtRet(h.own_return_pct)}`} />
                  </td>
                </tr>
              ))}
            </tbody>
            );
          })}
          {/* ⚠ THE POSITIONS THAT ARE GONE — the reason this table did not add up before. A book
              that sold a name in March has nothing left to list it with, so its result vanished
              from a table that looked complete. They get no Weight, no Sector and no ISIN, because
              they genuinely have none any more; a 0% weight there would say the book held none of
              it, which is a claim rather than a blank. */}
          {!!sold.length && (
            <tbody>
              <tr className="bg-inset border-y border-neutral-800/40">
                <td className="pl-4" />
                <td className="py-2 font-medium text-fg-strong" colSpan={3}>
                  <span className="inline-block w-2.5 h-2.5 rounded-sm mr-2 align-middle bg-neutral-600" />
                  No longer held
                  <span className="ml-2 px-1.5 py-0.5 rounded-md bg-overlay/5 text-[10px] font-normal text-fg-muted">
                    {sold.length}
                  </span>
                  <span className="ml-2 text-[10px] font-normal text-fg-faint">
                    sold out during the year
                  </span>
                </td>
                <td className="py-2 text-right font-mono text-fg-faint">—</td>
                {/* ⚠ THE UNREALISED PLACEHOLDER, AND IT MUST BE GATED LIKE THE COLUMN IT STANDS IN
                    FOR. A sold-out position has nothing unrealised, so the cell is empty — but an
                    empty cell still OCCUPIES the column, and leaving it ungated puts this row one
                    cell ahead of the header the moment Unrealised is hidden. */}
                {show('opening') && <td className="py-2 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted">{eur0n(soldSum.opening)}</td>}
                {show('valuenow') && <td />}
                {show('avgcapital') && <td className="py-2 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted">{eur0n(soldCap)}</td>}
                {show('unrealised') && <td />}
                {show('realised') && <td className={`py-2 text-right font-mono tabular-nums ${retTone(soldSum.realised)}`}>{eur0n(soldSum.realised)}</td>}
                {show('income') && <td className={`py-2 text-right font-mono tabular-nums ${retTone(soldSum.income)}`}>{eur0n(soldSum.income)}</td>}
                {show('result') && <td className={`py-2 text-right font-mono font-semibold tabular-nums ${retTone(soldSum.result)}`}>{eur0n(soldSum.result)}</td>}
                {show('contribution') && <td className={`py-2 text-right font-mono font-semibold tabular-nums ${retTone(soldSum.contribution)}`}>{ppt(soldSum.contribution)}</td>}
                {show('moneyweighted') && <td className={`py-2 text-right font-mono tabular-nums ${retTone(soldSum.mwr)}`}>{fmtRet(soldSum.mwr)}</td>}
                <td className="pr-4" />
              </tr>
              {sold.map((p, i) => (
                <tr key={p.name ?? i} className="border-b border-neutral-800/[0.15] last:border-0 hover:bg-overlay/[0.03] transition-colors">
                  <td className="py-1.5 pl-4 pr-2 text-right font-mono text-[10px] text-fg-faint tabular-nums">{i + 1}</td>
                  <td className="py-1.5 pr-3 text-fg max-w-0" colSpan={3} title={p.name}>
                    <span className="truncate inline-block max-w-full align-bottom">{p.name}</span>
                    <span className="ml-2 text-[9px] text-fg-faint">
                      {p.first_sale === p.last_sale ? p.first_sale : `${p.first_sale} → ${p.last_sale}`}
                    </span>
                    {/* ⚠ THE REASON THE REALISED FIGURE IS AIRS'S `Res. YtD` AND NOT proceeds − cost:
                        part of this gain was made in earlier years and is correctly not counted. */}
                    {!!p.prior_year_eur && (
                      <span className="ml-2 text-[9px] text-warn-500"
                        title={`${eur0n(p.prior_year_eur)} of this position's realised result was earned in EARLIER years and is correctly not in this year's figure.`}>
                        {eur0n(p.prior_year_eur)} prior yr
                      </span>
                    )}
                  </td>
                  <td className="py-1.5 text-right font-mono text-fg-faint">—</td>
                  {/* Same placeholder, same gate — see the class header above. */}
                  {show('opening') && <td className={`py-1.5 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted`}>{eur0n(p.opening_eur)}</td>}
                  {show('valuenow') && <td />}
                  {show('avgcapital') && <td className={`py-1.5 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted`}>{eur0n(p.avg_capital_eur)}</td>}
                  {show('unrealised') && <td />}
                  {show('realised') && <td className={`py-1.5 text-right font-mono tabular-nums ${retTone(p.realised_result_eur)}`}>{eur0n(p.realised_result_eur)}</td>}
                  {show('income') && <td className={`py-1.5 text-right font-mono tabular-nums ${retTone(p.income_eur)}`}>{eur0n(p.income_eur)}</td>}
                  {show('result') && <td className={`py-1.5 text-right font-mono font-semibold tabular-nums ${retTone(p.result_eur)}`}>{eur0n(p.result_eur)}</td>}
                  {show('contribution') && <td className={`py-1.5 text-right font-mono tabular-nums ${retTone(p.contribution_pct)}`}>{ppt(p.contribution_pct)}</td>}
                  {show('moneyweighted') && <td className={`py-1.5 text-right font-mono tabular-nums ${retTone(p.return_pct)}`}>{fmtRet(p.return_pct)}</td>}
                  <td className="pr-4" />
                </tr>
              ))}
            </tbody>
          )}
          {/* ⚠ THE CHECK, AT THE FOOT OF THE TABLE IT CHECKS. Σ Contribution over every row above
              — held and sold — against AIRS's own return for the book. That the two agree is the
              statement this whole merge exists to make; showing the sum without the figure it
              should equal would be an assertion, not a check. */}
          {/* ⚠ PINNED TO THE BOTTOM OF THE SAME SCROLLPORT. Giving the table a viewport would
              otherwise have buried the one row that carries the check — Σ Contribution against
              AIRS's own return — under sixty holdings. A reconciliation you have to scroll to find
              is one nobody reads.
              ⚠ `[&_td]:bg-elevated`, a SOLID surface, for the same reason the header needs one:
              the row's own `bg-overlay/[0.05]` is translucent and the holdings would show through
              it. The tint is dropped rather than layered — an opaque total row that looks slightly
              different beats a tinted one you can read two numbers through. */}
          {grand.contribution != null && (
            <tfoot className="sticky bottom-0 z-20">
              <tr className="[&_td]:bg-elevated border-t-2 border-neutral-800/40 font-semibold">
                <td className="pl-4" />
                <td className="py-2 text-fg-strong" colSpan={3}>
                  The book’s year
                  <span className="ml-2 font-normal text-[10px] text-fg-faint">
                    {holdings.length + sold.length} positions, everything it held or sold
                  </span>
                </td>
                <td />
                {show('opening') && <td className={`py-2 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted`}>{eur0n(grand.opening)}</td>}
                {show('valuenow') && <td className={`py-2 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted`}>{eur0n(grand.valuenow)}</td>}
                {show('avgcapital') && <td className={`py-2 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted`}>{eur0n(grand.avgcapital)}</td>}
                {show('unrealised') && <td className={`py-2 text-right font-mono tabular-nums ${retTone(grand.unrealised)}`}>{eur0n(grand.unrealised)}</td>}
                {show('realised') && <td className={`py-2 text-right font-mono tabular-nums ${retTone(grand.realised)}`}>{eur0n(grand.realised)}</td>}
                {show('income') && <td className={`py-2 text-right font-mono tabular-nums ${retTone(grand.income)}`}>{eur0n(grand.income)}</td>}
                {show('result') && <td className={`py-2 text-right font-mono tabular-nums ${retTone(grand.result)}`}>{eur0n(grand.result)}</td>}
                {show('contribution') && <td className={`py-2 text-right font-mono tabular-nums ${retTone(grand.contribution)}`}>{ppt(grand.contribution)}</td>}
                {show('moneyweighted') && <td className={`py-2 text-right font-mono tabular-nums ${retTone(grand.mwr)}`}>{fmtRet(grand.mwr)}</td>}
                <td className={`py-2 pr-4 text-right font-mono tabular-nums ${retTone(realised?.book_ytd_pct)}`}>
                  {fmtRet(realised?.book_ytd_pct)}
                  <Provenance source="airs_volk" asOf={asOf} kind="formula"
                    what="The book’s own return for the year, from AIRS."
                    note="cumulatief_rendement — flow-aware, the system of record"
                    how={`AIRS’s own cumulatief_rendement, set against the Contribution total to its left

${ppt(grand.contribution)} from these rows vs ${fmtRet(realised?.book_ytd_pct)} from AIRS${reconciled ? ' — they agree' : ' — they do NOT agree'}`} />
                </td>
              </tr>
            </tfoot>
          )}
        </table>
      </div>
      {/* Said in words under the table, because a reader who has just added a column of euros
          wants to know whether it landed — not to compare two figures themselves. */}
      {/* ⚠ IT FOLLOWS THE COLUMN IT TALKS ABOUT. This line reads "the Contribution column adds to
          AIRS's own +5.83% exactly" — a statement with no referent once that column is hidden,
          which since the picker defaults to all-off would otherwise be its normal state. A caveat
          pointing at something not on screen is worse than no caveat: it makes a reader hunt for a
          column that is not there. */}
      {show('contribution') && grand.contribution != null && realised?.book_ytd_pct != null && (
        <div className="px-4 py-2 border-t border-neutral-800/40 text-[10px]">
          {reconciled ? (
            <span className="text-pos-400">
              ✓ These positions account for the whole year — the Contribution column adds to
              AIRS’s own {fmtRet(realised.book_ytd_pct)} exactly.
            </span>
          ) : (
            <span className="text-warn-500">
              ⚠ The Contribution column adds to {ppt(grand.contribution)} against AIRS’s own
              {' '}{fmtRet(realised.book_ytd_pct)} — {ppt(grand.contribution - realised.book_ytd_pct)}
              {' '}of the year is not explained by these rows.
              {realised.residual_reason ? ` ${realised.residual_reason}` : ''}
            </span>
          )}
        </div>
      )}
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
  /** The instrument or class whose Fundamental is open, over this modal. Null = closed. */
  const [fund, setFund] = useState<{ name: string; isin?: string; basket?: Basket } | null>(null);
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
                  /* Summed here rather than server-side: it is the same `closed_out` set the
                     Holdings table's own group already sums, and one source beats two. */
                  soldContribution={data.realised?.available
                    ? (data.realised.positions ?? [])
                      .filter((p) => p.closed_out)
                      .reduce((a, p) => a + (p.contribution_pct ?? 0), 0)
                    : null}
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
              /* ⚠ ONE TABLE AGAIN. It was briefly two — a composition view plus a separate ledger
                 — and that was the wrong shape: the sold positions were the only thing standing
                 between this table and a total, so the answer was to give them rows, not their own
                 card. `realised` carries them (and the book's own return to check against). */
              <PortfolioHoldings holdings={data.book_holdings ?? []} slices={data.allocation}
                onFundamental={setFund}
                note={data.book_note} bookName={data.book_portefeuille} realised={data.realised}
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
                {/* ⚠⚠ THESE VIEWS ARE WEIGHT-BASED, AND A SOLD POSITION HAS NO WEIGHT — so the
                    bars and the attribution below describe only what is still held. That is not a
                    rounding hole: measured on BUS_Offensief_Dyn, 22.5% of the year's movement was
                    realised on sales and is absent from every figure in this section.
                    ⚠ IT CANNOT BE FIXED BY ADDING THEM. A sold parcel's opening value is not
                    recoverable from AIRS's data (`proceeds − Res. YtD` gives its cost basis, which
                    for a February purchase is capital that did not exist on 1 January), and
                    allocation effect is undefined without a start weight. Inventing one
                    manufactures exactly the confident false finding this modal already documents —
                    a model holding 6% Healthcare credited +1.73pp for "avoiding" it. So the hole
                    is STATED, in the same idiom as `unpriced_pct` and `benchmark_coverage_pct`,
                    and the sold names are itemised in the Holdings view instead. */}
                <RealisedCoverageNote r={data.realised} />
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
                        /* ⚠ The share of the year Brinson structurally cannot see — see the
                           prop's own note. Only meaningful once the book's transactions have been
                           read; undefined otherwise, which the panel renders as no notice rather
                           than as a reassuring 0%. */
                        realisedSharePct={data.realised?.available
                          ? data.realised.realised_share_of_result_pct : null}
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
        {/* ⚠ RENDERED INSIDE THE CONTENT BOX, NOT BESIDE IT. This modal's backdrop closes it on
            click, and a nested modal's own backdrop covers the whole screen — mounted as a sibling
            of that backdrop, dismissing the Fundamental would bubble up and close the Analyse
            modal underneath it too. The content box already stops propagation, so putting it here
            makes the two dismiss independently. It still paints over everything: the nested
            backdrop is `fixed inset-0`, which escapes this box's layout but not its event tree. */}
        {fund && (
          <OwnerEarningsModal isin={fund.isin} basket={fund.basket} name={fund.name}
            onClose={() => setFund(null)} />
        )}
      </div>
    </div>
  );
}
