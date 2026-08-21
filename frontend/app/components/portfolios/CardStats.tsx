'use client';

import { chartTheme } from '../../../lib/chartTheme';
import { xToPeriod } from './marginData';
import { sharedSpan, spanNarrows, tileStats, type Span, type TileStats } from './windowStats';

/**
 * One stat tile. ⚠ EVERY TILE IN THE PORTFOLIOS CARD FAMILY IS THIS COMPONENT AND EVERY ONE IN A ROW
 * IS THE SAME SIZE (2026-08-18) — thirteen cards import it.
 *
 * ⚠⚠ THE WIDTH IS AN EQUAL SHARE OF THE ROW, NOT A FIXED `8rem` (2026-08-21). It was fixed, and
 * before that `min-w-[6.5rem]` with content-sized growth — which came out as a ragged set of
 * different widths (`+18.4%` narrow, `EUR 1,240` wide) and different heights the moment one label
 * wrapped and its neighbours did not. Five tiles of five shapes read as five unrelated readouts
 * rather than one row of comparable figures, which on the Quick Valuation card is exactly wrong,
 * since comparing them IS the card.
 *
 * `flex-1 basis-0` KEEPS ALL OF THAT AND DROPS THE PART THAT BROKE: every tile in a row still gets
 * an identical width, but that width now comes from the row instead of a constant. The constant was
 * only ever right for the card it was measured on. Once each chart grew a BENCHMARK tile beside its
 * own, four tiles at 8rem wanted ~636px inside a card that is ~444px at a 1920px viewport and
 * ~316px at 1440px — so they wrapped, and a pair meant to be read side by side ended up stacked.
 *
 * ⚠ `max-w-[8rem]` IS THE OLD CONSTANT, KEPT AS A CEILING. Without it a row of TWO tiles in a wide
 * container would stretch each to half the card — an enormous readout of one number
 * (`AccountTotalReturn` is exactly that shape). So: never wider than it used to be, narrower when
 * the row needs it. `min-w-0` is what actually permits the shrink — without it the flex base is the
 * content and nothing gives.
 *
 * ⚠ THE NUMBER GOT BIGGER AS THE BOX GOT SMALLER, AND THAT IS THE POINT OF THE EXERCISE. The value
 * is the only thing on a tile anybody reads at a glance; the label and the padding are scaffolding.
 * So the label drops to 9px, the padding to `px-1.5 py-1` and the box to 3.1rem tall, while the
 * value goes UP (`text-base` → `text-lg`). Shrinking everything uniformly would have made the tile
 * tidier and harder to read, which is the opposite of what a smaller tile is for.
 *
 * ⚠⚠ THE LABEL GETS **TWO** LINES, ALWAYS RESERVED, AND THAT IS WHAT MAKES THE HEIGHT CONSTANT.
 * One line was tried first and it is the obvious answer: truncate, hang the full text on `title`,
 * done. But at this width a single 10px line holds ~13 characters, and FOUR of the Quick Valuation
 * card's five labels are longer than that — `CURRENT SHARE PRICE`, `PRICE TARGET FY2035`,
 * `EST. CAGR TO FY2035`, `FCF / SHARE CAGR`. A row of tiles reading `CURRENT SHARE P…` /
 * `PRICE TARGET F…` / `EST. CAGR TO F…` is uniform and useless: the truncation lands exactly where
 * the labels stop differing. Two clamped lines fit every label in the family, and reserving the
 * space whether or not it is used is what keeps a one-line tile the same height as a two-line one.
 *
 * ⚠ THE VALUE STILL TRUNCATES, on purpose, and now it does so sooner — a 1440px viewport with
 * four tiles leaves ~58px of inner width, which is about five monospace glyphs at `text-lg`.
 * `title` carries the full figure, exactly as it always has for `EUR 124,000`. The alternative is
 * letting one wide value resize the tile it sits in, which is the raggedness above.
 *
 * ⚠ THE ⓘ IS `shrink-0`. Without it a long label truncates by eating its own info icon first, which
 * removes the explanation from precisely the tiles whose labels were too long to be self-evident.
 *
 * `color` (a chart hex) ties the tile to its line — a coloured left bar + matching value ink — and
 * OVERRIDES `tone`, so a caller wanting sign-coloured ink (red/green) must not also pass a colour.
 */
export function Stat({ label, value, tone, color, info }: {
  label: string; value: string; tone?: string; color?: string; info?: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-neutral-800/40 bg-inset px-1.5 py-1 h-[3.1rem]
                    flex-1 basis-0 min-w-0 max-w-[8rem]
                    flex flex-col justify-between overflow-hidden"
      style={color ? { borderLeft: `3px solid ${color}` } : undefined}>
      {/* ⚠ THE HEIGHT IS ON THE ROW, NOT LEFT TO THE TEXT — two lines’ worth at 9px/leading-tight,
          reserved whether the label uses them or not. `items-start` keeps the ⓘ beside the FIRST
          line rather than floating to the middle of a two-line label. */}
      <div className="flex items-start gap-1 h-[1.2rem] text-[9px] uppercase tracking-wide text-fg-muted leading-tight">
        <span className="line-clamp-2" title={label}>{label}</span>
        <span className="shrink-0 flex items-center leading-none">{info}</span>
      </div>
      <div title={value}
        className={`font-mono text-lg font-semibold leading-tight truncate ${color ? '' : (tone ?? 'text-fg-strong')}`}
        style={color ? { color } : undefined}>{value}</div>
    </div>
  );
}

/**
 * THE STAT TILES ABOVE EVERY LONG EQUITY CHART — the book's figure and, beside it, THE SAME FIGURE
 * FOR THE BENCHMARK.
 *
 * ⚠⚠ THE BENCHMARK TILE IS NOT A SECOND CALCULATION, IT IS THE SAME ONE OVER THE OTHER LINE. That
 * is the whole design, and it is the same argument `benchSeries` makes about the lines themselves:
 * a chart with two series on one axis is only honest if both were computed identically, and the
 * surest way to guarantee that is for there to be exactly one computation. `tileStats` runs twice,
 * over two maps. There is no "benchmark average" anywhere in this codebase to drift from the
 * portfolio's.
 *
 * ⚠⚠ AND BOTH SIDES ARE MEASURED OVER THE SPAN THE TWO LINES SHARE — see the ⚠⚠ block above
 * `sharedSpan`. Two tiles side by side are a subtraction waiting to happen; over different windows
 * that subtraction means nothing and nothing on screen would say so.
 *
 * ⚠ THE COLOUR IS THE LINE'S, AND THE LABEL SAYS IT TOO. Green is the benchmark on every chart in
 * this family without exception (`benchSeries`' palette note), so the tile is tied to its line by
 * ink — but the tiles WRAP at this card width, so position cannot be relied on to pair them, and
 * colour alone is not an encoding this repo accepts. Hence `Avg · AEX`: the label carries it.
 *
 * ⚠ `Stat` MOVED HERE FROM `MetricGrowthCard` AND IS RE-EXPORTED FROM THERE. Thirteen cards import
 * it from that file and always have; it had to move because the row below builds tiles and
 * `MetricGrowthCard` builds this row, which was an import cycle. The re-export is what keeps those
 * thirteen call sites untouched — see the note on it there.
 */

/** Both lines' figures over the one window they share, plus whether that window took anything off
 *  the book's own line. ⚠ COMPUTED ONCE PER CARD AND PASSED DOWN, because several cards also draw
 *  their average as a `ReferenceLine` — a second `tileStats` call for the line under the tile is
 *  how a card comes to plot a mean it does not print. */
export type PairedStats = {
  span: Span | null; own: TileStats; bench: TileStats | null; narrowed: boolean;
};

/** ⚠ THE ONE WINDOW RULE FOR THE WHOLE TAB — the growth cards call this too, for a fit and a
 *  point-to-point rate rather than a mean. Pure; see `windowStats` for the reasoning. */
export function pairedSpan(
  own: ReadonlyMap<number, number | null>, bench?: ReadonlyMap<number, number | null> | null,
): PairedStats {
  const span = bench ? sharedSpan(own, bench) : null;
  return {
    span,
    own: tileStats(own, span),
    // ⚠⚠ NO SHARED SPAN MEANS NO BENCHMARK TILE, NOT A TILE OVER THE INDEX'S OWN YEARS. A null
    // span has two causes and they must not be conflated: no benchmark selected (nothing to draw),
    // and a benchmark that overlaps this line in NOTHING. In the second case both sides would fall
    // back to their own full histories — two figures from two disjoint periods, printed as a pair,
    // which is precisely the comparison this whole window rule exists to refuse. The line beside it
    // already says the index has no years in common; a number here would contradict it.
    bench: bench && span ? tileStats(bench, span) : null,
    narrowed: spanNarrows(own, span),
  };
}

/** `Avg` → `Avg · AEX`. ⚠ ONE SEPARATOR, DEFINED ONCE — the tile labels are uppercased by `Stat`'s
 *  own styling and clamp to two lines, so a long index or company name wraps rather than truncating
 *  the word that says WHICH statistic it is. */
export const benchTileLabel = (base: string, label: string | null | undefined) =>
  `${base} · ${label ?? 'Benchmark'}`;

/** `2016–2024`, or a single period when the window is one point wide. */
export function spanText(span: Span): string {
  const from = xToPeriod(span.fromX);
  const to = xToPeriod(span.toX);
  return from === to ? from : `${from}–${to}`;
}

/**
 * The little line beside a narrowed tile row, naming the window both figures were measured over.
 *
 * ⚠⚠ IT IS THE ONLY THING THAT SAYS THE TILES MOVED. Selecting a benchmark can shorten the span the
 * figures cover — that is deliberate and it is what makes them comparable — but a number that
 * changes because of an unrelated control, silently, is indistinguishable from a bug. Rendered ONLY
 * when the span actually took something off the book's own line, so the common case (both lines
 * covering the same years) stays quiet.
 */
export function SpanNote({ span, narrowed, benchLabel }: {
  span: Span | null;
  /** ⚠ PASSED IN, NOT RE-DERIVED. Only the caller knows which series the tiles are ABOUT — the
   *  growth cards clip a point array, the ratio cards a map — and asking this component to work it
   *  out would mean it guessing at a series it was never handed. */
  narrowed: boolean;
  benchLabel?: string | null;
}) {
  if (!span || !narrowed) return null;
  return (
    // ⚠ `basis-full` PUTS IT ON ITS OWN LINE. The tiles are `flex-1 basis-0` and share what
    // the row has left, so a note sitting among them would take its content width off the top
    // and squeeze four tiles into the remainder — the note explaining a narrowing would have
    // caused one.
    <span className="basis-full text-[10px] text-fg-faint"
      title={'Both figures are measured over the years this line and the benchmark BOTH cover, so '
        + 'they answer the same question. Outside that span only one of the two lines has data, and '
        + 'an average or a rate taken over a different window is not comparable with the one beside '
        + 'it. Choose "None" as the benchmark to see this line over its own full history.'}>
      {spanText(span)} · shared with {benchLabel ?? 'the benchmark'}
    </span>
  );
}

/**
 * `Avg` + `Latest` for a RATIO card, book and benchmark — the tile row ten cards on this tab share.
 *
 * ⚠ TEN CARDS, ONE ROW COMPONENT, BECAUSE THEY WERE TEN COPIES OF THE SAME SIX LINES. Each one
 * computed `meanOf([...byYr.values()])` and a `Math.max(-Infinity, ...keys())` latest of its own;
 * adding a benchmark tile to each by hand would have been ten more chances for one card to average
 * over a window its neighbour does not.
 */
export function RatioStats({
  stats, benchLabel, fmt, avgLabel = 'Avg', latestLabel = 'Latest',
  avgInfo, latestInfo, children,
}: {
  stats: PairedStats;
  benchLabel?: string | null;
  fmt: (v: number | null) => string;
  /** `MarginCard` says "Avg margin"; the rest say "Avg". The benchmark tile inherits it, so the two
   *  can never be labelled as different statistics. */
  avgLabel?: string;
  latestLabel?: string;
  avgInfo?: React.ReactNode;
  latestInfo?: React.ReactNode;
  /** Extra tiles that belong to the BOOK only (a coverage figure), rendered after the pairs. */
  children?: React.ReactNode;
}) {
  const b = stats.bench;
  return (
    <div className="flex flex-wrap gap-2">
      {/* ⚠ PAIRED ADJACENTLY — own, then its benchmark twin. At this card width four tiles wrap to
          two rows, so ordering them own/own/bench/bench would split each pair across the break and
          leave the reader matching by colour across a line. */}
      <Stat label={avgLabel} value={fmt(stats.own.avg)} color={chartTheme.accent} info={avgInfo} />
      {b && <Stat label={benchTileLabel(avgLabel, benchLabel)} value={fmt(b.avg)}
        color={chartTheme.pos} />}
      <Stat label={latestLabel} value={fmt(stats.own.latest)} color={chartTheme.accent}
        info={latestInfo} />
      {b && <Stat label={benchTileLabel(latestLabel, benchLabel)} value={fmt(b.latest)}
        color={chartTheme.pos} />}
      {children}
      <SpanNote span={stats.span} narrowed={stats.narrowed} benchLabel={benchLabel} />
    </div>
  );
}
