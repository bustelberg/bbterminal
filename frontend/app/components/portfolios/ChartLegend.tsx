'use client';

/**
 * One legend entry per LINE, with a swatch drawn the way that line is drawn.
 *
 *  The cards used to pack several lines into one entry as prose. A solid blue swatch standing
 * for a dashed average is actively misleading: it shows the shape of the series line next to words
 * describing a reference line.
 *
 * A legend's whole job is "this mark means this thing". A dashed line gets a dashed swatch, or the
 * legend is a caption.
 *
 *  The dash geometry mirrors the recharts `strokeDasharray`, NOT AN EYEBALLED APPROXIMATION —
 * `5 3` for an average line — so a swatch and its line read as the same stroke rather than as two
 * similar-looking ideas. Opacity mirrors `strokeOpacity` for the same reason: a reference line is
 * deliberately recessive, and a legend that shows it at full strength promises a more prominent
 * mark than the chart draws.
 */

export type Stroke = 'solid' | 'dashed' | 'striped';

/** `strokeDasharray` + `strokeOpacity`, as CSS. Keep in step with the `ReferenceLine`s. */
const SWATCH: Record<Stroke, (c: string) => React.CSSProperties> = {
  solid: (c) => ({ background: c }),
  // recharts `strokeDasharray="5 3"` at strokeOpacity 0.6 — the per-card average line.
  dashed: (c) => ({
    backgroundImage: `repeating-linear-gradient(to right, ${c} 0 5px, transparent 5px 8px)`,
    opacity: 0.6,
  }),
  // recharts `strokeDasharray="4 3"` at full strength — the analysts' forecast leg.
  //
  //  At full opacity, unlike the two reference strokes above, because it is not a reference mark:
  // it carries values, with a dot on every forecast year exactly as the solid line has one on every
  // reported year. A recessive stroke read as annotation rather than as a series.
  striped: (c) => ({
    backgroundImage: `repeating-linear-gradient(to right, ${c} 0 4px, transparent 4px 7px)`,
  }),
};

export function LegendItem({ color, stroke = 'solid', label, title }: {
  color: string;
  stroke?: Stroke;
  label: React.ReactNode;
  title?: string;
}) {
  return (
    <span className="flex items-center gap-1.5" title={title}>
      {/*  `w-4`, NOT `w-3`. Three pixels of a 5-on-3-off dash is one dash and no gap — i.e. a solid
          swatch wearing a dashed style, which is worse than no swatch because it looks deliberate. */}
      <span className="w-4 h-0.5 inline-block rounded shrink-0" style={SWATCH[stroke](color)} />
      {label}
    </span>
  );
}
