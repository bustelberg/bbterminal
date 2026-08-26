import { chartTheme } from './chartTheme';

/**
 * The tilted X axis, once, for every chart in the Fundamental modal.
 *
 * ⚠ FOURTEEN CARDS HAD THIS INLINE, CHARACTER FOR CHARACTER. They sit in one grid and are read
 * against each other, so an axis that differs between two of them is a difference the reader has to
 * rule out before believing anything else on the screen — and a tilt applied to thirteen of fourteen
 * is exactly the kind of near-miss a copy-paste axis produces.
 *
 * ⚠ IT LIVES IN `lib/`, NOT BESIDE THE CARDS, because the charts that use it span two features:
 * `components/earnings/` (the /earnings dashboard) and `components/portfolios/` (the Fundamental
 * modal's cards). Putting it in either would make the other import across a feature boundary, and
 * it is the same class of thing as `chartTheme` — chart presentation with no feature attached — so
 * it belongs where `chartTheme` does.
 *
 * ⚠⚠ `angle` AND `textAnchor` MUST BE ON THE **AXIS**, NOT INSIDE `tick`. Recharts renders a tilted
 * label from either (`CartesianAxis` falls back to `tick.angle`), but the code that decides HOW MANY
 * ticks fit — `getTicks` → `getAngledTickWidth` — destructures `angle` from the AXIS props and
 * nothing else. Put it only in `tick` and the labels rotate while the fitting math stays blind to
 * it: the same ticks as before, now on a slant. Which looks like the change worked.
 */

/**
 * The tilt.
 *
 * ⚠⚠ 45° IS NOT A ROUND NUMBER PICKED FOR LOOKS — IT IS WHERE THE RETURNS STOP. Run recharts' own
 * `getAngledRectangleWidth`, which is `min(h/sin θ, w/cos θ)` — the packing PITCH of rotated labels,
 * not the bounding box (`w·cos θ + h·sin θ`) one would guess at. Labels that lean can overlap in the
 * corner without colliding, so the pitch collapses toward the label's HEIGHT and stops there.
 * At 12px, on a 270px plot:
 *
 *              `2015` (27x12)        `2015 Q2` (45x12)
 *      0°      27.0px →  8 labels     45.0px →  5 labels
 *     30°      24.0px →  9            24.0px →  9
 *     45°      17.0px → 12            17.0px → 12
 *     60°      13.9px → 14            13.9px → 14
 *     90°      12.0px → 15            12.0px → 15
 *
 * So 45° takes an annual axis from 8 years to 12 and a quarterly one from 5 to 12 — and past it the
 * curve flattens hard (12 → 14 → 15) because the pitch is already bounded by the height. Steeper
 * costs real legibility for two more labels; 45° is the conventional tilt, the one a reader parses
 * without turning their head, and it captures most of what is available.
 *
 * ⚠ NOTE BOTH CADENCES CONVERGE AT 45°. Past the threshold the formula switches to `h/sin θ`, which
 * has no `w` in it — so a wide label and a narrow one pack identically. That is why the quarterly
 * axis, the crowded one, gains the most: 45px of width stops mattering at all.
 *
 * ⚠ WHICH IS ALSO WHY THIS IS WORTH APPLYING TO THE DATE AXES, and not only the fiscal-period ones.
 * `getTickSize` reads `angle` for ANY horizontal axis — category, numeric or `scale="time"` — so the
 * Old-charts `2025-06` labels (7 chars, the widest on the screen) gain the most of anything here.
 */
const TILT_DEGREES = -45;

/**
 * Room for the tilted labels.
 *
 * ⚠ RECHARTS' DEFAULT IS 30px AND A TILTED LABEL DOES NOT FIT IN IT. At 45° a `2015 Q2` label
 * stands ~40px tall (`w·sin θ + h·cos θ`), so the default clips it — and the axis band is INSIDE
 * the container height, so a clipped label is silently cropped rather than pushing the card taller.
 * 46 clears the longest label this axis formats plus the tick line and margin.
 *
 * ⚠ IT COSTS THE PLOT ~16px, WHICH IS THE TRADE. The alternative is growing every card by 16px,
 * and these sit in an `auto-rows-fr` grid beside charts that would then have to grow too.
 */
export const PERIOD_AXIS_HEIGHT = 46;

/**
 * The tilt on its own — spread onto ANY horizontal `<XAxis>`, whatever it is keyed on.
 *
 * ⚠ IT CARRIES NO `dataKey` AND NO `tickFormatter` ON PURPOSE. The charts in this modal are keyed
 * five different ways — a fiscal-period index (`year`), a plain category (`date` as `2025-06-30`), a
 * numeric year with pinned ticks, and two `scale="time"` millisecond axes — and a shared helper that
 * guessed at the key would silently blank the axis on four of them. Only the presentation is shared.
 *
 * `fontSize` varies because it already did: the Long Equity grid draws at 12px and the wider
 * Old-charts labels at 11px. Standardising it here would be a second, unasked-for change riding
 * along with the tilt.
 */
export const tiltedAxis = ({ fontSize = 12 }: { fontSize?: number } = {}) => ({
  angle: TILT_DEGREES,
  // ⚠ `end`, SO THE LABEL HANGS BACK FROM ITS TICK rather than running forward off it. With a
  // negative angle the text rises to the right; anchoring at the start would push each label away
  // from the gridline it names and toward the next one.
  textAnchor: 'end' as const,
  height: PERIOD_AXIS_HEIGHT,
  tickMargin: 4,
  tick: { fontSize, fill: chartTheme.axisTick },
});

/**
 * Props for a fiscal-period `<XAxis>`. Spread it: `<XAxis {...periodAxis(xToPeriod)} />`.
 *
 * `tickFormatter` is the only thing that varies — two cards switch to `xToMonth` on a daily series.
 */
export const periodAxis = (tickFormatter: (x: number) => string) => ({
  ...tiltedAxis(),
  dataKey: 'year',
  tickFormatter,
});
