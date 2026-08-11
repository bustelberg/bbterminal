/**
 * WHERE EACH DECISION SITS ON THE YEAR — layout for the holding-timing timeline.
 *
 * The panel's three lines say the trading was worth €3,028. They cannot say that it was ONE sale
 * near the high and ONE repurchase near the low, two months apart. A date column can't either:
 * `03 Feb` and `23 Apr` are eight characters that carry no shape. Placed on an axis against the
 * price they were struck at, the decision reads at a glance.
 *
 * ⚠⚠ THESE ARE THE PRICES WE OBSERVED, NOT A PRICE HISTORY, AND THE DISTINCTION IS THE WHOLE
 *    HONESTY OF THE PICTURE. Every point is a real number we hold — the opening value, each traded
 *    price, today's value — and the segments between them are STRAIGHT LINES, not the path the
 *    price took. Adobe fell to €204 and came back; drawn as a line from €246 to €204 it looks
 *    monotonic, and a reader would conclude the repurchase was made on the way down rather than at
 *    a bottom we cannot see. The component labels the line as connecting observations, and nothing
 *    here should ever grow a `asset_price` fetch to "fill it in" — that is the yfinance world, it
 *    would NOT pass through the AIRS-traded points, and a curve that misses its own markers is
 *    worse than a line that admits it is one.
 *
 * ⚠ A TRADE WITH NO DATE CANNOT BE PLACED, AND IS COUNTED RATHER THAN DROPPED. Silently omitting it
 *    leaves a timeline whose markers don't add up to the table beside it; `undated` is returned so
 *    the caller can say so.
 *
 * ⚠ NO WINDOW, NO PICTURE. `buildTimeline` returns null without both bounds rather than inferring
 *    an axis from the trades themselves — an axis running first-trade → last-trade puts the first
 *    decision at the very start of the year, which is precisely the claim the chart exists to make
 *    and would be making up.
 *
 * Prices are already EUR and already in TODAY's share basis (a pre-split trade is converted
 * upstream in `airs_timing`), so every point is on one axis. Mixing bases would place a pre-split
 * trade ten times too high and draw a cliff that never happened.
 */

export const TL = {
  width: 720,
  height: 208,
  padL: 14,
  padR: 14,
  top: 58,      // room for two label lanes above the highest point
  bottom: 44,   // room for the date axis
  /** Two lanes, so two decisions a fortnight apart don't overprint. */
  laneGap: 20,
  /** Horizontal room one label needs before the next may reuse its lane. */
  minLabelGap: 104,
  /** Head/foot padding on the price axis so a marker never sits on the frame. */
  pricePad: 0.10,
} as const;

export type TimelineTrade = {
  datum?: string | null;
  kind?: string | null;
  quantity?: number | null;
  price_eur?: number | null;
  effect_eur?: number | null;
  rescaled?: boolean | null;
};

export type TimelinePointKind = 'open' | 'buy' | 'sell' | 'now';

export type TimelinePoint = {
  kind: TimelinePointKind;
  date: string;
  price: number;
  x: number;
  y: number;
  /** Null on the endpoints — "doing nothing" is not a decision with an effect. */
  effect: number | null;
  quantity: number | null;
  rescaled: boolean;
  /** 0 or 1 — which label row this point's callout sits in. */
  lane: number;
};

export type Timeline = {
  points: TimelinePoint[];
  /** Polyline through every observation, in date order. */
  path: string;
  /** Trades we could not place because AIRS gave them no date. */
  undated: number;
  priceLo: number;
  priceHi: number;
  start: string;
  end: string;
};

const ms = (d: string): number | null => {
  const t = Date.parse(`${d}T00:00:00Z`);
  return Number.isNaN(t) ? null : t;
};

export function buildTimeline(
  start: string | null | undefined,
  end: string | null | undefined,
  priceOpen: number | null | undefined,
  priceNow: number | null | undefined,
  trades: TimelineTrade[],
): Timeline | null {
  if (!start || !end || priceOpen == null || priceNow == null) return null;
  const t0 = ms(start);
  const t1 = ms(end);
  if (t0 == null || t1 == null || t1 <= t0) return null;

  let undated = 0;
  const raw: { kind: TimelinePointKind; date: string; t: number; price: number;
    effect: number | null; quantity: number | null; rescaled: boolean }[] = [
    { kind: 'open', date: start, t: t0, price: priceOpen, effect: null, quantity: null, rescaled: false },
  ];
  for (const tr of trades) {
    const t = tr.datum ? ms(tr.datum) : null;
    if (t == null || tr.price_eur == null) { undated += 1; continue; }
    raw.push({
      kind: tr.kind === 'sell' ? 'sell' : 'buy',
      date: tr.datum as string,
      // ⚠ CLAMPED INTO THE WINDOW. A trade dated outside it (a stale snapshot, a settlement date
      // past `tot`) would otherwise be drawn off the frame, where it reads as absent.
      t: Math.min(Math.max(t, t0), t1),
      price: tr.price_eur,
      effect: tr.effect_eur ?? 0,
      quantity: tr.quantity ?? null,
      rescaled: !!tr.rescaled,
    });
  }
  raw.push({ kind: 'now', date: end, t: t1, price: priceNow, effect: null, quantity: null, rescaled: false });
  raw.sort((a, b) => a.t - b.t);

  const prices = raw.map((p) => p.price);
  let lo = Math.min(...prices);
  let hi = Math.max(...prices);
  // A position that never moved has no range to scale against; give it one so the line is drawn
  // flat through the middle rather than divided by zero.
  if (hi - lo < 1e-9) { lo -= 1; hi += 1; } else {
    const pad = (hi - lo) * TL.pricePad;
    lo -= pad; hi += pad;
  }

  const plotW = TL.width - TL.padL - TL.padR;
  const plotH = TL.height - TL.top - TL.bottom;
  const x = (t: number) => TL.padL + ((t - t0) / (t1 - t0)) * plotW;
  const y = (p: number) => TL.top + (1 - (p - lo) / (hi - lo)) * plotH;

  // ⚠ GREEDY TWO-LANE PACKING, not alternating. Alternating looks tidy on two trades and collides
  // on three clustered ones; this only steps down when the previous label in that lane is still
  // within `minLabelGap`.
  const laneLastX = [-Infinity, -Infinity];
  const points: TimelinePoint[] = raw.map((p) => {
    const px = x(p.t);
    let lane = 0;
    if (p.kind === 'buy' || p.kind === 'sell') {
      lane = px - laneLastX[0] >= TL.minLabelGap ? 0 : 1;
      laneLastX[lane] = px;
    }
    return {
      kind: p.kind, date: p.date, price: p.price, x: px, y: y(p.price),
      effect: p.effect, quantity: p.quantity, rescaled: p.rescaled, lane,
    };
  });

  return {
    points,
    path: points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' '),
    undated, priceLo: lo, priceHi: hi, start, end,
  };
}

/** "3 Feb" — the axis has no room for a year, and every point is inside one. */
export function shortDay(iso: string): string {
  const t = ms(iso);
  if (t == null) return iso;
  return new Date(t).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' });
}
