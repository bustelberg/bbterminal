/** Pure helpers behind the "instruments involved" table under the correlation matrix.
 *
 * Everything here is geometry and slicing over the payload the correlation endpoint already
 * ships — no fetching, no formatting decisions that belong to the component — so the parts that
 * are easy to get subtly wrong (a null-vs-zero gap, a window boundary) can be unit-tested. That
 * is the repo's rule for frontend logic: extract the pure function rather than build a harness.
 */

/** The shared date axis + per-key value columns, exactly as the endpoint encodes them. */
export type SeriesBlock = {
  dates: string[];
  values: Record<string, (number | null)[]>;
};

export type SparkPoint = { date: string; value: number };

/** Rows of the series for one key, from `from` (inclusive), gaps dropped.
 *
 * ⚠ A `null` IS A DAY THAT INSTRUMENT DID NOT TRADE, NOT A ZERO. The axis is the union of every
 * instrument's trading days, so a Tokyo listing carries nulls on Japanese holidays that Paris
 * traded through. Coercing them (`?? 0`) draws a spike to the floor on each one; keeping them as
 * points with a null value makes most chart libraries do the same or break the line. They are
 * dropped, which renders as a straight segment across the holiday — the same "still held, last
 * price" reading `_index` takes on the backend.
 */
export function seriesPoints(block: SeriesBlock, key: string | null | undefined,
  from?: string): SparkPoint[] {
  if (!key) return [];
  const col = block.values[key];
  if (!col) return [];
  const out: SparkPoint[] = [];
  for (let i = 0; i < block.dates.length; i += 1) {
    const v = col[i];
    const d = block.dates[i];
    if (v === null || v === undefined) continue;
    if (from && d < from) continue;
    out.push({ date: d, value: v });
  }
  return out;
}

/** The first day of `year` — the YTD window's left edge for the instrument table.
 *
 * ⚠ THE TABLE'S WINDOW IS NOT A PORTFOLIO'S ANCHOR. A portfolio's YTD opens at
 * `max(1 Jan, its inception)` because pricing its weights before it held them is hindsight; an
 * INSTRUMENT has no inception to respect — it either traded on a day or it did not. So this is a
 * plain 1 January, and a row whose series starts later simply starts later, which its
 * `first_date` already says.
 */
export function ytdStart(asOf: string): string {
  return `${asOf.slice(0, 4)}-01-01`;
}

/** `d` attribute for a sparkline polyline over `points`, in a `w`x`h` box.
 *
 * ⚠ SCALED PER ROW, min-to-max of its OWN series. These are absolute EUR prices (and, for a
 * look-through row, an index based at 100), so one shared scale would flatten every cheap share
 * against Hermès at ~EUR 2,000 and show 240 straight lines. A sparkline answers "what shape",
 * never "how much" — the numeric columns beside it carry level.
 *
 * A flat series (max === min) is drawn on the centre line rather than divided by zero.
 */
export function sparkPath(points: SparkPoint[], w: number, h: number, pad = 1): string {
  if (points.length === 0) return '';
  const lo = Math.min(...points.map((p) => p.value));
  const hi = Math.max(...points.map((p) => p.value));
  const span = hi - lo;
  const inner = h - pad * 2;
  const x = (i: number) => (points.length === 1 ? w / 2 : (i / (points.length - 1)) * w);
  const y = (v: number) => (span === 0 ? h / 2 : pad + inner - ((v - lo) / span) * inner);
  return points.map((p, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(2)},${y(p.value).toFixed(2)}`)
    .join(' ');
}

/** Percentage change across the window, or null when there is nothing to compare.
 *
 * ⚠ null RATHER THAN 0 ON A SINGLE POINT. One observation is not a 0% return, it is no return —
 * and a column of quiet zeros is how a thin listing passes for a stable one.
 */
export function windowReturnPct(points: SparkPoint[]): number | null {
  if (points.length < 2) return null;
  const first = points[0].value;
  const last = points[points.length - 1].value;
  if (!(first > 0)) return null;
  return (last / first - 1) * 100;
}

/** The vendors behind one row, as a short label plus the long form for a tooltip.
 *
 * ⚠⚠ EVERY PRICED ROW HERE SAYS "yfinance", AND THAT SAMENESS IS THE ANSWER, NOT A BUG. This app
 * holds TWO price worlds — GuruFocus (`metric_data`, keyed on company_id) prices the /benchmarks
 * index and the momentum engine; yfinance (`asset_price`, keyed on ISIN) prices the AIRS books —
 * and only the second one reaches this matrix. Someone reading a correlation has no way to know
 * that without being told, and the two vendors disagree on adjustment convention and FX.
 *
 * ⚠ AND THE PRICE IS ONLY HALF OF IT. A EUR level for a USD holding is a yfinance close TIMES an
 * ECB rate — two vendors multiplied together — so the FX leg is named too. A EUR-quoted holding
 * has no second vendor because no conversion happens, which is why it reads plain "yfinance"
 * rather than being padded out to look like the others.
 */
export function sourceLabel(row: { price_source?: string | null; fx_source?: string | null }): {
  short: string; title: string;
} {
  const price = row.price_source;
  if (!price) {
    return {
      short: '—',
      title: 'No price series, so no vendor supplied anything for this row.',
    };
  }
  const fx = row.fx_source;
  if (!fx) {
    return {
      short: price,
      title: `Prices from ${price} (asset_price). Quoted in EUR, so no FX conversion is `
        + 'applied and no second vendor is involved.',
    };
  }
  if (fx === 'per holding') {
    return {
      short: `${price} · basket`,
      title: `Prices from ${price} (asset_price). This row is a certificate wrapping another `
        + 'model, so it has no currency of its own — each holding inside the wrapped basket '
        + 'converts to EUR on its own rate.',
    };
  }
  return {
    short: `${price} + ${fx}`,
    title: `Prices from ${price} (asset_price), converted to EUR at the ${fx} rate for each `
      + 'date. Both vendors are in the number: a EUR level is a close times a rate.',
  };
}

export type InstrumentSort = 'holdings' | 'weight' | 'name' | 'liquidity' | 'return';

/** Sort comparator for the table.
 *
 * ⚠ ABSENT SORTS TO THE BOTTOM IN BOTH DIRECTIONS — the rule the /portfolios table already
 * follows for its own absent states. An instrument with no liquidity figure is not the least
 * liquid one, and an unpriced row has no return to be worst at; either would otherwise take the
 * top of a descending sort and read as a finding.
 */
export function compareInstruments<T extends {
  name?: string | null; isin: string; in_portfolios?: number; weight_pct_sum?: number;
  med_adv_eur?: number | null;
}>(a: T, b: T, by: InstrumentSort, desc: boolean,
  returnOf: (r: T) => number | null): number {
  const dir = desc ? -1 : 1;
  if (by === 'name') {
    return dir * (a.name || a.isin).localeCompare(b.name || b.isin);
  }
  const pick = (r: T): number | null => {
    if (by === 'holdings') return r.in_portfolios ?? 0;
    if (by === 'weight') return r.weight_pct_sum ?? 0;
    if (by === 'liquidity') return r.med_adv_eur ?? null;
    return returnOf(r);
  };
  const av = pick(a);
  const bv = pick(b);
  if (av === null && bv === null) return (a.name || a.isin).localeCompare(b.name || b.isin);
  if (av === null) return 1;      // absent -> bottom, whichever way we are sorting
  if (bv === null) return -1;
  if (av === bv) return (a.name || a.isin).localeCompare(b.name || b.isin);
  return dir * (av - bv);
}
