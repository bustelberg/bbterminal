import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { MIN_YEAR_COVERAGE_PCT } from './marginData';

/**
 * The benchmark line every Long Equity chart can carry.
 *
 * ⚠ IT IS THE SAME ENDPOINT, THE SAME BODY SHAPE AND THE SAME CLIENT-SIDE HELPER AS THE
 * PORTFOLIO'S OWN LINE — only `{holdings|portfolio_id}` is swapped for `{universe}`. That is the
 * whole design. A chart with two lines on one axis is only honest if both were computed the same
 * way, and the surest way to guarantee that is for there to be exactly one computation: the card
 * calls `marginByYear` (or `debtRatioByYear`, …) twice, over two row sets. There is no
 * "benchmark margin" anywhere in the codebase to drift from the portfolio's.
 *
 * The index arrives CAP-WEIGHTED from the server (`_load_and_expand_members`'s universe branch
 * carries `market_cap_eur` as the weight), so the card's existing weighted average IS the
 * weighted-average benchmark the user asked for — nothing extra to weight here.
 *
 * ⚠ COVERAGE IS NOT 100% and differs per index. Only companies whose fundamentals we have ingested
 * contribute; an index we have barely ingested draws a confident-looking line over a fraction of
 * itself. The server reports what it drew — see `benchmark_margin`'s `coverage_pct`.
 */

/**
 * ⚠ THE BENCHMARK IS GREEN (`chartTheme.pos`) ON EVERY CHART, WITHOUT EXCEPTION. It has to be one
 * colour or the eye re-learns which line is the index on each of the fourteen cards. Validated
 * rather than eyeballed (`dataviz/scripts/validate_palette.js`): green↔the accent blue scores ΔE
 * 19.1 deutan, 20.7 normal. On the two cards that also carry an amber TREND line, green↔amber is
 * ΔE 7.9 protan — the 6–8 floor band, legal only with a second encoding, which those have (the
 * trend is dashed, the benchmark solid, and both are named in the legend).
 *
 * Green also carries a "positive" meaning in this app's ramps; here it means "the index", and
 * every chart names it. It is never used to say the benchmark is good.
 */

/**
 * A benchmark to draw beside the book: the index label + the cadence the tab is on.
 *
 * ⚠⚠ `'daily'` IS DELIBERATELY NOT IN THIS UNION, AND IT IS NOT AN OVERSIGHT. The two yield
 * cards offer a per-card daily toggle, but it only ever moves `holdingsTarget` — the benchmark
 * stays on the tab's cadence. Measured 2026-08-18 against the S&P 500: a DAILY benchmark request
 * returns 490 per-constituent series, 11s of server time and **54 MB** of JSON, all of it reduced
 * by the client to the single blended line the card draws. The book's own daily request is 2-4 MB.
 *
 * Widening this union is therefore a ~25x payload change with no visible cause: the card would
 * simply take forever and nothing would say why. If a daily benchmark is ever wanted, blend it
 * SERVER-SIDE first — do not ship 490 daily series so the browser can average them.
 */
export type BenchTarget = { universe: string; cadence: 'annual' | 'quarterly' };

/**
 * Row order in every Long Equity hover: the BENCHMARK first, then the book. Pass as `itemSorter`.
 *
 * ⚠⚠ IT WAS ALREADY COMING OUT THIS WAY, BY ACCIDENT, AND THAT IS THE REASON TO DECLARE IT.
 * Recharts' default sorter is `'name'` — alphabetical on the SERIES name, not on the label the
 * formatter produces — and every benchmark line here is named `bench`, which happens to sort before
 * `margin`, `ratio`, `value`, `yld` and `trend`. Rename one series (or add a card whose line is
 * called `bench_something`, or `assets`) and that card alone silently flips its two rows, on a
 * screen of fourteen charts where the reader has learned the first row is the index. An order the
 * eye relies on across a whole tab cannot rest on the alphabet.
 *
 * ⚠ THE INDEX GOES FIRST because it is the constant: it is the same line on all fourteen cards,
 * so a fixed position makes it the thing you read past rather than the thing you have to find. The
 * book's own line — the one that differs per card and per portfolio — reads as the answer beneath.
 *
 * It is a sort KEY, not a comparator: recharts sorts ascending on what this returns.
 */
export const benchmarkFirst = (item: { name?: unknown }): number =>
  (item.name === 'bench' ? 0 : 1);

/** `{canonical ISIN: {period: market cap in EUR}}` — the index's weighting basis, fetched once. */
export type CapTable = Record<string, Record<string, number>>;

/**
 * Which endpoints have had `market_cap_by_period` LIFTED OUT of their rows.
 *
 * ⚠ THE `*-inputs` FAMILY, AND NOT `portfolio-revenue-matrix`. That one is a drill-down that
 * renders the cap and the weight in its own cells, so it still ships them inline and must not have
 * a second copy spliced over the top. Naming the rule here rather than at eleven call sites is
 * what keeps a new card from silently drawing a flat-weighted index line — see `spliceCaps` for
 * why that failure is invisible.
 */
const CAPS_LIFTED_OUT = /-inputs$/;

/**
 * Put the shared cap table back on the rows, exactly as the server used to.
 *
 * ⚠⚠ `{}` AND ABSENT ARE DIFFERENT ANSWERS AND THIS IS WHERE THAT IS PRESERVED. `weightAt` reads
 * an EMPTY `market_cap_by_period` as "this constituent is out of every period's average" and a
 * MISSING one as "fall back to `weight_pct`, flat, for all of them". So every row of an index
 * response gets the key — `{}` when we hold no cap for it (4 of ACWI's 1,514) — and none of them
 * gets it when the whole table is empty, which is the shape the server produced with
 * `if caps else {}`. Get this wrong and the benchmark line still draws, still looks plausible, and
 * is weighted by the wrong thing; there is no blank cell anywhere to notice.
 *
 * Pure and exported for the test beside it: this is the half of the split payload that can be
 * wrong without anything failing.
 */
export function spliceCaps<T>(data: T, caps: CapTable): T {
  if (!caps || Object.keys(caps).length === 0) return data;
  const d = data as { rows?: { isin?: string }[] };
  if (!Array.isArray(d?.rows)) return data;
  return {
    ...d,
    rows: d.rows.map((r) => ({ ...r, market_cap_by_period: caps[r.isin ?? ''] ?? {} })),
  } as T;
}

/**
 * Fetch one `*-inputs` endpoint for a benchmark, or nothing when no benchmark is selected.
 *
 * The overlay never breaks the chart under it — a failed index costs the second line and nothing
 * else. But it does NOT fail silently: it returns the reason, and `benchNote` turns that into one
 * short line in the legend. An overlay that just doesn't appear is indistinguishable from an index
 * that matches the portfolio exactly, and there is no way for the reader to tell which they got.
 * The full detail goes to the console, as everywhere else here.
 *
 * ⚠⚠ TWO REQUESTS, AND THE SECOND IS NOT OPTIONAL. `market_cap_by_period` used to ride on every
 * row of all ten card responses — the same table ten times, measured at 29.9% of each ACWI payload
 * (~4.8 MB of the tab's 13.21 MB), which gzip cannot dedupe because it cannot see across
 * responses. It now comes from `/universe-period-caps` once. The ten cards ask for it in the same
 * instant and `apiFetch` stores the in-flight promise, so that is ONE request, not ten.
 *
 * ⚠⚠ AND THE CARD WAITS FOR BOTH. Handing over rows the moment they land, with the caps still in
 * flight, would draw a line weighted by `weight_pct` — today's cap, flat across every year — which
 * is precisely the look-ahead bias `weightAt` exists to avoid, and it would then silently redraw.
 * For the same reason a FAILED cap fetch is an ERROR here, not a fallback: an index line that
 * quietly changes what it is weighted by is worse than no index line.
 */
export function useBenchInputs<T>(
  path: string, target: BenchTarget | null | undefined,
): [T | null, string | null] {
  const [state, setState] = useState<[T | null, string | null]>([null, null]);
  const key = target ? `${target.universe}|${target.cadence}` : '';
  useEffect(() => {
    let alive = true;
    void (async () => {
      setState([null, null]);
      if (!target) return;
      const post = (p: string) => apiFetch(`${API_URL}/api/earnings/${p}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(target),
      });
      /** The body, or a thrown reason — one shape for both requests.
       *  ⚠ THE RESPONSE TRAVELS WITH THE ERROR (`cause`) so the console keeps the full diagnostic
       *  — including WHICH of the two requests failed — while the UI gets `message`, one short
       *  line. Same rule as everywhere else here. */
      const read = async (p: string): Promise<unknown> => {
        const r = await post(p);
        const b = await r.json().catch(() => null);
        if (!r.ok) {
          throw new Error((b?.detail as string) ?? `HTTP ${r.status}`,
            { cause: { path: p, status: r.status, body: b } });
        }
        return b;
      };
      try {
        const wantCaps = CAPS_LIFTED_OUT.test(path);
        // In parallel: the wait is the slower of the two, not their sum. The caps request is
        // shared with the other nine cards and is ~0.19 MB against a card's ~0.34 MB.
        const [rows, caps] = await Promise.all([
          read(path),
          wantCaps ? read('universe-period-caps') as Promise<{ caps?: CapTable }> : null,
        ]);
        if (!alive) return;
        setState([spliceCaps(rows as T, caps?.caps ?? {}), null]);
      } catch (e) {
        const detail = e instanceof Error ? e.message : String(e);
        console.warn(`[bb:bench] ${path} ${target?.universe}: ${detail}`, e);
        if (alive) setState([null, detail]);
      }
    })();
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path, key]);
  return state;
}

/**
 * Why a selected benchmark drew no line (or barely one) — one short sentence, or null.
 *
 * ⚠ THE ABSENCES LOOK IDENTICAL ON SCREEN AND HAVE DIFFERENT FIXES: the request is still in flight
 * (wait), the request failed (read the console), or it succeeded and every period fell under the
 * weight-coverage floor (`MIN_YEAR_COVERAGE_PCT`). Collapsing them into "no line" is the same
 * mistake as showing an unpriced holding as 0%.
 *
 * ⚠ THE FLOOR IS WHY A WHOLE CARD CAN HAVE NO INDEX LINE, AND IT IS USUALLY STRUCTURAL RATHER THAN
 * A GAP IN OUR INGEST. Measured 2026-08-04 on "Interest / op. profit": the ratio needs a POSITIVE
 * operating income, and **a bank does not report an operating income line at all** (GuruFocus
 * template 'B') — ING, ABN AMRO, JPMorgan, Bank of America, Morgan Stanley and Goldman all carry
 * interest expense with no operating income, and insurers (NN, ASR, Aegon) carry neither. Their
 * weight still sits in the denominator, so AEX coverage lands at 72–80% and cleared the 80% floor
 * in exactly ONE year of twelve; the S&P cleared it in none. A book of 20 industrials clears it
 * every year, which is why the portfolio line is there and the index's is not. ⚠ Those measured
 * figures are why the floor moved to 50 (2026-08-12): at 72–80% covered, that card's index line was
 * being withheld over a fifth of a book it genuinely spans.
 *
 * ⚠ ONE PERIOD IS CALLED OUT SEPARATELY, because a lone dot on a chart reads as a rendering glitch.
 * (It is also why the benchmark carries dots at all — a one-point series drawn as a bare line is
 * invisible, which is how this was mistaken for a missing fetch in the first place.)
 */
export function benchNote(
  target: BenchTarget | null | undefined,
  data: unknown, error: string | null, series: Map<number, number | null> | null,
  /**
   * Whether THIS caller applied the coverage floor to `series`.
   *
   * ⚠⚠ IT USED TO ASSERT THE FLOOR UNCONDITIONALLY, AND THAT SENT ME AFTER THE WRONG BUG. The ratio
   * cards build their series through `*ByYear`, which applies `MIN_YEAR_COVERAGE_PCT` here — for
   * them the sentence is measured and true. The GROWTH cards pass raw blended rows with no floor
   * applied on the client at all, so an empty series there could be anything, and naming the floor
   * turned "the benchmark returned nothing" into a confident diagnosis. Measured 2026-08-12: AEX
   * quarterly Revenue reported "no period clears the coverage floor" while the real cause was the
   * blend aligning points by YEAR against weights keyed by QUARTER — every point dropped before
   * coverage was computed at all (`quarter_bucket`).
   */
  flooredHere = true,
): string | null {
  if (!target) return null;
  if (error) return `${target.universe}: ${error}`;
  if (!data) return `${target.universe}: loading…`;
  if (!flooredHere) {
    // What is observable from here, and nothing more. The reason lives on the server's
    // `blend_notes` for this code.
    if (!series || series.size === 0) {
      return `${target.universe}: the blended series came back empty — see the console`;
    }
    return series.size === 1
      ? `${target.universe}: one period only — a single dot, not a line` : null;
  }
  // ⚠ THE NUMBER IS READ, NEVER SPELT. It used to be "80%" in both strings, so the day the floor
  // moved the legend would have gone on quoting a floor that no longer existed — a caption that
  // contradicts the chart it explains, and nothing would have failed.
  if (!series || series.size === 0) {
    return `${target.universe}: no period clears the ${MIN_YEAR_COVERAGE_PCT}% coverage floor`;
  }
  if (series.size === 1) {
    return `${target.universe}: one period only — the rest fall under the `
      + `${MIN_YEAR_COVERAGE_PCT}% coverage floor`;
  }
  return null;
}

/**
 * The chart rows for a portfolio series + an optional benchmark series.
 *
 * ⚠ THE X UNION, NOT THE PORTFOLIO'S PERIODS. An index reaches back further than most books, and
 * clipping it to the book's own span would silently redraw the benchmark's history every time a
 * holding changed. `connectNulls` on both lines covers the ragged ends.
 */
export function mergeSeries(
  own: Map<number, number | null>,
  bench: Map<number, number | null> | null,
  ownKey = 'value',
): Record<string, number | null>[] {
  const xs = new Set<number>([...own.keys()]);
  if (bench) for (const x of bench.keys()) xs.add(x);
  return [...xs].sort((a, b) => a - b).map((x) => ({
    year: x,
    [ownKey]: own.get(x) ?? null,
    ...(bench ? { bench: bench.get(x) ?? null } : {}),
  }));
}

/**
 * Rebase one or both level series to 100 at a COMMON anchor — the axis the level cards actually
 * plot. Returns the indexed maps plus the anchor year, or null when nothing can be anchored.
 *
 * ⚠ THE RATIO CARDS NEED NOTHING LIKE THIS; the level cards cannot do without it. A margin is a %
 * and the two lines are already in the same unit. Revenue is EUR millions for one company and a
 * blended index for the S&P — drawn raw they are two scales on one axis, i.e. the dual-axis
 * mistake with the second axis hidden, and the reader would compare a company against 100.
 *
 * The level cards are on a LOG axis, where a multiplicative rebase is a pure vertical shift: the
 * SHAPE — the growth rate, which is the whole comparison — is untouched. That is what makes it
 * safe to index BOTH lines rather than scale one onto the other, which is what this replaced
 * (`rebaseOnto`, removed once nothing called it): stretching the index to a company's absolute
 * level put a true-looking but meaningless number on the benchmark line, and someone reading it
 * as absolute is how you conclude the S&P's revenue is EUR 300bn. Indexed, neither line pretends
 * to be an amount, and the actual values live in the hover.
 *
 * ⚠⚠ THE ANCHOR IS THE FIRST YEAR THE SERIES SHARE, NEVER EACH SERIES' OWN FIRST POINT. A company
 * whose data starts in 2018 rebased on 2018, drawn against an index rebased on 2015, compares a
 * seven-year path against a four-year one and calls the difference performance. Same rule, and the
 * same reason, as `rebaseOnto` — which this replaces on the level cards, because indexing BOTH
 * sides is honest where scaling one onto the other only looks like it.
 *
 * ⚠ BOTH VALUES AT THE ANCHOR MUST BE > 0. Dividing by zero is obvious; dividing by a NEGATIVE is
 * the dangerous one, because it silently FLIPS the series and the chart still renders — a company
 * whose FCF/share began negative would appear to collapse as it recovered. This is not
 * hypothetical: the dividend-per-share card was dropped from this tab precisely because its series
 * starts at 0.00 and the level rebase cannot survive it.
 *
 * ⚠ REFUSES RATHER THAN INVENTING ONE. A null here means the caller keeps ABSOLUTE values, which
 * is the honest fallback — the raw number is always true, it just is not comparable.
 */
/**
 * Does this level series change sign — i.e. can it be an INDEX ON A LOG AXIS at all?
 *
 * ⚠⚠ THE TWO DECISIONS ("index it?" and "which axis?") ARE ONE DECISION, AND SPLITTING THEM IS
 * WHAT BROKE. `rebaseSeries` refused to index a sign-changing series and the card fell back to
 * absolute values, saying so in the legend — but the Y axis stayed LOGARITHMIC and the chart data
 * still nulled everything ≤ 0. The fallback promised the real numbers and then hid exactly the
 * ones that had triggered it. Measured: AMD's 2015-16 losses and Intel's 2024 were invisible on
 * both paths, so a reader saw a line that simply began late, with nothing to say why.
 *
 * ⚠ `!(v > 0)` RATHER THAN `v <= 0`, so a null or a NaN counts as "cannot be logged" too. A hole
 * in the series is not a sign change, but it is equally unplottable on a log axis, and the honest
 * axis for either is the linear one.
 *
 * ⚠ REVENUE NEVER TRIPS THIS. It is EPS, FCF/share and net income — the lines that go negative —
 * which is why the check is on the DATA and not on the metric's name.
 */
export function seriesCrossesZero(values: Iterable<number | null | undefined>): boolean {
  for (const v of values) if (!(typeof v === 'number' && v > 0)) return true;
  return false;
}

export function rebaseSeries(
  own: Map<number, number | null>, bench: Map<number, number | null> | null,
): { own: Map<number, number | null>; bench: Map<number, number | null> | null; anchor: number } | null {
  const usable = (m: Map<number, number | null>, x: number) => {
    const v = m.get(x);
    return v != null && v > 0;
  };
  const anchor = [...own.keys()].sort((a, b) => a - b)
    .find((x) => usable(own, x) && (!bench || usable(bench, x)));
  if (anchor == null) return null;
  const scale = (m: Map<number, number | null>) => {
    const base = m.get(anchor) as number;
    return new Map([...m].map(([x, v]) => [x, v == null || v <= 0 ? null : (v / base) * 100]));
  };
  return { own: scale(own), bench: bench ? scale(bench) : null, anchor };
}

/** Both series' values, for a shared y-domain — a benchmark drawn off-axis is worse than none. */
export function withBench(
  own: Iterable<number | null>, bench: Map<number, number | null> | null,
): number[] {
  return [...own, ...(bench ? [...bench.values()] : [])].filter((v): v is number => v != null);
}
