import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';

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

/** A benchmark to draw beside the book: the index label + the cadence the tab is on. */
export type BenchTarget = { universe: string; cadence: 'annual' | 'quarterly' };

/**
 * Fetch one `*-inputs` endpoint for a benchmark, or nothing when no benchmark is selected.
 *
 * The overlay never breaks the chart under it — a failed index costs the second line and nothing
 * else. But it does NOT fail silently: it returns the reason, and `benchNote` turns that into one
 * short line in the legend. An overlay that just doesn't appear is indistinguishable from an index
 * that matches the portfolio exactly, and there is no way for the reader to tell which they got.
 * The full detail goes to the console, as everywhere else here.
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
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/${path}`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(target),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) {
          const detail = (b?.detail as string) ?? `HTTP ${r.status}`;
          console.warn(`[bb:bench] ${path} ${target.universe}: ${detail}`, b);
          setState([null, detail]);
          return;
        }
        setState([b as T, null]);
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
 * 80% weight-coverage floor (`MIN_YEAR_COVERAGE_PCT`). Collapsing them into "no line" is the same
 * mistake as showing an unpriced holding as 0%.
 *
 * ⚠ THE FLOOR IS WHY A WHOLE CARD CAN HAVE NO INDEX LINE, AND IT IS USUALLY STRUCTURAL RATHER THAN
 * A GAP IN OUR INGEST. Measured 2026-08-04 on "Interest / op. profit": the ratio needs a POSITIVE
 * operating income, and **a bank does not report an operating income line at all** (GuruFocus
 * template 'B') — ING, ABN AMRO, JPMorgan, Bank of America, Morgan Stanley and Goldman all carry
 * interest expense with no operating income, and insurers (NN, ASR, Aegon) carry neither. Their
 * weight still sits in the denominator, so AEX coverage lands at 72–80% and clears the floor in
 * exactly ONE year of twelve; the S&P clears it in none. A book of 20 industrials clears it every
 * year, which is why the portfolio line is there and the index's is not.
 *
 * ⚠ ONE PERIOD IS CALLED OUT SEPARATELY, because a lone dot on a chart reads as a rendering glitch.
 * (It is also why the benchmark carries dots at all — a one-point series drawn as a bare line is
 * invisible, which is how this was mistaken for a missing fetch in the first place.)
 */
export function benchNote(
  target: BenchTarget | null | undefined,
  data: unknown, error: string | null, series: Map<number, number | null> | null,
): string | null {
  if (!target) return null;
  if (error) return `${target.universe}: ${error}`;
  if (!data) return `${target.universe}: loading…`;
  if (!series || series.size === 0) {
    return `${target.universe}: no period clears the 80% coverage floor`;
  }
  if (series.size === 1) {
    return `${target.universe}: one period only — the rest fall under the 80% coverage floor`;
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
 * A benchmark LEVEL series, scaled to start where the subject's does — the only honest way to put
 * an index beside a level on ONE axis.
 *
 * ⚠ THE RATIO CARDS NEED NOTHING LIKE THIS; the level cards cannot do without it. A margin is a %
 * and the two lines are already in the same unit. Revenue is EUR millions for one company and a
 * rebased blend index for the S&P — drawn raw they are two scales on one axis, i.e. the dual-axis
 * mistake with the second axis hidden, and the reader would compare a company against 100.
 *
 * Both level cards are on a LOG axis, where a multiplicative rebase is a pure vertical shift: the
 * SHAPE — the growth rate, which is the whole comparison — is untouched. The anchor is the first
 * period BOTH series have (never each series' own first point, which would silently compare
 * different starting years), so the lines meet there by construction and diverge by their growth.
 *
 * Returns null when they share no period: with nothing to anchor on, any scale factor is invented.
 * Every caller labels the line "(rebased)" — a scaled series that reads as an absolute one is how
 * someone concludes the S&P's revenue is EUR 300bn.
 */
export function rebaseOnto(
  bench: Map<number, number | null>, own: Map<number, number | null>,
): Map<number, number | null> | null {
  const anchor = [...bench.keys()].sort((a, b) => a - b).find((x) => {
    const b = bench.get(x); const o = own.get(x);
    return b != null && o != null && b > 0 && o > 0;
  });
  if (anchor == null) return null;
  const k = (own.get(anchor) as number) / (bench.get(anchor) as number);
  return new Map([...bench].map(([x, v]) => [x, v == null ? null : v * k]));
}

/** Both series' values, for a shared y-domain — a benchmark drawn off-axis is worse than none. */
export function withBench(
  own: Iterable<number | null>, bench: Map<number, number | null> | null,
): number[] {
  return [...own, ...(bench ? [...bench.values()] : [])].filter((v): v is number => v != null);
}
