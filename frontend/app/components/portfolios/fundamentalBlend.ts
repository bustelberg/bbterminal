/**
 * THE BLENDED LINE, AND THE ARITHMETIC UNDER EVERY CELL THAT EXPLAINS IT.
 *
 * ⚠⚠ IT LIVED INSIDE `MatrixTable`, WHICH MEANT THE BOOK'S LINE AND THE INDEX'S COULD NEVER MEET.
 * Each table computed its own `blend` in its own `useMemo`, so nothing above them held both — and
 * anything that COMPARES the two (the CAGR table, and whatever comes after it) had no way to ask.
 * The maths never depended on the component: the memo's only dependency was `data`. So it is a pure
 * function of one payload, which is also what makes it testable without rendering anything.
 *
 * ⚠ THE BODY IS MOVED VERBATIM. Every ⚠ below was paid for by a wrong number on screen — the
 * per-period cap weighting (NVIDIA at 0.63% of FY2018, not 7.46%), the chained growth rather than
 * averaged levels (which drew a 388 → 285 crash no constituent experienced), the carry-forward
 * bound, the coverage floor. Re-deriving any of it "the same way" somewhere else is how this file
 * comes to disagree with the chart it exists to explain.
 *
 * ⚠ `Row` / `Resp` AND THE PERIOD HELPERS CAME WITH IT, because they are the shape of the payload
 * rather than anything about a modal. `HoldingsRevenueModal` re-exports the ones it used to own, so
 * no call site moved.
 */
import { MIN_YEAR_COVERAGE_PCT } from './marginData';
import { memberScale, stepGrowth } from './stepGrowth';

export type Row = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  /** The key the per-row refresh fetches on — a real `company.company_id`, not the `analysis_id`
   *  that hides under that name elsewhere. Absent only if the backend predates it, which is why
   *  the control is rendered conditionally rather than assuming it. */
  company_id?: number | null;
  /** When we last ASKED GuruFocus for this company's financials (ISO). NULL/absent = never asked,
   *  which makes every empty period `not_tried` rather than `no_data`. See `cellState`. */
  financials_fetched_at?: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data'; revenue: Record<string, number | null>;
  /**
   * The FILINGS this row's `LTM` cell was rolled from, and the rule that rolled them.
   *
   * ⚠⚠ THE LTM COLUMN IS THE ONLY ONE THIS APP ASSEMBLED. Every other cell is a figure the company
   * filed for that fiscal period; the LTM is `k` consecutive filings combined under a declared
   * rule, and those quarters reach the browser NOWHERE else — the tab's "Quarterly" toggle looks
   * like the place to check and is not, because the server rolls those too, so it shows more
   * trailing years rather than the filings under them. Hence one ⓘ per cell of this column.
   */
  ltm_parts?: { date: string; value: number }[] | null;
  ltm_rule?: string | null;
  /** INDEX ROWS ONLY — the numerator the weight beside it was divided out of (cap ÷ Σcap).
   *  Absent on a portfolio, where the weight is a holding weight and no cap is involved. */
  market_cap_eur?: number | null;
  /**
   * INDEX ROWS ONLY — the market cap as at each fiscal period, in EUR, converted at that
   * period's own end date (`period_caps_eur`).
   *
   * ⚠ THIS, NOT `market_cap_eur`, IS WHAT WEIGHTS EACH PERIOD. Weighting 2018's revenue by today's
   * cap is look-ahead bias: measured on the S&P, NVIDIA is carried at 7.46% of a year it was 0.63%
   * of. Absent for a portfolio (a holding weight has no market cap behind it), and SPARSE within
   * an index — a period with no filed cap is missing rather than padded, because the company is
   * then left out of that period's average entirely.
   */
  market_cap_by_period?: Record<string, number>;
  /**
   * THE EUROS THIS ROW CONTRIBUTES TO THE METRIC, per fiscal period — `per_share × shares`,
   * converted at that period's own end rate (index form), or `wᵢ·Fᵢ/capᵢ` (portfolio form).
   *
   * ⚠⚠ ITS PRESENCE IS THE SWITCH BETWEEN TWO CONSTRUCTIONS. Where rows carry it, the line is
   * `ΣFᵢ(d)/ΣFᵢ(a) − 1` — growth of a SUM — and the Contribution column is the exact
   * decomposition of that. Where they do not, both fall back to the cap-weighted growth chain.
   * The two differ by a lot and neither is obviously wrong on screen: measured on ACWI revenue,
   * ~9.95%/yr averaged against +4.60%/yr summed, and on FCF/share +19.1% against +7.56%.
   *
   * ⚠ ABSENT, NOT `{}`, WHEN THE BACKEND HAS NO EUROS FOR THE ROW — an empty map would claim the
   * aggregate and have nothing to sum. Sparse WITHIN a row is fine and expected (a period with no
   * share count or no FX rate simply has no figure), and the per-step intersection handles it.
   */
  fund_by_period?: Record<string, number>;
};
/** Universe requests only: how the weights were arrived at, and who fell out. See the backend's
 *  `weight_basis` — the names it lists are NOT in the index at any weight. */
export type WeightBasis = {
  members: number; weighted: number; excluded: { name: string | null; reason: string }[];
};
export type Resp = { years: string[]; rows: Row[]; holdings: number; weight_basis?: WeightBasis };

/**
 * Is this column an analyst FORECAST rather than a reported period? The `e` suffix is the
 * backend's (`2026e`), chosen so a consensus can never be merged into the year it forecasts —
 * an off-calendar filer can have both a filed FY2026 and a FY2026 estimate.
 */
export const isEstimatePeriod = (p: string) => p.endsWith('e');

/**
 * Period order: reported, then `LTM`, then the forecast years — the client twin of the backend's
 * `_period_sort_key`.
 *
 * ⚠⚠ A PLAIN SORT IS WRONG IN A WAY THAT LOOKS LIKE DATA. `'LTM' > '2026e'` lexically, so the
 * trailing twelve months — the newest thing actually known — would sit AFTER five forecast years.
 * That is not only a column order: the per-row sorts feed the Rebased base (the FIRST period) and
 * the YoY comparison (the PREVIOUS period), so an estimate would become the thing a reported year
 * is measured against.
 */
export const periodOrder = (a: string, b: string) => {
  const rank = (p: string) => (p === 'LTM' ? 1 : isEstimatePeriod(p) ? 2 : 0);
  return rank(a) - rank(b) || a.localeCompare(b);
};

/** Everything one payload's line is made of — the weighted level series, the per-period
 *  denominators, and the reasons a row is not in it. */
export type Blend = ReturnType<typeof buildBlend>;

/**
 * The weighted line for ONE payload, plus the per-cell arithmetic behind it.
 *
 * Pure: same payload in, same answer out, no React. See the module header for why it is out here.
 */
/**
 * ⚠⚠ METRICS DRAWN FROM MEMBERS POSITIVE IN EVERY PERIOD — the client twin of
 * `earnings._POSITIVE_ONLY_METRICS`, and it MUST stay one.
 *
 * The chart's line is computed on the server; this footer reproduces it from the same rows. A rule
 * applied there and not here means the drill-down explains a line it cannot reach — which this file
 * has already done once (it dropped rows `_prepare` KEEPS, and the comment claimed they matched),
 * so the failure mode is documented rather than hypothetical.
 */
const POSITIVE_ONLY_METRICS = new Set(['fcf_ps']);

export function buildBlend(data: Resp, metric?: string) {
    /**
     * ⚠ THE SAME FILTER THE SERVER APPLIES, over the same window. A member with any negative value
     * is out of the line entirely — see `POSITIVE_ONLY_METRICS`. It stays in `data.rows`, so the
     * table still LISTS it; what it loses is its vote in the footer.
     */
    const eligible = (r: Row): boolean => {
      if (!metric || !POSITIVE_ONLY_METRICS.has(metric)) return true;
      // ⚠ `revenue` IS THE FIELD NAME FOR "THE SERIES", whatever metric it holds.
      const vals = Object.values(r.revenue ?? {}).filter((v): v is number => v != null);
      return vals.length > 0 && vals.every((v) => v >= 0);
    };
    /**
     * This row's weight IN THIS PERIOD — the mirror of the backend's `_fundamental_blend
     * ._weight_at`, and it has to stay one because the footer below reproduces the plotted line
     * the server computed. An index weights by the cap it HAD in that period; a portfolio has no
     * cap history, so its single holding weight applies to every period. The absence of
     * `market_cap_by_period` is the signal for the second case.
     *
     * Null (never 0) when an index constituent has no cap that period: it is left out of that
     * period's average entirely, numerator and denominator both.
     */
    const wAt = (r: Row, y: string): number | null => {
      const per = r.market_cap_by_period;
      if (per) {
        const v = per[y];
        if (v && v > 0) return v;
        // ⚠ AS-OF, mirroring `_weight_at` and `marginData.weightAt`. A cap is a stock: the last
        // one filed stands until a newer one exists.
        const earlier = Object.keys(per).filter((k) => k <= y && per[k] > 0);
        return earlier.length ? per[earlier.reduce((a, b) => (a > b ? a : b))] : null;
      }
      const w = r.market_cap_eur ?? r.weight_pct;
      return w && w > 0 ? w : null;
    };
    /**
     * Why a row contributes NOTHING to the line — row-level, so it holds for every period.
     *
     * ⚠⚠ THIS EXISTS BECAUSE THE ABSENCE LOOKED LIKE A BUG. Measured on AITopSelectie OFF FX:
     * Advanced Micro Devices is a 5% holding whose FCF/share the table happily lists, and whose
     * weight line was simply blank in every period. The reason is real and one line up — its first
     * reported period (2015) is **−0.411**, and a LEVEL series is rebased to 100 at its own first
     * point, so `100 × v ÷ −0.411` inverts every later point: AMD's 2020 `+0.644` would plot as
     * −157, a collapse that exists only in the arithmetic. `_prepare` drops it for exactly this
     * (`non_positive_base`) and the blend never sees it.
     *
     * That is the right maths and it was silent, which is the one thing this table must never be:
     * a blank a reader cannot account for gets read as a broken cell, and the next move is to go
     * re-ingest data that is already there.
     */
    const excluded = new Map<Row, string>();
    const parts: { r: Row; idx: Record<string, number> }[] = [];
    // ⚠ KEYED ON THE ROW OBJECT, NOT ON THE ISIN. A payload can carry the same ISIN twice (a model
    // listing one instrument at two weights — VTopSelectie holds CapitaLand at 2% and 3%), and an
    // ISIN key would give both rows the first one's weight. `rows` below is a sort of these same
    // objects, so identity is stable for the render.
    const partOf = new Map<Row, { r: Row; idx: Record<string, number> }>();
    /**
     * ⚠⚠ COVERAGE IS MEASURED ON THE **STABLE** WEIGHT, NOT THE PER-PERIOD CAP — mirroring
     * `_fundamental_blend.blend_series`, and it is the difference between the floor working
     * and doing nothing at all.
     *
     * The per-period cap comes out of the same GuruFocus blob as the figure, so a company that has
     * not filed FY2026 has no FY2026 cap either. Measuring coverage with it divides the filers by
     * the filers and reads ~100% in exactly the period where almost nobody has reported — which is
     * how FY2026 came to draw a full-height point made almost entirely of NVIDIA.
     *
     * Measured on the S&P: FY2026 is 13.4% covered on this basis and was reading 100.0% on the
     * per-period one.
     */
    const coverW: Record<string, number> = {};
    let coverTotal = 0;
    const stableW = (r: Row): number => {
      const w = r.market_cap_eur ?? r.weight_pct;
      return w && w > 0 ? w : 0;
    };
    for (const r of data.rows) {
      // ⚠⚠ BEFORE THE DENOMINATOR, UNLIKE THE BASE TEST BELOW — and the difference is real. The
      // base test mirrors `_prepare`, which runs INSIDE `blend_series` on members it was already
      // handed, so its drops belong in the coverage denominator. This filter runs one level up, in
      // `_blend_rows`, BEFORE `blend_series` is called at all: an excluded member was never handed
      // over, so counting it here would report a coverage the server never computed.
      if (!eligible(r)) continue;
      const periods = Object.keys(r.revenue).filter((p) => r.revenue[p] != null).sort(periodOrder);
      if (!periods.length) {
        // Nothing filed at all — the row already says so via `status`, so no second badge.
        continue;
      }
      // ⚠ COUNTED IN THE DENOMINATOR **BEFORE** THE BASE TEST, because that is the order
      // `blend_series` uses: it takes the total over every member handed to it, and `_prepare`
      // drops the non-positive bases afterwards. Filtering first would shrink the denominator,
      // lift every coverage figure, and let a period slip over the floor that the chart omits.
      coverTotal += stableW(r);
      /**
       * ⚠⚠ THE FIRST **POSITIVE** PERIOD, NOT THE FIRST REPORTED ONE — and this line used to claim
       * in a comment that it matched `_prepare` while doing something stricter (2026-08-25). The
       * server skips forward to the first positive figure and keeps the member; this dropped it
       * outright, so any company whose earliest year happens to be negative was in the CHART and
       * missing from the drill-down that explains it — a footer that cannot reach the line above
       * it, and a Contribution column silently short by that company.
       *
       * ⚠ A leading zero or negative on a flow line is usually not a measurement. GuruFocus
       * back-fills the years before a company existed: Universal Music sits inside Vivendi until
       * the 2021 spin-off and its 2017 revenue is stored as `0`. Anchoring on that throws away
       * every good year after it; skipping to the first positive period starts the curve where
       * the company's history really starts.
       */
      const basePeriod = periods.find((p) => (r.revenue[p] as number) > 0);
      if (basePeriod == null) {
        excluded.set(r, 'it never reports a positive figure for this metric, and a level series is '
          + 'indexed to 100 at its own first point — there is no base to divide by. The figures '
          + 'below are still this company’s; only the blended line leaves it out.');
        continue;
      }
      const base = r.revenue[basePeriod] as number;
      // ⚠ AND ITS PRE-BASE PERIODS GO WITH IT, exactly as `_prepare` truncates them. A zero before
      // the anchor would rebase to 0 and read as a company that lost everything rather than one
      // that had not started. ⚠ The EUROS are not truncated — see the `fund` fill below, which is
      // the whole reason it is a separate pass.
      const idx: Record<string, number> = {};
      for (const p of periods.slice(periods.indexOf(basePeriod))) {
        idx[p] = 100 * (r.revenue[p] as number) / base;
      }
      const part = { r, idx };
      parts.push(part);
      partOf.set(r, part);
    }
    const level: Record<string, { value: number; covered: number }> = {};
    // ⚠⚠ THE DENOMINATOR IN FORCE FOR EACH PERIOD, AND IT IS WHY A PER-YEAR WEIGHT EXISTS AT ALL.
    // Two things move it: the constituents that REPORTED that period, and — now that the basis is
    // the period's own market cap — what each of them was worth at the time. NVIDIA is 0.63% of
    // FY2018 and 7.46% by today's cap; only the first is a fact about 2018.
    const denom: Record<string, number> = {};
    const coverN: Record<string, number> = {};
    /**
     * ⚠⚠ EACH ROW'S LATEST FIGURE STANDS UNTIL IT REPORTS AGAIN — the client twin of
     * `_fundamental_blend.carry_forward`, and the reason this table's figures reconcile with the
     * line above it. Without the carry a semi-annual filer simply left Q1/Q3, the contributor set
     * alternated, and the index sawtoothed ±20% on composition alone.
     *
     * ⚠ A CARRIED VALUE IS NOT COVERAGE. `coverW`/`coverN` count only the periods a row actually
     * reported, so the floor still sees the newest period for what it is.
     *
     * ⚠ BOUNDED to ~a year (in periods: 4 quarters or 1 year), so a holding that stops reporting
     * falls out rather than being held flat for the rest of the axis.
     */
    const isQuarterly = data.years.some((y) => y.includes('-Q'));
    const maxCarry = isQuarterly ? 4 : 1;
    /**
     * ⚠⚠ WHICH PERIOD EACH ROW'S FIGURE CAME FROM — `{}` for its own, the source period when it was
     * carried. Without this the weight column CANNOT sum to 100%: a carried row is in the
     * denominator (its figure is in the average) but showed no weight, so the shares silently added
     * to less than the whole. The Total row totals that column, so the gap would have been visible
     * as a number that is supposed to be a constant and isn't.
     */
    const from: Record<string, Record<string, string>> = {};
    /**
     * ⚠⚠ COVERAGE IS COUNTED FIRST, IN ITS OWN PASS, SO THE CARRY CAN BE GATED ON IT. A carried
     * figure exists to hold the basket still in a period the chart DRAWS — at AEX Q1/Q3 only twelve
     * of twenty-two constituents file, and without it the index alternates between two baskets and
     * sawtooths ±20% on composition alone. In a period the chart REFUSES it does nothing at all:
     * FY2026 has one reporter, the floor rejects it, and twenty-one companies were showing their
     * 2025 figure in a column that feeds no line. That reads as a projection, which it is not.
     *
     * Counting coverage here is what makes the gate possible: it is computed from the OWN values
     * only (a carried figure is never coverage), so it does not depend on the carry it decides.
     */
    for (const p of parts) {
      for (const y of data.years) {
        if (p.idx[y] == null || !wAt(p.r, y)) continue;
        coverW[y] = (coverW[y] ?? 0) + stableW(p.r);
        coverN[y] = (coverN[y] ?? 0) + 1;
      }
    }
    const drawn = (y: string) =>
      (100 * (coverW[y] ?? 0) / (coverTotal || 1)) >= MIN_YEAR_COVERAGE_PCT
      && (100 * (coverN[y] ?? 0) / (parts.length || 1)) >= MIN_YEAR_COVERAGE_PCT;
    /** Each part's value at each period it contributed to — own or carried. The chaining below
     *  takes ratios between periods that need not be adjacent, so the values have to be kept. */
    const at = new Map<typeof parts[number], Record<string, number>>();
    /**
     * ⚠⚠ THE EUROS, CARRIED ON THEIR OWN CLOCK — the client twin of the second `carry_forward` in
     * `_fundamental_blend.blend_series`. A row's euros are a filing like its value, so a
     * semi-annual filer's trailing figure stands in the quarters it does not file; uncarried it
     * would drop out of them, the per-step intersection would shrink to the quarterly filers, and
     * the aggregate would sawtooth on composition alone.
     *
     * ⚠ ITS OWN `lastF`/`sinceF`, NOT THE VALUE'S. A row can have a value at a period and no
     * euros there (no share count, no FX rate for that date), and reusing the value's carry state
     * would then look the euros up at a period that has none and silently drop the row from the
     * step. Same bound, same gate, separate clock — which is what the server does by calling
     * `carry_forward` twice.
     */
    const fund = new Map<typeof parts[number], Record<string, number>>();
    for (const p of parts) {
      let last: { idx: number; y: string } | null = null;
      let since = 0;
      let lastF: { v: number; y: string } | null = null;
      let sinceF = 0;
      at.set(p, {});
      fund.set(p, {});
      for (const y of data.years) {
        const own = p.idx[y];
        /**
         * ⚠⚠ THE CARRY MUST NOT CROSS BETWEEN REPORTED AND FORECAST PERIODS. This walks the union
         * axis — actuals, then `LTM`, then the `…e` columns — so without this reset a company's
         * newest REPORTED figure is carried straight into the forecast columns: it takes a weight
         * there, joins the footer's blended `2026e`, and renders in carried italics. Measured:
         * NVIDIA (a January filer, FY2026 already closed) showed a weight and italics under
         * `2026e` while KLA, whose last actual sits a different distance from the column, showed
         * neither — two rows treated differently by an accident of fiscal calendar.
         *
         * It is the same error the server had (`_drop_superseded_forecasts`) and the reason is the
         * same: a reported number is not a forecast, and carrying one into a forecast period puts
         * a known figure into a line that claims to be an expectation. Carrying WITHIN the forecast
         * block (2026e → 2027e) is untouched — that is a forecast holding until the next one, which
         * is what the server's own `carry_forward` does.
         */
        if (last && isEstimatePeriod(y) !== isEstimatePeriod(last.y)) { last = null; since = 0; }
        if (own != null) { last = { idx: own, y }; since = 0; } else if (last) { since += 1; }
        /**
         * ⚠⚠ THE EUROS' CARRY CLOCK ADVANCES HERE, BEFORE ANY `continue` BELOW — otherwise a
         * period this row is not in at all (no value, or no cap) would not age the carry, and the
         * euros would be held further than the server holds them. On the server the two carries
         * are two independent `carry_forward` passes over the whole axis, so neither can be
         * shortened by the other's gaps; this is that, unrolled.
         *
         * ⚠ Same forecast reset, same reason: a filed figure must not be carried into a column
         * that claims to be a forecast.
         */
        const ownF = p.r.fund_by_period?.[y];
        if (lastF && isEstimatePeriod(y) !== isEstimatePeriod(lastF.y)) { lastF = null; sinceF = 0; }
        if (ownF != null) { lastF = { v: ownF, y }; sinceF = 0; } else if (lastF) { sinceF += 1; }
        const w = wAt(p.r, y);
        /**
         * ⚠⚠ THE EUROS ARE WRITTEN BEFORE THE VALUE'S GATE, ON THE WEIGHT ALONE — the client twin
         * of the separate `fund` loop in `blend_series`. A sum never divides a member by itself,
         * so none of the rebase's preconditions apply to it, and hanging the euros off `v` would
         * make a member's presence in the SUM depend on whether its rebased LEVEL exists. It is
         * the same decoupling the server needed and for the same case.
         */
        const fv = ownF ?? (lastF && sinceF <= maxCarry && drawn(y) ? lastF.v : null);
        if (fv != null && w) fund.get(p)![y] = fv;
        // ⚠ ONLY INTO A PERIOD THE CHART DRAWS — see the ⚠⚠ on `drawn`. Elsewhere a carried figure
        // holds up nothing and reads as a projection.
        const carried = own == null && last && since <= maxCarry && drawn(y) ? last : null;
        const v = own ?? carried?.idx ?? null;
        if (v == null) continue;
        if (!w) continue;                     // no cap on or before this period ⇒ out of it
        denom[y] = (denom[y] ?? 0) + w;
        at.get(p)![y] = v;
        if (carried) (from[p.r.isin] ??= {})[y] = carried.y;
      }
    }
    /**
     * ⚠⚠ THE LINE IS CHAINED FROM WEIGHTED GROWTH, NOT AVERAGED FROM REBASED LEVELS — the client
     * twin of `_fundamental_blend.blend_series`'s level path, and the Total row must equal what the
     * chart draws or this table explains a number that is not on it.
     *
     *     index[p] = index[anchor] × (1 + Σ w·g / Σ w),   g = value(p)/value(anchor) − 1
     *
     * Averaging rebased levels makes the line an artefact of WHEN each member's history starts:
     * every member is 100 at its own first period, so a constituent joining the panel drags the
     * average toward 100 and the index "moves" on composition alone. Measured on the AEX annual
     * revenue line, that drew a 388 → 285 crash into 2023 that no constituent experienced.
     *
     * ⚠ THE ANCHOR IS THE LAST DRAWN PERIOD, not the previous one: a period under the floor is not
     * drawn, and measuring the next step from it would compound a move nobody could see.
     */
    /** ⚠ ONCE PER ROW, NOT ONCE PER STEP — the same figure at every interval, and the loop below
     *  asks for it periods x rows times. Mirrors where the backend computes it. */
    const scaleOf = new Map<typeof parts[number], number>(
      parts.map((p) => [p, memberScale(Object.values(at.get(p) ?? {}))]));
    /**
     * ⚠⚠ THE LINE'S OWN MOVE, DECOMPOSED BY MEMBER, IN PERCENTAGE POINTS OF THAT MOVE — and it is
     * computed HERE, inside the loop that chains the line, for the reason everything else in this
     * file is: `pp_i = 100 · w_i·g_i ÷ Σw` sums to `100 · Σw·g ÷ Σw`, which IS the step the next
     * line multiplies into the index. So the column adds up to the footer exactly, and "who moved
     * this line" stops being a guess read off two adjacent figures.
     *
     * ⚠⚠ THE DENOMINATOR IS **NEITHER** `denom[y]` NOR THE TABLE'S WEIGHT — it is Σw over the
     * members that SPAN THIS INTERVAL. A row present at `y` but absent (or refused: a non-positive
     * anchor, an immaterial base, an implausible step — see `stepGrowth`) at the anchor sits in
     * `denom[y]` and is not in this move at all. Dividing by `denom[y]` would scale every
     * contribution down by the weight of whoever could not be measured over the interval, and the
     * column would land short of the footer by exactly that — a decomposition that does not add up
     * to its own total is not one, it is a pile of plausible numbers.
     *
     * ⚠ `from` IS PART OF THE ANSWER, NOT METADATA. The anchor is the last DRAWN period, which is
     * the previous column only while every column is drawn; under the coverage floor the move spans
     * two years (or five quarters), and a pp figure whose interval is unstated reads as one period's
     * contribution. `spanPct` is how much of the period's line weight the decomposition covers.
     *
     * ⚠ KEYED ON THE ROW OBJECT, NOT THE ISIN — the same rule as `partOf` above: one payload can
     * carry an ISIN twice at two weights, and an ISIN key hands both rows the first one's figures.
     */
    const step: Record<string, { from: string; growthPct: number; spanPct: number }> = {};
    /**
     * ⚠⚠ BOTH FACTORS, NOT JUST THE PRODUCT — `pp === sharePct × growthPct ÷ 100`, exactly, so the
     * cell can show a reader the multiplication it is looking at instead of asserting a number.
     *
     * ⚠ `sharePct` IS NOT THE WEIGHT THE CELL PRINTS UNDER IT, and since 2026-08-21 it is not even
     * measured at the same period. The printed weight divides this period's cap by `denom[y]` — the
     * composition of the index NOW, which is what a weight column should say. This one is the
     * member's share of the weight AT THE ANCHOR, because that is the denominator the contribution
     * was actually taken over (see the ⚠⚠ on `w` above). Quoting the printed weight as the factor
     * gives a multiplication that does not reach the pp.
     */
    const contrib = new Map<Row,
      Record<string, { pp: number; growthPct: number | null; sharePct: number | null }>>();
    /**
     * ⚠⚠ WHICH CONSTRUCTION THIS IS — and it decides the line AND its decomposition together.
     * `true` when any row shipped `fund_by_period`, matching the server's
     * `any(p["fund"] for p in prepared)`. Splitting the two would put a chart drawn one way over a
     * table that decomposes the other, and the table is the thing people check.
     */
    const aggregate = parts.some((p) => Object.keys(fund.get(p) ?? {}).length > 0);
    let anchor: string | null = null;
    let chained = 100;
    for (const y of data.years) {
      if (!denom[y] || !drawn(y)) continue;
      if (aggregate && anchor != null) {
        /**
         * ⚠⚠ GROWTH OF A SUM, AND ITS DECOMPOSITION IS AN IDENTITY RATHER THAN AN APPROXIMATION:
         *
         *     G   = ΣFᵢ(y)/ΣFᵢ(a) − 1 = Σ(Fᵢ(y) − Fᵢ(a)) / ΣFᵢ(a)
         *     ppᵢ = 100 · (Fᵢ(y) − Fᵢ(a)) / ΣFᵢ(a)        so   Σppᵢ = 100·G, exactly
         *
         * The column adds to the footer because the algebra says so, not because it nearly does —
         * and no row is dropped for crossing zero: −200 → +300 contributes +500/ΣFᵢ(a), cleanly.
         *
         * ⚠ `sharePct × growthPct ÷ 100 = pp` still holds and is still what the cell shows, but
         * only where `Fᵢ(a) > 0`. Below zero there is no rate and no share of a negative base to
         * quote; the pp is exact regardless, so both factors go null and the pp stands alone.
         *
         * ⚠⚠ INTERSECTED PER STEP. A sum changes when its members change, so `Σ(everyone at y)`
         * over `Σ(everyone at a)` would report composition as growth. Only rows with euros at
         * BOTH ends are in it — the same discipline the growth path gets for free, since a row
         * that cannot span a step has no `g`.
         */
        const a = anchor;                     // narrowed for the closures below
        const spanning = parts.filter(
          (p) => fund.get(p)?.[a] != null && fund.get(p)?.[y] != null);
        const sA = spanning.reduce((s, p) => s + fund.get(p)![a], 0);
        const sD = spanning.reduce((s, p) => s + fund.get(p)![y], 0);
        // ⚠ NO RATIO WITHOUT A POSITIVE BASE, and no line past a non-positive numerator — the same
        // two guards, and for the same reasons, as the server's aggregate branch. A negative
        // aggregate makes every later point a sign-flipped value a log axis cannot draw, so the
        // series STOPS (visible) rather than continuing into points that vanish (not).
        if (!spanning.length || sA <= 0) { anchor = y; continue; }
        if (sD <= 0) break;
        chained *= sD / sA;
        step[y] = {
          from: a,
          growthPct: 100 * (sD / sA - 1),
          // ⚠ COVERAGE IN THE UNIT THE LINE IS BUILT FROM — what share of this period's euros the
          // decomposition speaks for. The growth path asks the same question in weight; asking it
          // in weight here would mix two bases and could exceed 100%.
          spanPct: 100 * sD / (parts.reduce(
            (s, p) => s + (fund.get(p)?.[y] ?? 0), 0) || sD),
        };
        for (const p of spanning) {
          const base = fund.get(p)![a];
          const now = fund.get(p)![y];
          const byPeriod = contrib.get(p.r) ?? {};
          byPeriod[y] = {
            pp: 100 * (now - base) / sA,
            growthPct: base > 0 ? 100 * (now / base - 1) : null,
            sharePct: base > 0 ? 100 * base / sA : null,
          };
          contrib.set(p.r, byPeriod);
        }
        anchor = y;
        level[y] = { value: chained, covered: 100 * (coverW[y] ?? 0) / (coverTotal || 1) };
        continue;
      }
      if (aggregate) {
        // The first drawn period IS the base — the same rule as the growth chain below.
        anchor = y;
        level[y] = { value: chained, covered: 100 * (coverW[y] ?? 0) / (coverTotal || 1) };
        continue;
      }
      if (anchor != null) {
        let num = 0;
        let den = 0;
        let spanAtY = 0;
        /**
         * ⚠ HELD, NOT WRITTEN STRAIGHT INTO `contrib`. Each term's share is over the FINAL `den`,
         * which is not known until every part has been asked — and both guards below can still
         * discard the whole step, so writing as we go would leave contributions behind for a period
         * that ends up with no line point for them to be a share of.
         */
        const terms: { r: Row; w: number; g: number }[] = [];
        for (const p of parts) {
          /**
           * ⚠⚠ THE WEIGHT IS THE **ANCHOR'S**, NOT THIS PERIOD'S — the client twin of the ⚠⚠ in
           * `_fundamental_blend.blend_series`, and it was worth 9 percentage points a year
           * (2026-08-21). `g` spans anchor -> y, so weighting it by the cap at `y` weights each
           * constituent's growth by a number that already contains that growth (cap = price x
           * shares). Measured on ACWI's share price 2015->2025: +20.21%/yr end-weighted against
           * +11.14%/yr anchor-weighted, where the index really did ~10-11%.
           *
           * ⚠ IT MUST MATCH THE SERVER EXACTLY. This function exists to reproduce the plotted line
           * in the drill-down's footer; weighted differently it would print a `Rebased` total that
           * disagrees with the chart it was opened from, and both would look reasonable.
           */
          const w = wAt(p.r, anchor);
          // ⚠ THE SHARED RULE, NOT AN INLINE `prev > 0`. That guard caught zero and missed the
          // near-zero base — which is what let one holding drive an index through zero and take
          // most of the line off a log axis with it. See `stepGrowth`.
          const g = stepGrowth(at.get(p)?.[anchor], at.get(p)?.[y], scaleOf.get(p) ?? 0);
          if (!w || g == null) continue;
          num += w * g;
          den += w;
          // ⚠⚠ THE SPANNING MEMBERS' WEIGHT **AT THIS PERIOD**, kept alongside the anchor-weighted
          // `den` and used for `spanPct` alone. Those are two different questions and they need
          // two different bases: the MOVE is weighted at the anchor (see above — it was worth 9pp
          // a year), while COVERAGE asks what share of the line drawn at `y` the decomposition
          // speaks for, and the line at `y` is composed of this period's weights. Dividing the
          // anchor-weighted `den` by either period's total mixes the two and can exceed 100%.
          spanAtY += wAt(p.r, y) ?? 0;
          terms.push({ r: p.r, w, g });
        }
        if (den <= 0) continue;               // nothing spans this interval — no honest move
        // ⚠ EVERY ROW WIPED OUT. `stepGrowth` floors each at −100%, so this is an exact −1 and the
        // index is 0 from here on — values a log axis cannot draw. It stops rather than emitting
        // points that would vanish silently, which is the failure this whole rule exists to end.
        if (1 + num / den <= 0) break;
        chained *= 1 + num / den;
        step[y] = { from: anchor,
                    growthPct: 100 * num / den,
                    // ⚠⚠ COVERAGE OF **THIS PERIOD'S** LINE, and it is `spanAtY`, not `den`. The
                    // move is anchor-weighted (rightly); coverage asks a different question — what
                    // share of the line drawn at `y` this decomposition speaks for — and the line
                    // at `y` is made of this period's weights. So both sides of this ratio are
                    // period-`y` weights and it is a genuine subset share: it cannot exceed 100%,
                    // and it matches what the tooltip beside it claims ("of this period's weight
                    // that spans the interval").
                    //
                    // ⚠ I SHIPPED `den / denom[anchor]` HERE while fixing the anchor weighting and
                    // it was wrong twice: it answers "how much of the ANCHOR's weight survived",
                    // which is not the sentence next to it, and on the pinned case it read 100%
                    // where half the period's weight could not be measured over the interval.
                    spanPct: 100 * spanAtY / (denom[y] || 1) };
        for (const t of terms) {
          const byPeriod = contrib.get(t.r) ?? {};
          // ⚠ A ZERO HERE IS A MEASUREMENT, NOT AN ABSENCE — the opposite of the weight line's rule,
          // and the two sit in the same cell. This member was in the move and did not move (a
          // carried figure is the common case: same value at both ends, so `g` is exactly 0). "Not
          // in this step" is the MISSING key, which the reader sees as a dash.
          byPeriod[y] = { pp: 100 * t.w * t.g / den,
                          growthPct: 100 * t.g,
                          sharePct: 100 * t.w / den };
          contrib.set(t.r, byPeriod);
        }
      }
      anchor = y;
      level[y] = { value: chained, covered: 100 * (coverW[y] ?? 0) / (coverTotal || 1) };
    }
    // The Total row's own check: Σ of the shares the cells above it display. 100.00% by
    // construction — each is `wAt ÷ denom` and `denom` is their sum — so anything else is drift,
    // which is exactly why it is worth printing.
    const weightSum: Record<string, number> = {};
    for (const y of Object.keys(denom)) weightSum[y] = 100;
    return { level, denom, partOf, wAt, excluded, from, weightSum, step, contrib,
             contributors: parts.length,
             coveredNames: Object.fromEntries(data.years.map(
               (y) => [y, 100 * (coverN[y] ?? 0) / (parts.length || 1)])) };
}
