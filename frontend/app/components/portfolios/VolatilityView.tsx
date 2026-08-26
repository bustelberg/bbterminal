'use client';

/**
 * VOLATILITY — the Risk panel's fourth view. `σ_p = √(Σ(Rₜ − R̄)²/(T−1)) · √f`.
 *
 * The same construction as the tracking error, on the book's OWN returns rather than on the
 * difference — and the same `σ_p` the correlation view puts inside `σₐ² = σₚ² + σᵦ² − 2ρσₚσᵦ`. One
 * series, one function; two figures a click apart that disagreed would tell the reader that one of
 * them is wrong and nothing about which.
 *
 * ⚠⚠ NO CASH-FLOW CONTAMINATION, BY CONSTRUCTION RATHER THAN BY CHAIN-LINKING. The usual hazard is
 * measuring risk off an ACCOUNT VALUE: a deposit reads as a huge gain and a withdrawal as a crash,
 * so a book that merely received money looks turbulent. Time-weighted returns exist to strip that
 * out. This series never has flows in it — it is a weighted basket of instrument price returns, so
 * money moving in or out of the account changes nothing in it.
 *
 * ⚠ THE PRICE IS THE OTHER CAVEAT AND THE PANEL STATES IT: today's weights, carried backwards. So
 * this is the volatility of the portfolio AS IT STANDS, not the one the client lived through.
 *
 * ⚠ DOWNSIDE DEVIATION IS SORTINO'S, not the semi-deviation — divided by ALL n, against a target of
 * 0. Both are called "downside deviation"; this is the one the Sortino beside it is built on, so
 * the ratio equals its own parts.
 */
import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { v } from '../../../lib/dynamicValue';
import { dayOf } from './asOfLine';
import { sourceField, sourceLabel, sourceVendor, type SourceKey } from '../../../lib/provenance';
import { traceError } from '../../../lib/debugTrace';
import { withWorked, subNum } from './workedFormula';
import type { PortfolioVolatility } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';

const FREQS = [
  { key: 'weekly', label: 'Weekly' },
  { key: 'monthly', label: 'Monthly' },
  { key: 'daily', label: 'Daily' },
] as const;

const pct2 = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);
const signed2 = (v: number | null | undefined) =>
  (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`);

/**
 * EVERY SYMBOL THIS VIEW USES, DEFINED ONCE — same rule as the tracking-error and correlation
 * views. `Rₜ`, `T` and `f` appear on four of the seven cards between them.
 */
const LEGEND = {
  R: (bookName: string) => `${v(bookName)}'s return in period t, in EUR`,
  RBar: 'the mean return over the window — subtracted so σ measures spread, not level',
  T: (n: number | null | undefined) => `the number of periods (${v(n)} here)`,
  f: (n: number | null | undefined) => `periods per year (${v(n)}), the annualisation factor`,
  sigma: 'the answer: one standard deviation of the return, per year',
  // ⚠ THE min(·,0) IS THE WHOLE DIFFERENCE FROM σ. Every up period contributes exactly zero, so
  // this is a spread of losses only — and the divisor is still ALL periods, which is Sortino's
  // convention rather than the semi-deviation's.
  minR: 'every gain replaced by zero, so only the losing periods contribute anything',
  sigmaD: 'the answer: the spread of the losses alone, per year',
  prod: 'the periods CHAINED — what the sleeve actually compounded to, not the average of its steps',
  Rann: 'the annualised return from the tile beside this one',
  rf: 'the risk-free rate the return is measured above — ⚠ stated because a Sharpe quoted without '
    + "one is not comparable with anybody else's",
  sharpe: 'the answer: return per unit of TOTAL volatility, up and down alike',
  sortino: 'the answer: the same return per unit of DOWNSIDE only',
};

export default function VolatilityView({
  holdings, benchmark, portfolioName, portfolioAsOf, portfolioFetchedAt, portfolioSource,
}: {
  holdings: ActiveShareHolding[];
  benchmark: string;
  /** The book's identity, forwarded from the panel — see `ActiveSharePanel`'s own props. */
  portfolioName: string;
  portfolioAsOf?: string | null;
  portfolioFetchedAt?: string | null;
  portfolioSource: SourceKey;
}) {
  const [data, setData] = useState<PortfolioVolatility | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [freq, setFreq] = useState<'daily' | 'weekly' | 'monthly'>('weekly');

  const key = `${benchmark}|${freq}|${holdings.length}`
    + `|${holdings.reduce((s, h) => s + h.weight_pct, 0).toFixed(4)}`;

  useEffect(() => {
    let cancelled = false;
    setData(null);
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/portfolio/volatility`
          + `?benchmark=${encodeURIComponent(benchmark)}&frequency=${freq}`,
          { method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ holdings }) });
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setError(null);
        setData(b as PortfolioVolatility);
      } catch (e) {
        traceError('volatility', 'the volatility could not be computed', e);
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  const period = data?.frequency === 'daily' ? 'day'
    : data?.frequency === 'monthly' ? 'month' : 'week';

  /**
   * WHAT EVERY CARD HERE IS MEASURED FROM, AND OVER WHAT WINDOW — built once, seven cards.
   *
   * ⚠ THE SAME TWO STRINGS THE TRACKING-ERROR AND CORRELATION VIEWS BUILD. All three read one
   * `build_paired_series`; three separately-worded Wheres would be three places for the sources
   * to drift apart while the numbers stayed identical.
   */
  const where = data?.available
    ? `${v(data.observations)} ${v(data.frequency)} returns, prices from ${v(sourceField('yfinance'))} `
      + `at ${v(sourceVendor('yfinance'))}, weights from ${v(sourceLabel(portfolioSource))}, `
      + `against ${v(data.benchmark)}'s tracker.`
    : '';

  /** ⚠ TWO CLOCKS — a price window and a weights date. See the ⚠⚠ in `TrackingErrorView`. */
  const when = data?.available
    ? `Returns: ${v(data.window_from ?? 'no recorded start')} to `
      + `${v(data.window_to ?? 'no recorded end')} (${v(data.observations)} periods)\n`
      + `${v(portfolioName)} weights: ${v(dayOf(portfolioAsOf) ?? 'no recorded date')}`
      + `${dayOf(portfolioFetchedAt) && dayOf(portfolioFetchedAt) !== dayOf(portfolioAsOf)
        ? ` (read ${v(dayOf(portfolioFetchedAt))})` : ''}`
    : '';

  /** A ratio, or a dash. ⚠ A dash is a MEASUREMENT here — see the Sortino note. */
  const num2 = (n: number | null | undefined) => (n == null ? '—' : n.toFixed(2));

  /**
   * ONE ENTRY PER MEASURE, EACH CARRYING BOTH SIDES.
   *
   * ⚠⚠ THE PAIRING LIVES IN THE DATA, NOT IN THE LAYOUT. Built as twelve separate tiles, "which
   * ACWI number goes with which of ours" was a question the reader answered by reading labels; here
   * a measure is one object with an `own` and a `bench`, so the two cannot be rendered apart or get
   * out of order. Adding a seventh measure is one entry, in one place, and it arrives in both rows.
   */
  const MEASURES = data?.available ? [
    {
      key: 'vol',
      label: 'Volatility',
      own: pct2(data.volatility_pct),
      bench: pct2(data.benchmark_volatility_pct),
      info: <InfoTip className="ml-0.5" content={<AspectCard
        what={`How much ${portfolioName}'s return has varied, period to period. The ${data.benchmark} `
          + 'row is the identical calculation over the tracker\'s own series.'}
        where={where}
        when={when}
        worked={data.volatility_pct == null ? '' : withWorked(
          String.raw`\sigma = \sqrt{\dfrac{\sum_t (R_t - \bar{R})^2}{T - 1}}\;\sqrt{f}`,
          String.raw`T = ${data.observations},\; f = ${data.periods_per_year}`
          + String.raw` \;\Rightarrow\; ${subNum(data.volatility_pct, 2)}\%`)}
        legend={[
          { sym: String.raw`R_t`, is: LEGEND.R(portfolioName) },
          { sym: String.raw`\bar{R}`, is: LEGEND.RBar },
          { sym: 'T', is: LEGEND.T(data.observations) },
          { sym: 'f', is: LEGEND.f(data.periods_per_year) },
          { sym: String.raw`\sigma`, is: LEGEND.sigma },
        ]} />} />,
    },
    {
      key: 'dd',
      label: 'Downside deviation',
      own: pct2(data.downside_dev_pct),
      bench: pct2(data.benchmark_downside_dev_pct),
      info: <InfoTip className="ml-0.5" content={<AspectCard
        what={'Volatility that charges only for losses. Every gain is replaced by zero before the '
          + 'spread is taken, so a losing period raises this and a winning one dilutes it — where '
          + 'the volatility column treats a +5% period exactly as hard as a −5% one.'}
        where={where}
        when={when}
        worked={data.downside_dev_pct == null ? '' : withWorked(
          String.raw`\sigma_d = \sqrt{\dfrac{\sum_t \min(R_t,\; 0)^2}{T}}\;\sqrt{f}`,
          String.raw`T = ${data.observations},\; f = ${data.periods_per_year}`
          + String.raw` \;\Rightarrow\; ${subNum(data.downside_dev_pct, 2)}\%`)}
        legend={[
          { sym: String.raw`\min(R_t,\; 0)`, is: LEGEND.minR },
          { sym: 'T', is: LEGEND.T(data.observations) },
          { sym: 'f', is: LEGEND.f(data.periods_per_year) },
          { sym: String.raw`\sigma_d`, is: LEGEND.sigmaD },
        ]} />} />,
    },
    {
      key: 'worst',
      label: `Worst ${period}`,
      own: signed2(data.worst_period_pct),
      ownTone: 'text-neg-300',
      bench: signed2(data.benchmark_worst_period_pct),
      info: <InfoTip className="ml-0.5" content={<AspectCard
        what={`The single worst ${period} in the window, for each side.`}
        where={`Best was ${v(signed2(data.best_period_pct))} against `
          + `${v(signed2(data.benchmark_best_period_pct))}; `
          + `${v(`${data.negative_periods_pct?.toFixed(2)}%`)} of ${period}s were negative against `
          + `${v(`${data.benchmark_negative_periods_pct?.toFixed(2)}%`)}.`}
        when={when}
        how={'⚠ NOBODY HAS EVER EXPERIENCED "18% ANNUALISED VOLATILITY". They have experienced the '
          + `worst ${period}. For a fat-tailed book the two are far apart, which is exactly when σ `
          + 'on its own misleads — so this column is the reality check on the first one.'} />} />,
    },
    {
      key: 'ret',
      label: 'Return',
      own: signed2(data.return_ann_pct),
      ownTone: (data.return_ann_pct ?? 0) >= 0 ? 'text-pos-300' : 'text-neg-300',
      bench: signed2(data.benchmark_return_ann_pct),
      info: <InfoTip className="ml-0.5" content={<AspectCard
        what={`What ${portfolioName} compounded at over the window, against what ${data.benchmark} did.`}
        where={where}
        when={when}
        worked={data.return_ann_pct == null ? '' : withWorked(
          String.raw`\left( \prod_t (1 + R_t) \right)^{f/T} - 1`,
          String.raw`T = ${data.observations},\; f = ${data.periods_per_year}`
          + String.raw` \;\Rightarrow\; ${subNum(data.return_ann_pct, 2)}\%`)}
        legend={[
          { sym: String.raw`\prod_t`, is: LEGEND.prod },
          { sym: String.raw`R_t`, is: LEGEND.R(portfolioName) },
          { sym: 'T', is: LEGEND.T(data.observations) },
          { sym: 'f', is: LEGEND.f(data.periods_per_year) },
        ]}
        how={'Here so the two ratios beside it can be checked — a risk number without the return '
          + 'it bought is half a sentence. ⚠ NOT the active return: that is this row minus the one '
          + 'below it only in the loosest sense, and the Tracking error view computes it properly.'} />} />,
    },
    {
      key: 'sharpe',
      label: 'Sharpe',
      own: num2(data.sharpe),
      bench: num2(data.benchmark_sharpe),
      info: <InfoTip className="ml-0.5" content={<AspectCard
        what="Return per unit of total volatility, up and down alike."
        where={where}
        when={when}
        worked={data.sharpe == null ? '' : withWorked(
          String.raw`\text{Sharpe} = \dfrac{R_{\text{ann}} - r_f}{\sigma}`,
          String.raw`\dfrac{${subNum(data.return_ann_pct ?? 0, 2)}\% - ${subNum(data.risk_free_pct ?? 0, 2)}\%}`
          + String.raw`{${subNum(data.volatility_pct ?? 0, 2)}\%}`
          + ` = ${data.sharpe.toFixed(2)}`)}
        legend={[
          { sym: String.raw`R_{\text{ann}}`, is: LEGEND.Rann },
          { sym: 'r_f', is: LEGEND.rf },
          { sym: String.raw`\sigma`, is: LEGEND.sigma },
          { sym: String.raw`\text{Sharpe}`, is: LEGEND.sharpe },
        ]} />} />,
    },
    {
      key: 'sortino',
      label: 'Sortino',
      own: num2(data.sortino),
      bench: num2(data.benchmark_sortino),
      info: <InfoTip className="ml-0.5" content={<AspectCard
        what="The same ratio, over downside deviation instead of total volatility."
        where={where}
        when={when}
        worked={data.sortino == null ? '' : withWorked(
          String.raw`\text{Sortino} = \dfrac{R_{\text{ann}} - r_f}{\sigma_d}`,
          String.raw`\dfrac{${subNum(data.return_ann_pct ?? 0, 2)}\% - ${subNum(data.risk_free_pct ?? 0, 2)}\%}`
          + String.raw`{${subNum(data.downside_dev_pct ?? 0, 2)}\%}`
          + ` = ${data.sortino.toFixed(2)}`)}
        legend={[
          { sym: String.raw`R_{\text{ann}}`, is: LEGEND.Rann },
          { sym: String.raw`\sigma_d`, is: LEGEND.sigmaD },
          { sym: String.raw`\text{Sortino}`, is: LEGEND.sortino },
        ]}
        how={'⚠ A DASH MEANS NOTHING EVER FELL BELOW THE TARGET — there is no downside to divide '
          + 'by. That is a measurement, not a missing number.'} />} />,
    },
  ] : [];

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-[11px] text-fg-faint">Measured</span>
        {FREQS.map((x) => (
          <button key={x.key} type="button" onClick={() => setFreq(x.key)}
            className={`cursor-pointer rounded-md border px-2 py-0.5 text-[11px] transition-colors ${
              freq === x.key ? 'bg-accent-600 text-white border-transparent'
                : 'bg-elevated border-neutral-800/40 text-fg-muted hover:text-accent-300'}`}>
            {x.label}
          </button>
        ))}
        {/* ⚠⚠ ANNUALISED IS MARKED ONCE, FOR THE WHOLE VIEW, and that is why the tiles no longer
            carry "(ann.)". It applies to σ, downside deviation, the return and both ratios — but
            only ONE tile ever said so, which left the other four looking like weekly numbers next
            to a "Weekly" control. A per-tile marker also could not survive naming the tiles after
            the book and the index: "BUSTELBERG OFFENSIEF VOLATILITY (ANN.)" wraps to three lines
            at 9px and breaks the row it is supposed to line up with. */}
        <span className="text-[11px] text-fg-faint">· shown annualised</span>
      </div>

      {error && <p className="text-xs text-neg-300">{error}</p>}
      {!data && !error && <p className="text-xs text-fg-subtle">Computing volatility…</p>}
      {data && !data.available && <p className="text-xs text-fg-muted">{data.reason}</p>}

      {data?.available && (
        <>
          {/*
            ⚠⚠ ONE ROW PER SIDE, ONE COLUMN PER MEASURE — not twelve tiles. Every figure here has a
            benchmark twin, and as tiles the pairing had to be inferred from adjacency: "Bustelberg
            Offensief volatility" beside "ACWI volatility" worked, but the other five had no twin on
            screen at all and the reader had nothing to scale them against. A matrix states the
            pairing structurally, names each side ONCE instead of on every tile, and gives the two
            values of a measure the same column so they can be read as one comparison.

            ⚠ THE ⓘ IS ON THE COLUMN, because a measure's definition is the same for both rows. Its
            worked line substitutes the BOOK's numbers — the index row is the identical formula over
            the other series, which the `what` says rather than doubling every card.

            ⚠ `table-fixed` IS WHAT MAKES THE COLUMNS EQUAL, and `overflow-x-auto` on the wrapper is
            the project rule for a dense table: it scrolls in its own box rather than squashing its
            columns or pushing the dialog sideways.
          */}
          <div className="rounded-lg border border-neutral-800/40 overflow-x-auto">
            <table className="w-full table-fixed text-xs min-w-[46rem]">
              <thead>
                <tr className="text-fg-faint [&>th]:px-2.5 [&>th]:py-1.5 [&>th]:font-medium
                  [&>th]:text-[9px] [&>th]:uppercase [&>th]:tracking-wider [&>th]:align-bottom
                  [&>th]:bg-inset">
                  <th className="text-left w-[9rem]" />
                  {MEASURES.map((m) => (
                    <th key={m.key} className="text-right">
                      <span className="inline-flex items-center gap-1">{m.label}{m.info}</span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {/* ⚠ THE BOOK FIRST AND IN THE STRONGER INK. Both rows are facts, but only one of
                    them is the thing the reader opened this panel to look at. */}
                <tr className="[&>td]:px-2.5 [&>td]:py-2 [&>td]:border-t
                  [&>td]:border-neutral-800/20">
                  <td className="text-fg-soft font-medium truncate" title={portfolioName}>
                    {portfolioName}
                  </td>
                  {MEASURES.map((m) => (
                    <td key={m.key}
                      className={`text-right font-mono tabular-nums text-base ${m.ownTone ?? 'text-fg-strong'}`}>
                      {m.own}
                    </td>
                  ))}
                </tr>
                <tr className="[&>td]:px-2.5 [&>td]:py-2 [&>td]:border-t
                  [&>td]:border-neutral-800/20">
                  <td className="text-fg-muted truncate" title={data.benchmark}>{data.benchmark}</td>
                  {MEASURES.map((m) => (
                    <td key={m.key}
                      className="text-right font-mono tabular-nums text-base text-fg-muted">
                      {m.bench}
                    </td>
                  ))}
                </tr>
              </tbody>
            </table>
          </div>

          {data.cadence_note && <p className="text-[11px] text-fg-faint">{data.cadence_note}</p>}

          <p className="text-[11px] text-fg-faint leading-relaxed">
            {/* ⚠ THE BOOK IS NAMED AND THE WINDOW IS DATED — same fix as `TrackingErrorView`'s own
                footnote. "Today's weights over 5 years" asserted a start date instead of reporting
                one, and the paired grid rarely reaches the full five. ⚠ Not badged: a bare <p> is
                outside the card system, and `v()` only renders inside one. */}
            {`${portfolioName}'s stock sleeve at its current weights, priced from `}
            {`${data.window_from ?? 'an unrecorded start'} to ${data.window_to ?? 'an unrecorded end'} `}
            {`(${data.priced_holdings} of ${data.total_holdings} priced). `}
            Deposits and withdrawals cannot distort this — it is a weighted basket of instrument
            returns, not an account value, so there are no flows in the series to strip out. The
            cost is that the weights are today&apos;s: this is the volatility of the portfolio as it
            stands, not the one the client lived through.
          </p>
        </>
      )}
    </div>
  );
}
