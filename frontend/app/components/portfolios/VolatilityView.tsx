'use client';

/**
 * VOLATILITY — the Risk panel's fourth view. `σ_p = √(Σ(Rₜ − R̄)²/(T−1)) · √f`.
 *
 * The same construction as the tracking error, on the book's OWN returns rather than on the
 * difference — and the same `σ_p` the correlation view puts inside `σₐ² = σₚ² + σᵦ² − 2ρσₚσᵦ`. One
 * series, one function; two figures a click apart that disagreed would tell the reader that one of
 * them is wrong and nothing about which.
 *
 *  No cash-flow contamination, by construction rather than by chain-linking. The usual hazard is
 * measuring risk off an ACCOUNT VALUE: a deposit reads as a huge gain and a withdrawal as a crash,
 * so a book that merely received money looks turbulent. Time-weighted returns exist to strip that
 * out. This series never has flows in it — it is a weighted basket of instrument price returns, so
 * money moving in or out of the account changes nothing in it.
 *
 *  The price is the other caveat and the panel states it: today's weights, carried backwards. So
 * this is the volatility of the portfolio AS IT STANDS, not the one the client lived through.
 *
 *  Downside deviation is sortino's, not the semi-deviation — divided by ALL n, against a target of
 * 0. Both are called "downside deviation"; this is the one the Sortino beside it is built on, so
 * the ratio equals its own parts.
 */
import { apiFetch } from '../../../lib/apiFetch';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';
import { chartTheme } from '../../../lib/chartTheme';
import { API_URL } from '../../../lib/apiUrl';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { useRiskCopy } from './riskCopy';
import LoadingDots from './LoadingDots';
import { v } from '../../../lib/dynamicValue';
import { dayOf } from './asOfLine';
import { sourceField, sourceLabel, sourceVendor, type SourceKey } from '../../../lib/provenance';
import { withWorked, subNum } from './workedFormula';
import type { PortfolioVolatility } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';
import { riskRequestKey, useRiskResult } from './useRiskResult';

const pct2 = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);
const signed2 = (v: number | null | undefined) =>
  (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`);

/**
 * Every symbol this view uses, defined once — same rule as the tracking-error and correlation
 * views. `Rₜ`, `T` and `f` appear on four of the seven cards between them.
 */
const LEGEND = {
  R: (bookName: string) => `${v(bookName)}'s return in period t, in EUR`,
  RBar: 'the average return over the measured window',
  T: (n: number | null | undefined) => `the number of periods (${v(n)} here)`,
  f: (n: number | null | undefined) => `periods per year (${v(n)}), the annualisation factor`,
  sigma: 'annualised volatility',
  //  THE min(·,0) IS THE WHOLE DIFFERENCE FROM σ. Every up period contributes exactly zero, so
  // this is a spread of losses only — and the divisor is still ALL periods, which is Sortino's
  // convention rather than the semi-deviation's.
  minR: 'the lower of the period return and zero, so gains contribute zero',
  sigmaD: 'annualised downside deviation',
  prod: 'multiply the period returns to compound them over time',
  Rann: 'the annualised return from the tile beside this one',
  rf: 'the annual risk-free rate deducted from the return',
  sharpe: 'annualised excess return per unit of total volatility',
  sortino: 'annualised excess return per unit of downside deviation',
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
  const t = useRiskCopy();
  const freq = 'monthly';

  const requestUrl = `${API_URL}/api/airs/portfolio/volatility`
    + `?benchmark=${encodeURIComponent(benchmark)}&frequency=${freq}&years=5&data_window=shared-v1`;
  const requestBody = JSON.stringify({ holdings });
  const key = riskRequestKey(requestUrl, requestBody);
  const { data, error } = useRiskResult<PortfolioVolatility>(key, async () => {
    const r = await apiFetch(requestUrl, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: requestBody,
    });
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    return b as PortfolioVolatility;
  }, 'volatility', 'the volatility could not be computed');

  const period = 'month';

  /**
   * What every card here is measured from, and over what window — built once, seven cards.
   *
   *  The same two strings the tracking-error and correlation views build. All three read one
   * `build_paired_series`; three separately-worded Wheres would be three places for the sources
   * to drift apart while the numbers stayed identical.
   */
  const where = data?.available
    ? `${v(data.observations)} ${v(data.frequency)} returns, prices from ${v(sourceField('yfinance'))} `
      + `at ${v(sourceVendor('yfinance'))}, weights from ${v(sourceLabel(portfolioSource))}, `
      + `against ${v(data.benchmark)}'s tracker.`
    : '';

  /**  TWO CLOCKS — a price window and a weights date. See the  in `TrackingErrorView`. */
  const when = data?.available
    ? `Returns: ${v(data.window_from ?? 'no recorded start')} to `
      + `${v(data.window_to ?? 'no recorded end')} (${v(data.observations)} periods)\n`
      + `${v(portfolioName)} weights: ${v(dayOf(portfolioAsOf) ?? 'no recorded date')}`
      + `${dayOf(portfolioFetchedAt) && dayOf(portfolioFetchedAt) !== dayOf(portfolioAsOf)
        ? ` (read ${v(dayOf(portfolioFetchedAt))})` : ''}`
    : '';

  /** A ratio, or a dash.  A dash is a MEASUREMENT here — see the Sortino note. */
  const num2 = (n: number | null | undefined) => (n == null ? '—' : n.toFixed(2));

  /**
   * One entry per measure, each carrying both sides.
   *
   *  The pairing lives in the data, not in the layout. Built as twelve separate tiles, "which
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
        what={t.vol.cards.volatility.what}
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
        ]}
        how={t.vol.cards.volatility.how} />} />,
    },
    {
      key: 'dd',
      label: 'Downside deviation',
      own: pct2(data.downside_dev_pct),
      bench: pct2(data.benchmark_downside_dev_pct),
      info: <InfoTip className="ml-0.5" content={<AspectCard
        what={t.vol.cards.downside.what}
        where={where}
        when={when}
        worked={data.downside_dev_pct == null ? '' : withWorked(
          String.raw`\sigma_d = \sqrt{\dfrac{\sum_t \min(R_t,\; 0)^2}{T}}\;\sqrt{f}`,
          String.raw`T = ${data.observations},\; f = ${data.periods_per_year}`
          + String.raw` \;\Rightarrow\; ${subNum(data.downside_dev_pct, 2)}\%`)}
        legend={[
          { sym: String.raw`\min(R_t,\; 0)`, is: LEGEND.minR },
          { sym: String.raw`R_t`, is: LEGEND.R(portfolioName) },
          { sym: 'T', is: LEGEND.T(data.observations) },
          { sym: 'f', is: LEGEND.f(data.periods_per_year) },
          { sym: String.raw`\sigma_d`, is: LEGEND.sigmaD },
        ]}
        how={t.vol.cards.downside.how} />} />,
    },
    {
      key: 'worst',
      label: `Worst ${period}`,
      own: signed2(data.worst_period_pct),
      ownTone: 'text-neg-300',
      bench: signed2(data.benchmark_worst_period_pct),
      info: <InfoTip className="ml-0.5" content={<AspectCard
        what={t.vol.cards.worst.what}
        where={`Best was ${v(signed2(data.best_period_pct))} against `
          + `${v(signed2(data.benchmark_best_period_pct))}; `
          + `${v(`${data.negative_periods_pct?.toFixed(2)}%`)} of ${period}s were negative against `
          + `${v(`${data.benchmark_negative_periods_pct?.toFixed(2)}%`)}.`}
        when={when}
        how={t.vol.cards.worst.how} />} />,
    },
    {
      key: 'ret',
      label: 'Return',
      own: signed2(data.return_ann_pct),
      ownTone: (data.return_ann_pct ?? 0) >= 0 ? 'text-pos-300' : 'text-neg-300',
      bench: signed2(data.benchmark_return_ann_pct),
      info: <InfoTip className="ml-0.5" content={<AspectCard
        what={t.vol.cards.ret.what}
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
        how={t.vol.cards.ret.how} />} />,
    },
    {
      key: 'sharpe',
      label: 'Sharpe',
      own: num2(data.sharpe),
      bench: num2(data.benchmark_sharpe),
      info: <InfoTip className="ml-0.5" content={<AspectCard
        what={t.vol.cards.sharpe.what}
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
        ]}
        how={t.vol.cards.sharpe.how} />} />,
    },
    {
      key: 'sortino',
      label: 'Sortino',
      own: num2(data.sortino),
      bench: num2(data.benchmark_sortino),
      info: <InfoTip className="ml-0.5" content={<AspectCard
        what={t.vol.cards.sortino.what}
        where={where}
        when={when}
        worked={data.sortino == null ? '' : withWorked(
          String.raw`\text{Sortino} = \dfrac{R_{\text{ann}} - r_f}{\sigma_d}`,
          String.raw`\dfrac{${subNum(data.return_ann_pct ?? 0, 2)}\% - ${subNum(data.risk_free_pct ?? 0, 2)}\%}`
          + String.raw`{${subNum(data.downside_dev_pct ?? 0, 2)}\%}`
          + ` = ${data.sortino.toFixed(2)}`)}
        legend={[
          { sym: String.raw`R_{\text{ann}}`, is: LEGEND.Rann },
          { sym: 'r_f', is: LEGEND.rf },
          { sym: String.raw`\sigma_d`, is: LEGEND.sigmaD },
          { sym: String.raw`\text{Sortino}`, is: LEGEND.sortino },
        ]}
        how={t.vol.cards.sortino.how} />} />,
    },
  ] : [];

  return (
    <div className="space-y-3">

      {error && <p className="text-xs text-neg-300">{error}</p>}
      {!data && !error && (
        <div className="absolute inset-0 grid place-items-center text-center px-6" role="status">
          <p className="text-2xl font-medium text-fg-strong">{t.common.computing} <LoadingDots /></p>
        </div>
      )}
      {data && !data.available && <p className="text-xs text-fg-muted">{data.reason}</p>}

      {data?.available && (
        <>
          {/*
             ONE ROW PER SIDE, ONE COLUMN PER MEASURE — not twelve tiles. Every figure here has a
            benchmark twin, and as tiles the pairing had to be inferred from adjacency: "Bustelberg
            Offensief volatility" beside "ACWI volatility" worked, but the other five had no twin on
            screen at all and the reader had nothing to scale them against. A matrix states the
            pairing structurally, names each side ONCE instead of on every tile, and gives the two
            values of a measure the same column so they can be read as one comparison.

             THE ⓘ IS ON THE COLUMN, because a measure's definition is the same for both rows. Its
            worked line substitutes the BOOK's numbers — the index row is the identical formula over
            the other series, which the `what` says rather than doubling every card.

             `table-fixed` IS WHAT MAKES THE COLUMNS EQUAL, and `overflow-x-auto` on the wrapper is
            the project rule for a dense table: it scrolls in its own box rather than squashing its
            columns or pushing the dialog sideways.
          */}
          <div className="flex flex-col items-center gap-2">
            <p className="text-sm font-medium text-fg-subtle text-center">{t.vol.question(portfolioName, data.benchmark)}</p>
            <div className="flex flex-wrap justify-center gap-2">
              {MEASURES.filter((m) => m.key === 'vol').map((m) => (
              <div key={m.key} className="rounded-lg border border-neutral-800/40 bg-elevated px-4 py-2.5 min-w-[8.5rem]">
                <div className="text-sm text-fg-faint flex items-center gap-1">{m.label}{m.info}</div>
                <div className={`font-mono text-2xl tabular-nums ${m.ownTone ?? 'text-fg-strong'}`}>{m.own}</div>
                <div className="mt-1 text-xs text-fg-muted">{data.benchmark}: <span className="font-mono tabular-nums">{m.bench}</span></div>
              </div>
              ))}
            </div>
          </div>

          {data.cadence_note && <p className="text-[11px] text-fg-faint">{data.cadence_note}</p>}
          {data.volatility_pct != null && data.benchmark_volatility_pct != null && data.benchmark_volatility_pct > 0 && (
            <p className="text-center text-sm text-fg-soft">
              {portfolioName} is <span className="font-mono font-semibold">{(data.volatility_pct / data.benchmark_volatility_pct).toFixed(2)}×</span> as volatile as {data.benchmark} over this period.
            </p>
          )}
          <section className="rounded-xl border border-neutral-800/40 bg-elevated p-4">
            <h5 className="mb-4 text-center text-base font-medium text-fg-strong">Rolling 12-month volatility</h5>
            {(data.rolling_volatility?.length ?? 0) > 0 ? (
              <ResponsiveContainer width="100%" height={280}>
                <LineChart data={data.rolling_volatility} margin={{ top: 8, right: 16, bottom: 4, left: 8 }}>
                  <CartesianGrid stroke={chartTheme.gridEarnings} strokeDasharray="3 3" />
                  <XAxis dataKey="date" minTickGap={50} tick={{ fontSize: 12, fill: chartTheme.axisTick }} tickFormatter={(date: string) => new Date(`${date}T00:00:00Z`).toLocaleDateString('en-GB', { month: 'short', year: '2-digit', timeZone: 'UTC' })} />
                  <YAxis domain={[0, 'auto']} tick={{ fontSize: 12, fill: chartTheme.axisTick }} tickFormatter={(value: number) => `${value.toFixed(0)}%`} />
                  <Tooltip formatter={(value) => `${Number(value).toFixed(2)}%`} />
                  <Legend />
                  <Line type="monotone" dataKey="portfolio_pct" name={portfolioName} stroke={chartTheme.pos} strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="benchmark_pct" name={data.benchmark} stroke={chartTheme.compare} strokeWidth={2} strokeDasharray="5 3" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            ) : <p className="py-10 text-center text-sm text-fg-muted">At least 12 months of overlapping returns are needed.</p>}
            <p className="mt-3 text-center text-xs text-fg-faint">Each point shows annualized volatility over the preceding 12 months. Uses today&apos;s AIRS stock holdings and weights applied to historical EUR prices.</p>
          </section>

        </>
      )}
    </div>
  );
}
