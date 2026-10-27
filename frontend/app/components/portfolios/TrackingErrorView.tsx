'use client';

/**
 * Realised tracking error — the Risk panel's second view.
 *
 *     aₜ = Rₜᵖ − Rₜᵇ      TE = √( 1/(T−1) · Σ (aₜ − ā)² ) · √f
 *
 *  It sits beside active share because the two answer the same question from opposite ends, and
 * that is the whole reason for the switch rather than two separate buttons. Active share is what
 * the book LOOKS like against the index; tracking error is what that difference has actually DONE.
 * A book can be 80% active and track closely (it owns different names in the same sectors), or 30%
 * active and wander (its few bets are enormous). Neither number is complete alone, and reading one
 * as a proxy for the other is the standard mistake.
 *
 *  Every label says "REALISED". The other definition — ex-ante, `√(wₐᵀΣwₐ)` from a covariance
 * matrix — is a FORECAST, needs a risk model we do not have, and routinely disagrees with this one.
 * A tile reading just "Tracking error" would be read as whichever the reader is used to.
 *
 *  The active return is shown beside it, always. TE is the SPREAD of that quantity, and the two
 * are constantly confused — so the panel prints both rather than letting one stand for the other.
 */
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { useRiskCopy } from './riskCopy';
import LoadingDots from './LoadingDots';
import { v } from '../../../lib/dynamicValue';
import { dayOf } from './asOfLine';
import { sourceField, sourceLabel, sourceVendor, type SourceKey } from '../../../lib/provenance';
import { withWorked, subNum, subPct2, workedBand } from './workedFormula';
import { oneSigmaBand } from './activeBand';
import type { TrackingError } from '../../../lib/types/api';
import { riskRequestKey, useRiskResult } from './useRiskResult';
import type { ActiveShareHolding } from './ActiveSharePanel';

const pct2 = (n: number | null | undefined) => (n == null ? '—' : `${n.toFixed(2)}%`);
const performanceDate = (date?: string | null) => date
  ? new Date(`${date}T12:00:00Z`).toLocaleDateString('en-GB', {
    day: 'numeric', month: 'long', year: 'numeric', timeZone: 'UTC',
  }) : '—';

/** The full CAGR substitution behind a return tile, using this response's paired-close window. */
function annualisedReturnWorked(totalPct: number | null | undefined,
                                factor: number | null | undefined,
                                annualPct: number | null | undefined) {
  if (totalPct == null || factor == null || annualPct == null) return '';
  return withWorked(
    String.raw`R_{ann} = (1 + R_{window})^{f/T} - 1`,
    String.raw`(1 + ${subNum(totalPct / 100, 6)})^{${subNum(factor, 4)}} - 1 = ${subNum(annualPct, 2)}\%`,
  );
}

/**
 *  The symbols moved to `riskCopy.te.legend` (2026-09-01) — DEFINED ONCE, NOW IN TWO LANGUAGES.
 * Four cards share `aₜ`, `T` and `f`, and written per tile the definitions drift on the first edit:
 * a view where `T` means "paired periods" in one tooltip and "observations" in the next has taught
 * the reader that the symbol is decorative. That argument is unchanged; what changed is that the
 * one place is now the copy module, so a Dutch reader gets the definition rather than the digits.
 *
 *  THE "the answer:" PREFIX IS PART OF THE TRANSLATED STRING (`teAnswer`, `IR`), not glued on
 * here — concatenated at the call site the Dutch card would read "the answer: het antwoord".
 */

function Tile({ label, value, tone, info }: {
  label: string; value: string; tone?: string; info?: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-neutral-800/40 bg-elevated px-4 py-2.5 min-w-[8.5rem]">
      <div className="text-sm text-fg-faint flex items-center gap-1">
        {label}{info}
      </div>
      <div className={`font-mono text-2xl tabular-nums ${tone ?? 'text-fg-strong'}`}>{value}</div>
    </div>
  );
}

export default function TrackingErrorView({
  holdings, benchmark, benchmarkWeights, portfolioName, portfolioAsOf, portfolioFetchedAt, portfolioSource,
}: {
  holdings: ActiveShareHolding[];
  portefeuille?: string | null;
  benchmark: string;
  benchmarkWeights?: Record<string, number>;
  /**
   * The book's identity, forwarded from the panel — see the  on `ActiveSharePanel`'s own props.
   *
   *  This view has two clocks and they are not the same one. The WEIGHTS are today's book at its
   * AIRS valuation date; the RETURNS are a five-year price window ending at the last close both
   * series shared. A card that named only one would date half of what it measured.
   */
  portfolioName: string;
  portfolioAsOf?: string | null;
  portfolioFetchedAt?: string | null;
  portfolioSource: SourceKey;
}) {
  const freq = 'monthly';

  //  A string key, not the array — `holdings` is rebuilt on every parent render, so depending on
  // its identity would refetch for ever.
  const requestUrl = `${API_URL}/api/airs/portfolio/tracking-error`
    // v4 adds the component start/end marks used by the audit table. This must be a new cache
    // identity: an earlier response is mathematically valid but cannot explain every displayed row.
    + `?benchmark=${encodeURIComponent(benchmark)}&frequency=${freq}&years=5&data_window=available-history-v1`;
  const requestBody = JSON.stringify({ holdings, benchmark_weights: benchmarkWeights });
  const key = riskRequestKey(requestUrl, requestBody);
  const { data, error } = useRiskResult<TrackingError>(key, async () => {
    const r = await apiFetch(requestUrl, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: requestBody,
    });
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    return b as TrackingError;
  }, 'tracking-error', 'the tracking error could not be computed');

  const comparison = data?.historical_comparison;
  const portfolioCagr = comparison?.available ? comparison.portfolio_cagr_pct : null;
  const activeReturn = comparison?.available ? comparison.active_return_pp : null;
  const benchmarkHistory = data?.benchmark_history;

  /**
   *  The reading the number does not give anyone on its own — see `activeBand`. A tracking error
   * is a spread with no stated centre, and the centre every reader supplies is the benchmark; ours
   * is ā, because `_tracking_error.py` subtracts it. So the tile prints the interval rather than
   * leaving the reader to assume the symmetric one, which belongs to the other definition.
   *
   *  Computed here and not inside the tooltip, so the arithmetic is unit-tested (`activeBand`)
   * and this file only formats it. Returns null on any missing operand, and the worked line then
   * falls back to the formula alone.
   */
  const band = oneSigmaBand(
    data?.mean_active_per_period_pct, data?.periods_per_year, data?.tracking_error_pct);

  /**
   * What every card in this view is measured from, built once.
   *
   *  Four cards, one pair of inputs. Each tile is a different reading of the SAME paired series,
   * so four separately-worded Wheres would be four places for the sources to drift — and two tiles
   * naming different vendors for one number is unreconcilable from the outside.
   */
  const where = data?.available
    ? `${v(data.observations)} ${v(data.frequency)} return comparisons. Portfolio prices use `
      + `${v(sourceField('yfinance'))} from ${v(sourceVendor('yfinance'))}, with weights from `
      + `${v(sourceLabel(portfolioSource))}. The benchmark is ${v(data.benchmark)} tracker `
      + `${v(data.benchmark_isin ?? 'not resolved')}.`
    : '';

  /**
   *  Two clocks, both stated. The returns span a real window that is NOT "five years back from
   * today" — a recently-listed holding shortens the grid and a stale series ends it early — and the
   * WEIGHTS are today's book at its own AIRS valuation date. The card used to say "trailing window
   * — the sleeve as it stands today, carried backwards", which asserted both and dated neither.
   */
  const t = useRiskCopy();
  const when = data?.available
    ? `Returns: ${v(data.window_from ?? 'no recorded start')} to `
      + `${v(data.window_to ?? 'no recorded end')} (${v(data.observations)} periods)\n`
      + `${v(portfolioName)} weights: ${v(dayOf(portfolioAsOf) ?? 'no recorded date')}`
      + `${dayOf(portfolioFetchedAt) && dayOf(portfolioFetchedAt) !== dayOf(portfolioAsOf)
        ? ` (read ${v(dayOf(portfolioFetchedAt))})` : ''}`
    : '';

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
          <div className="flex flex-col items-center gap-3">
            <div className="flex flex-col items-center gap-1">
              <p className="text-xs text-fg-faint">Five-year hypothetical performance if we held today’s portfolio 5 years ago · {performanceDate(benchmarkHistory?.start_date)} to {performanceDate(benchmarkHistory?.end_date)}</p>
              <div className="flex flex-wrap items-center justify-center gap-2">
              <Tile label={`${portfolioName} CAGR (5 years, today's weights)`} value={pct2(portfolioCagr)}
                info={<InfoTip wide content={<AspectCard
                  what="Five-year reconstruction using today's weights among holdings with available history."
                  where="AIRS weights and EUR-converted historical holding prices."
                  when={`${benchmarkHistory?.start_date ?? '—'} to ${benchmarkHistory?.end_date ?? '—'}`}
                  how={comparison?.available
                    ? `Eligible current holdings are rescaled to 100% each period. Coverage: minimum ${pct2(comparison.min_weight_covered_pct)}; average ${pct2(comparison.avg_weight_covered_pct)}.`
                    : comparison?.reason ?? 'Five-year history is unavailable.'}
                  worked={annualisedReturnWorked(comparison?.total_return_pct,
                    comparison?.years_elapsed ? 1 / comparison.years_elapsed : null, portfolioCagr)}
                  calculation={<div className="border-t border-neutral-800/40 pt-2">
                    <table className="w-full border-collapse text-[11px] leading-snug">
                      <thead>
                        <tr className="border-b border-neutral-800/40 text-fg-faint">
                          <th scope="col" className="py-2 pr-3 text-left font-medium">Holding</th>
                          <th scope="col" className="px-2 py-2 text-right font-medium">Weight</th>
                          <th scope="col" className="py-2 pl-3 text-right font-medium whitespace-nowrap">Included from</th>
                        </tr>
                      </thead>
                      <tbody>
                        {comparison?.holdings_history?.map(holding => <tr key={holding.isin} className="border-b border-neutral-800/25 last:border-0">
                          <td className="py-2 pr-3 text-fg-subtle">{holding.name}</td>
                          <td className="px-2 py-2 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted">{holding.weight_pct.toFixed(2)}%</td>
                          <td className="py-2 pl-3 text-right font-mono tabular-nums whitespace-nowrap text-fg-muted">{holding.first_used ?? 'Unavailable'}</td>
                        </tr>)}
                      </tbody>
                    </table>
                  </div>}
                />} />} />
              <span aria-hidden="true" className="text-xl text-fg-faint">−</span>
              <Tile label={`${data.benchmark} CAGR (${data.benchmark_history?.complete ? '5 years' : 'available history'})`} value={pct2(benchmarkHistory?.complete ? benchmarkHistory.cagr_pct : null)}
                info={<InfoTip wide className="ml-0.5" content={<AspectCard
                  what={`CAGR of ${data.benchmark} over its ${data.benchmark_history?.complete ? 'five-year' : 'available'} history.`}
                  where="EUR-converted benchmark price histories at the configured policy weights."
                  when={`${benchmarkHistory?.start_date ?? '—'} to ${benchmarkHistory?.end_date ?? '—'}`}
                  how="Compound the policy-weighted returns on shared trading dates, then annualise the resulting growth using the actual calendar years elapsed."
                  calculation={benchmarkHistory?.components?.length ? (
                    <div className="border-t border-neutral-800/40 pt-2 text-[11px]">
                      <div className="mb-1 font-medium text-fg-subtle">Benchmark legs — {benchmarkHistory.complete ? 'last five years' : 'available benchmark history'} ({benchmarkHistory.start_date} to {benchmarkHistory.end_date})</div>
                      <div className="grid grid-cols-[1fr_auto_auto_auto_auto_auto] gap-x-2 gap-y-1 tabular-nums">
                        <span className="text-fg-faint">Leg</span><span className="text-right text-fg-faint">Weight</span><span className="text-right text-fg-faint">Start EUR</span><span className="text-right text-fg-faint">End EUR</span><span className="text-right text-fg-faint">Window</span><span className="text-right text-fg-faint">CAGR</span>
                        {benchmarkHistory.components.map((component) => <div key={component.name} className="contents">
                          <span>{component.name}</span>
                          <span className="text-right">{component.weight_pct.toFixed(1)}%</span>
                          <span className="text-right">{component.start_price_eur == null ? '—' : `${component.start_price_eur.toFixed(2)} · ${component.start_date ?? '—'}`}</span>
                          <span className="text-right">{component.end_price_eur == null ? '—' : `${component.end_price_eur.toFixed(2)} · ${component.end_date ?? '—'}`}</span>
                          <span className="text-right">{pct2(component.window_return_pct)}</span>
                          <span className="text-right">{pct2(component.annualized_return_pct)}</span>
                        </div>)}
                      </div>
                      <p className="mt-2 font-medium">Policy benchmark CAGR over this period: {pct2(benchmarkHistory.cagr_pct)}</p>
                      <p className="mt-1 text-fg-muted">EUR-adjusted closes. Policy weights are rebalanced on shared trading dates. The five-year comparison uses these dates; the tracking-error window is stated separately.</p>
                    </div>
                  ) : null} />} />} />
              <span aria-hidden="true" className="text-xl text-fg-faint">=</span>
              <Tile label="Active return (5 years)" value={activeReturn == null ? '—' : `${activeReturn.toFixed(2)}pp`}
                info={<InfoTip content={<AspectCard
                  what="Today's portfolio five-year CAGR minus the policy benchmark five-year CAGR."
                  when={`${benchmarkHistory?.start_date ?? '—'} to ${benchmarkHistory?.end_date ?? '—'}`}
                  how={activeReturn == null ? 'Requires usable holding histories throughout the five-year window.'
                    : `${pct2(portfolioCagr)} − ${pct2(benchmarkHistory?.cagr_pct)} = ${activeReturn.toFixed(2)}pp`}
                />} />} />
              </div>
              {!comparison?.available && <p className="max-w-2xl text-center text-xs text-fg-faint">{comparison?.reason}</p>}
            </div>
            <div className="flex flex-col items-center">
              <p className="mb-1 text-xs text-fg-faint">Tracking error window: {data.window_from} to {data.window_to} · {data.observations} {data.frequency} returns{data.preliminary ? ' · preliminary' : ''}</p>
              <Tile label={t.te.trackingError} value={pct2(data.tracking_error_pct).replace('%', 'pp')}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={t.te.cards.te.what}
                where={where}
                when={when}
                worked={data.tracking_error_pct == null ? '' : withWorked(
                  String.raw`a_t = R_t^{\,p} - R_t^{\,b}\quad\Rightarrow\quad TE = \sqrt{\dfrac{\sum_t (a_t - \bar{a})^2}{T - 1}}\;\sqrt{f}`,
                  String.raw`\bar{a} = ${subNum(data.mean_active_per_period_pct ?? 0, 4)}\%,\; \sigma_a = ${subNum(data.active_return_std_per_period_pct ?? 0, 4)}\%,\; T = ${data.observations},\; f = ${data.periods_per_year}`
                  + String.raw` \;\Rightarrow\; ${subNum(data.active_return_std_per_period_pct ?? 0, 4)}\% \times \sqrt{${data.periods_per_year}} = ${subNum(data.tracking_error_pct, 2)}\%`
                  //  A second worked line, because the band is a second piece of arithmetic and
                  // not a restatement of the first. `\\[4pt]` is the same separator `withWorked`
                  // puts between the symbolic and substituted halves, so all three lines set as
                  // one aligned display rather than as a formula with a sentence stuck under it.
                  + (band ? String.raw` \\[4pt] ` + workedBand(band) : ''))}
                legend={[
                  { sym: String.raw`a_t`, is: t.te.legend.a },
                  { sym: String.raw`R_t^{\,p},\; R_t^{\,b}`, is: t.te.legend.R(portfolioName, data.benchmark) },
                  { sym: String.raw`\bar{a}`, is: t.te.legend.aBar },
                  { sym: 'T', is: t.te.legend.T(data.observations) },
                  { sym: 'f', is: t.te.legend.f(data.periods_per_year) },
                  { sym: String.raw`TE`, is: t.te.legend.teAnswer },
                ]}
                how={(band
                  ? `${t.te.bandReading(subPct2(band.lo), subPct2(band.hi),
                    subPct2(band.centre), data.benchmark)} `
                  : '') + t.te.cards.te.how} />} />} />
            </div>
          </div>


        </>
      )}
    </div>
  );
}
