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
    + `?benchmark=${encodeURIComponent(benchmark)}&frequency=${freq}&years=5&data_window=shared-v1`;
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

  const periodsPerYear = 12;

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
  const activeReturnAverage = data?.active_return_ann_pct == null ? 'n/a'
    : `${data.active_return_ann_pct >= 0 ? '+' : ''}${data.active_return_ann_pct.toFixed(2)}pp`;

  /**
   * What every card in this view is measured from, built once.
   *
   *  Four cards, one pair of inputs. Each tile is a different reading of the SAME paired series,
   * so four separately-worded Wheres would be four places for the sources to drift — and two tiles
   * naming different vendors for one number is unreconcilable from the outside.
   */
  const where = data?.available
    ? `${v(data.observations)} monthly return comparisons. Portfolio prices use `
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
              <p className="text-sm font-medium text-fg-subtle">{t.te.activeReturnQuestion(portfolioName, data.benchmark)}</p>
              <div className="flex flex-wrap items-center justify-center gap-2">
              <Tile label={t.te.benchmarkReturn(data.benchmark)} value={pct2(data.benchmark_return_ann_pct)} />
              <span aria-hidden="true" className="text-xl text-fg-faint">+</span>
            <Tile label={t.te.activeReturn}
              value={pct2(data.active_return_ann_pct).replace('%', 'pp')}
              tone={(data.active_return_ann_pct ?? 0) >= 0 ? 'text-pos-300' : 'text-neg-300'}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={`Annualized portfolio return minus ${data.benchmark}'s return, in percentage points. Calculated before rounding.`}
                where={where}
                when={when}
                worked={data.active_return_ann_pct == null ? '' : withWorked(
                  String.raw`\text{Active return} = \text{Portfolio annualized return} - \text{Benchmark annualized return}`,
                  String.raw`${subNum(data.portfolio_return_ann_pct ?? 0, 4)}\% - (${subNum(data.benchmark_return_ann_pct ?? 0, 4)}\%) \approx ${subNum(data.active_return_ann_pct, 2)}\text{pp}`)}
                how="Each series is compounded and annualized separately, then the benchmark return is subtracted. This uses today's AIRS holdings and weights over the historical period; it is not your account's realized performance." />} />} />
              <span aria-hidden="true" className="text-xl text-fg-faint">=</span>
              <Tile label={t.te.portfolioReturn(portfolioName)} value={pct2(data.portfolio_return_ann_pct)}
                tone={(data.portfolio_return_ann_pct ?? 0) >= 0 ? 'text-pos-300' : 'text-neg-300'} />
              </div>
            </div>
            <div className="flex flex-col items-center">
              <p className="mb-1 text-sm font-medium text-fg-subtle">{t.te.trackingErrorQuestion(activeReturnAverage)}</p>
              <Tile label={t.te.trackingError} value={pct2(data.tracking_error_pct).replace('%', 'pp')}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={t.te.cards.te.what}
                where={where}
                when={when}
                worked={data.tracking_error_pct == null ? '' : withWorked(
                  String.raw`a_t = R_t^{\,p} - R_t^{\,b}\quad\Rightarrow\quad TE = \sqrt{\dfrac{\sum_t (a_t - \bar{a})^2}{T - 1}}\;\sqrt{f}`,
                  String.raw`T = ${data.observations},\; f = ${data.periods_per_year}`
                  + String.raw` \;\Rightarrow\; ${subNum(data.tracking_error_pct, 2)}\%`
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

          {data.cadence_note && (
            <p className="text-[11px] text-warn-300">{data.cadence_note}</p>
          )}

        </>
      )}
    </div>
  );
}
