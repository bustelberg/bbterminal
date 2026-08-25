'use client';

/**
 * REALISED TRACKING ERROR — the Risk panel's second view.
 *
 *     aₜ = Rₜᵖ − Rₜᵇ      TE = √( 1/(T−1) · Σ (aₜ − ā)² ) · √f
 *
 * ⚠⚠ IT SITS BESIDE ACTIVE SHARE BECAUSE THE TWO ANSWER THE SAME QUESTION FROM OPPOSITE ENDS, and
 * that is the whole reason for the switch rather than two separate buttons. Active share is what
 * the book LOOKS like against the index; tracking error is what that difference has actually DONE.
 * A book can be 80% active and track closely (it owns different names in the same sectors), or 30%
 * active and wander (its few bets are enormous). Neither number is complete alone, and reading one
 * as a proxy for the other is the standard mistake.
 *
 * ⚠ EVERY LABEL SAYS "REALISED". The other definition — ex-ante, `√(wₐᵀΣwₐ)` from a covariance
 * matrix — is a FORECAST, needs a risk model we do not have, and routinely disagrees with this one.
 * A tile reading just "Tracking error" would be read as whichever the reader is used to.
 *
 * ⚠ THE ACTIVE RETURN IS SHOWN BESIDE IT, always. TE is the SPREAD of that quantity, and the two
 * are constantly confused — so the panel prints both rather than letting one stand for the other.
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
import { withWorked, subNum, subPct2, workedBand } from './workedFormula';
import { oneSigmaBand } from './activeBand';
import type { TrackingError } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';

/** ⚠ WEEKLY FIRST AND SELECTED BY DEFAULT — see the ⚠ on `daily`. Order is the recommendation. */
const FREQS = [
  { key: 'weekly', label: 'Weekly', f: 52 },
  { key: 'monthly', label: 'Monthly', f: 12 },
  { key: 'daily', label: 'Daily', f: 252 },
] as const;

const pct2 = (n: number | null | undefined) => (n == null ? '—' : `${n.toFixed(2)}%`);

/**
 * EVERY SYMBOL THIS VIEW USES, DEFINED ONCE.
 *
 * ⚠⚠ FOUR CARDS SHARE `aₜ`, `T` AND `f`. Written out per tile, the definitions drift on the first
 * edit — and a view where `T` means "paired periods" in one tooltip and "observations" in the next
 * has taught the reader that the symbol is decorative. One variable, one sentence, four cards.
 *
 * ⚠ THE "the answer:" PREFIX MARKS THE SYMBOL THE TILE ACTUALLY PRINTS, so a reader scanning a
 * six-row legend can find the one that is the number in front of them.
 */
const LEGEND = {
  a: 'the active return in period t — what the sleeve did that period, minus what the tracker did',
  R: (bookName: string, bench: string | null | undefined) =>
    `${v(bookName)}'s and ${v(bench)}'s own returns in that period, both in EUR`,
  aBar: 'the mean active return over the window — the band above is centred on it, not on zero',
  T: (n: number | null | undefined) =>
    `the number of paired periods (${v(n)} here) — the intersection of the two calendars`,
  f: (n: number | null | undefined) => `periods per year (${v(n)}), the annualisation factor`,
  te: 'one standard deviation of the active return, per year',
  // ⚠ THE PRODUCT IS THE POINT OF THIS TILE, and it is what separates it from the mean the TE card
  // uses: chaining the periods answers what the gap actually grew to, averaging them does not.
  prod: 'the periods CHAINED, not averaged — this is what the gap compounded to, which is why it '
    + 'sits slightly below the arithmetic mean the band on the tracking-error tile is centred on',
  Ra: 'the active return from the tile beside this one, annualised',
  IR: 'the answer: active return per unit of the tracking error taken to earn it',
};

function Tile({ label, value, tone, info }: {
  label: string; value: string; tone?: string; info?: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 min-w-[8rem]">
      <div className="text-[9px] uppercase tracking-wider text-fg-faint flex items-center gap-1">
        {label}{info}
      </div>
      <div className={`font-mono text-xl tabular-nums ${tone ?? 'text-fg-strong'}`}>{value}</div>
    </div>
  );
}

export default function TrackingErrorView({
  holdings, benchmark, portfolioName, portfolioAsOf, portfolioFetchedAt, portfolioSource,
}: {
  holdings: ActiveShareHolding[];
  benchmark: string;
  /**
   * The book's identity, forwarded from the panel — see the ⚠ on `ActiveSharePanel`'s own props.
   *
   * ⚠ THIS VIEW HAS TWO CLOCKS AND THEY ARE NOT THE SAME ONE. The WEIGHTS are today's book at its
   * AIRS valuation date; the RETURNS are a five-year price window ending at the last close both
   * series shared. A card that named only one would date half of what it measured.
   */
  portfolioName: string;
  portfolioAsOf?: string | null;
  portfolioFetchedAt?: string | null;
  portfolioSource: SourceKey;
}) {
  const [data, setData] = useState<TrackingError | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [freq, setFreq] = useState<'daily' | 'weekly' | 'monthly'>('weekly');

  // ⚠ A STRING KEY, NOT THE ARRAY — `holdings` is rebuilt on every parent render, so depending on
  // its identity would refetch for ever.
  const key = `${benchmark}|${freq}|${holdings.length}`
    + `|${holdings.reduce((s, h) => s + h.weight_pct, 0).toFixed(4)}`;

  useEffect(() => {
    let cancelled = false;
    setData(null);
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/portfolio/tracking-error`
          + `?benchmark=${encodeURIComponent(benchmark)}&frequency=${freq}`,
          { method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ holdings }) });
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setError(null);
        setData(b as TrackingError);
      } catch (e) {
        traceError('tracking-error', 'the tracking error could not be computed', e);
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  const f = FREQS.find((x) => x.key === freq)!;

  /**
   * ⚠⚠ THE READING THE NUMBER DOES NOT GIVE ANYONE ON ITS OWN — see `activeBand`. A tracking error
   * is a spread with no stated centre, and the centre every reader supplies is the benchmark; ours
   * is ā, because `_tracking_error.py` subtracts it. So the tile prints the interval rather than
   * leaving the reader to assume the symmetric one, which belongs to the other definition.
   *
   * ⚠ COMPUTED HERE AND NOT INSIDE THE TOOLTIP, so the arithmetic is unit-tested (`activeBand`)
   * and this file only formats it. Returns null on any missing operand, and the worked line then
   * falls back to the formula alone.
   */
  const band = oneSigmaBand(
    data?.mean_active_per_period_pct, data?.periods_per_year, data?.tracking_error_pct);

  /**
   * WHAT EVERY CARD IN THIS VIEW IS MEASURED FROM, built once.
   *
   * ⚠ FOUR CARDS, ONE PAIR OF INPUTS. Each tile is a different reading of the SAME paired series,
   * so four separately-worded Wheres would be four places for the sources to drift — and two tiles
   * naming different vendors for one number is unreconcilable from the outside.
   */
  const where = data?.available
    ? `${v(data.observations)} ${v(data.frequency)} active returns, prices from `
      + `${v(sourceField('yfinance'))} at ${v(sourceVendor('yfinance'))}, weights from `
      + `${v(sourceLabel(portfolioSource))}, against ${v(data.benchmark)}'s tracker `
      + `${v(data.benchmark_isin ?? 'not resolved')}.`
    : '';

  /**
   * ⚠⚠ TWO CLOCKS, BOTH STATED. The returns span a real window that is NOT "five years back from
   * today" — a recently-listed holding shortens the grid and a stale series ends it early — and the
   * WEIGHTS are today's book at its own AIRS valuation date. The card used to say "trailing window
   * — the sleeve as it stands today, carried backwards", which asserted both and dated neither.
   */
  const when = data?.available
    ? `Returns: ${v(data.window_from ?? 'no recorded start')} to `
      + `${v(data.window_to ?? 'no recorded end')} (${v(data.observations)} periods)\n`
      + `${v(portfolioName)} weights: ${v(dayOf(portfolioAsOf) ?? 'no recorded date')}`
      + `${dayOf(portfolioFetchedAt) && dayOf(portfolioFetchedAt) !== dayOf(portfolioAsOf)
        ? ` (read ${v(dayOf(portfolioFetchedAt))})` : ''}`
    : '';

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-[11px] text-fg-faint">Measured</span>
        {FREQS.map((x) => (
          <button key={x.key} type="button" onClick={() => setFreq(x.key)}
            title={x.key === 'daily'
              ? 'f = 252. ⚠ The tracker closes at 16:30 London and a US holding at 21:00, so daily '
                + 'closes are not synchronous — which lowers the measured covariance and INFLATES '
                + 'the tracking error.'
              : `f = ${x.f} observations per year.`}
            className={`cursor-pointer rounded-md border px-2 py-0.5 text-[11px] transition-colors ${
              freq === x.key ? 'bg-accent-600 text-white border-transparent'
                : 'bg-elevated border-neutral-800/40 text-fg-muted hover:text-accent-300'}`}>
            {x.label}
          </button>
        ))}
      </div>

      {error && <p className="text-xs text-neg-300">{error}</p>}
      {!data && !error && <p className="text-xs text-fg-subtle">Computing tracking error…</p>}
      {data && !data.available && <p className="text-xs text-fg-muted">{data.reason}</p>}

      {data?.available && (
        <>
          <div className="flex flex-wrap gap-2">
            <Tile label="Tracking error (realised)" value={pct2(data.tracking_error_pct)}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="How much the book's return has diverged from the benchmark's, annualised."
                where={where}
                when={when}
                worked={data.tracking_error_pct == null ? '' : withWorked(
                  String.raw`a_t = R_t^{\,p} - R_t^{\,b}\quad\Rightarrow\quad TE = \sqrt{\dfrac{\sum_t (a_t - \bar{a})^2}{T - 1}}\;\sqrt{f}`,
                  String.raw`T = ${data.observations},\; f = ${data.periods_per_year}`
                  + String.raw` \;\Rightarrow\; ${subNum(data.tracking_error_pct, 2)}\%`
                  // ⚠ A SECOND WORKED LINE, because the band is a second piece of arithmetic and
                  // not a restatement of the first. `\\[4pt]` is the same separator `withWorked`
                  // puts between the symbolic and substituted halves, so all three lines set as
                  // one aligned display rather than as a formula with a sentence stuck under it.
                  + (band ? String.raw` \\[4pt] ` + workedBand(band) : ''))}
                legend={[
                  { sym: String.raw`a_t`, is: LEGEND.a },
                  { sym: String.raw`R_t^{\,p},\; R_t^{\,b}`, is: LEGEND.R(portfolioName, data.benchmark) },
                  { sym: String.raw`\bar{a}`, is: LEGEND.aBar },
                  { sym: 'T', is: LEGEND.T(data.observations) },
                  { sym: 'f', is: LEGEND.f(data.periods_per_year) },
                  { sym: String.raw`TE`, is: `the answer: ${LEGEND.te}` },
                ]}
                how={(band
                  ? `A typical year lands ā ± TE — between ${v(subPct2(band.lo))} and `
                    + `${v(subPct2(band.hi))} against ${v(data.benchmark)}, centred on the mean active `
                    + `return of ${v(subPct2(band.centre))} and NOT on zero. About two years in `
                    + 'three; active returns are fatter-tailed than normal, so read it as a scale '
                    + 'rather than a promise. ⚠ THE CENTRE IS THE ARITHMETIC mean annualised, so '
                    + 'it sits a little above the geometric Active return tile beside it — the gap '
                    + 'is roughly TE²/2, and a ±σ band is only coherent around the arithmetic one. '
                  : '')
                  + '⚠ REALISED (ex-post), not the ex-ante forecast from a covariance matrix — '
                  + 'those are different numbers and routinely disagree. ā IS subtracted and the '
                  + 'divisor is T−1 (Bessel); some providers do neither — that version is '
                  + 'symmetric about the benchmark, and larger.'} />} />} />
            <Tile label="Active return (ann.)"
              value={pct2(data.active_return_ann_pct)}
              tone={(data.active_return_ann_pct ?? 0) >= 0 ? 'text-pos-300' : 'text-neg-300'}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="What the sleeve earned above or below the benchmark, per year."
                where={where}
                when={when}
                worked={data.active_return_ann_pct == null ? '' : withWorked(
                  String.raw`\left( \prod_t (1 + a_t) \right)^{f/T} - 1`,
                  String.raw`T = ${data.observations},\; f = ${data.periods_per_year}`
                  + String.raw` \;\Rightarrow\; ${subNum(data.active_return_ann_pct, 2)}\%`)}
                legend={[
                  { sym: String.raw`a_t`, is: LEGEND.a },
                  { sym: String.raw`\prod_t`, is: LEGEND.prod },
                  { sym: 'T', is: LEGEND.T(data.observations) },
                  { sym: 'f', is: LEGEND.f(data.periods_per_year) },
                ]}
                how={'⚠ THIS IS THE QUANTITY THE TILE BESIDE IT MEASURES THE VOLATILITY OF. They '
                  + 'are constantly confused: a book can wander a long way from its index and end '
                  + 'up exactly level, which is a large tracking error and no active return.'} />} />} />
            <Tile label="Information ratio"
              value={data.information_ratio == null ? '—' : data.information_ratio.toFixed(2)}
              tone={(data.information_ratio ?? 0) >= 0 ? 'text-pos-300' : 'text-neg-300'}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="Active return per unit of tracking error."
                where={where}
                when={when}
                worked={data.information_ratio == null || data.tracking_error_pct == null ? ''
                  : withWorked(
                    String.raw`IR = \dfrac{R_a}{TE}`,
                    String.raw`\dfrac{${subNum(data.active_return_ann_pct ?? 0, 2)}\%}`
                    + String.raw`{${subNum(data.tracking_error_pct, 2)}\%}`
                    + ` = ${data.information_ratio.toFixed(2)}`)}
                legend={[
                  { sym: 'R_a', is: LEGEND.Ra },
                  { sym: String.raw`TE`, is: LEGEND.te },
                  { sym: String.raw`IR`, is: LEGEND.IR },
                ]}
                how={'Whether the divergence was worth taking. ⚠ A dash means the tracking error '
                  + 'is ~0 — there is no risk to divide by, not that the ratio is zero.'} />} />} />
            <Tile label="Observations" value={`${data.observations}`} tone="text-fg-muted"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={`The T in the formula — ${v(data.frequency)} periods both series had.`}
                where={where}
                when={when}
                how={'A Stockholm listing and a London-traded tracker do not share holidays; '
                  + 'zipping them offsets the two series from the first mismatch onward and '
                  + 'produces a plausible figure measured against the wrong days.'} />} />} />
          </div>

          {data.cadence_note && (
            <p className="text-[11px] text-warn-300">{data.cadence_note}</p>
          )}

          <p className="text-[11px] text-fg-faint leading-relaxed">
            {/* ⚠ THE BOOK IS NAMED AND THE WINDOW IS DATED, same rule as the cards above — see the
                ⚠⚠ on `when`. "Today's sleeve carried back 5 years" asserted a start date rather
                than reporting one, and the grid rarely reaches the full five. ⚠ NOT badged: this
                is a plain <p> outside the card system, and `v()` only renders inside one. */}
            {`${portfolioName}'s stock sleeve at its current weights, priced from `}
            {`${data.window_from ?? 'an unrecorded start'} to ${data.window_to ?? 'an unrecorded end'} — `}
            not the book&apos;s realised history, so a name bought in March contributes its January
            return. It is the same portfolio the Active share view describes.
            {data.avg_weight_covered_pct != null && data.avg_weight_covered_pct < 99.5 && (
              <>
                {` The average period covered ${data.avg_weight_covered_pct.toFixed(2)}% of the sleeve`}
                {` (${data.priced_holdings} of ${data.total_holdings} holdings priced); the rest were `}
                renormalised over, never carried at zero.
              </>
            )}
            {` Differenced against ${data.benchmark}'s investable tracker, f = ${f.f}.`}
          </p>
        </>
      )}
    </div>
  );
}
