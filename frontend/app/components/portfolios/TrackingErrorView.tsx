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
import { traceError } from '../../../lib/debugTrace';
import { withWorked, subNum } from './workedFormula';
import type { TrackingError } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';

/** ⚠ WEEKLY FIRST AND SELECTED BY DEFAULT — see the ⚠ on `daily`. Order is the recommendation. */
const FREQS = [
  { key: 'weekly', label: 'Weekly', f: 52 },
  { key: 'monthly', label: 'Monthly', f: 12 },
  { key: 'daily', label: 'Daily', f: 252 },
] as const;

const pct2 = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);

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

export default function TrackingErrorView({ holdings, benchmark }: {
  holdings: ActiveShareHolding[];
  benchmark: string;
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
                where={`${data.observations} ${data.frequency} active returns over ${data.years} years, against ${data.benchmark}'s tracker.`}
                when="Trailing window — the sleeve as it stands today, carried backwards."
                worked={data.tracking_error_pct == null ? '' : withWorked(
                  String.raw`a_t = R_t^{\,p} - R_t^{\,b}\quad\Rightarrow\quad TE = \sqrt{\dfrac{\sum_t (a_t - \bar{a})^2}{T - 1}}\;\sqrt{f}`,
                  String.raw`T = ${data.observations},\; f = ${data.periods_per_year}`
                  + String.raw` \;\Rightarrow\; ${subNum(data.tracking_error_pct, 2)}\%`)}
                how={'⚠ REALISED (ex-post), not the ex-ante forecast from a covariance matrix — '
                  + 'those are different numbers and routinely disagree. ā IS subtracted and the '
                  + 'divisor is T−1 (Bessel); some providers do neither, which reads higher.'} />} />} />
            <Tile label="Active return (ann.)"
              value={pct2(data.active_return_ann_pct)}
              tone={(data.active_return_ann_pct ?? 0) >= 0 ? 'text-pos-300' : 'text-neg-300'}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="What the sleeve earned above or below the benchmark, per year."
                where="The same active returns, compounded — not their spread."
                how={'⚠ THIS IS THE QUANTITY THE TILE BESIDE IT MEASURES THE VOLATILITY OF. They '
                  + 'are constantly confused: a book can wander a long way from its index and end '
                  + 'up exactly level, which is a large tracking error and no active return.'} />} />} />
            <Tile label="Information ratio"
              value={data.information_ratio == null ? '—' : data.information_ratio.toFixed(2)}
              tone={(data.information_ratio ?? 0) >= 0 ? 'text-pos-300' : 'text-neg-300'}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="Active return per unit of tracking error."
                where="Active return ÷ tracking error, both annualised."
                worked={data.information_ratio == null || data.tracking_error_pct == null ? ''
                  : String.raw`\dfrac{${subNum(data.active_return_ann_pct ?? 0, 2)}\%}`
                    + String.raw`{${subNum(data.tracking_error_pct, 2)}\%}`
                    + ` = ${data.information_ratio.toFixed(2)}`}
                how={'Whether the divergence was worth taking. ⚠ A dash means the tracking error '
                  + 'is ~0 — there is no risk to divide by, not that the ratio is zero.'} />} />} />
            <Tile label="Observations" value={`${data.observations}`} tone="text-fg-muted"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={`The T in the formula — ${data.frequency} periods both series had.`}
                where="The INTERSECTION of the two calendars, never a positional pairing."
                how={'A Stockholm listing and a London-traded tracker do not share holidays; '
                  + 'zipping them offsets the two series from the first mismatch onward and '
                  + 'produces a plausible figure measured against the wrong days.'} />} />} />
          </div>

          {data.cadence_note && (
            <p className="text-[11px] text-warn-300">{data.cadence_note}</p>
          )}

          <p className="text-[11px] text-fg-faint leading-relaxed">
            {`Today's stock sleeve at today's weights, carried back ${data.years} years — `}
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
