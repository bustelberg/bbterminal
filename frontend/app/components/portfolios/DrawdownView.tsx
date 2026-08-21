'use client';

/**
 * MAX DRAWDOWN — the Risk panel's fifth view.
 *
 *     Wₜ = ∏(1 + Rₛ)    Mₜ = max_{s≤t} Wₛ    DDₜ = Wₜ/Mₜ − 1    MDD = min DDₜ
 *
 * ⚠⚠ THIS IS THE RECONSTRUCTION, NOT THE CLIENT'S DRAWDOWN, and the panel says so where it cannot
 * be missed. It rebuilds a series from the holdings as they stand TODAY, so it carries look-ahead
 * bias (those weights were chosen with hindsight) and survivorship bias (names since sold are
 * absent — and the sold ones skew towards the fallers). The number for a client report is the one
 * from the AIRS returns, with real trades, real costs and real timing. Two different figures, not
 * interchangeable.
 *
 * ⚠⚠ DAILY BY DEFAULT — THE OPPOSITE OF THE OTHER RISK VIEWS. Tracking error, correlation and beta
 * default to weekly because they compare TWO series whose closes are hours apart. A drawdown
 * compares a series with itself, so that bias does not exist, and coarsening does real damage the
 * other way: a dip that recovers inside the period is invisible. The cadence comparison is on
 * screen for exactly that reason — measured, not asserted.
 *
 * ⚠ AND THE PERCENTAGE IS THE LEAST USEFUL PART. "−31.4%" is one number; "peaked 19 Feb, bottomed
 * 7 Apr after 33 days, back to level 12 Aug after another 91" is a conversation.
 */
import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { traceError } from '../../../lib/debugTrace';
import { withWorked, subNum } from './workedFormula';
import type { PortfolioDrawdown } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';

/** ⚠ DAILY FIRST — the order is the recommendation, and it is deliberately not the other views'. */
const FREQS = [
  { key: 'daily', label: 'Daily' },
  { key: 'weekly', label: 'Weekly' },
  { key: 'monthly', label: 'Monthly' },
] as const;

const pct2 = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);
const day = (d: string | null | undefined) =>
  (!d ? '—' : new Date(`${d}T00:00:00`).toLocaleDateString(undefined,
    { day: 'numeric', month: 'short', year: 'numeric' }));

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

export default function DrawdownView({ holdings, benchmark }: {
  holdings: ActiveShareHolding[];
  benchmark: string;
}) {
  const [data, setData] = useState<PortfolioDrawdown | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [freq, setFreq] = useState<'daily' | 'weekly' | 'monthly'>('daily');

  const key = `${benchmark}|${freq}|${holdings.length}`
    + `|${holdings.reduce((s, h) => s + h.weight_pct, 0).toFixed(4)}`;

  useEffect(() => {
    let cancelled = false;
    setData(null);
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/portfolio/drawdown`
          + `?benchmark=${encodeURIComponent(benchmark)}&frequency=${freq}`,
          { method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ holdings }) });
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setError(null);
        setData(b as PortfolioDrawdown);
      } catch (e) {
        traceError('drawdown', 'the drawdown could not be computed', e);
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  const unit = data?.frequency === 'daily' ? 'trading days'
    : data?.frequency === 'monthly' ? 'months' : 'weeks';
  const worst = data?.worst;
  const byFreq = data?.by_frequency ?? {};

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-[11px] text-fg-faint">Measured</span>
        {FREQS.map((x) => (
          <button key={x.key} type="button" onClick={() => setFreq(x.key)}
            title={x.key === 'daily'
              ? 'The most accurate basis for a drawdown — a dip that recovers inside a week or a '
                + 'month is invisible at those cadences.'
              : '⚠ Coarser than daily: any fall that recovers within the period is not seen, so '
                + 'this reads structurally shallower.'}
            className={`cursor-pointer rounded-md border px-2 py-0.5 text-[11px] transition-colors ${
              freq === x.key ? 'bg-accent-600 text-white border-transparent'
                : 'bg-elevated border-neutral-800/40 text-fg-muted hover:text-accent-300'}`}>
            {x.label}
          </button>
        ))}
      </div>

      {error && <p className="text-xs text-neg-300">{error}</p>}
      {!data && !error && <p className="text-xs text-fg-subtle">Computing drawdowns…</p>}
      {data && !data.available && <p className="text-xs text-fg-muted">{data.reason}</p>}

      {data?.available && (
        <>
          {/* ⚠⚠ THE PROVENANCE WARNING LEADS, above the numbers rather than in a footnote. It is
              not a caveat about precision — it says this is a DIFFERENT QUANTITY from the one a
              client report carries, and a reader who takes it for the client's own drawdown has
              been misled by the panel rather than by the data. */}
          <p className="text-[11px] text-warn-300 leading-relaxed">
            ⚠ Reconstructed from today&apos;s holdings, not the client&apos;s realised experience.
            Names since sold are absent and today&apos;s weights were chosen with hindsight, so this
            reads shallower than what was actually lived through. The client&apos;s own figure comes
            from the AIRS returns.
          </p>

          <div className="flex flex-wrap gap-2">
            <Tile label={`Max drawdown (${data.frequency})`} value={pct2(data.max_drawdown_pct)}
              tone="text-neg-300"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The deepest peak-to-trough fall in the window."
                where={`${data.observations} ${data.frequency} returns over ${data.years} years.`}
                worked={data.max_drawdown_pct == null ? '' : withWorked(
                  'Wₜ = ∏(1 + Rₛ),  Mₜ = max Wₛ,  MDD = min (Wₜ/Mₜ − 1)',
                  `${day(worst?.peak_date)} → ${day(worst?.trough_date)}`
                  + `  =  ${subNum(data.max_drawdown_pct, 2)}%`)}
                how={'⚠ THE CADENCE IS IN THE LABEL because it changes the answer: a fall that '
                  + 'recovers inside a week is invisible to a weekly series. See the comparison '
                  + 'below.'} />} />} />
            <Tile label={`${data.benchmark} max drawdown`}
              value={pct2(data.benchmark_max_drawdown_pct)} tone="text-fg-muted"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The index's own deepest fall, over the same periods."
                where="Same formula, same window, different series."
                how={'For scale. ⚠ It carries none of this book\'s survivorship bias — the index '
                  + 'kept its fallers — so the gap between the two flatters the book.'} />} />} />
            <Tile label="Today" value={pct2(data.current_drawdown_pct)}
              tone={(data.current_drawdown_pct ?? 0) < -0.005 ? 'text-neg-300' : 'text-pos-300'}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="How far below its own high water mark the sleeve sits right now."
                where="0% means it ended the window at a new high."
                how={'⚠ "Worst ever −31%" and "down 28% right now" are very different '
                  + 'conversations, and the second is the one being had.'} />} />} />
            <Tile label="Falls over 5%" value={`${data.episodes_total ?? 0}`} tone="text-fg-muted"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={`Distinct peak-to-trough episodes in the window (${data.episodes_total} in all).`}
                where="An episode ends only when the previous high is regained."
                how={'⚠ ONE NUMBER HIDES WHETHER IT WAS A PATTERN OR AN EVENT. One −30% and four '
                  + '−25%s share a maximum and are not the same risk. ⚠ A 40% fall that bounces 5% '
                  + 'and falls further is ONE drawdown, not two — splitting on direction would '
                  + 'report shallow dips and no crash.'} />} />} />
          </div>

          {/* ⚠ THE CADENCE COMPARISON, MEASURED IN THE SAME REQUEST. Stating "monthly understates"
              and leaving the reader to believe it is weaker than showing by how much on their own
              book — and the gap is percentage points, not noise. */}
          <div className="rounded-lg border border-neutral-800/40 bg-inset px-3 py-2">
            <div className="text-[10px] uppercase tracking-wider text-fg-faint mb-1">
              The same drawdown, measured three ways
            </div>
            <div className="flex flex-wrap gap-x-6 gap-y-1">
              {FREQS.map((x) => (
                <span key={x.key} className="text-[11px] text-fg-muted">
                  {x.label}{' '}
                  <span className={`font-mono tabular-nums ${
                    freq === x.key ? 'text-fg-strong' : 'text-fg-muted'}`}>
                    {pct2(byFreq[x.key])}
                  </span>
                </span>
              ))}
            </div>
            <p className="text-[10px] text-fg-faint mt-1">
              Coarser cadences cannot see a fall that recovers inside the period, so they read
              shallower. Daily is the honest basis for this measure.
            </p>
          </div>

          {worst && (
            <div className="rounded-lg border border-neutral-800/40 px-3 py-2">
              <div className="text-[10px] uppercase tracking-wider text-fg-faint mb-1">
                The worst one, in full
              </div>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-x-4 gap-y-1 text-[11px]">
                <div><span className="text-fg-faint">Peak</span>{' '}
                  <span className="text-fg-soft">{day(worst.peak_date)}</span></div>
                <div><span className="text-fg-faint">Trough</span>{' '}
                  <span className="text-fg-soft">{day(worst.trough_date)}</span>
                  <span className="text-fg-faint"> ({worst.decline_periods} {unit})</span></div>
                <div><span className="text-fg-faint">Recovered</span>{' '}
                  {worst.recovered
                    ? (
                      <>
                        <span className="text-fg-soft">{day(worst.recovery_date)}</span>
                        <span className="text-fg-faint"> ({worst.recovery_periods} {unit})</span>
                      </>
                    )
                    /* ⚠ NOT A DASH. "Still underwater" is a fact about the book; a dash reads as
                       a missing figure. */
                    : <span className="text-warn-300">still underwater</span>}
                </div>
                <div><span className="text-fg-faint">Peak to peak</span>{' '}
                  <span className="text-fg-soft">
                    {worst.total_periods == null ? '—' : `${worst.total_periods} ${unit}`}
                  </span></div>
              </div>
            </div>
          )}

          {(data.episodes ?? []).length > 1 && (
            <div>
              <div className="text-[10px] uppercase tracking-wider text-fg-faint mb-1">
                Deepest falls
              </div>
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-fg-faint [&>th]:py-1 [&>th]:font-medium">
                    <th className="text-left">Peak</th>
                    <th className="text-left">Trough</th>
                    <th className="text-right">Depth</th>
                    <th className="text-right">Decline</th>
                    <th className="text-right">Recovery</th>
                  </tr>
                </thead>
                <tbody>
                  {(data.episodes ?? []).map((e) => (
                    <tr key={`${e.peak_date}|${e.trough_date}`}
                      className="[&>td]:py-1 [&>td]:border-t [&>td]:border-neutral-800/20">
                      <td className="text-fg-soft">{day(e.peak_date)}</td>
                      <td className="text-fg-soft">{day(e.trough_date)}</td>
                      <td className="text-right font-mono tabular-nums text-neg-300">
                        {pct2(e.depth_pct)}
                      </td>
                      <td className="text-right font-mono tabular-nums text-fg-muted">
                        {e.decline_periods}
                      </td>
                      <td className="text-right font-mono tabular-nums text-fg-muted">
                        {e.recovered ? e.recovery_periods
                          : <span className="text-warn-300">open</span>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <p className="text-[11px] text-fg-faint leading-relaxed">
            {`Today's stock sleeve at today's weights over ${data.years} years `}
            {`(${data.priced_holdings} of ${data.total_holdings} priced). `}
            Durations are in {unit} of the selected cadence, not calendar days.
          </p>
        </>
      )}
    </div>
  );
}
