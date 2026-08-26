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
import { v } from '../../../lib/dynamicValue';
import { dayOf } from './asOfLine';
import { sourceField, sourceLabel, sourceVendor, type SourceKey } from '../../../lib/provenance';
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

/**
 * EVERY SYMBOL THIS VIEW USES, DEFINED ONCE — same rule as the other three risk views.
 *
 * ⚠ A DRAWDOWN IS THREE QUANTITIES, NOT ONE, and the reader has to hold all three at once: the
 * wealth curve, its running maximum, and the gap between them. Defining them per card would be
 * three chances to describe the high-water mark differently.
 */
const LEGEND = {
  Rs: (bookName: string) => `${v(bookName)}'s return in period s, in EUR`,
  W: 'the wealth curve — one euro compounded through every period up to t, cash flows absent',
  M: 'the HIGH-WATER MARK: the best that curve had reached by t, so it never falls',
  MDD: 'the answer: the deepest the curve ever sat below its own peak',
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

export default function DrawdownView({
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

  /**
   * WHAT EVERY CARD HERE IS MEASURED FROM, AND OVER WHAT WINDOW — built once.
   *
   * ⚠ THE SAME TWO STRINGS THE OTHER THREE RISK VIEWS BUILD, from one `build_paired_series`.
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
                where={where}
                when={when}
                worked={data.max_drawdown_pct == null ? '' : withWorked(
                  // ⚠ ONE STATEMENT PER LINE, BROKEN HERE RATHER THAN BY THE BOX. The formula
                  // block honours `\n`; left as one line it wraps at whatever operator lands on
                  // the 22rem edge, which is harder to read than no formula at all.
                  String.raw`W_t = \prod_s (1 + R_s)\qquad M_t = \max_{s \le t} W_s\qquad MDD = \min_t \left( \dfrac{W_t}{M_t} - 1 \right)`,
                  String.raw`\text{${day(worst?.peak_date)}} \;\rightarrow\; \text{${day(worst?.trough_date)}}`
                  + String.raw` \;=\; ${subNum(data.max_drawdown_pct, 2)}\%`)}
                legend={[
                  { sym: 'R_s', is: LEGEND.Rs(portfolioName) },
                  { sym: 'W_t', is: LEGEND.W },
                  { sym: 'M_t', is: LEGEND.M },
                  { sym: String.raw`MDD`, is: LEGEND.MDD },
                ]}
                /* ⚠ NO `how`. The cadence point is made three times over already: the label
                   carries the frequency, each cadence button's title says what coarsening costs,
                   and the comparison table below measures it on this book rather than asserting
                   it. A fourth statement in a tooltip was the only one nobody could act on. */
                />} />} />
            <Tile label={`${data.benchmark} max drawdown`}
              value={pct2(data.benchmark_max_drawdown_pct)} tone="text-fg-muted"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={`The same measurement run over ${data.benchmark}'s own tracker instead of `
                  + `${portfolioName} — same formula, same periods, so the two are directly `
                  + 'comparable.'}
                where={where}
                when={when}
                how={'For scale. ⚠ It carries none of this book\'s survivorship bias — the index '
                  + 'kept its fallers — so the gap between the two flatters the book.'} />} />} />
            {/* ⚠⚠ THE VALUE IS THE FILTERED COUNT, and it did not used to be. The label has
                always promised "over 5%" while the tile showed `episodes_total`, which counts
                every peak-to-recovery cycle including a bad afternoon that came back the next
                session — 68 of them here against 6 real falls. A heading that names a
                threshold the number does not apply is worse than no threshold. */}
            <Tile label={`Falls over ${Math.abs(data.episode_threshold_pct ?? 5).toFixed(0)}%`}
              value={`${data.episodes_over_threshold ?? 0}`} tone="text-fg-muted"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={`How many distinct falls of at least `
                  + `${Math.abs(data.episode_threshold_pct ?? 5).toFixed(0)}% the sleeve had. A `
                  + 'fall opens when the wealth curve leaves a high-water mark and closes only '
                  + 'when it regains it, so a slide that bounces part-way and drops again is ONE '
                  + `fall, not two. ${v(data.episodes_total ?? 0)} cycles in all once every `
                  + 'shallower dip is counted too.'}
                where={where}
                when={when}
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
            {/* ⚠ THE BOOK IS NAMED AND THE WINDOW IS DATED — same fix as the tracking-error and
                volatility footnotes. "Today's weights over 5 years" asserted a start date
                instead of reporting one, and the paired grid rarely reaches the full five. */}
            {`${portfolioName}'s stock sleeve at its current weights, priced from `}
            {`${data.window_from ?? 'an unrecorded start'} to ${data.window_to ?? 'an unrecorded end'} `}
            {`(${data.priced_holdings} of ${data.total_holdings} priced). `}
            Durations are in {unit} of the selected cadence, not calendar days.
          </p>
        </>
      )}
    </div>
  );
}
