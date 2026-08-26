'use client';

/**
 * CORRELATION — the Risk panel's third view. `ρ(X,Y) = Cov(X,Y) / (σ_X · σ_Y)`.
 *
 * Two uses, both about RISK:
 *   · ρ between the POSITIONS — are these twenty names twenty bets, or one bet held twenty times?
 *   · ρ between the PORTFOLIO and the BENCHMARK — the same fact the tracking-error view reports,
 *     from the other side: `σ_a² = σ_p² + σ_b² − 2ρ σ_p σ_b`. Lower correlation with the index
 *     means higher tracking error, mechanically, and the panel prints both sides of that so it can
 *     be checked instead of believed.
 *
 * ⚠⚠ THIS IS NOT ATTRIBUTION AND IS DELIBERATELY NOT IN THE SAME PANEL AS IT. Attribution
 * (Brinson-Fachler) DECOMPOSES the active return — allocation, selection, interaction — into terms
 * that sum to it exactly. Correlation appears nowhere in that decomposition and sums to nothing.
 * Attribution says where the excess came FROM; correlation says how far the book can diverge AT
 * ALL. Putting them in one view would imply they reconcile, and they are not that kind of number —
 * so Attribution keeps its own dialog and this stays here, beside the other two risk measures.
 *
 * ⚠ THE CELL PALETTE IS `CorrelationMatrix`'s, NOT A NEW ONE. Blue (accent) for NEGATIVE, amber
 * (warn) for POSITIVE, magnitude driving the tint — chosen there because red↔green is exactly the
 * pair that collapses under deuteranopia (the app's palette note measures blue+amber at ΔE 103
 * against that pair's 4.9). A second convention on a second matrix in the same app would be worse
 * than either.
 */
import { useEffect, useState } from 'react';
import type { CSSProperties } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { v } from '../../../lib/dynamicValue';
import { dayOf } from './asOfLine';
import { sourceField, sourceLabel, sourceVendor, type SourceKey } from '../../../lib/provenance';
import { traceError } from '../../../lib/debugTrace';
import { withWorked, subNum } from './workedFormula';
import type { RiskCorrelation } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';

const FREQS = [
  { key: 'weekly', label: 'Weekly' },
  { key: 'monthly', label: 'Monthly' },
  { key: 'daily', label: 'Daily' },
] as const;

const rho2 = (v: number | null | undefined) =>
  (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}`);

/** ⚠ THE SAME FUNCTION AS `CorrelationMatrix.cellStyle`, kept in step deliberately — see header. */
function cellStyle(v: number | null, isDiag: boolean): CSSProperties {
  const base: CSSProperties = { border: '1px solid var(--color-card)' };
  if (isDiag) return { ...base, background: 'var(--color-inset)' };
  if (v == null) return { ...base, background: 'var(--color-elevated)' };
  const a = Math.min(1, Math.abs(v));
  const pct = Math.round(8 + a * 64);
  const hue = v >= 0 ? 'var(--color-warn-500)' : 'var(--color-accent-500)';
  return { ...base, background: `color-mix(in srgb, ${hue} ${pct}%, transparent)` };
}

/**
 * EVERY SYMBOL THIS VIEW USES, DEFINED ONCE — same rule as `TrackingErrorView`'s own table.
 *
 * ⚠ `rho` APPEARS ON THREE OF THE FOUR CARDS (ρ itself, R² which squares it, and σₐ which is
 * built from it). Three hand-written definitions of one symbol inside one view is how a reader
 * learns the legend is decorative.
 */
const LEGEND = {
  pb: (bookName: string, bench: string | null | undefined) =>
    `${v(bookName)}'s and ${v(bench)}'s return series over the window`,
  cov: 'how far the two move together — positive when they rise and fall in the same periods',
  sigmas: 'the standard deviation of each series on its own',
  rho: (bench: string | null | undefined) =>
    `the correlation between the sleeve and ${v(bench)} — the first tile`,
  rhoAnswer: 'the answer: the covariance stripped of both scales, so it lands in −1…+1',
  // ⚠ IT SAYS WHY THE TWO ARE EQUAL, because the formula visibly provokes the question and the
  // identity is a theorem rather than a definition — it holds for ONE predictor and no more.
  r2: (bookName: string) =>
    `the answer: the share of ${v(bookName)}'s variance the index accounts for. ⚠ It EQUALS `
    + 'ρ² because there is only ONE predictor here: the best-fit slope is ρ·σₚ/σᵦ, so the '
    + "fitted values carry ρ²σₚ² of the variance and the σ's cancel. Regress on a second "
    + 'thing as well and the identity stops holding',
  // ⚠ IT NAMES THE MATRIX. The symbol was defined as "one PAIR of holdings", which is true and
  // told nobody that the thing being averaged is sitting on the same screen — reported as not
  // understood. A reader who can see the cell can check the average.
  rhoIJ: 'how one holding moved against one other — ONE CELL of the matrix below, with i and j '
    + 'its row and its column',
  // ⚠⚠ THE DIVISOR IS THE MEASURED COUNT, NOT n(n−1)/2. `mean_rho = sum(pairs)/len(pairs)` and
  // `pairs` holds only the cells that cleared the overlap floor, so writing the textbook
  // coefficient stated a denominator the computation does not use — wrong on any book holding
  // a recently-listed name, and silently so.
  m: 'how many cells went into the average — the pairs with enough shared history to measure. '
    + 'The Where line above says how many pairs there could have been, so a gap there is what '
    + 'was left out rather than measured on ten weeks of overlap',
  rhoBar: 'the answer: the average of those cells, UNWEIGHTED — it asks whether these NAMES are '
    + 'alike, not whether the big positions are; weighting by size would answer a different '
    + 'question and make a concentrated book look better diversified',
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

export default function CorrelationView({
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
  const [data, setData] = useState<RiskCorrelation | null>(null);
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
          `${API_URL}/api/airs/portfolio/risk-correlation`
          + `?benchmark=${encodeURIComponent(benchmark)}&frequency=${freq}`,
          { method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ holdings }) });
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setError(null);
        setData(b as RiskCorrelation);
      } catch (e) {
        traceError('risk-correlation', 'the correlations could not be computed', e);
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  const labels = data?.labels ?? [];
  /** Every pair the matrix COULD have — n(n−1)/2. ⚠ Printed beside the measured count so a
   *  book whose thin pairs were dropped shows the gap instead of hiding it in the divisor. */
  const pairsPossible = (labels.length * (labels.length - 1)) / 2;
  const matrix = data?.matrix ?? [];

  /**
   * WHAT EVERY CARD HERE IS MEASURED FROM, AND OVER WHAT WINDOW — built once.
   *
   * ⚠ THE SAME TWO STRINGS THE TRACKING-ERROR VIEW BUILDS, from the same fields of the same
   * `build_paired_series` payload. These two panels claim an identity between their headline
   * figures (σₐ² = σₚ² + σᵦ² − 2ρσₚσᵦ); quoting different windows or different sources for it
   * would undermine the one claim they exist to let a reader check.
   */
  const where = data?.available
    ? `${v(data.observations)} ${v(data.frequency)} returns on one aligned calendar, prices from `
      + `${v(sourceField('yfinance'))} at ${v(sourceVendor('yfinance'))}, weights from `
      + `${v(sourceLabel(portfolioSource))}, against ${v(data.benchmark)}'s tracker.`
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
              ? '⚠ Daily closes are not synchronous — the tracker closes at 16:30 London, a US '
                + 'holding at 21:00 — which mechanically LOWERS every correlation against it.'
              : undefined}
            className={`cursor-pointer rounded-md border px-2 py-0.5 text-[11px] transition-colors ${
              freq === x.key ? 'bg-accent-600 text-white border-transparent'
                : 'bg-elevated border-neutral-800/40 text-fg-muted hover:text-accent-300'}`}>
            {x.label}
          </button>
        ))}
      </div>

      {error && <p className="text-xs text-neg-300">{error}</p>}
      {!data && !error && <p className="text-xs text-fg-subtle">Computing correlations…</p>}
      {data && !data.available && <p className="text-xs text-fg-muted">{data.reason}</p>}

      {data?.available && (
        <>
          <div className="flex flex-wrap gap-2">
            <Tile label={`ρ vs ${data.benchmark}`} value={rho2(data.benchmark_corr)}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="How closely the stock sleeve has moved with the benchmark."
                where={where}
                when={when}
                worked={data.benchmark_corr == null ? '' : withWorked(
                  String.raw`\rho = \dfrac{\mathrm{Cov}(p,\, b)}{\sigma_p \,\sigma_b}`,
                  String.raw`\sigma_p = ${subNum(data.portfolio_vol_pct ?? 0, 2)}\%`
                  + String.raw`\quad \sigma_b = ${subNum(data.benchmark_vol_pct ?? 0, 2)}\%`
                  + String.raw`\quad\Rightarrow\quad \rho = ${data.benchmark_corr.toFixed(4)}`)}
                legend={[
                  { sym: 'p,\\; b', is: LEGEND.pb(portfolioName, data.benchmark) },
                  { sym: String.raw`\mathrm{Cov}(p,\, b)`, is: LEGEND.cov },
                  { sym: String.raw`\sigma_p,\; \sigma_b`, is: LEGEND.sigmas },
                  { sym: String.raw`\rho`, is: LEGEND.rhoAnswer },
                ]}
                /* ⚠ NO `how`. It carried the σₐ identity written out in Unicode — which the
                   Active-vol tile two along now sets properly in its own `worked`, from the same
                   numbers. One formula, typeset once, on the card whose figure it produces. */
                />} />} />
            <Tile label="R²" value={data.r_squared == null ? '—' : data.r_squared.toFixed(2)}
              tone="text-fg-muted"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The share of the book's movement the index explains."
                where={where}
                when={when}
                worked={data.r_squared == null || data.benchmark_corr == null ? '' : withWorked(
                  String.raw`R^2 = \rho^2`,
                  String.raw`${data.benchmark_corr.toFixed(4)}^2 = ${data.r_squared.toFixed(4)}`)}
                legend={[
                  { sym: String.raw`\rho`, is: LEGEND.rho(data.benchmark) },
                  { sym: String.raw`R^2`, is: LEGEND.r2(portfolioName) },
                ]}
                how={'ρ = 0.90 and "81% of the movement" are the same fact and land very '
                  + 'differently, which is why both are on screen.'} />} />} />
            <Tile label="Mean ρ between positions"
              value={rho2(data.mean_pair_corr)}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="How alike the holdings are to each other — the diversification check."
                where={`${v(data.pairs_measured)} of ${v(pairsPossible)} possible pairs — the ones with at least ${v(data.min_pair_observations)} overlapping returns, across ${v(labels.length)} holdings. Prices from ${v(sourceField('yfinance'))} at ${v(sourceVendor('yfinance'))}.`}
                when={when}
                /* ⚠⚠ THE SUM IS THE NUMERATOR IN BOTH HALVES. Written as `(1/m) Σ` the symbolic
                   line has a coefficient beside a sum while the substituted line is one
                   fraction — the same arithmetic, but a reader matching them finds 255.53
                   where the 1 should be and no Σ anywhere. Reported as not making sense, and
                   it did not: a worked line has to sit position-for-position under its
                   formula or it is two expressions, not one checked twice.
                   ⚠ AND `m` PRINTS AS AN INTEGER. `workedRatio` gives both operands two
                   decimals, which is right for money and wrong for a count — "946.00 pairs"
                   reads as a measurement, so the division is built here instead. */
                worked={data.pair_rho_sum == null || !data.pairs_measured ? '' : withWorked(
                  String.raw`\bar{\rho} = \dfrac{\sum_{\text{pairs}} \rho_{ij}}{m}`,
                  String.raw`\dfrac{${subNum(data.pair_rho_sum, 2)}}{${data.pairs_measured}}`
                  + ` = ${rho2(data.mean_pair_corr)}`)}
                legend={[
                  { sym: String.raw`\rho_{ij}`, is: LEGEND.rhoIJ },
                  { sym: 'm', is: LEGEND.m },
                  { sym: String.raw`\bar{\rho}`, is: LEGEND.rhoBar },
                ]}
                /* ⚠ NO `how`. The one fact it carried — that the mean is UNWEIGHTED, so this is a
                   question about the names rather than the sizes — is already the `rhoBar` legend
                   row, attached to the symbol it qualifies. */
                />} />} />
          </div>

          {data.cadence_note && <p className="text-[11px] text-warn-300">{data.cadence_note}</p>}
          {/* ⚠ THE RECONCILIATION, IN PUBLIC. It is ~1e-13 when all is well; a visible number here
              means the two views stopped measuring the same thing, which is exactly the failure a
              shared series exists to prevent and the last thing that should be silent. */}
          {(data.identity_gap_pp ?? 0) > 0.005 && (
            <p className="text-[11px] text-neg-300">
              {`⚠ σₐ measured and σₐ implied by ρ differ by ${data.identity_gap_pp!.toFixed(3)}pp — `}
              the two series are no longer identical. This is a bug, not a market fact.
            </p>
          )}

          {labels.length > 1 && (
            <>
              <div className="flex items-baseline gap-3 flex-wrap pt-1">
                <h5 className="text-xs font-medium text-fg-strong">Between the positions</h5>
                <span className="text-[11px] text-fg-faint">
                  Blue = diverging, amber = moving together. Ordered by weight.
                </span>
              </div>
              {/* ⚠ ITS OWN horizontal scroll — a 49-name matrix must never widen the dialog. */}
              <div className="overflow-auto">
                <table className="border-separate" style={{ borderSpacing: 0 }}>
                  <tbody>
                    {matrix.map((row, i) => (
                      <tr key={labels[i]}>
                        <th scope="row"
                          className="sticky left-0 z-10 bg-card pr-2 text-right text-[10px]
                            font-normal text-fg-muted whitespace-nowrap max-w-[12rem] truncate">
                          {labels[i]}
                        </th>
                        {row.map((v, j) => (
                          <td key={labels[j]} style={cellStyle(v, i === j)}
                            title={`${labels[i]} × ${labels[j]}: ${v == null ? 'too few overlapping returns' : rho2(v)}`}
                            className="w-4 h-4 min-w-4" />
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 pt-1">
                <div>
                  <h6 className="text-[11px] font-medium text-fg-muted mb-1">
                    Least correlated — what actually diversifies
                  </h6>
                  {(data.least_correlated ?? []).map((p) => (
                    <div key={`${p.a}|${p.b}`} className="flex justify-between gap-2 text-[11px] py-0.5">
                      <span className="text-fg-soft truncate">{p.a} × {p.b}</span>
                      <span className="font-mono tabular-nums text-fg-muted shrink-0">{rho2(p.rho)}</span>
                    </div>
                  ))}
                </div>
                <div>
                  <h6 className="text-[11px] font-medium text-fg-muted mb-1">
                    Most correlated — one bet held twice
                  </h6>
                  {(data.most_correlated ?? []).map((p) => (
                    <div key={`${p.a}|${p.b}`} className="flex justify-between gap-2 text-[11px] py-0.5">
                      <span className="text-fg-soft truncate">{p.a} × {p.b}</span>
                      <span className="font-mono tabular-nums text-fg-muted shrink-0">{rho2(p.rho)}</span>
                    </div>
                  ))}
                </div>
              </div>
            </>
          )}

          <p className="text-[11px] text-fg-faint leading-relaxed">
            {`Today's stock sleeve at today's weights over ${data.years} years `}
            {`(${data.priced_holdings} of ${data.total_holdings} priced). `}
            A pair with fewer than {data.min_pair_observations} overlapping returns is left blank
            rather than tinted — over ten weeks a correlation is noise with a sign, and a coloured
            cell looks exactly as authoritative as one measured over five years.
          </p>
        </>
      )}
    </div>
  );
}
