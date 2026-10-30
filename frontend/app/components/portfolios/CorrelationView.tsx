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
 *  This is not attribution and is deliberately not in the same panel as it. Attribution
 * (Brinson-Fachler) DECOMPOSES the active return — allocation, selection, interaction — into terms
 * that sum to it exactly. Correlation appears nowhere in that decomposition and sums to nothing.
 * Attribution says where the excess came FROM; correlation says how far the book can diverge AT
 * ALL. Putting them in one view would imply they reconcile, and they are not that kind of number —
 * so Attribution keeps its own dialog and this stays here, beside the other two risk measures.
 *
 *  The cell palette is `CorrelationMatrix`'s, NOT A NEW ONE. Blue (accent) for NEGATIVE, amber
 * (warn) for POSITIVE, magnitude driving the tint — chosen there because red↔green is exactly the
 * pair that collapses under deuteranopia (the app's palette note measures blue+amber at ΔE 103
 * against that pair's 4.9). A second convention on a second matrix in the same app would be worse
 * than either.
 */
import { useEffect, useState } from 'react';
import type { CSSProperties } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import InfoTip from '../InfoTip';
import SimpleExplanation, { useInfoFacts } from './SimpleExplanation';
import { useRiskCopy } from './riskCopy';
import LoadingDots from './LoadingDots';
import type { SourceKey } from '../../../lib/provenance';
import type { RiskCorrelation } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';
import { riskRequestKey, useRiskResult } from './useRiskResult';
import { invalidateReadCache } from '../../../lib/readCache';
import { startJob } from '../../../lib/stores/jobs';
import { AUTOMATIC_RISK_REPAIRS, riskHistoryQuery, riskHistoryYears } from './riskHistoryWindow';

// A repair may conclude that an instrument genuinely only began trading recently.  Start one
// repair per exact sleeve during this browser session, rather than spending vendor calls again
// every time the reader reopens the still-honest preliminary view.
const startedPreliminaryRepairs = new Set<string>();

const rho2 = (v: number | null | undefined) =>
  (v == null ? '—' : v.toFixed(2));

/**  THE SAME FUNCTION AS `CorrelationMatrix.cellStyle`, kept in step deliberately — see header. */
function cellStyle(v: number | null, isDiag: boolean): CSSProperties {
  const base: CSSProperties = { border: '1px solid var(--color-card)' };
  if (isDiag) return { ...base, background: 'var(--color-inset)' };
  if (v == null) return { ...base, background: 'var(--color-elevated)' };
  const a = Math.min(1, Math.abs(v));
  const pct = Math.round(8 + a * 64);
  const hue = v >= 0 ? 'var(--color-warn-500)' : 'var(--color-accent-500)';
  return { ...base, background: `color-mix(in srgb, ${hue} ${pct}%, transparent)` };
}

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

export default function CorrelationView({
  holdings, portfolioName, portfolioAsOf,
}: {
  holdings: ActiveShareHolding[];
  benchmark: string;
  benchmarkWeights?: Record<string, number>;
  /** The book's identity, forwarded from the panel — see `ActiveSharePanel`'s own props. */
  portfolioName: string;
  portfolioAsOf?: string | null;
  portfolioFetchedAt?: string | null;
  portfolioSource: SourceKey;
}) {
  const t = useRiskCopy();
  const facts = useInfoFacts();
  const benchmark = 'ACWI';
  const stockHoldings = holdings.filter((holding) =>
    !holding.is_fund
    && (!holding.bucket || holding.bucket === 'Equity')
    && !/\b(etf|etp|etn|ucits|fund|fonds|certificate|certificaten|index|perp|bond|obligat|coupon|treasur|gilt|bund|govie|sovereign|floating\s*rate|frn|high\s*yield|fixed\s*income)\b|\d[.,]?\d*\s*%/i.test(holding.name ?? ''),
  );
  const totalStockWeight = stockHoldings.reduce((sum, holding) => sum + (holding.weight_pct ?? 0), 0);
  const correlationHoldings = stockHoldings.map(holding => ({
    ...holding, weight_pct: totalStockWeight > 0 ? (holding.weight_pct ?? 0) / totalStockWeight * 100 : 0,
  }));
  const requestUrl = `${API_URL}/api/airs/portfolio/risk-correlation`
    + `?benchmark=${encodeURIComponent(benchmark)}&frequency=weekly&${riskHistoryQuery()}`;
  const requestBody = JSON.stringify({ holdings: correlationHoldings, benchmark_weights: { Stocks: 100, Bonds: 0 } });
  const repairIdentity = riskRequestKey(requestUrl, requestBody);
  const [repairVersion, setRepairVersion] = useState(0);
  // A completed repair invalidates the shared read cache and advances this key, causing this one
  // view to ask again immediately instead of waiting for the reader to close and reopen the tab.
  const key = `${repairIdentity}\nrepair:${repairVersion}`;
  const { data, error } = useRiskResult<RiskCorrelation>(key, async () => {
    const r = await apiFetch(requestUrl, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: requestBody, noReadCache: true,
    });
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    return b as RiskCorrelation;
  }, 'risk-correlation', 'the correlations could not be computed');

  useEffect(() => {
    if (!AUTOMATIC_RISK_REPAIRS || !data?.available || (!data.preliminary && !data.excluded_holdings?.length)
      || startedPreliminaryRepairs.has(repairIdentity)) return;
    startedPreliminaryRepairs.add(repairIdentity);
    void startJob(
      `${API_URL}/api/airs/portfolio/risk-correlation/repair/job?benchmark=${encodeURIComponent(benchmark)}&years=${riskHistoryYears()}&full_history=true&yahoo_only=true`,
      'Repairing risk price history',
      { headers: { 'Content-Type': 'application/json' }, body: requestBody },
    ).then(async ({ done }) => {
      const job = await done;
      if (job.status === 'done') {
        invalidateReadCache('risk correlation history repaired');
        setRepairVersion((version) => version + 1);
      }
    }).catch(() => {
      // `startJob` reports the failure through the standard job popup.  The preliminary numbers
      // remain visible; a failed repair must not replace actual observed data with an error state.
    });
  }, [benchmark, data?.available, data?.preliminary, data?.excluded_holdings?.length, repairIdentity, requestBody]);

  const labels = data?.labels ?? [];
  /** Every pair the matrix COULD have — n(n−1)/2.  Printed beside the measured count so a
   *  book whose thin pairs were dropped shows the gap instead of hiding it in the divisor. */
  const matrix = data?.matrix ?? [];


  return (
    <div className="mx-auto w-full max-w-6xl space-y-4">
      <p className="text-xs text-fg-muted">Since 1 January 2021 · weekly returns in EUR · through the latest available close</p>
      {!!data?.excluded_holdings?.length && <details className="text-xs text-fg-muted"><summary>Missing history · {data.excluded_holdings.length} excluded holdings</summary><p>{data.excluded_holdings.join(', ')}</p></details>}

      {error && <p className="text-xs text-neg-300">{error}</p>}
      {!data && !error && (
        <div className="grid min-h-40 place-items-center text-center px-6" role="status">
          <p className="text-2xl font-medium text-fg-strong">{t.common.computing} <LoadingDots /></p>
        </div>
      )}
      {data && !data.available && <p className="text-xs text-fg-muted">{data.reason}</p>}

      {data?.available && (
        <>
          <div className="flex flex-wrap justify-center gap-3">
            <Tile label={`${portfolioName} — ${t.corr.rhoVs('ACWI')}`} value={rho2(data.benchmark_corr)}
              info={<InfoTip className="ml-0.5" content={<SimpleExplanation details={<>
                <p>{facts.sample(data.window_from, data.window_to, data.observations, data.frequency)}</p>
                <p>{facts.weights(portfolioAsOf)}</p>
              </>}>{t.corr.cards.rho.how}</SimpleExplanation>} />} />
            <Tile label={t.corr.meanPair}
              value={rho2(data.mean_pair_corr)}
              info={<InfoTip className="ml-0.5" content={<SimpleExplanation details={<>
                <p>{facts.sample(data.window_from, data.window_to, data.observations, data.frequency)}</p>
                <p>{facts.pairs(data.pairs_measured, labels.length * (labels.length - 1) / 2, data.min_pair_observations)}</p>
              </>}>{t.corr.cards.meanPair.how}</SimpleExplanation>} />} />
          </div>

          {data.cadence_note && <p className="text-[11px] text-warn-300">{data.cadence_note}</p>}
          {/*  THE RECONCILIATION, IN PUBLIC. It is ~1e-13 when all is well; a visible number here
              means the two views stopped measuring the same thing, which is exactly the failure a
              shared series exists to prevent and the last thing that should be silent. */}
          {(data.identity_gap_pp ?? 0) > 0.005 && (
            <p className="text-[11px] text-neg-300">
              {` σₐ measured and σₐ implied by ρ differ by ${data.identity_gap_pp!.toFixed(3)}pp — `}
              the two series are no longer identical. This is a bug, not a market fact.
            </p>
          )}

          {labels.length > 1 && (
            <>
              {/*  ITS OWN horizontal scroll — a 49-name matrix must never widen the dialog.
                   THE RIGHT PADDING IS THE TILT'S OVERHANG, not spacing. A 45° label anchored to
                  the LAST column extends up and to the RIGHT of the table box; without room for it
                  the scroll container clips the final holding's name, which is the one a reader is
                  most likely to be hunting for. 7.5rem of text at 45° projects ~5.3rem across. */}
              <div className="flex min-w-0 flex-col gap-5">
              <div className="order-2 min-w-0">
              <p className="mb-3 text-xs text-fg-muted">Grouped by similar returns · hierarchical clustering</p>
              <div className="mx-auto w-fit max-w-full overflow-auto pr-[5.5rem]">
                <table className="border-separate" style={{ borderSpacing: 0 }}>
                  {/*  THE COLUMN NAMES, ROTATED — added on request 2026-08-31. A cell is 16px
                      wide, so horizontal labels were never an option and the matrix shipped with
                      one axis unlabelled: every column had to be counted off against the rows to
                      be identified, which on a 49-name book is not something anybody does.
                       TILTED 45°, NOT VERTICAL — changed on request 2026-09-01 ("tilt the names a
                      little so we can read those better"). They were `vertical-rl` + `rotate-180`,
                      i.e. a full 90°, which is legible only one word at a time: the eye has no
                      baseline to run along, so reading a column meant tilting your head. 45° is the
                      angle the rest of the app already tilts at (`lib/chartAxis.ts`, where it was
                      measured as the point at which returns stop), so this is the house angle
                      rather than a new one.
                       AND THE TILT COSTS THE OLD GEOMETRIC GUARANTEE, which is why the cap
                      matters more now than it did. At 90° a label occupied exactly its own 16px
                      column and could never touch its neighbour however long the name was. At 45°
                      the labels are parallel lines 16·sin45° = 11.3px apart — clear of one another
                      at 10px type, but each one now extends beyond its own column, so the length
                      has to be bounded in CSS instead of by the geometry.
                       `max-w-[7.5rem]` + `truncate` IS THAT BOUND, and it is set FROM the header
                      height: 5.5rem of height admits 5.5/sin45° = 7.8rem of text, so a longer name
                      would be clipped by the header box at an arbitrary point rather than
                      ellipsised at a deliberate one. It shows ~40% more of each name than the
                      vertical version did in the same height. The full name stays on `title`.
                       THE PIVOT IS THE CELL'S OWN CENTRE (`left-1/2` + `origin-bottom-left`), so a
                      name starts at the column it belongs to and rises away from it. Anchoring the
                      END of the text at the column instead would put the first characters — the
                      ones that identify a holding — furthest from it, AND would tilt the names out
                      over the LEFT edge, where the sticky row-label column and the sticky corner
                      already sit at `z-10`/`z-20` and would slide across them on any sideways
                      scroll. Leaning right is the only side of this table that is free. */}
                  <thead>
                    <tr>
                      {/* The corner.  Sticky like the row labels AND above them (`z-20`), or it
                          slides under them the moment the matrix is scrolled sideways. */}
                      <th className="sticky left-0 z-20 bg-card" />
                      {labels.map((name) => (
                        <th key={name} scope="col" title={name}
                          className="relative p-0 align-bottom h-[7rem] w-5 min-w-5">
                          <span className="absolute bottom-1 left-1/2 block origin-bottom-left
                                           -rotate-45 max-w-[9rem] truncate text-left
                                           text-xs font-normal text-fg-muted">
                            {name}
                          </span>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {matrix.map((row, i) => (
                      <tr key={labels[i]}>
                        <th scope="row"
                          className="sticky left-0 z-10 bg-card pr-2 text-right text-xs
                            font-normal text-fg-muted whitespace-nowrap max-w-[12rem] truncate">
                          {labels[i]}
                        </th>
                        {row.map((v, j) => (
                          <td key={labels[j]} style={cellStyle(v, i === j)}
                            title={`${labels[i]} × ${labels[j]}: ${v == null ? 'too few overlapping returns' : rho2(v)}`}
                            className="w-5 h-5 min-w-5 text-center text-sm leading-none text-black">
                            {v == null && i !== j ? '×' : null}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="mt-3 flex flex-wrap items-center justify-center gap-x-4 gap-y-2 text-xs text-fg-muted" aria-label="Correlation matrix legend">
                {([-1, 0, 1] as const).map((value) => (
                  <span key={value} className="inline-flex items-center gap-1.5">
                    <span className="h-3 w-3 rounded-sm" style={cellStyle(value, false)} aria-hidden />
                    {value === -1 ? '−1 Opposite movement' : value === 0 ? '0 No correlation' : '+1 Moving together'}
                  </span>
                ))}
                <span className="inline-flex items-center gap-1.5">
                  <span className="inline-flex h-3 w-3 items-center justify-center text-sm leading-none text-black" aria-hidden>×</span>
                  Insufficient data
                </span>
              </div>
              </div>

              <div className="order-1 grid w-full min-w-0 grid-cols-1 gap-4 pt-1 sm:grid-cols-2 [&>div]:min-w-0 [&>div]:rounded-lg [&>div]:border [&>div]:border-neutral-800/40 [&>div]:bg-elevated [&>div]:p-4">
                <div>
                  <h6 className="text-center text-base font-medium text-fg-muted mb-3">
                    Least correlated — what actually diversifies
                  </h6>
                  {(data.least_correlated ?? []).map((p) => (
                    <div key={`${p.a}|${p.b}`} className="flex justify-between gap-3 text-sm py-1">
                      <span className="text-fg-soft truncate">{p.a} × {p.b}</span>
                      <span className="font-mono tabular-nums text-fg-muted shrink-0">{rho2(p.rho)}</span>
                    </div>
                  ))}
                </div>
                <div>
                  <h6 className="text-center text-base font-medium text-fg-muted mb-3">
                    Most correlated — one bet held twice
                  </h6>
                  {(data.most_correlated ?? []).map((p) => (
                    <div key={`${p.a}|${p.b}`} className="flex justify-between gap-3 text-sm py-1">
                      <span className="text-fg-soft truncate">{p.a} × {p.b}</span>
                      <span className="font-mono tabular-nums text-fg-muted shrink-0">{rho2(p.rho)}</span>
                    </div>
                  ))}
                </div>
              </div>
              </div>
            </>
          )}

        </>
      )}
    </div>
  );
}
