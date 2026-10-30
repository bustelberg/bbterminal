'use client';

/**
 * Max drawdown — the Risk panel's fifth view.
 *
 *     Wₜ = ∏(1 + Rₛ)    Mₜ = max_{s≤t} Wₛ    DDₜ = Wₜ/Mₜ − 1    MDD = min DDₜ
 *
 *  This is the reconstruction, not the client's drawdown, and the panel says so where it cannot
 * be missed. It rebuilds a series from the holdings as they stand TODAY, so it carries look-ahead
 * bias (those weights were chosen with hindsight) and survivorship bias (names since sold are
 * absent — and the sold ones skew towards the fallers). The number for a client report is the one
 * from the AIRS returns, with real trades, real costs and real timing. Two different figures, not
 * interchangeable.
 *
 *  Daily only. A drawdown compares a series with itself, so there is no cross-market close-time
 * bias to trade off. Coarsening instead hides a fall that recovers inside a week or month.
 *
 *  And the percentage is the least useful part. "−31.4%" is one number; "peaked 19 Feb, bottomed
 * 7 Apr after 33 days, back to level 12 Aug after another 91" is a conversation.
 */
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import InfoTip from '../InfoTip';
import SimpleExplanation, { useInfoFacts } from './SimpleExplanation';
import { useRiskCopy } from './riskCopy';
import LoadingDots from './LoadingDots';
import type { SourceKey } from '../../../lib/provenance';
import type { PortfolioDrawdown } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';
import { riskRequestKey, useRiskResult } from './useRiskResult';
import { riskHistoryQuery } from './riskHistoryWindow';

const pct2 = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);
const day = (d: string | null | undefined) =>
  (!d ? '—' : new Date(`${d}T00:00:00`).toLocaleDateString(undefined,
    { day: 'numeric', month: 'short', year: 'numeric' }));

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

export default function DrawdownView({
  holdings, benchmark, benchmarkWeights, portfolioName, portfolioAsOf,
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
  const requestUrl = `${API_URL}/api/airs/portfolio/drawdown`
    + `?benchmark=${encodeURIComponent(benchmark)}&frequency=daily&${riskHistoryQuery()}`;
  const requestBody = JSON.stringify({ holdings, benchmark_weights: benchmarkWeights });
  const key = riskRequestKey(requestUrl, requestBody);
  const { data, error } = useRiskResult<PortfolioDrawdown>(key, async () => {
    const r = await apiFetch(requestUrl, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: requestBody, noReadCache: true,
    });
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    return b as PortfolioDrawdown;
  }, 'drawdown', 'the drawdown could not be computed');

  const topEpisodes = [...(data?.episodes ?? [])]
    .sort((a, b) => a.depth_pct - b.depth_pct)
    .slice(0, 10);


  return (
    <div className="space-y-3">
      {error && <p className="text-xs text-neg-300">{error}</p>}
      {!!data?.excluded_holdings?.length && <details className="text-xs text-fg-muted"><summary>Missing history · {data.excluded_holdings.length} excluded holdings</summary><p>{data.excluded_holdings.join(', ')}</p></details>}
      {!data && !error && (
        <div className="absolute inset-0 grid place-items-center text-center px-6" role="status">
          <p className="text-2xl font-medium text-fg-strong">{t.common.computing} <LoadingDots /></p>
        </div>
      )}
      {data && !data.available && <p className="text-xs text-fg-muted" title={data.reason ?? undefined}>{/benchmark.*history/i.test(data.reason ?? '')
        ? <>Waiting for Yahoo benchmark history; retrying automatically… <LoadingDots /></> : data.reason}</p>}

      {data?.available && (
        <>
          <div className="flex flex-col items-center gap-2">
            <p className="text-sm font-medium text-fg-subtle text-center">
              What would have been the worst drawdown of {portfolioName} since 1 January 2021?
            </p>
            <Tile label={t.dd.maxDrawdown(data.frequency)} value={pct2(data.max_drawdown_pct)}
              tone="text-neg-300"
              info={<InfoTip className="ml-0.5" content={<SimpleExplanation details={<>
                {data.worst && <p>{facts.worst(data.worst.peak_date, data.worst.trough_date, data.max_drawdown_pct)}</p>}
                <p>{facts.sample(data.window_from, data.window_to, data.observations, data.frequency)}</p>
                <p>{facts.weights(portfolioAsOf)}</p>
              </>}>{t.dd.cards.maxDrawdown.how}</SimpleExplanation>} />} />
          </div>

          {topEpisodes.length > 0 && (
            <div>
              <div className="text-[10px] uppercase tracking-wider text-fg-faint mb-1">
                10 worst drawdowns
              </div>
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-fg-faint [&>th]:py-1 [&>th]:font-medium">
                    <th className="text-left">{t.dd.peak}</th>
                    <th className="text-left">{t.dd.trough}</th>
                    <th className="text-right">{t.dd.colDepth}</th>
                    <th className="text-right">Fall (trading days)</th>
                    <th className="text-right">Recovery (trading days)</th>
                  </tr>
                </thead>
                <tbody>
                  {topEpisodes.map((e) => (
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
                          : <span className="text-warn-300">{t.dd.open}</span>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <p className="text-[11px] text-fg-faint leading-relaxed">
            {`Data window: ${data.window_from ?? 'unknown'} to ${data.window_to ?? 'unknown'}; `}
            {`${data.priced_holdings} of ${data.total_holdings} holdings priced.`}
          </p>
        </>
      )}
    </div>
  );
}
