'use client';

/**
 * CONCENTRATION — the Risk panel's sixth view.
 *
 *     C₁₀ = Σᵢ₌₁¹⁰ w₍ᵢ₎        HHI = Σ wᵢ²        N_eff = 1 / HHI
 *
 *  On companies, not on lines — the same folding Active share uses. Alphabet A + Alphabet C is
 * ONE position, and counting two would understate concentration exactly at the top, where the ten
 * largest are decided. Two views of the same book disagreeing about how many positions it holds
 * would be worse than either. (The code says `issuer` and still should — see the  in
 * `ActiveSharePanel` for why the screen says company and the fold does not.)
 *
 *  Both denominators are on screen. "Of the stock sleeve" is what compares across books and is
 * the panel's convention everywhere else; "of the whole book" is what is true in absolute terms for
 * a book carrying 30% cash. The choice genuinely changes the number, so making it silently would be
 * picking a side of a real question on the reader's behalf.
 *
 *  Hhi is the better measure and C₁₀ IS THE ONE PEOPLE ASK FOR, so both are here with N_eff given
 * the most prominent tile. A cut at exactly ten is arbitrary: two books with identical C₁₀ can be an
 * even ten-name portfolio and one dominated by its top three. N_eff has no cut-off and reads in
 * units anybody can hold in their head.
 */
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import SimpleExplanation, { useInfoFacts } from './SimpleExplanation';
import { useRiskCopy } from './riskCopy';
import LoadingDots from './LoadingDots';
import { v } from '../../../lib/dynamicValue';
import { dayOf, dayRange } from './asOfLine';
import type { SourceKey } from '../../../lib/provenance';
import type { PortfolioConcentration } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';
import { riskRequestKey, useRiskResult } from './useRiskResult';

/**  TWO DECIMALS ON EVERY NON-INTEGER, ACROSS ALL SEVEN VIEWS. One decimal read as false
 *  precision on a figure the reader is asked to check against a table that carries two: "79.5%"
 *  beside rows summing to 79.53 invites the arithmetic to be redone and found wrong. Counts
 *  (issuers, observations, lines, periods) stay integers — they ARE integers. */
const pct2 = (n: number | null | undefined) => (n == null ? '—' : `${n.toFixed(2)}%`);
const TOP_POSITIONS = 10;

function Tile({ label, value, sub, tone, info }: {
  label: string; value: string; sub?: string; tone?: string; info?: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-neutral-800/40 bg-elevated px-4 py-2.5 min-w-[8.5rem]">
      <div className="text-sm text-fg-faint flex items-center gap-1">
        {label}{info}
      </div>
      <div className={`font-mono text-2xl tabular-nums ${tone ?? 'text-fg-strong'}`}>{value}</div>
      {sub && <div className="text-[10px] text-fg-faint">{sub}</div>}
    </div>
  );
}

export default function ConcentrationView({
  holdings, benchmark, benchmarkStart, portfolioName, portfolioAsOf, portfolioFetchedAt,
  benchmarkWeights,
}: {
  holdings: ActiveShareHolding[];
  benchmark: string;
  benchmarkStart?: string | null;
  /** The book's identity, forwarded from the panel — see `ActiveSharePanel`'s own props. */
  portfolioName: string;
  portfolioAsOf?: string | null;
  portfolioFetchedAt?: string | null;
  portfolioSource: SourceKey;
  /** Fixed Stocks/Bonds policy for a risk-profile benchmark. */
  benchmarkWeights?: Record<string, number>;
}) {
  const t = useRiskCopy();
  const facts = useInfoFacts();
  const requestUrl = `${API_URL}/api/airs/portfolio/concentration?benchmark=${encodeURIComponent(benchmark)}&view=whole-portfolio-weights-v6`
    + `${benchmarkStart ? `&benchmark_start=${encodeURIComponent(benchmarkStart)}` : ''}`;
  const requestBody = JSON.stringify({ holdings, benchmark_weights: benchmarkWeights });
  const key = riskRequestKey(requestUrl, requestBody);
  const { data, error } = useRiskResult<PortfolioConcentration>(key, async () => {
    const r = await apiFetch(requestUrl, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: requestBody,
    });
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    return b as PortfolioConcentration;
  }, 'concentration', 'the concentration could not be computed');

  const rows = data?.top ?? [];
  const benchmarkRows = data?.benchmark_top ?? [];

  /**
   *  The book's date alone for its own three cards — no index appears in them, so dating the
   * caps beside them would date a side the number does not contain. Same split as the Stocks tile
   * on Active share.
   */
  const whenBook = `${v(portfolioName)} weights: ${v(dayOf(portfolioAsOf) ?? 'no recorded date')}`
    + `${dayOf(portfolioFetchedAt) && dayOf(portfolioFetchedAt) !== dayOf(portfolioAsOf)
      ? ` (read ${v(dayOf(portfolioFetchedAt))})` : ''}`;

  /** Both sides, for the one card that weights the index. */
  const whenBoth = data?.available
    ? `${whenBook}\n${v(data.benchmark)} weights based on marketcap from yfinance: `
      + `${v(dayRange(data.benchmark_caps_from, data.benchmark_caps_to) ?? 'no recorded date')}`
      + `${(data.benchmark_caps_unstamped ?? 0) > 0
        ? ` (${v(data.benchmark_caps_unstamped)} undated)` : ''}`
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
          <div className="flex flex-col items-center gap-2">
            <p className="text-sm font-medium text-fg-subtle text-center">{t.conc.question(portfolioName, benchmark)}</p>
            <div className="flex flex-wrap justify-center gap-2">
            <Tile label={t.conc.effective}
              value={data.effective_positions == null ? '—' : data.effective_positions.toFixed(2)}
              sub={`of ${data.issuers} companies held`}
              info={<InfoTip className="ml-0.5" content={<div className="space-y-2 text-xs">
                <p>{t.conc.effectiveCalculation}</p>
                {data.effective_positions != null && <p>{t.conc.effectiveMeaning(data.effective_positions.toFixed(2))}</p>}
                <p>{facts.weights(portfolioAsOf)}</p>
              </div>} />} />
            <Tile label={benchmark}
              value={data.benchmark_effective_positions == null ? '—'
                : data.benchmark_effective_positions.toFixed(2)}
              tone="text-fg-muted"
              sub={`of ${data.benchmark_issuers}`}
              info={<InfoTip className="ml-0.5" content={<div className="space-y-2 text-xs">
                <p>{t.conc.effectiveCalculation}</p>
                {data.benchmark_effective_positions != null && <p>{t.conc.effectiveMeaning(data.benchmark_effective_positions.toFixed(2))}</p>}
                {data.benchmark_covered_pct != null && <p>{facts.coverage(data.benchmark_covered_pct, benchmark)}</p>}
              </div>} />} />
            </div>
          </div>
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <div>
              <div className="mb-1 text-[10px] uppercase tracking-wider text-fg-faint">Top {TOP_POSITIONS} {portfolioName} positions</div>
              <table className="w-full text-[11px]">
                <thead><tr className="text-fg-faint [&>th]:py-1 [&>th]:font-medium"><th className="text-left">Company</th><th className="text-right">Weight</th></tr></thead>
                <tbody>{rows.slice(0, TOP_POSITIONS).map((row) => <tr key={row.name} className="[&>td]:border-t [&>td]:border-neutral-800/20 [&>td]:py-1">
                  <td className="text-fg-soft">{row.name || `Position ${row.rank}`}</td>
                  <td className="text-right font-mono tabular-nums">
                    <span className="inline-flex items-center gap-1 whitespace-nowrap">
                      {pct2(row.weight_pct)}
                      <InfoTip content={<SimpleExplanation details={<p>{pct2(row.weight_pct)} · {facts.dated('AIRS', portfolioAsOf)}</p>}>{t.conc.companyWeightExplanation}</SimpleExplanation>} />
                    </span>
                  </td>
                </tr>)}</tbody>
              </table>
            </div>
            <div>
              <div className="mb-1 text-[10px] uppercase tracking-wider text-fg-faint">Top {TOP_POSITIONS} {data.benchmark} positions</div>
              <table className="w-full text-[11px]">
                <thead><tr className="text-fg-faint [&>th]:py-1 [&>th]:font-medium"><th className="text-left">Company</th><th className="text-right">Weight</th></tr></thead>
                <tbody>{benchmarkRows.slice(0, TOP_POSITIONS).map((row) => <tr key={row.name} className="[&>td]:border-t [&>td]:border-neutral-800/20 [&>td]:py-1">
                  <td className="text-fg-soft">{row.name || `${data.benchmark} constituent ${row.rank}`}</td>
                  <td className="text-right font-mono tabular-nums">
                    <span className="inline-flex items-center gap-1 whitespace-nowrap">
                      {pct2(row.weight_pct)}
                      <InfoTip content={<AspectCard
                        what={`${row.name}: ${pct2(row.weight_pct)} of ${data.benchmark}. Share classes of the same company are combined.`}
                        where={data.benchmark === 'ACWI'
                          ? 'Bundled iShares ACWI holdings workbook and stored Yahoo Finance prices.'
                          : `${data.benchmark} constituents and stored Yahoo Finance market capitalisations.`}
                        when={data.benchmark === 'ACWI'
                          ? 'Workbook snapshot rolled forward to the latest available prices.' : whenBoth}
                        how={data.benchmark === 'ACWI'
                          ? 'Each workbook weight is multiplied by its EUR price growth since the snapshot, then divided by the sum of all priced constituents’ grown weights × 100. Uses the same benchmark weights as Active Share.'
                          : 'Company market capitalisation divided by the total market capitalisation of the priced benchmark constituents × 100.'}
                      />} />
                    </span>
                  </td>
                </tr>)}</tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
