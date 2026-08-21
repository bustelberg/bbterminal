'use client';

/**
 * VOLATILITY — the Risk panel's fourth view. `σ_p = √(Σ(Rₜ − R̄)²/(T−1)) · √f`.
 *
 * The same construction as the tracking error, on the book's OWN returns rather than on the
 * difference — and the same `σ_p` the correlation view puts inside `σₐ² = σₚ² + σᵦ² − 2ρσₚσᵦ`. One
 * series, one function; two figures a click apart that disagreed would tell the reader that one of
 * them is wrong and nothing about which.
 *
 * ⚠⚠ NO CASH-FLOW CONTAMINATION, BY CONSTRUCTION RATHER THAN BY CHAIN-LINKING. The usual hazard is
 * measuring risk off an ACCOUNT VALUE: a deposit reads as a huge gain and a withdrawal as a crash,
 * so a book that merely received money looks turbulent. Time-weighted returns exist to strip that
 * out. This series never has flows in it — it is a weighted basket of instrument price returns, so
 * money moving in or out of the account changes nothing in it.
 *
 * ⚠ THE PRICE IS THE OTHER CAVEAT AND THE PANEL STATES IT: today's weights, carried backwards. So
 * this is the volatility of the portfolio AS IT STANDS, not the one the client lived through.
 *
 * ⚠ DOWNSIDE DEVIATION IS SORTINO'S, not the semi-deviation — divided by ALL n, against a target of
 * 0. Both are called "downside deviation"; this is the one the Sortino beside it is built on, so
 * the ratio equals its own parts.
 */
import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { traceError } from '../../../lib/debugTrace';
import { withWorked, subNum } from './workedFormula';
import type { PortfolioVolatility } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';

const FREQS = [
  { key: 'weekly', label: 'Weekly' },
  { key: 'monthly', label: 'Monthly' },
  { key: 'daily', label: 'Daily' },
] as const;

const pct2 = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);
const signed2 = (v: number | null | undefined) =>
  (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`);

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

export default function VolatilityView({ holdings, benchmark }: {
  holdings: ActiveShareHolding[];
  benchmark: string;
}) {
  const [data, setData] = useState<PortfolioVolatility | null>(null);
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
          `${API_URL}/api/airs/portfolio/volatility`
          + `?benchmark=${encodeURIComponent(benchmark)}&frequency=${freq}`,
          { method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ holdings }) });
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setError(null);
        setData(b as PortfolioVolatility);
      } catch (e) {
        traceError('volatility', 'the volatility could not be computed', e);
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  const period = data?.frequency === 'daily' ? 'day'
    : data?.frequency === 'monthly' ? 'month' : 'week';

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-[11px] text-fg-faint">Measured</span>
        {FREQS.map((x) => (
          <button key={x.key} type="button" onClick={() => setFreq(x.key)}
            className={`cursor-pointer rounded-md border px-2 py-0.5 text-[11px] transition-colors ${
              freq === x.key ? 'bg-accent-600 text-white border-transparent'
                : 'bg-elevated border-neutral-800/40 text-fg-muted hover:text-accent-300'}`}>
            {x.label}
          </button>
        ))}
      </div>

      {error && <p className="text-xs text-neg-300">{error}</p>}
      {!data && !error && <p className="text-xs text-fg-subtle">Computing volatility…</p>}
      {data && !data.available && <p className="text-xs text-fg-muted">{data.reason}</p>}

      {data?.available && (
        <>
          <div className="flex flex-wrap gap-2">
            <Tile label="Volatility (ann.)" value={pct2(data.volatility_pct)}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="How much the sleeve's own return has varied, annualised."
                where={`${data.observations} ${data.frequency} returns over ${data.years} years.`}
                worked={data.volatility_pct == null ? '' : withWorked(
                  'σ = √( Σ(Rₜ − R̄)² ÷ (T − 1) ) × √f',
                  `T = ${data.observations}, f = ${data.periods_per_year}`
                  + `  →  ${subNum(data.volatility_pct, 2)}%`)}
                how={'⚠⚠ NO CASH FLOWS IN IT, and not because they were chain-linked out — this is '
                  + 'a weighted basket of instrument price returns, not an account value, so a '
                  + 'deposit or withdrawal is simply not in the series. ⚠ Same σₚ the Correlation '
                  + 'view uses in σₐ² = σₚ² + σᵦ² − 2ρσₚσᵦ.'} />} />} />
            <Tile label="Downside deviation" value={pct2(data.downside_dev_pct)}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The same spread, counting only the periods that lost money."
                where={`√( mean( min(Rₜ, 0)² ) ) × √f — target 0, over all ${data.observations} periods.`}
                how={'⚠ SORTINO\'S CONVENTION, not the semi-deviation (below-MEAN observations '
                  + 'only, divided by how many there are), which reads higher. Both are called '
                  + '"downside deviation"; this is the one the Sortino below is built on. '
                  + 'Volatility punishes a good month exactly as hard as a bad one; this does not, '
                  + 'which is closer to how the loss is actually experienced.'} />} />} />
            <Tile label={`${data.benchmark} volatility`} value={pct2(data.benchmark_volatility_pct)}
              tone="text-fg-muted"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The index's own volatility, on the same periods."
                where="Same function, same series, different input."
                how={'⚠ FOR SCALE, NOT AS A VERDICT. A sleeve more volatile than its index is not '
                  + 'by itself worse — that difference is what the Active share and Tracking '
                  + 'error views are about.'} />} />} />
            <Tile label={`Worst ${period}`} value={signed2(data.worst_period_pct)}
              tone="text-neg-300"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={`The single worst ${period} in the window.`}
                where={`Best was ${signed2(data.best_period_pct)}; ${data.negative_periods_pct?.toFixed(0)}% of ${period}s were negative.`}
                how={'⚠ NOBODY HAS EVER EXPERIENCED "18% ANNUALISED VOLATILITY". They have '
                  + `experienced the worst ${period}. For a fat-tailed book the two are far apart, `
                  + 'which is exactly when σ on its own misleads.'} />} />} />
          </div>

          <div className="flex flex-wrap gap-2">
            <Tile label="Return (ann.)" value={signed2(data.return_ann_pct)}
              tone={(data.return_ann_pct ?? 0) >= 0 ? 'text-pos-300' : 'text-neg-300'}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="What the sleeve compounded at over the same window."
                where={`Geometric: ∏(1 + Rₜ)^(f ÷ T) − 1. ${data.benchmark} did ${signed2(data.benchmark_return_ann_pct)}.`}
                how={'Here so the two ratios beside it can be checked — a risk number without the '
                  + 'return it bought is half a sentence.'} />} />} />
            <Tile label="Sharpe" value={data.sharpe == null ? '—' : data.sharpe.toFixed(2)}
              tone="text-fg-muted"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="Return per unit of total volatility."
                where={`At a risk-free rate of ${data.risk_free_pct?.toFixed(1)}%.`}
                worked={data.sharpe == null ? '' : `${subNum(data.return_ann_pct ?? 0, 2)}%`
                  + ` ÷ ${subNum(data.volatility_pct ?? 0, 2)}% = ${data.sharpe.toFixed(2)}`}
                how={'⚠ THE RISK-FREE RATE IS STATED because a Sharpe quoted without it is not '
                  + 'comparable with anybody else\'s, and at current rates that is not cosmetic.'} />} />} />
            <Tile label="Sortino" value={data.sortino == null ? '—' : data.sortino.toFixed(2)}
              tone="text-fg-muted"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The same ratio, over downside deviation instead of total volatility."
                where="Return ÷ downside deviation, both annualised."
                worked={data.sortino == null ? '' : `${subNum(data.return_ann_pct ?? 0, 2)}%`
                  + ` ÷ ${subNum(data.downside_dev_pct ?? 0, 2)}% = ${data.sortino.toFixed(2)}`}
                how={'⚠ A DASH MEANS NOTHING EVER FELL BELOW THE TARGET — there is no downside to '
                  + 'divide by. That is a measurement, not a missing number.'} />} />} />
          </div>

          {data.cadence_note && <p className="text-[11px] text-fg-faint">{data.cadence_note}</p>}

          <p className="text-[11px] text-fg-faint leading-relaxed">
            {`Today's stock sleeve at today's weights over ${data.years} years `}
            {`(${data.priced_holdings} of ${data.total_holdings} priced). `}
            Deposits and withdrawals cannot distort this — it is a weighted basket of instrument
            returns, not an account value, so there are no flows in the series to strip out. The
            cost is that the weights are today&apos;s: this is the volatility of the portfolio as it
            stands, not the one the client lived through.
          </p>
        </>
      )}
    </div>
  );
}
