'use client';

/** Rolling 12-month annualised volatility of the current stock basket and benchmark. */
import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';
import { chartTheme } from '../../../lib/chartTheme';
import { API_URL } from '../../../lib/apiUrl';
import { useRiskCopy } from './riskCopy';
import LoadingDots from './LoadingDots';
import type { SourceKey } from '../../../lib/provenance';
import type { PortfolioVolatility } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';
import { riskRequestKey, useRiskResult } from './useRiskResult';
import { riskHistoryQuery, riskHistoryYears } from './riskHistoryWindow';
import { invalidateReadCache } from '../../../lib/readCache';
import { startJob } from '../../../lib/stores/jobs';

// One automatic repair per exact sleeve is enough: a genuinely new instrument cannot acquire
// pre-launch history, and reopening the preliminary view must not repeat vendor work.
const startedPreliminaryRepairs = new Set<string>();

const pct2 = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);
export default function VolatilityView({
  holdings, benchmark, benchmarkWeights, portfolioName,
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
  const freq = 'monthly';

  const requestUrl = `${API_URL}/api/airs/portfolio/volatility`
    + `?benchmark=${encodeURIComponent(benchmark)}&frequency=${freq}&${riskHistoryQuery()}`;
  const requestBody = JSON.stringify({ holdings, benchmark_weights: benchmarkWeights });
  const repairIdentity = riskRequestKey(requestUrl, requestBody);
  const [repairVersion, setRepairVersion] = useState(0);
  const key = `${repairIdentity}\nrepair:${repairVersion}`;
  const { data, error } = useRiskResult<PortfolioVolatility>(key, async () => {
    const r = await apiFetch(requestUrl, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: requestBody, noReadCache: true,
    });
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    return b as PortfolioVolatility;
  }, 'volatility', 'the volatility could not be computed');

  useEffect(() => {
    if (!data?.available || (!data.preliminary && !data.excluded_holdings?.length)
      || startedPreliminaryRepairs.has(repairIdentity)) return;
    startedPreliminaryRepairs.add(repairIdentity);
    void startJob(
      `${API_URL}/api/airs/portfolio/risk-correlation/repair/job?benchmark=${encodeURIComponent(benchmark)}&years=${riskHistoryYears()}&full_history=true&yahoo_only=true`,
      'Repairing risk price history',
      { headers: { 'Content-Type': 'application/json' }, body: requestBody },
    ).then(async ({ done }) => {
      const job = await done;
      if (job.status === 'done') {
        invalidateReadCache('volatility history repaired');
        setRepairVersion((version) => version + 1);
      }
    }).catch(() => {
      // The standard job popup reports failures; retain the real preliminary sample either way.
    });
  }, [benchmark, data?.available, data?.preliminary, data?.excluded_holdings?.length, repairIdentity, requestBody]);

  const latest = data?.rolling_volatility?.at(-1);

  return (
    <div className="space-y-3">

      {error && <p className="text-xs text-neg-300">{error}</p>}
      {!data && !error && (
        <div className="absolute inset-0 grid place-items-center text-center px-6" role="status">
          <p className="text-2xl font-medium text-fg-strong">{t.common.computing} <LoadingDots /></p>
        </div>
      )}
      {data && !data.available && <p className="text-xs text-fg-muted" title={data.reason ?? undefined}>{/benchmark.*history/i.test(data.reason ?? '')
        ? <>Waiting for Yahoo benchmark history; retrying automatically… <LoadingDots /></> : data.reason}</p>}

      {data?.available && (
        <>
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            {[{ name: portfolioName, value: latest?.portfolio_pct }, { name: data.benchmark, value: latest?.benchmark_pct }].map(item => (
              <div key={item.name} className="rounded-lg border border-neutral-800/40 bg-elevated p-4">
                <p className="text-sm text-fg-muted">{item.name} · rolling 12-month volatility</p>
                <p className="mt-1 font-mono text-2xl tabular-nums text-fg-strong">{pct2(typeof item.value === 'number' ? item.value : null)}</p>
              </div>
            ))}
          </div>
          <section className="rounded-xl border border-neutral-800/40 bg-elevated p-4">
            <h5 className="mb-4 text-center text-base font-medium text-fg-strong">Rolling 12-month volatility</h5>
            {(data.rolling_volatility?.length ?? 0) > 0 ? (
              <ResponsiveContainer width="100%" height={280}>
                <LineChart data={data.rolling_volatility} margin={{ top: 8, right: 16, bottom: 4, left: 8 }}>
                  <CartesianGrid stroke={chartTheme.gridEarnings} strokeDasharray="3 3" />
                  <XAxis dataKey="date" minTickGap={50} tick={{ fontSize: 12, fill: chartTheme.axisTick }} tickFormatter={(date: string) => new Date(`${date}T00:00:00Z`).toLocaleDateString('en-GB', { month: 'short', year: '2-digit', timeZone: 'UTC' })} />
                  <YAxis domain={[0, 'auto']} tick={{ fontSize: 12, fill: chartTheme.axisTick }} tickFormatter={(value: number) => `${value.toFixed(0)}%`} />
                  <Tooltip formatter={(value) => `${Number(value).toFixed(2)}%`} />
                  <Legend />
                  <Line type="monotone" dataKey="portfolio_pct" name={portfolioName} stroke={chartTheme.pos} strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="benchmark_pct" name={data.benchmark} stroke={chartTheme.compare} strokeWidth={2} strokeDasharray="5 3" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            ) : <p className="py-10 text-center text-sm text-fg-muted">At least 12 months of overlapping returns are needed.</p>}
            <p className="mt-3 text-center text-xs text-fg-faint">{latest?.date ? `Latest rolling window ends ${latest.date}. ` : ''}Annualised volatility over the preceding 12 months, using historical EUR returns.</p>
          </section>

        </>
      )}
    </div>
  );
}
