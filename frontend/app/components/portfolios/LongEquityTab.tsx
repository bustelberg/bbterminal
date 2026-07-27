'use client';

import { useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import MetricGrowthCard, { type MetricCfg } from './MetricGrowthCard';
import MarginCard from './MarginCard';
import CashReturnCard from './CashReturnCard';
import DebtRatioCard from './DebtRatioCard';
import InterestBurdenCard from './InterestBurdenCard';

/**
 * The "Long Equity" tab: a grid of growth cards (Revenue, FCF/share, …), each a
 * {@link MetricGrowthCard} — the metric per fiscal year on a log axis with an exponential-trend
 * R²/CAGR, the benchmark's same-metric growth, and a per-holding drill-down. The company's metrics
 * are fetched ONCE here (they carry every code) and handed to each card, which extracts its own.
 */

type MetricRow = { metric_code: string; target_date: string; numeric_value: number | null };
type MetricsResponse = { currency?: string | null; company_name?: string | null; metrics: MetricRow[] };

// Each card is one metric. `codes` carries BOTH GuruFocus section spellings (see the backend's
// `_METRIC_CODES`); `benchmarkMetric` is the `metric` param for the benchmark + holdings endpoints.
const CARDS: MetricCfg[] = [
  {
    title: 'Revenue', noun: 'revenue', unit: 'millions', kind: 'growth', benchmarkMetric: 'revenue',
    codes: ['annuals__Income Statement__Revenue', 'annuals__income_statement__Revenue'],
  },
  {
    title: 'FCF / share', noun: 'FCF/share', unit: 'per_share', kind: 'growth', benchmarkMetric: 'fcf_ps',
    codes: ['annuals__Per Share Data__Free Cash Flow per Share',
      'annuals__per_share_data__Free Cash Flow per Share'],
  },
];

export default function LongEquityTab({ isin, name, basket, portfolioId }: {
  isin?: string;
  name?: string | null;
  basket?: { holdings: { isin: string; weight: number; name?: string }[] };
  portfolioId?: number;
}) {
  const isAgg = !!basket || portfolioId != null;
  const [data, setData] = useState<MetricsResponse | null>(null);
  const [err, setErr] = useState<string | null>(null);

  // ⚠ Memoised — it's a card/modal effect dep, so a fresh object each render would refetch forever.
  const holdingsTarget = useMemo(() => (isAgg
    ? (basket
      ? { holdings: basket.holdings.map((h) => ({ isin: h.isin, name: h.name, weight: h.weight })) }
      : { portfolio_id: portfolioId })
    : { holdings: [{ isin: isin ?? '', weight: 1 }] }), [isAgg, basket, portfolioId, isin]);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setErr(null); setData(null);
      try {
        // A single company is keyed by ISIN; a portfolio is the blended pseudo-company (each metric
        // blends as a LEVEL → a growth index). Both return every metric code.
        const r = isAgg
          ? await apiFetch(`${API_URL}/api/earnings/fundamental-blend-metrics`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(basket
              ? { holdings: basket.holdings.map((h) => ({ isin: h.isin, name: h.name, weight: h.weight })) }
              : { portfolio_id: portfolioId }),
          })
          : await apiFetch(`${API_URL}/api/earnings/by-isin/${encodeURIComponent(isin ?? '')}/metrics`);
        if (r.status === 404) { if (alive) setData({ metrics: [] }); return; }
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as MetricsResponse);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [isin, isAgg, basket, portfolioId]);

  if (err) return <p className="text-xs text-neg-300 py-16 text-center">{err}</p>;

  // Fixed order across the grid: Revenue, FCF/share, FCF-SBC margin, Cash return on capital,
  // Debt / assets ex-GW, Interest / op. profit — four per row.
  const [revenue, fcfPs] = CARDS;
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
      <MetricGrowthCard key={revenue.title} cfg={revenue}
        metrics={data?.metrics ?? null} isAgg={isAgg} currency={data?.currency}
        holdingsTarget={holdingsTarget} holdingsName={name ?? data?.company_name} />
      <MetricGrowthCard key={fcfPs.title} cfg={fcfPs}
        metrics={data?.metrics ?? null} isAgg={isAgg} currency={data?.currency}
        holdingsTarget={holdingsTarget} holdingsName={name ?? data?.company_name} />
      <MarginCard holdingsTarget={holdingsTarget} holdingsName={name ?? data?.company_name} />
      <CashReturnCard holdingsTarget={holdingsTarget} holdingsName={name ?? data?.company_name} />
      <DebtRatioCard holdingsTarget={holdingsTarget} holdingsName={name ?? data?.company_name} />
      <InterestBurdenCard holdingsTarget={holdingsTarget} holdingsName={name ?? data?.company_name} />
    </div>
  );
}
