'use client';

import { useEffect, useMemo, useState, useDeferredValue } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { useFxToEur } from '../../../lib/hooks/useFxToEur';
import InfoTip from '../InfoTip';
import type { ChartCadence, MetricRow } from '../earnings/types';
import { SNAPSHOT_BAND_CHARTS } from '../earnings/snapshotBandCharts';
import BandScorecard from '../earnings/BandScorecard';
import ForwardPEChart from '../earnings/ForwardPEChart';
import RelativeGrowthChart from '../earnings/RelativeGrowthChart';
import FCFShareChart from '../earnings/FCFShareChart';
import MetricBandChart from '../earnings/MetricBandChart';
import SnapshotStats from '../earnings/SnapshotStats';

/** The full /earnings chart suite, reused inside the /portfolios Fundamental
 * modal for a SINGLE company. Data comes from the company-keyed earnings
 * metrics resolved BY ISIN (`GET /api/earnings/by-isin/{isin}/metrics`): only
 * instruments backed by a `company` row (~13%) have these metrics — the rest
 * 404, and this panel shows a friendly note while the modal's owner-earnings /
 * price tabs (which work for every ISIN) still render above it. Single-company
 * only, so every compare/portfolio prop the dashboard threads is omitted. */

const PE_INFO = "Forward P/E — the share price divided by next-fiscal-year consensus EPS estimate; how much you pay today per dollar of earnings the company is expected to make next year. Lower = cheaper relative to expected earnings; compare a stock to its own 'period avg' to judge whether it's trading rich or cheap versus its own history.";
const RG_INFO = "Share Price vs Owner Earnings — compares how fast the stock price has grown against the company's underlying earning power (Owner Earnings ≈ EPS excluding non-recurring items). Both indexed to 100 and drawn on a log scale, so slope = growth rate: price outpacing earnings = multiple expansion, earnings outpacing price = de-rating.";
const FCF_INFO = "Free Cash Flow per share — the cash a business throws off after funding its capital spending, divided by shares outstanding; the fuel for dividends, buybacks and debt paydown. Converted to EUR at the historical FX rate so companies in different currencies compare directly.";

// Only metrics from this year onward feed the charts — matches the /earnings
// dashboard's default 2015 start year so axes stay sane.
const START_DATE = '2015-01-01';

type MetricsResponse = {
  company_id: number;
  company_name: string | null;
  currency: string | null;
  metrics: MetricRow[];
};

type State =
  | { kind: 'loading' }
  | { kind: 'none' }          // 404 — no company row for this ISIN
  | { kind: 'error'; message: string }
  | { kind: 'ready'; data: MetricsResponse };

function ChartCard({ title, info, children }: { title: string; info?: string; children: React.ReactNode }) {
  return (
    <div className="bg-page rounded-lg border border-accent-500/20 p-4 space-y-2 overflow-hidden min-w-0">
      <h3 className="text-fg-strong text-sm font-medium flex items-center gap-1.5">
        <span className="truncate">{title}</span>
        {info && <InfoTip text={info} />}
      </h3>
      {children}
    </div>
  );
}

export default function FundamentalCharts({ isin, name }: { isin: string; name?: string | null }) {
  const [state, setState] = useState<State>({ kind: 'loading' });
  const [cadence, setCadence] = useState<ChartCadence>('annual');
  const [hideOutliers, setHideOutliers] = useState(false);
  const [snapshotOpen, setSnapshotOpen] = useState(false);
  const deferredCadence = useDeferredValue(cadence);

  useEffect(() => {
    let cancelled = false;
    const ctrl = new AbortController();
    (async () => {
      setState({ kind: 'loading' });
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/by-isin/${encodeURIComponent(isin)}/metrics`, { signal: ctrl.signal });
        if (cancelled) return;
        if (r.status === 404) { setState({ kind: 'none' }); return; }
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setState({ kind: 'error', message: b?.detail ?? `HTTP ${r.status}` }); return; }
        setState({ kind: 'ready', data: b as MetricsResponse });
      } catch (e) {
        if (!cancelled) setState({ kind: 'error', message: e instanceof Error ? e.message : String(e) });
      }
    })();
    return () => { cancelled = true; ctrl.abort(); };
  }, [isin]);

  const currency = state.kind === 'ready' ? state.data.currency : null;
  const fx = useFxToEur(currency);

  const metrics = useMemo(
    () => (state.kind === 'ready' ? state.data.metrics.filter((m) => m.target_date >= START_DATE) : []),
    [state],
  );

  if (state.kind === 'loading') {
    return <p className="text-[11px] text-fg-subtle py-8 text-center">Loading fundamentals…</p>;
  }
  if (state.kind === 'none') {
    return (
      <p className="text-[11px] text-fg-faint py-8 text-center max-w-xl mx-auto">
        Full fundamental charts are available for companies in our dataset — this instrument isn’t one of them.
      </p>
    );
  }
  if (state.kind === 'error') {
    return (
      <p className="text-[11px] text-neg-400 py-8 text-center">Couldn’t load fundamentals — {state.message}</p>
    );
  }

  const nameA = name ?? undefined;

  return (
    <div className="space-y-4">
      {/* Cadence + outlier toggles — mirror the /earnings chart controls. */}
      <div className="flex items-center gap-2 flex-wrap">
        <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5" title="Quarterly matches the Snapshot Stats values; Annual uses fiscal-year figures.">
          {(['quarterly', 'annual'] as ChartCadence[]).map((c) => (
            <button
              key={c}
              type="button"
              onClick={() => setCadence(c)}
              className={`px-2.5 py-1 rounded-md text-xs font-medium transition-colors ${cadence === c ? 'bg-accent-600 text-fg-strong' : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/5'}`}
            >
              {c === 'quarterly' ? 'Quarterly' : 'Annual'}
            </button>
          ))}
        </div>
        <button
          type="button"
          onClick={() => setHideOutliers((v) => !v)}
          aria-pressed={hideOutliers}
          title="Drop impossible extreme outliers (e.g. a -10000% margin glitch) so the axis stays sane. Off by default."
          className={`px-2.5 py-1 rounded-lg text-xs font-medium border transition-colors ${hideOutliers ? 'bg-accent-600 text-fg-strong border-transparent' : 'text-fg-muted border-neutral-700 hover:text-fg-strong hover:bg-overlay/5'}`}
        >
          Hide outliers
        </button>
      </div>

      {/* At-a-glance scorecard — one green/amber/red circle per banded chart. */}
      <BandScorecard
        charts={SNAPSHOT_BAND_CHARTS}
        metrics={metrics}
        cadence={deferredCadence}
        hideOutliers={hideOutliers}
      />

      <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
        <ChartCard title="Forward P/E" info={PE_INFO}>
          <ForwardPEChart metrics={metrics} nameA={nameA} hideOutliers={hideOutliers} />
        </ChartCard>

        <ChartCard title="Share Price vs. Owners Earnings" info={RG_INFO}>
          <RelativeGrowthChart metrics={metrics} nameA={nameA} />
        </ChartCard>

        <ChartCard title="FCF / share" info={FCF_INFO}>
          <FCFShareChart metrics={metrics} nameA={nameA} toEurA={fx.toEur} />
        </ChartCard>

        {SNAPSHOT_BAND_CHARTS.map((cfg) => (
          <ChartCard key={cfg.key} title={cfg.title} info={cfg.headerInfo}>
            <MetricBandChart
              metrics={metrics}
              nameA={nameA}
              cadence={deferredCadence}
              hideOutliers={hideOutliers}
              buildSeries={cfg.buildSeries}
              band={cfg.band}
              format={cfg.format}
              axisFormat={cfg.axisFormat}
              subtitle={cfg.subtitle}
              cadenceLabel={cfg.cadenceLabel}
              infoText={cfg.chartInfo}
              emptyText={cfg.emptyText}
            />
          </ChartCard>
        ))}
      </div>

      {/* Snapshot Stats — collapsed by default, like the dashboard. */}
      <div className="rounded-lg border border-neutral-800/40">
        <button
          type="button"
          onClick={() => setSnapshotOpen((v) => !v)}
          className="flex items-center gap-2 text-left w-full px-4 py-3"
          aria-expanded={snapshotOpen}
        >
          <span className="text-fg-faint text-sm w-3">{snapshotOpen ? '▾' : '▸'}</span>
          <h3 className="text-fg-strong text-sm font-medium">Snapshot Stats</h3>
        </button>
        {snapshotOpen && (
          <div className="px-4 pb-4">
            <SnapshotStats metrics={metrics} labelA={nameA} />
          </div>
        )}
      </div>
    </div>
  );
}
