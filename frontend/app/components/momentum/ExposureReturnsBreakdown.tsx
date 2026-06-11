'use client';

import { useMemo } from 'react';

import CollapsibleCard from './CollapsibleCard';
import type { BacktestResult } from '../../../lib/stores/momentum';

/**
 * "Did the regime filter skip more downside than upside?" overview.
 *
 * Splits every month into two buckets by the filter's exposure that period —
 * full (100%) vs de-risked (<100%) — and compares the broad MARKET return
 * (equal-weight universe, which is unscaled by exposure, so it's the return
 * we'd have captured or skipped). If the de-risked bucket is more often down
 * and more negative on average than the full bucket, the signal is ducking
 * the right months. It also tallies the actual return skipped by de-risking
 * (the `1 − exposure` slice of each month), split into avoided-downside vs
 * missed-upside.
 *
 * Renders only when a run carries varied exposure (regime filter active with
 * at least one de-risked month). Reads `universe_return_pct` + `exposure_scale`
 * off the period records (exposure_scale is omitted from the payload when 1.0,
 * so a missing value means full exposure).
 */

const EPS = 0.9999; // exposure ≥ this counts as "full"

type Row = { date: string; ret: number; exposure: number };

type Stats = {
  n: number;
  mean: number;
  compounded: number;
  pctUp: number;
  avgUp: number;
  avgDown: number;
  worst: number;
};

function statsOf(rets: number[]): Stats | null {
  const n = rets.length;
  if (!n) return null;
  const sum = rets.reduce((a, b) => a + b, 0);
  const ups = rets.filter((r) => r > 0);
  const downs = rets.filter((r) => r < 0);
  const compounded = (rets.reduce((f, r) => f * (1 + r / 100), 1) - 1) * 100;
  return {
    n,
    mean: sum / n,
    compounded,
    pctUp: (ups.length / n) * 100,
    avgUp: ups.length ? ups.reduce((a, b) => a + b, 0) / ups.length : 0,
    avgDown: downs.length ? downs.reduce((a, b) => a + b, 0) / downs.length : 0,
    worst: Math.min(...rets),
  };
}

const fmtPct = (v: number, sign = false) => `${sign && v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
const cls = (v: number) => (v > 0 ? 'text-pos-400' : v < 0 ? 'text-neg-400' : 'text-fg-soft');

export default function ExposureReturnsBreakdown({
  result,
  defaultCollapsed = false,
}: {
  result: BacktestResult;
  defaultCollapsed?: boolean;
}) {
  const data = useMemo(() => {
    const rows: Row[] = [];
    for (const r of result.monthly_records ?? []) {
      if (r.universe_return_pct == null) continue;
      if (r.market_health == null) continue; // only regime-active runs
      rows.push({ date: r.date.slice(0, 10), ret: r.universe_return_pct, exposure: r.exposure_scale ?? 1.0 });
    }
    const full = rows.filter((p) => p.exposure >= EPS);
    const derisk = rows.filter((p) => p.exposure < EPS);

    // Return actually skipped by de-risking: the (1 − exposure) slice of each
    // de-risked month's market move, split into avoided downside / missed upside.
    let skippedDown = 0, skippedUp = 0, nDown = 0, nUp = 0;
    for (const p of derisk) {
      const skipped = (1 - p.exposure) * p.ret;
      if (p.ret < 0) { skippedDown += skipped; nDown += 1; }
      else if (p.ret > 0) { skippedUp += skipped; nUp += 1; }
    }
    return {
      rows,
      full: statsOf(full.map((p) => p.ret)),
      derisk: statsOf(derisk.map((p) => p.ret)),
      skippedDown, skippedUp, net: skippedDown + skippedUp, nDown, nUp,
    };
  }, [result]);

  if (!data.derisk || data.derisk.n === 0 || !data.full) return null;

  const { full, derisk } = data;
  // The filter "works" if de-risked months are, on average, weaker and more
  // often down than full-exposure months.
  const ducksDownside = derisk.mean < full.mean && derisk.pctUp < full.pctUp;

  return (
    <CollapsibleCard
      title="Monthly returns by exposure"
      defaultCollapsed={defaultCollapsed}
      rightSlot={<span className="text-[11px] font-mono text-fg-faint">{full.n} full · {derisk.n} de-risked</span>}
      bodyClassName="px-5 py-4 space-y-4"
    >
      <p className="text-[11px] text-fg-faint">
        Each month&apos;s broad-market return (equal-weight universe), split by the filter&apos;s exposure that period. The de-risked column is the months you&apos;d have trimmed — ideally weaker and more often down than the full-exposure months.
      </p>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <Bucket label="Full exposure (100%)" color="text-fg-soft" s={full} />
        <Bucket label="De-risked (<100%)" color="text-warn-300" s={derisk} />
      </div>

      {/* What de-risking actually skipped, in return terms. */}
      <div className="rounded-lg border border-neutral-800/40 bg-page/40 px-4 py-3 space-y-1.5">
        <div className="text-[11px] uppercase tracking-wide text-fg-faint">Return skipped by de-risking</div>
        <div className="grid grid-cols-3 gap-3 text-sm">
          <Stat label={`Avoided downside (${data.nDown} mo)`} value={fmtPct(data.skippedDown, true)} cls={cls(-1)} />
          <Stat label={`Missed upside (${data.nUp} mo)`} value={fmtPct(data.skippedUp, true)} cls={cls(1)} />
          <Stat label="Net effect on captured return" value={fmtPct(data.net, true)} cls={cls(data.net)} />
        </div>
        <p className="text-[10px] text-fg-faint">
          Counts the <span className="font-mono">1 − exposure</span> slice of each de-risked month&apos;s market move. A negative &quot;avoided downside&quot; that outweighs the missed upside (positive net) means trimming those months helped.
        </p>
      </div>

      <div className={`text-xs rounded-lg px-3 py-2 border ${ducksDownside ? 'bg-pos-500/10 border-pos-500/20 text-pos-300' : 'bg-warn-500/5 border-warn-500/20 text-warn-300'}`}>
        {ducksDownside
          ? `✓ De-risked months averaged ${fmtPct(derisk.mean, true)} vs ${fmtPct(full.mean, true)} at full exposure, and were down ${(100 - derisk.pctUp).toFixed(0)}% of the time vs ${(100 - full.pctUp).toFixed(0)}% — the filter is trimming the weaker months.`
          : `⚠ De-risked months averaged ${fmtPct(derisk.mean, true)} vs ${fmtPct(full.mean, true)} at full exposure — the signal isn't clearly ducking more downside than upside on this window. Inspect the health/RSI alignment above.`}
      </div>
    </CollapsibleCard>
  );
}

function Bucket({ label, color, s }: { label: string; color: string; s: Stats }) {
  return (
    <div className="rounded-lg border border-neutral-800/40 bg-card-alt/40 px-4 py-3 space-y-2">
      <div className={`text-xs font-medium ${color}`}>{label}</div>
      <div className="grid grid-cols-2 gap-x-4 gap-y-1.5 text-[12px]">
        <Stat label="Months" value={String(s.n)} cls="text-fg-soft" />
        <Stat label="% down" value={`${(100 - s.pctUp).toFixed(0)}%`} cls="text-fg-soft" />
        <Stat label="Avg return" value={fmtPct(s.mean, true)} cls={cls(s.mean)} />
        <Stat label="Compounded" value={fmtPct(s.compounded, true)} cls={cls(s.compounded)} />
        <Stat label="Avg up-month" value={fmtPct(s.avgUp, true)} cls={cls(s.avgUp)} />
        <Stat label="Avg down-month" value={fmtPct(s.avgDown, true)} cls={cls(s.avgDown)} />
        <Stat label="Worst month" value={fmtPct(s.worst, true)} cls={cls(s.worst)} />
      </div>
    </div>
  );
}

function Stat({ label, value, cls: c }: { label: string; value: string; cls: string }) {
  return (
    <div>
      <div className="text-[10px] text-fg-faint">{label}</div>
      <div className={`font-mono ${c}`}>{value}</div>
    </div>
  );
}
