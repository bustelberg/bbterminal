'use client';

import { useEffect, useMemo, useState } from 'react';

import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import InfoTip from '../InfoTip';
import Spinner from '../Spinner';
import { chartTheme } from '../../../lib/chartTheme';
import type { Portfolio } from './usePortfolios';

type Side = {
  name: string | null;
  sector_weights: Record<string, number>;
  sector_returns: Record<string, number | null>;
};
type AttributionResult = {
  year: number;
  universe: string;
  sectors: string[];
  a: Side;
  b: Side;
  matrix: number[][]; // [row = whose weights (a,b)][col = whose returns (a,b)]
};

const C_A = chartTheme.accent;
const C_B = chartTheme.compare;

const pct = (v: number | null | undefined) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`);
const retCls = (v: number | null | undefined) => (v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400');

const INFO =
  'Cross-portfolio sector attribution. Each cell = Σ over sectors of (row portfolio’s weight in the sector) × ' +
  '(column portfolio’s return in the sector). The column portfolio’s sector return is the within-sector, ' +
  'weighted return of the stocks IT holds there. The diagonal is each portfolio’s own one-year return; the ' +
  'off-diagonal mixes one portfolio’s allocation with the other’s stock-picking. Compare DOWN a column ' +
  '(same returns, different weights) to isolate allocation skill; ACROSS a row (same weights, different returns) ' +
  'for stock-selection skill. A sector a portfolio doesn’t hold contributes 0%. Sectors come from the chosen ' +
  'universe’s membership; returns are EUR price returns over the chosen calendar year.';

/** Brinson-style allocation×selection matrix for two portfolios. Shown on
 * /earnings when both comparison sides are portfolios. */
export default function AttributionMatrix({ portfolioA, portfolioB }: { portfolioA: Portfolio; portfolioB: Portfolio }) {
  const currentYear = new Date().getFullYear();
  const [universe, setUniverse] = useState('Leonteq');
  const [universes, setUniverses] = useState<string[]>(['Leonteq']);
  const [year, setYear] = useState(currentYear);
  const [data, setData] = useState<AttributionResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [showDetail, setShowDetail] = useState(false);

  const years = useMemo(() => Array.from({ length: currentYear - 2014 }, (_, i) => currentYear - i), [currentYear]);

  useEffect(() => {
    apiFetch(`${API_URL}/api/earnings/sector-universes`)
      .then((r) => r.json())
      .then((d) => { if (Array.isArray(d?.universes) && d.universes.length) setUniverses(d.universes); })
      .catch(() => {});
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/portfolios/attribution?a=${portfolioA.id}&b=${portfolioB.id}&universe=${encodeURIComponent(universe)}&year=${year}`);
        if (!r.ok) throw new Error((await r.json().catch(() => ({}))).detail ?? `HTTP ${r.status}`);
        const d = (await r.json()) as AttributionResult;
        if (!cancelled) setData(d);
      } catch (e) {
        if (!cancelled) { setErr(e instanceof Error ? e.message : String(e)); setData(null); }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [portfolioA.id, portfolioB.id, universe, year]);

  const aName = portfolioA.name;
  const bName = portfolioB.name;
  const m = data?.matrix;
  const headCls = 'px-3 py-2 text-xs font-medium text-center';
  const selCls = 'bg-page border border-neutral-700 rounded-lg px-2 py-1 text-xs text-fg-strong outline-none focus:border-accent-500';

  return (
    <section className="bg-card rounded-xl border border-accent-500/20 p-5 space-y-4">
      <div className="flex items-center gap-2 flex-wrap">
        <h2 className="text-fg-strong font-medium">Allocation × Selection</h2>
        <InfoTip text={INFO} />
        {loading && <Spinner size={12} />}
        <div className="ml-auto flex items-center gap-2">
          <span className="text-xs text-fg-subtle">Sectors</span>
          <select value={universe} onChange={(e) => setUniverse(e.target.value)} className={selCls}>
            {universes.map((u) => <option key={u} value={u}>{u}</option>)}
          </select>
          <span className="text-xs text-fg-subtle">Year</span>
          <select value={year} onChange={(e) => setYear(Number(e.target.value))} className={selCls}>
            {years.map((y) => <option key={y} value={y}>{y}{y === currentYear ? ' (YTD)' : ''}</option>)}
          </select>
        </div>
      </div>

      {err && <div className="text-sm text-neg-300 bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2">{err}</div>}

      {m && (
        <>
          <div className="overflow-x-auto">
            <table className="text-sm border-collapse">
              <thead>
                <tr>
                  <th className="px-3 py-2" />
                  <th className={headCls} colSpan={2}><span className="text-fg-subtle font-normal">Returns from →</span></th>
                </tr>
                <tr>
                  <th className="px-3 py-2 text-xs text-fg-subtle font-normal text-left">Weights from ↓</th>
                  <th className={headCls} style={{ color: C_A }}>{aName}</th>
                  <th className={headCls} style={{ color: C_B }}>{bName}</th>
                </tr>
              </thead>
              <tbody>
                {([0, 1] as const).map((row) => (
                  <tr key={row}>
                    <td className="px-3 py-2.5 text-xs font-medium" style={{ color: row === 0 ? C_A : C_B }}>{row === 0 ? aName : bName}</td>
                    {([0, 1] as const).map((col) => {
                      const v = m[row][col];
                      const diag = row === col;
                      return (
                        <td
                          key={col}
                          className={`px-5 py-2.5 text-center font-mono ${retCls(v)} ${diag ? 'bg-accent-500/5 font-semibold' : ''}`}
                          title={diag ? `${row === 0 ? aName : bName}'s actual ${data!.year} return` : `${row === 0 ? aName : bName}'s sector weights × ${col === 0 ? aName : bName}'s sector returns`}
                        >
                          {pct(v)}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <p className="text-xs text-fg-subtle leading-relaxed">
            Diagonal (shaded) = each portfolio&apos;s actual {data.year} return.
            Down a column → same stock returns, different sector weights = <span className="text-fg-soft">allocation</span> effect.
            Across a row → same weights, different stocks = <span className="text-fg-soft">selection</span> effect.
            Sectors: {data.universe}.
          </p>

          <button
            type="button"
            onClick={() => setShowDetail((v) => !v)}
            className="text-xs text-accent-400 hover:text-accent-300 transition-colors"
          >
            {showDetail ? 'Hide' : 'Show'} per-sector weights & returns
          </button>
          {showDetail && (
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead className="text-fg-subtle">
                  <tr className="border-b border-neutral-800/60">
                    <th className="px-3 py-2 text-left">Sector</th>
                    <th className="px-3 py-2 text-right" style={{ color: C_A }}>{aName} wt</th>
                    <th className="px-3 py-2 text-right" style={{ color: C_A }}>{aName} ret</th>
                    <th className="px-3 py-2 text-right" style={{ color: C_B }}>{bName} wt</th>
                    <th className="px-3 py-2 text-right" style={{ color: C_B }}>{bName} ret</th>
                  </tr>
                </thead>
                <tbody>
                  {data.sectors.map((s) => (
                    <tr key={s} className="border-b border-neutral-800/30">
                      <td className="px-3 py-1.5 text-fg-soft">{s}</td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-muted">{((data.a.sector_weights[s] ?? 0) * 100).toFixed(0)}%</td>
                      <td className={`px-3 py-1.5 text-right font-mono ${retCls(data.a.sector_returns[s])}`}>{pct(data.a.sector_returns[s])}</td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-muted">{((data.b.sector_weights[s] ?? 0) * 100).toFixed(0)}%</td>
                      <td className={`px-3 py-1.5 text-right font-mono ${retCls(data.b.sector_returns[s])}`}>{pct(data.b.sector_returns[s])}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </section>
  );
}
