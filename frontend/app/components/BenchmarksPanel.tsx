'use client';

import { Fragment, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import type { ReconstructedIndex } from '../../lib/types/api';

/** The indices we rebuild from our own constituents.
 *
 * ACWI is deliberately included even though it is NOT fully priced: GuruFocus does not cover
 * the UK, India, Ireland or AU/NZ, so a slice of the published index is absent and a
 * cap-weighted rebuild renormalises that weight across the rest. The panel surfaces the
 * coverage ratio and calls it indicative rather than exact. */
const INDICES = [
  { label: 'SP500', name: 'S&P 500' },
  { label: 'ACWI', name: 'ACWI' },
];

const pct = (v: number | null | undefined, dp = 2) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(dp)}%`;

const tone = (v: number | null | undefined) =>
  v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400';

/** Benchmarks — a cap-weighted index rebuilt from OUR membership, prices and FX.
 *
 * Why rebuild an index we could just read off SPY: because this one is computed the same way
 * a portfolio is, so a benchmark number and a portfolio number are comparable line for line.
 * It is checked against the real thing — 2026 YTD comes out +9.10% USD against SPY's +9.02%.
 *
 * The two things that make that agreement possible are both counter-intuitive, so they are
 * spelled out in the footnote rather than buried: the weights are as of the START of the
 * year (weighting by today's market cap is look-ahead bias, and would print +21.70%), and
 * three constituents' price series had to be un-split (our stored closes are not
 * split-adjusted and cannot self-heal).
 */
export default function BenchmarksPanel() {
  const [data, setData] = useState<Record<string, ReconstructedIndex>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      // Independently, in parallel: ACWI is ~8s and one index failing must not blank the
      // other. Only surface an error if EVERY index failed.
      const results = await Promise.allSettled(
        INDICES.map(async (ix) => {
          const r = await apiFetch(`${API_URL}/api/benchmarks/index/${ix.label}`);
          if (!r.ok) throw new Error(`${ix.label}: HTTP ${r.status}`);
          return [ix.label, (await r.json()) as ReconstructedIndex] as const;
        }),
      );
      if (cancelled) return;
      const out: Record<string, ReconstructedIndex> = {};
      const errs: string[] = [];
      for (const res of results) {
        if (res.status === 'fulfilled') out[res.value[0]] = res.value[1];
        else errs.push(res.reason instanceof Error ? res.reason.message : String(res.reason));
      }
      setData(out);
      if (Object.keys(out).length === 0) setError(errs.join('; ') || 'Failed to load');
      setLoading(false);
    })();
    return () => { cancelled = true; };
  }, []);

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div>
        <h3 className="text-sm font-semibold text-fg-strong">Benchmarks</h3>
        <p className="text-[11px] text-fg-faint mt-0.5">
          Cap-weighted, rebuilt from our own constituents — same basis as a portfolio, so the
          numbers are comparable.
        </p>
      </div>

      {loading && <p className="text-xs text-fg-subtle">Computing…</p>}
      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>
      )}

      {!loading && !error && (
        <div className="overflow-auto rounded-lg border border-neutral-800/40">
          <table className="w-full text-xs">
            <thead className="bg-card">
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                <th className="px-3 py-1.5 font-medium text-left">Benchmark</th>
                <th className="px-3 py-1.5 font-medium text-right">Members</th>
                <th className="px-3 py-1.5 font-medium text-right">YTD (€)</th>
                <th className="px-3 py-1.5 font-medium text-right">YTD (local)</th>
                <th className="px-3 py-1.5 font-medium text-left">As of</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {INDICES.map((ix) => {
                const d = data[ix.label];
                if (!d) return null;
                const isOpen = open === ix.label;
                return (
                  <Fragment key={ix.label}>
                    <tr onClick={() => setOpen(isOpen ? null : ix.label)}
                      className="hover:bg-accent-500/10 transition-colors cursor-pointer">
                      <td className="px-3 py-1.5 text-fg whitespace-nowrap">
                        <span className="text-fg-faint mr-1.5">{isOpen ? '▾' : '▸'}</span>
                        {ix.name}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-soft">{d.member_count}</td>
                      <td className={`px-3 py-1.5 text-right font-mono font-semibold ${tone(d.ytd_eur_pct)}`}>
                        {pct(d.ytd_eur_pct)}
                      </td>
                      <td className={`px-3 py-1.5 text-right font-mono ${tone(d.ytd_local_pct)}`}>
                        {pct(d.ytd_local_pct)}
                      </td>
                      <td className="px-3 py-1.5 font-mono text-fg-subtle whitespace-nowrap">{d.as_of ?? '—'}</td>
                    </tr>
                    {isOpen && (
                      <tr>
                        <td colSpan={5} className="px-3 py-3 bg-inset">
                          <IndexDetail d={d} />
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function IndexDetail({ d }: { d: ReconstructedIndex }) {
  const [q, setQ] = useState('');
  const needle = q.trim().toLowerCase();
  const members = (d.members ?? []).filter((m) =>
    !needle || `${m.company_name ?? ''} ${m.ticker ?? ''}`.toLowerCase().includes(needle));

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-3 flex-wrap text-[11px]">
        <span className="text-fg-soft">
          <span className="font-mono text-fg">{d.priced_of_universe}</span> priced ·{' '}
          weights as of <span className="font-mono">{d.start_date}</span>
        </span>
        <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search constituent…"
          className="bg-page border border-neutral-700 rounded-lg px-3 py-1 text-[11px] text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 w-52 ml-auto" />
      </div>

      {/* A corrected price is a CLAIM. Show it — never adjust silently. */}
      {(d.split_adjusted?.length ?? 0) > 0 && (
        <div className="bg-warn-500/10 border border-warn-500/20 rounded-lg px-3 py-2 text-[11px] text-warn-300">
          <span className="font-semibold">Split-adjusted on the fly:</span>{' '}
          {d.split_adjusted!.map((s) => `${s.ticker} ×${s.factor.toFixed(3)}`).join(' · ')}.
          Our stored closes are not split-adjusted and cannot self-heal (the ingest only
          fetches dates newer than what we hold), so these series were rescaled here. Left
          raw, KLA alone would read −80% and take a weight it never had.
        </div>
      )}

      <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[50vh]">
        <table className="w-full text-xs">
          <thead className="bg-card sticky top-0">
            <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
              <th className="px-3 py-1.5 font-medium text-left">Company</th>
              <th className="px-3 py-1.5 font-medium text-left">Ticker</th>
              <th className="px-3 py-1.5 font-medium text-right">Weight</th>
              <th className="px-3 py-1.5 font-medium text-right">YTD (local)</th>
              <th className="px-3 py-1.5 font-medium text-right">YTD (€)</th>
              <th className="px-3 py-1.5 font-medium text-right">Mkt cap (€bn)</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {members.map((m) => (
              <tr key={m.company_id} className="hover:bg-overlay/[0.02]">
                <td className="px-3 py-1.5 text-fg-soft">{m.company_name ?? '—'}</td>
                <td className="px-3 py-1.5 font-mono text-fg-muted">{m.ticker ?? '—'}</td>
                <td className="px-3 py-1.5 text-right font-mono text-fg">{m.weight_pct.toFixed(2)}%</td>
                <td className={`px-3 py-1.5 text-right font-mono ${tone(m.return_local_pct)}`}>
                  {pct(m.return_local_pct)}
                </td>
                <td className={`px-3 py-1.5 text-right font-mono ${tone(m.return_eur_pct)}`}>
                  {pct(m.return_eur_pct)}
                </td>
                <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                  {(m.market_cap_eur / 1e9).toFixed(1)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <p className="text-[10px] text-fg-faint leading-relaxed">
        {d.note} Weights are as of the <strong>start of the year</strong>: weighting by
        today&apos;s market cap is look-ahead bias — it retroactively overweights whatever
        went up.{' '}
        {d.label === 'SP500' ? (
          <>
            It would print <span className="font-mono">+21.70%</span> instead of{' '}
            <span className="font-mono">{pct(d.ytd_local_pct)}</span>. Checked against SPY (the
            real index), which is <span className="font-mono">+9.02%</span> USD for the same
            window.
          </>
        ) : (
          <>
            Coverage is <strong>partial</strong> (<span className="font-mono">{d.priced_of_universe}</span>{' '}
            priced): GuruFocus does not cover the UK, India, Ireland or AU/NZ, so those
            constituents are absent and their weight is renormalised across the rest. Treat this
            as indicative, not exact.
          </>
        )}
      </p>
    </div>
  );
}
