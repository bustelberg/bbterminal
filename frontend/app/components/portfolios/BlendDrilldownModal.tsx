'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';

/** The holdings behind ONE point of a blended portfolio chart — and the ones missing from it.
 *
 * ⚠ IT DOES NOT COMPUTE ANYTHING. Every figure here is read from
 * `POST /api/earnings/fundamental-blend-breakdown`, which decomposes the point through the SAME
 * `_prepare` the chart's line is built from. Re-deriving the members here "the same way" would be
 * a second copy of the harmonic/ratio/level rules, and a panel that quietly disagrees with the
 * chart above it is worse than no panel — it gets checked once and trusted from then on.
 */

type Member = {
  isin: string | null;
  name: string | null;
  weight_pct: number;
  value: number;
  raw_value: number | null;
  share_pct: number | null;
  swing: number | null;
};

type Excluded = { isin: string | null; name: string | null; weight_pct: number; reason: string };

type Breakdown = {
  kind: 'multiple' | 'ratio' | 'level';
  metric_code: string;
  period: string;
  value: number | null;
  covered_pct: number;
  excluded_pct: number;
  members: Member[];
  excluded: Excluded[];
};

/** ⚠ Each reason is a DIFFERENT fact and they must not read alike. "Has not reported yet" and
 *  "reported a loss, so the multiple was dropped" produce the same blank in a chart and mean
 *  opposite things about the holding. */
const REASON: Record<string, string> = {
  no_point_in_period: 'no figure for this period',
  non_positive_multiple: 'loss-making — a negative multiple has no reciprocal',
  non_positive_base: 'no positive base year, so it cannot be rebased',
  no_data: 'no data for this metric',
  no_weight: 'zero weight in the model',
};

/** How the components relate to the line — different per kind, and saying so is the point. */
const SHARE_NOTE: Record<Breakdown['kind'], string> = {
  multiple: 'A multiple blends harmonically, so shares are of the aggregate 1/value (the yield). '
    + 'The cheapest holding carries the largest share, not the largest one.',
  ratio: 'A ratio blends arithmetically: share = weight × value, renormalised over the holdings '
    + 'that reported here.',
  level: 'A level is rebased to 100 at its first year before weighting, so the charted value is '
    + 'an index. The amount as reported is shown beside it.',
};

export default function BlendDrilldownModal({
  title, metricCode, period, portfolioId, basket, format, onClose,
}: {
  title: string;
  metricCode: string;
  period: string;
  portfolioId?: number;
  basket?: { holdings: { isin: string; weight: number; name?: string }[] };
  /** The chart's own formatter, so a value reads the same here as on the axis it came from. */
  format?: (v: number) => string;
  onClose: () => void;
}) {
  const [data, setData] = useState<Breakdown | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/fundamental-blend-breakdown`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            metric_code: metricCode, period,
            ...(basket
              ? { holdings: basket.holdings.map((h) => ({ isin: h.isin, name: h.name, weight: h.weight })) }
              : { portfolio_id: portfolioId }),
          }),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as Breakdown);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [metricCode, period, portfolioId, basket]);

  const fmt = (v: number | null) => (v == null ? '—' : format ? format(v) : v.toFixed(2));

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[80vw] h-[80vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">{title}</h2>
          <span className="text-fg-muted text-sm">{period}</span>
          {data?.value != null && (
            <span className="font-mono text-fg-strong">{fmt(data.value)}</span>
          )}
          {data && (
            <span className="text-[11px] text-fg-faint">
              blended over {data.covered_pct.toFixed(1)}% of weight
            </span>
          )}
          <button type="button" onClick={onClose}
            className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-4">
          {err && <p className="text-xs text-neg-300">{err}</p>}
          {!data && !err && <p className="text-xs text-fg-subtle">Loading breakdown…</p>}

          {data && (
            <>
              <p className="text-[11px] text-fg-faint max-w-3xl">{SHARE_NOTE[data.kind]}</p>

              <div className="overflow-auto rounded-lg border border-neutral-800/40">
                <table className="w-auto text-xs whitespace-nowrap">
                  <thead className="bg-page">
                    <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                      <th className="px-3 py-1.5 font-medium text-left">Holding</th>
                      <th className="px-3 py-1.5 font-medium text-right">Weight</th>
                      <th className="px-3 py-1.5 font-medium text-right">
                        {data.kind === 'level' ? 'Index' : 'Value'}
                      </th>
                      {data.kind === 'level' && (
                        <th className="px-3 py-1.5 font-medium text-right">As reported</th>
                      )}
                      <th className="px-3 py-1.5 font-medium text-right">Share</th>
                      <th className="px-3 py-1.5 font-medium text-right"
                        title="What the blended figure would read WITHOUT this holding. A share is size; this is influence — the two disagree constantly.">
                        Swing
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.members.map((m) => (
                      <tr key={`${m.isin ?? m.name}`} className="border-b border-neutral-800/20 hover:bg-overlay/[0.02]">
                        <td className="px-3 py-1.5 text-fg-soft">
                          <span className="inline-block max-w-[28ch] truncate align-bottom">{m.name ?? m.isin}</span>
                        </td>
                        <td className="px-3 py-1.5 text-right font-mono text-fg-muted">{m.weight_pct.toFixed(1)}%</td>
                        <td className="px-3 py-1.5 text-right font-mono text-fg-strong">{fmt(m.value)}</td>
                        {data.kind === 'level' && (
                          <td className="px-3 py-1.5 text-right font-mono text-fg-muted">
                            {m.raw_value == null ? '—' : m.raw_value.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                          </td>
                        )}
                        <td className="px-3 py-1.5 text-right font-mono text-fg-muted">
                          {m.share_pct == null ? '—' : `${m.share_pct.toFixed(1)}%`}
                        </td>
                        <td className={`px-3 py-1.5 text-right font-mono ${
                          (m.swing ?? 0) > 0 ? 'text-pos-400' : (m.swing ?? 0) < 0 ? 'text-neg-400' : 'text-fg-faint'}`}>
                          {m.swing == null ? '—' : `${m.swing > 0 ? '+' : ''}${fmt(m.swing)}`}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* ⚠ The exclusions are half the answer — a thin point can only be recognised as
                  thin if the weight that is NOT in it is named. */}
              {data.excluded.length > 0 && (
                <div className="space-y-1">
                  <h3 className="text-xs font-semibold text-fg-strong">
                    Not in this figure ({data.excluded_pct.toFixed(1)}% of weight)
                  </h3>
                  <div className="overflow-auto rounded-lg border border-neutral-800/40">
                    <table className="w-auto text-xs whitespace-nowrap">
                      <tbody>
                        {data.excluded.map((e) => (
                          <tr key={`${e.isin ?? e.name}`} className="border-b border-neutral-800/20">
                            <td className="px-3 py-1.5 text-fg-soft">
                              <span className="inline-block max-w-[28ch] truncate align-bottom">{e.name ?? e.isin}</span>
                            </td>
                            <td className="px-3 py-1.5 text-right font-mono text-fg-muted">{e.weight_pct.toFixed(1)}%</td>
                            <td className="px-3 py-1.5 text-fg-faint">{REASON[e.reason] ?? e.reason}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
