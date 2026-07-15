'use client';

import { useEffect, useState } from 'react';
import type { CSSProperties } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import type { PortfolioCorrelationMatrix } from '../../lib/types/api';

type Window = 'ytd' | 'trailing_12m';

const WINDOWS: { key: Window; label: string }[] = [
  { key: 'ytd', label: 'YTD' },
  { key: 'trailing_12m', label: 'Trailing 12m' },
];

const fmtCorr = (v: number | null | undefined) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}`;

/** The "no number here" marker — a single calm black cross on white. ONE component, rendered
 * both in the grid's null cells and the legend swatch, so the key is exactly what's on the grid
 * and they cannot drift. Stroke is `fg-strong` (#11161d, near-black ink). */
function NullMark() {
  return (
    <svg viewBox="0 0 10 10" preserveAspectRatio="none" aria-hidden
      style={{ display: 'block', width: '100%', height: '100%' }}>
      <line x1="2.5" y1="2.5" x2="7.5" y2="7.5"
        stroke="var(--color-fg-strong)" strokeWidth="1" strokeLinecap="round" />
      <line x1="7.5" y1="2.5" x2="2.5" y2="7.5"
        stroke="var(--color-fg-strong)" strokeWidth="1" strokeLinecap="round" />
    </svg>
  );
}

/**
 * Diverging cell colour, CVD-SAFE by construction: blue (accent) for NEGATIVE correlation
 * (a diversifier) ↔ amber (warn) for POSITIVE (moves together), neutral at 0. Red↔green — the
 * intuitive choice — is exactly the pair that collapses under deuteranopia; the app's own
 * palette note measures blue+amber at ΔE 103 against that pair's 4.9, so we use it here.
 * Magnitude drives the tint alpha; the exact value lives in the hover readout.
 */
function cellStyle(v: number | null, isDiag: boolean): CSSProperties {
  const base: CSSProperties = { border: '1px solid var(--color-card)' };
  if (isDiag) return { ...base, background: 'var(--color-inset)' };
  if (v == null) {
    // No number to make — too few overlapping returns. A white cell carrying a single cross
    // (NullMark), read as "unknown", not as 0.
    return { ...base, background: 'var(--color-elevated)' };
  }
  const a = Math.min(1, Math.abs(v));
  const pct = Math.round(8 + a * 64); // 8% → 72% saturation
  const hue = v >= 0 ? 'var(--color-warn-500)' : 'var(--color-accent-500)';
  return { ...base, background: `color-mix(in srgb, ${hue} ${pct}%, transparent)` };
}

/** Portfolio codes are long; keep the axis compact but keep the full value for the tooltip. */
const short = (l: string) => (l.length > 22 ? `${l.slice(0, 21)}…` : l);

/**
 * Pairwise correlation of the LISTED (>5-holding) AIRS model portfolios' daily EUR returns —
 * the "42 of 95" the table above shows by default. Two windows (YTD, trailing 12m), each a
 * heatmap over the SAME return series the YTD column is read off, so the matrix cannot disagree
 * with the numbers it sits under. A cell is null (hatched) when the pair share fewer than
 * `min_overlap_days` common returns — a model defined last week has nothing to correlate yet.
 */
export default function CorrelationMatrix() {
  const [data, setData] = useState<PortfolioCorrelationMatrix | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [win, setWin] = useState<Window>('ytd');
  const [hover, setHover] = useState<{ i: number; j: number } | null>(null);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/airs/model-portfolios/correlations`);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const d = (await r.json()) as PortfolioCorrelationMatrix;
        if (!cancelled) setData(d);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, []);

  const matrix = data ? data[win] : [];
  const obs = data ? (win === 'ytd' ? data.ytd_obs : data.trailing_12m_obs) : [];
  const labels = data?.labels ?? [];

  const readout = data && hover
    ? {
      a: labels[hover.i], b: labels[hover.j],
      v: matrix[hover.i]?.[hover.j] ?? null,
      overlap: Math.min(obs[hover.i] ?? 0, obs[hover.j] ?? 0),
      self: hover.i === hover.j,
    }
    : null;

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div className="flex items-start justify-between gap-3 flex-wrap">
        <div>
          <h3 className="text-sm font-semibold text-fg-strong">Portfolio correlations</h3>
          <p className="text-[11px] text-fg-faint mt-0.5">
            Pairwise correlation of daily EUR returns across the listed model portfolios
            {data ? ` · ${labels.length}` : ''} — the same return series their YTD is read off.
            Blue = moves oppositely (diversifying), amber = moves together.
          </p>
        </div>
        <div className="flex rounded-lg border border-neutral-800/40 overflow-hidden text-[11px]">
          {WINDOWS.map((w) => (
            <button key={w.key} onClick={() => setWin(w.key)}
              className={`px-3 py-1 transition-colors ${
                win === w.key ? 'bg-accent-600 text-white' : 'text-fg-soft hover:bg-overlay/5'
              }`}>
              {w.label}
            </button>
          ))}
        </div>
      </div>

      {loading && <p className="text-xs text-fg-subtle">Computing…</p>}
      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>
      )}

      {!loading && !error && data && labels.length > 0 && (
        <>
          {/* Hover readout — one pair at a time, so cells stay color-only at this density. */}
          <div className="text-[11px] min-h-[1.5rem] flex items-center gap-2">
            {readout ? (
              readout.self ? (
                <span className="text-fg-soft"><span className="font-medium text-fg">{readout.a}</span> — self</span>
              ) : (
                <span className="text-fg-soft">
                  <span className="font-medium text-fg">{readout.a}</span>
                  <span className="text-fg-faint"> ↔ </span>
                  <span className="font-medium text-fg">{readout.b}</span>
                  <span className="text-fg-faint"> · {WINDOWS.find((w) => w.key === win)!.label} corr </span>
                  <span className={`font-mono font-semibold ${
                    readout.v == null ? 'text-fg-faint'
                      : readout.v >= 0 ? 'text-warn-400' : 'text-accent-400'
                  }`}>{fmtCorr(readout.v)}</span>
                  {readout.v == null
                    ? <span className="text-fg-faint"> (&lt; {data.min_overlap_days}d overlap)</span>
                    : <span className="text-fg-faint"> · {readout.overlap} overlapping days</span>}
                </span>
              )
            ) : (
              <span className="text-fg-faint">Hover a cell to read the pair. As of {data.as_of}.</span>
            )}
          </div>

          {/* Legend */}
          <div className="flex items-center gap-x-4 gap-y-2 text-[10px] text-fg-faint flex-wrap">
            <div className="inline-flex items-center gap-2">
              <span className="text-accent-400">−1 perfect negative correlation</span>
              <div className="flex flex-col items-stretch gap-0.5">
                <div className="h-2.5 w-44 rounded border border-neutral-800/30"
                  style={{ background: 'linear-gradient(to right, var(--color-accent-500), var(--color-inset), var(--color-warn-500))' }} />
                <span className="text-center leading-none">0 uncorrelated</span>
              </div>
              <span className="text-warn-400">perfect positive correlation +1</span>
            </div>
            <span className="inline-flex items-center gap-1.5 ml-auto">
              <span className="inline-block w-5 h-5 rounded-sm border border-neutral-700 overflow-hidden"
                style={{ background: 'var(--color-elevated)' }}>
                <NullMark />
              </span>
              too few overlapping days
            </span>
          </div>

          <div className="overflow-auto max-h-[75vh] rounded-lg border border-neutral-800/40">
            <table className="border-separate text-[10px]" style={{ borderSpacing: 0 }}>
              <thead>
                <tr>
                  <th className="sticky left-0 top-0 z-30 bg-card" />
                  {labels.map((l, j) => (
                    <th key={j} title={l}
                      className={`sticky top-0 z-20 bg-card h-24 align-bottom p-0 font-normal ${
                        hover?.j === j ? 'text-accent-400 font-semibold' : 'text-fg-muted'
                      }`}>
                      <div className="mx-auto whitespace-nowrap [writing-mode:vertical-rl] rotate-180 pb-1"
                        style={{ maxHeight: '5.5rem', overflow: 'hidden' }}>
                        {short(l)}
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {labels.map((l, i) => (
                  <tr key={i}>
                    <th title={l}
                      className={`sticky left-0 z-10 bg-card text-right pr-2 pl-1 whitespace-nowrap font-normal max-w-[11rem] truncate ${
                        hover?.i === i ? 'text-accent-400 font-semibold' : 'text-fg-muted'
                      }`}>
                      {short(l)}
                    </th>
                    {matrix[i].map((v, j) => (
                      <td key={j}
                        onMouseEnter={() => setHover({ i, j })}
                        onMouseLeave={() => setHover(null)}
                        style={{ ...cellStyle(v, i === j), width: 20, height: 20, minWidth: 20 }}
                        className="cursor-default p-0">
                        {i !== j && v == null && <NullMark />}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <p className="text-[10px] text-fg-faint leading-relaxed">
            Pearson correlation of daily EUR returns, pairwise-complete: each pair uses only the
            trading days both portfolios have a return, so different inception dates shorten the
            overlap rather than inject zeros. A cell needs at least {data.min_overlap_days} common
            returns or it is left blank. The matrix is symmetric — a pair reads the same either
            way; the diagonal is a portfolio with itself.
          </p>
        </>
      )}
    </section>
  );
}
