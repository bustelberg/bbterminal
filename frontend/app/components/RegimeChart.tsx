'use client';

import { useMemo, useRef, useState } from 'react';

/** bull/bear × calm/turbulent regime of the equal-weight universe index
 * (etoro-yfinance methodology). Background bands = regime; line = the index;
 * dashed = its 200-day mean. Self-contained SVG (works light + dark). */

export type RegimeData = {
  dates: string[]; index: number[]; ma200: number[];
  bull: boolean[]; turb: boolean[];
  current?: { bull: boolean; turb: boolean; date: string };
  universe?: { name?: string; size?: number };
};

// bull? + turbulent? → the four regimes.
const REG = {
  bc: { label: 'Bull · Calm', fill: 'rgba(22,163,74,0.16)', dot: '#16a34a' },
  bt: { label: 'Bull · Turbulent', fill: 'rgba(217,119,6,0.16)', dot: '#d97706' },
  rc: { label: 'Bear · Calm', fill: 'rgba(234,88,12,0.18)', dot: '#ea580c' },
  rt: { label: 'Bear · Turbulent', fill: 'rgba(220,38,38,0.18)', dot: '#dc2626' },
} as const;
type RegKey = keyof typeof REG;
const regKey = (b: boolean, t: boolean): RegKey => (b ? (t ? 'bt' : 'bc') : (t ? 'rt' : 'rc'));

const W = 960, H = 340, padL = 6, padR = 6, padT = 12, padB = 22;

export default function RegimeChart({ data, bands = true }: { data: RegimeData; bands?: boolean }) {
  const n = data.dates.length;
  const svgRef = useRef<SVGSVGElement>(null);
  const [hi, setHi] = useState<number | null>(null);

  const g = useMemo(() => {
    if (n < 2) return null;
    const base = data.index[0] || 1;
    const reb = data.index.map((v) => (v / base) * 100);
    const mar = data.ma200.map((v) => (v / base) * 100);
    let lo = Math.min(...reb, ...mar), top = Math.max(...reb, ...mar);
    const pad = (top - lo) * 0.05 || 1; lo -= pad; top += pad;
    const x = (i: number) => padL + (i / (n - 1)) * (W - padL - padR);
    const y = (v: number) => padT + (1 - (v - lo) / (top - lo)) * (H - padT - padB);
    const path = (a: number[]) => a.map((v, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join('');

    // Contiguous regime runs → background bands.
    const bands: { key: RegKey; x0: number; x1: number }[] = [];
    let s = 0;
    for (let i = 1; i <= n; i++) {
      const prev = regKey(data.bull[s], data.turb[s]);
      if (i === n || regKey(data.bull[i], data.turb[i]) !== prev) {
        bands.push({ key: prev, x0: x(s), x1: i >= n ? W - padR : x(i) });
        s = i;
      }
    }
    // Year gridlines.
    const xTicks: { x: number; label: string }[] = [];
    let lastYr = '';
    for (let i = 0; i < n; i++) {
      const yr = data.dates[i].slice(0, 4);
      if (yr !== lastYr) { lastYr = yr; xTicks.push({ x: x(i), label: yr }); }
    }
    return { reb, line: path(reb), ma: path(mar), bands, xTicks, x };
  }, [data, n]);

  if (!g) return <div className="text-xs text-fg-subtle py-6 text-center">Not enough history to chart a regime.</div>;

  const onMove = (e: React.MouseEvent) => {
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect) return;
    const vbX = ((e.clientX - rect.left) / rect.width) * W;
    const i = Math.round(((vbX - padL) / (W - padL - padR)) * (n - 1));
    setHi(Math.max(0, Math.min(n - 1, i)));
  };
  const cur = data.current;

  return (
    <div className="space-y-2">
      {bands && (
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div className="flex items-center gap-3 flex-wrap text-[11px]">
            {(Object.keys(REG) as RegKey[]).map((k) => (
              <span key={k} className="flex items-center gap-1.5 text-fg-muted">
                <span className="inline-block h-2.5 w-2.5 rounded-sm" style={{ background: REG[k].dot }} />{REG[k].label}
              </span>
            ))}
          </div>
          {cur && (
            <span className="text-[11px] px-2 py-0.5 rounded-full border font-medium"
              style={{ borderColor: REG[regKey(cur.bull, cur.turb)].dot, color: REG[regKey(cur.bull, cur.turb)].dot }}>
              Now: {REG[regKey(cur.bull, cur.turb)].label}
            </span>
          )}
        </div>
      )}

      <div className="relative">
        <svg ref={svgRef} viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ height: 'auto' }}
          onMouseMove={onMove} onMouseLeave={() => setHi(null)}>
          {bands && g.bands.map((b, i) => (
            <rect key={i} x={b.x0} y={padT} width={Math.max(0.5, b.x1 - b.x0)} height={H - padT - padB} fill={REG[b.key].fill} />
          ))}
          {g.xTicks.map((t, i) => (
            <g key={i}>
              <line x1={t.x} x2={t.x} y1={padT} y2={H - padB} stroke="var(--color-neutral-800)" strokeOpacity={0.15} />
              <text x={t.x + 2} y={H - 8} fontSize={9} fill="var(--color-fg-faint)" className="font-mono">{t.label}</text>
            </g>
          ))}
          <path d={g.ma} fill="none" stroke="var(--color-fg-faint)" strokeWidth={1} strokeDasharray="3 3" opacity={0.8} />
          <path d={g.line} fill="none" stroke="var(--color-fg-strong)" strokeWidth={1.4} />
          {hi != null && (
            <line x1={g.x(hi)} x2={g.x(hi)} y1={padT} y2={H - padB} stroke="var(--color-accent-500)" strokeWidth={1} strokeOpacity={0.6} />
          )}
        </svg>
        {hi != null && (
          <div className="absolute top-0 pointer-events-none bg-popover border border-neutral-800/40 rounded-lg px-2.5 py-1.5 text-[11px] shadow-md"
            style={{ left: `calc(${(g.x(hi) / W) * 100}% + 6px)`, transform: g.x(hi) > W * 0.7 ? 'translateX(-110%)' : undefined }}>
            <div className="font-mono text-fg-soft">{data.dates[hi]}</div>
            {bands && (
              <div className="flex items-center gap-1.5 mt-0.5" style={{ color: REG[regKey(data.bull[hi], data.turb[hi])].dot }}>
                <span className="inline-block h-2 w-2 rounded-sm" style={{ background: REG[regKey(data.bull[hi], data.turb[hi])].dot }} />
                {REG[regKey(data.bull[hi], data.turb[hi])].label}
              </div>
            )}
            <div className="text-fg-muted font-mono mt-0.5">index {g.reb[hi].toFixed(1)}</div>
          </div>
        )}
      </div>
    </div>
  );
}
