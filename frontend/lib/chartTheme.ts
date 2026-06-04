/**
 * Centralized colour tokens for recharts / SVG charts.
 *
 * Charts can't use the Tailwind `@theme` design tokens (recharts takes colour
 * strings as props, not utility classes), so this file is the chart-side
 * mirror of the surface + accent/pos/neg/warn ramps in `app/globals.css`.
 * Keep the two in sync — `accent`→indigo, `pos`→emerald, `neg`→rose/red,
 * `warn`→amber, greys→gray-*. Re-skin every chart by repointing values here.
 *
 * NOTE — three tooltip "families" + two grid greys exist because the pages
 * drifted historically: momentum/backtest matches the elevated surface,
 * /earnings the card surface, and sparkline/fx/indicators the popover surface.
 * Preserved exactly here for zero visual change; collapse to one tooltip look
 * later if you want them unified site-wide.
 */
export const chartTheme = {
  // ── Chrome ───────────────────────────────────────────────────────────────
  grid: '#1f2937',          // CartesianGrid stroke (momentum, fx, indicators, sparkline)
  gridEarnings: '#1e2330',  // CartesianGrid stroke on /earnings (a touch warmer)
  axisTick: '#6b7280',      // axis tick labels — gray-500
  axisLabel: '#9ca3af',     // tooltip / legend label text — gray-400
  zeroLine: '#374151',      // y=0 / reference line — gray-700

  // ── Semantic series colours (mirror the accent/pos/neg/warn ramps) ───────
  accent: '#818cf8',        // indigo-400 — primary strategy / series A / area fills
  accentStrong: '#6366f1',  // indigo-600 — price line (relative-growth)
  warn: '#f59e0b',          // amber-500 — comparison series B / alpha line
  pos: '#34d399',           // emerald-400 — OE actual
  neg: '#f87171',           // red-400 — OE estimate / negative-value dot
  negStrong: '#ef4444',     // red-500 — go-live line, period-avg / forward-PE mean
  negDeep: '#dc2626',       // red-600 — negative-value dot (series B)
  universe: '#9ca3af',      // gray-400 — universe baseline line

  // ── Go-live marker (vertical dashed line + its label) ────────────────────
  goLiveLine: '#ef4444',
  goLiveLabel: '#f87171',

  // ── Drawdown overlay (variable opacity per band) ─────────────────────────
  drawdown: (opacity: number) => `rgba(244,63,94,${opacity})`,

  // ── Tooltip surfaces (recharts contentStyle / labelStyle / itemStyle) ────
  tooltip: {                // momentum / backtest — elevated surface
    contentStyle: { background: '#1a1d27', border: '1px solid rgba(75,85,99,0.4)', borderRadius: 8, fontSize: 13 },
    labelStyle: { color: '#9ca3af' },
    itemStyle: { color: '#e5e7eb' },
  },
  tooltipPopover: {         // sparkline / fx / indicators — popover surface
    contentStyle: { backgroundColor: '#1e2230', border: '1px solid rgba(107,114,128,0.3)', borderRadius: '8px', fontSize: 12 },
    labelStyle: { color: '#9ca3af' },
  },
  tooltipCard: {            // /earnings — card surface
    contentStyle: { backgroundColor: '#151821', border: '1px solid #374151', borderRadius: '8px' },
    labelStyle: { color: '#9ca3af' },
  },

  // ── Qualitative multi-series palette (variant comparison lines) ──────────
  // index 0 = active strategy; the rest cycle for comparison series.
  series: ['#818cf8', '#f59e0b', '#34d399', '#f472b6', '#60a5fa', '#a78bfa', '#fb7185', '#22d3ee'],
} as const;
