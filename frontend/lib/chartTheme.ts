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
  // ── Chrome (light theme — Azure Blanc) ───────────────────────────────────
  grid: '#e2e8f0',          // CartesianGrid stroke — slate-200
  gridEarnings: '#e8edf4',  // CartesianGrid stroke on /earnings (a touch cooler)
  axisTick: '#64748b',      // axis tick labels — slate-500
  axisLabel: '#475569',     // tooltip / legend label text — slate-600
  zeroLine: '#cbd5e1',      // y=0 / reference line — slate-300

  // ── Semantic series colours (mirror the accent/pos/neg/warn ramps), tuned
  //    to read on white. ─────────────────────────────────────────────────
  accent: '#0ea5e9',        // sky-500 — primary strategy / series A / area fills
  accentStrong: '#0284c7',  // sky-600 — price line (relative-growth)
  warn: '#d97706',          // amber-600 — comparison series B / alpha line
  pos: '#0f9d58',           // green — OE actual
  neg: '#dc2626',           // red-600 — OE estimate / negative-value dot
  negStrong: '#b91c1c',     // red-700 — go-live line, period-avg / forward-PE mean
  negDeep: '#991b1b',       // red-800 — negative-value dot (series B)
  universe: '#64748b',      // slate-500 — universe baseline line

  // ── Go-live marker (vertical dashed line + its label) ────────────────────
  goLiveLine: '#dc2626',
  goLiveLabel: '#b91c1c',

  // ── Drawdown overlay (variable opacity per band) ─────────────────────────
  drawdown: (opacity: number) => `rgba(220,38,38,${opacity})`,

  // ── Tooltip surfaces (recharts contentStyle / labelStyle / itemStyle).
  //    White surfaces with a soft navy hairline + drop shadow on light. ────
  tooltip: {                // momentum / backtest — elevated surface
    contentStyle: { background: '#ffffff', border: '1px solid rgba(20,32,80,0.12)', borderRadius: 8, fontSize: 13, boxShadow: '0 8px 24px -10px rgba(20,32,80,0.2)' },
    labelStyle: { color: '#64748b' },
    itemStyle: { color: '#1d2a44' },
  },
  tooltipPopover: {         // sparkline / fx / indicators — popover surface
    contentStyle: { backgroundColor: '#ffffff', border: '1px solid rgba(20,32,80,0.10)', borderRadius: '8px', fontSize: 12, boxShadow: '0 8px 24px -10px rgba(20,32,80,0.18)' },
    labelStyle: { color: '#64748b' },
  },
  tooltipCard: {            // /earnings — card surface
    contentStyle: { backgroundColor: '#ffffff', border: '1px solid #e2e8f0', borderRadius: '8px', boxShadow: '0 8px 24px -10px rgba(20,32,80,0.18)' },
    labelStyle: { color: '#64748b' },
  },

  // ── Qualitative multi-series palette (variant comparison lines), all
  //    readable on white. index 0 = active strategy; rest cycle. ──────────
  series: ['#0ea5e9', '#d97706', '#0f9d58', '#db2777', '#2563eb', '#7c3aed', '#e11d48', '#0891b2'],
} as const;
