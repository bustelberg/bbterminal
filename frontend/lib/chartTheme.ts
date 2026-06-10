/**
 * Centralized colour tokens for recharts / SVG charts.
 *
 * Charts can't use the Tailwind `@theme` design tokens (recharts takes colour
 * strings as props, not utility classes), so this file is the chart-side
 * mirror of the surface + accent/pos/neg/warn ramps in `app/globals.css`.
 * Keep the two in sync — for the "Paper" theme: `accent`→steel-blue,
 * `pos`→muted green, `neg`→muted red, `warn`→muted amber, greys→cool-gray.
 * Re-skin every chart by repointing values here.
 *
 * NOTE — three tooltip "families" + two grid greys exist because the pages
 * drifted historically: momentum/backtest matches the elevated surface,
 * /earnings the card surface, and sparkline/fx/indicators the popover surface.
 * Preserved exactly here for zero structural change; collapse to one tooltip
 * look later if you want them unified site-wide.
 */
export const chartTheme = {
  // ── Chrome (light theme — Paper, calm) ───────────────────────────────────
  grid: '#eceef2',          // CartesianGrid stroke — soft cool gray
  gridEarnings: '#eef0f4',  // CartesianGrid stroke on /earnings (a touch lighter)
  axisTick: '#6c757f',      // axis tick labels — fg-subtle
  axisLabel: '#525c67',     // tooltip / legend label text — fg-muted
  zeroLine: '#d7dce2',      // y=0 / reference line — soft gray

  // ── Semantic series colours (mirror the accent/pos/neg/warn ramps), muted
  //    and tuned to read on white. ─────────────────────────────────────────
  accent: '#3b82c9',        // brand blue — primary strategy / series A / area fills
  accentStrong: '#2c6bb0',  // deeper blue — price line (relative-growth)
  // Non-status line colours. Green/amber/red are RESERVED for the scoring
  // bands, so comparison + multi-metric LINES use these instead.
  compare: '#7c5cc0',       // violet — comparison series B (vs blue series A)
  magenta: '#c44f9c',       // magenta — extra qualitative line (relative-growth OE estimate)
  warn: '#c0891a',          // amber — comparison series B / alpha line
  pos: '#2ca86a',           // green — OE actual
  neg: '#d8443d',           // red — OE estimate / negative-value dot
  negStrong: '#b5352f',     // deeper red — go-live line, period-avg / forward-PE mean
  negDeep: '#8f2a26',       // darkest red — negative-value dot (series B)
  universe: '#828c9b',      // cool-gray — universe baseline line

  // ── Go-live marker (vertical dashed line + its label) ────────────────────
  goLiveLine: '#d8443d',
  goLiveLabel: '#b5352f',

  // ── Drawdown overlay (variable opacity per band) ─────────────────────────
  drawdown: (opacity: number) => `rgba(216,68,61,${opacity})`,

  // ── Tooltip surfaces (recharts contentStyle / labelStyle / itemStyle).
  //    White surfaces with a soft cool-gray hairline + a whisper shadow. ────
  tooltip: {                // momentum / backtest — elevated surface
    contentStyle: { background: '#ffffff', border: '1px solid #e6e9ef', borderRadius: 8, fontSize: 13, boxShadow: '0 8px 24px -14px rgba(17,24,39,0.18)' },
    labelStyle: { color: '#6c757f' },
    itemStyle: { color: '#283039' },
  },
  tooltipPopover: {         // sparkline / fx / indicators — popover surface
    contentStyle: { backgroundColor: '#ffffff', border: '1px solid #e9ecf1', borderRadius: '8px', fontSize: 12, boxShadow: '0 8px 24px -14px rgba(17,24,39,0.16)' },
    labelStyle: { color: '#6c757f' },
  },
  tooltipCard: {            // /earnings — card surface
    contentStyle: { backgroundColor: '#ffffff', border: '1px solid #e9ecf1', borderRadius: '8px', boxShadow: '0 8px 24px -14px rgba(17,24,39,0.16)' },
    labelStyle: { color: '#6c757f' },
  },

  // ── Qualitative multi-series palette (variant comparison lines), all
  //    muted + readable on white. index 0 = active strategy; rest cycle. ────
  series: ['#3b82c9', '#c0891a', '#2ca86a', '#cf5577', '#6a8fd0', '#8b6fc0', '#27a3ac', '#d08a3a'],
} as const;
