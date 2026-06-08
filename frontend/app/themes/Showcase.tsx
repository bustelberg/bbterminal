import Link from 'next/link';
import type { CSSProperties, ReactNode } from 'react';
import type { Theme } from './themes';

/**
 * Full-screen theme preview. A self-contained mock of a BBTerminal-style
 * dashboard (chrome, KPIs, a holdings table with gains/losses, charts,
 * buttons, inputs, badges, alerts, typography) rendered entirely against the
 * theme's `--t-*` CSS custom properties via inline styles — so the SAME
 * markup demonstrates any candidate theme. `fixed inset-0` so it covers the
 * real app chrome for an honest, edge-to-edge look.
 */

// ── token shorthands (read the --t-* vars set on the root) ──────────
const C = {
  bg: 'var(--t-bg)', sidebar: 'var(--t-sidebar)', card: 'var(--t-card)',
  cardAlt: 'var(--t-card-alt)', elevated: 'var(--t-elevated)', inset: 'var(--t-inset)',
  border: 'var(--t-border)', borderStrong: 'var(--t-border-strong)',
  fg: 'var(--t-fg)', fgMuted: 'var(--t-fg-muted)', fgSubtle: 'var(--t-fg-subtle)',
  accent: 'var(--t-accent)', accent2: 'var(--t-accent-2)', accentFg: 'var(--t-accent-fg)',
  accentSoft: 'var(--t-accent-soft)', pos: 'var(--t-pos)', neg: 'var(--t-neg)',
  warn: 'var(--t-warn)', radius: 'var(--t-radius)', shadow: 'var(--t-shadow)',
};

const cardStyle: CSSProperties = {
  background: C.card, border: `1px solid ${C.border}`,
  borderRadius: C.radius, boxShadow: C.shadow,
};

const HOLDINGS = [
  { tk: 'KIOXIA', ex: 'TSE', sec: 'Technology', px: '4,452 JPY', mtd: 3.18, ytd: 41.2, w: 4.4 },
  { tk: 'BE', ex: 'NYSE', sec: 'Utilities', px: '28.40 USD', mtd: -5.62, ytd: 12.7, w: 4.2 },
  { tk: 'SOI', ex: 'XPAR', sec: 'Technology', px: '146.2 EUR', mtd: 1.05, ytd: -8.4, w: 4.1 },
  { tk: 'VSH', ex: 'NYSE', sec: 'Industrials', px: '19.86 USD', mtd: -2.10, ytd: 22.9, w: 3.9 },
  { tk: 'NEL', ex: 'OSL', sec: 'Materials', px: '2.98 NOK', mtd: 7.44, ytd: -31.5, w: 3.7 },
  { tk: 'UMI', ex: 'XBRU', sec: 'Materials', px: '23.42 EUR', mtd: 0.42, ytd: 5.1, w: 3.6 },
];

const MONTHLY = [2.1, -1.4, 3.8, 5.2, -2.6, 1.1, 4.4, -0.8, 2.9, -5.2, 6.1, 3.3];

function pct(v: number) {
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
}

export default function Showcase({ theme }: { theme: Theme }) {
  const rootStyle = { ...theme.vars, fontFamily: theme.font, background: C.bg, color: C.fg } as CSSProperties;

  return (
    <div className="fixed inset-0 z-[100] overflow-auto" style={rootStyle}>
      <div className="flex min-h-full">
        {/* ── Sidebar (chrome surface) ───────────────────────────── */}
        <aside
          className="hidden md:flex w-56 shrink-0 flex-col gap-1 p-3"
          style={{ background: C.sidebar, borderRight: `1px solid ${C.border}` }}
        >
          <div className="flex items-center gap-2 px-2 py-3 mb-2">
            <span className="inline-block h-6 w-6 rounded-md" style={{ background: C.accent }} />
            <span className="text-sm font-semibold tracking-tight">BBTerminal</span>
          </div>
          {['Dashboard', 'Backtest', 'Schedule', 'Companies', 'Universe', 'Earnings', 'FX Rates'].map((n, i) => (
            <div
              key={n}
              className="px-3 py-2 text-sm font-medium"
              style={
                i === 0
                  ? { background: C.accentSoft, color: C.accent, borderRadius: C.radius }
                  : { color: C.fgMuted, borderRadius: C.radius }
              }
            >
              {n}
            </div>
          ))}
          <div className="mt-auto px-3 py-2 text-xs" style={{ color: C.fgSubtle }}>
            v4.8 · {theme.name}
          </div>
        </aside>

        {/* ── Main column ────────────────────────────────────────── */}
        <main className="flex-1 min-w-0">
          {/* Top bar */}
          <header
            className="sticky top-0 z-10 flex items-center gap-3 px-6 py-3"
            style={{ background: C.bg, borderBottom: `1px solid ${C.border}` }}
          >
            <Link
              href="/themes"
              className="text-xs px-2.5 py-1.5 font-medium"
              style={{ color: C.fgMuted, border: `1px solid ${C.border}`, borderRadius: C.radius }}
            >
              ← Themes
            </Link>
            <div
              className="flex-1 max-w-sm px-3 py-1.5 text-sm"
              style={{ background: C.inset, border: `1px solid ${C.border}`, borderRadius: C.radius, color: C.fgSubtle }}
            >
              Search tickers, universes…
            </div>
            <button className="text-sm font-medium px-3 py-1.5" style={{ background: C.accent, color: C.accentFg, borderRadius: C.radius, boxShadow: C.shadow }}>
              Run backtest
            </button>
            <span className="inline-flex h-8 w-8 items-center justify-center text-xs font-semibold" style={{ background: C.elevated, color: C.fg, borderRadius: '999px', border: `1px solid ${C.border}` }}>
              RB
            </span>
          </header>

          <div className="px-6 py-6 space-y-6 max-w-[1100px]">
            {/* Page header */}
            <div>
              <div className="text-xs uppercase tracking-wider mb-1" style={{ color: C.fgSubtle }}>{theme.name}</div>
              <h1 className="text-2xl font-semibold tracking-tight">Momentum Portfolio</h1>
              <p className="text-sm mt-1 max-w-2xl" style={{ color: C.fgMuted }}>{theme.tagline}</p>
            </div>

            {/* KPI cards */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              {[
                { label: 'Total value', val: '€1.84M', delta: 1.2, sub: 'since inception' },
                { label: 'MTD return', val: '−5.18%', delta: -5.18, sub: 'as of 2026-06-05' },
                { label: 'YTD return', val: '+49.6%', delta: 49.6, sub: 'vs ACWI +11.3%' },
                { label: 'Sharpe', val: '1.42', delta: 0.0, sub: '3y rolling', neutral: true },
              ].map((k) => (
                <div key={k.label} className="p-4" style={cardStyle}>
                  <div className="text-xs mb-1.5" style={{ color: C.fgSubtle }}>{k.label}</div>
                  <div className="text-2xl font-semibold tabular-nums">{k.val}</div>
                  <div className="text-xs mt-1.5 tabular-nums" style={{ color: k.neutral ? C.fgMuted : k.delta >= 0 ? C.pos : C.neg }}>
                    {!k.neutral && (k.delta >= 0 ? '▲ ' : '▼ ')}{k.sub}
                  </div>
                </div>
              ))}
            </div>

            {/* Two-column: table + side panel */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              {/* Holdings table */}
              <div className="lg:col-span-2 overflow-hidden" style={cardStyle}>
                <div className="flex items-center justify-between px-4 py-3" style={{ borderBottom: `1px solid ${C.border}` }}>
                  <h2 className="text-sm font-semibold">Current holdings</h2>
                  <span className="text-xs" style={{ color: C.fgSubtle }}>6 of 24 · sector-weighted</span>
                </div>
                <table className="w-full text-sm">
                  <thead>
                    <tr style={{ color: C.fgSubtle }}>
                      {['Ticker', 'Sector', 'Price', 'MTD', 'YTD', 'Wt'].map((h, i) => (
                        <th key={h} className={`px-4 py-2 font-medium text-xs ${i >= 2 ? 'text-right' : 'text-left'}`}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {HOLDINGS.map((r, i) => (
                      <tr key={r.tk} style={{ borderTop: `1px solid ${C.border}`, background: i % 2 ? C.cardAlt : 'transparent' }}>
                        <td className="px-4 py-2.5">
                          <span className="font-semibold" style={{ color: C.accent }}>{r.tk}</span>
                          <span className="ml-1 text-xs" style={{ color: C.fgSubtle }}>·{r.ex}</span>
                        </td>
                        <td className="px-4 py-2.5" style={{ color: C.fgMuted }}>{r.sec}</td>
                        <td className="px-4 py-2.5 text-right tabular-nums" style={{ color: C.fg }}>{r.px}</td>
                        <td className="px-4 py-2.5 text-right tabular-nums" style={{ color: r.mtd >= 0 ? C.pos : C.neg }}>{pct(r.mtd)}</td>
                        <td className="px-4 py-2.5 text-right tabular-nums" style={{ color: r.ytd >= 0 ? C.pos : C.neg }}>{pct(r.ytd)}</td>
                        <td className="px-4 py-2.5 text-right tabular-nums" style={{ color: C.fgMuted }}>{r.w.toFixed(1)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Side panel: chips + bar chart + sparkline */}
              <div className="space-y-4">
                <div className="p-4" style={cardStyle}>
                  <div className="text-xs mb-2" style={{ color: C.fgSubtle }}>Filters</div>
                  <div className="flex flex-wrap gap-2">
                    <Chip label="Long-only" active />
                    <Chip label="By sector" active />
                    <Chip label="Min $30" />
                    <Chip label="ACWI" />
                  </div>
                </div>

                <div className="p-4" style={cardStyle}>
                  <div className="flex items-center justify-between mb-3">
                    <div className="text-xs" style={{ color: C.fgSubtle }}>Monthly returns</div>
                    <div className="text-xs tabular-nums" style={{ color: C.pos }}>+18.2% yr</div>
                  </div>
                  <div className="flex items-end gap-1 h-24">
                    {MONTHLY.map((m, i) => (
                      <div key={i} className="flex-1 flex flex-col justify-end h-full">
                        <div
                          style={{
                            height: `${Math.max(6, Math.abs(m) / 6.1 * 100)}%`,
                            background: m >= 0 ? C.pos : C.neg,
                            borderRadius: '2px',
                            opacity: 0.85,
                          }}
                        />
                      </div>
                    ))}
                  </div>
                </div>

                {/* Sparkline */}
                <div className="p-4" style={cardStyle}>
                  <div className="text-xs mb-2" style={{ color: C.fgSubtle }}>Equity curve</div>
                  <svg viewBox="0 0 200 60" className="w-full h-16" preserveAspectRatio="none">
                    <defs>
                      <linearGradient id={`g-${theme.slug}`} x1="0" y1="0" x2="1" y2="0">
                        <stop offset="0%" stopColor={C.accent} />
                        <stop offset="100%" stopColor={C.accent2} />
                      </linearGradient>
                    </defs>
                    <polyline
                      fill="none"
                      stroke={`url(#g-${theme.slug})`}
                      strokeWidth="2"
                      points="0,52 18,46 36,48 54,38 72,40 90,30 108,33 126,22 144,26 162,14 180,18 200,6"
                    />
                  </svg>
                </div>
              </div>
            </div>

            {/* Component gallery */}
            <div className="p-5 space-y-5" style={cardStyle}>
              <h2 className="text-sm font-semibold">Components</h2>

              {/* Buttons */}
              <Row label="Buttons">
                <button className="text-sm font-medium px-3.5 py-1.5" style={{ background: C.accent, color: C.accentFg, borderRadius: C.radius }}>Primary</button>
                <button className="text-sm font-medium px-3.5 py-1.5" style={{ background: C.elevated, color: C.fg, border: `1px solid ${C.border}`, borderRadius: C.radius }}>Secondary</button>
                <button className="text-sm font-medium px-3.5 py-1.5" style={{ color: C.fgMuted, borderRadius: C.radius }}>Ghost</button>
                <button className="text-sm font-medium px-3.5 py-1.5" style={{ background: 'transparent', color: C.neg, border: `1px solid ${C.neg}`, borderRadius: C.radius }}>Danger</button>
                <button className="text-sm font-medium px-3.5 py-1.5 opacity-40" style={{ background: C.elevated, color: C.fg, border: `1px solid ${C.border}`, borderRadius: C.radius }}>Disabled</button>
              </Row>

              {/* Badges */}
              <Row label="Badges">
                <Badge color={C.accent} soft={C.accentSoft}>Active</Badge>
                <Badge color={C.pos} soft="transparent">Gain</Badge>
                <Badge color={C.neg} soft="transparent">Loss</Badge>
                <Badge color={C.warn} soft="transparent">Stale</Badge>
                <Badge color={C.fgMuted} soft="transparent">Neutral</Badge>
              </Row>

              {/* Inputs */}
              <Row label="Inputs">
                <div className="px-3 py-1.5 text-sm w-44" style={{ background: C.inset, border: `1px solid ${C.borderStrong}`, borderRadius: C.radius, color: C.fg }}>Text input</div>
                <div className="px-3 py-1.5 text-sm w-44 flex items-center justify-between" style={{ background: C.inset, border: `1px solid ${C.border}`, borderRadius: C.radius, color: C.fgMuted }}>
                  Select<span style={{ color: C.fgSubtle }}>▾</span>
                </div>
                {/* toggle */}
                <span className="inline-flex items-center h-6 w-11 p-0.5" style={{ background: C.accent, borderRadius: '999px' }}>
                  <span className="h-5 w-5 ml-auto" style={{ background: '#fff', borderRadius: '999px' }} />
                </span>
                {/* checkbox */}
                <span className="inline-flex h-5 w-5 items-center justify-center text-xs" style={{ background: C.accent, color: C.accentFg, borderRadius: '4px' }}>✓</span>
                <span className="px-3 py-1.5 text-sm" style={{ border: `1px solid ${C.accent}`, boxShadow: `0 0 0 3px ${C.accentSoft}`, borderRadius: C.radius, color: C.fg }}>Focused</span>
              </Row>

              {/* Alerts */}
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <Alert color={C.pos} title="Rebalance complete" body="24 holdings priced through 2026-06-05." />
                <Alert color={C.neg} title="Pipeline error" body="3 of 401 companies failed to refresh." />
                <Alert color={C.warn} title="Data stale" body="ACWI universe is 8 days behind." />
                <Alert color={C.accent} title="Next rebalance" body="Scheduled for Jul 06, 2026 · 04:00." />
              </div>

              {/* Typography */}
              <Row label="Type">
                <div className="space-y-1">
                  <div className="text-2xl font-semibold">Display 24</div>
                  <div className="text-base font-medium">Heading 16</div>
                  <div className="text-sm" style={{ color: C.fg }}>Body 14 — the quick brown fox.</div>
                  <div className="text-xs" style={{ color: C.fgMuted }}>Muted 12 · secondary copy</div>
                  <div className="text-xs" style={{ color: C.fgSubtle }}>Subtle 12 · captions</div>
                  <div className="text-sm tabular-nums tracking-tight">1,234,567.89 · +49.61% · −5.18%</div>
                </div>
              </Row>
            </div>
          </div>
        </main>

        {/* ── Floating "popover/modal" sample ────────────────────── */}
        <div className="hidden xl:block fixed bottom-6 right-6 w-64 p-4" style={{ ...cardStyle, background: C.elevated }}>
          <div className="text-sm font-semibold mb-1">Popover surface</div>
          <p className="text-xs mb-3" style={{ color: C.fgMuted }}>Elevated panels, menus and modals use a lighter surface with the theme shadow.</p>
          <div className="flex gap-2">
            <button className="flex-1 text-xs font-medium py-1.5" style={{ background: C.accent, color: C.accentFg, borderRadius: C.radius }}>Confirm</button>
            <button className="flex-1 text-xs font-medium py-1.5" style={{ color: C.fgMuted, border: `1px solid ${C.border}`, borderRadius: C.radius }}>Cancel</button>
          </div>
        </div>
      </div>
    </div>
  );
}

function Row({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex flex-wrap items-center gap-3">
      <span className="text-xs w-16 shrink-0" style={{ color: C.fgSubtle }}>{label}</span>
      {children}
    </div>
  );
}

function Chip({ label, active }: { label: string; active?: boolean }) {
  return (
    <span
      className="text-xs px-2.5 py-1 font-medium"
      style={
        active
          ? { background: C.accentSoft, color: C.accent, borderRadius: '999px', border: `1px solid ${C.accent}` }
          : { color: C.fgMuted, borderRadius: '999px', border: `1px solid ${C.border}` }
      }
    >
      {label}
    </span>
  );
}

function Badge({ color, soft, children }: { color: string; soft: string; children: ReactNode }) {
  return (
    <span className="text-xs px-2 py-0.5 font-medium" style={{ color, background: soft, border: `1px solid ${color}`, borderRadius: '999px' }}>
      {children}
    </span>
  );
}

function Alert({ color, title, body }: { color: string; title: string; body: string }) {
  return (
    <div className="px-3.5 py-3 flex gap-3" style={{ background: C.cardAlt, border: `1px solid ${C.border}`, borderLeft: `3px solid ${color}`, borderRadius: C.radius }}>
      <div>
        <div className="text-sm font-medium" style={{ color }}>{title}</div>
        <div className="text-xs mt-0.5" style={{ color: C.fgMuted }}>{body}</div>
      </div>
    </div>
  );
}
