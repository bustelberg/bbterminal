import Link from 'next/link';
import type { CSSProperties, ReactNode } from 'react';
import type { LuxTheme } from './lux-themes';

/**
 * Glass/gradient theme preview. Same dashboard content as `Showcase`, but
 * styled for the web3/luxury direction: frosted-glass surfaces (backdrop
 * blur), hairline borders, gradient accents/text/charts and soft glow — all
 * driven by the theme's `--t-*` vars. `fixed inset-0` for an edge-to-edge look.
 */

const C = {
  bg: 'var(--t-bg)', mesh: 'var(--t-mesh)', glass: 'var(--t-glass)', glassStrong: 'var(--t-glass-strong)',
  inset: 'var(--t-inset)', hair: 'var(--t-hairline)', hairStrong: 'var(--t-hairline-strong)',
  fg: 'var(--t-fg)', fgMuted: 'var(--t-fg-muted)', fgSubtle: 'var(--t-fg-subtle)',
  accent: 'var(--t-accent)', accentFg: 'var(--t-accent-fg)', grad: 'var(--t-grad)', gradSoft: 'var(--t-grad-soft)',
  pos: 'var(--t-pos)', neg: 'var(--t-neg)', warn: 'var(--t-warn)', radius: 'var(--t-radius)', glow: 'var(--t-glow)',
};

const glass: CSSProperties = {
  background: C.glass,
  border: `1px solid ${C.hair}`,
  borderRadius: C.radius,
  boxShadow: C.glow,
  backdropFilter: 'blur(18px)',
  WebkitBackdropFilter: 'blur(18px)',
};
const gradText: CSSProperties = {
  backgroundImage: C.grad,
  WebkitBackgroundClip: 'text',
  backgroundClip: 'text',
  color: 'transparent',
};
const gradBtn: CSSProperties = { backgroundImage: C.grad, color: C.accentFg, borderRadius: C.radius };

const HOLDINGS = [
  { tk: 'KIOXIA', ex: 'TSE', sec: 'Technology', px: '4,452 JPY', mtd: 3.18, ytd: 41.2, w: 4.4 },
  { tk: 'BE', ex: 'NYSE', sec: 'Utilities', px: '28.40 USD', mtd: -5.62, ytd: 12.7, w: 4.2 },
  { tk: 'SOI', ex: 'XPAR', sec: 'Technology', px: '146.2 EUR', mtd: 1.05, ytd: -8.4, w: 4.1 },
  { tk: 'VSH', ex: 'NYSE', sec: 'Industrials', px: '19.86 USD', mtd: -2.10, ytd: 22.9, w: 3.9 },
  { tk: 'NEL', ex: 'OSL', sec: 'Materials', px: '2.98 NOK', mtd: 7.44, ytd: -31.5, w: 3.7 },
  { tk: 'UMI', ex: 'XBRU', sec: 'Materials', px: '23.42 EUR', mtd: 0.42, ytd: 5.1, w: 3.6 },
];
const MONTHLY = [2.1, -1.4, 3.8, 5.2, -2.6, 1.1, 4.4, -0.8, 2.9, -5.2, 6.1, 3.3];
const pct = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;

export default function ShowcaseLux({ theme }: { theme: LuxTheme }) {
  const rootStyle = {
    ...theme.vars,
    fontFamily: theme.font,
    background: C.bg,
    backgroundImage: C.mesh,
    backgroundAttachment: 'fixed',
    color: C.fg,
  } as CSSProperties;

  return (
    <div className="fixed inset-0 z-[100] overflow-auto" style={rootStyle}>
      <div className="flex min-h-full">
        {/* ── Glass sidebar ──────────────────────────────────────── */}
        <aside
          className="hidden md:flex w-56 shrink-0 flex-col gap-1 p-3 m-3 mr-0 h-[calc(100vh-1.5rem)] sticky top-3"
          style={glass}
        >
          <div className="flex items-center gap-2 px-2 py-3 mb-2">
            <span className="inline-block h-7 w-7" style={{ backgroundImage: C.grad, borderRadius: '10px', boxShadow: C.glow }} />
            <span className="text-sm font-semibold tracking-tight">BBTerminal</span>
          </div>
          {['Dashboard', 'Backtest', 'Schedule', 'Companies', 'Universe', 'Earnings', 'FX Rates'].map((n, i) => (
            <div
              key={n}
              className="px-3 py-2 text-sm font-medium"
              style={
                i === 0
                  ? { backgroundImage: C.gradSoft, color: C.fg, borderRadius: '12px', border: `1px solid ${C.hair}` }
                  : { color: C.fgMuted, borderRadius: '12px' }
              }
            >
              {n}
            </div>
          ))}
          <div className="mt-auto px-3 py-2 text-xs" style={{ color: C.fgSubtle }}>v4.8 · {theme.name}</div>
        </aside>

        {/* ── Main ───────────────────────────────────────────────── */}
        <main className="flex-1 min-w-0">
          <header className="sticky top-0 z-10 flex items-center gap-3 px-6 py-3" style={{ borderBottom: `1px solid ${C.hair}` }}>
            <Link href="/themes" className="text-xs px-2.5 py-1.5 font-medium" style={{ color: C.fgMuted, border: `1px solid ${C.hair}`, borderRadius: C.radius }}>
              ← Themes
            </Link>
            <div className="flex-1 max-w-sm px-3 py-1.5 text-sm" style={{ background: C.inset, border: `1px solid ${C.hair}`, borderRadius: C.radius, color: C.fgSubtle }}>
              Search tickers, universes…
            </div>
            <button className="text-sm font-semibold px-3.5 py-1.5" style={{ ...gradBtn, boxShadow: C.glow }}>Run backtest</button>
            <span className="inline-flex h-8 w-8 items-center justify-center text-xs font-semibold" style={{ backgroundImage: C.grad, color: C.accentFg, borderRadius: '999px' }}>RB</span>
          </header>

          <div className="px-6 py-6 space-y-6 max-w-[1100px]">
            {/* Header with gradient display type */}
            <div>
              <div className="text-xs uppercase tracking-[0.2em] mb-2" style={{ color: C.fgSubtle }}>{theme.name}</div>
              <h1 className="text-3xl font-semibold tracking-tight" style={gradText}>Momentum Portfolio</h1>
              <p className="text-sm mt-2 max-w-2xl" style={{ color: C.fgMuted }}>{theme.tagline}</p>
            </div>

            {/* KPI glass cards */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              {[
                { label: 'Total value', val: '€1.84M', sub: 'since inception', grad: true },
                { label: 'MTD return', val: '−5.18%', sub: 'as of 2026-06-05', tone: 'neg' },
                { label: 'YTD return', val: '+49.6%', sub: 'vs ACWI +11.3%', tone: 'pos' },
                { label: 'Sharpe', val: '1.42', sub: '3y rolling' },
              ].map((k) => (
                <div key={k.label} className="p-4" style={glass}>
                  <div className="text-xs mb-1.5" style={{ color: C.fgSubtle }}>{k.label}</div>
                  <div
                    className="text-2xl font-semibold tabular-nums"
                    style={k.grad ? gradText : { color: k.tone === 'pos' ? C.pos : k.tone === 'neg' ? C.neg : C.fg }}
                  >
                    {k.val}
                  </div>
                  <div className="text-xs mt-1.5" style={{ color: C.fgSubtle }}>{k.sub}</div>
                </div>
              ))}
            </div>

            {/* Table + side panel */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              <div className="lg:col-span-2 overflow-hidden" style={glass}>
                <div className="flex items-center justify-between px-4 py-3" style={{ borderBottom: `1px solid ${C.hair}` }}>
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
                    {HOLDINGS.map((r) => (
                      <tr key={r.tk} style={{ borderTop: `1px solid ${C.hair}` }}>
                        <td className="px-4 py-2.5">
                          <span className="font-semibold" style={gradText}>{r.tk}</span>
                          <span className="ml-1 text-xs" style={{ color: C.fgSubtle }}>·{r.ex}</span>
                        </td>
                        <td className="px-4 py-2.5" style={{ color: C.fgMuted }}>{r.sec}</td>
                        <td className="px-4 py-2.5 text-right tabular-nums">{r.px}</td>
                        <td className="px-4 py-2.5 text-right tabular-nums" style={{ color: r.mtd >= 0 ? C.pos : C.neg }}>{pct(r.mtd)}</td>
                        <td className="px-4 py-2.5 text-right tabular-nums" style={{ color: r.ytd >= 0 ? C.pos : C.neg }}>{pct(r.ytd)}</td>
                        <td className="px-4 py-2.5 text-right tabular-nums" style={{ color: C.fgMuted }}>{r.w.toFixed(1)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="space-y-4">
                <div className="p-4" style={glass}>
                  <div className="text-xs mb-2" style={{ color: C.fgSubtle }}>Filters</div>
                  <div className="flex flex-wrap gap-2">
                    <Chip label="Long-only" active /><Chip label="By sector" active />
                    <Chip label="Min $30" /><Chip label="ACWI" />
                  </div>
                </div>

                <div className="p-4" style={glass}>
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
                            backgroundImage: m >= 0 ? C.grad : 'none',
                            background: m >= 0 ? undefined : C.neg,
                            borderRadius: '4px',
                          }}
                        />
                      </div>
                    ))}
                  </div>
                </div>

                <div className="p-4" style={glass}>
                  <div className="text-xs mb-2" style={{ color: C.fgSubtle }}>Equity curve</div>
                  <svg viewBox="0 0 200 64" className="w-full h-16" preserveAspectRatio="none">
                    <defs>
                      <linearGradient id={`ln-${theme.slug}`} x1="0" y1="0" x2="1" y2="0">
                        <stop offset="0%" stopColor={C.accent} />
                        <stop offset="100%" stopColor={C.pos} />
                      </linearGradient>
                      <linearGradient id={`ar-${theme.slug}`} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor={C.accent} stopOpacity="0.35" />
                        <stop offset="100%" stopColor={C.accent} stopOpacity="0" />
                      </linearGradient>
                    </defs>
                    <path d="M0,56 18,50 36,52 54,40 72,42 90,32 108,35 126,22 144,26 162,14 180,18 200,6 L200,64 L0,64 Z" fill={`url(#ar-${theme.slug})`} />
                    <polyline fill="none" stroke={`url(#ln-${theme.slug})`} strokeWidth="2.5" strokeLinecap="round" points="0,56 18,50 36,52 54,40 72,42 90,32 108,35 126,22 144,26 162,14 180,18 200,6" />
                  </svg>
                </div>
              </div>
            </div>

            {/* Component gallery */}
            <div className="p-5 space-y-5" style={glass}>
              <h2 className="text-sm font-semibold">Components</h2>

              <Row label="Buttons">
                <button className="text-sm font-semibold px-4 py-1.5" style={{ ...gradBtn, boxShadow: C.glow }}>Primary</button>
                <button className="text-sm font-medium px-4 py-1.5" style={{ background: C.glassStrong, color: C.fg, border: `1px solid ${C.hairStrong}`, borderRadius: C.radius }}>Secondary</button>
                <button className="text-sm font-medium px-4 py-1.5" style={{ color: C.fgMuted, borderRadius: C.radius }}>Ghost</button>
                {/* gradient-outline button */}
                <span className="inline-block p-px" style={{ backgroundImage: C.grad, borderRadius: C.radius }}>
                  <span className="block text-sm font-medium px-4 py-1.5" style={{ background: C.bg, color: C.fg, borderRadius: `calc(${C.radius} - 1px)` }}>Gradient outline</span>
                </span>
                <button className="text-sm font-medium px-4 py-1.5" style={{ color: C.neg, border: `1px solid ${C.neg}`, borderRadius: C.radius }}>Danger</button>
              </Row>

              <Row label="Badges">
                <span className="text-xs px-2.5 py-0.5 font-medium" style={{ backgroundImage: C.grad, color: C.accentFg, borderRadius: '999px' }}>Active</span>
                <Pill color={C.pos}>Gain</Pill><Pill color={C.neg}>Loss</Pill><Pill color={C.warn}>Stale</Pill>
                <Pill color={C.fgMuted}>Neutral</Pill>
              </Row>

              <Row label="Inputs">
                <div className="px-3 py-1.5 text-sm w-44" style={{ background: C.inset, border: `1px solid ${C.hair}`, borderRadius: C.radius, color: C.fg }}>Text input</div>
                <div className="px-3 py-1.5 text-sm w-40 flex items-center justify-between" style={{ background: C.inset, border: `1px solid ${C.hair}`, borderRadius: C.radius, color: C.fgMuted }}>
                  Select<span style={{ color: C.fgSubtle }}>▾</span>
                </div>
                <span className="inline-flex items-center h-6 w-11 p-0.5" style={{ backgroundImage: C.grad, borderRadius: '999px' }}>
                  <span className="h-5 w-5 ml-auto bg-white" style={{ borderRadius: '999px' }} />
                </span>
                <span className="px-3 py-1.5 text-sm" style={{ background: C.inset, border: `1px solid transparent`, borderRadius: C.radius, color: C.fg, boxShadow: `0 0 0 1.5px ${C.accent}, 0 0 16px ${C.accent}` }}>Focused</span>
              </Row>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <Alert color={C.pos} title="Rebalance complete" body="24 holdings priced through 2026-06-05." />
                <Alert color={C.neg} title="Pipeline error" body="3 of 401 companies failed to refresh." />
                <Alert color={C.warn} title="Data stale" body="ACWI universe is 8 days behind." />
                <Alert grad title="Next rebalance" body="Scheduled for Jul 06, 2026 · 04:00." />
              </div>

              <Row label="Type">
                <div className="space-y-1">
                  <div className="text-2xl font-semibold" style={gradText}>Gradient display 24</div>
                  <div className="text-base font-medium">Heading 16</div>
                  <div className="text-sm">Body 14 — the quick brown fox.</div>
                  <div className="text-xs" style={{ color: C.fgMuted }}>Muted 12 · secondary copy</div>
                  <div className="text-xs" style={{ color: C.fgSubtle }}>Subtle 12 · captions</div>
                  <div className="text-sm tabular-nums tracking-tight">1,234,567.89 · +49.61% · −5.18%</div>
                </div>
              </Row>
            </div>
          </div>
        </main>

        <div className="hidden xl:block fixed bottom-6 right-6 w-64 p-4" style={{ ...glass, background: C.glassStrong }}>
          <div className="text-sm font-semibold mb-1" style={gradText}>Popover surface</div>
          <p className="text-xs mb-3" style={{ color: C.fgMuted }}>Menus and modals use a stronger frosted glass with the same hairline + glow.</p>
          <div className="flex gap-2">
            <button className="flex-1 text-xs font-semibold py-1.5" style={gradBtn}>Confirm</button>
            <button className="flex-1 text-xs font-medium py-1.5" style={{ color: C.fgMuted, border: `1px solid ${C.hair}`, borderRadius: C.radius }}>Cancel</button>
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
          ? { backgroundImage: C.gradSoft, color: C.fg, borderRadius: '999px', border: `1px solid ${C.hairStrong}` }
          : { color: C.fgMuted, borderRadius: '999px', border: `1px solid ${C.hair}` }
      }
    >
      {label}
    </span>
  );
}

function Pill({ color, children }: { color: string; children: ReactNode }) {
  return (
    <span className="text-xs px-2 py-0.5 font-medium" style={{ color, border: `1px solid ${color}`, borderRadius: '999px' }}>
      {children}
    </span>
  );
}

function Alert({ color, grad, title, body }: { color?: string; grad?: boolean; title: string; body: string }) {
  return (
    <div className="px-3.5 py-3 flex gap-3 overflow-hidden relative" style={{ background: C.inset, border: `1px solid ${C.hair}`, borderRadius: C.radius }}>
      <span className="absolute left-0 top-0 bottom-0 w-[3px]" style={grad ? { backgroundImage: C.grad } : { background: color }} />
      <div className="pl-1.5">
        <div className="text-sm font-medium" style={grad ? gradText : { color }}>{title}</div>
        <div className="text-xs mt-0.5" style={{ color: C.fgMuted }}>{body}</div>
      </div>
    </div>
  );
}
