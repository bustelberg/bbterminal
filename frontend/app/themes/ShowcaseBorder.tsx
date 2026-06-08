import Link from 'next/link';
import type { CSSProperties, ReactNode } from 'react';
import type { Theme } from './themes';

/**
 * White-based theme preview where the gradient shows up ONLY as borders.
 * Surfaces, text, fills, accents and charts are all solid; card / button /
 * input / chip edges get a gradient via the padding-box/border-box
 * double-background trick (`gborder`). Same dashboard content as the other
 * showcases.
 */
const C = {
  bg: 'var(--t-bg)', card: 'var(--t-card)', cardAlt: 'var(--t-card-alt)', elevated: 'var(--t-elevated)',
  inset: 'var(--t-inset)', divider: 'var(--t-divider)',
  fg: 'var(--t-fg)', fgMuted: 'var(--t-fg-muted)', fgSubtle: 'var(--t-fg-subtle)',
  accent: 'var(--t-accent)', accentFg: 'var(--t-accent-fg)', accentSoft: 'var(--t-accent-soft)',
  pos: 'var(--t-pos)', neg: 'var(--t-neg)', warn: 'var(--t-warn)',
  grad: 'var(--t-grad)', radius: 'var(--t-radius)', shadow: 'var(--t-shadow)',
};

/** Gradient border: solid `fill` via padding-box, the gradient via border-box. */
function gborder(fill: string, radius: string = C.radius, width = 1): CSSProperties {
  return {
    border: `${width}px solid transparent`,
    borderRadius: radius,
    backgroundImage: `linear-gradient(${fill}, ${fill}), ${C.grad}`,
    backgroundOrigin: 'border-box',
    backgroundClip: 'padding-box, border-box',
    WebkitBackgroundClip: 'padding-box, border-box',
  };
}
const card = (): CSSProperties => ({ ...gborder(C.card), boxShadow: C.shadow });

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

export default function ShowcaseBorder({ theme }: { theme: Theme }) {
  const rootStyle = { ...theme.vars, fontFamily: theme.font, background: C.bg, color: C.fg } as CSSProperties;

  return (
    <div className="fixed inset-0 z-[100] overflow-auto" style={rootStyle}>
      <div className="flex min-h-full">
        {/* Sidebar */}
        <aside className="hidden md:flex w-56 shrink-0 flex-col gap-1 p-3" style={{ background: C.card, borderRight: `1px solid ${C.divider}` }}>
          <div className="flex items-center gap-2 px-2 py-3 mb-2">
            <span className="inline-block h-7 w-7" style={gborder(C.card, '9px')} />
            <span className="text-sm font-semibold tracking-tight">BBTerminal</span>
          </div>
          {['Dashboard', 'Backtest', 'Schedule', 'Companies', 'Universe', 'Earnings', 'FX Rates'].map((n, i) => (
            <div
              key={n}
              className="px-3 py-2 text-sm font-medium"
              style={i === 0 ? { ...gborder(C.card, '10px'), color: C.accent } : { color: C.fgMuted, borderRadius: '10px' }}
            >
              {n}
            </div>
          ))}
          <div className="mt-auto px-3 py-2 text-xs" style={{ color: C.fgSubtle }}>v4.8 · {theme.name}</div>
        </aside>

        {/* Main */}
        <main className="flex-1 min-w-0">
          {/* Top bar with a gradient bottom hairline */}
          <header className="sticky top-0 z-10 flex items-center gap-3 px-6 py-3" style={{ background: C.bg }}>
            <span className="absolute left-0 right-0 bottom-0 h-px" style={{ backgroundImage: C.grad }} />
            <Link href="/themes" className="text-xs px-2.5 py-1.5 font-medium" style={{ ...gborder(C.bg), color: C.fgMuted }}>← Themes</Link>
            <div className="flex-1 max-w-sm px-3 py-1.5 text-sm" style={{ background: C.inset, borderRadius: C.radius, color: C.fgSubtle }}>Search tickers, universes…</div>
            <button className="text-sm font-semibold px-3.5 py-1.5" style={{ ...gborder(C.card), color: C.accent }}>Run backtest</button>
            <span className="inline-flex h-8 w-8 items-center justify-center text-xs font-semibold" style={{ ...gborder(C.card, '999px'), color: C.accent }}>RB</span>
          </header>

          <div className="px-6 py-6 space-y-6 max-w-[1100px]">
            <div>
              <div className="text-xs uppercase tracking-[0.18em] mb-1.5" style={{ color: C.fgSubtle }}>{theme.name}</div>
              <h1 className="text-3xl font-semibold tracking-tight">Momentum Portfolio</h1>
              <p className="text-sm mt-2 max-w-2xl" style={{ color: C.fgMuted }}>{theme.tagline}</p>
            </div>

            {/* KPI cards — gradient borders, solid numbers */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              {[
                { label: 'Total value', val: '€1.84M', sub: 'since inception', tone: 'fg' },
                { label: 'MTD return', val: '−5.18%', sub: 'as of 2026-06-05', tone: 'neg' },
                { label: 'YTD return', val: '+49.6%', sub: 'vs ACWI +11.3%', tone: 'pos' },
                { label: 'Sharpe', val: '1.42', sub: '3y rolling', tone: 'fg' },
              ].map((k) => (
                <div key={k.label} className="p-4" style={card()}>
                  <div className="text-xs mb-1.5" style={{ color: C.fgSubtle }}>{k.label}</div>
                  <div className="text-2xl font-semibold tabular-nums" style={{ color: k.tone === 'pos' ? C.pos : k.tone === 'neg' ? C.neg : C.fg }}>{k.val}</div>
                  <div className="text-xs mt-1.5" style={{ color: C.fgSubtle }}>{k.sub}</div>
                </div>
              ))}
            </div>

            {/* Table + side panel */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              <div className="lg:col-span-2 overflow-hidden" style={card()}>
                <div className="flex items-center justify-between px-4 py-3" style={{ borderBottom: `1px solid ${C.divider}` }}>
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
                      <tr key={r.tk} style={{ borderTop: `1px solid ${C.divider}` }}>
                        <td className="px-4 py-2.5"><span className="font-semibold" style={{ color: C.accent }}>{r.tk}</span><span className="ml-1 text-xs" style={{ color: C.fgSubtle }}>·{r.ex}</span></td>
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
                <div className="p-4" style={card()}>
                  <div className="text-xs mb-2" style={{ color: C.fgSubtle }}>Filters</div>
                  <div className="flex flex-wrap gap-2">
                    <Chip label="Long-only" active /><Chip label="By sector" active />
                    <Chip label="Min $30" /><Chip label="ACWI" />
                  </div>
                </div>
                <div className="p-4" style={card()}>
                  <div className="flex items-center justify-between mb-3">
                    <div className="text-xs" style={{ color: C.fgSubtle }}>Monthly returns</div>
                    <div className="text-xs tabular-nums" style={{ color: C.pos }}>+18.2% yr</div>
                  </div>
                  <div className="flex items-end gap-1 h-24">
                    {MONTHLY.map((m, i) => (
                      <div key={i} className="flex-1 flex flex-col justify-end h-full">
                        <div style={{ height: `${Math.max(6, Math.abs(m) / 6.1 * 100)}%`, background: m >= 0 ? C.accent : C.neg, borderRadius: '3px', opacity: m >= 0 ? 0.9 : 0.85 }} />
                      </div>
                    ))}
                  </div>
                </div>
                <div className="p-4" style={card()}>
                  <div className="text-xs mb-2" style={{ color: C.fgSubtle }}>Equity curve</div>
                  <svg viewBox="0 0 200 60" className="w-full h-16" preserveAspectRatio="none">
                    <polyline fill="none" stroke={C.accent} strokeWidth="2" strokeLinecap="round" points="0,52 18,46 36,48 54,38 72,40 90,30 108,33 126,22 144,26 162,14 180,18 200,6" />
                  </svg>
                </div>
              </div>
            </div>

            {/* Component gallery */}
            <div className="p-5 space-y-5" style={card()}>
              <h2 className="text-sm font-semibold">Components</h2>

              <Row label="Buttons">
                <button className="text-sm font-semibold px-4 py-1.5" style={{ ...gborder(C.card), color: C.accent }}>Gradient border</button>
                <button className="text-sm font-semibold px-4 py-1.5" style={{ background: C.accent, color: C.accentFg, borderRadius: C.radius }}>Solid accent</button>
                <button className="text-sm font-medium px-4 py-1.5" style={{ color: C.fgMuted, borderRadius: C.radius }}>Ghost</button>
                <button className="text-sm font-medium px-4 py-1.5" style={{ ...gborder(C.card), color: C.neg }}>Danger</button>
                <button className="text-sm font-medium px-4 py-1.5 opacity-40" style={{ ...gborder(C.card), color: C.fgMuted }}>Disabled</button>
              </Row>

              <Row label="Badges">
                <span className="text-xs px-2.5 py-0.5 font-medium" style={{ ...gborder(C.card, '999px'), color: C.accent }}>Active</span>
                <Pill color={C.pos}>Gain</Pill><Pill color={C.neg}>Loss</Pill><Pill color={C.warn}>Stale</Pill>
                <Pill color={C.fgMuted}>Neutral</Pill>
              </Row>

              <Row label="Inputs">
                <div className="px-3 py-1.5 text-sm w-44" style={{ background: C.card, border: `1px solid ${C.divider}`, borderRadius: C.radius, color: C.fg }}>Text input</div>
                <div className="px-3 py-1.5 text-sm w-40 flex items-center justify-between" style={{ background: C.card, border: `1px solid ${C.divider}`, borderRadius: C.radius, color: C.fgMuted }}>Select<span style={{ color: C.fgSubtle }}>▾</span></div>
                {/* focused = gradient border */}
                <span className="px-3 py-1.5 text-sm w-44 inline-block" style={{ ...gborder(C.card), color: C.fg }}>Focused (gradient)</span>
                <span className="inline-flex items-center h-6 w-11 p-0.5" style={{ background: C.accent, borderRadius: '999px' }}><span className="h-5 w-5 ml-auto bg-white" style={{ borderRadius: '999px' }} /></span>
              </Row>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <Alert color={C.pos} title="Rebalance complete" body="24 holdings priced through 2026-06-05." />
                <Alert color={C.neg} title="Pipeline error" body="3 of 401 companies failed to refresh." />
                <Alert color={C.warn} title="Data stale" body="ACWI universe is 8 days behind." />
                <Alert color={C.accent} title="Next rebalance" body="Scheduled for Jul 06, 2026 · 04:00." />
              </div>

              <Row label="Type">
                <div className="space-y-1">
                  <div className="text-2xl font-semibold">Display 24</div>
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

        <div className="hidden xl:block fixed bottom-6 right-6 w-64 p-4" style={card()}>
          <div className="text-sm font-semibold mb-1">Popover surface</div>
          <p className="text-xs mb-3" style={{ color: C.fgMuted }}>Menus and modals are white cards with the same gradient edge + soft shadow.</p>
          <div className="flex gap-2">
            <button className="flex-1 text-xs font-semibold py-1.5" style={{ background: C.accent, color: C.accentFg, borderRadius: C.radius }}>Confirm</button>
            <button className="flex-1 text-xs font-medium py-1.5" style={{ ...gborder(C.card), color: C.fgMuted }}>Cancel</button>
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
      style={active ? { ...gborder(C.card, '999px'), color: C.accent } : { color: C.fgMuted, borderRadius: '999px', border: `1px solid ${C.divider}` }}
    >
      {label}
    </span>
  );
}

function Pill({ color, children }: { color: string; children: ReactNode }) {
  return <span className="text-xs px-2 py-0.5 font-medium" style={{ color, border: `1px solid ${color}`, borderRadius: '999px' }}>{children}</span>;
}

function Alert({ color, title, body }: { color: string; title: string; body: string }) {
  return (
    <div className="px-3.5 py-3 flex gap-2.5 items-start" style={card()}>
      <span className="mt-1 h-2 w-2 shrink-0 rounded-full" style={{ background: color }} />
      <div>
        <div className="text-sm font-medium" style={{ color }}>{title}</div>
        <div className="text-xs mt-0.5" style={{ color: C.fgMuted }}>{body}</div>
      </div>
    </div>
  );
}
