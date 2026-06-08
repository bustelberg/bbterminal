import Link from 'next/link';
import type { CSSProperties, ReactNode } from 'react';
import type { PrestigeTheme } from './prestige-themes';

/**
 * Flagship "$100k" preview. Same dashboard content as the other showcases,
 * dialled up with: slowly-rotating conic-gradient borders (`@property --pa`),
 * a film-grain + drifting-glow background, gradient display type and a
 * sheen-swept primary button. All CSS — no client JS.
 */
const C = {
  bg: 'var(--t-bg)', card: 'var(--t-card)', card2: 'var(--t-card-2)', elevated: 'var(--t-elevated)',
  inset: 'var(--t-inset)', divider: 'var(--t-divider)',
  fg: 'var(--t-fg)', fgMuted: 'var(--t-fg-muted)', fgSubtle: 'var(--t-fg-subtle)',
  accent: 'var(--t-accent)', accentFg: 'var(--t-accent-fg)', gold: 'var(--t-gold)',
  pos: 'var(--t-pos)', neg: 'var(--t-neg)', warn: 'var(--t-warn)',
  grad: 'var(--t-grad)', radius: 'var(--t-radius)',
};

/** Static gradient border (padding-box / border-box) for the larger cards. */
function sborder(fill: string, radius: string = C.radius): CSSProperties {
  return {
    border: '1px solid transparent',
    borderRadius: radius,
    backgroundImage: `linear-gradient(${fill}, ${fill}), ${C.grad}`,
    backgroundOrigin: 'border-box',
    backgroundClip: 'padding-box, border-box',
    WebkitBackgroundClip: 'padding-box, border-box',
  };
}

const GRAIN =
  "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='140' height='140'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='2' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E\")";

const CSS = `
@property --pa { syntax: '<angle>'; inherits: false; initial-value: 0deg; }
@keyframes pa-spin { to { --pa: 360deg; } }
@keyframes pa-float { 0%,100% { transform: translate3d(0,0,0); } 50% { transform: translate3d(0,-26px,0); } }
@keyframes pa-sheen { 0% { transform: translateX(-140%) skewX(-18deg); } 55%,100% { transform: translateX(360%) skewX(-18deg); } }
@keyframes pa-rise { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: none; } }

.pcard { position: relative; isolation: isolate; animation: pa-rise .5s ease both; }
.pcard::before {
  content: ''; position: absolute; inset: 0; border-radius: inherit; padding: 1px;
  background: conic-gradient(from var(--pa), var(--t-c3), var(--t-c1), var(--t-c2), var(--t-c1), var(--t-c3));
  -webkit-mask: linear-gradient(#000 0 0) content-box, linear-gradient(#000 0 0);
  -webkit-mask-composite: xor; mask-composite: exclude;
  animation: pa-spin 9s linear infinite; pointer-events: none;
}
.pcard::after {
  content: ''; position: absolute; inset: 0; border-radius: inherit; pointer-events: none;
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.06);
}
.psheen { position: relative; overflow: hidden; }
.psheen > .sheen {
  position: absolute; top: 0; bottom: 0; width: 30%; pointer-events: none;
  background: linear-gradient(100deg, transparent, rgba(255,255,255,0.45), transparent);
  transform: translateX(-140%) skewX(-18deg); animation: pa-sheen 5s ease-in-out infinite;
}
.pglow { animation: pa-float 14s ease-in-out infinite; }
`;

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
const gradText: CSSProperties = { backgroundImage: C.grad, WebkitBackgroundClip: 'text', backgroundClip: 'text', color: 'transparent' };

export default function ShowcasePrestige({ theme }: { theme: PrestigeTheme }) {
  const rootStyle = {
    ...theme.vars,
    fontFamily: theme.font,
    background: C.bg,
    backgroundImage: `radial-gradient(60% 50% at 18% -8%, var(--t-glow1), transparent 70%), radial-gradient(45% 45% at 100% 0%, var(--t-glow2), transparent 70%), radial-gradient(120% 90% at 50% 120%, rgba(0,0,0,0.5), transparent 60%)`,
    backgroundAttachment: 'fixed',
    color: C.fg,
  } as CSSProperties;

  return (
    <div className="fixed inset-0 z-[100] overflow-auto" style={rootStyle}>
      <style>{CSS}</style>
      {/* drifting glow blobs */}
      <div className="pointer-events-none fixed -left-32 top-10 h-96 w-96 rounded-full pglow" style={{ background: 'radial-gradient(circle, var(--t-glow1), transparent 65%)', filter: 'blur(20px)' }} />
      <div className="pointer-events-none fixed right-0 bottom-0 h-[28rem] w-[28rem] rounded-full pglow" style={{ background: 'radial-gradient(circle, var(--t-glow1), transparent 65%)', filter: 'blur(24px)', animationDelay: '-6s' }} />
      {/* film grain */}
      <div className="pointer-events-none fixed inset-0" style={{ backgroundImage: GRAIN, opacity: 0.05, mixBlendMode: 'soft-light' }} />

      <div className="relative flex min-h-full">
        {/* Sidebar */}
        <aside className="hidden md:flex w-56 shrink-0 flex-col gap-1 p-3" style={{ background: C.card, borderRight: `1px solid ${C.divider}` }}>
          <div className="flex items-center gap-2.5 px-2 py-3 mb-3">
            <span className="inline-block h-7 w-7" style={{ backgroundImage: C.grad, borderRadius: '9px' }} />
            <span className="text-sm font-semibold tracking-tight">BBTerminal</span>
            <span className="ml-auto text-[9px] uppercase tracking-[0.2em]" style={{ color: C.gold }}>Pro</span>
          </div>
          {['Dashboard', 'Backtest', 'Schedule', 'Companies', 'Universe', 'Earnings', 'FX Rates'].map((n, i) => (
            <div key={n} className="px-3 py-2 text-sm font-medium" style={i === 0 ? { ...sborder(C.card, '11px'), color: C.fg } : { color: C.fgMuted, borderRadius: '11px' }}>{n}</div>
          ))}
          <div className="mt-auto px-3 py-2 text-xs" style={{ color: C.fgSubtle }}>v4.8 · {theme.name}</div>
        </aside>

        {/* Main */}
        <main className="flex-1 min-w-0">
          <header className="sticky top-0 z-10 flex items-center gap-3 px-6 py-3" style={{ background: 'color-mix(in srgb, var(--t-bg) 72%, transparent)', backdropFilter: 'blur(10px)', WebkitBackdropFilter: 'blur(10px)', borderBottom: `1px solid ${C.divider}` }}>
            <Link href="/themes" className="text-xs px-2.5 py-1.5 font-medium" style={{ ...sborder(C.card), color: C.fgMuted }}>← Themes</Link>
            <div className="flex-1 max-w-sm px-3 py-1.5 text-sm" style={{ background: C.inset, border: `1px solid ${C.divider}`, borderRadius: C.radius, color: C.fgSubtle }}>Search tickers, universes…</div>
            <button className="psheen text-sm font-semibold px-4 py-1.5" style={{ backgroundImage: C.grad, color: C.accentFg, borderRadius: C.radius }}>
              <span className="sheen" />Run backtest
            </button>
            <span className="inline-flex h-8 w-8 items-center justify-center text-xs font-semibold pcard" style={{ background: C.card, borderRadius: '999px', color: C.accent }}>RB</span>
          </header>

          <div className="px-6 py-7 space-y-6 max-w-[1100px]">
            {/* Hero card — animated conic edge + gradient display type */}
            <div className="pcard p-7" style={{ background: C.card, borderRadius: C.radius }}>
              <div className="text-[11px] uppercase tracking-[0.32em] mb-3" style={{ color: C.gold }}>Private wealth · momentum</div>
              <h1 className="text-4xl font-semibold tracking-tight leading-[1.05]" style={gradText}>Momentum Portfolio</h1>
              <p className="text-sm mt-3 max-w-2xl leading-relaxed" style={{ color: C.fgMuted }}>{theme.tagline}</p>
              <div className="flex flex-wrap gap-2.5 mt-5">
                <button className="psheen text-sm font-semibold px-5 py-2" style={{ backgroundImage: C.grad, color: C.accentFg, borderRadius: C.radius }}><span className="sheen" />View report</button>
                <button className="text-sm font-medium px-5 py-2" style={{ ...sborder(C.card), color: C.fg }}>Rebalance</button>
              </div>
            </div>

            {/* KPI cards — animated conic edge */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              {[
                { label: 'Total value', val: '€1.84M', sub: 'since inception', grad: true },
                { label: 'MTD return', val: '−5.18%', sub: 'as of 2026-06-05', tone: 'neg' },
                { label: 'YTD return', val: '+49.6%', sub: 'vs ACWI +11.3%', tone: 'pos' },
                { label: 'Sharpe', val: '1.42', sub: '3y rolling' },
              ].map((k) => (
                <div key={k.label} className="pcard p-4" style={{ background: C.card, borderRadius: C.radius }}>
                  <div className="text-xs mb-1.5" style={{ color: C.fgSubtle }}>{k.label}</div>
                  <div className="text-2xl font-semibold tabular-nums" style={k.grad ? gradText : { color: k.tone === 'pos' ? C.pos : k.tone === 'neg' ? C.neg : C.fg }}>{k.val}</div>
                  <div className="text-xs mt-1.5" style={{ color: C.fgSubtle }}>{k.sub}</div>
                </div>
              ))}
            </div>

            {/* Table + side panel — static gradient edges */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              <div className="lg:col-span-2 overflow-hidden" style={sborder(C.card)}>
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
                    {HOLDINGS.map((r, i) => (
                      <tr key={r.tk} style={{ borderTop: `1px solid ${C.divider}`, background: i % 2 ? C.card2 : 'transparent' }}>
                        <td className="px-4 py-2.5"><span className="font-semibold" style={gradText}>{r.tk}</span><span className="ml-1 text-xs" style={{ color: C.fgSubtle }}>·{r.ex}</span></td>
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
                <div className="p-4" style={sborder(C.card)}>
                  <div className="text-xs mb-2" style={{ color: C.fgSubtle }}>Filters</div>
                  <div className="flex flex-wrap gap-2">
                    <Chip label="Long-only" active /><Chip label="By sector" active /><Chip label="Min $30" /><Chip label="ACWI" />
                  </div>
                </div>
                <div className="p-4" style={sborder(C.card)}>
                  <div className="flex items-center justify-between mb-3">
                    <div className="text-xs" style={{ color: C.fgSubtle }}>Monthly returns</div>
                    <div className="text-xs tabular-nums" style={{ color: C.pos }}>+18.2% yr</div>
                  </div>
                  <div className="flex items-end gap-1 h-24">
                    {MONTHLY.map((m, i) => (
                      <div key={i} className="flex-1 flex flex-col justify-end h-full">
                        <div style={{ height: `${Math.max(6, Math.abs(m) / 6.1 * 100)}%`, backgroundImage: m >= 0 ? C.grad : 'none', background: m >= 0 ? undefined : C.neg, borderRadius: '4px' }} />
                      </div>
                    ))}
                  </div>
                </div>
                <div className="p-4" style={sborder(C.card)}>
                  <div className="text-xs mb-2" style={{ color: C.fgSubtle }}>Equity curve</div>
                  <svg viewBox="0 0 200 64" className="w-full h-16" preserveAspectRatio="none">
                    <defs>
                      <linearGradient id={`pln-${theme.slug}`} x1="0" y1="0" x2="1" y2="0"><stop offset="0%" stopColor="var(--t-c1)" /><stop offset="100%" stopColor="var(--t-c2)" /></linearGradient>
                      <linearGradient id={`par-${theme.slug}`} x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="var(--t-c1)" stopOpacity="0.4" /><stop offset="100%" stopColor="var(--t-c1)" stopOpacity="0" /></linearGradient>
                    </defs>
                    <path d="M0,56 18,50 36,52 54,40 72,42 90,32 108,35 126,22 144,26 162,14 180,18 200,6 L200,64 L0,64 Z" fill={`url(#par-${theme.slug})`} />
                    <polyline fill="none" stroke={`url(#pln-${theme.slug})`} strokeWidth="2.5" strokeLinecap="round" points="0,56 18,50 36,52 54,40 72,42 90,32 108,35 126,22 144,26 162,14 180,18 200,6" />
                  </svg>
                </div>
              </div>
            </div>

            {/* Gallery */}
            <div className="p-5 space-y-5" style={sborder(C.card)}>
              <h2 className="text-sm font-semibold">Components</h2>
              <Row label="Buttons">
                <button className="psheen text-sm font-semibold px-4 py-1.5" style={{ backgroundImage: C.grad, color: C.accentFg, borderRadius: C.radius }}><span className="sheen" />Primary</button>
                <button className="text-sm font-medium px-4 py-1.5" style={{ ...sborder(C.card), color: C.fg }}>Gradient border</button>
                <button className="text-sm font-medium px-4 py-1.5" style={{ color: C.fgMuted, borderRadius: C.radius }}>Ghost</button>
                <button className="text-sm font-medium px-4 py-1.5" style={{ ...sborder(C.card), color: C.neg }}>Danger</button>
              </Row>
              <Row label="Badges">
                <span className="text-xs px-2.5 py-0.5 font-medium" style={{ backgroundImage: C.grad, color: C.accentFg, borderRadius: '999px' }}>Active</span>
                <span className="text-xs px-2.5 py-0.5 font-medium" style={{ color: C.gold, border: `1px solid ${C.gold}`, borderRadius: '999px' }}>Premium</span>
                <Pill color={C.pos}>Gain</Pill><Pill color={C.neg}>Loss</Pill><Pill color={C.warn}>Stale</Pill>
              </Row>
              <Row label="Inputs">
                <div className="px-3 py-1.5 text-sm w-44" style={{ background: C.inset, border: `1px solid ${C.divider}`, borderRadius: C.radius, color: C.fg }}>Text input</div>
                <span className="px-3 py-1.5 text-sm inline-block" style={{ ...sborder(C.card), color: C.fg }}>Focused (gradient)</span>
                <span className="inline-flex items-center h-6 w-11 p-0.5" style={{ backgroundImage: C.grad, borderRadius: '999px' }}><span className="h-5 w-5 ml-auto bg-white" style={{ borderRadius: '999px' }} /></span>
              </Row>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <Alert color={C.pos} title="Rebalance complete" body="24 holdings priced through 2026-06-05." />
                <Alert color={C.neg} title="Pipeline error" body="3 of 401 companies failed to refresh." />
                <Alert color={C.warn} title="Data stale" body="ACWI universe is 8 days behind." />
                <Alert color={C.accent} title="Next rebalance" body="Scheduled for Jul 06, 2026 · 04:00." />
              </div>
              <Row label="Type">
                <div className="space-y-1">
                  <div className="text-2xl font-semibold" style={gradText}>Gradient display 24</div>
                  <div className="text-base font-medium">Heading 16</div>
                  <div className="text-sm">Body 14 — the quick brown fox.</div>
                  <div className="text-xs" style={{ color: C.fgMuted }}>Muted 12 · secondary copy</div>
                  <div className="text-sm tabular-nums tracking-tight">1,234,567.89 · +49.61% · −5.18%</div>
                </div>
              </Row>
            </div>
          </div>
        </main>

        <div className="pcard hidden xl:block fixed bottom-6 right-6 w-64 p-4" style={{ background: C.elevated, borderRadius: C.radius }}>
          <div className="text-sm font-semibold mb-1" style={gradText}>Popover surface</div>
          <p className="text-xs mb-3" style={{ color: C.fgMuted }}>Menus and modals carry the same rotating gradient edge, grain and inner highlight.</p>
          <div className="flex gap-2">
            <button className="psheen flex-1 text-xs font-semibold py-1.5" style={{ backgroundImage: C.grad, color: C.accentFg, borderRadius: C.radius }}><span className="sheen" />Confirm</button>
            <button className="flex-1 text-xs font-medium py-1.5" style={{ ...sborder(C.elevated), color: C.fgMuted }}>Cancel</button>
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
  return <span className="text-xs px-2.5 py-1 font-medium" style={active ? { ...sborder(C.card, '999px'), color: C.fg } : { color: C.fgMuted, borderRadius: '999px', border: `1px solid ${C.divider}` }}>{label}</span>;
}
function Pill({ color, children }: { color: string; children: ReactNode }) {
  return <span className="text-xs px-2 py-0.5 font-medium" style={{ color, border: `1px solid ${color}`, borderRadius: '999px' }}>{children}</span>;
}
function Alert({ color, title, body }: { color: string; title: string; body: string }) {
  return (
    <div className="px-3.5 py-3 flex gap-2.5 items-start" style={sborder(C.card)}>
      <span className="mt-1 h-2 w-2 shrink-0 rounded-full" style={{ background: color }} />
      <div><div className="text-sm font-medium" style={{ color }}>{title}</div><div className="text-xs mt-0.5" style={{ color: C.fgMuted }}>{body}</div></div>
    </div>
  );
}
