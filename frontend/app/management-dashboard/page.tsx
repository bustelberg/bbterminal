'use client';

import { useEffect, useState } from 'react';
import { API_URL } from '../../lib/apiUrl';
import { trace } from '../../lib/debugTrace';
import { useMgmtCopy } from '../components/management/managementCopy';
import PortfolioOverviewPanel from '../components/PortfolioOverviewPanel';

/**
 * The AIRS portfolios — a sibling of Execution instruments in the asset-pipeline nav, not a tab
 * inside it: they are different universes. Instruments are per-ISIN rows we resolve and price;
 * portfolios are AirSPMS constructs whose positions happen to carry the ISIN that joins the two.
 *
 * `PortfolioOverviewPanel` is THE table. A portfolio is TWO rows in AIRS — the Fixed one (weights,
 * ISINs, your nickname; AIRS values none of it) and the Dynamic one (the real book: money, returns,
 * no ISIN) — and it composes the pair into the row you actually want: named from the Fixed side,
 * every number AIRS's own, and each value carrying its own provenance (where / when / how).
 *
 * THREE TABS, SPLIT BY WHAT THE PANEL IS ABOUT — and each is a different UNIT OF ANALYSIS, which
 * is why stacking them read as one long page of trailing detail. Overview is one row per
 * portfolio. Cross-portfolio is a PAIR of portfolios: how they move together, a question no single
 * row can answer. Benchmarks are not portfolios at all — they are indices rebuilt from our own
 * constituents, the yardstick the other two are measured against rather than another thing we hold.
 */
type TabKey = 'overview' | 'cross' | 'benchmarks';

/**
 * ⚠ THE ORDER LIVES HERE, THE WORDS LIVE IN `managementCopy`. A tab's label and its hover are
 * copy and are translated; which tabs exist and in what order is a fact about the page. Keeping
 * the strings here would have meant a second English original for the translation to drift from.
 *
 * ⚠⚠ `cross` AND `benchmarks` WERE TAKEN OFF THE PAGE ON 2026-09-01, ON REQUEST AND EXPLICITLY
 * "FOR NOW". Nothing behind them was deleted: `CorrelationMatrix.tsx`, `BenchmarksPanel.tsx` and
 * `benchmarks/FundamentalGridPane.tsx` are untouched on disk, every `/api/benchmarks/index/*`
 * route still serves, and the scheduled jobs that keep that data current (`benchmark_index_refresh`
 * 06:30 Mon-Fri, `benchmark_fundamentals_fill` quarterly, the held-ETF leg of the 05:00
 * `price_update`) are unchanged. `TabKey` still names all three, and their copy is still translated
 * in both languages, so putting one back is: add it to this array and re-add its two lines below.
 *
 * ⚠⚠ AND THE BENCHMARK DATA IS STILL READ, JUST NOT THROUGH THESE ROUTES. The Fundamental modal's
 * Long Equity benchmark line does NOT call `/api/benchmarks/index/*` — it reads
 * `/api/earnings/fundamental-blend-metrics` + `/universe-period-caps`, i.e. the TEMPLATE UNIVERSE
 * (ACWI / SP500 / AEX) and the `metric_data` rows behind its constituents. What the removed tab
 * owned was the MANUAL FILL for exactly those rows — its two job buttons rebuild an index's
 * constituents and ingest their statements — which is why removing it cannot break the modal but
 * does remove the only on-demand "fill this now" control. The schedule is what keeps it current;
 * a gap now waits for the next tick instead of a button. See `docs/airs-portfolios.md`.
 */
const TAB_ORDER: TabKey[] = ['overview'];

export default function Page() {
  const t = useMgmtCopy();
  const [tab, select] = useState<TabKey>('overview');
  /**
   * ⚠⚠ THE LAZY-MOUNT LATCH WENT WITH THE TWO TABS (2026-09-01) AND COMES BACK WITH THEM. It was a
   * `seen` set: a pane was not rendered until its tab had been opened, and after that stayed
   * mounted and hidden with CSS — because each was a real request (an N×N correlation grid over
   * every portfolio, a full index rebuild), expensive enough not to spend on a page load nobody
   * asked for and expensive enough not to re-spend when the reader flicks back. Overview is the
   * only pane left and it mounts unconditionally, so the set had exactly one member, was never
   * read, and was state that existed to answer a question nobody asks.
   *
   * ⚠ RESTORING A TAB MEANS RESTORING THIS TOO. Re-adding a pane without it makes the page fetch
   * that panel on every load of a dashboard whose default tab is Overview — which is the cost the
   * latch was written for, and it will not look like a regression, only like a slow page.
   */
  // ⚠ THE FIRST LINE IN THE CONSOLE NAMES THE ENVIRONMENT, because the most common way this page
  // is "broken" in production is that it is pointed somewhere else. A wrong `NEXT_PUBLIC_API_URL`
  // — or a session that never attached — makes every panel render empty, and every panel's own
  // message then blames the data. One line at boot separates "the database has nothing in it"
  // from "we are not talking to the database you think we are", which is otherwise a long hour.
  useEffect(() => {
    trace('page', 'management-dashboard mounted', {
      api: API_URL,
      origin: typeof window !== 'undefined' ? window.location.origin : '?',
      // Presence only — a bearer token is never logged (see `apiFetch`).
      signedIn: typeof document !== 'undefined' && document.cookie.includes('sb-'),
      // ⚠ THE TRACE NAMES WHAT IS ACTUALLY MOUNTED. It listed the Cross-portfolio and Benchmarks
      // panes as "lazy" — true while they were tabs, and after they were removed it would have had
      // the first line in the console describing a page that no longer exists, which is the one
      // line whose whole job is to be trusted.
      panels: { Overview: ['PortfolioOverviewPanel'] },
      tabsRemoved: ['cross', 'benchmarks'],
      silence: "localStorage.setItem('bb.debug','0') to quiet these traces",
    });
  }, []);

  return (
    <div className="min-h-screen bg-page text-fg">
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">{t.page.title}</h1>
      </div>

      <div className="px-8 py-6 space-y-6">
        {/* The segmented control the rest of the app uses for a small, fixed set of named views
            (see the holdings table's "Weight returns by") — discrete choices, not a slider.
            ⚠ NOT RENDERED AT ONE TAB. A segmented control with a single, permanently-pressed
            button is a control that cannot do anything, and a reader who presses it and sees
            nothing move learns to distrust the ones that work. It comes back on its own the
            moment `TAB_ORDER` grows — see the note there. */}
        {TAB_ORDER.length > 1 && (
        <div className="inline-flex rounded-lg border border-neutral-800/40 overflow-hidden text-xs">
          {TAB_ORDER.map((k) => (
            <button key={k} type="button" onClick={() => select(k)} title={t.page.tabs[k].note}
              aria-pressed={tab === k}
              className={`px-3 py-1.5 font-medium transition-colors ${tab === k
                ? 'bg-accent-600 text-white'
                : 'text-fg-subtle hover:bg-overlay/5'}`}>
              {t.page.tabs[k].label}
            </button>
          ))}
        </div>
        )}

        {/* ⚠ HIDDEN, NOT UNMOUNTED. Unmounting throws away the table's expanded rows, sort and
            filters — and refetches the panel — on a switch a reader makes to glance at one thing
            and come back. */}
        <div className={tab === 'overview' ? '' : 'hidden'}>
          <PortfolioOverviewPanel />
        </div>
      </div>
    </div>
  );
}
