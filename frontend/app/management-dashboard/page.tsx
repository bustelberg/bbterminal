'use client';

import { useEffect, useState } from 'react';
import { API_URL } from '../../lib/apiUrl';
import { trace } from '../../lib/debugTrace';
import BenchmarksPanel from '../components/BenchmarksPanel';
import CorrelationMatrix from '../components/CorrelationMatrix';
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

const TABS: { key: TabKey; label: string; note: string }[] = [
  { key: 'overview', label: 'Overview',
    note: 'Each portfolio on its own — its holdings, weights and returns.' },
  { key: 'cross', label: 'Cross-portfolio',
    note: 'How the portfolios move together. A pairwise view: no single portfolio has this number.' },
  { key: 'benchmarks', label: 'Benchmarks',
    note: 'The indices the portfolios are measured against, cap-weighted and rebuilt from our own constituents.' },
];

export default function Page() {
  const [tab, setTab] = useState<TabKey>('overview');
  // ⚠ MOUNTED ON FIRST VISIT, THEN KEPT. Each panel is a real request — the correlation matrix an
  // N×N grid over every portfolio's YTD, the benchmarks a full index rebuild — expensive enough
  // not to spend on a page load nobody asked for, and expensive enough not to re-spend every time
  // the reader flicks back. So a pane is not rendered until its tab is opened, and after that it
  // stays mounted and is hidden with CSS: the table keeps its expanded rows, sort and filters, and
  // nothing refetches on a switch. Safe because none of these panels polls — they fetch on mount —
  // so a hidden pane costs nothing.
  const [seen, setSeen] = useState<Set<TabKey>>(() => new Set<TabKey>(['overview']));
  // Latched in the handler, not in an effect: the tab changing IS the event, so deriving it from
  // the resulting state would be a render-then-correct round trip for something already known.
  const select = (k: TabKey) => {
    setSeen((s) => (s.has(k) ? s : new Set(s).add(k)));
    setTab(k);
  };
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
      // ⚠ Only Overview mounts on load; the other two are lazy, so silence from them is expected
      // until their tab is opened, not a panel that failed.
      panels: {
        Overview: ['PortfolioOverviewPanel'],
        'Cross-portfolio': ['CorrelationMatrix (lazy)'],
        Benchmarks: ['BenchmarksPanel (lazy)'],
      },
      silence: "localStorage.setItem('bb.debug','0') to quiet these traces",
    });
  }, []);

  return (
    <div className="min-h-screen bg-page text-fg">
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">Management Dashboard</h1>
      </div>

      <div className="px-8 py-6 space-y-6">
        {/* The segmented control the rest of the app uses for a small, fixed set of named views
            (see the holdings table's "Weight returns by") — two discrete choices, not a slider. */}
        <div className="inline-flex rounded-lg border border-neutral-800/40 overflow-hidden text-xs">
          {TABS.map((t) => (
            <button key={t.key} type="button" onClick={() => select(t.key)} title={t.note}
              aria-pressed={tab === t.key}
              className={`px-3 py-1.5 font-medium transition-colors ${tab === t.key
                ? 'bg-accent-600 text-white'
                : 'text-fg-subtle hover:bg-overlay/5'}`}>
              {t.label}
            </button>
          ))}
        </div>

        {/* ⚠ HIDDEN, NOT UNMOUNTED. Unmounting throws away the table's expanded rows, sort and
            filters — and refetches the panel — on a switch a reader makes to glance at one thing
            and come back. */}
        <div className={tab === 'overview' ? '' : 'hidden'}>
          <PortfolioOverviewPanel />
        </div>
        {seen.has('cross') && (
          <div className={tab === 'cross' ? '' : 'hidden'}>
            <CorrelationMatrix />
          </div>
        )}
        {seen.has('benchmarks') && (
          <div className={tab === 'benchmarks' ? '' : 'hidden'}>
            <BenchmarksPanel />
          </div>
        )}
      </div>
    </div>
  );
}
