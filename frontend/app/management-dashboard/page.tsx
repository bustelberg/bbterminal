'use client';

import { useEffect } from 'react';
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
 */
export default function Page() {
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
      panels: ['PortfolioOverviewPanel', 'CorrelationMatrix', 'BenchmarksPanel'],
      silence: "localStorage.setItem('bb.debug','0') to quiet these traces",
    });
  }, []);

  return (
    <div className="min-h-screen bg-page text-fg">
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">Management Dashboard</h1>
      </div>

      <div className="px-8 py-6 space-y-6">
        <PortfolioOverviewPanel />
        <CorrelationMatrix />
        <BenchmarksPanel />
      </div>
    </div>
  );
}
