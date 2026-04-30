import Link from 'next/link';
import { cookies } from 'next/headers';
import { createClient } from '../lib/supabase/server';

type Tile = {
  href: string;
  label: string;
  description: string;
  badge?: string;
  // Tiles marked `userVisible: true` are shown to regular users; everything
  // else is admin-only. Mirrors the Sidebar's `userVisible` flag so the
  // home page never advertises a page the user can't actually open.
  userVisible?: true;
};

const tiles: Tile[] = [
  {
    href: '/earnings',
    label: 'Earnings Dashboard',
    description: 'Browse per-company earnings metrics pulled from GuruFocus, with quick refresh by source.',
    userVisible: true,
  },
  {
    href: '/momentum',
    label: 'Momentum',
    description: 'Backtest momentum strategies and inspect Current Picks — locked monthly holdings plus a daily-picks history.',
  },
  {
    href: '/universe',
    label: 'Universe Overview',
    description: 'Criteria-driven universe screener — apply filters to companies and save labelled, derived universes.',
  },
  {
    href: '/longequity-universe',
    label: 'LongEquity Universe',
    description: 'Monthly snapshots of the LongEquity universe, grouped by region and country. Run the ingest pipeline from here.',
  },
  {
    href: '/universe_index',
    label: 'SP500 Universe',
    description: 'Reconstructed S&P 500 memberships over time, with monthly tickers and change history.',
  },
  {
    href: '/acwi',
    label: 'ACWI Universe',
    description: 'iShares ACWI holdings and MSCI announcement explorer — review additions, deletions, and net changes.',
  },
  {
    href: '/fx-rates',
    label: 'FX Rates',
    description: 'View FX rate coverage and history, and sync the latest ECB / Yahoo rates into the database.',
  },
  {
    href: '/airs-portfolio',
    label: 'AIRS Portfolio',
    description: 'Broker scanner and AIRS Excel upload — parses holdings and computes YTD returns in EUR and local currency.',
    badge: 'Under construction',
  },
  {
    href: '/request_gurufocus',
    label: 'Request GuruFocus',
    description: 'Trigger GuruFocus indicator fetches for selected companies and exchanges.',
  },
  {
    href: '/benchmarks',
    label: 'Benchmarks',
    description: 'Manage index benchmarks (SPY, ACWI, …) — add tickers, fetch prices, and inspect coverage.',
  },
  {
    href: '/companies',
    label: 'Companies',
    description: 'Searchable, filterable company table with inline edit, add, and delete (cascades metric and weight rows).',
    userVisible: true,
  },
];

export default async function Home() {
  const supabase = await createClient();
  const { data: { user } } = await supabase.auth.getUser();
  const cookieStore = await cookies();

  // Match the middleware's role + view-as logic so the home tiles always
  // reflect what the user can *actually* navigate to.
  const realRole = (user?.app_metadata as { role?: string } | undefined)?.role === 'admin' ? 'admin' : 'user';
  const viewAs = cookieStore.get('view_as')?.value;
  const effectiveRole: 'admin' | 'user' = realRole === 'admin' && viewAs !== 'user' ? 'admin' : 'user';

  const visibleTiles = effectiveRole === 'admin'
    ? tiles
    : tiles.filter((t) => t.userVisible);

  return (
    <div className="px-8 py-8 max-w-6xl">
      <div className="mb-8">
        <h1 className="text-2xl font-semibold text-white mb-2">Welcome to BBTerminal</h1>
        <p className="text-sm text-gray-400 leading-relaxed">
          Analyse stocks based on data from LongEquity and index universes, enriched with data from GuruFocus.
        </p>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {visibleTiles.map(({ href, label, description, badge }) => (
          <Link
            key={href}
            href={href}
            className="group block bg-[#151821] rounded-xl border border-gray-800/40 p-5 hover:border-indigo-500/40 hover:bg-[#181b25] transition-colors"
          >
            <div className="flex items-start justify-between gap-3 mb-2">
              <h2 className="text-base font-semibold text-white group-hover:text-indigo-400 transition-colors">
                {label}
              </h2>
              {badge && (
                <span className="shrink-0 text-xs font-medium px-2 py-0.5 rounded-md bg-amber-500/10 text-amber-400 border border-amber-500/20">
                  {badge}
                </span>
              )}
            </div>
            <p className="text-sm text-gray-400 leading-relaxed">
              {description}
            </p>
          </Link>
        ))}
      </div>
    </div>
  );
}
