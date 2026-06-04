'use client';

import { useState } from 'react';
import type { NetAddition } from './types';

/** MSCI Standard-Index net additions matched against current ACWI holdings.
 * Owns its own search box; hidden entirely until there's data or a load in
 * flight. */
export default function NetAdditionsTable({
  netAdditions,
  netAdditionsLoading,
  netAdditionsStats,
  fetching,
}: {
  netAdditions: NetAddition[];
  netAdditionsLoading: boolean;
  netAdditionsStats: { total: number; matched: number } | null;
  fetching: boolean;
}) {
  const [netAdditionsSearch, setNetAdditionsSearch] = useState('');

  if (netAdditions.length === 0 && !netAdditionsLoading) return null;

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40">
      <div className="px-5 py-4 border-b border-neutral-800/40 flex items-center gap-4">
        <div className="flex-1">
          <h2 className="text-sm font-medium text-fg-soft">
            Net Additions
            {netAdditionsStats && (
              <span className="text-fg-subtle font-normal ml-2">
                ({netAdditionsStats.total} total, {netAdditionsStats.matched} matched)
              </span>
            )}
            {(netAdditionsLoading || fetching) && <span className="text-fg-subtle font-normal ml-2 animate-pulse">loading...</span>}
          </h2>
          <p className="text-fg-subtle text-xs mt-0.5">
            Companies added to MSCI Standard Index and not subsequently deleted, matched against current ACWI holdings
          </p>
        </div>
        <input
          type="text"
          placeholder="Search..."
          value={netAdditionsSearch}
          onChange={e => setNetAdditionsSearch(e.target.value)}
          className="bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-sm text-fg placeholder-fg-subtle focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none w-56"
        />
      </div>
      {netAdditions.length > 0 && (() => {
        const q = netAdditionsSearch.toLowerCase();
        const filtered = q
          ? netAdditions.filter(item =>
              item.company_name.toLowerCase().includes(q) ||
              item.country.toLowerCase().includes(q) ||
              (item.matched_ticker ?? '').toLowerCase().includes(q) ||
              (item.matched_name ?? '').toLowerCase().includes(q)
            )
          : netAdditions;
        return (
        <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-card z-10">
              <tr className="text-fg-muted text-xs uppercase tracking-wider">
                <th className="text-left px-3 py-1.5 font-medium w-10">#</th>
                <th className="text-left px-3 py-1.5 font-medium w-14">CC</th>
                <th className="text-left px-3 py-1.5 font-medium">Announcement</th>
                <th className="text-left px-3 py-1.5 font-medium w-28">Added</th>
                <th className="text-left px-3 py-1.5 font-medium w-16 text-center">Match</th>
                <th className="text-left px-3 py-1.5 font-medium w-20">Ticker</th>
                <th className="text-left px-3 py-1.5 font-medium">Matched Holding</th>
                <th className="text-left px-3 py-1.5 font-medium w-20">Method</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/30">
              {filtered.map((item, i) => (
                <tr key={item.href} className="hover:bg-overlay/[0.02]">
                  <td className="px-3 py-2 text-fg-subtle font-mono text-xs">{i + 1}</td>
                  <td className="px-3 py-2 text-fg-muted font-mono text-xs">{item.country}</td>
                  <td className="px-3 py-2 whitespace-nowrap">
                    <a
                      href={item.href}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-accent-400 hover:text-accent-300 transition-colors"
                    >
                      {item.company_name}
                    </a>
                  </td>
                  <td className="px-3 py-2 text-fg-muted font-mono text-xs whitespace-nowrap">{item.date}</td>
                  <td className="px-3 py-2 text-center">
                    {item.matched ? (
                      <span className="text-pos-400">&#10003;</span>
                    ) : (
                      <span className="text-neg-400">&#10007;</span>
                    )}
                  </td>
                  <td className="px-3 py-2 text-fg font-mono text-xs">{item.matched_ticker ?? ''}</td>
                  <td className="px-3 py-2 text-fg-soft text-xs whitespace-nowrap">
                    {item.matched_name ?? ''}
                  </td>
                  <td className="px-3 py-2 text-fg-subtle font-mono text-xs">{item.match_method}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        );
      })()}
    </div>
  );
}
