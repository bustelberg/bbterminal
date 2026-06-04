'use client';

import { useMemo, useState } from 'react';
import type { TimelineRow } from './types';

/** Matched MSCI additions within the feasible universe, sorted by effective
 * date. Owns its search box; `additionTimeline` is derived in `useAcwiData`
 * (net additions ⨝ feasible holdings, deduped to first appearance). */
export default function AdditionTimelineTable({
  additionTimeline,
}: {
  additionTimeline: TimelineRow[];
}) {
  const [timelineSearch, setTimelineSearch] = useState('');

  const filteredTimeline = useMemo(() => {
    if (!timelineSearch) return additionTimeline;
    const q = timelineSearch.toLowerCase();
    return additionTimeline.filter(r =>
      r.ticker.toLowerCase().includes(q) ||
      r.name.toLowerCase().includes(q) ||
      r.country.toLowerCase().includes(q) ||
      r.cc.toLowerCase().includes(q) ||
      r.sector.toLowerCase().includes(q) ||
      (r.gf_exchange ?? '').toLowerCase().includes(q) ||
      r.effective_date.toLowerCase().includes(q)
    );
  }, [additionTimeline, timelineSearch]);

  return (
    <div className="bg-[#151821] rounded-xl border border-gray-800/40">
      <div className="px-5 py-4 border-b border-gray-800/40 flex items-center gap-4">
        <div>
          <h2 className="text-sm font-medium text-gray-300">
            Addition Timeline
            <span className="text-gray-500 font-normal ml-2">
              ({filteredTimeline.length.toLocaleString()}
              {timelineSearch ? ` of ${additionTimeline.length.toLocaleString()}` : ''})
            </span>
          </h2>
          <p className="text-gray-500 text-xs mt-0.5">
            Matched MSCI additions in the feasible universe, sorted by effective date (most recent first).
          </p>
        </div>
        <input
          type="text"
          placeholder="Search by ticker, name, country, sector, exchange, date..."
          value={timelineSearch}
          onChange={e => setTimelineSearch(e.target.value)}
          className="ml-auto bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-1.5 text-sm text-gray-200 placeholder-gray-500 w-80 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
        />
      </div>
      <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-[#151821] z-10">
            <tr className="text-gray-400 text-xs uppercase tracking-wider">
              <th className="text-left px-3 py-2.5 font-medium w-10">#</th>
              <th className="text-left px-3 py-2.5 font-medium w-40">Effective Date</th>
              <th className="text-left px-3 py-2.5 font-medium w-24">Ticker</th>
              <th className="text-left px-3 py-2.5 font-medium">Name</th>
              <th className="text-left px-3 py-2.5 font-medium w-16">GF</th>
              <th className="text-left px-3 py-2.5 font-medium">Sector</th>
              <th className="text-left px-3 py-2.5 font-medium">Country</th>
              <th className="text-left px-3 py-2.5 font-medium w-20">GF Exch</th>
              <th className="text-left px-3 py-2.5 font-medium w-20">Announcement</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800/30">
            {filteredTimeline.map((r, i) => (
              <tr key={`${r.ticker}-${r.effective_date}-${i}`} className="hover:bg-white/[0.02]">
                <td className="px-3 py-2 text-gray-500 font-mono text-xs">{i + 1}</td>
                <td className="px-3 py-2 text-gray-200 font-mono text-xs whitespace-nowrap">{r.effective_date}</td>
                <td className="px-3 py-2 text-white font-mono font-medium">{r.ticker}</td>
                <td className="px-3 py-2 text-gray-200 max-w-[280px] truncate">{r.name}</td>
                <td className="px-3 py-2 text-xs">
                  {r.gurufocus_url ? (
                    <a
                      href={r.gurufocus_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-indigo-400 hover:text-indigo-300 transition-colors"
                    >
                      link
                    </a>
                  ) : (
                    <span className="text-gray-600">&mdash;</span>
                  )}
                </td>
                <td className="px-3 py-2 text-gray-400 text-xs">{r.sector}</td>
                <td className="px-3 py-2 text-gray-400 text-xs">{r.country}</td>
                <td className="px-3 py-2 text-indigo-400 font-mono text-xs">{r.gf_exchange ?? <span className="text-gray-600">US</span>}</td>
                <td className="px-3 py-2 text-xs">
                  {r.href ? (
                    <a
                      href={r.href}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-indigo-400 hover:text-indigo-300 transition-colors"
                    >
                      link
                    </a>
                  ) : (
                    <span className="text-gray-600">&mdash;</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
