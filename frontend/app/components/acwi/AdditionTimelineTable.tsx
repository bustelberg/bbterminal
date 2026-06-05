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
    <div className="bg-card rounded-xl border border-neutral-800/40">
      <div className="px-5 py-4 border-b border-neutral-800/40 flex items-center gap-4">
        <div>
          <h2 className="text-sm font-medium text-fg-soft">
            Addition Timeline
            <span className="text-fg-subtle font-normal ml-2">
              ({filteredTimeline.length.toLocaleString()}
              {timelineSearch ? ` of ${additionTimeline.length.toLocaleString()}` : ''})
            </span>
          </h2>
          <p className="text-fg-subtle text-xs mt-0.5">
            Matched MSCI additions in the feasible universe, sorted by effective date (most recent first).
          </p>
        </div>
        <input
          type="text"
          placeholder="Search by ticker, name, country, sector, exchange, date..."
          value={timelineSearch}
          onChange={e => setTimelineSearch(e.target.value)}
          className="ml-auto bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-sm text-fg placeholder-fg-subtle w-80 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
        />
      </div>
      <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-card z-10">
            <tr className="text-fg-muted text-xs uppercase tracking-wider">
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
          <tbody className="divide-y divide-neutral-800/30">
            {filteredTimeline.map((r, i) => (
              <tr key={`${r.ticker}-${r.effective_date}-${i}`} className="hover:bg-overlay/[0.02]">
                <td className="px-3 py-2 text-fg-subtle font-mono text-xs">{i + 1}</td>
                <td className="px-3 py-2 text-fg font-mono text-xs whitespace-nowrap">{r.effective_date}</td>
                <td className="px-3 py-2 text-fg-strong font-mono font-medium">{r.ticker}</td>
                <td className="px-3 py-2 text-fg max-w-[280px] truncate">{r.name}</td>
                <td className="px-3 py-2 text-xs">
                  {r.gurufocus_url ? (
                    <a
                      href={r.gurufocus_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-accent-400 hover:text-accent-300 transition-colors"
                    >
                      link
                    </a>
                  ) : (
                    <span className="text-fg-faint">&mdash;</span>
                  )}
                </td>
                <td className="px-3 py-2 text-fg-muted text-xs">{r.sector}</td>
                <td className="px-3 py-2 text-fg-muted text-xs">{r.country}</td>
                <td className="px-3 py-2 text-accent-400 font-mono text-xs">{r.gf_exchange ?? <span className="text-fg-faint">US</span>}</td>
                <td className="px-3 py-2 text-xs">
                  {r.href ? (
                    <a
                      href={r.href}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-accent-400 hover:text-accent-300 transition-colors"
                    >
                      link
                    </a>
                  ) : (
                    <span className="text-fg-faint">&mdash;</span>
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
