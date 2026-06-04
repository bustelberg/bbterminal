'use client';

import { useMemo, useState } from 'react';
import LoadingDots from '../LoadingDots';
import { actionStyle } from './format';
import type { Announcement, Detail } from './types';

/** MSCI Index announcements explorer. Owns the constituent-only toggle +
 * search box (and the derived filtered list); per-row detail fetching is
 * delegated to the parent's `getDetail` / `fetchDetail` so manual fetches
 * stay in the shared hook state. */
export default function AnnouncementsTable({
  announcements,
  annLoading,
  annError,
  getDetail,
  fetchDetail,
  fetching,
}: {
  announcements: Announcement[];
  annLoading: boolean;
  annError: string | null;
  getDetail: (a: Announcement) => Detail | undefined;
  fetchDetail: (href: string) => void;
  fetching: boolean;
}) {
  const [constituentOnly, setConstituentOnly] = useState(true);
  const [annSearch, setAnnSearch] = useState('');

  const filteredAnnouncements = useMemo(() => {
    let list = announcements;
    if (constituentOnly) list = list.filter(a => a.is_constituent_change);
    if (annSearch) {
      const q = annSearch.toLowerCase();
      list = list.filter(a => a.title.toLowerCase().includes(q));
    }
    return list;
  }, [announcements, constituentOnly, annSearch]);

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40">
      <div className="px-5 py-4 border-b border-neutral-800/40 flex items-center gap-4">
        <div>
          <h2 className="text-sm font-medium text-fg-soft">
            MSCI Index Announcements
            {filteredAnnouncements.length > 0 && (
              <span className="text-fg-subtle font-normal ml-2">
                ({filteredAnnouncements.length}{constituentOnly ? ` of ${announcements.length}` : ''})
              </span>
            )}
          </h2>
          <p className="text-fg-subtle text-xs mt-0.5">
            {constituentOnly ? 'Constituent changes only' : 'All announcements'} from MSCI Standard Indexes
          </p>
        </div>
        <input
          type="text"
          placeholder="Search announcements..."
          value={annSearch}
          onChange={e => setAnnSearch(e.target.value)}
          className="ml-auto bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-sm text-fg placeholder-fg-subtle w-64 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
        />
        <button
          onClick={() => setConstituentOnly(v => !v)}
          className={`text-xs px-3 py-1.5 rounded-lg border transition-colors ${
            constituentOnly
              ? 'bg-accent-600/20 border-accent-500/40 text-accent-400'
              : 'bg-transparent border-neutral-700 text-fg-muted hover:bg-overlay/5'
          }`}
        >
          {constituentOnly ? 'Constituent changes' : 'All announcements'}
        </button>
      </div>
      {annError && (
        <div className="mx-5 mt-3 bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3 text-neg-400 text-sm">
          {annError}
        </div>
      )}
      {annLoading ? (
        <div className="px-5 py-4 text-fg-muted text-sm"><LoadingDots label="Loading announcements" /></div>
      ) : (
        <div className="overflow-x-auto max-h-[400px] overflow-y-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-card">
              <tr className="text-fg-muted text-xs uppercase tracking-wider">
                <th className="text-left px-3 py-2.5 font-medium w-32">Date</th>
                <th className="text-left px-3 py-2.5 font-medium">Announcement</th>
                <th className="text-center px-3 py-2.5 font-medium w-20">Detail</th>
                <th className="text-left px-3 py-2.5 font-medium w-24">Action</th>
                <th className="text-left px-3 py-2.5 font-medium w-40">Effective Date</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/30">
              {filteredAnnouncements.map((a, i) => {
                const detail = getDetail(a);
                return (
                  <tr key={i} className="hover:bg-overlay/[0.02]">
                    <td className="px-3 py-2.5 text-fg-muted font-mono text-xs whitespace-nowrap">{a.date}</td>
                    <td className="px-3 py-2.5">
                      {a.href ? (
                        <a
                          href={a.href}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-accent-400 hover:text-accent-300 transition-colors"
                        >
                          {a.title}
                        </a>
                      ) : (
                        <span className="text-fg">{a.title}</span>
                      )}
                    </td>
                    <td className="px-3 py-2.5 text-center">
                      {!detail && a.href && !fetching && (
                        <button
                          onClick={() => fetchDetail(a.href)}
                          className="text-xs px-2 py-1 rounded border border-neutral-700 text-fg-muted hover:bg-overlay/5 hover:text-fg transition-colors"
                        >
                          Fetch
                        </button>
                      )}
                      {!detail && fetching && (
                        <span className="text-fg-faint text-xs">...</span>
                      )}
                      {detail?.loading && (
                        <span className="text-fg-subtle text-xs animate-pulse">...</span>
                      )}
                      {detail?.error && (
                        <span className="text-neg-400 text-xs" title={detail.error}>err</span>
                      )}
                    </td>
                    <td className="px-3 py-2.5">
                      {detail && !detail.loading && (
                        detail.standard ? (
                          <span className={`text-xs font-medium px-2 py-0.5 rounded ${actionStyle(detail.standard)}`}>
                            {detail.standard}
                          </span>
                        ) : (
                          <span className="text-fg-faint text-xs">N/A</span>
                        )
                      )}
                    </td>
                    <td className="px-3 py-2.5 text-fg-soft font-mono text-xs whitespace-nowrap">
                      {detail && !detail.loading && (detail.standard ? detail.effective_date ?? '' : '')}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
