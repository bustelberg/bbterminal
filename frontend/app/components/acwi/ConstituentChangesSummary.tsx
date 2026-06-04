'use client';

import { actionStyle } from './format';
import type { DetailSummaryGroups } from './types';

/** Constituent-change announcements grouped by parsed action (ADDED /
 * DELETED / ADDED+DELETED). Rendered only once at least one detail has been
 * parsed (`hasFetchedDetails`, gated by the parent). */
export default function ConstituentChangesSummary({
  detailSummary,
  fetching,
}: {
  detailSummary: DetailSummaryGroups;
  fetching: boolean;
}) {
  return (
    <div className="bg-[#151821] rounded-xl border border-gray-800/40">
      <div className="px-5 py-4 border-b border-gray-800/40">
        <h2 className="text-sm font-medium text-gray-300">
          Constituent Changes Summary
          {fetching && <span className="text-gray-500 font-normal ml-2 animate-pulse">fetching...</span>}
        </h2>
        <p className="text-gray-500 text-xs mt-0.5">Parsed from MSCI announcement details</p>
      </div>
      <div className="p-5 space-y-4">
        {(['ADDED', 'DELETED', 'ADDED+DELETED'] as const).map(action => {
          const items = detailSummary[action];
          if (!items || items.length === 0) return null;
          return (
            <div key={action}>
              <div className="flex items-center gap-2 mb-2">
                <span className={`text-xs font-medium px-2 py-0.5 rounded ${actionStyle(action)}`}>
                  {action}
                </span>
                <span className="text-gray-500 text-xs">{items.length} announcement{items.length !== 1 ? 's' : ''}</span>
              </div>
              <div className="overflow-x-auto max-h-[250px] overflow-y-auto">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-[#151821]">
                    <tr className="text-gray-400 text-xs uppercase tracking-wider">
                      <th className="text-left px-3 py-1.5 font-medium w-32">Date</th>
                      <th className="text-left px-3 py-1.5 font-medium">Announcement</th>
                      <th className="text-left px-3 py-1.5 font-medium w-40">Effective Date</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-800/30">
                    {items.map(({ announcement: a, detail: d }, i) => (
                      <tr key={i} className="hover:bg-white/[0.02]">
                        <td className="px-3 py-2 text-gray-400 font-mono text-xs whitespace-nowrap">{a.date}</td>
                        <td className="px-3 py-2">
                          <a
                            href={a.href}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-indigo-400 hover:text-indigo-300 transition-colors"
                          >
                            {a.title}
                          </a>
                        </td>
                        <td className="px-3 py-2 text-gray-300 font-mono text-xs whitespace-nowrap">
                          {d.effective_date ?? ''}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
