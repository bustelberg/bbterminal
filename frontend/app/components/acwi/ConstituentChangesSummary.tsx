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
    <div className="bg-card rounded-xl border border-neutral-800/40">
      <div className="px-5 py-4 border-b border-neutral-800/40">
        <h2 className="text-sm font-medium text-fg-soft">
          Constituent Changes Summary
          {fetching && <span className="text-fg-subtle font-normal ml-2 animate-pulse">fetching...</span>}
        </h2>
        <p className="text-fg-subtle text-xs mt-0.5">Parsed from MSCI announcement details</p>
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
                <span className="text-fg-subtle text-xs">{items.length} announcement{items.length !== 1 ? 's' : ''}</span>
              </div>
              <div className="overflow-x-auto max-h-[250px] overflow-y-auto">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-card">
                    <tr className="text-fg-muted text-xs uppercase tracking-wider">
                      <th className="text-left px-3 py-1.5 font-medium w-32">Date</th>
                      <th className="text-left px-3 py-1.5 font-medium">Announcement</th>
                      <th className="text-left px-3 py-1.5 font-medium w-40">Effective Date</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-neutral-800/30">
                    {items.map(({ announcement: a, detail: d }, i) => (
                      <tr key={i} className="hover:bg-overlay/[0.02]">
                        <td className="px-3 py-2 text-fg-muted font-mono text-xs whitespace-nowrap">{a.date}</td>
                        <td className="px-3 py-2">
                          <a
                            href={a.href}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-accent-400 hover:text-accent-300 transition-colors"
                          >
                            {a.title}
                          </a>
                        </td>
                        <td className="px-3 py-2 text-fg-soft font-mono text-xs whitespace-nowrap">
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
