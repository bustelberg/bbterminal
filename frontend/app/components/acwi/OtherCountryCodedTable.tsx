'use client';

import type { Announcement } from './types';

/** Non-constituent, country-coded MSCI announcements (updates, reviews,
 * policy changes). Hidden when there are none. */
export default function OtherCountryCodedTable({
  otherCountryCoded,
}: {
  otherCountryCoded: Announcement[];
}) {
  if (otherCountryCoded.length === 0) return null;

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40">
      <div className="px-5 py-4 border-b border-neutral-800/40">
        <h2 className="text-sm font-medium text-fg-soft">
          Other Country-Coded Announcements
          <span className="text-fg-subtle font-normal ml-2">({otherCountryCoded.length})</span>
        </h2>
        <p className="text-fg-subtle text-xs mt-0.5">Non-constituent announcements (updates, reviews, policy changes)</p>
      </div>
      <div className="overflow-x-auto max-h-[250px] overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-card">
            <tr className="text-fg-muted text-xs uppercase tracking-wider">
              <th className="text-left px-3 py-2.5 font-medium w-32">Date</th>
              <th className="text-left px-3 py-2.5 font-medium">Announcement</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/30">
            {otherCountryCoded.map((a, i) => (
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
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
