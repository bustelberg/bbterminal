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
    <div className="bg-[#151821] rounded-xl border border-gray-800/40">
      <div className="px-5 py-4 border-b border-gray-800/40">
        <h2 className="text-sm font-medium text-gray-300">
          Other Country-Coded Announcements
          <span className="text-gray-500 font-normal ml-2">({otherCountryCoded.length})</span>
        </h2>
        <p className="text-gray-500 text-xs mt-0.5">Non-constituent announcements (updates, reviews, policy changes)</p>
      </div>
      <div className="overflow-x-auto max-h-[250px] overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-[#151821]">
            <tr className="text-gray-400 text-xs uppercase tracking-wider">
              <th className="text-left px-3 py-2.5 font-medium w-32">Date</th>
              <th className="text-left px-3 py-2.5 font-medium">Announcement</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800/30">
            {otherCountryCoded.map((a, i) => (
              <tr key={i} className="hover:bg-white/[0.02]">
                <td className="px-3 py-2.5 text-gray-400 font-mono text-xs whitespace-nowrap">{a.date}</td>
                <td className="px-3 py-2.5">
                  {a.href ? (
                    <a
                      href={a.href}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-indigo-400 hover:text-indigo-300 transition-colors"
                    >
                      {a.title}
                    </a>
                  ) : (
                    <span className="text-gray-200">{a.title}</span>
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
