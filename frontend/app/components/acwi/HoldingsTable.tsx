'use client';

import { useMemo, useState } from 'react';
import { fmtMv, fmtNum } from './format';
import type { Holding } from './types';

/** The full "All Holdings" table — owns its filter box + sortable columns
 * (the only sortable table in the view). */
export default function HoldingsTable({ holdings }: { holdings: Holding[] }) {
  const [filter, setFilter] = useState('');
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);

  const filtered = useMemo(() => {
    let result = holdings;
    if (filter) {
      const q = filter.toLowerCase();
      result = result.filter(
        h =>
          h.Ticker.toLowerCase().includes(q) ||
          h.Name.toLowerCase().includes(q) ||
          h.Sector.toLowerCase().includes(q) ||
          h.Location.toLowerCase().includes(q)
      );
    }
    if (sortCol) {
      const numericCols = new Set(['Weight (%)', 'Market Value', 'Price', 'Quantity', 'FX Rate']);
      const isNumeric = numericCols.has(sortCol);
      result = [...result].sort((a, b) => {
        const av = (a as Record<string, string>)[sortCol] ?? '';
        const bv = (b as Record<string, string>)[sortCol] ?? '';
        if (isNumeric) {
          const na = parseFloat(av) || 0;
          const nb = parseFloat(bv) || 0;
          return sortAsc ? na - nb : nb - na;
        }
        return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
      });
    }
    return result;
  }, [holdings, filter, sortCol, sortAsc]);

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40">
      <div className="px-5 py-4 border-b border-neutral-800/40 flex items-center gap-4">
        <h2 className="text-sm font-medium text-fg-soft">All Holdings</h2>
        <input
          type="text"
          placeholder="Filter by ticker, name, sector, country..."
          value={filter}
          onChange={e => setFilter(e.target.value)}
          className="ml-auto bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-sm text-fg placeholder-fg-subtle w-72 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
        />
        <span className="text-fg-subtle text-xs">{filtered.length.toLocaleString()} shown</span>
      </div>
      <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-card z-10">
            <tr className="text-fg-muted text-xs uppercase tracking-wider">
              <th className="text-left px-3 py-2.5 font-medium">#</th>
              {([
                ['Ticker', 'Ticker', 'left'],
                ['Name', 'Name', 'left'],
                [null, 'GuruFocus', 'left'],
                ['Sector', 'Sector', 'left'],
                ['Location', 'Location', 'left'],
                ['Price', 'Price', 'right'],
                ['Exchange', 'Exchange', 'left'],
                ['gf_exchange', 'GF Exchange', 'left'],
                ['Currency', 'Currency', 'left'],
                ['gf_currency', 'GF Currency', 'left'],
                ['Weight (%)', 'Weight', 'right'],
                ['Market Value', 'Market Value', 'right'],
              ] as const).map(([key, label, align]) => (
                <th
                  key={label}
                  className={`text-${align} px-3 py-2.5 font-medium ${key ? 'cursor-pointer select-none hover:text-fg transition-colors' : ''}`}
                  onClick={key ? () => {
                    if (sortCol === key) {
                      setSortAsc(!sortAsc);
                    } else {
                      setSortCol(key);
                      setSortAsc(true);
                    }
                  } : undefined}
                >
                  {label}
                  {key && sortCol === key && (
                    <span className="ml-1 text-accent-400">{sortAsc ? '▲' : '▼'}</span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/30">
            {filtered.map((h, i) => (
              <tr key={`${h.Ticker}-${i}`} className="hover:bg-overlay/[0.02]">
                <td className="px-3 py-2.5 text-fg-subtle font-mono text-xs">{i + 1}</td>
                <td className="px-3 py-2.5 text-fg-strong font-mono font-medium">{h.Ticker}</td>
                <td className="px-3 py-2.5 text-fg max-w-[200px] truncate">{h.Name}</td>
                <td className="px-3 py-2.5 text-xs">
                  {h.gurufocus_url ? (
                    <a
                      href={h.gurufocus_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-accent-400 hover:text-accent-300 transition-colors"
                    >
                      link
                    </a>
                  ) : (
                    <span className="text-fg-faint">—</span>
                  )}
                </td>
                <td className="px-3 py-2.5 text-fg-muted">{h.Sector}</td>
                <td className="px-3 py-2.5 text-fg-muted">{h.Location}</td>
                <td className="px-3 py-2.5 text-fg-soft font-mono text-right">{fmtNum(h.Price)}</td>
                <td className="px-3 py-2.5 text-fg-muted text-xs">{h.Exchange}</td>
                <td className="px-3 py-2.5 text-accent-400 font-mono text-xs">{h.gf_exchange ?? <span className="text-fg-faint">-</span>}</td>
                <td className="px-3 py-2.5 text-fg-muted font-mono text-xs">{h.Currency}</td>
                <td className="px-3 py-2.5 text-accent-400 font-mono text-xs">{h.gf_currency ?? <span className="text-fg-faint">-</span>}</td>
                <td className="px-3 py-2.5 text-fg-soft font-mono text-right">{fmtNum(h['Weight (%)'])}%</td>
                <td className="px-3 py-2.5 text-fg-soft font-mono text-right">{fmtMv(h['Market Value'])}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
