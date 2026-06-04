'use client';

import { useMemo, useState } from 'react';
import { fmtMv, fmtNum } from './format';
import type { Holding } from './types';

/** The GuruFocus-feasible subset of holdings (USA/Europe/Asia ex-Russia).
 * Owns its filter box + the "1 per exchange" dedupe toggle used to spot-
 * check URL mappings. `feasibleHoldings` is pre-filtered in `useAcwiData`. */
export default function FeasibleUniverseTable({
  feasibleHoldings,
}: {
  feasibleHoldings: Holding[];
}) {
  const [feasibleFilter, setFeasibleFilter] = useState('');
  const [onePerExchange, setOnePerExchange] = useState(false);

  const feasibleDisplay = useMemo(() => {
    let result = feasibleHoldings;
    if (feasibleFilter) {
      const q = feasibleFilter.toLowerCase();
      result = result.filter(
        h =>
          h.Ticker.toLowerCase().includes(q) ||
          h.Name.toLowerCase().includes(q) ||
          h.Sector.toLowerCase().includes(q) ||
          h.Location.toLowerCase().includes(q) ||
          (h.gf_exchange ?? '').toLowerCase().includes(q)
      );
    }
    if (onePerExchange) {
      const seen = new Set<string>();
      const deduped: Holding[] = [];
      for (const h of result) {
        const key = h.gf_exchange ?? 'US';
        if (seen.has(key)) continue;
        seen.add(key);
        deduped.push(h);
      }
      result = deduped.sort((a, b) =>
        (a.gf_exchange ?? 'US').localeCompare(b.gf_exchange ?? 'US')
      );
    }
    return result;
  }, [feasibleHoldings, feasibleFilter, onePerExchange]);

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40">
      <div className="px-5 py-4 border-b border-neutral-800/40 flex items-center gap-4">
        <div>
          <h2 className="text-sm font-medium text-fg-soft">
            Feasible ACWI Universe
            <span className="text-fg-subtle font-normal ml-2">
              ({feasibleDisplay.length.toLocaleString()}
              {' of '}{feasibleHoldings.length.toLocaleString()})
            </span>
          </h2>
          <p className="text-fg-subtle text-xs mt-0.5">
            USA, Europe, and Asia (excluding Russia).
            {onePerExchange && ' One representative per GuruFocus exchange — click GuruFocus links to manually verify URL mappings.'}
          </p>
        </div>
        <input
          type="text"
          placeholder="Filter by ticker, name, sector, country, gf exchange..."
          value={feasibleFilter}
          onChange={e => setFeasibleFilter(e.target.value)}
          className="ml-auto bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-sm text-fg placeholder-fg-subtle w-72 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
        />
        <button
          onClick={() => setOnePerExchange(v => !v)}
          className={`text-xs px-3 py-1.5 rounded-lg border transition-colors whitespace-nowrap ${
            onePerExchange
              ? 'bg-accent-600/20 border-accent-500/40 text-accent-400'
              : 'bg-transparent border-neutral-700 text-fg-muted hover:bg-overlay/5'
          }`}
        >
          {onePerExchange ? '1 per exchange' : 'All feasible'}
        </button>
      </div>
      <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-card z-10">
            <tr className="text-fg-muted text-xs uppercase tracking-wider">
              <th className="text-left px-3 py-2.5 font-medium">#</th>
              <th className="text-left px-3 py-2.5 font-medium">Ticker</th>
              <th className="text-left px-3 py-2.5 font-medium">Name</th>
              <th className="text-left px-3 py-2.5 font-medium">GuruFocus</th>
              <th className="text-left px-3 py-2.5 font-medium">Sector</th>
              <th className="text-left px-3 py-2.5 font-medium">Location</th>
              <th className="text-right px-3 py-2.5 font-medium">Price</th>
              <th className="text-left px-3 py-2.5 font-medium">Exchange</th>
              <th className="text-left px-3 py-2.5 font-medium">GF Exchange</th>
              <th className="text-left px-3 py-2.5 font-medium">Currency</th>
              <th className="text-left px-3 py-2.5 font-medium">GF Currency</th>
              <th className="text-right px-3 py-2.5 font-medium">Weight</th>
              <th className="text-right px-3 py-2.5 font-medium">Market Value</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/30">
            {feasibleDisplay.map((h, i) => (
              <tr key={`feasible-${h.Ticker}-${h.Exchange}-${i}`} className="hover:bg-overlay/[0.02]">
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
                    <span className="text-fg-faint">&mdash;</span>
                  )}
                </td>
                <td className="px-3 py-2.5 text-fg-muted">{h.Sector}</td>
                <td className="px-3 py-2.5 text-fg-muted">{h.Location}</td>
                <td className="px-3 py-2.5 text-fg-soft font-mono text-right">{fmtNum(h.Price)}</td>
                <td className="px-3 py-2.5 text-fg-muted text-xs">{h.Exchange}</td>
                <td className="px-3 py-2.5 text-accent-400 font-mono text-xs">{h.gf_exchange ?? <span className="text-fg-faint">US</span>}</td>
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
