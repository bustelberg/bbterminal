'use client';

import Spinner from '../Spinner';
import InfoTip from '../InfoTip';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import { universeChipStyle } from './styles';
import type { Company } from './types';

/** Format an absolute EUR market cap compactly (€3.95T / €420.5B / €88.0M). */
function fmtMktCapEur(v: number): string {
  if (v >= 1e12) return `€${(v / 1e12).toFixed(2)}T`;
  if (v >= 1e9) return `€${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `€${(v / 1e6).toFixed(1)}M`;
  return `€${Math.round(v).toLocaleString()}`;
}

/** One non-editing company row: status badges (delisted / out-of-scope /
 * GF-lookup / dupe), the GuruFocus ticker link, clickable universe chips,
 * and the admin-only Edit/Delete actions. */
export default function CompanyRow({
  company: c,
  isAdmin,
  membershipsLoading,
  duplicateNames,
  deletingId,
  onEdit,
  onDelete,
  onFindExchange,
  onToggleUniverse,
}: {
  company: Company;
  isAdmin: boolean;
  membershipsLoading: boolean;
  duplicateNames: Set<string>;
  deletingId: number | null;
  onEdit: (id: number) => void;
  onDelete: (id: number, name: string) => void;
  onFindExchange: (c: Company) => void;
  onToggleUniverse: (u: string) => void;
}) {
  return (
    <tr className="border-b border-neutral-800/30 hover:bg-overlay/[0.02] transition-colors group">
      <td className="px-4 py-2.5 text-fg-faint text-xs">{c.company_id}</td>
      <td className={`px-3 py-2.5 font-medium ${c.delisted_at ? 'text-fg-subtle' : 'text-fg'}`}>
        <span className={c.delisted_at ? 'line-through' : ''}>{c.company_name ?? '—'}</span>
        {c.delisted_at && (
          <span
            className="ml-2 px-1.5 py-0.5 text-[10px] font-medium bg-neg-500/15 text-neg-300 border border-neg-500/25 rounded"
            title={`Marked delisted on ${new Date(c.delisted_at).toLocaleString()} — GuruFocus returned no fetchable data. Excluded from backtests.`}
          >
            DELISTED
          </span>
        )}
        {c.out_of_scope_at && !c.delisted_at && (
          <span
            className="ml-2 px-1.5 py-0.5 text-[10px] font-medium bg-warn-500/15 text-warn-300 border border-warn-500/30 rounded"
            title={`Out of scope: ${c.out_of_scope_reason ?? '(no reason given)'}. Marked ${new Date(c.out_of_scope_at).toLocaleString()}. Excluded from universe membership and skipped by the price phase — see backend/index_universe/gf_ticker_overrides.json.`}
          >
            OUT OF SCOPE
          </span>
        )}
        {c.gurufocus_lookup_failed_at && !c.delisted_at && !c.out_of_scope_at && (
          <button
            type="button"
            onClick={() => onFindExchange(c)}
            className="ml-2 px-1.5 py-0.5 text-[10px] font-medium bg-neg-500/15 text-neg-300 border border-neg-500/25 rounded hover:bg-neg-500/25 hover:text-neg-200 transition-colors cursor-pointer"
            title={`GuruFocus returned "Stock not found" on the primary exchange + every fallback as of ${new Date(c.gurufocus_lookup_failed_at).toLocaleString()}. Likely the exchange on this row is wrong. Click to probe GuruFocus for the correct exchange.`}
          >
            GF LOOKUP
          </button>
        )}
        {c.company_name && duplicateNames.has(c.company_name.toLowerCase().trim()) && (
          <span className="ml-2 px-1.5 py-0.5 text-[10px] font-medium bg-warn-500/15 text-warn-400 border border-warn-500/25 rounded" title="Duplicate company name">
            DUPE
          </span>
        )}
        {c.market_cap_eur != null && (
          <span className="ml-1.5 inline-flex align-middle">
            <InfoTip text={`Market cap: ${fmtMktCapEur(c.market_cap_eur)}${c.market_cap_date ? `\nas of ${c.market_cap_date} · converted to EUR` : ''}`} />
          </span>
        )}
      </td>
      <td className="px-3 py-2.5">
        <a
          href={guruFocusUrl(c.gurufocus_ticker, c.gurufocus_exchange)}
          target="_blank"
          rel="noopener noreferrer"
          className="text-accent-400 hover:text-accent-300 hover:underline transition-colors"
        >
          {c.gurufocus_ticker}
        </a>
      </td>
      <td className="px-3 py-2.5 text-fg-muted">{c.gurufocus_exchange}</td>
      <td className="px-3 py-2.5 text-fg-muted font-mono text-xs">{c.isin ?? '—'}</td>
      <td className="px-3 py-2.5 text-fg-muted">{c.country ?? '—'}</td>
      <td className="px-3 py-2.5 text-fg-muted text-xs">{c.sector ?? '—'}</td>
      <td className="px-3 py-2.5">
        {(c.universes ?? []).length === 0 ? (
          membershipsLoading ? (
            <Spinner size={10} className="h-2.5 w-2.5 text-fg-faint" />
          ) : (
            <span className="text-xs text-fg-faint">—</span>
          )
        ) : (
          <div className="flex flex-wrap gap-1">
            {c.universes.map((u) => (
              <button
                key={u}
                onClick={() => onToggleUniverse(u)}
                style={universeChipStyle(u)}
                title={`Filter by ${u}`}
                className="px-1.5 py-0.5 rounded text-[10px] font-medium border hover:brightness-125 transition"
              >
                {u}
              </button>
            ))}
          </div>
        )}
      </td>
      <td className="px-3 py-2.5">
        {isAdmin && (
          deletingId === c.company_id ? (
            <span className="inline-flex items-center gap-1.5 text-xs text-neg-400">
              <Spinner size={12} className="h-3 w-3 text-neg-400" />
              Deleting…
            </span>
          ) : (
            <div className="flex gap-1.5 opacity-0 group-hover:opacity-100 transition-opacity">
              <button
                onClick={() => onEdit(c.company_id)}
                disabled={deletingId !== null}
                className="px-2.5 py-1 rounded-lg text-xs text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Edit
              </button>
              <button
                onClick={() => onDelete(c.company_id, c.company_name ?? c.gurufocus_ticker)}
                disabled={deletingId !== null}
                className="px-2.5 py-1 rounded-lg text-xs text-fg-faint hover:text-neg-400 hover:bg-neg-500/10 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Delete
              </button>
            </div>
          )
        )}
      </td>
    </tr>
  );
}
