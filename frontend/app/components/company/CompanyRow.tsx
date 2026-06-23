'use client';

import type { CSSProperties } from 'react';
import Spinner from '../Spinner';
import InfoTip from '../universe/InfoTip';
import OpenFigiBadge from './OpenFigiBadge';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import { fmtMktCapEur, fmtMktCapNative } from './format';
import type { Company } from './types';

/** One non-editing company row: status badges (delisted / out-of-scope /
 * GF-lookup / dupe), the GuruFocus ticker link, the OpenFIGI verification
 * badge, clickable universe chips, and the admin-only Edit/Delete actions. */
export default function CompanyRow({
  company: c,
  isAdmin,
  membershipsLoading,
  sectorsLoading,
  loading,
  duplicateIsins,
  nameDupes,
  deletingId,
  verifyingId,
  onEdit,
  onDelete,
  onFindExchange,
  onFetchGfName,
  onVerifyOpenfigi,
  onToggleUniverse,
  universeStyle,
}: {
  company: Company;
  isAdmin: boolean;
  membershipsLoading: boolean;
  sectorsLoading: boolean;
  /** Table (re)load in flight — the Mkt Cap cell spins (its value arrives with
   * the company row itself, so it has no separate fetch like sector does). */
  loading: boolean;
  duplicateIsins: Set<string>;
  /** `company_id → the other same-name companies` (one side missing an ISIN) —
   * drives the NAME DUPE badge. */
  nameDupes: Map<number, Company[]>;
  deletingId: number | null;
  verifyingId: number | null;
  onEdit: (id: number) => void;
  onDelete: (id: number, name: string) => void;
  onFindExchange: (c: Company) => void;
  onFetchGfName: (c: Company) => void;
  onVerifyOpenfigi: (c: Company) => void;
  onToggleUniverse: (u: string) => void;
  universeStyle: (label: string) => CSSProperties;
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
        {c.isin && duplicateIsins.has(c.isin.trim()) && (
          <span className="ml-2 px-1.5 py-0.5 text-[10px] font-medium bg-warn-500/15 text-warn-400 border border-warn-500/25 rounded" title={`Duplicate — another company has the same ISIN (${c.isin}), i.e. the same security stored twice.`}>
            DUPE
          </span>
        )}
        {nameDupes.has(c.company_id) && (
          <span
            className="ml-2 px-1.5 py-0.5 text-[10px] font-medium bg-warn-500/15 text-warn-300 border border-warn-500/30 rounded cursor-help"
            title={`Likely duplicate by NAME — another row holds the same company but isn't caught by the ISIN check (one side has no ISIN). Also stored as: ${(nameDupes.get(c.company_id) ?? []).map((o) => `#${o.company_id} ${o.gurufocus_exchange}:${o.gurufocus_ticker}${o.isin ? ` (${o.isin})` : ' (no ISIN)'}`).join(', ')}.`}
          >
            NAME DUPE
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
      <td className="px-3 py-2.5">
        <span className="inline-flex items-center gap-1.5">
          <OpenFigiBadge status={c.openfigi_status} name={c.openfigi_name} checkedAt={c.openfigi_checked_at} />
          {isAdmin && (
            verifyingId === c.company_id ? (
              <Spinner size={10} className="h-2.5 w-2.5 text-fg-faint" />
            ) : (
              <button
                type="button"
                onClick={() => onVerifyOpenfigi(c)}
                disabled={verifyingId !== null}
                title="Re-verify this ISIN against OpenFIGI now"
                className="opacity-0 group-hover:opacity-100 text-fg-faint hover:text-fg-strong transition-opacity disabled:opacity-30"
              >
                ⟳
              </button>
            )
          )}
        </span>
      </td>
      <td className="px-3 py-2.5 text-fg-muted">{c.country ?? '—'}</td>
      <td className="px-3 py-2.5 text-fg-muted text-xs">
        {c.sector ? (
          <span className="inline-flex items-center gap-1.5">
            <span>{c.sector}</span>
            {c.sector_source && (
              <span
                style={universeStyle(c.sector_source)}
                title={`Sector from the ${c.sector_source} universe`}
                className="px-1 py-0.5 rounded text-[9px] font-medium border leading-none"
              >
                {c.sector_source.split(' (')[0]}
              </span>
            )}
          </span>
        ) : sectorsLoading ? (
          <Spinner size={10} className="h-2.5 w-2.5 text-fg-faint" />
        ) : (
          '—'
        )}
      </td>
      <td className="px-3 py-2.5 text-right font-mono text-xs text-fg-muted whitespace-nowrap">
        {loading ? (
          <Spinner size={10} className="inline-block h-2.5 w-2.5 text-fg-faint" />
        ) : c.market_cap_eur != null ? (
          <span className="inline-flex items-center justify-end gap-1.5">
            <span>{fmtMktCapEur(c.market_cap_eur)}</span>
            <InfoTip
              text={[
                c.market_cap_date ? `As of ${c.market_cap_date}.` : null,
                c.market_cap_native != null && c.market_cap_currency && c.market_cap_currency !== 'EUR'
                  ? `Native ${fmtMktCapNative(c.market_cap_native, c.market_cap_currency)}, converted at ${c.market_cap_fx_rate} ${c.market_cap_currency}/EUR (ECB rate) → ${fmtMktCapEur(c.market_cap_eur)}.`
                  : 'Quoted in EUR — no FX conversion.',
              ].filter(Boolean).join(' ')}
            />
          </span>
        ) : c.gf_unsubscribed ? (
          <span
            className="px-1.5 py-0.5 text-[10px] font-medium bg-warn-500/15 text-warn-300 border border-warn-500/30 rounded cursor-help"
            title={`${c.gurufocus_exchange} is outside our GuruFocus subscription (India, AU/NZ, Russia, Africa, LatAm) — no market-cap / price / ISIN data is available for this listing.`}
          >
            UNSUBSCRIBED
          </span>
        ) : (
          '—'
        )}
      </td>
      <td className="px-3 py-2.5">
        {(c.universes ?? []).length === 0 ? (
          membershipsLoading ? (
            <Spinner size={10} className="h-2.5 w-2.5 text-fg-faint" />
          ) : (
            <span
              className="px-1.5 py-0.5 text-[10px] font-medium bg-warn-500/15 text-warn-300 border border-warn-500/30 rounded"
              title="Not a member of any frozen universe snapshot. The company is kept (no longer pruned) and flagged here."
            >
              No membership
            </span>
          )
        ) : (
          <div className="flex flex-wrap gap-1">
            {c.universes.map((u) => (
              <button
                key={u}
                onClick={() => onToggleUniverse(u)}
                style={universeStyle(u)}
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
                onClick={() => onFetchGfName(c)}
                disabled={deletingId !== null}
                title="Fetch this listing's name from GuruFocus and (after confirm) correct the row"
                className="px-2.5 py-1 rounded-lg text-xs text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                GF name
              </button>
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
