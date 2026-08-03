'use client';

import { useCallback, useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import { type Basket } from './types';

/**
 * What a portfolio-level fundamentals view can and cannot reach — BY WEIGHT.
 *
 * ⚠ COVERAGE IS THE FIRST ANSWER, NOT A FOOTNOTE. Every holding that cannot be reached is weight
 * that would silently drop out of any blend, and a blended figure over half a book presented as
 * the book's is the same fabrication the AIRS return coverage floor already refuses.
 *
 * ⚠ A COUNT WOULD LIE. Nine covered minnows and one uncovered giant is not 90% coverage.
 */
type Row = {
  isin: string | null; name: string | null; weight_pct: number; reason: string;
  company_name: string | null;
  // The GuruFocus exchange + ticker the company sits on — the reason an `unsubscribed` row is
  // unreachable, the halves of the GuruFocus URL, and blank for a `no_company` row until ingested.
  exchange?: string | null;
  ticker?: string | null;
  // The certificate this stock was looked THROUGH from (a linked Leonteq AMC that IS a model
  // portfolio), if any — so a constituent reads "via Star Selection Index" rather than as a
  // mystery top-level holding.
  via_certificate?: string | null;
};
type Coverage = {
  holdings: number; covered_pct: number;
  by_reason_pct: Record<string, number>; rows: Row[];
};

// The two gaps a GuruFocus fetch can close (mirrors `_fundamental_ingest.INGESTABLE_REASONS`):
// `no_company` (resolve the ISIN, create the company, fetch) and `no_metrics` (company exists,
// just fetch). Everything else is a purchase decision or a category the question doesn't apply
// to — no button, because a fetch could never deliver it.
const INGESTABLE = new Set(['no_company', 'no_metrics']);

// The result of a per-row ingest attempt, kept keyed by ISIN so the badge survives a coverage
// reload (the row may still be present if GuruFocus had no data for it). `exchange`/`ticker` are
// the listing the fetch actually used — the honest answer to "which GF exchange did this go
// through", which a `no_company` row cannot show until it is ingested.
type IngestState = {
  busy?: boolean; status?: string; detail?: string;
  exchange?: string | null; ticker?: string | null;
};
const INGEST_BADGE: Record<string, { label: string; tone: string }> = {
  ingested: { label: '✓ ingested', tone: 'text-pos-400' },
  no_data: { label: 'no data', tone: 'text-warn-300' },
  unsubscribed: { label: 'unsubscribed', tone: 'text-warn-400' },
  not_found: { label: 'not found', tone: 'text-fg-muted' },
  not_equity: { label: 'not an equity', tone: 'text-fg-muted' },
  error: { label: 'failed', tone: 'text-neg-300' },
};

// ⚠ `unsubscribed` and `no_company` are NOT synonyms: one is a purchase decision, the other a
// five-minute ingest. They are worded so the difference survives being skim-read.
const REASON: Record<string, { label: string; note: string; tone: string }> = {
  covered: { label: 'covered', tone: 'text-pos-400',
    note: 'a company row exists and its fundamentals can be fetched.' },
  unsubscribed: { label: 'no GuruFocus subscription', tone: 'text-warn-400',
    note: 'a real company, on an exchange outside our GuruFocus subscription (India, UK, Ireland, Russia, Africa, LatAm, AU/NZ). The data exists and we cannot buy it — the only gap here a purchase would fix.' },
  no_company: { label: 'company not ingested', tone: 'text-warn-300',
    note: 'an equity we hold no company row for. A gap in our own ingest, not in the subscription — fixable by adding it.' },
  no_metrics: { label: 'fundamentals not ingested', tone: 'text-warn-300',
    note: 'the company IS in our database and no fundamentals have been fetched for it. A third remedy again: not a purchase, not adding the company — running the earnings ingest. Measured 2026-07-23: 2,776 company rows, seven with any annual metric.' },
  fund: { label: 'fund (holds companies, is not one)', tone: 'text-fg-muted',
    note: 'an ETF or fund has no income statement of its own. Looking through to its constituents is a different feature, not a gap in this one.' },
  not_equity: { label: 'not an equity', tone: 'text-fg-muted',
    note: 'a bond, future, FX or crypto line. A coupon is not an earnings stream — the question does not apply.' },
  cash: { label: 'cash', tone: 'text-fg-faint', note: 'no ISIN, nothing to look up.' },
};

export default function FundamentalCoverage({ basket, portfolioId, onIngested }: {
  basket?: Basket; portfolioId?: number;
  // Fired after any ingest completes, so the parent can refresh the blended charts (whose data
  // this may have just created). The coverage table reloads itself.
  onIngested?: () => void;
}) {
  const [cov, setCov] = useState<Coverage | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [reloadKey, setReloadKey] = useState(0);
  const [statuses, setStatuses] = useState<Record<string, IngestState>>({});
  const [busyAll, setBusyAll] = useState(false);

  useEffect(() => {
    let alive = true;
    const body = basket
      ? { holdings: basket.holdings.map((h) => ({ isin: h.isin, name: h.name, weight: h.weight })) }
      : { portfolio_id: portfolioId };
    void (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/fundamental-coverage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        if (alive) { setCov((await r.json()) as Coverage); setErr(null); }
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [basket, portfolioId, reloadKey]);

  // One ISIN. Runs the fetch, records its outcome, and leaves the caller to reload.
  const ingestOne = useCallback(async (isin: string, name: string | null) => {
    setStatuses((s) => ({ ...s, [isin]: { busy: true } }));
    try {
      const r = await apiFetch(`${API_URL}/api/earnings/fundamental-coverage/ingest`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ isin, name }),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const j = (await r.json()) as {
        status?: string; detail?: string; exchange?: string | null; ticker?: string | null;
      };
      setStatuses((s) => ({
        ...s,
        [isin]: { status: j.status, detail: j.detail, exchange: j.exchange, ticker: j.ticker },
      }));
    } catch (e) {
      setStatuses((s) => ({
        ...s, [isin]: { status: 'error', detail: e instanceof Error ? e.message : String(e) },
      }));
    }
  }, []);

  const ingestRow = useCallback(async (isin: string, name: string | null) => {
    await ingestOne(isin, name);
    setReloadKey((k) => k + 1);
    onIngested?.();
  }, [ingestOne, onIngested]);

  // Every distinct ingestable ISIN in the table. ⚠ SEQUENTIAL, and deduped by ISIN — a stock
  // held both directly and inside a certificate is ONE fetch, and hammering GuruFocus in parallel
  // is how a bulk run gets throttled into the empty-list failure mode the ingest guards against.
  const ingestAll = useCallback(async (rows: Row[]) => {
    const distinct = new Map<string, Row>();
    for (const r of rows) {
      if (r.isin && INGESTABLE.has(r.reason) && !distinct.has(r.isin)) distinct.set(r.isin, r);
    }
    setBusyAll(true);
    for (const r of distinct.values()) await ingestOne(r.isin as string, r.name);
    setBusyAll(false);
    setReloadKey((k) => k + 1);
    onIngested?.();
  }, [ingestOne, onIngested]);

  if (err) return <p className="text-xs text-neg-300 py-8 text-center">{err}</p>;
  if (!cov) return <p className="text-xs text-fg-subtle py-8 text-center">Checking coverage…</p>;

  // A row drops out when it is covered OR when its ingest just succeeded. The second clause is
  // belt-and-braces on top of the reload: a reload reclassifies a covered holding away, but an
  // ingested listing that happens to lack the exact coverage sentinel metric would otherwise
  // linger as `no_metrics` — the user's mental model is "I ingested it, it's done, remove it".
  // Keyed on ISIN, so a holding shown twice (directly + via a certificate) disappears in both.
  const excluded = cov.rows.filter((r) =>
    r.reason !== 'covered' && !(r.isin && statuses[r.isin]?.status === 'ingested'));
  // ⚠ `unsubscribed` IS THE ONE GAP A PURCHASE WOULD FIX — a real company on an exchange outside
  // our GuruFocus subscription, not a hole in our own ingest. It is surfaced FIRST, in its own
  // section, so the reader can tell "we cannot buy this" from "we have not ingested this" at a
  // glance. Every other exclusion (not ingested / no metrics / fund / …) is a gap on our side.
  const subscription = excluded.filter((r) => r.reason === 'unsubscribed');
  const others = excluded.filter((r) => r.reason !== 'unsubscribed');
  const sumW = (rs: Row[]) => rs.reduce((a, r) => a + r.weight_pct, 0);

  return (
    <div className="space-y-4">
      {/* ⚠ The covered-weight headline + reason bar were removed on request. What remains is the
          EXCLUSIONS table, which is the load-bearing half: a reader can see a chart is blended
          over less than the whole book only if the missing holdings are named. The share of
          weight is still stated per section, so nothing that qualifies a blended figure is lost. */}
      {excluded.length > 0 && (
        <div className="space-y-4">
          {subscription.length > 0 && (
            <ExclusionSection
              title="Outside GuruFocus subscription"
              weight={sumW(subscription)}
              rows={subscription}
              statuses={statuses} busyAll={busyAll}
              onIngest={ingestRow} onIngestAll={ingestAll} />
          )}
          {others.length > 0 && (
            <ExclusionSection
              // No subscription gap → keep the original single-table heading.
              title={subscription.length > 0 ? 'Other gaps' : 'Not included'}
              weight={sumW(others)}
              rows={others}
              statuses={statuses} busyAll={busyAll}
              onIngest={ingestRow} onIngestAll={ingestAll} />
          )}
        </div>
      )}
    </div>
  );
}

function ExclusionSection({ title, weight, rows, statuses, busyAll, onIngest, onIngestAll }: {
  title: string; weight: number; rows: Row[];
  statuses: Record<string, IngestState>; busyAll: boolean;
  onIngest: (isin: string, name: string | null) => void;
  onIngestAll: (rows: Row[]) => void;
}) {
  const ingestable = rows.filter((r) => r.isin && INGESTABLE.has(r.reason));
  const distinctIngestable = new Set(ingestable.map((r) => r.isin));
  const anyBusy = busyAll || ingestable.some((r) => statuses[r.isin as string]?.busy);

  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between gap-3">
        <h4 className="text-xs font-semibold text-fg-strong">
          {title} ({weight.toFixed(1)}% of weight)
        </h4>
        {distinctIngestable.size > 0 && (
          <button
            type="button"
            onClick={() => onIngestAll(rows)}
            disabled={anyBusy}
            title="Fetch the GuruFocus fundamentals for every ingestable holding in this section."
            className="text-[11px] px-2 py-0.5 rounded-lg border border-accent-600/40 text-accent-400 hover:bg-overlay/5 disabled:opacity-50"
          >
            {busyAll ? 'Ingesting…' : `Ingest all (${distinctIngestable.size})`}
          </button>
        )}
      </div>
      <div className="overflow-auto rounded-lg border border-neutral-800/40">
        <table className="w-auto text-xs whitespace-nowrap">
          <thead className="bg-card">
            <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
              <th className="px-3 py-1.5 font-medium text-left">Instrument</th>
              <th className="px-3 py-1.5 font-medium text-right">Weight</th>
              <th className="px-3 py-1.5 font-medium text-left">Why not</th>
              <th className="px-3 py-1.5 font-medium text-left">GF exchange</th>
              <th className="px-3 py-1.5 font-medium text-left">Ticker</th>
              {ingestable.length > 0 && <th className="px-3 py-1.5 font-medium text-left">Ingest</th>}
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {rows.map((r, i) => {
              const st = r.isin ? statuses[r.isin] : undefined;
              const canIngest = !!r.isin && INGESTABLE.has(r.reason);
              // Prefer what the ingest just resolved over the (possibly-blank) coverage values.
              const gfExchange = st?.exchange || r.exchange;
              const gfTicker = st?.ticker || r.ticker;
              return (
                // ⚠ INDEXED KEY — look-through can surface the same ISIN twice (a stock held both
                // directly and inside a linked certificate), so `isin` alone is no longer unique.
                <tr key={`${r.isin ?? r.name}-${r.via_certificate ?? ''}-${i}`}
                  className="hover:bg-overlay/[0.02]">
                  <td className="px-3 py-1.5 text-fg-soft">
                    <span className="inline-block max-w-[28ch] truncate align-bottom"
                      title={r.name ?? ''}>{r.name ?? '—'}</span>
                    {r.isin && <span className="text-fg-faint font-mono text-[10px] ml-2">{r.isin}</span>}
                    {r.via_certificate && (
                      <span className="text-fg-faint text-[10px] ml-2 italic"
                        title={`Looked through the linked certificate "${r.via_certificate}".`}>
                        via {r.via_certificate}
                      </span>
                    )}
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                    {r.weight_pct.toFixed(2)}%
                  </td>
                  <td className="px-3 py-1.5" title={REASON[r.reason]?.note}>
                    {r.reason === 'unsubscribed' ? (
                      // A subscription gap is a hard "can't buy this" — a badge, not prose, so it
                      // reads as a distinct state at a glance (mirrors the /companies UNSUBSCRIBED
                      // badge).
                      <span className="inline-block text-[9px] font-medium uppercase tracking-wide px-1.5 py-0.5 rounded border bg-warn-500/15 text-warn-300 border-warn-500/25">
                        Unsubscribed
                      </span>
                    ) : (
                      <span className={REASON[r.reason]?.tone ?? 'text-fg-muted'}>
                        {REASON[r.reason]?.label ?? r.reason}
                      </span>
                    )}
                  </td>
                  <td className="px-3 py-1.5 font-mono text-[11px] text-fg-subtle">
                    {/* Prefer the listing the ingest actually resolved to — a `no_company` row has
                        no exchange/ticker on the coverage side until then. */}
                    {gfExchange || <span className="text-fg-faint">—</span>}
                  </td>
                  <td className="px-3 py-1.5 font-mono text-[11px]">
                    {gfTicker ? (
                      // The GuruFocus summary page for this listing. Same URL rule the rest of the
                      // app uses (US names go bare, everything else exchange-prefixed).
                      <a href={guruFocusUrl(gfTicker, gfExchange)}
                        target="_blank" rel="noopener noreferrer"
                        className="text-accent-400 hover:underline"
                        title="Open the GuruFocus summary page">
                        {gfTicker} ↗
                      </a>
                    ) : <span className="text-fg-faint">—</span>}
                  </td>
                  {ingestable.length > 0 && (
                    <td className="px-3 py-1.5">
                      {canIngest && (
                        st?.busy ? (
                          <span className="text-[10px] text-fg-faint">ingesting…</span>
                        ) : st?.status ? (
                          <span className="inline-flex items-center gap-1.5">
                            <span className={`text-[10px] ${INGEST_BADGE[st.status]?.tone ?? 'text-fg-muted'}`}
                              title={st.detail}>
                              {INGEST_BADGE[st.status]?.label ?? st.status}
                            </span>
                            {/* A row that didn't succeed can be retried (e.g. after a throttle). */}
                            {st.status !== 'ingested' && (
                              <button type="button" onClick={() => onIngest(r.isin as string, r.name)}
                                disabled={busyAll}
                                className="text-[10px] text-accent-400 hover:underline disabled:opacity-50">
                                retry
                              </button>
                            )}
                          </span>
                        ) : (
                          <button
                            type="button"
                            onClick={() => onIngest(r.isin as string, r.name)}
                            disabled={busyAll}
                            title="Fetch this holding's GuruFocus fundamentals and load them."
                            className="text-[11px] px-2 py-0.5 rounded-lg border border-accent-600/40 text-accent-400 hover:bg-overlay/5 disabled:opacity-50"
                          >
                            Ingest
                          </button>
                        )
                      )}
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
