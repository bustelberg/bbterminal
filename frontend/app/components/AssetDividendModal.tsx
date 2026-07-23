'use client';

import { useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { chartTheme } from '../../lib/chartTheme';
import type { AssetGridRow, DividendCoverageEntry, DividendPaymentsResponse } from '../../lib/types/api';
import LwLineChart from './LwLineChart';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? '';

type PayMode = 'each' | 'ttm';
type Point = { date: string; value: number };

// Why a resolved ISIN still can't be charted — mirrors the backend's resolver
// statuses (routers/_gf_listing.py). Both negative-cached: the API call is spent.
const UNRESOLVED: Record<string, string> = {
  not_found: 'GuruFocus does not know this ISIN, so there is no listing to price.',
  unsubscribed: 'GuruFocus lists this ISIN only on exchanges outside our subscription.',
};

/** Cash paid per unit held — ONE primitive, ONE chart, for every instrument.
 *
 * A stock and an ETF have exactly one thing in common on this axis, and it is all we
 * need: a timeseries of (date, cash per unit). GuruFocus's `stock/{sym}/dividend`
 * returns precisely that for both, in the DECLARATION currency, retro-adjusted for
 * splits so the whole history sits on today's share basis.
 *
 * The fiscal-period cadences this modal used to offer (annual / quarterly, from
 * `financials`) are GONE from the UI. They were a company-only detour: DERIVED (the
 * payments summed inside a fiscal year), unavailable for any ETF, and lagging by up to
 * a year — NVIDIA's $0.01 → $0.25 hike sat invisible inside FY2027 until 2027. The
 * payment feed shows it the day it's declared. The endpoints still exist on the API
 * surface; nothing here calls them.
 *
 * TWO BRIDGES to reach that one series, because the grid holds two kinds of instrument:
 *   company-backed — ISIN → `company` → GuruFocus. Equities we ingest.
 *   listing-backed — ISIN → GuruFocus `isin/{ISIN}`. ETFs, which have no `company` row.
 * An unresolved ISIN is resolved on open (one API call, cached forever, misses included).
 *
 * NATIVE | EUR, the only two charts. Each payment converts at the FX rate on ITS OWN pay
 * date, so the EUR line carries the currency leg. Points older than our `fx_rate`
 * coverage are omitted from the EUR panel — never drawn as zero. */
export default function AssetDividendModal({
  row, isin, entry, onClose, onResolved,
}: {
  row: AssetGridRow;
  isin: string;
  /** Undefined when this ISIN has never been resolved — the modal resolves it. */
  entry?: DividendCoverageEntry;
  onClose: () => void;
  /** Hands the resolved listing + payout status back so the grid's cells update. */
  onResolved?: (entry: DividendCoverageEntry) => void;
}) {
  const [resolved, setResolved] = useState<DividendCoverageEntry | undefined>(entry);
  const [resolving, setResolving] = useState(!entry);
  const [payments, setPayments] = useState<DividendPaymentsResponse | null>(null);
  const [payMode, setPayMode] = useState<PayMode>('each');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const companyId = resolved?.company_id ?? null;
  const blocked = resolved?.status && resolved.status !== 'ok' ? resolved.status : null;

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const onResolvedRef = useRef(onResolved);
  onResolvedRef.current = onResolved;
  const resolvedRef = useRef(resolved);
  resolvedRef.current = resolved;

  // Resolve-on-open. Only when the grid had no entry for this ISIN — i.e. we have
  // never asked GuruFocus about it. One API call, cached server-side forever.
  useEffect(() => {
    if (entry) return;
    let cancelled = false;
    (async () => {
      setResolving(true); setError(null);
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/dividends/isin/${encodeURIComponent(isin)}/resolve`,
          { method: 'POST' });
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        const e = b as DividendCoverageEntry;
        setResolved(e);
        onResolvedRef.current?.(e);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setResolving(false);
      }
    })();
    return () => { cancelled = true; };
  }, [isin, entry]);

  // THE only data load. Company-backed rows go through their company_id (that bridge
  // also validates the ticker); everything else through the ISIN.
  useEffect(() => {
    if (blocked || !resolved) return;
    const url = companyId != null
      ? `${API_URL}/api/asset-pipeline/dividends/${companyId}/payments`
      : `${API_URL}/api/asset-pipeline/dividends/isin/${encodeURIComponent(isin)}/payments`;
    let cancelled = false;
    (async () => {
      setLoading(true); setError(null);
      try {
        const r = await apiFetch(url);
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        const p = b as DividendPaymentsResponse;
        setPayments(p);
        // Tell the grid whether this instrument pays anything, so its cell can flip to
        // "NO PAYOUTS" instead of still offering a Fetch. The backend persisted the
        // same fact; this just saves a coverage reload. `resolved` is read through a
        // ref, not the dep array — putting it there would re-run this effect the moment
        // the ISIN resolves, and the cleanup would cancel the request it just started.
        const entryNow = resolvedRef.current;
        if (entryNow) onResolvedRef.current?.({ ...entryNow, has_payments: p.payments.length > 0 });
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [isin, companyId, resolved, blocked]);

  const rows = payments?.payments ?? [];
  // `each` plots the payment itself; `ttm` the trailing annual total. Same two charts —
  // `each` answers "did it just change?", `ttm` answers "what does one unit pay a year?"
  // and is the one that compares like-for-like across instruments and frequencies.
  const dates = rows.map((p) => p.date);
  const nativeVals = rows.map((p) => (payMode === 'ttm' ? p.ttm : p.value));
  const eurVals = rows.map((p) => (payMode === 'ttm' ? p.ttm_eur : p.value_eur));

  const native: Point[] = dates.flatMap((d, i) => (nativeVals[i] == null ? [] : [{ date: d, value: nativeVals[i]! }]));
  const eur: Point[] = dates.flatMap((d, i) => (eurVals[i] == null ? [] : [{ date: d, value: eurVals[i]! }]));
  const droppedPreFx = native.length - eur.length;

  const ccy = (payments?.currency ?? '').toUpperCase();
  const isEurNative = ccy === 'EUR';
  const busy = resolving || loading;
  const empty = !busy && !blocked && rows.length === 0;
  const lastNative = native.at(-1);
  const lastEur = eur.at(-1);
  const latestTtm = rows.at(-1)?.ttm;
  const notHome = (payments?.is_home ?? resolved?.is_home) === false;
  const listingOnly = !!resolved && companyId == null;

  const panel = (label: string, data: Point[], color: string, unit: string) => (
    <div className="flex-1 min-w-[320px]">
      <div className="text-[10px] uppercase tracking-wide text-fg-faint mb-1">{label}</div>
      {data.length > 0 ? (
        <LwLineChart data={data} scale="linear" unit={unit} color={color} />
      ) : (
        <div className="w-full aspect-[16/9] max-h-[72vh] min-h-[300px] flex items-center justify-center text-center px-6 text-[11px] text-fg-faint border border-neutral-800/40 rounded-lg">
          {payments?.fx_from
            ? <>Every payment predates our FX coverage (from {payments.fx_from}).</>
            : <>No FX rate available to convert {ccy || 'this currency'}.</>}
        </div>
      )}
    </div>
  );

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-scrim/60"
      onClick={onClose} role="dialog" aria-modal="true">
      <div className="bg-card border border-neutral-800/40 rounded-xl shadow-xl w-[80vw] h-[80vh] overflow-auto p-4"
        onClick={(e) => e.stopPropagation()}>

        <div className="flex items-start justify-between gap-3 mb-3">
          <div className="min-w-0">
            <div className="flex items-baseline gap-2 flex-wrap">
              <span className="text-base font-semibold text-fg-strong">Cash paid per unit held</span>
              <span className="text-sm font-mono text-fg-soft">{isin}</span>
              {row.name && <span className="text-sm text-fg-soft truncate">{row.name}</span>}
            </div>
            <div className="text-[11px] text-fg-faint mt-0.5">
              GuruFocus
              {payments?.symbol && ` · ${payments.symbol}`}
              {companyId != null ? ` · company #${companyId}` : listingOnly ? ' · listing (no company row)' : ''}
              {ccy && ` · paid in ${ccy}`}
              {lastNative && ` · latest ${fmt(lastNative.value)} ${ccy}`}
              {lastEur && !isEurNative && ` = ${fmt(lastEur.value)} EUR`}
              {lastNative && ` (${lastNative.date})`}
              {latestTtm != null && ` · trailing 12m ${fmt(latestTtm)} ${ccy}`}
            </div>
          </div>
          <div className="flex items-center gap-2">
            {!blocked && rows.length > 0 && (
              <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5"
                title="Each payment as declared, or the trailing annual total (the sum of the last k payments). The trailing total is the one that compares across instruments — a quarterly payer's single payment is a quarter of an annual payer's.">
                {(['each', 'ttm'] as PayMode[]).map((m) => (
                  <button key={m} type="button" onClick={() => setPayMode(m)}
                    className={`text-[11px] px-2.5 py-1 rounded-md transition-colors ${
                      payMode === m ? 'bg-accent-600 text-white' : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/[0.04]'}`}>
                    {m === 'each' ? 'per payment' : 'trailing 12m'}
                  </button>
                ))}
              </div>
            )}
            <button type="button" onClick={onClose} aria-label="Close"
              className="text-fg-faint hover:text-fg-strong text-xl leading-none px-1 -mt-1">×</button>
          </div>
        </div>

        {error && <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300 mb-3">{error}</div>}

        {/* Charted off a listing that is NOT this row's own. The AMOUNTS are right —
            GuruFocus reports a payout in its declaration currency on every listing of
            the ISIN (Apple: 0.27 USD on Nasdaq, Xetra, Zurich and Milan alike,
            measured). The HISTORY is what may be short: Milan holds 35 of Apple's 91
            payments, Zurich 63 with a five-year hole. Warn, don't hide — and the
            trailing-12m line is suppressed wherever its window would span a gap. */}
        {!busy && !blocked && notHome && rows.length > 0 && (
          <div className="bg-warn-500/10 border border-warn-500/20 rounded-lg px-3 py-2 text-[11px] text-warn-300 mb-3">
            <span className="font-semibold">Not this row’s own listing.</span>{' '}
            GuruFocus has no {row.currency ?? 'local'} line for this ISIN, so these payments come
            from {payments?.symbol ?? 'another listing'}. Same security, same per-unit amounts
            (GuruFocus reports the declaration currency on every listing) — but its history may be{' '}
            <strong>incomplete</strong>, and a trailing-12m total is hidden wherever its window
            would span a gap.
          </div>
        )}

        {busy && (
          <p className="text-[11px] text-fg-subtle py-16 text-center">
            {resolving ? 'Resolving this ISIN with GuruFocus…' : 'Loading payments…'}
          </p>
        )}

        {!busy && blocked && (
          <div className="py-16 text-center max-w-2xl mx-auto space-y-2">
            <p className="text-sm text-fg-soft">No payment history available for this ISIN.</p>
            <p className="text-[11px] text-fg-faint">{UNRESOLVED[blocked] ?? `Unresolved (${blocked}).`}</p>
          </div>
        )}

        {/* Zero payments is an ANSWER, not a gap — and for an accumulating fund it's the
            correct one. Say which, rather than showing an empty chart that reads as a
            missing fetch. */}
        {empty && !error && (
          <div className="py-16 text-center max-w-2xl mx-auto space-y-2">
            <p className="text-sm text-fg-soft">This {listingOnly ? 'fund' : 'company'} pays nothing out.</p>
            <p className="text-[11px] text-fg-faint">
              GuruFocus returned no payments at all for {payments?.symbol ?? 'this listing'}.
              {listingOnly
                ? ' An ACCUMULATING ETF reinvests its income into NAV instead of distributing it, so an empty history here is the answer — not a data gap.'
                : ' This company has never paid a dividend.'}
            </p>
          </div>
        )}

        {!busy && !blocked && rows.length > 0 && (
          <div className="space-y-1">
            {/* A payout already in EUR needs no conversion; a second identical panel
                would be noise, so it gets the full width. */}
            {isEurNative
              ? <div className="flex gap-4">{panel('EUR', native, chartTheme.accentStrong, ccy)}</div>
              : (
                <div className="flex gap-4 flex-wrap">
                  {panel(`Native (${ccy})`, native, chartTheme.accentStrong, ccy)}
                  {panel('EUR', eur, chartTheme.pos, 'EUR')}
                </div>
              )}
            <div className="text-[10px] text-fg-faint">
              {native.length} {payMode === 'ttm' ? 'trailing totals' : 'payments'}
              {' · '}{dates[0]} → {dates.at(-1)}
              {' · '}Cash per unit held, as declared. Split-adjusted to today’s share basis, so the
              whole history is on one footing.
              {listingOnly && ' An ETF’s payout is a DISTRIBUTION: it can include return of capital and capital gains, not only dividend income.'}
              {!isEurNative && ' · Right: converted at the ECB rate on each payment’s own pay date, so the line carries the FX leg.'}
              {droppedPreFx > 0 && payments?.fx_from &&
                ` · ${droppedPreFx} earlier point${droppedPreFx > 1 ? 's' : ''} omitted from EUR: no rate before ${payments.fx_from}.`}
              {' · '}Scroll to zoom, drag to pan, double-click to reset.
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function fmt(v: number): string {
  return v.toLocaleString(undefined, { maximumFractionDigits: 4 });
}
