'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { chartTheme } from '../../lib/chartTheme';
import type { AssetGridRow, DividendPaymentsResponse, DividendSeriesResponse } from '../../lib/types/api';
import LwLineChart from './LwLineChart';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? '';

type Cadence = 'annual' | 'quarterly' | 'payments';
type PayMode = 'each' | 'ttm';
type Point = { date: string; value: number };

/** Dividends-per-share for one grid row, from GuruFocus.
 *
 * The row is a Yahoo asset; dividends are keyed by GuruFocus `company_id`. The
 * bridge is the ISIN, resolved server-side.
 *
 * THREE CADENCES, and the third is not a convenience:
 *   annual/quarterly — fiscal-period totals from `financials`. A period only gains
 *     a point once it CLOSES, so a mid-year hike is invisible for up to a year.
 *     NVIDIA went from $0.01 to $0.25 per quarter with an ex-date of 2026-06-04,
 *     inside FY2027; the annual chart correctly reads $0.04 (FY2026) and cannot
 *     show the hike until 2027.
 *   payments — the live per-payment feed. Shows the $0.25 the day it is declared.
 *
 * FETCH-ON-OPEN: if nothing is stored, the modal pulls from GuruFocus immediately.
 *
 * NATIVE | EUR side by side, mirroring `AssetDualChart` on the price modal. Each
 * payment converts at the FX rate on its own pay date, so the EUR panel carries the
 * currency leg. Points older than our `fx_rate` coverage are omitted from the EUR
 * panel — never drawn as zero. */
export default function AssetDividendModal({
  row, companyId, onClose, onFetched,
}: {
  row: AssetGridRow;
  companyId: number;
  onClose: () => void;
  /** Lets the grid flip this row's `has_data` without refetching the coverage map. */
  onFetched?: () => void;
}) {
  const [series, setSeries] = useState<DividendSeriesResponse | null>(null);
  const [payments, setPayments] = useState<DividendPaymentsResponse | null>(null);
  const [cadence, setCadence] = useState<Cadence>('annual');
  const [payMode, setPayMode] = useState<PayMode>('each');
  const [loading, setLoading] = useState(true);
  const [fetching, setFetching] = useState(false);
  const [loadingPayments, setLoadingPayments] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // One auto-fetch per mount. Without this, a fetch that legitimately returns an
  // empty series (a company that has never paid a dividend) would re-fetch forever.
  const autoFetched = useRef(false);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const pull = useCallback(async () => {
    setFetching(true); setError(null);
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/dividends/${companyId}/fetch`, { method: 'POST' });
      const b = await r.json().catch(() => null);
      if (!r.ok) setError(b?.detail ?? `HTTP ${r.status}`);
      else { setSeries(b as DividendSeriesResponse); onFetched?.(); }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setFetching(false); }
  }, [companyId, onFetched]);

  // `pull` changes identity whenever the parent re-renders (its `onFetched` is an
  // inline arrow). Reading it through a ref keeps this effect keyed on `companyId`
  // alone, so a successful fetch doesn't trigger a second GET.
  const pullRef = useRef(pull);
  pullRef.current = pull;

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true); setError(null);
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/dividends/${companyId}`);
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        const s = b as DividendSeriesResponse;
        setSeries(s);
        if (!s.annual.length && !s.quarterly.length && !autoFetched.current) {
          autoFetched.current = true;
          void pullRef.current();
        }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [companyId]);

  // The payment feed is a separate GuruFocus endpoint; only pay for it when asked.
  //
  // The request-once guard is a REF, not `loadingPayments` state. With state in the
  // dep array, setting it re-runs the effect, whose cleanup flips `cancelled` and
  // discards the in-flight response — the spinner then never clears.
  const paymentsRequested = useRef(false);
  useEffect(() => { paymentsRequested.current = false; setPayments(null); }, [companyId]);

  useEffect(() => {
    if (cadence !== 'payments' || paymentsRequested.current) return;
    paymentsRequested.current = true;
    let cancelled = false;
    (async () => {
      setLoadingPayments(true); setError(null);
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/dividends/${companyId}/payments`);
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); paymentsRequested.current = false; }
        else setPayments(b as DividendPaymentsResponse);
      } catch (e) {
        if (!cancelled) { setError(e instanceof Error ? e.message : String(e)); paymentsRequested.current = false; }
      } finally {
        if (!cancelled) setLoadingPayments(false);
      }
    })();
    return () => { cancelled = true; };
  }, [cadence, companyId]);

  const isPayments = cadence === 'payments';
  const rawPeriods = (cadence === 'annual' ? series?.annual : series?.quarterly) ?? [];
  const rawPayments = payments?.payments ?? [];

  // `each` plots the payment; `ttm` plots the trailing annual total. The trailing
  // line is what compares like-for-like with the annual chart.
  const nativeVals = isPayments
    ? rawPayments.map((p) => (payMode === 'ttm' ? p.ttm : p.value))
    : rawPeriods.map((p) => p.value);
  const eurVals = isPayments
    ? rawPayments.map((p) => (payMode === 'ttm' ? p.ttm_eur : p.value_eur))
    : rawPeriods.map((p) => p.value_eur);
  const dates = isPayments ? rawPayments.map((p) => p.date) : rawPeriods.map((p) => p.date);

  const native: Point[] = dates.flatMap((d, i) => (nativeVals[i] == null ? [] : [{ date: d, value: nativeVals[i]! }]));
  const eur: Point[] = dates.flatMap((d, i) => (eurVals[i] == null ? [] : [{ date: d, value: eurVals[i]! }]));
  const droppedPreFx = native.length - eur.length;

  // Fall back to `series` while the payments feed loads, so switching cadence
  // doesn't blank the currency out of the header.
  const src = (isPayments ? payments : series) ?? series;
  const ccy = (src?.currency ?? '').toUpperCase();
  const isEurNative = ccy === 'EUR';
  const busy = loading || fetching || (isPayments && loadingPayments);
  const empty = !busy && dates.length === 0;
  const lastNative = native.at(-1);
  const lastEur = eur.at(-1);
  const latestTtm = rawPayments.at(-1)?.ttm;

  const panel = (label: string, data: Point[], color: string, unit: string) => (
    <div className="flex-1 min-w-[320px]">
      <div className="text-[10px] uppercase tracking-wide text-fg-faint mb-1">{label}</div>
      {data.length > 0 ? (
        <LwLineChart data={data} scale="linear" unit={unit} color={color} />
      ) : (
        <div className="w-full aspect-[16/9] max-h-[72vh] min-h-[300px] flex items-center justify-center text-center px-6 text-[11px] text-fg-faint border border-neutral-800/40 rounded-lg">
          {src?.fx_from
            ? <>Every period predates our FX coverage (from {src.fx_from}).</>
            : <>No FX rate available to convert {ccy || 'this currency'}.</>}
        </div>
      )}
    </div>
  );

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-scrim/60"
      onClick={onClose} role="dialog" aria-modal="true">
      <div className="bg-card border border-neutral-800/40 rounded-xl shadow-xl w-[98vw] max-w-[1900px] max-h-[94vh] overflow-auto p-4"
        onClick={(e) => e.stopPropagation()}>

        <div className="flex items-start justify-between gap-3 mb-3">
          <div className="min-w-0">
            <div className="flex items-baseline gap-2 flex-wrap">
              <span className="text-base font-semibold text-fg-strong">Dividends per share</span>
              <span className="text-sm font-mono text-fg-soft">{row.isin}</span>
              {row.name && <span className="text-sm text-fg-soft truncate">{row.name}</span>}
            </div>
            <div className="text-[11px] text-fg-faint mt-0.5">
              GuruFocus · company #{companyId}
              {ccy && ` · reported in ${ccy}`}
              {lastNative && ` · latest ${fmt(lastNative.value)} ${ccy}`}
              {lastEur && !isEurNative && ` = ${fmt(lastEur.value)} EUR`}
              {lastNative && ` (${lastNative.date})`}
              {latestTtm != null && ` · trailing 12m ${fmt(latestTtm)} ${ccy}`}
            </div>
          </div>
          <div className="flex items-center gap-2">
            {isPayments && (
              <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5"
                title="Each payment, or the trailing annual total (sum of the last k payments).">
                {(['each', 'ttm'] as PayMode[]).map((m) => (
                  <button key={m} type="button" onClick={() => setPayMode(m)}
                    className={`text-[11px] px-2.5 py-1 rounded-md transition-colors ${
                      payMode === m ? 'bg-accent-600 text-white' : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/[0.04]'}`}>
                    {m === 'each' ? 'per payment' : 'trailing 12m'}
                  </button>
                ))}
              </div>
            )}
            <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5">
              {(['annual', 'quarterly', 'payments'] as Cadence[]).map((c) => (
                <button key={c} type="button" onClick={() => setCadence(c)}
                  title={c === 'payments'
                    ? 'Every declared cash payment — shows a mid-year dividend change immediately'
                    : `Fiscal-${c === 'annual' ? 'year' : 'quarter'} totals; a period appears only once it closes`}
                  className={`text-[11px] px-2.5 py-1 rounded-md transition-colors ${
                    cadence === c ? 'bg-accent-600 text-white' : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/[0.04]'}`}>
                  {c}
                </button>
              ))}
            </div>
            <button type="button" onClick={onClose} aria-label="Close"
              className="text-fg-faint hover:text-fg-strong text-xl leading-none px-1 -mt-1">×</button>
          </div>
        </div>

        {error && <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300 mb-3">{error}</div>}

        {busy && (
          <p className="text-[11px] text-fg-subtle py-16 text-center">
            {fetching ? 'Fetching from GuruFocus…' : 'Loading…'}
          </p>
        )}

        {empty && !error && (
          <div className="py-16 text-center space-y-3">
            <p className="text-sm text-fg-subtle">GuruFocus returned no dividend history for this company.</p>
            <button type="button" onClick={pull} disabled={fetching}
              className="text-xs px-3 py-1.5 rounded-lg bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-50">
              Retry fetch
            </button>
          </div>
        )}

        {!busy && dates.length > 0 && (
          <div className="space-y-1">
            {/* An EUR-reported dividend needs no conversion; a second identical panel
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
              {native.length} {isPayments ? (payMode === 'ttm' ? 'trailing totals' : 'payments') : `${cadence} periods`}
              {' · '}{dates[0]} → {dates.at(-1)}
              {isPayments
                ? ' · Live payment feed: a dividend change shows here immediately, while the fiscal-period charts only move once a period closes.'
                : ' · Fiscal-period totals: a period appears only once it closes, so a mid-year change can lag by up to a year — see the payments cadence.'}
              {!isEurNative && ' · Right: converted at the ECB rate on each payment date, so the line carries the FX leg.'}
              {droppedPreFx > 0 && src?.fx_from &&
                ` · ${droppedPreFx} earlier point${droppedPreFx > 1 ? 's' : ''} omitted from EUR: no rate before ${src.fx_from}.`}
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
