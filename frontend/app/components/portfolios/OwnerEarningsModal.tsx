'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { runSSE } from '../../../lib/stream';
import { chartTheme } from '../../../lib/chartTheme';
import type { FinancialSeriesResponse } from '../../../lib/types/api';
import { type Basket } from './PerformanceModal';

type Cadence = 'annual' | 'quarterly';
type Basis = 'native' | 'eur';
type Mode = 'earnings' | 'price';
type ValueKind = 'millions' | 'price' | 'index';
type Row = { date: string; label: string; value: number | null; value_eur: number | null };
/** The lean price-series payload (`GET …/asset-pipeline/price-series/isin/{isin}`). */
type PriceSeries = {
  isin: string; symbol?: string | null; currency?: string | null;
  points: { date: string; value: number; value_eur: number | null }[];
};

const MS_PER_YEAR = 365.25 * 86_400_000;

/** A compact x-axis label: the fiscal year for an annual point, `'YY Qn` for a quarter. */
function periodLabel(date: string, cadence: Cadence): string {
  const [y, m] = date.split('-');
  if (cadence === 'annual') return y;
  return `${y.slice(2)} Q${Math.ceil(Number(m) / 3)}`;
}

/**
 * A LOG-LINEAR fit of a series against time — the steadiness test.
 *
 * The ideal is a quantity (owner earnings, or the share price) that COMPOUNDS at a steady rate,
 * which is EXPONENTIAL growth — a straight line on a log axis. So we regress log(value) on elapsed
 * years (NOT the raw value: fitting a line to an exponential curve scores the best compounders
 * WORST, because the curve bends away from any straight line). R² of that log fit is the
 * steadiness; the slope is the continuously-compounded rate, so CAGR = e^slope − 1.
 *
 * Non-positive periods have no log and are excluded. `line` carries the fitted value at every date.
 */
type Fit = {
  r2: number | null;
  cagr: number | null;
  line: (number | null)[];
  used: number;      // periods that entered the fit (value > 0)
  have: number;      // periods that had a value at all
};

function logFit(dates: string[], levels: (number | null)[]): Fit {
  const t0 = dates.length ? Date.parse(dates[0]) : 0;
  const pts: { t: number; ly: number }[] = [];
  let have = 0;
  for (let i = 0; i < dates.length; i++) {
    const v = levels[i];
    if (v == null) continue;
    have += 1;
    if (v > 0) pts.push({ t: (Date.parse(dates[i]) - t0) / MS_PER_YEAR, ly: Math.log(v) });
  }
  const line: (number | null)[] = dates.map(() => null);
  const used = pts.length;
  if (used < 3) return { r2: null, cagr: null, line, used, have };

  const meanT = pts.reduce((a, p) => a + p.t, 0) / used;
  const meanY = pts.reduce((a, p) => a + p.ly, 0) / used;
  let sxx = 0, sxy = 0, syy = 0;
  for (const p of pts) {
    const dt = p.t - meanT, dy = p.ly - meanY;
    sxx += dt * dt; sxy += dt * dy; syy += dy * dy;
  }
  if (sxx === 0) return { r2: null, cagr: null, line, used, have };
  const b = sxy / sxx;
  const a = meanY - b * meanT;
  let ssRes = 0;
  for (const p of pts) ssRes += (p.ly - (a + b * p.t)) ** 2;
  const r2 = syy > 0 ? 1 - ssRes / syy : null;
  for (let i = 0; i < dates.length; i++) {
    line[i] = Math.exp(a + b * ((Date.parse(dates[i]) - t0) / MS_PER_YEAR));
  }
  return { r2, cagr: Math.exp(b) - 1, line, used, have };
}

/**
 * "Is this company fundamentally good?" — two facets of the SAME test, on a shared engine:
 *
 *   OWNER EARNINGS   net income + D&A − capex, the cash the business throws off to owners.
 *   STOCK PRICE      what the market pays for it (yfinance, split-adjusted).
 *
 * Both are charted on a LOG axis with a fitted trend, and the headline is the R² of that fit — a
 * textbook compounder's points sit on the line (R² → 1), a lumpy one scatters. Steadiness is
 * measured on BOTH the reporting/listing currency (the thing itself) and EUR (what a euro owner
 * sees, FX included): a steady foreign business/stock can have a lower EUR R² purely from currency
 * swings, which are not the company's doing.
 */
type OeProgress = { done: number; total: number; statuses: { isin: string; status: string }[] };

/** Per-holding status shown in the blend progress list (mirrors the backend `_holding_oe`). */
const OE_STATUS: Record<string, { label: string; tone: string }> = {
  ok: { label: 'owner earnings ✓', tone: 'text-pos-400' },
  thin: { label: 'too few years', tone: 'text-warn-300' },
  no_data: { label: 'no data', tone: 'text-fg-faint' },
  'n/a': { label: 'n/a (bank etc.)', tone: 'text-fg-faint' },
  none: { label: 'no financials', tone: 'text-fg-faint' },
};

export default function OwnerEarningsModal({
  isin, name, basket, portfolioId, onClose,
}: {
  isin?: string;
  name?: string | null;
  basket?: Basket;
  portfolioId?: number;    // a whole model portfolio, resolved to a basket server-side
  onClose: () => void;
}) {
  const [mode, setMode] = useState<Mode>('earnings');
  const [cadence, setCadence] = useState<Cadence>('annual');
  const [basis, setBasis] = useState<Basis>('native');
  // A group, an ad-hoc basket, or a whole portfolio all render as an aggregate INDEX (base 100 /
  // 1.0, EUR, annual) — as opposed to a single instrument's currency amount.
  const isAgg = !!basket || portfolioId != null;
  const reqKey = basket ? `b:${basket.holdings.map((h) => `${h.isin}:${h.weight}`).join(',')}`
    : portfolioId != null ? `p:${portfolioId}` : `i:${isin}`;

  // Owner-earnings (fetched on open). For a group it streams per-holding progress.
  const [oe, setOe] = useState<FinancialSeriesResponse | null>(null);
  const [oeLoading, setOeLoading] = useState(true);
  const [oeError, setOeError] = useState<string | null>(null);
  const [oeProgress, setOeProgress] = useState<OeProgress | null>(null);

  // Price series.
  const [px, setPx] = useState<PriceSeries | null>(null);
  const [pxLoading, setPxLoading] = useState(true);
  const [pxError, setPxError] = useState<string | null>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const postBody = basket
    ? { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ holdings: basket.holdings, label: basket.label }) }
    : undefined;

  useEffect(() => {
    let cancelled = false;
    const ctrl = new AbortController();
    (async () => {
      setOeLoading(true); setOeError(null); setOeProgress(null);
      try {
        if (isAgg) {
          // An aggregate (basket / whole portfolio) streams per-holding progress, then the result.
          const url = basket
            ? `${API_URL}/api/asset-pipeline/basket/owner-earnings/stream`
            : `${API_URL}/api/airs/model-portfolios/${portfolioId}/owner-earnings-stream`;
          const init: RequestInit = basket
            ? { method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ holdings: basket.holdings, label: basket.label }) }
            : { method: 'GET' };
          await runSSE(url, init, (data) => {
            if (cancelled) return;
            const ev = data as { type: string; done?: number; total?: number; isin?: string;
              status?: string; payload?: FinancialSeriesResponse };
            if (ev.type === 'progress') {
              setOeProgress((prev) => ({
                done: ev.done ?? 0, total: ev.total ?? 0,
                statuses: ev.isin
                  ? [...(prev?.statuses ?? []), { isin: ev.isin, status: ev.status ?? '' }]
                  : (prev?.statuses ?? []),
              }));
            } else if (ev.type === 'result' && ev.payload) {
              setOe(ev.payload);
            }
          }, ctrl.signal);
        } else {
          const r = await apiFetch(`${API_URL}/api/asset-pipeline/owner-earnings/isin/${encodeURIComponent(isin ?? '')}`);
          const b = await r.json().catch(() => null);
          if (cancelled) return;
          if (!r.ok) { setOeError(b?.detail ?? `HTTP ${r.status}`); return; }
          setOe(b as FinancialSeriesResponse);
        }
      } catch (e) {
        if (!cancelled) setOeError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setOeLoading(false);
      }
    })();
    return () => { cancelled = true; ctrl.abort(); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reqKey]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setPxLoading(true); setPxError(null);
      try {
        const r = basket
          ? await apiFetch(`${API_URL}/api/asset-pipeline/basket/price-series`, postBody)
          : portfolioId != null
            ? await apiFetch(`${API_URL}/api/airs/model-portfolios/${portfolioId}/price-series`)
            : await apiFetch(`${API_URL}/api/asset-pipeline/price-series/isin/${encodeURIComponent(isin ?? '')}?years=10`);
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setPxError(b?.detail ?? `HTTP ${r.status}`); return; }
        setPx(b as PriceSeries);
      } catch (e) {
        if (!cancelled) setPxError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setPxLoading(false);
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reqKey]);

  // Every chart looks back at most 10 years — recent compounding, not a company's whole history.
  const cutoff = useMemo(() => {
    const d = new Date();
    d.setFullYear(d.getFullYear() - 10);
    return d.toISOString().slice(0, 10);
  }, []);

  const oeRows: Row[] = useMemo(() => {
    const rows = (cadence === 'annual' ? oe?.annual : oe?.quarterly) ?? [];
    return rows.filter((p) => p.date >= cutoff).map((p) => ({
      date: p.date, label: periodLabel(p.date, cadence),
      value: p.value ?? null, value_eur: p.value_eur ?? null,
    }));
  }, [oe, cadence, cutoff]);

  const pxRows: Row[] = useMemo(() => (px?.points ?? []).filter((p) => p.date >= cutoff).map((p) => ({
    date: p.date, label: p.date.slice(0, 4), value: p.value ?? null, value_eur: p.value_eur ?? null,
  })), [px, cutoff]);

  const isEarnings = mode === 'earnings';
  const rows = isEarnings ? oeRows : pxRows;
  const ccy = ((isEarnings ? oe?.currency : px?.currency) ?? '').toUpperCase();
  const symbol = isEarnings ? oe?.symbol : px?.symbol;
  const note = isEarnings
    ? oe?.note
    : (isAgg ? 'Value-weighted EUR index (base 1.0) of the holdings.' : 'Monthly split-adjusted close — price only, dividends excluded.');
  const notHome = !isAgg && isEarnings && oe?.is_home === false;
  const notApplicable = isEarnings && oe?.applicable === false;
  const loading = isEarnings ? oeLoading : pxLoading;
  const error = isEarnings ? oeError : pxError;
  const title = name ?? basket?.label ?? isin ?? '';
  // An aggregate's charts are INDICES (blended earnings base 100, price base 1.0), not currency —
  // so no "EUR" suffix and no native/EUR split.
  const seriesLabel = isAgg ? (isEarnings ? 'Owner earnings (blended)' : 'Value index') : (isEarnings ? 'Owner earnings' : 'Stock price');
  const valueKind: ValueKind = isAgg ? 'index' : (isEarnings ? 'millions' : 'price');

  const isEurNative = ccy === 'EUR' || ccy === '';
  const effBasis: Basis = isEurNative ? 'eur' : basis;

  const { fitNative, fitEur, chartData, negatives } = useMemo(() => {
    const dates = rows.map((p) => p.date);
    const native = rows.map((p) => p.value);
    const eur = rows.map((p) => p.value_eur);
    const fN = logFit(dates, native);
    const fE = logFit(dates, eur);
    const level = effBasis === 'eur' ? eur : native;
    const fitLine = (effBasis === 'eur' ? fE : fN).line;
    const cd = rows.map((p, i) => ({
      label: p.label, date: p.date,
      // A non-positive level cannot sit on a log axis — null it so the line simply gaps there.
      value: level[i] != null && level[i]! > 0 ? level[i] : null,
      fit: fitLine[i],
    }));
    // The years that gap the log line — and WHY. A negative owner-earnings year is a LOSS year, a
    // fundamental fact worth naming, not a hole. Named here so 2009 doesn't read as missing data.
    const neg = rows.flatMap((p, i) => (level[i] != null && level[i]! <= 0
      ? [{ label: p.label, value: level[i]! }] : []));
    return { fitNative: fN, fitEur: fE, chartData: cd, negatives: neg };
  }, [rows, effBasis]);

  const shownFit = effBasis === 'eur' ? fitEur : fitNative;
  const canFit = chartData.some((d) => d.fit != null);
  const tickInterval = Math.max(0, Math.floor(chartData.length / 10) - 1);
  const moneyCcy = effBasis === 'eur' ? 'EUR' : ccy;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-scrim/60"
      onClick={onClose} role="dialog" aria-modal="true">
      <div className="bg-card border border-neutral-800/40 rounded-xl shadow-xl w-[98vw] max-w-[1200px] max-h-[94vh] overflow-auto p-4"
        onClick={(e) => e.stopPropagation()}>

        <div className="flex items-start justify-between gap-3 mb-2">
          <div className="min-w-0">
            <div className="flex items-baseline gap-2 flex-wrap">
              <span className="text-base font-semibold text-fg-strong">Fundamental</span>
              {isAgg
                ? <span className="text-sm text-fg-soft truncate">{title}{basket ? ' · group' : ' · portfolio'}</span>
                : <>
                    <span className="text-sm font-mono text-fg-soft">{isin}</span>
                    {name && <span className="text-sm text-fg-soft truncate">{name}</span>}
                  </>}
            </div>
          </div>
          <button type="button" onClick={onClose} aria-label="Close"
            className="text-fg-faint hover:text-fg-strong text-xl leading-none px-1 -mt-1">×</button>
        </div>

        {/* Primary tab bar (what to look at) + the per-facet toggles. */}
        <div className="flex items-center justify-between gap-2 flex-wrap mb-3">
          <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5">
            {([['earnings', 'Owner earnings'], ['price', 'Stock price']] as [Mode, string][]).map(([m, lab]) => (
              <button key={m} type="button" onClick={() => setMode(m)}
                className={`text-xs px-3 py-1 rounded-md transition-colors ${
                  mode === m ? 'bg-accent-600 text-white' : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/[0.04]'}`}>
                {lab}
              </button>
            ))}
          </div>
          <div className="flex items-center gap-2">
            {!isEurNative && !error && !notApplicable && (
              <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5">
                {(['native', 'eur'] as Basis[]).map((bs) => (
                  <button key={bs} type="button" onClick={() => setBasis(bs)}
                    className={`text-[11px] px-2.5 py-1 rounded-md transition-colors ${
                      basis === bs ? 'bg-accent-600 text-white' : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/[0.04]'}`}>
                    {bs === 'native' ? ccy || 'Native' : 'EUR'}
                  </button>
                ))}
              </div>
            )}
            {isEarnings && !isAgg && !error && !notApplicable && (
              <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5">
                {(['annual', 'quarterly'] as Cadence[]).map((c) => (
                  <button key={c} type="button" onClick={() => setCadence(c)}
                    className={`text-[11px] px-2.5 py-1 rounded-md transition-colors ${
                      cadence === c ? 'bg-accent-600 text-white' : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/[0.04]'}`}>
                    {c}
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* A 404 here is an expected dead end (a fund, a dead OTC line, no priced listing). */}
        {error && (
          <div className="py-16 text-center max-w-2xl mx-auto space-y-2">
            <p className="text-sm text-fg-soft">No {seriesLabel.toLowerCase()} series for this instrument.</p>
            <p className="text-[11px] text-fg-faint">{error}</p>
          </div>
        )}

        {!loading && !error && notHome && canFit && (
          <div className="bg-warn-500/10 border border-warn-500/20 rounded-lg px-3 py-2 text-[11px] text-warn-300 mb-3">
            <span className="font-semibold">Not this instrument’s own listing.</span>{' '}
            These figures come from {symbol}, and GuruFocus reports financials in the{' '}
            <strong>listing’s</strong> currency, converted per fiscal period — a different currency
            basis than the row’s own listing would give.
          </div>
        )}

        {loading && isAgg && isEarnings && oeProgress ? (
          <div className="py-8 max-w-md mx-auto">
            <p className="text-[11px] text-fg-subtle text-center mb-2">
              Blending owner earnings… {oeProgress.done}/{oeProgress.total} holdings
            </p>
            <div className="h-1 rounded-full bg-neutral-800/40 overflow-hidden mb-3">
              <div className="h-full bg-accent-500 transition-all"
                style={{ width: `${oeProgress.total ? (100 * oeProgress.done) / oeProgress.total : 0}%` }} />
            </div>
            <div className="space-y-0.5 max-h-[46vh] overflow-auto">
              {oeProgress.statuses.map((s, i) => (
                <div key={`${s.isin}-${i}`} className="flex items-center justify-between gap-2 text-[11px]">
                  <span className="text-fg-soft truncate">
                    {basket?.holdings.find((h) => h.isin === s.isin)?.name ?? s.isin}
                  </span>
                  <span className={`whitespace-nowrap ${OE_STATUS[s.status]?.tone ?? 'text-fg-faint'}`}>
                    {OE_STATUS[s.status]?.label ?? s.status}
                  </span>
                </div>
              ))}
            </div>
          </div>
        ) : loading ? (
          <p className="text-[11px] text-fg-subtle py-16 text-center">Loading {seriesLabel.toLowerCase()}…</p>
        ) : null}

        {!loading && !error && notApplicable && (
          <div className="py-16 text-center max-w-2xl mx-auto space-y-2">
            <p className="text-sm text-fg-soft">Owner earnings cannot be computed for this company.</p>
            <p className="text-[11px] text-fg-faint">
              GuruFocus renders its statements with the{' '}
              <strong>{oe?.template ?? 'industry'}</strong> template, which is missing one of the
              three lines owner earnings is built from (net income + D&A − capex).
            </p>
          </div>
        )}

        {!loading && !error && !notApplicable && !canFit && (
          <p className="text-sm text-fg-subtle py-16 text-center max-w-xl mx-auto">
            Not enough to fit a trend for {symbol ?? 'this instrument'} — a steadiness fit needs at
            least three periods with <strong>positive</strong> {seriesLabel.toLowerCase()}
            {shownFit.have > 0 && shownFit.used < 3 &&
              ` (only ${shownFit.used} of ${shownFit.have} qualify${effBasis === 'eur' ? ' in EUR' : ''})`}.
          </p>
        )}

        {!loading && !error && !notApplicable && canFit && (
          <div className="space-y-2">
            {/* The headline: R² on BOTH bases + the CAGR the fit implies. */}
            <div className="flex flex-wrap items-center gap-x-5 gap-y-1 text-xs">
              <span className="text-[10px] uppercase tracking-wide text-fg-faint">Steadiness R²</span>
              <R2Stat label={isEurNative ? 'EUR' : ccy || 'Native'} r2={fitNative.r2} />
              {!isEurNative && <R2Stat label="EUR" r2={fitEur.r2} />}
              {shownFit.cagr != null && (
                <span className="text-fg-muted">
                  CAGR{' '}
                  <span className={`font-mono font-semibold ${shownFit.cagr >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
                    {shownFit.cagr >= 0 ? '+' : ''}{(shownFit.cagr * 100).toFixed(1)}%/yr
                  </span>
                  <span className="text-fg-faint"> ({moneyCcy || 'native'}{isEarnings ? '' : ', price only'})</span>
                </span>
              )}
            </div>

            <ResponsiveContainer width="100%" height={360}>
              <LineChart data={chartData} margin={{ top: 8, right: 16, bottom: 4, left: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.grid} vertical={false} />
                <XAxis dataKey="label" tick={{ fill: chartTheme.axisTick, fontSize: 11 }} tickLine={false}
                  interval={tickInterval} padding={{ left: 12, right: 16 }} />
                <YAxis
                  scale="log" domain={['auto', 'auto']} allowDataOverflow
                  tick={{ fill: chartTheme.axisTick, fontSize: 11 }} tickLine={false} width={54}
                  tickFormatter={(v: number) => fmtValue(v, valueKind)}
                />
                <Tooltip
                  {...chartTheme.tooltipCard}
                  labelFormatter={(_l, payload) => String(payload?.[0]?.payload?.date ?? '')}
                  formatter={(value, key) => [
                    money(Number(value), moneyCcy, valueKind),
                    key === 'fit' ? 'Trend' : seriesLabel,
                  ]}
                />
                <Line type="monotone" dataKey="fit" stroke={chartTheme.warn} strokeWidth={2}
                  strokeDasharray="5 4" dot={false} isAnimationActive={false} connectNulls name="fit" />
                <Line type="monotone" dataKey="value" stroke={chartTheme.accent}
                  strokeWidth={isEarnings ? 1.25 : 1.75}
                  dot={isEarnings ? { r: 3, fill: chartTheme.accent } : false}
                  isAnimationActive={false} connectNulls={false} name="value" />
              </LineChart>
            </ResponsiveContainer>

            <div className="text-[10px] text-fg-faint">
              {shownFit.used} of {shownFit.have} {isEarnings ? (cadence === 'annual' ? 'years' : 'quarters') : 'months'} fitted
              {shownFit.used < shownFit.have && `; ${shownFit.have - shownFit.used} non-positive excluded`}.
              {' · '}Log axis; steady compounding is a straight line. R²{' '}
              (<span className="text-pos-400">&gt;0.95 steady</span> ·
              <span className="text-warn-300"> 0.8–0.95 lumpy</span> ·
              <span className="text-neg-400"> &lt;0.8 erratic</span>),
              shown for {isEarnings ? 'reporting' : 'listing'} currency and EUR.
              {note && <> · {note}</>}
            </div>
            {/* Named, not hidden: a negative period can't sit on a log axis, and a reader seeing a
                gap assumes missing data. A loss period is a fundamental fact, so it is stated. */}
            {negatives.length > 0 && (
              <div className="text-[10px] text-neg-400">
                Excluded (non-positive): {negatives.map((nv) => `${nv.label} ${money(nv.value, moneyCcy, valueKind)}`).join(', ')}.
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

/** An R² with a quality colour: green steady, amber lumpy, red erratic, grey when unmeasurable. */
function R2Stat({ label, r2 }: { label: string; r2: number | null }) {
  const tone = r2 == null ? 'text-fg-faint'
    : r2 >= 0.95 ? 'text-pos-400' : r2 >= 0.8 ? 'text-warn-300' : 'text-neg-400';
  return (
    <span className="text-fg-muted">
      {label}{' '}
      <span className={`font-mono font-semibold ${tone}`}>{r2 == null ? '—' : r2.toFixed(2)}</span>
    </span>
  );
}

/** Format a value for an axis tick / tooltip. Millions → "100bn"; a share price → "150"/"12.34";
 *  an index (basket, unitless) → a plain rebased number. */
function fmtValue(v: number, kind: ValueKind): string {
  const a = Math.abs(v);
  if (kind === 'index') return a >= 100 ? v.toFixed(0) : a >= 10 ? v.toFixed(1) : v.toFixed(2);
  if (kind === 'price') return a >= 1000 ? v.toFixed(0) : a >= 10 ? v.toFixed(1) : v.toFixed(2);
  if (a >= 1e6) return `${(v / 1e6).toFixed(a >= 1e7 ? 0 : 1)}tn`;
  if (a >= 1e3) return `${(v / 1e3).toFixed(a >= 1e4 ? 0 : 1)}bn`;
  return `${v.toFixed(0)}M`;
}

/** A value with its currency appended — except an index, which is unitless. */
function money(value: number, ccy: string, kind: ValueKind): string {
  return kind === 'index' ? fmtValue(value, kind) : `${fmtValue(value, kind)} ${ccy}`;
}
