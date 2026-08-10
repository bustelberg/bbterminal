'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { chartTheme } from '../../lib/chartTheme';
import type { AssetGridRow, FinancialSeriesResponse } from '../../lib/types/api';
import LwLineChart from './LwLineChart';

type Cadence = 'annual' | 'quarterly';
type Point = { date: string; value: number };
/** Registry keys, mirroring `_asset_financials._ITEMS`. Adding a column is adding one. */
export type LineItem =
  | 'revenue' | 'gross_profit' | 'ebit' | 'interest_expense' | 'pretax_income'
  | 'income_tax' | 'net_income' | 'depreciation_amort' | 'eps_diluted'
  | 'operating_cash_flow' | 'capex' | 'total_debt' | 'cash_and_equivalents'
  | 'shareholders_equity' | 'shares_outstanding' | 'forward_eps' | 'revenue_growth_est' | 'eps_lt_growth_est';

/** Shown only while the response — which carries the real label — is in flight. */
const FALLBACK_LABEL: Record<LineItem, string> = {
  revenue: 'Revenue',
  gross_profit: 'Gross profit',
  ebit: 'EBIT',
  interest_expense: 'Interest expense',
  pretax_income: 'Pretax income',
  income_tax: 'Income tax',
  net_income: 'Net income',
  depreciation_amort: 'D&A',
  eps_diluted: 'EPS (diluted)',
  operating_cash_flow: 'Operating CF',
  capex: 'Capex',
  total_debt: 'Total debt',
  cash_and_equivalents: 'Cash & equiv.',
  shareholders_equity: 'Equity',
  shares_outstanding: 'Shares out.',
  forward_eps: 'Forward EPS',
  revenue_growth_est: 'Rev growth (est)',
  eps_lt_growth_est: 'EPS LTG (est)',
};

/** Lines GuruFocus reports as a NEGATIVE number BY CONVENTION (an outflow — the sign
 * carries no information). We chart them as reported, no silent flip, and say so.
 *
 * Operating cash flow is deliberately NOT here: Apple's is +111,482 and JPMorgan's is
 * −147,782, because a bank's operating cash flow routinely goes negative as loans and
 * trading assets grow. There the sign IS the information, and telling the user "this
 * line is reported negative" would be a lie about half the companies. */
const NEGATIVE_BY_CONVENTION = new Set<LineItem>(['interest_expense', 'income_tax', 'capex']);

/** One income-statement line — TWO charts, native currency and EUR.
 *
 * ONE THING HERE IS THE OPPOSITE OF THE DIVIDEND COLUMN, and it decides how to read the
 * "not this row's listing" warning:
 *
 *   Dividends are reported in the DECLARATION currency on every listing of an ISIN.
 *     Apple pays 0.27 USD whether you read it off Nasdaq, Xetra, Zurich or Milan. So a
 *     foreign listing gives the same number, only a possibly-shorter history.
 *
 *   Revenue is FX-CONVERTED INTO THE LISTING'S TRADING CURRENCY, per fiscal period.
 *     CSX reports USD; its Xetra line comes back in EUR, at each year's own rate:
 *         FY2024-12   Nasdaq 14,540 USD    Xetra 13,885.700 EUR   (x0.955)
 *         FY2025-12   Nasdaq 14,092 USD    Xetra 12,034.568 EUR   (x0.854)
 *     Same company, same year, DIFFERENT number. So here a non-home listing is not a
 *     cosmetic detail — it changes the currency basis of the whole series.
 *
 * VALUES ARE IN MILLIONS. 14,092 is $14.1bn, and rendering it as dollars would be off by
 * six orders of magnitude. The axis says so, and the header formats it as bn/M. */
export default function AssetFinancialModal({
  row, isin, item, onClose, onLoaded,
}: {
  row: AssetGridRow;
  isin: string;
  /** Which income-statement line to chart — `_ITEMS` on the backend. */
  item: LineItem;
  onClose: () => void;
  /** Lets the grid badge NO DATA without reloading the coverage map. */
  onLoaded?: (hasFinancials: boolean) => void;
}) {
  const [data, setData] = useState<FinancialSeriesResponse | null>(null);
  const [cadence, setCadence] = useState<Cadence>('annual');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true); setError(null);
      try {
        const r = await apiFetch(
          `${API_URL}/api/asset-pipeline/financials/isin/${encodeURIComponent(isin)}/${item}`);
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); onLoaded?.(false); return; }
        const d = b as FinancialSeriesResponse;
        setData(d);
        // The company HAS financials — even when THIS line doesn't apply to it (a bank
        // has no gross profit). The flag is about the blob, so the other column's cell
        // must not be badged NO DATA just because this line is N/A.
        onLoaded?.(true);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
    // onLoaded is an inline arrow in the parent; excluding it keeps this keyed on the ISIN.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isin, item]);

  const rows = (cadence === 'annual' ? data?.annual : data?.quarterly) ?? [];
  const native: Point[] = rows.map((p) => ({ date: p.date, value: p.value }));
  const eur: Point[] = rows.flatMap((p) => (p.value_eur == null ? [] : [{ date: p.date, value: p.value_eur }]));
  const droppedPreFx = native.length - eur.length;

  const ccy = (data?.currency ?? '').toUpperCase();
  const isEurNative = ccy === 'EUR';
  const notHome = data?.is_home === false;
  const last = native.at(-1);
  const lastEur = eur.at(-1);
  // The company HAS financials, but this LINE does not exist in its industry template.
  // A bank has no gross profit — no cost of goods sold, so no such line. An answer.
  const notApplicable = data?.applicable === false;
  // The backend sends the label; this is only the pre-load placeholder.
  const title = data?.label ?? FALLBACK_LABEL[item];
  // How the line reads INSIDE a sentence. NOT `title.toLowerCase()` — that yields
  // "a fund has no ebit". An acronym is not a word.
  const phrase = data?.phrase || FALLBACK_LABEL[item].toLowerCase();
  // EPS is a currency amount PER SHARE, every other line is a currency amount in
  // MILLIONS. Both convert to EUR the same way; only the scale and the axis differ.
  // Render 7.46 on the millions path and it reads "$7.46 million" — off by 1e6, and it
  // looks completely reasonable.
  const perShare = data?.unit === 'per_share';
  // A COUNT, not currency. There is no EUR version of a share count — dividing
  // "15,004.697 million shares" by an FX rate is not a quantity — so this renders ONE
  // chart, and the backend leaves value_eur null rather than inventing a number.
  const isCount = data?.unit === 'shares';
  // A RATE, in percent. Not currency either — "10.09% / 1.17 EUR-per-USD" is the same
  // category error as converting a share count. Shares and percents share one mechanism:
  // no EUR panel, one chart. `nonCurrency` is what the render branches on.
  const isPercent = data?.unit === 'percent';
  const nonCurrency = isCount || isPercent;
  const scale = isCount
    ? 'millions of shares'
    : isPercent ? 'percent' : perShare ? `${ccy} per share` : `${ccy}, millions`;
  // A SCALAR — no series at all. The long-term growth consensus is ONE number, not a
  // timeseries, and GuruFocus publishes it as one. Plotting a single point would dress it
  // up as a trend, so the number is shown as a number.
  const scalar = data?.scalar_value ?? null;
  const isScalar = scalar != null;

  const panel = (label: string, pts: Point[], color: string, unit: string) => (
    <div className="flex-1 min-w-[320px]">
      <div className="text-[11px] uppercase tracking-wide text-fg-faint mb-1">{label}</div>
      {pts.length > 0 ? (
        <LwLineChart data={pts} scale="linear" unit={unit} color={color} />
      ) : (
        <div className="w-full aspect-[16/9] max-h-[72vh] min-h-[300px] flex items-center justify-center text-center px-6 text-[12px] text-fg-faint border border-neutral-800/40 rounded-lg">
          {data?.fx_from
            ? <>Every period predates our FX coverage (from {data.fx_from}).</>
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
              <span className="text-base font-semibold text-fg-strong">{title}</span>
              <span className="text-sm font-mono text-fg-soft">{isin}</span>
              {row.name && <span className="text-sm text-fg-soft truncate">{row.name}</span>}
            </div>
            <div className="text-[12px] text-fg-faint mt-0.5">
              GuruFocus
              {data?.symbol && ` · ${data.symbol}`}
              {(ccy || nonCurrency) && ` · reported in ${scale}`}
              {last && ` · latest ${isCount ? count(last.value)
                : isPercent ? pct(last.value) : money(last.value, ccy, perShare)}`}
              {lastEur && !isEurNative && !nonCurrency && ` = ${money(lastEur.value, 'EUR', perShare)}`}
              {last && ` (${last.date})`}
            </div>
          </div>
          <div className="flex items-center gap-2">
            {/* A scalar has no cadence — offering annual/quarterly for a single figure
                would imply two of them exist. */}
            {!error && !notApplicable && !isScalar && (
              <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5">
                {(['annual', 'quarterly'] as Cadence[]).map((c) => (
                  <button key={c} type="button" onClick={() => setCadence(c)}
                    className={`text-[12px] px-2.5 py-1 rounded-md transition-colors ${
                      cadence === c ? 'bg-accent-600 text-white' : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/[0.04]'}`}>
                    {c}
                  </button>
                ))}
              </div>
            )}
            <button type="button" onClick={onClose} aria-label="Close"
              className="text-fg-faint hover:text-fg-strong text-xl leading-none px-1 -mt-1">×</button>
          </div>
        </div>

        {/* A 404 here is an expected dead end (a fund, a dead OTC line), not a fault —
            the backend sends the reason as prose, so show it as an explanation. */}
        {error && (
          <div className="py-16 text-center max-w-2xl mx-auto space-y-2">
            <p className="text-sm text-fg-soft">No revenue series for this row.</p>
            <p className="text-[12px] text-fg-faint">{error}</p>
          </div>
        )}

        {/* Unlike the payout column, a non-home listing here changes the NUMBER, not just
            the history length — GuruFocus converts financials into the listing's own
            currency. Worth a louder warning. */}
        {!loading && !error && notHome && rows.length > 0 && (
          <div className="bg-warn-500/10 border border-warn-500/20 rounded-lg px-3 py-2 text-[12px] text-warn-300 mb-3">
            <span className="font-semibold">Not this row’s own listing.</span>{' '}
            These figures come from {data?.symbol}, and GuruFocus reports financials in the{' '}
            <strong>listing’s</strong>{' '}currency — converted per fiscal period, not the company’s
            reporting currency. So this series is on a different currency basis than the row’s own
            listing would give (CSX: 14,092 USD on Nasdaq vs 12,034.6 EUR on Xetra, FY2025).
          </div>
        )}

        {loading && <p className="text-[12px] text-fg-subtle py-16 text-center">Loading {phrase}…</p>}

        {/* A SCALAR: one figure, no series. Showing it as a single-point chart would dress
            a number up as a trend, so it is shown as a number. */}
        {!loading && !error && isScalar && (
          <div className="py-16 text-center max-w-2xl mx-auto space-y-3">
            <div className="text-5xl font-semibold text-fg-strong font-mono">{pct(scalar!)}</div>
            <p className="text-sm text-fg-soft">{title} · {data?.symbol}</p>
            {data?.note && <p className="text-[12px] text-fg-faint">{data.note}</p>}
            <p className="text-[12px] text-fg-faint">
              A single consensus figure — GuruFocus publishes no series for it, so there is
              nothing to chart. It is a forecast of the growth <em>rate</em>, not of earnings,
              and being a rate it is not currency: there is no EUR version.
            </p>
          </div>
        )}

        {/* The line does not EXIST in this company's industry template — an answer, not a
            gap. A bank has no cost of goods sold, so its income statement has no gross
            profit line at all: JPMorgan reports Interest Income and Net Interest Income.
            Drawing an empty chart here would imply the number is missing; it isn't. */}
        {!loading && !error && notApplicable && (
          <div className="py-16 text-center max-w-2xl mx-auto space-y-2">
            <p className="text-sm text-fg-soft">
              {title} does not apply to this company.
            </p>
            <p className="text-[12px] text-fg-faint">
              GuruFocus renders its income statement with the{' '}
              <strong>{data?.template ?? 'industry'}</strong> template, which has no “{phrase}”
              line{data?.template === 'bank' ? ' — a bank has no cost of goods sold, so it reports Interest Income and Net Interest Income instead' : ''}.
              This is a property of the industry, not a hole in the data.
            </p>
          </div>
        )}

        {!loading && !error && !notApplicable && !isScalar && rows.length === 0 && (
          <p className="text-sm text-fg-subtle py-16 text-center">
            GuruFocus returned no {cadence} {phrase} for {data?.symbol ?? 'this listing'}.
          </p>
        )}

        {!loading && !error && !notApplicable && rows.length > 0 && (
          <div className="space-y-1">
            {/* A share COUNT and a PERCENT have no EUR counterpart, so they get ONE
                full-width chart. An empty second panel would imply a missing conversion;
                there is nothing to convert. */}
            {nonCurrency
              ? <div className="flex gap-4">
                  {panel(isCount ? 'Millions of shares' : 'Percent', native,
                    chartTheme.accentStrong, isCount ? 'M sh' : '%')}
                </div>
              : isEurNative
              ? <div className="flex gap-4">
                  {panel(perShare ? 'EUR per share' : 'EUR (millions)', native, chartTheme.accentStrong,
                    perShare ? 'EUR/sh' : 'EUR M')}
                </div>
              : (
                <div className="flex gap-4 flex-wrap">
                  {panel(`Native (${scale})`, native, chartTheme.accentStrong,
                    perShare ? `${ccy}/sh` : `${ccy} M`)}
                  {panel(perShare ? 'EUR per share' : 'EUR (millions)', eur, chartTheme.pos,
                    perShare ? 'EUR/sh' : 'EUR M')}
                </div>
              )}
            <div className="text-[11px] text-fg-faint">
              {native.length} {cadence} periods · {rows[0].date} → {rows.at(-1)?.date}
              {' · '}Values are {isCount
                ? <>a <strong>share count</strong> in millions — <em>not</em>{' '}currency, so there is no EUR
                  version: converting a number of shares at an FX rate would mean nothing.</>
                : isPercent
                ? <>a <strong>rate, in percent</strong> — <em>not</em>{' '}currency, so there is no EUR version:
                  dividing a growth rate by an FX rate would mean nothing.</>
                : perShare
                ? <>per <strong>share</strong> in {ccy || 'the listing currency'} — <em>not</em> millions.</>
                : <>in <strong>millions</strong> of {ccy || 'the listing currency'}.</>}
              {/* A DERIVED series must say it is derived — the user is entitled to know
                  this number was computed here rather than read from GuruFocus. */}
              {data?.note && <> · <strong>Derived:</strong> {data.note}</>}
              {/* The line runs below zero on purpose. Flipping the sign to make it "look
                  right" would make our number disagree with the source it cites. */}
              {NEGATIVE_BY_CONVENTION.has(item) &&
                ' · GuruFocus reports this line NEGATIVE — it is an outflow — and it is charted as reported, never sign-flipped. A 0 is a real value, not a missing period.'}
              {' · '}The rolling “TTM” column GuruFocus appends to the fiscal axis is dropped — it
              duplicates the latest period rather than being one.
              {!isEurNative && ' · Right: each period converted at the ECB rate on its own period-end date, so the line carries the FX leg.'}
              {droppedPreFx > 0 && data?.fx_from &&
                ` · ${droppedPreFx} earlier period${droppedPreFx > 1 ? 's' : ''} omitted from EUR: no rate before ${data.fx_from}.`}
              {' · '}Scroll to zoom, drag to pan, double-click to reset.
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

/** A rate, already in percent units (14.8 -> "+14.8%"). Signed, because a negative growth
 *  rate is a real and important answer. */
function pct(v: number): string {
  return `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`;
}

/** A share count in millions -> a readable magnitude. 15,004.697 = 15.00bn shares. */
function count(millions: number): string {
  return millions >= 1000
    ? `${(millions / 1000).toFixed(2)}bn shares`
    : `${millions.toLocaleString(undefined, { maximumFractionDigits: 1 })}M shares`;
}

/** Millions -> a readable magnitude (14092 = 14.09bn), or a per-share amount as-is.
 *  EPS is 7.46 a share, NOT "7.46 million" — the same number, six orders apart. */
function money(value: number, ccy: string, perShare = false): string {
  if (perShare) return `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })} ${ccy}/sh`;
  const millions = value;
  const abs = Math.abs(millions);
  if (abs >= 1e6) return `${(millions / 1e6).toFixed(2)}tn ${ccy}`;
  if (abs >= 1e3) return `${(millions / 1e3).toFixed(2)}bn ${ccy}`;
  return `${millions.toLocaleString(undefined, { maximumFractionDigits: 1 })}M ${ccy}`;
}
