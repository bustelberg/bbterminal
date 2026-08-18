'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { invalidateReadCache } from '../../../lib/readCache';
import { cancelJob, jobsStore, startLocalJob } from '../../../lib/stores/jobs';
import { API_URL } from '../../../lib/apiUrl';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { calculateEGM, EGM_DEFAULTS, type EgmAssumptions } from './egm';
import { egmSource, reverseDcfSource } from './egmInputs';
import EgmAssumptionsModal from './EgmAssumptionsModal';
import ReverseDcfPanel, { type GrowthEstimates } from './ReverseDcfPanel';
import { type MetricRow } from './quickValuation';

/**
 * The "Deep Valuation" tab — an Earnings Growth Model panel for ONE company.
 *
 * A 10-year annualised return and a fair value from three drivers: earnings growth, dividend yield
 * and the change in the P/E multiple. The maths lives in `egm.ts` (pure) and the inputs come out of
 * the metrics payload in `egmInputs.ts` (pure); this file fetches once, renders, and recalculates
 * in the browser as the assumptions change — no server round-trip.
 *
 * ⚠ SINGLE COMPANY ONLY. Forward P/E, next-year EPS and a dividend yield are per-share facts about
 * one issuer; a basket has none of them in a summable form.
 *
 * ⚠ THE ASSUMPTIONS ARE THE USER'S AND THE REFERENCES ARE NOT INPUTS. `analystGrowth5Y` and
 * `medianPE5Y` are shown beside the two fields they speak to, and one click copies either into the
 * field — but nothing computes from them unless the user puts them there. A "reference" that
 * quietly seeds the model is an assumption nobody made.
 */

const KEY = (isin: string) => `egm:${isin}`;

/** Overrides are stored per instrument, keyed by ISIN — the identity every other surface in this
 *  app uses, and stable where a ticker is not (the same issuer trades under several). */
function loadSaved(isin: string): Partial<EgmAssumptions> {
  if (typeof window === 'undefined') return {};
  try {
    const raw = window.localStorage.getItem(KEY(isin));
    if (!raw) return {};
    const p = JSON.parse(raw) as Partial<EgmAssumptions>;
    // Only finite numbers survive — a hand-edited or half-written entry must not poison the model.
    const out: Partial<EgmAssumptions> = {};
    for (const k of ['growthRate', 'exitPE', 'hurdleRate', 'years'] as const) {
      if (typeof p?.[k] === 'number' && Number.isFinite(p[k])) out[k] = p[k];
    }
    // ⚠ The yield is stored ONLY when overridden. Persisting the measured value would freeze last
    // period's figure into this instrument for ever, and the field would stop tracking the data
    // it is supposed to default to.
    if (typeof p?.dividendYield === 'number' && Number.isFinite(p.dividendYield)) {
      out.dividendYield = p.dividendYield;
    }
    return out;
  } catch { return {}; }
}

/**
 * One assumption: a label, the value, and — where there is one — a reference figure that one click
 * copies in. Kept as a STRING while typing so an intermediate "1." or "-" doesn't get parsed into
 * a valuation and bounce the caret.
 *
 * ⚠⚠ ONE ROW, INCLUDING THE REFERENCE. It was a label-over-input stack, then a row with the
 * reference on a SECOND line beneath it — which doubles the height of every field that has one,
 * for a figure that is one click and not part of the model. Four fields became eight lines. The
 * reference is now a chip at the end of the same row: same affordance, half the table.
 *
 * ⚠ THE SUFFIX SLOT IS RESERVED WHETHER OR NOT IT IS USED. Exit P/E is a multiple and carries no
 * `%`; without a fixed slot its input sits a character right of the other three, which is exactly
 * the raggedness a column of aligned fields exists to remove.
 *
 * ⚠ THE CHIP IS A BUTTON AND LOOKS LIKE ONE. As a bare number with a separate "use" link beside
 * it, the number read as data — as though the field were already showing it.
 */
function Field({
  label, value, onChange, suffix, step = '0.1', placeholder, info, hint, hintTitle, onUseHint,
}: {
  label: string; value: string; onChange: (v: string) => void;
  suffix?: string; step?: string; placeholder?: string;
  /**
   * Where this field's DEFAULT comes from, and what unit it is in.
   *
   * ⚠⚠ THERE WAS NO SUCH ICON HERE AT ALL until 2026-08-18, and three of the four defaults are
   * HOUSE CONSTANTS rather than facts about the company — a 10% growth rate, a 20x exit multiple
   * and a 10% hurdle, identical on every name this tab opens. Shown as pre-filled figures beside
   * two that ARE measured, they read as this company's numbers. On a stock trading at 78.5x, the
   * 20x default is doing more to the answer than every other input combined.
   */
  info?: React.ReactNode;
  /** The reference figure, already formatted. Null when there is none to offer. */
  hint?: string | null;
  /** What the reference IS, on hover — for a caveat that qualifies ONE field. It used to be a
   *  standing paragraph under all four: a line every reader pays for, about a figure they may
   *  never use. */
  hintTitle?: string;
  onUseHint?: () => void;
}) {
  return (
    <label className="flex items-center gap-2 py-1">
      <span className="min-w-0 flex-1 truncate text-[12px] text-fg-muted">{label}</span>
      <input type="number" step={step} value={value} placeholder={placeholder}
        onChange={(e) => onChange(e.target.value)}
        className="w-16 shrink-0 bg-page border border-neutral-700 rounded px-1.5 py-0.5 text-[12px] font-mono text-fg-strong text-right focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30" />
      <span className="w-2 shrink-0 text-[11px] text-fg-muted">{suffix}</span>
      {/* ⚠ FIXED-WIDTH SLOT so the four inputs stay in one column whether a field has a
          reference or not — `Hurdle rate` has none, and without this its input would slide right.
          ⚠⚠ AND THE CHIP IS ALWAYS A <button>, NEVER SOMETIMES A <span>. The dividend row used to
          render a plain span while the field was showing its measured default and swap to a button
          the moment it was overridden — different padding, different box, so TYPING IN THE FIELD
          resized the row beside it. A control that is not currently applicable is DISABLED, which
          keeps its geometry; it does not stop existing. */}
      <span className="w-14 shrink-0 text-right">
        {hint && (
          <button type="button" onClick={onUseHint} disabled={!onUseHint}
            title={hintTitle ?? `Use ${hint}`}
            className={`rounded px-1 py-px font-mono text-[10px] ${onUseHint
              ? 'text-accent-400 hover:bg-overlay/5 hover:underline'
              : 'text-fg-faint cursor-default'}`}>
            {hint}
          </button>
        )}
      </span>
      {/* ⚠⚠ THE ⓘ IS A TRAILING SLOT, NOT A SUFFIX ON THE LABEL. Beside the label its x landed
          wherever that label happened to end — `Growth rate ⓘ` and `Dividend yield ⓘ` are forty
          pixels apart, so a column of four explanations read as scattered punctuation. Last slot,
          fixed width, and every ⓘ on both panels lands on one vertical line: the boxes are
          equal-width grid cells and both panels are the same card, so "the same distance from the
          right edge" is the same x everywhere. */}
      <span className="flex w-9 shrink-0 items-center">{info}</span>
    </label>
  );
}

/** `GET|POST /api/asset-pipeline/latest-close/isin/{isin}` — the fields this panel reads. */
type LatestClose = {
  date: string; symbol?: string | null; stale_days?: number;
  close: number; currency: string; close_in?: number | null; in_currency?: string | null;
};

export default function DeepValuationTab({ isin, name }: { isin: string; name?: string | null }) {
  const [metrics, setMetrics] = useState<MetricRow[] | null>(null);
  const [currency, setCurrency] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showWorking, setShowWorking] = useState(false);
  const [growthEst, setGrowthEst] = useState<GrowthEstimates | null>(null);
  /**
   * The toast this panel's refresh is reporting into, while it runs.
   *
   * ⚠⚠ THE ID, NOT A BOOLEAN — because the button has to be able to CANCEL, and cancelling needs
   * the handle. A private `refreshing` flag could only spin; the reader's one way to stop the work
   * would have been the toast in the corner, which is not where they pressed.
   *
   * ⚠ THE STATE IS READ BACK OUT OF THE STORE, NOT MIRRORED HERE. `cancelRequested` flips the
   * moment Cancel is pressed and `status` follows when the work actually stops — two facts the
   * store already owns, and a local copy of them is a second source of truth that drifts in
   * exactly the window the button is trying to describe.
   */
  /**
   * The yfinance close — the price this panel actually shows, and the one ↻ refreshes.
   *
   * ⚠⚠ NOT THE `close_price` IN THE METRICS PAYLOAD. That is `metric_data` filed by GURUFOCUS;
   * this is `asset_price`, filed by YFINANCE. Two price worlds, keyed differently
   * (`company_id` vs `analysis_id`) — the pair `timeseries.resolve()` refuses to mix. The panel
   * read the first and the refresh button wrote the second, so the button did real work, the
   * toast said so truthfully, and the figure never moved.
   *
   * ⚠ `undefined` = NOT ASKED YET, `null` = ASKED AND THERE IS NONE. The card must not claim
   * "GuruFocus" for the length of one request on a company that has a perfectly good Yahoo close
   * arriving.
   */
  const [live, setLive] = useState<LatestClose | null | undefined>(undefined);
  const [jobId, setJobId] = useState<string | null>(null);
  // ⚠ THE SAME SUBSCRIPTION `JobToaster` USES — `store.use` with a selector, so this re-renders
  // on the job frames and nothing else.
  const jobs = jobsStore.use((st) => st.jobs);
  const job = jobId == null ? null : jobs.find((j) => j.id === jobId) ?? null;
  const refreshing = job?.status === 'running';
  const cancelling = refreshing && job.cancelRequested;

  // Held as strings so a half-typed value stays on screen; parsed on every render for the model.
  //
  // ⚠ THE SAVED OVERRIDES ARE READ IN THE INITIALISER, NOT IN AN EFFECT. Loading them afterwards
  // means one render at the defaults first — a fair value the user never assumed, on screen long
  // enough to be read. The parent keys this component on the ISIN, so a different instrument
  // remounts and re-reads rather than needing a reset effect.
  const [growthStr, setGrowthStr] = useState(
    () => ((loadSaved(isin).growthRate ?? EGM_DEFAULTS.growthRate) * 100).toFixed(1));
  const [exitStr, setExitStr] = useState(
    () => String(loadSaved(isin).exitPE ?? EGM_DEFAULTS.exitPE));
  const [hurdleStr, setHurdleStr] = useState(
    () => ((loadSaved(isin).hurdleRate ?? EGM_DEFAULTS.hurdleRate) * 100).toFixed(1));
  // ⚠ BLANK MEANS "USE THE MEASURED YIELD" — the same convention as the reverse DCF's starting
  // cash flow. The measured value isn't known when this initialiser runs (the payload hasn't
  // loaded), so seeding the string from it is impossible; an empty override that resolves later
  // is, and it keeps the field tracking the data until someone deliberately types over it.
  const [divStr, setDivStr] = useState(() => {
    const saved = loadSaved(isin).dividendYield;
    return saved == null ? '' : (saved * 100).toFixed(2);
  });

  /**
   * The one payload this panel is built from.
   *
   * ⚠ EXTRACTED FROM THE EFFECT SO A BUTTON CAN RUN IT AGAIN (2026-08-18). `blank` is the
   * difference between the two callers: on a company change the panel must clear (a valuation for
   * the previous instrument, on screen under this one's name, is the worst state available), but a
   * REFRESH must leave the figures up — blanking them for the length of a request makes the panel
   * flash and, worse, collapses its height, which is exactly what everything else here was fixed
   * to stop doing.
   */
  const load = useCallback(async (blank: boolean, signal?: AbortSignal) => {
    if (blank) { setMetrics(null); setCurrency(null); }
    setErr(null);
    // ⚠ `?cadence=annual` spelt out so this shares the Long Equity tab's cached payload — see the
    // same line in `QuickValuationTab`. It is the server's default, so the wire is unchanged; only
    // the cache key matches.
    const r = await apiFetch(
      `${API_URL}/api/earnings/by-isin/${encodeURIComponent(isin)}/metrics?cadence=annual`,
      { signal });
    if (r.status === 404) { setMetrics([]); return 'no financials for this company'; }
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    // ⚠ A CANCELLED RUN MUST NOT WRITE. A cacheable read is SHARED, so aborting one caller does
    // not stop the request — it resolves anyway, and without this check the panel would repaint
    // from a refresh the reader had already stopped.
    if (signal?.aborted) return 'cancelled';
    setMetrics((b?.metrics ?? []) as MetricRow[]);
    setCurrency(b?.currency ?? null);
    return undefined;
  }, [isin]);

  // ⚠ THE MOUNT LOAD KEEPS ITS OWN try/catch — it is not a job, it is the panel appearing, and a
  // toast for "this tab opened" is noise. `startLocalJob` handles the refresh path's failures.
  useEffect(() => {
    void (async () => {
      try { await load(true); } catch (e) {
        setErr(e instanceof Error ? e.message : String(e));
      }
    })();
  }, [load]);

  /**
   * Re-read the payload, past the cache.
   *
   * ⚠⚠ THE INVALIDATION IS THE WHOLE POINT. `/by-isin/{isin}/metrics` is on `readCache`'s
   * allowlist with a ten-minute TTL, so a plain re-fetch is answered from memory and the button
   * does nothing at all — visibly nothing, since the figure it was pressed to move stays put. The
   * cache is dropped whole rather than by key: `readKey` folds in the view-as-user flag, and a
   * near-miss there fails the same silent way.
   *
   * ⚠ IT CANNOT MAKE OUR STORED PRICE NEWER THAN INGEST HAS MADE IT. This re-reads what the server
   * holds; if the price series itself is behind, that is a pipeline question and the ⓘ says so
   * rather than letting the button imply otherwise.
   */
  /**
   * Go to Yahoo for this instrument's missing bars, then re-read the panel.
   *
   * ⚠⚠ IT ACTUALLY FETCHES NOW. It used to drop the read cache and re-request our own metrics
   * payload, which brings back whatever the ingest last stored — so on a company the pipeline had
   * not reached, the button was honest work with no possible effect: the date it was pressed to
   * move could not move. `…/latest-close/isin/{isin}/refresh` asks YAHOO for the gap.
   *
   * ⚠ THE GAP, NOT THE SERIES — see `_pull_latest_close`. The server opens Yahoo's window five
   * days before our newest stored bar, so refreshing a two-day-old price downloads a handful of
   * bars rather than the decades a full `store_series` would. That distinction is the difference
   * between a button and a page that hangs, and it is why this is a dedicated endpoint rather than
   * a flag on the cheap GET every card fires on mount.
   *
   * ⚠ THEN THE PANEL RE-READS, AND THE CACHE MUST BE DROPPED BETWEEN THE TWO. The metrics payload
   * is on `readCache`'s allowlist with a ten-minute TTL, so a re-request straight after the write
   * is answered from memory with the price we just replaced — the fetch would succeed and the
   * figure would not move, which is indistinguishable from the fetch having failed.
   */
  const refreshPrice = useCallback(() => {
    setJobId(startLocalJob(
      `${name ?? isin} — share price`, 'valuation.price',
      async (signal) => {
        const r = await apiFetch(
          `${API_URL}/api/asset-pipeline/latest-close/isin/${encodeURIComponent(isin)}/refresh`
          + (currency ? `?currency=${encodeURIComponent(currency)}` : ''),
          { method: 'POST', signal });
        const b = await r.json().catch(() => null) as (LatestClose & { detail?: string }) | null;
        // ⚠ 404 IS AN ANSWER, NOT A FAILURE — no priced Yahoo listing, or an instrument we have
        // never ingested. Painting that red beside the real errors teaches the reader to ignore
        // both; the card says what it is and the panel keeps the price it already had.
        if (r.status === 404) return b?.detail ?? 'no priced Yahoo listing for this instrument';
        if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
        if (signal.aborted) return 'cancelled';
        // ⚠⚠ THE ANSWER IS THE RESPONSE, NOT A RE-READ. This endpoint returns the refreshed close
        // in the same shape the GET does, so the panel repaints from what it just fetched. The
        // previous version re-requested the METRICS payload instead — a different price series
        // entirely, which the write could not touch, so the toast went green and nothing moved.
        if (b?.date) setLive(b);
        // The cached GET for this ISIN now holds a superseded close; Quick Valuation reads it too.
        invalidateReadCache('refreshed the share price on the Deep Valuation tab');
        return b?.date ? `close of ${b.date}` : 'price re-read';
      }));
  }, [name, isin, currency]);

  /**
   * ⚠ NOT FETCHED UNTIL THE REPORTING CURRENCY IS KNOWN. The close is converted server-side into
   * the currency the EPS and cash-flow series are filed in; asking before we know it would get a
   * price in the listing's own currency, and a price divided into a figure filed in another is a
   * yield wrong by the exchange rate that still looks entirely plausible.
   *
   * ⚠ A 404 IS THE FALLBACK PATH, NOT AN ERROR — no priced Yahoo listing for this ISIN. The panel
   * keeps the GuruFocus close and the card says which one it is showing.
   */
  useEffect(() => {
    if (!currency) return;
    let alive = true;
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/asset-pipeline/latest-close/isin/${encodeURIComponent(isin)}`
          + `?currency=${encodeURIComponent(currency)}`);
        if (!alive) return;
        setLive(r.ok ? (await r.json()) as LatestClose : null);
      } catch { if (alive) setLive(null); }
    })();
    return () => { alive = false; };
  }, [isin, currency]);

  // ⚠ A SECOND REQUEST, AND THE ONLY ONE ON THIS TAB. These rates are scalars with no date, so
  // they never reach `metric_data` and cannot ride the metrics payload. Failure is silent by
  // design — the comparison column is context, and losing it must not take the panel with it.
  useEffect(() => {
    let alive = true;
    void (async () => {
      setGrowthEst(null);
      try {
        const r = await apiFetch(
          `${API_URL}/api/earnings/by-isin/${encodeURIComponent(isin)}/growth-estimates`);
        if (!r.ok) return;
        const b = await r.json().catch(() => null);
        if (alive) setGrowthEst((b?.fields ?? null) as GrowthEstimates | null);
      } catch { /* context only */ }
    })();
    return () => { alive = false; };
  }, [isin]);

  const today = new Date().toISOString().slice(0, 10);
  const src = useMemo(() => egmSource(metrics ?? [], today), [metrics, today]);

  const dcfSrc = useMemo(() => reverseDcfSource(metrics ?? []), [metrics]);

  // Blank → the measured yield; typed → the reader's. Resolved here so `calculateEGM` only ever
  // sees one number and the panel and the model cannot disagree about which it used.
  const divOverride = divStr.trim() === '' ? null : parseFloat(divStr) / 100;
  const yieldUsed = divOverride != null && Number.isFinite(divOverride)
    ? divOverride : src.dividendYield;

  const assumptions: EgmAssumptions = useMemo(() => {
    const n = (s: string, fallback: number) => {
      const v = parseFloat(s);
      return Number.isFinite(v) ? v : fallback;
    };
    return {
      growthRate: n(growthStr, EGM_DEFAULTS.growthRate * 100) / 100,
      dividendYield: yieldUsed,
      exitPE: n(exitStr, EGM_DEFAULTS.exitPE),
      hurdleRate: n(hurdleStr, EGM_DEFAULTS.hurdleRate * 100) / 100,
      years: EGM_DEFAULTS.years,
    };
  }, [growthStr, exitStr, hurdleStr, yieldUsed]);

  // Persist per instrument, but only once the payload has loaded — writing on mount would stamp
  // the defaults over a saved override before the load effect has restored it.
  useEffect(() => {
    if (typeof window === 'undefined' || metrics == null) return;
    try {
      window.localStorage.setItem(KEY(isin), JSON.stringify({
        growthRate: assumptions.growthRate, exitPE: assumptions.exitPE,
        hurdleRate: assumptions.hurdleRate,
        // Only the override — see `loadSaved`.
        ...(divOverride != null && Number.isFinite(divOverride) ? { dividendYield: divOverride } : {}),
      }));
    } catch { /* a full or blocked localStorage must not take the panel down */ }
    // ⚠ `divOverride` is a dep in its own right. Typing the measured yield in by hand leaves
    // `assumptions.dividendYield` unchanged — same number — so without this the effect would not
    // re-run and the override would never be written; it would silently revert on reopen.
  }, [isin, assumptions, metrics, divOverride]);

  const reset = useCallback(() => {
    setGrowthStr((EGM_DEFAULTS.growthRate * 100).toFixed(1));
    setExitStr(String(EGM_DEFAULTS.exitPE));
    setHurdleStr((EGM_DEFAULTS.hurdleRate * 100).toFixed(1));
    setDivStr('');                                  // back to the measured yield, not to zero
  }, []);

  /**
   * The price the panel shows and the model computes from, and where it came from.
   *
   * ⚠ THE yfinance CLOSE WINS AND THE GURUFOCUS ONE IS THE FALLBACK — in that order because the
   * label says "now": the first is refreshable from this row, the second moves only when the
   * GuruFocus ingest runs. `close_in` (converted into the reporting currency) is required, not
   * `close`: the raw one may be in the listing's own currency, and dividing a dollar EPS by a euro
   * price is a multiple wrong by the exchange rate that still looks plausible.
   */
  const livePrice = live?.close_in != null && live.close_in > 0 ? live.close_in : null;
  const price = livePrice ?? src.price;
  const priceDate = livePrice != null ? live!.date : src.priceDate;
  const priceFromYahoo = livePrice != null;

  /**
   * How many calendar days old the stored close is.
   *
   * ⚠ CALENDAR, NOT TRADING, DAYS — and the threshold is set for it. A Friday close read on Monday
   * is three calendar days old and perfectly current, so anything under a week is normal; past
   * that the series is genuinely behind. Counting trading days properly needs an exchange calendar
   * this panel has no business carrying for one badge.
   */
  const priceAgeDays = priceDate == null ? null
    : Math.floor((Date.parse(`${today}T00:00:00Z`) - Date.parse(`${priceDate}T00:00:00Z`))
      / 86_400_000);
  const priceStale = priceAgeDays != null && priceAgeDays > 7;

  const r = calculateEGM({ ...src, price }, assumptions);
  const isDefault = assumptions.growthRate === EGM_DEFAULTS.growthRate
    && assumptions.exitPE === EGM_DEFAULTS.exitPE
    && assumptions.hurdleRate === EGM_DEFAULTS.hurdleRate
    && divOverride == null;

  // The multiple the price implies on the consensus EPS — the vendor's forward P/E is a separate
  // reading and the two need not agree. Surfaced in the tooltip rather than substituted.
  const impliedPE = price != null && src.epsNextFY != null && src.epsNextFY > 0
    ? price / src.epsNextFY : null;

  /** `2026-07-31` reads as a database key; a card is read by a person. */
  const onDate = (iso: string | null) => (iso == null ? 'n/a'
    : new Date(`${iso}T00:00:00Z`).toLocaleDateString('en-GB',
      { day: 'numeric', month: 'long', year: 'numeric', timeZone: 'UTC' }));

  const ccy = currency ? `${currency} ` : '';
  const bareMoney = (v: number | null) => (v == null ? 'n/a' : v.toFixed(2));
  const money = (v: number | null) => (v == null ? 'n/a' : `${ccy}${bareMoney(v)}`);
  const pct1 = (v: number | null) => (v == null ? 'n/a' : `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`);
  const mult = (v: number | null) => (v == null ? 'n/a' : `${v.toFixed(1)}x`);

  if (err) return <p className="text-xs text-neg-300 py-16 text-center">{err}</p>;
  if (metrics == null) return <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>;

  return (
    <div className="space-y-4">
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-4 min-w-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        {/* ⚠ THE HORIZON STAYS, AND IT IS THE PART THAT CANNOT GO. With the subtitle removed
            nothing else on the panel says how long the figures run for, and an annualised return
            with no window is not a smaller claim than a wrong one — it is an unreadable one. It
            keeps the faint, smaller weight so the NAME is still what the eye lands on.
            ⚠ INTERPOLATED, NEVER TYPED. `assumptions.years` is fixed at 10 today (the EGM has no
            horizon field, unlike the reverse DCF below), but a hardcoded "10" in a heading is a
            caption that silently stops being true the day one is added. */}
        <h4 className="text-base font-semibold text-fg-strong">
          Expected Growth Model (EGM)
          <span className="ml-1.5 text-[12px] font-normal text-fg-faint">
            for a {assumptions.years}-year horizon
          </span>
        </h4>
        {/* ⚠ NO SUBTITLE — REMOVED 2026-08-18, and the bridge below is why. It read
            "earnings growth + dividend yield + change in the multiple, over 10 years", which was
            the right three drivers, the wrong operator (they compound; the sum is +6.2% against an
            answer of +5.8%) and, once the bridge landed, a prose restatement of the three rows
            directly beneath it. A caption that names what the next element already shows is text
            the reader has to read twice to discover they did not need it. */}
      </div>

      {/**
        * ⚠⚠ INPUT LEFT, OUTPUT RIGHT — ONE PAIR OF RECTANGLES, EQUAL WIDTH AND HEIGHT.
        *
        * It was two stacked bordered boxes of different widths (a `max-w-md` bridge over a
        * full-width assumptions block) with a bare line of prose floating between them, in the
        * order output-then-input. Three shapes, three widths, and the causal direction running up
        * the screen. A model with four inputs and two outputs has exactly one honest shape: what
        * you assume, and what that produces, side by side.
        *
        * ⚠ THE HEIGHTS MATCH FOR FREE — grid children stretch, so neither box can end up the odd
        * one out as the fair-value line appears or a hint wraps. `justify-center` on each body
        * then keeps the content optically centred in whichever box is the shorter of the two,
        * rather than pinned to the top with a pool of space beneath it.
        *
        * ⚠ ONE COLUMN BELOW `md`. Side by side at 320px would put a 16-character label, an input
        * and a `%` into about 130px.
        */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 items-stretch">

        {/* ── INPUT ────────────────────────────────────────────────────────────────────────── */}
        <div className="flex flex-col rounded-lg border border-neutral-800/40 bg-inset p-3">
          <div className="flex items-center gap-2">
            <span className="text-[11px] uppercase tracking-wide text-fg-faint">Input</span>
            {/* ⚠ `Reset`, not `Reset to defaults` — it is only ever on screen while something IS
                off-default, so the qualifier answers a question nobody can be asking.
                ⚠⚠ RENDERED ALWAYS, HIDDEN WITH `invisible`. Mounting it on the first keystroke
                made the header row taller (a bordered button against a bare label), which grew
                this box, which grew the OUTPUT box beside it through the grid's stretch — the
                whole panel jumped because someone typed a digit. `visibility: hidden` reserves the
                geometry and still removes it from the tab order and the accessibility tree, which
                a `disabled` button would not. */}
            <button type="button" onClick={reset} aria-hidden={isDefault} tabIndex={isDefault ? -1 : 0}
              title="Put every assumption back to its default"
              className={`ml-auto rounded border border-neutral-700 px-1.5 py-0.5 text-[11px] text-fg-soft hover:bg-overlay/5 ${
                isDefault ? 'invisible' : ''}`}>
              Reset
            </button>
          </div>

          <div className="flex flex-col divide-y divide-neutral-800/30">
            <Field label="Growth rate" value={growthStr} onChange={setGrowthStr} suffix="%"
              info={<InfoTip content={<AspectCard
                what="Assumed growth in earnings per share."
                where={`Your input. Default ${(EGM_DEFAULTS.growthRate * 100).toFixed(0)}% — a house assumption, not this company's${src.analystGrowth5Y != null ? `; analysts imply ${(src.analystGrowth5Y * 100).toFixed(1)}%` : ''}.`}
                when={`Every year, for ${assumptions.years} years.`}
                how="Percent per year. Raw input, no formula." />} />}
              hint={src.analystGrowth5Y != null
                ? `${(src.analystGrowth5Y * 100).toFixed(1)}%` : null}
              hintTitle={'Analysts’ implied growth — the CAGR of the consensus EPS estimates, not '
                + 'a published long-term rate. Click to use it.'}
              onUseHint={src.analystGrowth5Y != null
                ? () => setGrowthStr(((src.analystGrowth5Y as number) * 100).toFixed(1)) : undefined} />
            {/* ⚠ THE CHIP IS THE FIGURE, NOT A SENTENCE ABOUT IT. `5y median P/E: 57.3` under a
                field labelled `Exit P/E` repeated the label an inch above it and named a source
                the hover can carry. */}
            <Field label="Exit P/E" value={exitStr} onChange={setExitStr} step="0.5"
              info={<InfoTip content={<AspectCard
                what="Assumed P/E when you sell."
                where={`Your input. Default ${EGM_DEFAULTS.exitPE}x — a house assumption, not this company's${src.medianPE5Y != null ? `; its 5-year median is ${src.medianPE5Y.toFixed(1)}x` : ''}.`}
                when={`End of year ${assumptions.years}.`}
                how={`A multiple, not a percent. Raw input, no formula${src.forwardPE != null && src.forwardPE > 0 ? ` — the rerating leg is ${src.forwardPE.toFixed(1)}x → ${assumptions.exitPE.toFixed(1)}x` : ''}.`} />} />}
              hint={src.medianPE5Y != null ? src.medianPE5Y.toFixed(1) : null}
              hintTitle="This company’s own median P/E over the last five years. Click to use it."
              onUseHint={src.medianPE5Y != null
                ? () => setExitStr((src.medianPE5Y as number).toFixed(1)) : undefined} />
            <Field label="Hurdle rate" value={hurdleStr} onChange={setHurdleStr} suffix="%"
              info={<InfoTip content={<AspectCard
                what="The return you require."
                where={`Your input. Default ${(EGM_DEFAULTS.hurdleRate * 100).toFixed(0)}% — a house assumption; there is no company version of it.`}
                when="Per year."
                how="Percent per year. Raw input; it sets the fair value, not the expected return." />} />} />
            {/* ⚠ AN ASSUMPTION, NOT A READING. The model applies this yield in EVERY one of the
                ten years, so it is a claim about the next decade; the measured figure is only its
                default. Blank = use what GuruFocus reports. */}
            <Field label="Dividend yield" value={divStr} onChange={setDivStr} suffix="%"
              info={<InfoTip content={<AspectCard
                what="Assumed dividend yield."
                where={src.dividendYield != null
                  ? `GuruFocus — ${(src.dividendYield * 100).toFixed(2)}% reported.`
                  : 'GuruFocus reports none, so 0% is assumed.'}
                when={`Every year, for ${assumptions.years} years.`}
                how="Percent per year. Raw data, no formula." />} />}
              placeholder={src.dividendYield == null ? '0.00' : (src.dividendYield * 100).toFixed(2)}
              hint={src.dividendYield == null ? null
                : `${(src.dividendYield * 100).toFixed(2)}%`}
              hintTitle={divOverride != null
                ? 'The yield GuruFocus reports. Click to go back to it.'
                : 'The yield GuruFocus reports — in use.'}
              onUseHint={divOverride != null ? () => setDivStr('') : undefined} />
          </div>

          {/* ⚠ ONE CONTROL, NOT A LABEL PLUS AN AFFORDANCE BESIDE IT. It read `Assumptions`
              (a dotted-underlined button) followed by `raw data ↗` (a static span) — two pieces of
              text for one click, and the half that looked like the link was not the button. */}
          <button type="button" onClick={() => setShowWorking(true)}
            title="Show the raw data behind these defaults"
            className="mt-1.5 self-start text-[11px] text-fg-faint underline decoration-dotted underline-offset-2 hover:text-fg-strong">
            raw data ↗
          </button>
        </div>

        {/* ── OUTPUT ───────────────────────────────────────────────────────────────────────── */}
        <div className="flex flex-col rounded-lg border border-neutral-800/40 bg-inset p-3">
          <span className="text-[11px] uppercase tracking-wide text-fg-faint">Output</span>

          <div className="flex flex-1 flex-col justify-center">
            {/**
              * ⚠⚠ THE TABLE IS ALWAYS THE SAME FOUR ROWS. It used to swap for a paragraph whenever
              * `calculateEGM` refused — and refusing is something the READER can cause: type `0`
              * into Exit P/E and the model has no multiple to rerate to, so the entire output box
              * changed shape mid-keystroke. Worse, the paragraph blamed a missing forward P/E — a
              * DATA fault — for what was a four-character edit.
              *
              * So the rows are constant and only the VALUES go `n/a`. The reason moves to the total
              * row's ⓘ, which is present either way: the SHAPE of the panel stops being a signal,
              * which is what lets the numbers be one.
              */}
            <table className="w-full table-fixed text-[12px]">
              {/* The last column is the ⓘ slot — see `Field`. Empty on every row with no
                  explanation, which is what holds the ones that have to a single vertical line. */}
              <colgroup>
                <col />
                <col className="w-[4.75rem]" />
                <col className="w-[3.75rem]" />
                <col className="w-9" />
              </colgroup>
              <tbody>
                {(r.bridge?.legs ?? [{ key: 'growth' as const }, { key: 'yield' as const },
                  { key: 'multiple' as const }]).map((leg) => {
                  const rate = 'rate' in leg ? leg.rate : null;
                  const factor = 'factor' in leg ? leg.factor : null;
                  return (
                    <tr key={leg.key}>
                      <td className="truncate py-0.5 text-fg-muted">
                        {leg.key === 'growth' ? 'Earnings growth'
                          : leg.key === 'yield' ? 'Dividend yield'
                            // ⚠ THE ENDPOINTS COME OFF THE LEG WHERE THERE IS ONE — they travel with
                            // the arithmetic (see `EgmLeg`), so a label can never name a different
                            // pair than the figure beside it was computed from. With no bridge
                            // nothing was computed, so the fields themselves are the only source.
                            : (
                              <>Multiple <span className="font-mono text-fg-soft">
                                {mult('from' in leg ? leg.from ?? null : src.forwardPE)}
                                {' → '}
                                {mult('to' in leg ? leg.to ?? null : assumptions.exitPE)}
                              </span></>
                            )}
                      </td>
                      {/* ⚠ NO `/yr` ON THE LEGS. Every row here is annualised, so repeating the unit
                          four times states one fact four times; it is said ONCE, on the answer,
                          where a reader taking only that number away still gets it. */}
                      <td className={`py-0.5 pl-2 text-right font-mono tabular-nums ${
                        (rate ?? 0) >= 0 ? 'text-fg-soft' : 'text-neg-400'}`}>
                        {rate == null ? 'n/a' : pct1(rate)}
                      </td>
                      <td className="py-0.5 pl-2 text-right font-mono tabular-nums text-fg-faint">
                        {factor == null ? '—' : `×${factor.toFixed(3)}`}
                      </td>
                      <td />
                    </tr>
                  );
                })}
                <tr className="border-t border-neutral-700/60">
                  <td className="truncate pt-1 font-medium text-fg-strong">Expected return</td>

                  <td className={`pt-1 pl-2 text-right font-mono tabular-nums font-semibold ${
                    (r.bridge?.rate ?? 0) >= 0 ? 'text-pos-500' : 'text-neg-500'}`}>
                    {r.bridge == null ? 'n/a'
                      : <>{pct1(r.bridge.rate)}<span className="font-normal text-fg-faint">/yr</span></>}
                  </td>
                  <td className="pt-1 pl-2 text-right font-mono tabular-nums text-fg-faint">
                    {r.bridge == null ? '—' : `×${r.bridge.factor.toFixed(3)}`}
                  </td>
                  <td className="pt-1">
                    {/* ⚠⚠ THE COMPOUNDING NOTE IS IN THE HOVER, IT DID NOT GO AWAY. It was two lines
                        of prose under the rule. True and load-bearing — but on a panel whose job is
                        "as little as possible", a permanent paragraph about an arithmetic subtlety
                        is the first thing a reader skips, and the `×` column beside it already
                        SHOWS the correct arithmetic to anyone checking. The visible design carries
                        the proof; the hover carries the warning — and, when the model refuses, the
                        reason, which is the only place left for it now the rows never change. */}
                    <InfoTip content={<AspectCard
                      what="Annual return these assumptions imply."
                      where="Computed from the three rows above."
                      when={`Per year, over ${assumptions.years} years.`}
                      how={r.bridge
                        ? `(1+growth) × (1+yield) × (exit ÷ forward P/E)^1/${assumptions.years} − 1. The rates compound, so the × column ties and the % column does not: ${pct1(r.bridge.sumOfRates)} added, ${pct1(r.bridge.rate)} compounded.`
                        : src.forwardPE == null || !(src.forwardPE > 0)
                          ? 'No usable forward P/E for this company — nothing to rerate from.'
                          : 'These assumptions have no compounding path — check the exit P/E, growth and hurdle on the left.'} />} />
                  </td>
                </tr>
              </tbody>
            </table>

            {/**
              * ⚠⚠ THE CONCLUSION IS TWO PRICES AND THE MOVE BETWEEN THEM. It was a `Fair value`
              * line — EPS × the hurdle-clearing multiple — which answers a DIFFERENT question
              * ("what may I pay?") in the same shape as this one ("what do I get?"), and the two
              * sat one above the other as competing verdicts. What the bridge above computes is a
              * return; the thing a return is about is a price you buy at and a price you sell at,
              * so those are the three rows.
              *
              * ⚠⚠ THE IMPLIED PRICE IS THE CAPITAL LEG AND THE RETURN IS THE TOTAL, AND ON A PAYER
              * THEY DO NOT TIE. Dividends are cash you were paid, not price you can sell at, so
              * compounding them into a "share price" would quote a figure no screen will ever
              * show. `priceReturn` is what the two prices give exactly; `totalReturn` adds the
              * dividends. They are IDENTICAL on a non-payer — which is most of the names this tab
              * is opened on, and precisely why a bug here would go unseen — so the dividend line
              * appears only when there is one to show.
              *
              * ⚠ `Fair value` HAS NOT BEEN DELETED, it moved into the hover on the implied price:
              * still an output, no longer a rival headline.
              */}
            {/* ⚠ UNCONDITIONAL, like the table above. `n/a` is a value; an absent table is a
                different panel, and `impliedPrice` goes null on inputs the reader types. */}
            {(
              /* Same fixed columns as the bridge above, and for the same reason — `USD 331.83`
                 and `USD 1,219.28` are different widths, and the reader changes which one it is. */
              <table className="mt-2 w-full table-fixed border-t border-neutral-800/40 pt-1.5 text-[12px]">
                {/* ⚠ THREE COLUMNS, MIRRORING THE BRIDGE ABOVE — label, figure, per-year rate. The
                    two price rows leave the third cell empty on purpose: it is what makes the
                    Return row's `/yr` read as a THIRD column rather than as something appended to
                    the figure beside it, and it keeps both tables on the same rhythm. */}
                <colgroup>
                  <col />
                  <col className="w-[6rem]" />
                  <col className="w-[3.75rem]" />
                  <col className="w-9" />
                </colgroup>
                <tbody>
                  <tr>
                    {/* ⚠ THE ⓘ AND THE ↻ SIT BEHIND THE FIGURE, NOT BEHIND THE NAME. Both are
                        about the NUMBER — where it came from, and how to re-read it — and after
                        the label they were separated from it by the whole width of the column.
                        In the trailing slot they also line up with every other ⓘ on the page. */}
                    <td className="truncate pt-1.5 text-fg-muted">Share price now</td>

                    <td className="pt-1.5 pl-2 text-right font-mono tabular-nums text-fg-soft">
                      {money(price)}
                    </td>
                    <td />
                    <td className="flex items-center gap-0.5 pt-1.5">
                      <InfoTip content={<AspectCard
                        what={`Closing stock price of ${name ?? isin}.`}
                        where={live === undefined ? 'Loading…'
                          : priceFromYahoo
                            ? `Yahoo Finance${live?.symbol ? ` — ${live.symbol}` : ''}.`
                            // ⚠ NAMED, NOT HIDDEN. Without a priced Yahoo listing the figure is
                            // GuruFocus's own close, which ↻ cannot move — and a card claiming
                            // Yahoo over it is how a button gets blamed for a missing instrument.
                            : 'GuruFocus — no priced Yahoo listing for this ISIN.'}
                        when={priceDate == null ? 'None stored.'
                          : onDate(priceDate) + (priceStale ? ` — ${priceAgeDays} days old` : '')}
                        how={priceFromYahoo
                          ? 'Raw data, no formula. ↻ fetches the newest closes from Yahoo.'
                          : 'Raw data, no formula. ↻ cannot refresh this one.'} />} />
                      {/* ⚠⚠ RENDERED ALWAYS, AND THE SAME SIZE IN ALL THREE STATES. Showing it only
                          when the price is stale would move the row the moment a refresh cleared
                          the staleness — the reader would press a button and watch it vanish along
                          with the layout under it. Amber when it is worth pressing, faint when it
                          is not, and the glyph never changes width. */}
                      {/**
                        * ⚠⚠ ONE CONTROL, THREE STATES — AND IT TURNS INTO THE CANCEL. The reader
                        * pressed the button HERE, so this is where the way to stop it belongs;
                        * sending them to the toast in the corner to undo something they started
                        * on this row is the same mistake as a Cancel that does nothing.
                        *   ↻  idle      — amber when the close is over a week old
                        *   ✕  running   — press again to abort the re-read
                        *   ⋯  cancelling — the window after the press, while the fetch unwinds
                        *
                        * ⚠ IT KEYS OFF THE TOAST STORE, NOT A LOCAL FLAG. `cancelRequested` flips
                        * on the press and `status` follows when the work actually stops; a private
                        * boolean would have to be kept in step with both and would be wrong in
                        * exactly the window this button exists to describe.
                        *
                        * ⚠ ONE CHARACTER IN EVERY STATE, in a fixed-width box. A glyph that
                        * changed width would move the figure beside it at the moment of the press
                        * — the layout rule the rest of this panel was fixed to.
                        */}
                      <button type="button"
                        onClick={() => (refreshing && jobId ? void cancelJob(jobId) : refreshPrice())}
                        disabled={cancelling}
                        aria-label={refreshing ? 'Cancel the re-read' : 'Re-read the stored close'}
                        title={cancelling ? 'Cancelling…'
                          : refreshing ? 'Re-reading — press to cancel'
                            : priceStale ? 'This close is over a week old — re-read it'
                              : 'Re-read the stored close'}
                        className={`ml-1 inline-block w-3.5 text-center align-middle text-[11px] leading-none ${
                          cancelling ? 'cursor-wait text-fg-faint'
                            : refreshing ? 'text-warn-400 hover:text-neg-400'
                              : priceStale ? 'text-warn-400 hover:text-warn-300'
                                : 'text-fg-faint hover:text-accent-400'}`}>
                        {cancelling ? '⋯' : refreshing ? '✕' : '↻'}
                      </button>
                    </td>
                  </tr>
                  <tr>
                    <td className="truncate py-0.5 text-fg-muted">Implied in {assumptions.years}y</td>

                    <td className="py-0.5 pl-2 text-right font-mono tabular-nums text-fg-strong font-semibold">
                      {money(r.impliedPrice)}
                    </td>
                    <td />
                    <td className="py-0.5">
                      <InfoTip content={<AspectCard
                        what="Share price at the end of the window."
                        where="Computed from the price and the two assumptions."
                        when={`${assumptions.years} years out.`}
                        how={`Price × (1+growth)^${assumptions.years} × (exit ÷ forward P/E). `
                          + `Price only — dividends are in the return below. `
                          + `Fair value at the hurdle ${money(r.fairValue)} (${mult(r.maxPE)}).`
                          // ⚠ THE ONE DISCLOSURE NOTHING ELSE CARRIES, kept to a single clause and
                          // shown only when true. This figure reruns from the VENDOR's forward
                          // P/E; when the price does not actually imply that multiple on the
                          // consensus EPS, the rerating leg starts from a multiple the market is
                          // not at. It has now outlived two hosts (the `Forward P/E now` tile and
                          // the fair-value line) — losing it in a copy edit is the live risk.
                          + (impliedPE != null && src.forwardPE != null
                            && Math.abs(impliedPE / src.forwardPE - 1) > 0.02
                            ? ` ⚠ Price ÷ EPS is ${impliedPE.toFixed(1)}x against the vendor's `
                              + `${src.forwardPE.toFixed(1)}x forward P/E.`
                            : '')} />} />
                    </td>
                  </tr>
                  <tr className="border-t border-neutral-800/40">
                    {/* ⚠⚠ THE `incl. div` NOTE IS IN THE HOVER, NOT INLINE. It appeared beside the
                        figure only when the two returns differed — i.e. the moment someone typed a
                        dividend yield — which reflowed the row that had just been edited. The ⓘ is
                        rendered unconditionally and its CONTENT changes instead, so a payer and a
                        non-payer are the same shape and only the words differ. */}
                    <td className="truncate pt-1 font-medium text-fg-strong">Return</td>

                    <td className={`pt-1 pl-2 text-right font-mono tabular-nums font-semibold ${
                      (r.priceReturn ?? 0) >= 0 ? 'text-pos-500' : 'text-neg-500'}`}>
                      {pct1(r.priceReturn)}
                    </td>
                    {/* ⚠⚠ `priceCagr`, NOT THE BRIDGE'S `expectedReturn`. On a dividend payer those
                        are different numbers — the total per year against the price leg per year —
                        and this cell sits beside two PRICES, so the only annual rate it may quote
                        is the one those two prices imply. Reaching for the bridge total here would
                        print a figure that cannot be derived from the subtraction above it, on the
                        one row whose whole job is to be that subtraction. They coincide on a
                        non-payer, which is most names here, so the wrong one would look right. */}
                    <td className={`pt-1 pl-2 text-right font-mono tabular-nums ${
                      (r.priceCagr ?? 0) >= 0 ? 'text-pos-500' : 'text-neg-500'}`}>
                      {r.priceCagr == null ? '—'
                        : <>{pct1(r.priceCagr)}<span className="text-fg-faint">/yr</span></>}
                    </td>
                    <td className="pt-1">
                      <InfoTip content={<AspectCard
                        what="Total price move over the window."
                        where="Computed from the two rows above."
                        when={`Over ${assumptions.years} years, not annualised.`}
                        how={r.totalReturn != null && r.priceReturn != null
                          && Math.abs(r.totalReturn - r.priceReturn) > 0.0001
                          ? `Implied price ÷ price now − 1. With dividends reinvested: ${pct1(r.totalReturn)}.`
                          : 'Implied price ÷ price now − 1. No dividend, so this is the whole return.'} />} />
                    </td>
                  </tr>
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>

      {showWorking && (
        // Handed the raw metric rows — the modal calls the SAME `…Working` functions the hints
        // above read their scalars from, so the table cannot disagree with the figure it explains.
        <EgmAssumptionsModal metrics={metrics} today={today} currency={currency}
          name={name} isin={isin} onClose={() => setShowWorking(false)} />
      )}
    </div>

    {/* The same question from the other end: the EGM asks what a set of assumptions is worth, the
        reverse DCF asks what the price already assumes. */}
    <ReverseDcfPanel src={dcfSrc} currency={currency} metrics={metrics} growthEst={growthEst}
      name={name} isin={isin} />
    </div>
  );
}
