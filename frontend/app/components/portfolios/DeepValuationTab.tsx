'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { Stat } from './MetricGrowthCard';
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

/** A percent-typed field: the user types 10 and the model gets 0.10. Kept as a STRING while typing
 *  so an intermediate "1." or "-" doesn't get parsed into a valuation and bounce the caret. */
function PctField({ label, value, onChange, hint, onUseHint, useLabel = 'use', placeholder }: {
  label: string; value: string; onChange: (v: string) => void;
  hint?: string | null; onUseHint?: () => void; useLabel?: string; placeholder?: string;
}) {
  return (
    <label className="flex flex-col gap-1 min-w-0">
      <span className="text-[12px] uppercase tracking-wide text-fg-muted">{label}</span>
      <span className="flex items-center gap-1">
        <input type="number" step="0.1" value={value} placeholder={placeholder}
          onChange={(e) => onChange(e.target.value)}
          className="w-20 bg-page border border-neutral-700 rounded-lg px-2 py-1 text-sm font-mono text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30" />
        <span className="text-xs text-fg-muted">%</span>
      </span>
      {hint && (
        <span className="text-[11px] text-fg-faint">
          {hint}
          {onUseHint && (
            <button type="button" onClick={onUseHint} className="ml-1 text-accent-400 hover:underline">{useLabel}</button>
          )}
        </span>
      )}
    </label>
  );
}

function NumField({ label, value, onChange, hint, onUseHint }: {
  label: string; value: string; onChange: (v: string) => void;
  hint?: string | null; onUseHint?: () => void;
}) {
  return (
    <label className="flex flex-col gap-1 min-w-0">
      <span className="text-[12px] uppercase tracking-wide text-fg-muted">{label}</span>
      <input type="number" step="0.5" value={value} onChange={(e) => onChange(e.target.value)}
        className="w-20 bg-page border border-neutral-700 rounded-lg px-2 py-1 text-sm font-mono text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30" />
      {hint && (
        <span className="text-[11px] text-fg-faint">
          {hint}
          {onUseHint && (
            <button type="button" onClick={onUseHint} className="ml-1 text-accent-400 hover:underline">use</button>
          )}
        </span>
      )}
    </label>
  );
}

export default function DeepValuationTab({ isin, name }: { isin: string; name?: string | null }) {
  const [metrics, setMetrics] = useState<MetricRow[] | null>(null);
  const [currency, setCurrency] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showWorking, setShowWorking] = useState(false);
  const [growthEst, setGrowthEst] = useState<GrowthEstimates | null>(null);

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

  useEffect(() => {
    let alive = true;
    void (async () => {
      setMetrics(null); setErr(null);
      try {
        // ⚠ `?cadence=annual` spelt out so this shares the Long Equity tab's cached payload — see
        // the same line in `QuickValuationTab`. It is the server's default, so the wire is
        // unchanged; only the cache key matches.
        const r = await apiFetch(
          `${API_URL}/api/earnings/by-isin/${encodeURIComponent(isin)}/metrics?cadence=annual`);
        if (r.status === 404) { if (alive) setMetrics([]); return; }
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setMetrics((b?.metrics ?? []) as MetricRow[]);
        setCurrency(b?.currency ?? null);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [isin]);

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

  const r = calculateEGM(src, assumptions);
  const isDefault = assumptions.growthRate === EGM_DEFAULTS.growthRate
    && assumptions.exitPE === EGM_DEFAULTS.exitPE
    && assumptions.hurdleRate === EGM_DEFAULTS.hurdleRate
    && divOverride == null;

  // The multiple the price implies on the consensus EPS — the vendor's forward P/E is a separate
  // reading and the two need not agree. Surfaced in the tooltip rather than substituted.
  const impliedPE = src.price != null && src.epsNextFY != null && src.epsNextFY > 0
    ? src.price / src.epsNextFY : null;

  const ccy = currency ? `${currency} ` : '';
  const money = (v: number | null) => (v == null ? 'n/a' : `${ccy}${v.toFixed(2)}`);
  const pct1 = (v: number | null) => (v == null ? 'n/a' : `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`);
  const mult = (v: number | null) => (v == null ? 'n/a' : `${v.toFixed(1)}x`);
  const ratio = (v: number | null) => (v == null ? 'n/a' : v.toFixed(2));

  if (err) return <p className="text-xs text-neg-300 py-16 text-center">{err}</p>;
  if (metrics == null) return <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>;

  return (
    <div className="space-y-4">
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-4 min-w-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        <h4 className="text-base font-semibold text-fg-strong">Expected return (EGM)</h4>
        <span className="text-[12px] text-fg-faint">
          earnings growth + dividend yield + change in the multiple, over {assumptions.years} years
        </span>
      </div>

      {/* Headline: what it is worth, against what it costs. */}
      <div className="flex items-end gap-4 flex-wrap">
        <div>
          <div className="text-[12px] uppercase tracking-wide text-fg-muted">Fair value</div>
          <div className="font-mono text-3xl font-semibold leading-tight text-fg-strong">
            {money(r.fairValue)}
          </div>
        </div>
        <div className={`font-mono text-2xl font-semibold leading-tight ${
          r.upside == null ? 'text-fg-muted' : r.upside >= 0 ? 'text-pos-500' : 'text-neg-500'}`}>
          {pct1(r.upside)}
        </div>
        <div className="text-xs text-fg-muted pb-1">
          price <span className="font-mono text-fg-soft">{money(src.price)}</span>
        </div>
      </div>

      <div className="flex flex-wrap gap-2">
        <Stat label={`Expected ${assumptions.years}-yr return`} value={pct1(r.expectedReturn)}
          color={chartTheme.accent}
          info={<InfoTip content={<AspectCard
            what="The annualised return from buying at TODAY's multiple and selling at the exit P/E."
            where={`Computed here: (1+growth)·(1+dividend yield)·(exit P/E ÷ forward P/E)^(1/${assumptions.years}) − 1.`}
            when={`Over ${assumptions.years} years.`}
            how="A forward P/E above the exit P/E drags it below the growth-plus-dividend base; below, the rerating adds to it. n/a for a loss-maker — there is no meaningful forward P/E to derate from." />} />} />
        {/* ⚠ THE DENOMINATOR, BESIDE THE THING IT IS COMPARED WITH. "Max P/E 49.4x" says nothing
            on its own, and reading it off the ratio tile means doing the division in your head. */}
        <Stat label="Forward P/E now" value={mult(src.forwardPE)}
          info={<InfoTip content={<AspectCard
            what="What the market charges today for next year's earnings — the multiple the max is measured against."
            where="GuruFocus `indicator_q_forward_pe_ratio`."
            when="Its latest observation."
            how={impliedPE != null && src.forwardPE != null
              && Math.abs(impliedPE / src.forwardPE - 1) > 0.02
              ? `⚠ Price ÷ next-FY EPS is ${impliedPE.toFixed(1)}x, ${((impliedPE / src.forwardPE - 1) * 100).toFixed(0)}% from the vendor's figure — different EPS basis or as-of date. The fair value uses the EPS, the expected return uses this, so the two tiles can disagree at the margin.`
              : 'A loss-maker has no meaningful forward P/E, and the two metrics that divide by it read n/a.'} />} />} />
        <Stat label="Max P/E you can pay" value={mult(r.maxPE)}
          info={<InfoTip content={<AspectCard
            what="The highest multiple you can pay today and still clear the hurdle rate."
            where={`Computed here: exit P/E · ((1+growth)(1+yield) ÷ (1+hurdle))^${assumptions.years}.`}
            when={`Over ${assumptions.years} years.`}
            how="When growth plus dividends outrun the hurdle, you are allowed to pay ABOVE the exit P/E — the compounding buys the premium back." />} />} />
        <Stat label="Max P/E ÷ current" value={ratio(r.peRatio)}
          tone={r.peRatio == null ? undefined : r.peRatio >= 1 ? 'text-pos-400' : 'text-fg-strong'}
          info={<InfoTip content={<AspectCard
            what="How much room there is to buy."
            where="Max P/E ÷ the forward P/E."
            when="Today."
            how="Above 1 means the price clears the hurdle at these assumptions; below 1 it does not." />} />} />
        {/* The other half of the fair value — maxPE is the multiple, this is what it multiplies.
            Moved up from the footnotes for the same reason the forward P/E was. */}
        <Stat label="Est. EPS next FY"
          value={src.epsNextFY == null ? 'n/a' : `${ccy}${src.epsNextFY.toFixed(2)}`}
          info={<InfoTip content={<AspectCard
            what="The consensus earnings per share for the next fiscal year — GuruFocus's “Estimated EPS for Next FY1 End”."
            where="Its analyst-estimate feed. The same figure its keyratios endpoint publishes under that name (verified equal on Apple: 8.760 both ways)."
            when={src.epsNextFYDate
              ? `Fiscal period ending ${src.epsNextFYDate.slice(0, 7)}.`
              : 'No future estimate ingested.'}
            how="Fair value is this × the max P/E. ⚠ Reported in the LISTING's currency — GuruFocus converts estimates per listing, so ASML reads €37.06 in Amsterdam and $42.83 on Nasdaq." />} />} />
        {/* No fair-value tile: it is the headline above, and the same number twice on one panel is
            two places for it to disagree. The row still shows both factors it comes from. */}
      </div>

      {/* The assumptions. Live, client-side, per instrument. */}
      <div className="rounded-lg border border-neutral-800/40 bg-inset p-3 space-y-2">
        <div className="flex items-center gap-2">
          {/* The header opens the working; the inputs below stay independently clickable. */}
          <button type="button" onClick={() => setShowWorking(true)}
            title="Show the raw data behind these defaults"
            className="text-[12px] uppercase tracking-wide text-fg-muted hover:text-fg-strong underline decoration-dotted underline-offset-2">
            Assumptions
          </button>
          <span className="text-[11px] text-fg-faint">raw data ↗</span>
          {!isDefault && (
            <button type="button" onClick={reset}
              className="ml-auto text-[12px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-soft hover:bg-overlay/5">
              Reset to defaults
            </button>
          )}
        </div>
        <div className="flex flex-wrap gap-4">
          <PctField label="Growth rate" value={growthStr} onChange={setGrowthStr}
            hint={src.analystGrowth5Y != null
              ? `analysts: ${(src.analystGrowth5Y * 100).toFixed(1)}%` : null}
            onUseHint={src.analystGrowth5Y != null
              ? () => setGrowthStr(((src.analystGrowth5Y as number) * 100).toFixed(1)) : undefined} />
          <NumField label="Exit P/E" value={exitStr} onChange={setExitStr}
            hint={src.medianPE5Y != null ? `5y median P/E: ${src.medianPE5Y.toFixed(1)}` : null}
            onUseHint={src.medianPE5Y != null
              ? () => setExitStr((src.medianPE5Y as number).toFixed(1)) : undefined} />
          <PctField label="Hurdle rate" value={hurdleStr} onChange={setHurdleStr} />
          {/* ⚠ AN ASSUMPTION, NOT A READING. The model applies this yield in EVERY one of the ten
              years, so it is a claim about the next decade; the measured figure is only its
              default. Blank = use what GuruFocus reports. */}
          <PctField label="Dividend yield" value={divStr} onChange={setDivStr}
            placeholder={src.dividendYield == null ? '0.00' : (src.dividendYield * 100).toFixed(2)}
            hint={src.dividendYield == null
              ? 'none reported — assumed 0%'
              : `reported: ${(src.dividendYield * 100).toFixed(2)}%${divOverride != null ? '' : ' (in use)'}`}
            onUseHint={divOverride != null ? () => setDivStr('') : undefined}
            useLabel="reset" />
        </div>
        {src.analystGrowth5Y != null && (
          // ⚠ NAMED FOR WHAT IT IS. GuruFocus's own `long_term_growth_rate_mean` is a scalar and is
          // never ingested (it has no date to sit on), so this is the CAGR the estimate series
          // implies — the same quantity GuruFocus publishes separately, and a near-twin that
          // diverges for high-growth names. Reference only; nothing computes from it.
          <p className="text-[11px] text-fg-faint">
            “analysts” is the CAGR implied by the consensus EPS estimates, not a published long-term rate.
          </p>
        )}
      </div>

      {/* The footnote row is gone: both figures it carried — forward P/E and next-FY EPS — now sit
          as tiles beside the numbers they feed, which is where a reader looks for them. */}

      {(src.forwardPE == null || src.epsNextFY == null) && (
        <p className="text-[12px] text-warn-300">
          {src.forwardPE == null && 'No forward P/E ingested — the expected return and the P/E ratio can’t be computed. '}
          {src.epsNextFY == null && 'No consensus EPS estimate ingested — there is no fair value to compare with the price. '}
          Everything that doesn’t depend on the missing input is still shown.
        </p>
      )}

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
