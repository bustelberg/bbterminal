'use client';

import { useMemo, useState } from 'react';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import {
  defaultDiscountRate, FORECAST_YEARS, impliedGrowth, marketCapOf,
  PERPETUITY_GROWTH,
} from './reverseDcf';
import { SOURCE_CODES, vendorName, type ReverseDcfSource } from './egmInputs';
import { forwardLegs, normalisedFcf } from './normalisedFcf';
// ⚠ THE SAME VOCABULARY THE RISK VIEWS USE. Every ⓘ stating a formula owes the reader the same
// expression with this company's operands in it, typeset by the same engine — see `workedFormula`.
// ⚠ THE EXPRESSIONS LIVE IN A PURE MODULE, not in this JSX: a LaTeX string is testable and a
// tooltip is not, and the failure they guard against (a bare `%` truncating the line) is invisible
// on screen. See `valuationFormulas` and its strict-mode render test.
import {
  workedCashFlowValued, workedForwardFcf, workedGrowthCapex, workedImpliedGrowth, workedMarketCap,
} from './valuationFormulas';
import ReverseDcfInputsModal from './ReverseDcfInputsModal';
import { type MetricRow } from './quickValuation';
// ⚠ RENDERED ONLY BY `AspectCard`/`Legend` — see `dynamicValue.v`. Every use below is in a card.
import { v } from '../../../lib/dynamicValue';
import { onDate } from './asOfLine';
import { useDeepValuationCopy } from './deepValuationCopy';

/**
 * Reverse DCF — a plain DCF run backwards. Grow free cash flow at `g` for `n` years, discount it,
 * add a terminal value; solve for the `g` at which that equals the market cap.
 *
 * Every assumption is on screen with a default, and every default that CAN come from the company's
 * own financials does — the discount rate is its published WACC. The two that cannot are marked as
 * conventions rather than dressed up as measurements: no company has its own perpetuity growth
 * rate, and none has its own forecast horizon.
 *
 * ⚠ BLANK MEANS "USE THE DEFAULT" in every field. Seeding the inputs with the computed values would
 * freeze them the moment the payload updates, and there would be no way to tell a number the reader
 * chose from one that merely arrived.
 */

/** 7% … 20%, the sweep shown under the answer. */
const SWEEP = Array.from({ length: 14 }, (_, i) => (7 + i) / 100);

/** GuruFocus's 3–5 year consensus rates, as PERCENTS. See the backend `_growth_estimates`. */
export type GrowthEstimates = {
  eps_3_5y: number | null;
  eps_nri_3_5y: number | null;
  ocf_ps_3_5y: number | null;
  revenue_3_5y: number | null;
};

/**
 * ⚠ THE DEFAULT IS THE VALUE, NOT A PLACEHOLDER. `value` is the override when one has been typed
 * and the computed default otherwise, so the box always shows the number actually in use — a
 * greyed placeholder reads as an empty field, and an empty field beside a valuation invites the
 * question of what it was computed from.
 *
 * Clearing the box hands control back to the default; `null` (never typed) and `''` (cleared) both
 * fall through to it.
 */
function Field({ label, value, onChange, suffix, info, tone, dim }: {
  label: string; value: string; onChange: (v: string) => void;
  suffix?: string; info?: React.ReactNode;
  /** `'total'` gives this row `DerivedRow`'s total weight — the rule and the border, so an
   *  editable total and a computed one are the same object at a glance. */
  tone?: 'total';
  /** ⚠ NOT DISABLED — DIMMED. A row that has stopped feeding the total is still a fact about the
   *  company and still the way back (clear the total and it takes over again), so it stays live
   *  and legible; what it loses is the claim to be part of the sum below it. */
  dim?: boolean;
}) {
  const total = tone === 'total';
  return (
    <label className={`flex items-center gap-2 py-1 ${total ? 'border-t border-neutral-800/40' : ''}${
      dim ? ' opacity-45' : ''}`}>
      <span className={`min-w-0 flex-1 truncate text-[12px] ${
        total ? 'text-fg-soft' : 'text-fg-muted'}`}>{label}</span>
      {/* ⚠ ONE WIDTH FOR EVERY BOX, whatever it holds. They were `w-32` / `w-36` / `w-20`, sized to
          their content — five inputs at three widths, so the column they form was a staircase.
          A market cap in millions is the widest thing here and it overflows INSIDE the box rather
          than widening it, which is what keeps the rows aligned and the panel from resizing as the
          reader types. */}
      <input type="number" value={value} onChange={(e) => onChange(e.target.value)}
        className={`w-24 shrink-0 rounded border border-neutral-700 bg-page px-1.5 py-0.5 text-right font-mono text-[12px] text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30${
          total ? ' font-semibold' : ''}`} />
      <span className="w-2 shrink-0 text-[11px] text-fg-muted">{suffix}</span>
      {/* ⚠⚠ THE ⓘ IS A TRAILING SLOT, NOT A SUFFIX ON THE LABEL — the same rule and the same width
          as the EGM panel above. Beside the label its x landed wherever that label happened to end
          (`Discount rate ⓘ` against `Target market cap (USD m) ⓘ` is most of the column apart), so
          five explanations read as scattered punctuation. Last slot, fixed width: every ⓘ on BOTH
          panels lands on one vertical line, because the boxes are equal-width grid cells and the
          two panels are the same card. */}
      <span className="flex w-9 shrink-0 items-center">{info}</span>
    </label>
  );
}

/**
 * A computed row in the input box — the same geometry as `Field`, without the input.
 *
 * ⚠⚠ THE WIDTHS ARE COPIED FROM `Field` ON PURPOSE (`w-24` value, `w-2` suffix, `w-9` info). These
 * rows sit interleaved with the editable ones, so a value slot even a pixel narrower puts the
 * numbers on two columns and the ⓘ on two more — the exact scattering the ⚠⚠ in `Field` records
 * fixing. The box border is the only thing that differs, because these cannot be typed in.
 *
 * ⚠ `tone` CARRIES NO MEANING BEYOND EMPHASIS. The total is the figure the model actually values,
 * so it is the one a reader's eye should land on; the two corrections are workings.
 *
 * ⚠⚠ EXCEPT `ref`, WHICH IS NOT PART OF THE SUM AND MUST NOT READ AS ONE. It carries the base the
 * panel did NOT take (see the base selector), and it sits BELOW the total — where a row with a
 * leading `−` or `+` would be read as one more term. Hence a dashed rule, the faintest ink, and a
 * label that starts with "vs": three signals that this is a comparison, not an addend. The
 * alternative was leaving it in a tooltip, which this panel's own history argues against — a
 * reader cannot check what they cannot see.
 */
function DerivedRow({ label, value, info, tone = 'step', dim }: {
  label: string; value: string; info?: React.ReactNode;
  /** ⚠ `sub` IS AN INPUT TO THE ROW BELOW IT, NOT A TERM OF THE SUM. Indented and unsigned: a `−`
   *  on the depreciation row would read as a deduction from the cash flow, which is the opposite
   *  of what it does (it makes the add-back SMALLER). */
  tone?: 'step' | 'total' | 'ref' | 'sub';
  /** See `Field`'s `dim`: a correction that is no longer reaching the total, kept legible. */
  dim?: boolean;
}) {
  const total = tone === 'total';
  const ref = tone === 'ref';
  const sub = tone === 'sub';
  return (
    <div className={`flex items-center gap-2 py-1 ${total ? 'border-t border-neutral-800/40' : ''}${
      ref ? 'border-t border-dashed border-neutral-800/30' : ''}${dim ? ' opacity-45' : ''}`}>
      <span className={`min-w-0 truncate text-[12px] flex-1 ${
        total ? 'text-fg-soft' : 'text-fg-faint'}${ref ? ' italic' : ''}${sub ? ' pl-3' : ''}`}>{label}</span>
      {/* ⚠ `border border-transparent` MATCHES `Field`'s INPUT BORDER. Without it this span is 2px
          shorter than an editable row's box, so the interleaved rows sit at slightly different
          heights and the numbers drift off each other's baseline — visible precisely because the
          four rows are meant to read as one sum. */}
      <span className={`w-24 shrink-0 rounded border border-transparent px-1.5 py-0.5 text-right font-mono text-[12px] ${
        total ? 'font-semibold text-fg-strong' : 'text-fg-muted'}`}>{value}</span>
      <span className="w-2 shrink-0" />
      <span className="flex w-9 shrink-0 items-center">{info}</span>
    </div>
  );
}

/**
 * ⚠⚠ THE THREE WAYS THE IMPLIED RATE AND THE ANALYST ROWS ARE NOT LIKE FOR LIKE. Two were already
 * stated on the individual rows (metric, horizon); the THIRD was not stated anywhere, and it is
 * the one a reader cannot infer from the labels.
 *
 * PER SHARE vs TOTAL. The model grows TOTAL free cash flow and solves it against the TOTAL market
 * cap. Both consensus rows are PER SHARE. A company retiring 2% of its shares a year grows per
 * share about 2pp faster than in total, for ever, with no change in the business — so the analyst
 * rows sit systematically above the implied rate on any buyback-heavy company and the price looks
 * more conservative than it is. Nothing on the row says so, because "EPS" and "OCF/sh" read as
 * metric names rather than as a different denominator.
 *
 * Declared once and appended to all three rows, so the three cards cannot drift apart.
 */
// ⚠ THE THREE SHARED SENTENCES MOVED TO `deepValuationCopy.dcf` (2026-09-01) — they were
// module-level template literals, which is exactly where a translated string cannot live: a
// language is a per-render fact and a module constant is evaluated once at import. They are still
// ONE sentence in ONE place, which was the point of hoisting them; the place is now the copy tree
// (`notLikeForLike`, `normOff`, `ttmNote`).


/**
 * ⚠⚠ THE WINDOW THE FOUR FLOW LINES ARE MEASURED OVER, AND IT HAS TO BE ON SCREEN. Free cash flow,
 * stock comp, capex and depreciation are trailing twelve months where four quarters exist and the
 * last fiscal year otherwise (`egmInputs.flowLegs`) — and the two can be far apart: measured on
 * Meta, capex is −69,691 on the last fiscal year against **−89,325** trailing, so the growth-capex
 * row reads 51,075 one way and 66,596 the other. A reader reconciling this panel against
 * GuruFocus's own page needs to know which of the two they are looking at, and the vendor's page
 * shows the trailing one.
 */

export default function ReverseDcfPanel({ src, currency, metrics, name, isin, growthEst, today }: {
  src: ReverseDcfSource; currency?: string | null;
  metrics: MetricRow[]; name?: string | null; isin: string;
  /** ⚠ PASSED IN, NOT READ FROM THE CLOCK, and it reaches the raw-data modal from here rather than
   *  being re-derived there — "which estimate is next year's" has to be the same question in the
   *  panel and in the table that claims to show what the panel read. Same convention as
   *  `egmSource(metrics, today)` one tab over. */
  today: string;
  /** Analysts' 3–5y consensus — context beside the implied rate, never an input to it. */
  growthEst?: GrowthEstimates | null;
}) {
  const t = useDeepValuationCopy();
  // ⚠ `null` MEANS "NEVER TYPED", which is not the same as `''` (cleared). The defaults are not
  // known when this component first renders — the payload has not loaded — so they cannot be
  // seeded into state; instead the input DISPLAYS the default until an override exists, which also
  // means the box keeps tracking the data if the payload updates underneath it.
  const [fcfStr, setFcfStr] = useState<string | null>(null);
  /**
   * The TOTAL, typed directly — the figure the model discounts, bypassing the base and both
   * corrections.
   *
   * ⚠⚠ IT IS A SECOND INPUT FOR ONE NUMBER, AND THE PANEL HAS TO SAY WHICH ONE WON. The base box
   * exists so a typed figure still flows through the corrections ("the FCF was really 9,000" and
   * having the stock comp silently stop being deducted would be a second, invisible edit). That
   * rule is intact — this is the other question: "value THIS, whatever the workings say", for a
   * normalised year you do not believe, a restructuring, or a figure from your own model. When it
   * is set the three rows above are dimmed and the corrections read `—`, so the sum on screen
   * never disagrees with the number beneath it.
   *
   * ⚠ CLEARING IT HANDS CONTROL BACK, like every other box: `null` (never typed) and `''`
   * (cleared) both fall through to the computed total, and the base above resumes feeding it.
   */
  const [totalStr, setTotalStr] = useState<string | null>(null);
  const [targetStr, setTargetStr] = useState<string | null>(null);
  const [rateStr, setRateStr] = useState<string | null>(null);
  const [perpStr, setPerpStr] = useState<string | null>(null);
  const [yearsStr, setYearsStr] = useState<string | null>(null);
  const [showRaw, setShowRaw] = useState(false);
  /**
   * WHICH cash flow the model starts from — next year's consensus, or the last one filed.
   *
   * ⚠ `null` MEANS "NEVER CHOSEN", like every input box above, so the default follows the data: a
   * company with a consensus gets the forward base, one without falls back to the filing. Seeding
   * this with a concrete mode would freeze it before the payload arrived and leave a company that
   * HAS estimates valued on last year's cash for no reason a reader could see.
   *
   * ⚠⚠ THE SWAP IS NOT SILENT, AND THAT IS THE WHOLE OF THE UI CHANGE. The base is named in the
   * field's own label WITH ITS PERIOD, the mode sits in the header as a control rather than a
   * setting, and the base NOT taken is a row of its own beneath the total. A reverse DCF that
   * quietly changed its starting cash flow would move every number on the panel — the implied
   * growth, the sweep, the comparison against the analyst rows — with nothing on screen to say
   * which of the company and the method had changed.
   */
  const [baseMode, setBaseMode] = useState<'forward' | 'reported' | null>(null);
  /**
   * ⚠ DEFAULT ON, LIKE THE LONG EQUITY TAB'S SBC BOX AND FOR THE SAME REASON: the uncorrected
   * figure is the flattering one on the SBC leg, and the reported one is the misleading one on the
   * capex leg (it charges a decade of expansion against a single year). A reader who never touches
   * this control should get the better number, not the raw one.
   *
   * ⚠ IT IS A MODE, NOT AN INPUT, and it still counts as `dirty` when switched OFF so `Reset`
   * genuinely restores the panel's defaults rather than most of them.
   */
  const [normalise, setNormalise] = useState(true);

  // The defaults, derived here so the boxes and the model read the same numbers.
  /**
   * ⚠⚠ THE BOX HOLDS THE **REPORTED** FIGURE AND THE CORRECTIONS ARE THEIR OWN ROWS BENEATH IT.
   * It used to hold the corrected number, which meant the one visible input silently disagreed
   * with the vendor's filing and the only way to find out was to open a tooltip. A reader cannot
   * check arithmetic they cannot see — so the base, each correction and the total are now four
   * lines, and the total is what the model values.
   *
   * ⚠ AN OVERRIDE TYPED INTO THE BOX STILL FLOWS THROUGH THE CORRECTIONS, because it replaces the
   * BASE rather than the result. Typing "the FCF was really 9,000" and having the stock comp
   * silently stop being deducted would be a second, invisible edit.
   */
  /**
   * Next year's free cash flow AND the capex/D&A pair that must accompany it.
   *
   * ⚠⚠ ONE CALL FOR BOTH, BECAUSE THEY ARE ONE DECISION. The vendor's consensus FCF nets a FORWARD
   * capex; our fallback nets the TRAILING one. Take the vendor's base with a trailing add-back and
   * the correction no longer cancels — Meta FY2026 lands at 46,872 instead of 57,250. See
   * `forwardLegs`, which owns the rule and the measurements.
   */
  const fwd = forwardLegs({
    ocfEstimate: src.ocfEstimate, fcfEstimate: src.fcfEstimate,
    ebitdaEstimate: src.ebitdaEstimate, ebitEstimate: src.ebitEstimate,
    capex: src.capex, dep: src.dep, normalise,
  });
  const fwdFcf = fwd.fcf;
  /** ⚠ WHOSE FIGURE IS ON SCREEN. The card has to say which of the two it took rather than leave
   *  the reader to reconcile a 39.6bn difference against GuruFocus's own page. */
  const fcfEstDirect = fwd.vendor;
  /** ⚠ THE FORWARD BASE ONLY WHEN THERE IS ONE. Fewer than a fifth of ACWI's members carry a
   *  consensus at all, so "forward by default" has to mean "forward where it exists" or most
   *  companies would open on an n/a where a perfectly good filed figure was sitting. */
  const base = baseMode ?? (fwdFcf != null ? 'forward' : 'reported');
  const forward = base === 'forward';
  /** The fiscal period the consensus is for — `2027-12-31` → `FY2027e`. */
  const estFy = src.ocfEstimateDate ? `FY${src.ocfEstimateDate.slice(0, 4)}e` : null;
  /** ⚠ THE WINDOW, NAMED. "Most recent fiscal year" was hard-coded on four cards and stopped being
   *  true the day the flow legs went trailing — a `when` that states the wrong period is worse
   *  than none, because it is checkable and wrong. See `TTM_NOTE`. */
  const flowWhen = src.flowBasis.ttm
    ? `Trailing twelve months to ${v(src.flowBasis.date ?? 'the latest quarter')}.`
    : `Most recent fiscal year${src.flowBasis.date ? `, ${v(src.flowBasis.date)}` : ''}.`;
  const defFcf = forward ? fwdFcf : src.fcf;
  const defTarget = marketCapOf(src);
  const defPerp = PERPETUITY_GROWTH;
  const defRate = defaultDiscountRate(src.wacc, defPerp);
  const defYears = FORECAST_YEARS;

  const num = (s: string | null) => {
    if (s == null || s.trim() === '') return null;
    const v = parseFloat(s);
    return Number.isFinite(v) ? v : null;
  };
  const baseFcf = num(fcfStr) ?? defFcf;
  // ⚠⚠ `fwd.capex` / `fwd.dep`, NOT `src.capex` / `src.dep` — on the vendor base these are the
  // FORWARD pair, and using the trailing lines here is precisely the split basis `forwardLegs`
  // exists to prevent. On the reported base and on the derived forward base they ARE the trailing
  // lines, so this is the same call it always was for those.
  const legCapex = forward ? fwd.capex : src.capex;
  const legDep = forward ? fwd.dep : src.dep;
  const norm = useMemo(
    () => normalisedFcf({ fcf: baseFcf, sbc: src.sbc, capex: legCapex, dep: legDep }),
    [baseFcf, src.sbc, legCapex, legDep]);
  /** The total the rows above add up to. ⚠ `?? baseFcf` covers the case where nothing was
   *  correctable; `normalisedFcf` already returns `used === reported` there, so this is a null
   *  guard only. */
  const computedFcf = normalise ? (norm.used ?? baseFcf) : baseFcf;
  /** ⚠ THE TYPED TOTAL WINS OVER EVERYTHING, including the corrections — see `totalStr`. */
  const totalOverride = num(totalStr);
  const overridden = totalOverride != null;
  /** What the model actually values. */
  const fcf = overridden ? totalOverride : computedFcf;
  const target = num(targetStr) ?? defTarget;
  const perpetuityGrowth = (num(perpStr) ?? defPerp * 100) / 100;
  const rate = (num(rateStr) ?? defRate * 100) / 100;
  const years = num(yearsStr) ?? defYears;

  // What each box shows: the override if one was typed, else the default as a plain number.
  const show = (s: string | null, def: number | null, dp = 0) =>
    (s != null ? s : def == null ? '' : def.toFixed(dp));

  const dirty = [fcfStr, totalStr, targetStr, rateStr, perpStr, yearsStr].some((s) => s != null)
    || !normalise || baseMode != null;
  const reset = () => {
    setFcfStr(null); setTotalStr(null); setTargetStr(null); setRateStr(null);
    setPerpStr(null); setYearsStr(null);
    setNormalise(true); setBaseMode(null);
  };

  const growth = useMemo(() => impliedGrowth(src, {
    years, perpetuityGrowth, discountRates: [rate], fcfOverride: fcf, targetOverride: target,
  })[0]?.impliedGrowth ?? null, [src, years, perpetuityGrowth, rate, fcf, target]);

  // The sweep. Same solver, same overrides — so a cell and the headline cannot disagree about the
  // rate they share.
  const sweep = useMemo(() => impliedGrowth(src, {
    years, perpetuityGrowth, discountRates: SWEEP, fcfOverride: fcf, targetOverride: target,
  }), [src, years, perpetuityGrowth, fcf, target]);

  const missing: string[] = [];
  if (target == null) {
    if (src.price == null) missing.push('price');
    if (src.sharesOutstanding == null) missing.push('share count');
  }
  if (fcf == null) missing.push('free cash flow');

  const mn = (v: number | null) => (v == null ? 'n/a'
    : `${currency ? `${currency} ` : ''}${Math.round(v).toLocaleString('en-US')}M`);
  /**
   * The same figure in the unit a reader actually thinks in.
   *
   * ⚠⚠ THE BOXES HOLD MILLIONS AND NOTHING SAID SO EXCEPT `(m)` IN A LABEL. `312400` in an input
   * is unreadable at a glance and is off by a factor of a thousand from what anyone means when
   * they say "market cap" — so every ⓘ restates its default BOTH ways, and the one that is easy to
   * misread is the one the box holds. GuruFocus files shares and free cash flow in millions, so
   * price × shares comes out in millions too (`reverseDcf.ts`); this is a display, never a value
   * anything computes from.
   */
  const scaled = (v: number | null) => {
    if (v == null) return 'n/a';
    const c = currency ? `${currency} ` : '';
    const a = Math.abs(v);
    if (a >= 1_000_000) return `${c}${(v / 1_000_000).toFixed(2)}tn`;
    if (a >= 1_000) return `${c}${(v / 1_000).toFixed(2)}bn`;
    return `${c}${v.toFixed(0)}M`;
  };

  // ⚠ The comparison the whole panel is for: the model says the price implies X, and these are
  // what anyone actually forecasts. Rendered only when at least one arrived — an empty pair of
  // columns headed "analysts" reads as "they expect nothing".
  const eps35 = growthEst?.eps_3_5y ?? null;
  const ocf35 = growthEst?.ocf_ps_3_5y ?? null;
  // ⚠ THE MEAN OF THE TWO THAT ARE THERE, AND NOTHING WHEN ONE IS MISSING. Averaging a present
  // figure with an absent one silently returns the present one under a label that claims it is a
  // blend of both — the reader would have no way to tell a two-source average from a one-source
  // one. A single number is shown in its own column already.
  const avg35 = eps35 != null && ocf35 != null ? (eps35 + ocf35) / 2 : null;
  const analysts: [string, number | null, string][] = [
    [t.dcf.analystEps, eps35, t.dcf.analystEpsTip + t.dcf.notLikeForLike],
    [t.dcf.analystOcf, ocf35, t.dcf.analystOcfTip + t.dcf.notLikeForLike],
    [t.dcf.analystAvg, avg35, t.dcf.analystAvgTip + t.dcf.notLikeForLike],
  ];
  const hasAnalysts = analysts.some(([, v]) => v != null);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        <h4 className="text-base font-semibold text-fg-strong">{t.dcf.title}</h4>
        <span className="text-[12px] text-fg-faint">
          {t.dcf.subtitle}
        </span>
      </div>

      {/**
        * ⚠⚠ INPUT LEFT, OUTPUT RIGHT — the same pair of rectangles the EGM panel above uses, and
        * deliberately the same shape: these two answer one question from opposite ends ("what are
        * these assumptions worth" / "what does the price already assume"), so a reader moving
        * between them should not have to re-learn where anything is.
        *
        * ⚠ THE HEIGHTS MATCH FOR FREE — grid children stretch — and `justify-center` keeps each
        * body optically centred in whichever box is the shorter.
        */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 items-stretch">

        {/* ── INPUT ────────────────────────────────────────────────────────────────────────── */}
        <div className="flex flex-col rounded-lg border border-neutral-800/40 bg-inset p-3">
          <div className="flex items-center gap-2">
            <span className="text-[11px] uppercase tracking-wide text-fg-faint">Input</span>
            {/* ⚠ RENDERED ALWAYS, HIDDEN WITH `invisible` — mounting it on the first keystroke
                makes the header row taller, which grows this box, which grows the OUTPUT box
                beside it through the grid's stretch. The whole panel would jump because someone
                typed a digit. `visibility: hidden` reserves the geometry and still takes it out of
                the tab order and the accessibility tree, which `disabled` would not. */}
            {/* ⚠ `ml-auto` MOVED HERE FROM `Reset`, so the group stays right-aligned as one. The
                checkbox is always mounted — it is not conditional on anything — so it cannot make
                the header row grow or shrink under the reader. */}
            {/* ⚠⚠ ALWAYS MOUNTED, AND DISABLED RATHER THAN HIDDEN when no consensus exists — the
                same height rule as the Reset button and the correction rows. A control that
                appears for some companies and not others makes this box change height, which
                changes the OUTPUT box beside it through the grid's stretch. Disabled, the option
                still says the forward base EXISTS and this company has no estimate for it, which
                is a fact about the company rather than an absence a reader has to infer. */}
            <label className="ml-auto flex items-center gap-1.5 text-[11px] text-fg-soft whitespace-nowrap"
              title={t.dcf.baseTitle}>
              {t.dcf.base}
              <select value={base} onChange={(e) => setBaseMode(e.target.value as 'forward' | 'reported')}
                className="rounded border border-neutral-700 bg-page px-1 py-0.5 text-[11px] text-fg-strong focus:border-accent-500">
                <option value="forward" disabled={fwdFcf == null}>
                  {fwdFcf == null ? t.dcf.nextFYNone : (estFy ?? t.dcf.nextFY)}
                </option>
                <option value="reported">{t.dcf.lastReported}</option>
              </select>
            </label>
            <label className="flex items-center gap-1.5 text-[11px] text-fg-soft cursor-pointer whitespace-nowrap"
              title={t.dcf.normaliseTitle}>
              <input type="checkbox" checked={normalise}
                onChange={(e) => setNormalise(e.target.checked)}
                className="accent-accent-600 w-3.5 h-3.5" />
              {t.dcf.normalise}
            </label>
            <button type="button" onClick={reset} aria-hidden={!dirty} tabIndex={dirty ? 0 : -1}
              title={t.dcf.reset}
              className={`rounded border border-neutral-700 px-1.5 py-0.5 text-[11px] text-fg-soft hover:bg-overlay/5 ${
                dirty ? '' : 'invisible'}`}>
              Reset
            </button>
          </div>

          <div className="flex flex-col divide-y divide-neutral-800/30">
            {/* ⚠ THE LABEL NAMES THE PERIOD, because the two bases differ by a YEAR and by nothing
                a reader can see in the number itself. `Free cash flow (USD m)` over a consensus
                figure is the same defect `priceDate` fixed on the EGM panel: a word making a claim
                about time, over a number carrying none. */}
            <Field dim={overridden}
              label={`${t.dcf.freeCashFlow}${forward && estFy ? ` ${estFy}` : forward ? ` ${t.dcf.nextFY}` : ''}${currency ? ` (${currency}m)` : ' (m)'}`}
              value={show(fcfStr, defFcf)} onChange={setFcfStr}
              info={<InfoTip content={<AspectCard
                what={!forward ? t.dcf.fcfWhatReported
                  : fcfEstDirect ? 'Next fiscal year\'s free cash flow, as forecast.'
                    : 'Next fiscal year\'s free cash flow, derived.'}
                where={!forward ? t.common.guruFocus(vendorName(SOURCE_CODES.fcf))
                  : fcfEstDirect ? `GuruFocus, ${v(vendorName(SOURCE_CODES.fcfEstimate))}.`
                    : `GuruFocus, ${v(vendorName(SOURCE_CODES.ocfEstimate))} less ${v(vendorName(SOURCE_CODES.capex))}.`}
                when={forward ? v(estFy ?? t.dcf.nextFiscalYear) : flowWhen}
                // ⚠ NO WORKED LINE ON THE REPORTED BASE, and that is the rule rather than an
                // omission: it is a figure the vendor filed, not an arithmetic anybody performed.
                // A formula over raw data fabricates a derivation — the same reason the four
                // assumption boxes below carry none.
                // ⚠ NO WORKED LINE ON THE VENDOR'S OWN FIGURE — it is a filing, not an
                // arithmetic anybody performed here. Same rule as the reported base.
                worked={!forward || fcfEstDirect ? ''
                  : workedForwardFcf(src.ocfEstimate, src.capex, fwdFcf)}
                legend={!forward || fcfEstDirect || fwdFcf == null ? undefined : [
                  { sym: String.raw`OCF_{\text{est}}`,
                    is: t.dcf.legend.ocfEst(estFy ?? t.dcf.nextFiscalYear) },
                  { sym: 'C', is: t.dcf.legend.capexFiled },
                ]}
                how={(forward
                  ? fcfEstDirect ? t.dcf.fcfHowDirect : t.dcf.fcfHowDerived
                  : t.dcf.fcfHowReported)
                  + (defFcf != null ? t.dcf.inMillionsIs(mn(defFcf), scaled(defFcf))
                    : t.dcf.inMillions)} />} />} />

            {/* ⚠⚠ THE CORRECTIONS ARE ROWS, NOT A TOOLTIP. They were both folded into the figure
                above with the working hidden behind an ⓘ, which meant the one number on screen
                disagreed with the company's filing and nothing said so. Three lines and a total
                cost four rows of height and make the arithmetic checkable at a glance.

                ⚠ ALWAYS MOUNTED, INCLUDING WHEN `Normalise` IS OFF — they then read `—` and the
                total falls back to the reported figure. Hiding them would resize the input box,
                which resizes the OUTPUT box beside it through the grid's stretch, so the whole
                panel would jump on a checkbox. Same rule as the Reset button above. */}
            <DerivedRow label={t.dcf.rowSbc} dim={overridden}
              value={normalise && !overridden && norm.applied.sbc ? mn(norm.sbc) : '—'}
              info={<InfoTip content={<AspectCard
                what={t.dcf.cards.sbc.what}
                where={t.common.guruFocus(vendorName(SOURCE_CODES.sbc))}
                when={flowWhen}
                how={norm.applied.sbc ? t.dcf.sbcHow + t.dcf.normOff : t.dcf.sbcAbsent} />} />} />
            {/* ⚠⚠ THE TWO DIRECT FIGURES, ABOVE THE CORRECTION THEY MAKE. `+ Growth capex` alone is
                one number a reader cannot check against anything: it is a subtraction of two
                vendor lines, and the only way to reconcile it with GuruFocus's own page was to
                open the ⓘ. Shown as their own rows it is arithmetic anybody can do on screen —
                which is how the Meta basis mismatch surfaced (capex −69,691 on the last fiscal
                year against the −89,325 trailing figure the vendor prints).
                ⚠ NO LEADING SIGN AND INDENTED: these are the INPUTS to the row below, not two more
                terms of the sum above. A `−` on the depreciation row would read as a deduction
                from the cash flow, which is exactly what it is not. */}
            <DerivedRow tone="sub" dim={overridden}
              label={t.dcf.rowCapex}
              value={legCapex == null ? '—' : mn(Math.abs(legCapex))}
              info={<InfoTip content={<AspectCard
                what={t.dcf.cards.capex.what}
                where={t.common.guruFocus(vendorName(SOURCE_CODES.capex))}
                when={flowWhen}
                how={t.dcf.capexHow + t.dcf.ttmNote} />} />} />
            <DerivedRow tone="sub" dim={overridden}
              label={t.dcf.rowDA}
              value={legDep == null ? '—' : mn(legDep)}
              info={<InfoTip content={<AspectCard
                what={t.dcf.cards.da.what}
                where={t.common.guruFocus(vendorName(SOURCE_CODES.dep))}
                when={flowWhen}
                how={t.dcf.daHow + t.dcf.ttmNote} />} />} />
            <DerivedRow label={t.dcf.rowGrowthCapex} dim={overridden}
              value={normalise && !overridden && norm.applied.growthCapex ? mn(norm.growthCapex) : '—'}
              info={<InfoTip content={<AspectCard
                what={t.dcf.cards.growthCapex.what}
                where={t.dcf.cards.growthCapex.where}
                when={flowWhen}
                // ⚠ GATED ON `normalise` LIKE THE ROW ITSELF. With it off the row reads `—` because
                // the correction did not run; a tooltip still showing its arithmetic would be a
                // number the panel is not using, one hover away from a dash.
                worked={!normalise || overridden ? ''
                  : workedGrowthCapex(src.capex, src.dep, norm.growthCapex)}
                legend={!norm.applied.growthCapex ? undefined : [
                  // ⚠ THE BARS ARE IN THE FORMULA BECAUSE THE SIGN IS THE TRAP. The vendor files
                  // capex negative; written `C − D` the expression is always negative, always
                  // clamps to zero, and the add-back silently never happens on any company.
                  { sym: 'C', is: t.dcf.legend.C },
                  { sym: 'D', is: t.dcf.legend.D },
                ]}
                how={norm.applied.growthCapex
                  ? `${t.dcf.growthCapexAdded}

${t.dcf.growthCapexHow}${t.dcf.normOff}`
                  : t.dcf.growthCapexAbsent} />} />} />
            {/* ⚠⚠ EDITABLE, AND IT OVERRIDES THE THREE ROWS ABOVE IT. The base box answers "the
                cash flow was really X, now correct it"; this one answers "value X, whatever the
                workings say" — a normalised year you do not believe, a restructuring, a figure
                from your own model. Both exist because they are different questions, and the
                panel makes plain which one is live: type here and the rows above dim, their
                corrections read `—`, and the worked line disappears rather than explaining a
                total it no longer produces. Clearing it hands control straight back. */}
            <Field tone="total"
              label={`${t.dcf.cashFlowValued}${currency ? ` (${currency}m)` : ' (m)'}`}
              value={show(totalStr, computedFcf)} onChange={setTotalStr}
              info={<InfoTip content={<AspectCard
                what={overridden ? t.dcf.valuedWhatYours : t.dcf.valuedWhat}
                where={overridden ? t.dcf.valuedWhereYours : t.dcf.valuedWhere}
                when={overridden ? t.dcf.valuedWhenYours
                  : forward ? v(estFy ?? t.dcf.nextFiscalYear) : flowWhen}
                // ⚠ THE SYMBOLIC HALF CARRIES ONLY THE CORRECTIONS THAT RAN. A formula printing
                // `− S` over a company with no stock-comp line states an arithmetic that did not
                // happen — the same "an absent line is not a zero" rule the rows themselves keep,
                // one level up in the notation.
                worked={!normalise || overridden ? '' : workedCashFlowValued(
                  baseFcf, norm.applied.sbc ? norm.sbc : null,
                  norm.applied.growthCapex ? norm.growthCapex : null, fcf)}
                legend={!normalise || overridden || baseFcf == null ? undefined : [
                  { sym: 'F', is: forward ? t.dcf.legend.Fforward(estFy ?? t.dcf.nextFY)
                    : t.dcf.legend.Ffiled },
                  ...(norm.applied.sbc
                    ? [{ sym: 'S', is: t.dcf.legend.S as React.ReactNode }] : []),
                  ...(norm.applied.growthCapex
                    ? [{ sym: 'G', is: t.dcf.legend.G as React.ReactNode }] : []),
                ]}
                how={overridden
                  // ⚠ IT STATES WHAT IT REPLACED, IN NUMBERS. "Overridden" alone leaves the reader
                  // to remember what the panel would have said, which is the whole thing they are
                  // deciding against — and the row that held it is now dimmed and reads `—`.
                  ? `Base, corrections and Normalise all bypassed: the model discounts exactly ${v(mn(fcf))}.

The workings would have given ${v(computedFcf == null ? 'no figure' : mn(computedFcf))}. Clear the box to go back to them.`
                  : normalise
                    ? `${!norm.applied.sbc || !norm.applied.growthCapex
                      ? t.dcf.valuedHowPartial([!norm.applied.sbc ? t.dcf.correctionSbc : null,
                        !norm.applied.growthCapex ? t.dcf.correctionCapexDep : null]
                        .filter(Boolean).join(' / '))
                      : t.dcf.valuedHowAllRan}

Type a figure here to bypass them and value it directly.`
                    : t.dcf.valuedHowNormOff} />} />} />

            {/* ⚠⚠ THE BASE **NOT** TAKEN, ON SCREEN RATHER THAN IN A TOOLTIP. Switching base moves
                the implied growth, the whole sweep and the comparison against the analyst rows at
                once; without the other figure visible there is no way to tell a company that
                changed from a method that did. It sits below the total, in italic, behind a dashed
                rule and labelled "vs" — see `DerivedRow`'s `ref` tone for why all three. */}
            <DerivedRow tone="ref"
              label={forward
                ? 'vs last reported FCF'
                : `vs ${estFy ?? 'next FY'} FCF (derived)`}
              value={forward
                ? (src.fcf == null ? 'n/a' : mn(src.fcf))
                : (fwdFcf == null ? 'n/a' : mn(fwdFcf))}
              info={<InfoTip content={<AspectCard
                what={t.dcf.cards.baseNotUsed.what}
                where={forward ? t.common.guruFocus(vendorName(SOURCE_CODES.fcf))
                  : t.dcf.guruFocusLess(vendorName(SOURCE_CODES.ocfEstimate),
                    vendorName(SOURCE_CODES.capex))}
                when={forward ? flowWhen : v(estFy ?? t.dcf.nextFiscalYear)}
                how={(forward ? src.fcf : fwdFcf) == null
                  ? (forward
                    ? t.dcf.baseNotUsedNoFcf
                    : t.dcf.baseNotUsedNoConsensus)
                  : t.dcf.baseNotUsedCompare} />} />} />

            <Field label={`${t.dcf.targetMarketCap}${currency ? ` (${currency}m)` : ' (m)'}`}
              value={show(targetStr, defTarget)} onChange={setTargetStr}
              info={<InfoTip content={<AspectCard
                what={t.dcf.cards.solvedAgainst.what}
                where={t.dcf.cards.marketCap.where}
                // ⚠ TWO DATES, BECAUSE IT IS A PRODUCT OF TWO OBSERVATIONS and they are rarely
                // the same day: a close is daily, a diluted share count is a filing. One date over
                // both would date the market cap to whichever leg the label happened to name.
                when={[src.priceDate ? t.dcf.closeOn(onDate(src.priceDate)) : null,
                  src.sharesDate ? t.dcf.sharesOn(onDate(src.sharesDate)) : null,
                ].filter(Boolean).join(', ') || t.dcf.noDatesStored}
                worked={workedMarketCap(src.price, src.sharesOutstanding, defTarget)}
                legend={src.price == null || src.sharesOutstanding == null ? undefined : [
                  { sym: 'P_0', is: t.dcf.legend.p0 },
                  // ⚠ IN MILLIONS, WHICH IS WHY THE PRODUCT IS TOO. GuruFocus files the share
                  // count in millions, so price × shares lands in the same unit as the cash flow
                  // above it and no scaling happens anywhere in this panel.
                  { sym: 'N', is: t.dcf.legend.N },
                ]}
                how={t.dcf.cards.marketCap.how} />} />} />
            <Field label={t.dcf.rowDiscountRate} value={show(rateStr, defRate * 100, 1)} onChange={setRateStr}
              suffix="%"
              info={<InfoTip content={<AspectCard
                what={t.dcf.cards.discountRate.what}
                where={src.wacc != null && defRate === src.wacc
                  ? t.common.guruFocus(vendorName(SOURCE_CODES.wacc))
                  : t.dcf.houseDefault}
                when={src.waccDate == null ? t.dcf.noWaccStored
                  : v(onDate(src.waccDate))}
                how={t.dcf.cards.discountRate.how} />} />} />
            <Field label={t.dcf.rowPerpetuityGrowth} value={show(perpStr, defPerp * 100, 1)} onChange={setPerpStr}
              suffix="%"
              info={<InfoTip content={<AspectCard
                what={t.dcf.cards.perpetuityGrowth.what}
                where={t.dcf.cards.perpetuityGrowth.where}
                when={t.dcf.fromYearOnwards(String(defYears + 1))}
                how={t.dcf.cards.perpetuityGrowth.how} />} />} />
            <Field label={t.dcf.rowForecastYears} value={show(yearsStr, defYears)} onChange={setYearsStr}
              info={<InfoTip content={<AspectCard
                what={t.dcf.cards.forecastYears.what}
                where={t.dcf.cards.perpetuityGrowth.where}
                when={t.dcf.yearsOneToPlain(String(years))}
                how={t.dcf.cards.forecastYears.how} />} />} />
          </div>

          <button type="button" onClick={() => setShowRaw(true)}
            title={t.dcf.showFigures}
            className="mt-1.5 self-start text-[11px] text-fg-faint underline decoration-dotted underline-offset-2 hover:text-fg-strong">
            {t.dcf.rawData}
          </button>
        </div>

        {/* ── OUTPUT ───────────────────────────────────────────────────────────────────────── */}
        <div className="flex flex-col rounded-lg border border-neutral-800/40 bg-inset p-3">
          <span className="text-[11px] uppercase tracking-wide text-fg-faint">Output</span>

          <div className="flex flex-1 flex-col justify-center">
            {/**
              * ⚠⚠ THE ANSWER IS A TABLE ROW, NOT A SENTENCE. It was a paragraph — "That market cap
              * implies 24.3% annual FCF growth for 10 years" — with a 2xl number inside it, which
              * reflowed on every keystroke and could not line up with anything. The refusals below
              * were four more paragraphs of DIFFERENT lengths, so the panel changed height
              * depending on which input was missing.
              *
              * ⚠ THE ROWS ARE CONSTANT AND ONLY THE VALUE GOES `n/a` — the same rule the EGM panel
              * follows. The reason a solve failed is in the ⓘ, which is present either way, so the
              * SHAPE of the panel stops being a signal and the numbers can be one.
              */}
            <table className="w-full table-fixed text-[12px]">
              {/* The last column is the ⓘ slot — see `Field`. Empty on every row with no
                  explanation, which is what holds the ones that have to a single line.
                  ⚠ IT IS WIDER THAN THE ICON AND THE ICON IS PADDED BY THE DIFFERENCE: the figures
                  move left, the icons stay on the line they already sat on. Same pair as the EGM
                  tables above; see the ⚠⚠ there. */}
              <colgroup>
                <col />
                <col className="w-[5rem]" />
                <col className="w-[3.25rem]" />
              </colgroup>
              <tbody>
                <tr>
                  <td className="truncate py-0.5 font-medium text-fg-strong">{t.dcf.impliedGrowth}</td>

                  <td className="py-0.5 pl-2 text-right font-mono tabular-nums font-semibold text-fg-strong">
                    {/* ⚠ UNCOLOURED ON PURPOSE. Whether 50% is absurd or reasonable is the reader's
                        call, and a threshold picked out of the air was making it for them. */}
                    {fcf != null && fcf > 0 && growth != null
                      ? `${(growth * 100).toFixed(1)}%` : 'n/a'}
                  </td>
                  <td className="py-0.5 pl-4">
                    <InfoTip content={<AspectCard
                      what={t.dcf.cards.impliedGrowth.what}
                      where={t.dcf.cards.impliedGrowth.where}
                      when={t.dcf.yearsOneTo(String(years))}
                      // ⚠⚠ THE EQUATION IS WRITTEN AS AN EQUALITY, NOT AS A VALUE, because that is
                      // what a REVERSE DCF is: everything but `g` is known and `g` is what makes
                      // the two sides meet. Printed as `PV = …` it would read as a valuation the
                      // panel had computed, which is the one reading its subtitle exists to refuse.
                      worked={workedImpliedGrowth({
                        fcf, rate, perpetuityGrowth, years, target, growth })}
                      legend={fcf == null || !(fcf > 0) || growth == null || target == null
                        ? undefined : [
                          // ⚠ YEAR 1 IS `F` ITSELF — growth starts in year 2. A real convention
                          // with a real effect (one fewer compounding year than the naive
                          // reading), and the `t-1` exponent is the only other place it shows.
                          { sym: 'F', is: t.dcf.legend.F },
                          { sym: 'g', is: t.dcf.legend.g },
                          { sym: 'r', is: t.dcf.legend.r },
                          { sym: String.raw`g_\infty`, is: t.dcf.legend.gInf },
                          { sym: 'M', is: t.dcf.legend.M },
                        ]}
                      how={fcf != null && fcf > 0 && growth != null
                        ? t.dcf.impliedHow + t.dcf.notLikeForLike
                        : fcf != null && fcf <= 0
                          ? t.dcf.impliedNonPositive(mn(fcf))
                          : missing.length > 0
                            ? t.dcf.impliedMissing(missing.join(', '))
                            : !(rate > perpetuityGrowth)
                              ? t.dcf.impliedRateTooLow
                              : t.dcf.impliedNoRate} />} />
                  </td>
                </tr>

                {/**
                  * ⚠⚠ THE COMPARISON THE WHOLE PANEL IS FOR, MOVED OUT OF THE SWEEP. These were
                  * three extra columns bolted onto the discount-rate table and fenced off with
                  * borders, because they are NOT a solve at anything — they are what people
                  * actually forecast. The fencing existed to stop them reading as "the implied
                  * rate at some other discount rate"; as rows under the answer they cannot, and
                  * the sweep goes back to being one thing.
                  *
                  * ⚠ ONLY WHEN AT LEAST ONE ARRIVED. Three rows of `—` headed "analysts" reads as
                  * "they expect nothing".
                  */}
                {hasAnalysts && analysts.map(([label, value, tip], i) => (
                  <tr key={label} className={i === 0 ? 'border-t border-neutral-800/40' : ''}>
                    <td className={`truncate py-0.5 text-fg-muted ${i === 0 ? 'pt-1.5' : ''}`}
                      title={tip}>
                      {i === 0 && <span className="text-fg-faint">{t.dcf.analystsPrefix}</span>}{label}
                    </td>
                    <td className={`py-0.5 pl-2 text-right font-mono tabular-nums ${
                      i === analysts.length - 1 ? 'text-fg-strong' : 'text-fg-soft'
                    } ${i === 0 ? 'pt-1.5' : ''}`}>
                      {value == null ? '—' : `${value.toFixed(1)}%`}
                    </td>
                    <td />
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* The sensitivity, which is the honest context for a single figure: the answer moves ~1.5pp
          per point of discount rate, so a reader who disagrees with the rate can read their own
          off the row instead of taking this one. Click a column to make it the headline. */}
      {fcf != null && fcf > 0 && target != null && (
        <div className="space-y-1">
          {/* ⚠ FULL WIDTH, UNDER BOTH BOXES, AND NOT INSIDE THE OUTPUT ONE. Eleven discount
              rates do not fit in half a card without a horizontal scrollbar, and a sensitivity
              grid is a different kind of object from a conclusion: it is the honest context for a
              single figure (the answer moves ~1.5pp per point of discount rate), not part of the
              input → output story the two rectangles tell. Clicking a column still sets the rate,
              which is why it stays on the page at all. */}
          <div className="text-[11px] uppercase tracking-wide text-fg-faint">
            {t.dcf.impliedByDiscountRate}
          </div>
          <div className="overflow-x-auto rounded-lg border border-neutral-800/40 max-w-full">
            {/* ⚠⚠ `w-full` HERE, WHERE THE `Tables` TAB DELIBERATELY USES `w-fit` — the two look
                like contradictory rules and are the same rule applied to opposite shapes. That
                table is seven short percentages in a wide modal, so stretching it makes most of it
                the gaps between its own numbers. This one is FIFTEEN columns that already fill the
                card; sized to content it stops short of the two rectangles above it and the
                trailing whitespace reads as a rendering fault rather than a narrow table.

                ⚠ IT STILL SCROLLS ON A NARROW VIEWPORT. Every cell is `whitespace-nowrap`, so the
                table's minimum width is its content width — `w-full` only ever ADDS space, it
                cannot compress the columns into each other, and the `overflow-x-auto` wrapper
                keeps its job. ⚠ NOT `table-fixed`, which would force the sticky label column to
                the same width as a percentage cell. */}
            <table className="w-full text-xs">
              <tbody>
                <tr className="border-b border-neutral-800/40 bg-page">
                  <th className="px-2 py-1 text-left font-medium text-fg-faint text-[11px] uppercase tracking-wide sticky left-0 bg-page">
                    Discount
                  </th>
                  {sweep.map((s) => (
                    <td key={s.discountRate}
                      className={`px-2 py-1 text-right font-mono whitespace-nowrap cursor-pointer hover:bg-overlay/5 ${
                        Math.abs(s.discountRate - rate) < 0.0005
                          ? 'text-fg-strong font-semibold' : 'text-fg-muted'}`}
                      onClick={() => setRateStr((s.discountRate * 100).toFixed(1))}
                      title={t.dcf.useThisRate}>
                      {(s.discountRate * 100).toFixed(0)}%
                    </td>
                  ))}
                  {/* ⚠⚠ THE ANALYST COLUMNS ARE GONE FROM HERE (2026-08-18) — they are ROWS in
                      the Output box now. They were never part of this sweep: a forecast, not a
                      solve at some other discount rate, which is why they needed two border rules
                      and a caption to stop them being read as one. Under the answer they compare
                      with, they need neither, and this table goes back to being one thing. */}
                </tr>
                <tr>
                  <th className="px-2 py-1 text-left font-medium text-fg-faint text-[11px] uppercase tracking-wide sticky left-0 bg-card">
                    Growth
                  </th>
                  {sweep.map((s) => (
                    <td key={s.discountRate}
                      className={`px-2 py-1 text-right font-mono whitespace-nowrap cursor-pointer hover:bg-overlay/5 ${
                        Math.abs(s.discountRate - rate) < 0.0005
                          ? 'text-fg-strong font-semibold' : 'text-fg-soft'}`}
                      onClick={() => setRateStr((s.discountRate * 100).toFixed(1))}>
                      {/* A rate at or below the perpetuity growth has no valid terminal value, so
                          the cell is empty rather than showing a negative one. */}
                      {s.impliedGrowth == null ? '—' : `${(s.impliedGrowth * 100).toFixed(1)}%`}
                    </td>
                  ))}
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      )}

      {showRaw && (
        <ReverseDcfInputsModal metrics={metrics} currency={currency} today={today}
          name={name} isin={isin} fcf={fcf} target={target} discountRate={rate}
          years={years} perpetuityGrowth={perpetuityGrowth}
          onClose={() => setShowRaw(false)} />
      )}
    </div>
  );
}
