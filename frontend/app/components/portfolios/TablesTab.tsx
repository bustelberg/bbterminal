'use client';

import { useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import InfoTip from '../InfoTip';
import { useBenchInputs, type BenchTarget } from './benchSeries';
import { CAGR_BENCHMARKS, type CagrBenchmark } from './CagrTable';
import { roicByYear, type CashReturnInputs } from './cashReturnData';
import { investedCapitalBlend } from './investedCapitalData';
import { buildBlend, POSITIVE_ONLY_METRICS, type Blend, type Resp } from './fundamentalBlend';
import { traceEmpty } from '../../../lib/debugTrace';
import {
  CAGR_DECIMALS, cagrExcess, cagrPct, commonEndPeriod, forwardCagr, lineCagr, type Cagr,
} from './lineCagr';
import { marginByYear, xToPeriod, type MarginInputs } from './marginData';
import { grossMarginByYear, type GrossMarginInputs } from './grossMarginData';
import { cashConversionByYear, type CashConversionInputs } from './cashConversionData';
import {
  coverageFromBurden, interestBurdenByYear, type InterestBurdenInputs,
} from './interestBurdenData';
import HoldingsRevenueModal, { type Target } from './HoldingsRevenueModal';
import LoadingDots from './LoadingDots';
import GrossMarginInputsModal from './GrossMarginInputsModal';
import MarginInputsModal from './MarginInputsModal';
import CashReturnInputsModal from './CashReturnInputsModal';
import CashConversionInputsModal from './CashConversionInputsModal';
import InterestBurdenInputsModal from './InterestBurdenInputsModal';
import { COPY, MEASURE_KEYS, RATE_KEYS, type MeasureKey, type TablesCopy } from './tablesCopy';
import { AspectCard } from '../../../lib/tipCard';
import { withWorked } from './workedFormula';
import { latestCommonX, meanExcess, windowMean, type WindowMean } from './windowStats';
import {
  meanSub, rateSub, type MeanTransform,
} from './tablesSubstitution';
import { type Lang } from '../../../lib/i18n';

/**
 * `Tables` — every quality read on one screen, book against index, over 5 and 10 years.
 *
 * ⚠⚠ THERE IS ONE ROW PER LONG EQUITY CHART, AND THAT IS THE RULE THE TABLE IS FOR. A tab drawing
 * six level charts while summarising three left the reader to eyeball the other three off a log
 * axis — which is the one thing a summary exists to remove. Every LEVEL chart gets a rate row
 * (revenue, EPS, FCF/share, price, invested capital, shares outstanding); every RATIO chart gets a
 * window mean, because a percentage oscillating around a level does not compound and annualising it
 * is not a rate of anything.
 *
 * ⚠ BOTH AXES FILTER, INDEPENDENTLY — the windows (columns) and the measures (rows). Everything
 * defaults on; the state is two Sets over `WINDOWS` and `MEASURES`, and both the chips and the
 * table are built from those same two lists. Two rules that are not symmetric, each for its own
 * reason: the LAST WINDOW cannot be switched off (the expectation row's colspan divides by the
 * count, and a table of nothing but row labels is not a view anyone asked for), while zero ROWS is
 * a legal state that simply says so.
 *
 * ⚠⚠ EVERY SERIES HERE IS THE ONE A CARD IN THIS MODAL ALREADY DRAWS. FCF margin is `marginByYear`,
 * ROIC is `roicByYear`, and FCF/share is the same weighted, chained, coverage-floored line the
 * drill-down's `Rebased` footer prints. Nothing is re-aggregated: a summary that computes its own
 * version "the same way" is how a table comes to disagree with the chart two tabs from it, and this
 * one sits in the same modal as all three.
 *
 * ⚠ THE ROWS ANSWER TWO DIFFERENT QUESTIONS AND SAY SO. A per-share amount COMPOUNDS, so its
 * summary is a rate; a margin and a return on capital are ratios that do not, so theirs is an
 * average. Printing both under one "5y" heading without naming which is which would invite reading
 * a 12.3% average margin as 12.3% annual growth.
 *
 * ⚠ AND THE FCF/SHARE RATE IS POINT-TO-POINT, WHICH IS *NOT* WHAT THE GROWTH CARD SHOWS. That card
 * fits a log-linear trend through every year in the window (hence the R² beside it); this is
 * `(end/start)^(1/n) − 1`, the literal definition, decided deliberately. They will differ — most
 * where a single endpoint year is unrepresentative, which is exactly when the difference is worth
 * seeing — so the footnote names the divergence rather than leaving two "CAGR"s to be discovered.
 *
 * ⚠⚠ BUT IT IS A SMALL DIFFERENCE, AND TREATING IT AS AN ALL-PURPOSE EXCUSE HID A REAL BUG FOR
 * WEEKS. Measured on Bustelberg Offensief's actual FCF/share line: the card's fit is 27.46%/yr and
 * point-to-point over the SAME window is 28.00% — 0.5pp, with R² 0.99. When R² is high the two
 * CANNOT diverge much, because R² is precisely how close the line is to a constant-rate
 * exponential. So a big gap is never this: on 2026-08-17 the row read +19.0% against the card's
 * +28.0% because this tab was requesting `metric=fcf_per_share`, a key the backend's registry does
 * not carry (`fcf_ps`), and `_metric_codes` silently answered with REVENUE. The row was the book's
 * revenue growth wearing an FCF/share label, and this footnote was the reason nobody looked
 * further. ⚠ IF THESE TWO EVER DISAGREE BY MORE THAN ABOUT A POINT AT A HIGH R², THE EXPLANATION
 * IS NOT THE FIT — CHECK THAT BOTH SIDES ARE ACTUALLY READING THE SAME SERIES.
 */

/**
 * EVERY ROW OPENS THE GROUND NUMBERS IT WAS COMPUTED FROM.
 *
 * ⚠⚠ THE SAME DRILL-DOWN THE LONG EQUITY CARD OPENS, NOT A NEW ONE. Each of these panels already
 * exists, already reads the SAME endpoint this table read, and is already built so that "the table
 * cannot arrive at a different figure than the line it was opened from". Building a second inspector
 * for the same nine numbers would be a second computation to keep in step — which is the one thing
 * a verification view must never be.
 *
 * ⚠ SO A READER CAN CHECK THE CHAIN END TO END: the row says 79.3×, the panel lists every holding's
 * interest expense and operating profit for every year, its weight, and the ratio each pair
 * produced. Nothing between the vendor's figure and the cell is hidden.
 *
 * ⚠ THE RATE ROWS GO TO THE MATRIX, which is the right panel for them rather than a compromise: it
 * carries the per-holding level per period, the weight in force, the rebased index, AND the
 * per-period contribution decomposition — so a CAGR can be checked against the line it summarises
 * and the holdings that moved it.
 */
const MATRIX_ROWS: Partial<Record<MeasureKey, {
  metric: string; unit: 'millions' | 'per_share'; noun: string;
}>> = {
  revCagr: { metric: 'revenue', unit: 'millions', noun: 'revenue' },
  // ⚠ THE SAME `eps_nri` MATRIX THE EXPECTATION ROW OPENS — one series, two questions about it, so
  // both labels lead to the same numbers rather than to two panels a reader has to reconcile.
  epsCagr: { metric: 'eps_nri', unit: 'per_share', noun: 'EPS (excl. NRI)' },
  fcfCagr: { metric: 'fcf_ps', unit: 'per_share', noun: 'FCF per share' },
  priceCagr: { metric: 'price_ps', unit: 'per_share', noun: 'share price' },
  // ⚠ `shares` IS A COUNT, NOT A CURRENCY — `unit: 'per_share'` is the drill-down's "not millions"
  // setting rather than a claim that a share count is per share. The matrix formats it as a plain
  // figure either way; `millions` would divide it and print a book's 1.2bn shares as 1,200.
  sharesCagr: { metric: 'shares', unit: 'per_share', noun: 'shares outstanding' },
  epsFwd: { metric: 'eps_nri', unit: 'per_share', noun: 'EPS (excl. NRI)' },
};

/**
 * THE REQUEST FOR A ROW'S GROUND DATA — and the reason `MATRIX_ROWS` is read rather than the metric
 * key being written out again beside it.
 *
 * ⚠⚠ THE FETCH AND THE BLEND MUST NAME THE SAME METRIC, and nothing catches it when they do not.
 * `buildBlend(resp, metric)` applies the member rules on the KEY it is given — the positives-only
 * filter (`POSITIVE_ONLY_METRICS`) among them — so a row fetched as `eps_nri` and blended as
 * anything else is drawn over a DIFFERENT SET OF COMPANIES from the card charting the same series,
 * with both numbers looking perfectly ordinary. This tab has already paid for one metric-key
 * mismatch (`fcf_per_share` vs `fcf_ps`, +19.0% against +28.0% on the same book); that one at least
 * produced a visibly different series. This one would not.
 */
const matrixPath = (k: MeasureKey) =>
  `portfolio-revenue-matrix?metric=${MATRIX_ROWS[k]!.metric}`;

/** The blended line for a matrix row — the SAME key its data was fetched under. */
const blendOf = (k: MeasureKey, resp: Resp | null) =>
  (resp ? buildBlend(resp, MATRIX_ROWS[k]!.metric) : null);

/** The rows whose series is drawn from a FILTERED set of companies — see the footnote clause and
 *  `earnings._POSITIVE_ONLY_METRICS`. ⚠ DERIVED FROM `MATRIX_ROWS`, so it names rows by the metric
 *  they actually fetch rather than by a second list that can go stale against it. */
const POSITIVE_ONLY_ROWS: readonly MeasureKey[] = (
  Object.entries(MATRIX_ROWS) as [MeasureKey, { metric: string }][]
).filter(([, v]) => POSITIVE_ONLY_METRICS.has(v.metric)).map(([k]) => k);

const WINDOWS = [5, 10] as const;
type Window = (typeof WINDOWS)[number];
/** ⚠ THREE YEARS, AND ONLY THREE. The consensus thins fast — measured on ACWI, 2031e is carried by
 *  166 of 1,761 constituents against 2028e's 1,310 — so a 5- or 10-year "expectation" would be
 *  a handful of the largest names wearing the index's name. It spans EVERY shown window column for
 *  the same reason: there is one number here, and a 5y/10y pair would imply two. */
const EXPECTED_WINDOW = [3] as const;

/**
 * The rows are declared in `tablesCopy` (`MEASURE_KEYS`), and so are their names.
 *
 * ⚠ THE CHIPS AND THE TABLE BODY READ THE SAME KEY. A visibility control whose labels are typed
 * out a second time beside the rows they hide is one rename away from a chip that turns off a row
 * with a different name on it — and the reader has no way to tell which of the two is the lie. So
 * `rateRow`/`meanRow` take the KEY and look the label up, rather than being handed a string.
 *
 * ⚠ `copy.chip` IS SHORT WHERE `copy.rowLabel` IS FULL, because a control is scanned and a row is
 * read: "EPS (excl. NRI) expected, 3y" is the right thing on the row (it has to carry its own
 * window — see the ⚠ there) and far too long on a button sitting beside three others.
 */

/**
 * One filter chip. A TOGGLE, not a segment of a picker — and it has to look like one.
 *
 * ⚠ THE BENCHMARK CONTROL BESIDE THIS IS A JOINED SEGMENTED BAR, and that shape means "exactly one
 * of these". These two groups mean "any of these", so they are separate pills: borrowing the
 * segmented look would promise that turning 10y on turns 5y off.
 */
function Chip({ on, onClick, disabled, title, children }: {
  on: boolean; onClick: () => void; disabled?: boolean; title?: string;
  children: React.ReactNode;
}) {
  return (
    <button type="button" onClick={onClick} aria-pressed={on} disabled={disabled} title={title}
      className={`cursor-pointer rounded-md border px-2 py-0.5 text-[11px] font-medium
        transition-colors disabled:cursor-not-allowed ${
        on ? 'border-accent-500/50 bg-accent-500/15 text-accent-300'
           : 'border-neutral-700 text-fg-faint hover:text-fg-muted hover:bg-overlay/5'}`}>
      {children}
    </button>
  );
}

type Side = {
  margin: Map<number, number | null>;
  roic: Map<number, number | null>;
  /** ⚠ THE SAME THREE HELPERS THE LONG EQUITY CARDS DRAW (`grossMarginByYear`,
   *  `cashConversionByYear`, `interestBurdenByYear`), never a second aggregation "the same way" —
   *  a summary that recomputes its own version is how a table comes to disagree with the chart it
   *  summarises, which this file's header already warns about for the rows that were here first. */
  grossMargin: Map<number, number | null>;
  cashConv: Map<number, number | null>;
  /** ⚠ THE BURDEN, NOT THE COVERAGE — the row inverts it after the window mean. See
   *  `coverageFromBurden` for why every average has to happen on this side of the reciprocal. */
  intBurden: Map<number, number | null>;
  fcfPs: Resp | null;
  err: string | null;
};

/**
 * A MEAN row's percentage — one decimal.
 *
 * ⚠ THE RATE ROWS DO NOT COME THROUGH HERE ANY MORE (2026-09-03). They print at `CAGR_DECIMALS`
 * via `cagrPct`, because the same rate is quoted on the `Graphs` tiles one tab away and the two
 * must round identically; an average margin is quoted nowhere else and stays as it was. Two row
 * KINDS, two precisions — the rate rows say "per annum" and the mean rows say "average of", which
 * is already the distinction this table's header spends a paragraph on.
 */
const pctCell = (v: number, sign: boolean) =>
  `${sign && v >= 0 ? '+' : ''}${v.toFixed(1)}%`;

/**
 * What a mean row's numbers ARE. Every row here was a percentage until interest coverage arrived.
 *
 * ⚠⚠ A MULTIPLE PRINTED AS A PERCENTAGE IS A WRONG NUMBER THAT LOOKS RIGHT. Coverage of 12.4×
 * rendered as "12.4%" reads as a book paying out an eighth of its profit in interest — the exact
 * inverse of what it says, in a plausible-looking cell nobody would query. The unit travels with
 * the row rather than being assumed by the cell.
 *
 * ⚠ AND THE EXCESS FOLLOWS IT. A difference between two percentages is percentage POINTS; a
 * difference between two multiples is turns of cover. The suffix is what keeps them apart.
 */
type Unit = 'pct' | 'mult';
const unitCell = (v: number, unit: Unit, sign = false) => (unit === 'mult'
  ? `${sign && v >= 0 ? '+' : ''}${v.toFixed(1)}×`
  : pctCell(v, sign));

/**
 * A rate cell — a `Cagr`, or a dash whose tooltip says which absence this is.
 *
 * `ownWindow` = this row is measured over a window the column heading does not name (the 3-year
 * expectation). See the ⚠ on the years badge below for why that is not the same thing as `span`.
 */
function RateCell({ got, copy, span = 1, ownWindow = false, pending = false }:
{ got: Cagr | null; copy: TablesCopy; span?: number; ownWindow?: boolean;
  /** ⚠ IS SOMETHING ACTUALLY COMING? See `arriving` — dots that keep moving after a failed fetch,
   *  or over a value that will never exist, promise an arrival, which is a worse lie than the
   *  motionless `…` they replaced. */
  pending?: boolean }) {
  /**
   * ⚠⚠ A SPANNED CELL IS **CENTRED**, NOT RIGHT-ALIGNED, AND THAT IS NOT A STYLE CHOICE. Right
   * alignment is correct for a number that belongs to a column; this one belongs to NEITHER of the
   * two it spans, and right-aligning it parked the 3-year expectation hard against the `10y` edge —
   * reported, correctly, as looking like a ten-year figure. Centring is the only position that says
   * "this is not in either column", and it is why the row reads as its own thing at a glance.
   */
  const align = span > 1 ? 'text-center' : 'text-right';
  if (!got) {
    return (
      <td colSpan={span} className={`px-2.5 py-1 ${align} text-fg-faint`}>
        {pending ? <LoadingDots /> : '…'}
      </td>
    );
  }
  if (got.pct == null) {
    return (
      <td colSpan={span} className={`px-2.5 py-1 ${align}`}>
        <InfoTip text={got.reason} className="cursor-default text-fg-faint">—</InfoTip>
      </td>
    );
  }
  return (
    <td colSpan={span} className={`px-2.5 py-1 ${align} font-mono tabular-nums`}>
      {/* ⚠ THE WINDOW IS ON THE NUMBER. "3y" is a claim the data has to support, and the two sides
          do not always support it identically. */}
      <InfoTip className="cursor-default" text={copy.rateTip(got.from, got.to, got.years)}>
        <span className={got.pct >= 0 ? 'text-fg-soft' : 'text-neg-300'}>
          {/* ⚠ `cagrPct`, NOT `pctCell` — the same spelling the `Graphs` tile uses for the same
              rate. See `CAGR_DECIMALS`. */}
          {cagrPct(got.pct)}
        </span>
        {/* ⚠ THE WINDOW IS LABELLED, because a centred number under two headings is unambiguous
            about which column it is NOT in and silent about which window it IS. Same treatment as
            `MeanCell`'s `(4/5)` — the qualifier rides on the figure, not in a tooltip nobody opens.

            ⚠⚠ KEYED ON `ownWindow`, NOT ON `span > 1` — AND THAT DISTINCTION ONLY APPEARS ONCE THE
            COLUMNS CAN BE FILTERED. With one window shown the 3-year expectation spans exactly one
            column, so a `span > 1` test drops the badge and parks a 3y figure under a heading that
            says `5y`, right-aligned, with nothing left to contradict it — which is the ORIGINAL
            complaint about this row, reintroduced by a feature that has nothing to do with it. */}
        {ownWindow && (
          <span className="ml-1.5 text-[10px] text-fg-faint">{got.years}{copy.yearSuffix}</span>
        )}
      </InfoTip>
    </td>
  );
}

/** A mean cell — the value, how much of the window it covers, and the window itself on hover. */
function MeanCell({ got, copy, unit = 'pct', transform, pending = false }:
{ got: WindowMean | null; copy: TablesCopy; unit?: Unit; transform?: MeanTransform;
  /** See `RateCell`'s — same rule, same reason. */
  pending?: boolean }) {
  if (!got) {
    return (
      <td className="px-2.5 py-1 text-right text-fg-faint">{pending ? <LoadingDots /> : '…'}</td>
    );
  }
  if (got.mean == null) {
    return (
      <td className="px-2.5 py-1 text-right">
        <InfoTip text={got.reason} className="cursor-default text-fg-faint">—</InfoTip>
      </td>
    );
  }
  const shown = transform ? transform(got.mean) : got.mean;
  if (shown == null) {
    return (
      <td className="px-2.5 py-1 text-right">
        <InfoTip className="cursor-default text-fg-faint"
          text={copy.noCoverage(xToPeriod(got.fromX), xToPeriod(got.toX))}>—</InfoTip>
      </td>
    );
  }
  // ⚠ AN INCOMPLETE WINDOW IS MARKED ON THE NUMBER, not left to the tooltip. "12.3%" and
  // "12.3% (4 of 5)" are different claims and only the second is true.
  const short = got.n < got.of;
  return (
    <td className="px-2.5 py-1 text-right font-mono tabular-nums text-fg-soft">
      <InfoTip className="cursor-default"
        text={copy.meanTip(got.n, xToPeriod(got.fromX), xToPeriod(got.toX),
          short ? got.of : null)}>
        {unitCell(shown, unit)}
        {short && <span className="ml-1 text-[10px] text-warn-300">({got.n}/{got.of})</span>}
      </InfoTip>
    </td>
  );
}

export default function TablesTab({ holdingsTarget, holdingsName, sbcCorrection, lang }: {
  holdingsTarget: Target;
  holdingsName: string;
  sbcCorrection: boolean;
  /** ⚠ PASSED DOWN, NOT READ FROM `useLang` HERE — the choice is global (sidebar, every page) since
   *  2026-08-21 and this tab is one of the surfaces that answers it. See `lib/i18n.ts`; the
   *  remaining gaps are written down in `management/managementCopy.ts::UNTRANSLATED_SURFACES`. */
  lang: Lang;
}) {
  const copy = COPY[lang];
  /** ⚠ ITS OWN BENCHMARK PICKER — the tab's other cards follow whatever index the modal was opened
   *  against; here the question IS "against what", so it gets a control of its own.
   *  ⚠⚠ DEFAULTING TO ACWI (2026-09-03, on request; it was AEX). It has to match `LongEquityTab`'s
   *  default, because Graphs and Tables are two tabs of ONE modal showing the same series — one
   *  charted and one summarised — and a summary measured against a different index from the charts
   *  it summarises is a summary of something else. */
  const [bench, setBench] = useState<CagrBenchmark>('ACWI');

  /**
   * What is on screen. Two independent filters, both defaulting to everything.
   *
   * ⚠ SETS OF THE DECLARED KEYS, NOT BOOLEAN FLAGS. Four `showRoic`-style booleans is four places a
   * fifth measure has to be remembered, and the chips would still be built from a separate list.
   *
   * ⚠⚠ THE LAST WINDOW CANNOT BE TURNED OFF, and that is structural rather than a preference: the
   * expectation row's cell spans `shown.length / windows.length` columns, so an empty selection is
   * a division by zero and a table whose only column is its own row labels. The chip goes disabled
   * and says why, rather than being silently inert.
   */
  const [shownW, setShownW] = useState<Set<Window>>(() => new Set(WINDOWS));
  const [shownM, setShownM] = useState<Set<MeasureKey>>(() => new Set(MEASURE_KEYS));
  /** Which row's ground numbers are open, if any — see `MATRIX_ROWS`. */
  const [drill, setDrill] = useState<MeasureKey | null>(null);
  /** ⚠ FILTERED FROM `WINDOWS`, never from the Set — a Set has no order, and 10y/5y reversed is a
   *  table whose headings do not match the order every other read of this data uses. */
  const shown = useMemo(() => WINDOWS.filter((w) => shownW.has(w)), [shownW]);
  const on = (k: MeasureKey) => shownM.has(k);
  const toggleW = (w: Window) => setShownW((s) => {
    const next = new Set(s);
    if (next.has(w)) { if (next.size > 1) next.delete(w); } else next.add(w);
    return next;
  });
  const toggleM = (k: MeasureKey) => setShownM((s) => {
    const next = new Set(s);
    if (next.has(k)) next.delete(k); else next.add(k);
    return next;
  });
  const benchTarget: BenchTarget = useMemo(
    // ⚠ ANNUAL, WHATEVER THE TAB IS ON. A 5-year window of QUARTERS is fifteen months, and a
    // "5y CAGR" off it would be off by a factor of four — plausible and wrong on every row.
    () => ({ universe: bench, label: bench, cadence: 'annual' as const }), [bench]);

  // ── the book ──────────────────────────────────────────────────────────────────────────────
  const [marginData, setMarginData] = useState<MarginInputs | null>(null);
  const [roicData, setRoicData] = useState<CashReturnInputs | null>(null);
  const [fcfPs, setFcfPs] = useState<Resp | null>(null);
  const [epsNri, setEpsNri] = useState<Resp | null>(null);
  const [pricePs, setPricePs] = useState<Resp | null>(null);
  // ⚠ THE FOUR ADDED 2026-08-21 — revenue as a MATRIX (a level, chained like FCF/share) and three
  // ratio payloads read through the SAME helpers the Long Equity cards use, so a row here and the
  // card two tabs away cannot come to disagree about what "gross margin" is.
  const [revenue, setRevenue] = useState<Resp | null>(null);
  const [grossM, setGrossM] = useState<GrossMarginInputs | null>(null);
  const [cashConvD, setCashConvD] = useState<CashConversionInputs | null>(null);
  const [intBurden, setIntBurden] = useState<InterestBurdenInputs | null>(null);
  // ⚠ THE SHARE COUNT IS A LEVEL LIKE THE REST, and its CAGR is the wedge between the revenue row
  // and the two per-share rows: a book whose EPS outruns its revenue is either widening margins or
  // retiring stock, and nothing else on this table says which.
  const [sharesResp, setSharesResp] = useState<Resp | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    const post = async <T,>(path: string, body: unknown): Promise<T> => {
      const r = await apiFetch(`${API_URL}/api/earnings/${path}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const b = await r.json().catch(() => null);
      if (!r.ok) throw new Error((b as { detail?: string })?.detail ?? `HTTP ${r.status}`);
      return b as T;
    };
    void (async () => {
      setErr(null);
      try {
        // ⚠ THE SAME ENDPOINTS THE CARDS USE, so `apiFetch`'s read cache serves them when the Long
        // Equity tab has already loaded them — this tab is usually free to open.
        const [m, c, f, e, p, rev, gm, cc, ib, sh] = await Promise.all([
          post<MarginInputs>('margin-inputs', holdingsTarget),
          post<CashReturnInputs>('cash-return-inputs', holdingsTarget),
          post<Resp>(matrixPath('fcfCagr'), holdingsTarget),
          // ⚠⚠ `eps_nri` — EXCLUDING non-recurring items, whose paired forecast is
          // `annual_eps_nri_estimate`. GuruFocus also publishes an INCLUDING-NRI consensus
          // (`annual_per_share_eps_estimate`) that agrees to a cent on almost every company
          // (Apple 8.76 vs 8.77), so joining the wrong one onto this actual would put a one-off
          // impairment on the wrong side of the join and nothing on screen would say so. The
          // metric key carries the pairing — see the EPS card's own ⚠⚠ in `LongEquityTab`.
          post<Resp>(matrixPath('epsCagr'), holdingsTarget),
          // ⚠⚠ `price_ps` IS GURUFOCUS'S "Month End Stock Price" — the share price at each fiscal
          // YEAR END, filed alongside the fundamentals, not our daily `metric_data` closes. That is
          // deliberate and it is the only thing that makes this row comparable to the ones above
          // it: every other series on this table is indexed on the same fiscal-period axis, so a
          // price read on a calendar date would be measured over a window the neighbouring rows
          // are not. The cost is that the row is as fresh as the last filing, exactly like them.
          post<Resp>(matrixPath('priceCagr'), holdingsTarget),
          // ⚠ `revenue` IS THE MATRIX ENDPOINT'S DEFAULT METRIC, so this is the same request shape
          // as the three above it — no new endpoint, and `apiFetch`'s read cache means a reader who
          // has already opened Long Equity pays nothing for any of these four.
          post<Resp>(matrixPath('revCagr'), holdingsTarget),
          post<GrossMarginInputs>('gross-margin-inputs', holdingsTarget),
          post<CashConversionInputs>('cash-conversion-inputs', holdingsTarget),
          post<InterestBurdenInputs>('interest-burden-inputs', holdingsTarget),
          // ⚠ SAME ENDPOINT AND SAME SHAPE as the four matrices above — a share COUNT is a level
          // per period, so it blends by exactly the rule they do and needs nothing new.
          post<Resp>(matrixPath('sharesCagr'), holdingsTarget),
        ]);
        if (!alive) return;
        setMarginData(m); setRoicData(c); setFcfPs(f); setEpsNri(e); setPricePs(p);
        setRevenue(rev); setGrossM(gm); setCashConvD(cc); setIntBurden(ib); setSharesResp(sh);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [holdingsTarget]);

  // ── the index ─────────────────────────────────────────────────────────────────────────────
  const [bMargin, bMarginErr] = useBenchInputs<MarginInputs>('margin-inputs', benchTarget);
  const [bRoic, bRoicErr] = useBenchInputs<CashReturnInputs>('cash-return-inputs', benchTarget);
  const [bFcfPs, bFcfPsErr] = useBenchInputs<Resp>(
    matrixPath('fcfCagr'), benchTarget);
  const [bEpsNri, bEpsNriErr] = useBenchInputs<Resp>(
    matrixPath('epsCagr'), benchTarget);
  const [bPricePs, bPricePsErr] = useBenchInputs<Resp>(
    matrixPath('priceCagr'), benchTarget);
  const [bRevenue, bRevenueErr] = useBenchInputs<Resp>(
    matrixPath('revCagr'), benchTarget);
  const [bGrossM, bGrossMErr] = useBenchInputs<GrossMarginInputs>(
    'gross-margin-inputs', benchTarget);
  const [bCashConv, bCashConvErr] = useBenchInputs<CashConversionInputs>(
    'cash-conversion-inputs', benchTarget);
  const [bIntBurden, bIntBurdenErr] = useBenchInputs<InterestBurdenInputs>(
    'interest-burden-inputs', benchTarget);
  const [bShares, bSharesErr] = useBenchInputs<Resp>(
    matrixPath('sharesCagr'), benchTarget);

  const book: Side = useMemo(() => ({
    margin: marginByYear(marginData?.rows ?? [], sbcCorrection),
    roic: roicByYear(roicData?.rows ?? []),
    grossMargin: grossMarginByYear(grossM?.rows ?? []),
    // ⚠ FOLLOWS THE SBC CHECKBOX, like the margin row and unlike ROIC — its numerator is FCF.
    cashConv: cashConversionByYear(cashConvD?.rows ?? [], sbcCorrection),
    intBurden: interestBurdenByYear(intBurden?.rows ?? []),
    fcfPs,
    err,
  }), [marginData, roicData, grossM, cashConvD, intBurden, fcfPs, sbcCorrection, err]);

  const index: Side = useMemo(() => ({
    margin: bMargin ? marginByYear(bMargin.rows, sbcCorrection) : new Map(),
    roic: bRoic ? roicByYear(bRoic.rows) : new Map(),
    grossMargin: bGrossM ? grossMarginByYear(bGrossM.rows) : new Map(),
    cashConv: bCashConv ? cashConversionByYear(bCashConv.rows, sbcCorrection) : new Map(),
    intBurden: bIntBurden ? interestBurdenByYear(bIntBurden.rows) : new Map(),
    fcfPs: bFcfPs,
    // ⚠ EVERY BENCHMARK FETCH IS IN THIS CHAIN. One left out fails as a row of dashes whose
    // tooltip says the line has no periods — true, and it sends the reader to look at the data
    // rather than at the request that never arrived.
    err: bMarginErr ?? bRoicErr ?? bFcfPsErr ?? bEpsNriErr ?? bPricePsErr
      ?? bRevenueErr ?? bGrossMErr ?? bCashConvErr ?? bIntBurdenErr ?? bSharesErr,
  }), [bMargin, bRoic, bGrossM, bCashConv, bIntBurden, bFcfPs, sbcCorrection, bMarginErr,
    bRoicErr, bFcfPsErr, bEpsNriErr, bPricePsErr, bRevenueErr, bGrossMErr, bCashConvErr,
    bIntBurdenErr, bSharesErr]);

  /**
   * ⚠⚠ ONE WINDOW PER ROW, SHARED BY BOTH SIDES. Each series ends at its own latest year, and a
   * twenty-holding book crosses into a new fiscal year long before a 1,900-name index does — so
   * left alone the book would be averaged 2021-2025 against the index's 2020-2024, printed side by
   * side under one heading. Computed per ROW because the three series do not end together either.
   */
  const marginEnd = latestCommonX(book.margin, index.margin);
  const roicEnd = latestCommonX(book.roic, index.roic);
  // ⚠ ONE SHARED END PER ROW, not one for the table. The three added series end on their own
  // dates — gross profit is complete to a different quarter from net income — so pinning them all
  // to one period would silently shorten whichever row happened to be freshest. Same rule
  // `latestCommonX` already enforces for the two rows above.
  const grossEnd = latestCommonX(book.grossMargin, index.grossMargin);
  const convEnd = latestCommonX(book.cashConv, index.cashConv);
  const coverEnd = latestCommonX(book.intBurden, index.intBurden);
  /** ⚠⚠ THE METRIC KEY IS PASSED, AND IT IS NOT DECORATION. `buildBlend` applies the positives-only
   *  member rule (`POSITIVE_ONLY_METRICS`) only when it is told which metric it is holding — so
   *  omitting it draws this row over a DIFFERENT set of companies from the card that charts the
   *  same series, and the only symptom is two CAGRs for one book that disagree by a few points.
   *  That exact shape has cost this tab a wrong number before (`_metric_codes`, +19.0% vs +28.0%). */
  const bookBlend = useMemo(() => blendOf('fcfCagr', book.fcfPs), [book.fcfPs]);
  const idxBlend = useMemo(() => blendOf('fcfCagr', index.fcfPs), [index.fcfPs]);
  const fcfEnd = bookBlend && idxBlend ? commonEndPeriod(bookBlend.level, idxBlend.level) : null;
  /** ⚠ THE SAME `buildBlend` AS EVERY OTHER ROW, ON A DIFFERENT PAYLOAD — which is the whole of
   *  "weighted like the others". A price line assembled any other way (summing values, averaging
   *  levels) would be a second definition of the basket sitting one row from the first. */
  const bookRev = useMemo(() => blendOf('revCagr', revenue), [revenue]);
  const idxRev = useMemo(() => blendOf('revCagr', bRevenue), [bRevenue]);
  const revEnd = bookRev && idxRev ? commonEndPeriod(bookRev.level, idxRev.level) : null;
  const bookPrice = useMemo(() => blendOf('priceCagr', pricePs), [pricePs]);
  const idxPrice = useMemo(() => blendOf('priceCagr', bPricePs), [bPricePs]);
  const priceEnd = bookPrice && idxPrice
    ? commonEndPeriod(bookPrice.level, idxPrice.level) : null;
  const bookEps = useMemo(() => blendOf('epsCagr', epsNri), [epsNri]);
  const idxEps = useMemo(() => blendOf('epsCagr', bEpsNri), [bEpsNri]);
  /** ⚠ THE SHARED BASE IS AN **ACTUAL** — `commonEndPeriod` only ever returns a reported period, so
   *  the expectation is always measured from something that happened. See `forwardCagr`. */
  const epsBase = bookEps && idxEps ? commonEndPeriod(bookEps.level, idxEps.level) : null;
  /**
   * ⚠ THE HISTORICAL EPS ROW SHARES ITS END PERIOD WITH THE EXPECTATION ROW, and that is the point
   * rather than a saving: the consensus is measured FROM the latest period both sides reported, so
   * a history ending anywhere else would be a rate that does not hand over to the forecast beneath
   * it. One `commonEndPeriod`, read twice.
   */
  const bookShares = useMemo(
    () => blendOf('sharesCagr', sharesResp), [sharesResp]);
  const idxShares = useMemo(() => blendOf('sharesCagr', bShares), [bShares]);
  const sharesEnd = bookShares && idxShares
    ? commonEndPeriod(bookShares.level, idxShares.level) : null;
  /** ⚠ FROM `cash-return-inputs`, WHICH THIS TAB ALREADY LOADS for the ROIC row — invested capital
   *  is derived from the same two raw lines, so the row costs no request. `investedCapitalBlend` is
   *  the card's own construction, extracted rather than re-implemented. */
  const bookInvCap = useMemo(
    () => (roicData ? investedCapitalBlend(roicData.rows) : null), [roicData]);
  const idxInvCap = useMemo(() => (bRoic ? investedCapitalBlend(bRoic.rows) : null), [bRoic]);
  const invCapEnd = bookInvCap && idxInvCap
    ? commonEndPeriod(bookInvCap.level, idxInvCap.level) : null;

  /**
   * IS THIS ROW'S ANSWER STILL ON ITS WAY? — the gate on every animated `…` in this table.
   *
   * ⚠⚠ AN EMPTY CELL IS NOT ONE STATE, IT IS THREE, AND ONLY ONE OF THEM IS "WAIT". The row has no
   * value yet; or a fetch FAILED and nothing is coming; or both sides arrived and share no year at
   * all — a bank has no gross profit line, so its gross-margin row is permanently empty and
   * correct. Dots that keep moving over either of the last two promise an arrival, which is a
   * worse lie than the motionless `…` they replace: the reader waits instead of reading the error
   * banner two rows up.
   *
   * ⚠ SO IT ASKS THE ROW'S OWN PAYLOADS, not a table-wide "loading" flag. A per-row question needs
   * a per-row answer: with one flag for the table, the bank's gross-margin row would animate until
   * the LAST unrelated fetch landed and then stop, which is an indicator that tracks the wrong
   * thing.
   *
   * ⚠ AND IT REFUSES ON AN ERROR, checking BOTH sides — the book's `err` and the benchmark chain's
   * `index.err`, which are already rendered as their own lines above the table. `useBenchInputs`
   * leaves its payload null on failure exactly as it does while in flight, so the payload alone
   * cannot tell the two apart.
   */
  const arriving = (bookPayload: unknown, benchPayload: unknown) =>
    !err && !index.err && (bookPayload == null || benchPayload == null);

  const ready = marginData && roicData && fcfPs;
  const th = 'px-2.5 py-1 font-medium text-right whitespace-nowrap';

  /**
   * The row label's ⓘ: what it is, worked through, then the caveats.
   *
   * ⚠⚠ FORMULA, BLANK LINE, THE SAME FORMULA WITH NUMBERS IN IT — the shape the Money-weighted
   * column uses, and the reason it is a shape rather than prose is that the two halves answer
   * DIFFERENT doubts. Symbols say what was computed; the substitution says that this arithmetic
   * produces the figure on screen. Prose alone answers only the first, which is why a reader who
   * disbelieves a cell reads a perfectly clear definition and still disbelieves it.
   *
   * ⚠ THE PROSE STAYS, UNDERNEATH. Every one of these notes carries something the formula cannot
   * (a bank has no gross profit line; the price row has no FX leg; the forward row is not a
   * measurement) — losing those to make room for the worked example would trade a checkable number
   * for an uncheckable reading of it.
   *
   * ⚠ AND THE TOOLTIP IS PINNABLE, which is what makes the substitution worth rendering at all:
   * a click sticks it open and the expression can be selected and pasted into whatever the reader
   * checks numbers in. See `InfoTip`.
   */
  const tipFor = (k: MeasureKey, sub: string) => (
    <AspectCard what={copy.rowNote[k](sbcCorrection)}
      worked={withWorked(copy.rowFormula[k](sbcCorrection), sub)} />
  );

  /**
   * A row of RATES — one `Cagr` per side per window, plus the excess.
   *
   * ⚠ SHARED WITH THE FORWARD ROW ON PURPOSE. The historical rate and the expectation differ only
   * in which function produces the `Cagr`; rendering them twice is how one of them quietly stops
   * pinning both sides to the same window, or starts formatting a negative differently.
   */
  /**
   * ⚠⚠ A ROW THAT CANNOT BE MEASURED SAYS WHY, TO THE CONSOLE (2026-09-03). EPS and Share price
   * were empty here on a company whose data is demonstrably complete — measured through the
   * endpoint itself: NVIDIA returns 13 periods for `price_ps` and 18 for `eps_nri`, no nulls, no
   * negatives, the same shape as the `fcf_ps` row that works. Every link read correct on paper
   * (metric keys, section codes, fetch order, both benchmark and book), so the fault was in a
   * RUNTIME value nothing printed.
   *
   * ⚠⚠ AND THIS IS THE LINE THAT FOUND IT — the answer was "the level has 1 period(s): 2015". The
   * blend's materiality bar (`baseBarScale`) was refusing every step off a rebased base of 100
   * because NVIDIA's own median rebased value is 2,706, and the chain does not advance its anchor
   * on a refusal, so ONE refusal collapsed the whole line to its base point. A one-point line has
   * no window, which is why the dash's own reason talked about a start year 2010: it is
   * `commonEndPeriod − 5`, derived from that single point. Kept, because the class of failure is
   * general — this row is a summary of a line, and a line with no periods looks exactly like a
   * fetch that never arrived.
   *
   * ⚠ THE DASH'S TOOLTIP ALREADY CARRIES THE REASON, but a reason is only half of it: what is
   * missing is WHICH PERIODS the blended line actually has, and which end period it was pinned to.
   * That is three facts, they are known here, and none of them was reachable without a debugger.
   *
   * ⚠ CONSOLE, NOT THE UI — the repo's rule: the full diagnostic goes to `console.warn`, the reader
   * gets one short line. This adds nothing to the screen.
   */
  const traceRate = (k: MeasureKey, side: string, lvl: Blend['level'], y: number, got: Cagr) => {
    if (got.pct != null) return;
    const periods = Object.keys(lvl);
    traceEmpty('tables', `${k} ${side} ${y}y CAGR`,
      `${got.reason} · level has ${periods.length} period(s): ${periods.join(', ')}`);
  };

  const rateRow = (
    k: MeasureKey, a: Blend | null, b: Blend | null,
    rate: (lvl: Blend['level'], years: number) => Cagr,
    /** ⚠ THE ROW'S OWN PAYLOADS, NOT `a`/`b` — a `Blend` is null both while its response is in
     *  flight and after one that failed, so the blends cannot answer this. See `arriving`. */
    pending: boolean,
    windows: readonly number[] = shown,
  ) => (
    <tr key={k} className="[&>td]:border-b [&>td]:border-neutral-800/20">
      <td className="px-2.5 py-1 text-fg-soft whitespace-nowrap">
        {/* ⚠ THE LABEL IS THE BUTTON, and the ⓘ stays a separate, non-clickable thing beside it.
            One control per meaning: the note explains what the row IS, the label opens what it was
            computed FROM. Nesting the tip inside the button would make hovering to read the
            definition look like the first half of a click. */}
        <button type="button" onClick={() => setDrill(k)} title={copy.showNumbers}
          className="cursor-pointer text-left hover:text-fg-strong hover:underline
                     decoration-dotted underline-offset-2 transition-colors">
          {copy.rowLabel[k]}
        </button>
        {/* ⚠ THE BOOK'S SIDE, NOT THE INDEX'S — see `rateSub`. `windows` is ascending, so its
            last entry is the longest one on screen; the forward row overrides it with its own. */}
        <InfoTip className="ml-1" content={tipFor(k, rateSub(
          a && windows.length ? rate(a.level, windows[windows.length - 1]) : null))} />
      </td>
      {windows.map((y) => {
        const got = a ? rate(a.level, y) : null;
        if (a && got) traceRate(k, 'portfolio', a.level, y, got);
        return (
          <RateCell key={`a${y}`} got={got} copy={copy} pending={pending}
            span={shown.length / windows.length} ownWindow={windows !== shown} />
        );
      })}
      {windows.map((y) => {
        const got = b ? rate(b.level, y) : null;
        if (b && got) traceRate(k, 'benchmark', b.level, y, got);
        return (
          <RateCell key={`b${y}`} got={got} copy={copy} pending={pending}
            span={shown.length / windows.length} ownWindow={windows !== shown} />
        );
      })}
      {windows.map((y) => {
        const span = shown.length / windows.length;
        // ⚠ SAME RULE AS `RateCell` — a spanned excess belongs to neither column it covers, so
        // right-aligning it would park it under `10y` exactly as the value did.
        const align = span > 1 ? 'text-center' : 'text-right';
        if (!a || !b) {
          return (
            <td key={`e${y}`} colSpan={span}
              className={`px-2.5 py-1 ${align} text-fg-faint`}>
              {pending ? <LoadingDots /> : '…'}
            </td>
          );
        }
        const e = cagrExcess(rate(a.level, y), rate(b.level, y));
        return (
          <td key={`e${y}`} colSpan={span}
            className={`px-2.5 py-1 ${align} font-mono tabular-nums`}>
            {e.pp == null
              ? <InfoTip text={e.reason} className="cursor-default text-fg-faint">—</InfoTip>
              : <>
                <span className={e.pp >= 0 ? 'text-pos-300' : 'text-neg-300'}>
                  {/* ⚠ THE SAME DIGITS AS THE TWO CELLS IT IS THE DIFFERENCE OF, or the row stops
                      adding up on screen: 39.53 − 4.55 is 34.98, and 35.0 is what a reader gets
                      for doubting it. See `CAGR_DECIMALS`. */}
                  {`${e.pp >= 0 ? '+' : ''}${e.pp.toFixed(CAGR_DECIMALS)}`}
                </span>
                {/* Same rule as the value's badge — see `RateCell`. */}
                {windows !== shown
                  && <span className="ml-1.5 text-[10px] text-fg-faint">
                    {y}{copy.yearSuffix}
                  </span>}
              </>}
          </td>
        );
      })}
    </tr>
  );

  /** One metric's three cells (book, index, excess) for one window. */
  const meanRow = (
    k: MeasureKey,
    a: Map<number, number | null>, b: Map<number, number | null>, endX: number | null,
    /** ⚠ NOT DERIVABLE FROM `a`/`b` HERE — both are already-built Maps, and an EMPTY one means
     *  "still loading" and "this book has no such line" alike. See `arriving`. */
    pending: boolean,
    unit: Unit = 'pct',
    /** Applied AFTER the window mean — see `MeanTransform`. Coverage is the only row that needs it. */
    transform?: MeanTransform,
  ) => (
    <tr key={k} className="[&>td]:border-b [&>td]:border-neutral-800/20">
      <td className="px-2.5 py-1 text-fg-soft whitespace-nowrap">
        {/* ⚠ THE LABEL IS THE BUTTON, and the ⓘ stays a separate, non-clickable thing beside it.
            One control per meaning: the note explains what the row IS, the label opens what it was
            computed FROM. Nesting the tip inside the button would make hovering to read the
            definition look like the first half of a click. */}
        <button type="button" onClick={() => setDrill(k)} title={copy.showNumbers}
          className="cursor-pointer text-left hover:text-fg-strong hover:underline
                     decoration-dotted underline-offset-2 transition-colors">
          {copy.rowLabel[k]}
        </button>
        {/* ⚠ THE BOOK'S SIDE, NOT THE INDEX'S — see `meanSub`. */}
        <InfoTip className="ml-1" content={tipFor(k, meanSub(
          a, endX, shown[shown.length - 1] ?? 0, transform,
        ))} />
      </td>
      {shown.map((y) => (
        <MeanCell key={`a${y}`} copy={copy} unit={unit} transform={transform} pending={pending}
          got={endX == null ? null : windowMean(a, endX, y)} />
      ))}
      {shown.map((y) => (
        <MeanCell key={`b${y}`} copy={copy} unit={unit} transform={transform} pending={pending}
          got={endX == null ? null : windowMean(b, endX, y)} />
      ))}
      {shown.map((y) => {
        if (endX == null) {
          return (
            <td key={`e${y}`} className="px-2.5 py-1 text-right text-fg-faint">
              {pending ? <LoadingDots /> : '…'}
            </td>
          );
        }
        const wa = windowMean(a, endX, y);
        const wb = windowMean(b, endX, y);
        // ⚠ `meanExcess` STILL DECIDES WHETHER THERE IS AN EXCESS AT ALL — it carries the guard
        // that refuses two different windows, and a transformed difference over mismatched years
        // would be exactly as meaningless as an untransformed one.
        const e = meanExcess(wa, wb);
        // ⚠ BUT THE VALUE IS RECOMPUTED ON THE TRANSFORMED SIDE. For coverage, `e.pp` is a
        // difference of BURDENS — right sign, wrong quantity, wrong unit — and printing it beside
        // two figures in × would be a third number in a fourth unit.
        const diff = e.pp == null ? null
          : !transform ? e.pp
            : (() => {
              const ta = transform((wa as { mean: number }).mean);
              const tb = transform((wb as { mean: number }).mean);
              return ta == null || tb == null ? null : ta - tb;
            })();
        return (
          <td key={`e${y}`} className="px-2.5 py-1 text-right font-mono tabular-nums">
            {diff == null
              ? <InfoTip text={e.pp == null ? e.reason : copy.noCoverageExcess}
                className="cursor-default text-fg-faint">—</InfoTip>
              : <span className={diff >= 0 ? 'text-pos-300' : 'text-neg-300'}>
                {`${diff >= 0 ? '+' : ''}${diff.toFixed(1)}`}
              </span>}
          </td>
        );
      })}
    </tr>
  );

  /**
   * ⚠ ONE `target`/`benchTarget` PAIR FOR EVERY PANEL — the same two this table computed from, so
   * what opens is this book against this benchmark and not a differently-scoped view of the metric.
   */
  const drillProps = {
    target: holdingsTarget,
    portfolioName: holdingsName,
    benchTarget,
    benchLabel: bench,
    onClose: () => setDrill(null),
  };
  const matrix = drill ? MATRIX_ROWS[drill] : undefined;

  return (
    <div className="space-y-2 p-1">
      <div className="flex items-center gap-3 flex-wrap">
        {/* ⚠ THE TITLE FOLLOWS THE SELECTION. "Quality, five and ten years" over a table showing
            only one of them is a caption contradicting the thing it captions — and it is the line
            a reader trusts when they have forgotten which chips they left on. */}
        <h3 className="text-[13px] font-medium text-fg-strong">{copy.title(shown)}</h3>
        <div className="ml-auto flex items-center gap-1.5">
          {WINDOWS.map((w) => {
            const last = shownW.has(w) && shownW.size === 1;
            return (
              <Chip key={w} on={shownW.has(w)} onClick={() => toggleW(w)} disabled={last}
                title={last ? copy.lastWindowLocked : copy.showWindow(w)}>
                {w}{copy.yearSuffix}
              </Chip>
            );
          })}
          <span className="w-px h-4 bg-neutral-800/40 mx-1" aria-hidden />
          <div className="inline-flex rounded-lg border border-neutral-700 overflow-hidden text-[11px]">
            {CAGR_BENCHMARKS.map((b) => (
              <button key={b} type="button" onClick={() => setBench(b)} aria-pressed={bench === b}
                className={`cursor-pointer px-2.5 py-0.5 font-medium transition-colors ${
                  bench === b ? 'bg-accent-600 text-white' : 'text-fg-muted hover:bg-overlay/5'}`}>
                {b}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* ⚠ A SEPARATE LINE FROM THE WINDOW AND BENCHMARK CONTROLS, because it is a different KIND
          of choice: those two reshape the columns, these pick which questions get asked at all.
          Crowded onto one row the four measure names read as more index options. */}
      <div className="flex items-center gap-1.5 flex-wrap">
        <span className="text-[11px] text-fg-faint mr-0.5">{copy.rowsLabel}</span>
        {MEASURE_KEYS.map((k) => (
          <Chip key={k} on={on(k)} onClick={() => toggleM(k)}
            title={on(k) ? copy.hideRow(copy.chip[k]) : copy.showRow(copy.chip[k])}>
            {copy.chip[k]}
          </Chip>
        ))}
      </div>

      {err && <p className="text-xs text-neg-300">{err}</p>}
      {index.err && (
        <p className="text-xs text-warn-300">{bench}: {index.err}</p>
      )}
      {!ready && !err && <p className="text-xs text-fg-subtle">{copy.loading}</p>}

      {/* ⚠⚠ `w-fit`, NOT `w-full` — THE STRETCH WAS THE WHITESPACE. Seven columns of short
          percentages under `w-full` are spread across the whole modal, so most of the table is the
          gaps between its own numbers and the eye has to travel the width of the dialog to read one
          row. Sized to content, the figures sit close enough to be compared without tracking, which
          is the entire job of a summary table.

          ⚠ `max-w-full` KEEPS THE SCROLL BOX HONEST. `w-fit` alone would let the table push past
          the card on a narrow viewport instead of scrolling inside it — the horizontal-overflow
          rule the whole app follows (see the responsive notes in CLAUDE.md). */}
      {/* ⚠ ZERO ROWS IS A STATE, NOT A BUG — and unlike zero WINDOWS it costs nothing structurally,
          so the chips stay free rather than the last one locking. It just has to say so: a bordered
          box containing nothing but column headings reads as a failed load. */}
      {ready && shownM.size === 0 && (
        <p className="text-xs text-fg-subtle">{copy.noRows}</p>
      )}

      {ready && shownM.size > 0 && (
        <div className="w-fit max-w-full overflow-auto rounded-lg border border-neutral-800/40">
          <table className="text-xs">
            <thead className="bg-page">
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide">
                <th className="px-2.5 py-1 font-medium text-left" rowSpan={2}>
                  {copy.colMeasure}
                </th>
                <th className="px-2.5 py-1 font-medium text-center border-l border-neutral-800/40"
                  colSpan={shown.length}>{holdingsName}</th>
                <th className="px-2.5 py-1 font-medium text-center border-l border-neutral-800/40"
                  colSpan={shown.length}>{bench}</th>
                <th className="px-2.5 py-1 font-medium text-center border-l border-neutral-800/40"
                  colSpan={shown.length}>{copy.colExcess}</th>
              </tr>
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide
                             border-b border-neutral-800/40">
                {/* ⚠ BUILT FROM `shown`, THREE TIMES — the divider falls on the first column of each
                    group, so the rule is `i % shown.length`, not `i % 2`. Hardcoded at 2 it drew a
                    border down the middle of nothing as soon as one window was filtered out. */}
                {[...shown, ...shown, ...shown].map((w, i) => (
                  <th key={i} className={`${th} ${
                    i % shown.length === 0 ? 'border-l border-neutral-800/40' : ''}`}>
                    {w}{copy.yearSuffix}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {/* ⚠ THE RATE ROWS ARE NAMED `CAGR`/`expected`; the two means are named `avg`.
                  They are different questions and the labels are the only thing saying so. */}
              {on('revCagr') && rateRow('revCagr', bookRev, idxRev,
                (lvl, y) => lineCagr(lvl, y, revEnd ?? undefined),
                arriving(revenue, bRevenue))}
              {/* ⚠ THE HISTORY OF THE SERIES THE LAST ROW FORECASTS, on the same shared end period
                  (`epsBase`) — so the rate hands over to the expectation instead of ending
                  somewhere else. See the ⚠ where `epsBase` is computed. */}
              {on('epsCagr') && rateRow('epsCagr', bookEps, idxEps,
                (lvl, y) => lineCagr(lvl, y, epsBase ?? undefined),
                arriving(epsNri, bEpsNri))}
              {on('fcfCagr') && rateRow('fcfCagr', bookBlend, idxBlend,
                (lvl, y) => lineCagr(lvl, y, fcfEnd ?? undefined),
                arriving(fcfPs, bFcfPs))}
              {/* ⚠ ITS OWN `priceEnd`, NOT `fcfEnd`. Every row on this table pins both sides to the
                  latest period THEY share, and the price line does not end where the FCF line does:
                  a company files a fiscal-year-end price with the same statements, but the coverage
                  floor is crossed by a different set of constituents (a price exists where a per-
                  share FCF may not). Borrowing the neighbouring row's window would silently measure
                  one row over a span its own line does not reach — see `lineCagr`'s ⚠. */}
              {on('priceCagr') && rateRow('priceCagr', bookPrice, idxPrice,
                (lvl, y) => lineCagr(lvl, y, priceEnd ?? undefined),
                arriving(pricePs, bPricePs))}
              {/* ⚠ THE DENOMINATOR OF THE ROIC ROW BELOW, deliberately adjacent to it: capital
                  growing faster than the return on it is a book buying its growth, and neither row
                  says that alone. */}
              {/* ⚠ `roicData`/`bRoic` — invested capital is DERIVED from the ROIC payload (see
                  `bookInvCap`), so this row waits on that fetch and not on one of its own. */}
              {on('invCapCagr') && rateRow('invCapCagr', bookInvCap, idxInvCap,
                (lvl, y) => lineCagr(lvl, y, invCapEnd ?? undefined),
                arriving(roicData, bRoic))}
              {/* ⚠ THE WEDGE BETWEEN THE REVENUE ROW AND THE PER-SHARE ROWS — see its note. */}
              {on('sharesCagr') && rateRow('sharesCagr', bookShares, idxShares,
                (lvl, y) => lineCagr(lvl, y, sharesEnd ?? undefined),
                arriving(sharesResp, bShares))}
              {on('grossMargin')
                && meanRow('grossMargin', book.grossMargin, index.grossMargin, grossEnd,
                  arriving(grossM, bGrossM))}
              {on('fcfMargin') && meanRow('fcfMargin', book.margin, index.margin, marginEnd,
                arriving(marginData, bMargin))}
              {on('roic') && meanRow('roic', book.roic, index.roic, roicEnd,
                arriving(roicData, bRoic))}
              {on('cashConv') && meanRow('cashConv', book.cashConv, index.cashConv, convEnd,
                arriving(cashConvD, bCashConv))}
              {/* ⚠ `'mult'` — THE ONLY NON-PERCENTAGE ROW HERE. See `Unit`: 12.4× printed as
                  12.4% reads as the exact inverse of what it says. */}
              {/* ⚠⚠ THE ONLY ROW WITH A `transform`, and it is what keeps the average honest: the
                  cells hold the interest BURDEN and are inverted to coverage only after the window
                  mean. See `coverageFromBurden` — inverting per year instead drops every debt-free
                  year (a burden of 0 has no reciprocal) and lets one high-coverage year run away
                  with the mean. */}
              {on('intCover')
                && meanRow('intCover', book.intBurden, index.intBurden, coverEnd,
                  arriving(intBurden, bIntBurden), 'mult', coverageFromBurden)}
              {/* ⚠⚠ LAST, AND VISUALLY SEPARATED, BECAUSE IT IS THE ONLY ROW THAT IS NOT A
                  MEASUREMENT. Everything above happened; this is what analysts currently expect,
                  revised whenever they like and systematically optimistic. Sitting it among the
                  historical rows in the same ink would make a consensus read as a track record. */}
              {/* ⚠ THE WINDOW IS IN THE LABEL, NOT ONLY BESIDE THE FIGURE. Every other row takes
                  its window from the column heading, so this one has to carry its own before the
                  eye reaches a number — reported as unclear when the value merely sat under the
                  `10y` heading. Three signals now: the label, the centring, and the `3y` beside the
                  figure; the row is the only place in the table where a heading does not apply. */}
              {on('epsFwd') && rateRow('epsFwd', bookEps, idxEps,
                (lvl, y) => forwardCagr(lvl, y, epsBase ?? undefined),
                arriving(epsNri, bEpsNri), EXPECTED_WINDOW)}
            </tbody>
          </table>
        </div>
      )}

      {/* ⚠ THE FOOTNOTE FOLLOWS THE CHIPS. It used to assert two things unconditionally — that the
          headings read "5y/10y" and that the expectation is "centred across both columns" — and
          either can now be false. A note explaining a row that is switched off, or naming a column
          that is not on screen, is worse than no note: it is the part of the page a reader turns to
          precisely when they doubt what they are seeing. */}
      {/* ⚠ A `<div>`, NOT A `<p>` — AND THAT IS A CORRECTNESS FIX, NOT A STYLING ONE. This footnote
          embeds an `InfoTip`, whose `TipCard` is built from `<div>`s, and a `<div>` inside a `<p>`
          is invalid HTML: the browser's parser CLOSES the paragraph at the opening div, so the
          server's markup and React's tree disagree and hydration fails outright. It only surfaced
          once the "why they differ" tip was added to the prose. `leading-snug`/`max-w` carry over
          unchanged; nothing about the rendering was meant to move. */}
      <div className="text-[11px] text-fg-faint leading-snug max-w-[54rem]">
        {copy.footnote({
          windows: shown,
          showEps: on('epsFwd'),
          // ⚠ ANY rate row, not just FCF/share — the clause is about every one of them.
          showFcf: RATE_KEYS.some(on),
          showPrice: on('priceCagr'),
          // ⚠ THE ROWS THE MEMBER RULE ACTUALLY APPLIES TO, asked of `POSITIVE_ONLY_ROWS` rather
          // than listed here — the set lives beside the metric keys it names, so a metric joining
          // or leaving the rule cannot leave this sentence claiming the wrong rows.
          showFiltered: POSITIVE_ONLY_ROWS.some(on),
          whyLink: <InfoTip text={copy.whyDiffer}>{copy.whyDifferLabel}</InfoTip>,
        })}
      </div>

      {/* ⚠⚠ THE GROUND NUMBERS — the SAME panel the matching Long Equity card opens, never a second
          inspector. See `MATRIX_ROWS`. Each reads the endpoint this table read, so what a reader
          checks is the input to the figure they clicked, not a re-derivation of it.
          ⚠ RENDERED AT THE ROOT, not inside the row: these are dialogs and a `<tr>` is not a place
          to mount one — the browser hoists stray elements out of a table body, which is how a modal
          comes to render above the table it belongs to. */}
      {drill && matrix && (
        <HoldingsRevenueModal {...drillProps} metric={matrix.metric} unit={matrix.unit}
          noun={matrix.noun} seriesLabel={copy.rowLabel[drill]} />
      )}
      {drill === 'grossMargin' && <GrossMarginInputsModal {...drillProps} />}
      {drill === 'fcfMargin' && <MarginInputsModal {...drillProps} />}
      {drill === 'roic' && <CashReturnInputsModal {...drillProps} />}
      {/* ⚠ THE SAME PANEL AS `roic`, AND THAT IS THE RIGHT ANSWER RATHER THAN A SHORTCUT: invested
          capital is derived from the two raw lines `cash-return-inputs` returns, so this IS where
          its numbers are. A dedicated panel would be a second view of one payload. */}
      {drill === 'invCapCagr' && <CashReturnInputsModal {...drillProps} />}
      {drill === 'cashConv' && <CashConversionInputsModal {...drillProps} />}
      {/* ⚠ THE BURDEN'S OWN PANEL, WHICH IS THE HONEST ONE FOR A COVERAGE ROW. Coverage is one over
          the burden and there is no separate coverage series to inspect — the panel lists the
          interest expense and the operating profit every figure came from, which is what a reader
          checking 79.3× actually needs. See `coverageFromBurden`. */}
      {drill === 'intCover' && <InterestBurdenInputsModal {...drillProps} />}
    </div>
  );
}
