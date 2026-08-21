'use client';

import { useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import InfoTip from '../InfoTip';
import { useBenchInputs, type BenchTarget } from './benchSeries';
import { CAGR_BENCHMARKS, type CagrBenchmark } from './CagrTable';
import { roicByYear, type CashReturnInputs } from './cashReturnData';
import { buildBlend, type Blend, type Resp } from './fundamentalBlend';
import { cagrExcess, commonEndPeriod, forwardCagr, lineCagr, type Cagr } from './lineCagr';
import { marginByYear, xToPeriod, type MarginInputs } from './marginData';
import { grossMarginByYear, type GrossMarginInputs } from './grossMarginData';
import { cashConversionByYear, type CashConversionInputs } from './cashConversionData';
import {
  coverageFromBurden, interestBurdenByYear, type InterestBurdenInputs,
} from './interestBurdenData';
import HoldingsRevenueModal, { type Target } from './HoldingsRevenueModal';
import GrossMarginInputsModal from './GrossMarginInputsModal';
import MarginInputsModal from './MarginInputsModal';
import CashReturnInputsModal from './CashReturnInputsModal';
import CashConversionInputsModal from './CashConversionInputsModal';
import InterestBurdenInputsModal from './InterestBurdenInputsModal';
import { COPY, MEASURE_KEYS, type MeasureKey, type TablesCopy } from './tablesCopy';
import { latestCommonX, meanExcess, windowMean, type WindowMean } from './windowStats';
import {
  meanSub, rateSub, type MeanTransform,
} from './tablesSubstitution';
import { type Lang } from '../../../lib/i18n';

/**
 * `Tables` — the three quality reads on one screen, book against index, over 5 and 10 years.
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
  fcfCagr: { metric: 'fcf_ps', unit: 'per_share', noun: 'FCF per share' },
  priceCagr: { metric: 'price_ps', unit: 'per_share', noun: 'share price' },
  epsFwd: { metric: 'eps_nri', unit: 'per_share', noun: 'EPS (excl. NRI)' },
};

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
function RateCell({ got, copy, span = 1, ownWindow = false }:
{ got: Cagr | null; copy: TablesCopy; span?: number; ownWindow?: boolean }) {
  /**
   * ⚠⚠ A SPANNED CELL IS **CENTRED**, NOT RIGHT-ALIGNED, AND THAT IS NOT A STYLE CHOICE. Right
   * alignment is correct for a number that belongs to a column; this one belongs to NEITHER of the
   * two it spans, and right-aligning it parked the 3-year expectation hard against the `10y` edge —
   * reported, correctly, as looking like a ten-year figure. Centring is the only position that says
   * "this is not in either column", and it is why the row reads as its own thing at a glance.
   */
  const align = span > 1 ? 'text-center' : 'text-right';
  if (!got) return <td colSpan={span} className={`px-2.5 py-1 ${align} text-fg-faint`}>…</td>;
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
          {pctCell(got.pct, true)}
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
function MeanCell({ got, copy, unit = 'pct', transform }:
{ got: WindowMean | null; copy: TablesCopy; unit?: Unit; transform?: MeanTransform }) {
  if (!got) return <td className="px-2.5 py-1 text-right text-fg-faint">…</td>;
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
  /** ⚠ ITS OWN BENCHMARK, DEFAULTING TO AEX — the tab's other cards follow whatever index the modal
   *  was opened against; here the question IS "against what", so it gets a picker. */
  const [bench, setBench] = useState<CagrBenchmark>('AEX');

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
        const [m, c, f, e, p, rev, gm, cc, ib] = await Promise.all([
          post<MarginInputs>('margin-inputs', holdingsTarget),
          post<CashReturnInputs>('cash-return-inputs', holdingsTarget),
          post<Resp>('portfolio-revenue-matrix?metric=fcf_ps', holdingsTarget),
          // ⚠⚠ `eps_nri` — EXCLUDING non-recurring items, whose paired forecast is
          // `annual_eps_nri_estimate`. GuruFocus also publishes an INCLUDING-NRI consensus
          // (`annual_per_share_eps_estimate`) that agrees to a cent on almost every company
          // (Apple 8.76 vs 8.77), so joining the wrong one onto this actual would put a one-off
          // impairment on the wrong side of the join and nothing on screen would say so. The
          // metric key carries the pairing — see the EPS card's own ⚠⚠ in `LongEquityTab`.
          post<Resp>('portfolio-revenue-matrix?metric=eps_nri', holdingsTarget),
          // ⚠⚠ `price_ps` IS GURUFOCUS'S "Month End Stock Price" — the share price at each fiscal
          // YEAR END, filed alongside the fundamentals, not our daily `metric_data` closes. That is
          // deliberate and it is the only thing that makes this row comparable to the ones above
          // it: every other series on this table is indexed on the same fiscal-period axis, so a
          // price read on a calendar date would be measured over a window the neighbouring rows
          // are not. The cost is that the row is as fresh as the last filing, exactly like them.
          post<Resp>('portfolio-revenue-matrix?metric=price_ps', holdingsTarget),
          // ⚠ `revenue` IS THE MATRIX ENDPOINT'S DEFAULT METRIC, so this is the same request shape
          // as the three above it — no new endpoint, and `apiFetch`'s read cache means a reader who
          // has already opened Long Equity pays nothing for any of these four.
          post<Resp>('portfolio-revenue-matrix?metric=revenue', holdingsTarget),
          post<GrossMarginInputs>('gross-margin-inputs', holdingsTarget),
          post<CashConversionInputs>('cash-conversion-inputs', holdingsTarget),
          post<InterestBurdenInputs>('interest-burden-inputs', holdingsTarget),
        ]);
        if (!alive) return;
        setMarginData(m); setRoicData(c); setFcfPs(f); setEpsNri(e); setPricePs(p);
        setRevenue(rev); setGrossM(gm); setCashConvD(cc); setIntBurden(ib);
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
    'portfolio-revenue-matrix?metric=fcf_ps', benchTarget);
  const [bEpsNri, bEpsNriErr] = useBenchInputs<Resp>(
    'portfolio-revenue-matrix?metric=eps_nri', benchTarget);
  const [bPricePs, bPricePsErr] = useBenchInputs<Resp>(
    'portfolio-revenue-matrix?metric=price_ps', benchTarget);
  const [bRevenue, bRevenueErr] = useBenchInputs<Resp>(
    'portfolio-revenue-matrix?metric=revenue', benchTarget);
  const [bGrossM, bGrossMErr] = useBenchInputs<GrossMarginInputs>(
    'gross-margin-inputs', benchTarget);
  const [bCashConv, bCashConvErr] = useBenchInputs<CashConversionInputs>(
    'cash-conversion-inputs', benchTarget);
  const [bIntBurden, bIntBurdenErr] = useBenchInputs<InterestBurdenInputs>(
    'interest-burden-inputs', benchTarget);

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
    err: bMarginErr ?? bRoicErr ?? bFcfPsErr ?? bEpsNriErr ?? bPricePsErr
      ?? bRevenueErr ?? bGrossMErr ?? bCashConvErr ?? bIntBurdenErr,
  }), [bMargin, bRoic, bGrossM, bCashConv, bIntBurden, bFcfPs, sbcCorrection, bMarginErr,
    bRoicErr, bFcfPsErr, bEpsNriErr, bPricePsErr, bRevenueErr, bGrossMErr, bCashConvErr,
    bIntBurdenErr]);

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
  const bookBlend = useMemo(() => (book.fcfPs ? buildBlend(book.fcfPs) : null), [book.fcfPs]);
  const idxBlend = useMemo(() => (index.fcfPs ? buildBlend(index.fcfPs) : null), [index.fcfPs]);
  const fcfEnd = bookBlend && idxBlend ? commonEndPeriod(bookBlend.level, idxBlend.level) : null;
  /** ⚠ THE SAME `buildBlend` AS EVERY OTHER ROW, ON A DIFFERENT PAYLOAD — which is the whole of
   *  "weighted like the others". A price line assembled any other way (summing values, averaging
   *  levels) would be a second definition of the basket sitting one row from the first. */
  const bookRev = useMemo(() => (revenue ? buildBlend(revenue) : null), [revenue]);
  const idxRev = useMemo(() => (bRevenue ? buildBlend(bRevenue) : null), [bRevenue]);
  const revEnd = bookRev && idxRev ? commonEndPeriod(bookRev.level, idxRev.level) : null;
  const bookPrice = useMemo(() => (pricePs ? buildBlend(pricePs) : null), [pricePs]);
  const idxPrice = useMemo(() => (bPricePs ? buildBlend(bPricePs) : null), [bPricePs]);
  const priceEnd = bookPrice && idxPrice
    ? commonEndPeriod(bookPrice.level, idxPrice.level) : null;
  const bookEps = useMemo(() => (epsNri ? buildBlend(epsNri) : null), [epsNri]);
  const idxEps = useMemo(() => (bEpsNri ? buildBlend(bEpsNri) : null), [bEpsNri]);
  /** ⚠ THE SHARED BASE IS AN **ACTUAL** — `commonEndPeriod` only ever returns a reported period, so
   *  the expectation is always measured from something that happened. See `forwardCagr`. */
  const epsBase = bookEps && idxEps ? commonEndPeriod(bookEps.level, idxEps.level) : null;

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
  const tipFor = (k: MeasureKey, sub: string): string => {
    const parts = [copy.rowFormula[k](sbcCorrection)];
    if (sub) parts.push(sub);
    parts.push(copy.rowNote[k](sbcCorrection));
    return parts.join('\n\n');
  };

  /**
   * A row of RATES — one `Cagr` per side per window, plus the excess.
   *
   * ⚠ SHARED WITH THE FORWARD ROW ON PURPOSE. The historical rate and the expectation differ only
   * in which function produces the `Cagr`; rendering them twice is how one of them quietly stops
   * pinning both sides to the same window, or starts formatting a negative differently.
   */
  const rateRow = (
    k: MeasureKey, a: Blend | null, b: Blend | null,
    rate: (lvl: Blend['level'], years: number) => Cagr,
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
        <InfoTip className="ml-1" text={tipFor(k, rateSub(holdingsName,
          a && windows.length ? rate(a.level, windows[windows.length - 1]) : null))} />
      </td>
      {windows.map((y) => (
        <RateCell key={`a${y}`} got={a ? rate(a.level, y) : null} copy={copy}
          span={shown.length / windows.length} ownWindow={windows !== shown} />
      ))}
      {windows.map((y) => (
        <RateCell key={`b${y}`} got={b ? rate(b.level, y) : null} copy={copy}
          span={shown.length / windows.length} ownWindow={windows !== shown} />
      ))}
      {windows.map((y) => {
        const span = shown.length / windows.length;
        // ⚠ SAME RULE AS `RateCell` — a spanned excess belongs to neither column it covers, so
        // right-aligning it would park it under `10y` exactly as the value did.
        const align = span > 1 ? 'text-center' : 'text-right';
        if (!a || !b) {
          return <td key={`e${y}`} colSpan={span}
            className={`px-2.5 py-1 ${align} text-fg-faint`}>…</td>;
        }
        const e = cagrExcess(rate(a.level, y), rate(b.level, y));
        return (
          <td key={`e${y}`} colSpan={span}
            className={`px-2.5 py-1 ${align} font-mono tabular-nums`}>
            {e.pp == null
              ? <InfoTip text={e.reason} className="cursor-default text-fg-faint">—</InfoTip>
              : <>
                <span className={e.pp >= 0 ? 'text-pos-300' : 'text-neg-300'}>
                  {`${e.pp >= 0 ? '+' : ''}${e.pp.toFixed(1)}`}
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
        <InfoTip className="ml-1" text={tipFor(k, meanSub(
          holdingsName, a, endX, shown[shown.length - 1] ?? 0, transform,
        ))} />
      </td>
      {shown.map((y) => (
        <MeanCell key={`a${y}`} copy={copy} unit={unit} transform={transform}
          got={endX == null ? null : windowMean(a, endX, y)} />
      ))}
      {shown.map((y) => (
        <MeanCell key={`b${y}`} copy={copy} unit={unit} transform={transform}
          got={endX == null ? null : windowMean(b, endX, y)} />
      ))}
      {shown.map((y) => {
        if (endX == null) return <td key={`e${y}`} className="px-2.5 py-1 text-right text-fg-faint">…</td>;
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
                (lvl, y) => lineCagr(lvl, y, revEnd ?? undefined))}
              {on('fcfCagr') && rateRow('fcfCagr', bookBlend, idxBlend,
                (lvl, y) => lineCagr(lvl, y, fcfEnd ?? undefined))}
              {/* ⚠ ITS OWN `priceEnd`, NOT `fcfEnd`. Every row on this table pins both sides to the
                  latest period THEY share, and the price line does not end where the FCF line does:
                  a company files a fiscal-year-end price with the same statements, but the coverage
                  floor is crossed by a different set of constituents (a price exists where a per-
                  share FCF may not). Borrowing the neighbouring row's window would silently measure
                  one row over a span its own line does not reach — see `lineCagr`'s ⚠. */}
              {on('priceCagr') && rateRow('priceCagr', bookPrice, idxPrice,
                (lvl, y) => lineCagr(lvl, y, priceEnd ?? undefined))}
              {on('grossMargin')
                && meanRow('grossMargin', book.grossMargin, index.grossMargin, grossEnd)}
              {on('fcfMargin') && meanRow('fcfMargin', book.margin, index.margin, marginEnd)}
              {on('roic') && meanRow('roic', book.roic, index.roic, roicEnd)}
              {on('cashConv') && meanRow('cashConv', book.cashConv, index.cashConv, convEnd)}
              {/* ⚠ `'mult'` — THE ONLY NON-PERCENTAGE ROW HERE. See `Unit`: 12.4× printed as
                  12.4% reads as the exact inverse of what it says. */}
              {/* ⚠⚠ THE ONLY ROW WITH A `transform`, and it is what keeps the average honest: the
                  cells hold the interest BURDEN and are inverted to coverage only after the window
                  mean. See `coverageFromBurden` — inverting per year instead drops every debt-free
                  year (a burden of 0 has no reciprocal) and lets one high-coverage year run away
                  with the mean. */}
              {on('intCover')
                && meanRow('intCover', book.intBurden, index.intBurden, coverEnd, 'mult',
                  coverageFromBurden)}
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
                EXPECTED_WINDOW)}
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
          showFcf: on('fcfCagr'),
          showPrice: on('priceCagr'),
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
      {drill === 'cashConv' && <CashConversionInputsModal {...drillProps} />}
      {/* ⚠ THE BURDEN'S OWN PANEL, WHICH IS THE HONEST ONE FOR A COVERAGE ROW. Coverage is one over
          the burden and there is no separate coverage series to inspect — the panel lists the
          interest expense and the operating profit every figure came from, which is what a reader
          checking 79.3× actually needs. See `coverageFromBurden`. */}
      {drill === 'intCover' && <InterestBurdenInputsModal {...drillProps} />}
    </div>
  );
}
