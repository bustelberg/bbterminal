'use client';

import { useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import InfoTip from '../InfoTip';
import { useBenchInputs, type BenchTarget } from './benchSeries';
import { CAGR_BENCHMARKS, type CagrBenchmark } from './CagrTable';
import { roicByYear, type CashReturnInputs } from './cashReturnData';
import { buildBlend, type Resp } from './fundamentalBlend';
import { cagrExcess, commonEndPeriod, lineCagr } from './lineCagr';
import { marginByYear, xToPeriod, type MarginInputs } from './marginData';
import { type Target } from './HoldingsRevenueModal';
import { latestCommonX, meanExcess, windowMean, type WindowMean } from './windowStats';

/**
 * `Tables` — the three quality reads on one screen, book against index, over 5 and 10 years.
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
 * ⚠⚠ AND THE FCF/SHARE RATE IS POINT-TO-POINT, WHICH IS *NOT* WHAT THE GROWTH CARD SHOWS. That card
 * fits a log-linear trend through every year in the window (hence the R² beside it); this is
 * `(end/start)^(1/n) − 1`, the literal definition, decided deliberately. They will differ — most
 * where a single endpoint year is unrepresentative, which is exactly when the difference is worth
 * seeing — so the footnote names the divergence rather than leaving two "CAGR"s to be discovered.
 */

const WINDOWS = [5, 10] as const;

type Side = {
  margin: Map<number, number | null>;
  roic: Map<number, number | null>;
  fcfPs: Resp | null;
  err: string | null;
};

const pctCell = (v: number, sign: boolean) =>
  `${sign && v >= 0 ? '+' : ''}${v.toFixed(1)}%`;

/** A mean cell — the value, how much of the window it covers, and the window itself on hover. */
function MeanCell({ got }: { got: WindowMean | null }) {
  if (!got) return <td className="px-3 py-2 text-right text-fg-faint">…</td>;
  if (got.mean == null) {
    return (
      <td className="px-3 py-2 text-right">
        <InfoTip text={got.reason} className="cursor-default text-fg-faint">—</InfoTip>
      </td>
    );
  }
  // ⚠ AN INCOMPLETE WINDOW IS MARKED ON THE NUMBER, not left to the tooltip. "12.3%" and
  // "12.3% (4 of 5)" are different claims and only the second is true.
  const short = got.n < got.of;
  return (
    <td className="px-3 py-2 text-right font-mono tabular-nums text-fg-soft">
      <InfoTip className="cursor-default"
        text={`Mean of ${got.n} year${got.n === 1 ? '' : 's'} over ${xToPeriod(got.fromX)}–`
          + `${xToPeriod(got.toX)}${short ? `, of the ${got.of} asked for` : ''}. Weighted per year `
          + 'by the same weights the Long Equity chart uses — this is that line, averaged.'}>
        {pctCell(got.mean, false)}
        {short && <span className="ml-1 text-[10px] text-warn-300">({got.n}/{got.of})</span>}
      </InfoTip>
    </td>
  );
}

export default function TablesTab({ holdingsTarget, holdingsName, sbcCorrection }: {
  holdingsTarget: Target;
  holdingsName: string;
  sbcCorrection: boolean;
}) {
  /** ⚠ ITS OWN BENCHMARK, DEFAULTING TO AEX — the tab's other cards follow whatever index the modal
   *  was opened against; here the question IS "against what", so it gets a picker. */
  const [bench, setBench] = useState<CagrBenchmark>('AEX');
  const benchTarget: BenchTarget = useMemo(
    // ⚠ ANNUAL, WHATEVER THE TAB IS ON. A 5-year window of QUARTERS is fifteen months, and a
    // "5y CAGR" off it would be off by a factor of four — plausible and wrong on every row.
    () => ({ universe: bench, cadence: 'annual' }), [bench]);

  // ── the book ──────────────────────────────────────────────────────────────────────────────
  const [marginData, setMarginData] = useState<MarginInputs | null>(null);
  const [roicData, setRoicData] = useState<CashReturnInputs | null>(null);
  const [fcfPs, setFcfPs] = useState<Resp | null>(null);
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
        const [m, c, f] = await Promise.all([
          post<MarginInputs>('margin-inputs', holdingsTarget),
          post<CashReturnInputs>('cash-return-inputs', holdingsTarget),
          post<Resp>('portfolio-revenue-matrix?metric=fcf_per_share', holdingsTarget),
        ]);
        if (!alive) return;
        setMarginData(m); setRoicData(c); setFcfPs(f);
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
    'portfolio-revenue-matrix?metric=fcf_per_share', benchTarget);

  const book: Side = useMemo(() => ({
    margin: marginByYear(marginData?.rows ?? [], sbcCorrection),
    roic: roicByYear(roicData?.rows ?? []),
    fcfPs,
    err,
  }), [marginData, roicData, fcfPs, sbcCorrection, err]);

  const index: Side = useMemo(() => ({
    margin: bMargin ? marginByYear(bMargin.rows, sbcCorrection) : new Map(),
    roic: bRoic ? roicByYear(bRoic.rows) : new Map(),
    fcfPs: bFcfPs,
    err: bMarginErr ?? bRoicErr ?? bFcfPsErr,
  }), [bMargin, bRoic, bFcfPs, sbcCorrection, bMarginErr, bRoicErr, bFcfPsErr]);

  /**
   * ⚠⚠ ONE WINDOW PER ROW, SHARED BY BOTH SIDES. Each series ends at its own latest year, and a
   * twenty-holding book crosses into a new fiscal year long before a 1,900-name index does — so
   * left alone the book would be averaged 2021-2025 against the index's 2020-2024, printed side by
   * side under one heading. Computed per ROW because the three series do not end together either.
   */
  const marginEnd = latestCommonX(book.margin, index.margin);
  const roicEnd = latestCommonX(book.roic, index.roic);
  const bookBlend = useMemo(() => (book.fcfPs ? buildBlend(book.fcfPs) : null), [book.fcfPs]);
  const idxBlend = useMemo(() => (index.fcfPs ? buildBlend(index.fcfPs) : null), [index.fcfPs]);
  const fcfEnd = bookBlend && idxBlend ? commonEndPeriod(bookBlend.level, idxBlend.level) : null;

  const ready = marginData && roicData && fcfPs;
  const th = 'px-3 py-1.5 font-medium text-right whitespace-nowrap';

  /** One metric's three cells (book, index, excess) for one window. */
  const meanRow = (
    label: string, note: string,
    a: Map<number, number | null>, b: Map<number, number | null>, endX: number | null,
  ) => (
    <tr key={label} className="[&>td]:border-b [&>td]:border-neutral-800/20">
      <td className="px-3 py-2 text-fg-soft whitespace-nowrap">
        <InfoTip text={note} className="cursor-default">{label}</InfoTip>
      </td>
      {WINDOWS.map((y) => (
        <MeanCell key={`a${y}`} got={endX == null ? null : windowMean(a, endX, y)} />
      ))}
      {WINDOWS.map((y) => (
        <MeanCell key={`b${y}`} got={endX == null ? null : windowMean(b, endX, y)} />
      ))}
      {WINDOWS.map((y) => {
        if (endX == null) return <td key={`e${y}`} className="px-3 py-2 text-right text-fg-faint">…</td>;
        const e = meanExcess(windowMean(a, endX, y), windowMean(b, endX, y));
        return (
          <td key={`e${y}`} className="px-3 py-2 text-right font-mono tabular-nums">
            {e.pp == null
              ? <InfoTip text={e.reason} className="cursor-default text-fg-faint">—</InfoTip>
              : <span className={e.pp >= 0 ? 'text-pos-300' : 'text-neg-300'}>
                {`${e.pp >= 0 ? '+' : ''}${e.pp.toFixed(1)}`}
              </span>}
          </td>
        );
      })}
    </tr>
  );

  return (
    <div className="space-y-3 p-1">
      <div className="flex items-center gap-3 flex-wrap">
        <h3 className="text-sm font-medium text-fg-strong">Quality, five and ten years</h3>
        <div className="ml-auto inline-flex rounded-lg border border-neutral-700 overflow-hidden text-[11px]">
          {CAGR_BENCHMARKS.map((b) => (
            <button key={b} type="button" onClick={() => setBench(b)} aria-pressed={bench === b}
              className={`cursor-pointer px-2.5 py-0.5 font-medium transition-colors ${
                bench === b ? 'bg-accent-600 text-white' : 'text-fg-muted hover:bg-overlay/5'}`}>
              {b}
            </button>
          ))}
        </div>
      </div>

      {err && <p className="text-xs text-neg-300">{err}</p>}
      {index.err && (
        <p className="text-xs text-warn-300">{bench}: {index.err}</p>
      )}
      {!ready && !err && <p className="text-xs text-fg-subtle">Loading…</p>}

      {ready && (
        <div className="overflow-auto rounded-lg border border-neutral-800/40">
          <table className="w-full text-xs">
            <thead className="bg-page">
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide">
                <th className="px-3 py-1 font-medium text-left" rowSpan={2}>Measure</th>
                <th className="px-3 py-1 font-medium text-center border-l border-neutral-800/40"
                  colSpan={2}>{holdingsName}</th>
                <th className="px-3 py-1 font-medium text-center border-l border-neutral-800/40"
                  colSpan={2}>{bench}</th>
                <th className="px-3 py-1 font-medium text-center border-l border-neutral-800/40"
                  colSpan={2}>Excess (pp)</th>
              </tr>
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide
                             border-b border-neutral-800/40">
                {['5y', '10y', '5y', '10y', '5y', '10y'].map((w, i) => (
                  <th key={i} className={`${th} ${i % 2 === 0 ? 'border-l border-neutral-800/40' : ''}`}>
                    {w}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {/* ⚠ THE RATE ROW IS FIRST AND NAMED `CAGR`; the two below it are named `avg`. They
                  are different questions and the labels are the only thing saying so. */}
              <tr className="[&>td]:border-b [&>td]:border-neutral-800/20">
                <td className="px-3 py-2 text-fg-soft whitespace-nowrap">
                  <InfoTip className="cursor-default"
                    text={'Compound annual growth of the weighted FCF-per-share line, point to '
                      + 'point. ⚠ The Long Equity growth card fits a log-linear TREND through every '
                      + 'year instead (that is what its R² is about), so the two will differ — most '
                      + 'where one endpoint year is unrepresentative, which is when the gap is '
                      + 'worth seeing.'}>
                    FCF / share CAGR
                  </InfoTip>
                </td>
                {WINDOWS.map((y) => {
                  const got = bookBlend
                    ? lineCagr(bookBlend.level, y, fcfEnd ?? undefined) : null;
                  return (
                    <td key={`a${y}`} className="px-3 py-2 text-right font-mono tabular-nums">
                      {!got ? <span className="text-fg-faint">…</span>
                        : got.pct == null
                          ? <InfoTip text={got.reason} className="cursor-default text-fg-faint">—</InfoTip>
                          : <span className={got.pct >= 0 ? 'text-fg-soft' : 'text-neg-300'}>
                            {pctCell(got.pct, true)}
                          </span>}
                    </td>
                  );
                })}
                {WINDOWS.map((y) => {
                  const got = idxBlend ? lineCagr(idxBlend.level, y, fcfEnd ?? undefined) : null;
                  return (
                    <td key={`b${y}`} className="px-3 py-2 text-right font-mono tabular-nums">
                      {!got ? <span className="text-fg-faint">…</span>
                        : got.pct == null
                          ? <InfoTip text={got.reason} className="cursor-default text-fg-faint">—</InfoTip>
                          : <span className={got.pct >= 0 ? 'text-fg-soft' : 'text-neg-300'}>
                            {pctCell(got.pct, true)}
                          </span>}
                    </td>
                  );
                })}
                {WINDOWS.map((y) => {
                  if (!bookBlend || !idxBlend) {
                    return <td key={`e${y}`} className="px-3 py-2 text-right text-fg-faint">…</td>;
                  }
                  const e = cagrExcess(lineCagr(bookBlend.level, y, fcfEnd ?? undefined),
                                       lineCagr(idxBlend.level, y, fcfEnd ?? undefined));
                  return (
                    <td key={`e${y}`} className="px-3 py-2 text-right font-mono tabular-nums">
                      {e.pp == null
                        ? <InfoTip text={e.reason} className="cursor-default text-fg-faint">—</InfoTip>
                        : <span className={e.pp >= 0 ? 'text-pos-300' : 'text-neg-300'}>
                          {`${e.pp >= 0 ? '+' : ''}${e.pp.toFixed(1)}`}
                        </span>}
                    </td>
                  );
                })}
              </tr>
              {meanRow('FCF margin (avg)',
                `Free cash flow ${sbcCorrection ? 'net of stock comp ' : ''}÷ revenue, averaged over `
                + 'the window. A ratio does not compound, so this is a mean and not a rate — it is '
                + 'the Long Equity margin chart, averaged. Follows the SBC checkbox.',
                book.margin, index.margin, marginEnd)}
              {meanRow('ROIC (avg)',
                'GuruFocus’s own published return on invested capital, weight-weighted per year '
                + 'and averaged over the window. ⚠ Unaffected by the SBC checkbox — there is no '
                + 'numerator of ours to adjust.',
                book.roic, index.roic, roicEnd)}
            </tbody>
          </table>
        </div>
      )}

      <p className="text-[11px] text-fg-faint leading-relaxed">
        Both sides are measured over the <strong>same</strong> window per row — the latest year they
        share — so the Excess column subtracts like from like. A dash means one side has nothing
        there; hover it. The CAGR row is point-to-point and will not match the FCF/share growth
        card, which fits a trend through every year (
        <InfoTip text="Point-to-point is (end/start)^(1/n) − 1: only the two endpoint years matter, so one weak year at either end swings it. The card's log-linear fit uses all of them and reports R² for how well they line up. Neither is wrong; a wide gap between them means the endpoints are unrepresentative.">
          why they differ
        </InfoTip>
        ).
      </p>
    </div>
  );
}
