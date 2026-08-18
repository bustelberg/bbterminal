'use client';

import InfoTip from '../InfoTip';
import { type Blend } from './fundamentalBlend';
import { cagrExcess, commonEndPeriod, lineCagr } from './lineCagr';

/**
 * The `Table` view: what the book and the index compounded at, over five and ten years.
 *
 * ⚠ IT REPLACES THE TWO MATRICES RATHER THAN SITTING ABOVE THEM. Those are what this is derived
 * from, and the modal is already 84vh; a summary you have to scroll past its own inputs to reach is
 * a summary nobody reads. The other three views are still one click away.
 *
 * ⚠⚠ IT READS THE SAME LINE THE CHART DRAWS. `buildBlend(...).level` is the weighted, chained,
 * coverage-floored series the `Rebased` footer prints — so a CAGR here can be checked against the
 * row beneath it. Deriving it "the same way" from the raw cells is how a summary comes to disagree
 * with the table it summarises; see `lineCagr` for why doing it on the LINE is also the only way it
 * is defined at all when the metric goes negative.
 */

/** The benchmarks worth comparing a book against here. Order is the offer order; AEX leads because
 *  it is the default and the smallest, so it loads first and reads fastest. */
export const CAGR_BENCHMARKS = ['AEX', 'ACWI', 'SP500'] as const;
export type CagrBenchmark = typeof CAGR_BENCHMARKS[number];

const WINDOWS = [5, 10] as const;

const pct = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`;

/** One CAGR cell: the rate, or a dash whose tooltip says which absence this is. */
function Cell({ blend, years, endPeriod }: {
  blend: Blend | null; years: number; endPeriod: string | null;
}) {
  if (!blend) return <td className="px-3 py-2 text-right text-fg-faint">…</td>;
  const got = lineCagr(blend.level, years, endPeriod ?? undefined);
  if (got.pct == null) {
    return (
      <td className="px-3 py-2 text-right">
        {/* ⚠ `InfoTip`, NOT `title=` — the native tooltip sits for a second or two before appearing,
            which is long enough for a reader to conclude the dash means "zero" and move on. */}
        <InfoTip text={got.reason} className="cursor-default text-fg-faint">—</InfoTip>
      </td>
    );
  }
  return (
    <td className={`px-3 py-2 text-right font-mono tabular-nums
                    ${got.pct >= 0 ? 'text-fg-soft' : 'text-neg-300'}`}>
      {/* ⚠ THE WINDOW IS ON THE NUMBER, because "5y" is a claim the data has to support and does
          not always support identically on both rows — a book whose history starts in 2019 and an
          index reaching back to 2004 can both answer "5y" and only one of them can answer "10y". */}
      <InfoTip text={`${got.from} → ${got.to}, ${got.years} years, compounded annually. `
        + 'Measured on the weighted line the chart draws — the same series the Rebased footer '
        + 'prints — not re-derived from the cells.'}
        className="cursor-default">
        {pct(got.pct)}
      </InfoTip>
    </td>
  );
}

export default function CagrTable({
  portfolio, benchmark, portfolioName, benchLabel, metricLabel,
  benchChoice, onBenchChoice, benchLoading, benchErr,
}: {
  portfolio: Blend | null;
  benchmark: Blend | null;
  portfolioName: string;
  benchLabel: string;
  /** The chart's own name for the series — 'FCF / share', 'Revenue'. Names what is compounding. */
  metricLabel: string;
  benchChoice: CagrBenchmark;
  onBenchChoice: (b: CagrBenchmark) => void;
  benchLoading: boolean;
  benchErr: string | null;
}) {
  const th = 'px-3 py-1.5 font-medium text-right whitespace-nowrap';
  /**
   * ⚠⚠ ONE WINDOW FOR BOTH ROWS — see `lineCagr`'s `endPeriod`. Each line ends at its own latest
   * DRAWN period, and a twenty-holding book crosses the coverage floor for a new fiscal year weeks
   * before a 1,900-name index does. Left alone, the book would be measured 2020→2025 and the index
   * 2019→2024, printed side by side under one "5y" heading, and the Excess would be a subtraction
   * of two different questions.
   */
  const endPeriod = portfolio && benchmark
    ? commonEndPeriod(portfolio.level, benchmark.level)
    : null;
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-3 flex-wrap">
        <h3 className="text-sm font-medium text-fg-strong">
          {metricLabel} — compound annual growth
        </h3>
        {/* ⚠ THE PICKER IS THE TABLE'S OWN, NOT THE CHART'S. The drill-down inherits whatever
            benchmark the card behind it was drawn against, which is fine for the matrices; here the
            question is "against what", and having to close the modal and reopen it from a different
            chart to ask it of ACWI would make the comparison not worth having. */}
        <div className="ml-auto inline-flex rounded-lg border border-neutral-700 overflow-hidden text-[11px]">
          {CAGR_BENCHMARKS.map((b) => (
            <button key={b} type="button" onClick={() => onBenchChoice(b)}
              aria-pressed={benchChoice === b}
              className={`cursor-pointer px-2.5 py-0.5 font-medium transition-colors ${
                benchChoice === b ? 'bg-accent-600 text-white' : 'text-fg-muted hover:bg-overlay/5'}`}>
              {b}
            </button>
          ))}
        </div>
      </div>

      <div className="overflow-auto rounded-lg border border-neutral-800/40">
        <table className="w-full text-xs">
          <thead className="bg-page">
            <tr className="text-fg-faint text-[11px] uppercase tracking-wide
                           border-b border-neutral-800/40">
              <th className="px-3 py-1.5 font-medium text-left">Line</th>
              {WINDOWS.map((y) => (
                <th key={y} className={th}>{y}y CAGR</th>
              ))}
            </tr>
          </thead>
          <tbody>
            <tr className="[&>td]:border-b [&>td]:border-neutral-800/20">
              <td className="px-3 py-2 text-fg-soft">{portfolioName}</td>
              {WINDOWS.map((y) => <Cell key={y} blend={portfolio} years={y} endPeriod={endPeriod} />)}
            </tr>
            <tr className="[&>td]:border-b [&>td]:border-neutral-800/20">
              <td className="px-3 py-2 text-fg-soft">
                {benchLabel}
                {benchErr && (
                  <InfoTip text={benchErr} className="ml-1.5 cursor-default text-warn-300">⚠</InfoTip>
                )}
              </td>
              {WINDOWS.map((y) => <Cell key={y} blend={benchmark} years={y} endPeriod={endPeriod} />)}
            </tr>
            {/* ⚠ pp, NOT `%` — the difference between two rates is not itself a rate. Writing
                "3.2%" for a gap between 8.4% and 5.2% invites reading it as a relative one, which
                would be 62%. */}
            <tr className="bg-page font-semibold text-fg-strong">
              <td className="px-3 py-2">Excess (pp)</td>
              {WINDOWS.map((y) => {
                if (!portfolio || !benchmark) {
                  return <td key={y} className="px-3 py-2 text-right text-fg-faint">…</td>;
                }
                const e = cagrExcess(lineCagr(portfolio.level, y, endPeriod ?? undefined),
                                     lineCagr(benchmark.level, y, endPeriod ?? undefined));
                return (
                  <td key={y} className="px-3 py-2 text-right font-mono tabular-nums">
                    {e.pp == null
                      ? <InfoTip text={e.reason} className="cursor-default text-fg-faint">—</InfoTip>
                      : <span className={e.pp >= 0 ? 'text-pos-300' : 'text-neg-300'}>
                        {`${e.pp >= 0 ? '+' : ''}${e.pp.toFixed(1)}`}
                      </span>}
                  </td>
                );
              })}
            </tr>
          </tbody>
        </table>
      </div>

      {benchLoading && <p className="text-[11px] text-fg-subtle">Loading {benchChoice} constituents…</p>}

      {/* ⚠⚠ THE CAVEATS THAT DECIDE WHETHER THE NUMBER MEANS ANYTHING, STATED RATHER THAN IMPLIED.
          Each is a thing a reader would otherwise assume and each is false. */}
      {/* ⚠ A `<div>`, NOT A `<p>` — see the identical note on the `Tables` footnote. This prose
          embeds an `InfoTip`, whose card is built from `<div>`s, and the HTML parser closes a
          paragraph at an opening div: the server's markup then disagrees with React's tree and
          hydration fails. Purely a correctness fix; the text renders the same. */}
      <div className="text-[11px] text-fg-faint leading-relaxed">
        Measured on the weighted line, from the latest <strong>reported</strong> period back — never
        from an estimate or from <code>LTM</code>, so the span really is 5 or 10 years (
        <InfoTip text="An estimate endpoint would make this a forecast wearing the clothes of a track record. LTM is real and current, but it ends at the newest quarterly filing, so a span from FY2020 to LTM is five years AND SOME MONTHS — dividing by 5 there overstates the rate and nothing on screen would show the span was not 5.0.">
          why
        </InfoTip>
        ). A dash means the line has no point exactly that many years back — hover it for which.
        Both rows are cap-weighted, and a constituent with no stored cap is not in the line at any
        weight.
      </div>
    </div>
  );
}
