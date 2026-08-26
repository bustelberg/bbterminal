'use client';

import { AspectCard } from '../../../lib/tipCard';
import { workedRatio } from './workedFormula';
import InfoTip from '../InfoTip';
import { type BASIS, type PriceTarget } from './quickValuation';

/**
 * A price target from a demanded yield, and the return it implies.
 *
 *     forecast price = forecast <FCF or EPS>/share ÷ forecast yield
 *     CAGR           = (forecast price / today's price) ^ (1/years) − 1
 *
 * ⚠ THE BASIS IS FCF **OR** EPS, AND EVERY LABEL HERE COMES FROM IT (`basis`, the `BASIS` entry the
 * tab has switched on). Not one string in this panel says "FCF" literally — an earnings yield
 * rendered under an FCF label is not a broken panel, it is a plausible valuation of a company
 * nobody analysed.
 *
 * Both forecasts start from what the chart above already knows — the trend's value two years out,
 * and the company's own average yield over the decade — and both are editable, because the whole
 * point is to try your own.
 *
 * ⚠ "TODAY'S PRICE" IS LITERAL, AND IT IS THE ONE NON-FISCAL FIGURE ON THE PANEL. It is the newest
 * yfinance close, converted into the reporting currency the rows above are filed in. When there is
 * no priced Yahoo listing the fiscal year-end close stands in and the row SAYS `⚠ fiscal` — the
 * whole point of the change was that a year-old price was being printed under the word "current",
 * so an unlabelled fallback would put the bug straight back.
 *
 * ⚠ WHAT EACH BASIS IS *NOT* TRAVELS WITH IT, in `BASIS[...].caveat`, and this panel prints it —
 * an FCF that is not SBC-adjusted (the FCF-SBC cards elsewhere subtract stock compensation and are
 * a different, lower number), an EPS that is accrual rather than cash. Neither caveat is written
 * here, because a caveat hard-coded in the panel is one the switch cannot change.
 */

/**
 * ⚠ THE THREE HELPERS BELOW ARE AT MODULE SCOPE, NOT INSIDE THE COMPONENT. A component created
 * during render is a new type on every render, so React unmounts and remounts it — which drops the
 * caret out of the input after the first keystroke. The lint rule that catches this is protecting a
 * real bug.
 *
 * A dotted leader between label and value, the way a contents page or an invoice runs them — with
 * seven rows of ragged-right labels against right-aligned numbers, the eye otherwise has to guess
 * which value belongs to which line.
 *
 * The filler is a flex-1 span with a dotted bottom border, `aria-hidden` because it is decoration:
 * a screen reader announcing a row of dots between every label and figure is worse than silence.
 */
const fmtCagr = (v: number | null) =>
  (v == null ? '—' : `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`);

function Leader() {
  return (
    <span aria-hidden
      className="flex-1 min-w-[1rem] border-b border-dotted border-neutral-700/70 translate-y-[-0.2rem]" />
  );
}

function Row({ label, info, children }: {
  label: string; info?: React.ReactNode; children: React.ReactNode;
}) {
  return (
    <div className="flex items-baseline gap-2">
      <span className="flex items-center gap-1 text-[12px] text-fg-muted shrink-0">
        {label}{info}
      </span>
      <Leader />
      <span className="font-mono text-xs text-fg-soft whitespace-nowrap shrink-0">{children}</span>
    </div>
  );
}

function Input({ value, onChange, suffix, onRevert, revertTitle, disabled, disabledTitle, step }: {
  value: string; onChange: (v: string) => void; suffix?: string;
  /**
   * Put this field back to the figure the panel computed — see the ⚠⚠ below. Absent means the
   * field has no default to return to.
   */
  onRevert?: () => void;
  revertTitle?: string;
  /** ⚠ THE INPUT THAT CANNOT PRODUCE AN ANSWER IS CLOSED, NOT LEFT OPEN TO BE IGNORED. A growth
   *  rate typed against a negative base yields nothing, and a box that silently changes no figure
   *  on the panel reads as a broken control rather than as an inapplicable one. */
  disabled?: boolean;
  disabledTitle?: string;
  /** The arrow-key increment. A rate steps in whole points; a currency figure in units. */
  step?: number;
}) {
  return (
    <span className="flex items-center gap-1 justify-end">
      {/* ⚠⚠ IT REVERTS TO `null`, NOT TO THE DEFAULT'S CURRENT VALUE, AND THE DIFFERENCE OUTLIVES
          THE CLICK. `null` means "never typed", which is what makes the box keep TRACKING the
          computed figure (see `fcfStr`'s own note): the default moves when the live price lands,
          when the basis switches, when a refetch changes the fit. Writing the number in would put
          the field back to the right value and then freeze it there — a box that agrees with the
          panel now and silently disagrees with it a moment later, which is worse than the edit it
          undid.

          ⚠ IT SITS LEFT OF THE INPUT so the box does not move when it appears. A control that
          shifts the thing it belongs to, at the moment you start typing in it, is one you have to
          chase with the pointer. */}
      {onRevert && !disabled && (
        <button type="button" onClick={onRevert} title={revertTitle}
          aria-label="Reset to the computed figure"
          className="cursor-pointer text-[11px] leading-none px-1 rounded text-fg-faint
                     hover:text-accent-400 hover:bg-overlay/5 transition-colors">
          ↺
        </button>
      )}
      <input type="number" value={value} step={step} disabled={disabled}
        title={disabled ? disabledTitle : undefined}
        onChange={(e) => onChange(e.target.value)}
        className="w-20 bg-page border border-neutral-700 rounded px-1.5 py-0.5 text-xs font-mono text-fg-strong text-right focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 disabled:opacity-50 disabled:cursor-not-allowed" />
      {suffix && <span className="text-[11px] text-fg-muted">{suffix}</span>}
    </span>
  );
}

/** Where the "current" price came from — it decides what the panel is allowed to claim. */
export type PriceProvenance = {
  /** The close's OWN date: the yfinance bar's, or the fiscal year end. Never "today". */
  date: string | null;
  /** True = the newest yfinance close in the reporting currency. False = the fiscal year-end
   *  close, i.e. the old behaviour, which the panel must then admit to. */
  live: boolean;
  /** We have not asked yet. ⚠ NOT the same as `live: false` — a fallback badge shown for the
   *  length of one request is a provenance claim we have not established. */
  pending?: boolean;
  symbol?: string | null;
  staleDays?: number | null;
};

/** A live close older than this is flagged in the row. A weekend plus a holiday is four days; past
 *  a week the listing is not trading, or we have stopped fetching it, and either way the reader
 *  should not have to open a tooltip to find out. */
const STALE_WARN_DAYS = 7;

const fmtDate = (iso: string | null) => {
  if (!iso) return 'unknown date';
  const d = new Date(`${iso}T00:00:00Z`);
  return Number.isNaN(d.getTime()) ? iso
    : d.toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' });
};

export default function PriceTargetCalculator({
  target, years, currency, className = '',
  horizonYears, targetYear, price, basis: b,
  fcfStr, onFcf, defaultForecastFcfPs,
  cagrStr, onCagr, onResetCagr, shownCagrPct, defaultCagrPct, cagrDisabled,
  yieldStr, onYield, defaultForecastYield, onReset, onResetFcf,
}: {
  /** ⚠ COMPUTED BY THE PARENT, because the chart draws the price line out to the same target. One
   *  computation, two readers — see the priceTarget() helper. */
  target: PriceTarget;
  /** The FCF forecast's horizon past the last REPORTED year — a label, not the CAGR's divisor. */
  years: number;
  /** What the CAGR is actually annualised over: from `price.date` to the target. Between one and
   *  two years off a live price, exactly `years` off the fiscal one. */
  horizonYears: number;
  /** The fiscal year the target lands in, so the CAGR can name its endpoint instead of quoting a
   *  horizon like "1.4 years" that means nothing on the chart beside it. */
  targetYear: number | null;
  price: PriceProvenance;
  /** The switched-on `BASIS` entry — every label and info card on this panel reads from it. */
  basis: (typeof BASIS)[keyof typeof BASIS];
  currency?: string | null;
  /** Grid placement from the parent — the component owns its look, the layout owns its slot. */
  className?: string;
  /** null = never typed, so the box shows the default and keeps tracking it. */
  fcfStr: string | null;
  onFcf: (v: string) => void;
  defaultForecastFcfPs: number | null;
  /**
   * The GROWTH RATE the forecast per-share figure is reached by — the same assumption as `fcfStr`,
   * entered the way people actually hold it. `null` = not the live one, so the box shows
   * `shownCagrPct` (the rate the current end value implies) and keeps tracking it.
   *
   * ⚠⚠ THE TWO ARE ONE ASSUMPTION AND ONLY ONE IS AUTHORITATIVE. The parent's setters clear each
   * other, so typing in either box makes it the authority and hands the other its derived role —
   * which is why this component never has to decide which of two live values to believe.
   */
  cagrStr: string | null;
  onCagr: (v: string) => void;
  /** ⚠ BACK TO `null`, NOT TO THE DEFAULT'S CURRENT VALUE — same rule as `onResetFcf`. Null is
   *  what keeps the box TRACKING the rate the forecast implies as that figure moves. */
  onResetCagr: () => void;
  /** The rate the CURRENT forecast per-share figure implies, when the rate box is not the live
   *  one. `null` while it is (the box then shows the user's own text, unrounded). */
  shownCagrPct: number | null;
  /** The fitted trend's own annual growth — what ↺ returns to. */
  defaultCagrPct: number | null;
  /** No positive base to compound from, so no rate exists — see `compoundFrom`. The box is
   *  disabled and says why rather than accepting a number that can produce nothing. */
  cagrDisabled?: boolean;
  yieldStr: string | null;
  onYield: (v: string) => void;
  defaultForecastYield: number | null;
  /** Clear BOTH typed fields — the card-level control in the header. */
  onReset: () => void;
  /**
   * Clear the forecast-per-share field alone.
   *
   * ⚠ SEPARATE FROM `onReset`, BECAUSE THE TWO EDITS ARE INDEPENDENT ASSUMPTIONS. The header's
   * reset throws away the yield you chose along with the per-share figure you were correcting; on
   * a card whose whole subject is "change one input and watch the target move", that is a control
   * you learn not to press.
   */
  onResetFcf: () => void;
}) {
  const dirty = fcfStr != null || cagrStr != null || yieldStr != null;
  const show = (s: string | null, def: number | null, dp: number) =>
    (s != null ? s : def == null ? '' : def.toFixed(dp));

  const n2 = (v: number | null) => (v == null ? '—' : v.toFixed(2));
  const n1 = (v: number | null) => (v == null ? '—' : v.toFixed(1));
  const ccy = currency ? currency + ' ' : '';

  return (
    // ⚠ A FLEX COLUMN, BECAUSE THIS CARD IS SHORTER THAN THE CHART BESIDE IT. The 2×2 grid sizes
    // every cell to the tallest, so seven text rows in a cell built for a 320px plot leave a gap.
    // The rows stay at the top, the CAGR — the conclusion — is pushed to the bottom edge by its
    // own `mt-auto`, and the space lands between them where it reads as layout rather than as a
    // card that failed to finish rendering.
    <div className={`rounded-xl border border-neutral-800/40 bg-card p-4 space-y-1.5 min-w-0 flex flex-col ${className}`}>
      <div className="flex items-center gap-2 pb-1">
        <h4 className="text-base font-semibold text-fg-strong">Price target</h4>
        {dirty && (
          <button type="button" onClick={onReset}
            className="ml-auto text-[11px] text-accent-400 hover:underline">reset</button>
        )}
      </div>

      <Row label={`Current ${b.perShare}`}
        info={<InfoTip content={<AspectCard
          what={`The latest reported figure — ${b.what}.`}
          where={`${b.source} The same series the chart to the left plots.`}
          when="The most recent fiscal year, so up to a year old."
          how={b.caveat} />} />}>
        {ccy}{n2(target.currentPs)}
      </Row>
      {/* ⚠⚠ ABOVE THE FIGURE IT PRODUCES, BECAUSE THAT IS THE ORDER THE ASSUMPTION IS MADE IN.
          "It compounds at 12%" comes first and "so it reaches 41.20" follows; printed the other
          way round the rate reads as a statistic ABOUT the forecast rather than as the lever that
          sets it, which is the whole reason this row exists. Both are editable and each derives
          the other live — see `cagrStr` in `QuickValuationTab`. */}
      <Row label={`Forecast ${b.perShare} CAGR`}
        info={<InfoTip content={<AspectCard
          what={`The annual rate you expect ${b.perShare} to compound at.`}
          where={`Defaults to the fitted trend's OWN slope — the dotted projection on the chart is `
            + 'a straight line on a log axis, which IS a constant growth rate, so the default '
            + 'rate and the default forecast below are the same assumption stated two ways.'}
          when={`Compounded over ${years} years from the trend's value at the last reported year.`}
          how={'⚠ TYPE YOUR OWN AND EVERYTHING BELOW MOVES — the forecast figure, the forecast '
            + 'price and the target on the chart. Editing the figure below instead makes THAT the '
            + 'assumption and this box goes back to reporting the rate it implies. ⚠ It compounds '
            + `the FUNDAMENTAL over the fiscal horizon (${years} years past the last reported `
            + 'year); the CAGR at the foot of this panel is a different number — the PRICE return, '
            + "annualised from today's close."} />} />}>
        <Input value={cagrStr ?? (shownCagrPct == null ? '' : shownCagrPct.toFixed(1))}
          onChange={onCagr} suffix="%" step={1}
          disabled={cagrDisabled}
          disabledTitle={`No growth rate exists here: ${b.perShare} is not positive at the start `
            + 'of the window, and a negative figure does not compound to anything. Type a forecast '
            + 'figure below instead.'}
          onRevert={cagrStr == null ? undefined : onResetCagr}
          revertTitle={defaultCagrPct == null
            ? 'Clear your rate.'
            : `Back to ${defaultCagrPct.toFixed(1)}% — the fitted trend's own annual growth. The `
              + 'box then keeps tracking whatever rate the forecast below implies, which typing '
              + 'the number in would not.'} />
      </Row>
      <Row label={`Forecast ${b.perShare}`}
        info={<InfoTip content={<AspectCard
          what={`What the fitted trend says ${b.perShare} will be.`}
          where="The dotted projection on the chart to the left, converted from the index back into currency."
          when={`${years} years past the last reported one.`}
          how="⚠ An extrapolation, not a forecast anyone made — it continues the exponential through the last decade. Type your own over it, and ↺ puts this one back." />} />}>
        {/* ⚠⚠ "RESET TO THE OFFICIAL FORECAST" IS NOT AVAILABLE HERE AND THE BUTTON MUST NOT IMPLY
            IT IS. There is no analyst FCF forecast to return to — `BASIS.fcf.estimateCodes` is
            `null` because no analyst publishes one, which is a fact about the vendor rather than a
            gap in our ingest. What this restores is OUR fitted trend, and the tooltip says exactly
            that; a button labelled for a consensus that does not exist would be the more confident
            of the two wrong answers. (The EPS basis does have a consensus, but this field's default
            is the trend on both bases — see `forecastPs` in `QuickValuationTab`.) */}
        <Input value={show(fcfStr, defaultForecastFcfPs, 2)} onChange={onFcf}
          onRevert={fcfStr == null ? undefined : onResetFcf}
          revertTitle={defaultForecastFcfPs == null
            ? 'Clear your figure. There is no computed forecast to fall back to for this company.'
            : `Back to ${defaultForecastFcfPs.toFixed(2)} — the panel's own fitted trend, `
              + `${years} years past the last reported year. ⚠ Not an analyst forecast: nobody `
              + 'publishes one for free cash flow. The box then keeps tracking that figure as it '
              + 'moves, which typing the number in would not.'} />
      </Row>
      <Row label={`Current ${b.yieldInline}`}
        info={<InfoTip content={<AspectCard
          what={price.live
            ? "What the shares yield on this measure at today's price."
            : 'What the shares yielded on this measure at the last fiscal year-end price.'}
          where={`Current ${b.perShare} ÷ current share price — the two rows above and below.`}
          when={price.live
            ? `Last filed year's figure over the close of ${fmtDate(price.date)}. ⚠ Two dates, deliberately: that is what a current yield is.`
            : 'The latest fiscal year — both sides.'}
          worked={workedRatio(target.currentPs, target.currentPrice,
            target.currentYield == null ? '' : `${n1(target.currentYield)}%`, '', ` ${ccy}`)}
          how={`The starting point the forecast yield is judged against: the gap between the two IS the rerating this calculator assumes.${
            price.live ? ' ⚠ It will not match the “Latest” yield on the chart to the left, which is fiscal on both sides.' : ''}`} />} />}>
        {n1(target.currentYield)}%
      </Row>
      <Row label={`Forecast ${b.yieldInline}`}
        info={<InfoTip content={<AspectCard
          what="The yield you expect the market to price the shares at."
          where={`Defaults to this company's OWN average ${b.yieldInline} over the charted decade — the dashed line on the yield chart.`}
          when="At the end of the forecast window."
          how={`⚠ THE ASSUMPTION THAT DRIVES EVERYTHING BELOW. The forecast price is simply the forecast ${b.perShare} divided by this, so a percentage point here moves the target more than any other input.`} />} />}>
        <Input value={show(yieldStr, defaultForecastYield, 1)} onChange={onYield} suffix="%" />
      </Row>
      {/* ⚠ THE ONE ROW THAT IS NOT FISCAL. Everything above it comes from the last filed year;
          this is the market's price for the shares now, and the yield and CAGR are measured from
          it. The two provenances therefore cannot share one info card — a live figure described
          as a fiscal close, or the reverse, is worse than either being wrong. */}
      <Row label="Current share price"
        info={<InfoTip content={price.pending ? <AspectCard
          what="The price the return is measured from."
          where="Fetching today's yfinance close…"
          when="Until it lands, this is the fiscal year-end close."
          how="If the ISIN has no priced Yahoo listing it will stay that way, and the row will say so." />
          : price.live ? <AspectCard
          what="The price the return is measured from — today's."
          where={`yfinance (\`asset_price\`)${price.symbol ? ` — ${price.symbol}` : ''}, the same price series /portfolios values every model with, converted into ${currency ?? 'the reporting currency'} so it divides into the per-share figure above.`}
          when={`Its close of ${fmtDate(price.date)}${price.staleDays != null && price.staleDays > STALE_WARN_DAYS ? ` — ⚠ ${price.staleDays} days ago. A listing that has not traded in that long, or one we have stopped fetching.` : '.'}`}
          how={`⚠ THE ONLY LIVE NUMBER HERE. The ${b.perShare} above it is the last FILED year, so the yield is this year's price against last year's figure — which is what a current yield is, and why the two rows carry different dates.`} />
          : <AspectCard
            what="The price the return is measured from."
            where="GuruFocus `Month End Stock Price` — the close at the last fiscal year end."
            when="⚠ NOT TODAY'S QUOTE. It can be up to a year old, and the CAGR below is measured from it."
            how="⚠ THE FALLBACK. This ISIN has no priced Yahoo listing (or none we could convert into the reporting currency), so the live close could not be used and the fiscal one stands in." />} />}>
        {ccy}{n2(target.currentPrice)}
        {/* Provenance in the row, not only in the tooltip: a stale price that reads as live is the
            failure this row exists to fix, and nobody opens a tooltip to check a number that
            looks fine. */}
        {price.pending ? (
          <span className="ml-1 text-[11px] text-fg-faint">…</span>
        ) : !price.live ? (
          <span className="ml-1 text-[11px] text-warn-300" title="No live close available — this is the fiscal year-end price.">⚠ fiscal</span>
        ) : price.staleDays != null && price.staleDays > STALE_WARN_DAYS ? (
          <span className="ml-1 text-[11px] text-warn-300" title={`Last close ${fmtDate(price.date)} — ${price.staleDays} days ago.`}>⚠ {price.staleDays}d old</span>
        ) : null}
      </Row>
      <Row label="Forecast share price"
        info={<InfoTip content={<AspectCard
          what={`What the shares are worth if the forecast ${b.perShare} is priced at the forecast ${b.yieldInline}.`}
          where={`Forecast ${b.perShare} ÷ forecast ${b.yieldInline}.`}
          when={`${years} years out.`}
          // ⚠⚠ THE SAME NUMBER IS WORKED ON THE `Price target FY20xx` TILE IN `QuickValuationTab`
          // AND WAS SYMBOLS HERE — one figure explained two ways, one click apart, which reads as
          // two different computations rather than one shown twice.
          worked={workedRatio(target.forecastPs, target.forecastYield,
            target.forecastPrice == null ? '' : `${ccy}${n2(target.forecastPrice)}`, '', '%')}
          how="Blank when either input is non-positive: a zero yield divides to infinity, and a forecast that loses money has no price at a positive one." />} />}>
        {ccy}{n2(target.forecastPrice)}
      </Row>

      <div className="flex items-baseline gap-2 pt-1.5 mt-auto border-t border-neutral-800/40">
        <span className="flex items-center gap-1 text-[12px] text-fg-muted shrink-0">
          {/* Named by its ENDPOINT, not its length: off a live price the horizon is a fraction
              ("Est. 1.4-year CAGR" reads like a typo), and the fiscal year is the thing the chart
              beside it actually plots the target at. */}
          Est. CAGR{targetYear != null ? ` to FY${targetYear}` : ''}
          <InfoTip content={<AspectCard
            what={price.live
              ? "The annualised return from today's price to the forecast one."
              : 'The annualised return from the last fiscal year-end price to the forecast one.'}
            where="(forecast price ÷ current price) ^ (1/years) − 1."
            when={`${horizonYears.toFixed(1)} years — from the ${price.live ? 'close' : 'fiscal close'} of ${fmtDate(price.date)} to ${
              targetYear != null ? `the FY${targetYear} year end` : 'the forecast year'}. ${
              price.live
                ? '⚠ Less than the ' + years + '-year projection above it: the forecast sits ' + years + ' years past the last REPORTED year, and today is already part of the way there.'
                : `The full ${years}-year projection, because the price is the fiscal one.`}`}
            /* ⚠ ALL FOUR OPERANDS ARE IN SCOPE HERE and always were; the card simply never
               used them. `horizonYears` is the one that matters most — it is a fraction off a
               live price, and the whole reason the tile is named by its endpoint rather than its
               length, so seeing it in the exponent is what makes the number self-explaining. */
            worked={target.forecastPrice != null && target.currentPrice != null
              && target.currentPrice > 0 && target.cagr != null
              ? `(${n2(target.forecastPrice)} ÷ ${n2(target.currentPrice)})`
                + ` ^ (1 ÷ ${horizonYears.toFixed(1)}) − 1`
                + ` = ${target.cagr >= 0 ? '+' : ''}${(target.cagr * 100).toFixed(1)}%`
              : ''}
            how="⚠ Price only — no dividends, and no return on the cash the business throws off in the meantime. It answers what the multiple and the cash flow do to the share price, not what you would earn holding it." />} />
        </span>
        <Leader />
        {/* The one figure here that is a conclusion rather than an input, so it carries the sign's
            colour — a target below today's price is the finding, not a formatting accident. */}
        <span className={`font-mono text-base font-semibold shrink-0 ${
          target.cagr == null ? 'text-fg-muted' : target.cagr >= 0 ? 'text-pos-500' : 'text-neg-500'}`}>
          {fmtCagr(target.cagr)}
        </span>
      </div>
    </div>
  );
}
