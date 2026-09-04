'use client';

/**
 * ONE CARD HEADING ON THE `Graphs` TAB — the title, and the ⓘ that says what the chart is.
 *
 * ⚠⚠ IT EXISTS SO THERE IS ONE HEADING, NOT TWELVE (2026-09-03, on request: "each graph should
 * have an info icon explaining what it is that we are viewing, and caveats to keep in mind").
 * Every ratio card wrote its own `<h4 className="text-base font-semibold text-fg-strong">`, twelve
 * copies of one line — so adding a tip to each meant twelve chances to space it differently, and
 * the next card added to the tab would have started from whichever neighbour got copied.
 *
 * ⚠ THE CARD KEEPS ITS OWN LAYOUT. Two cards put a control on the title row (`DailyToggle`) and
 * one truncates inside a `flex-nowrap` header, so this renders the HEADING and nothing around it —
 * it is not a header bar. `className` is how those cards pass the `truncate min-w-0` their own row
 * needs; without it this would have to grow a prop per caller's layout.
 *
 * ⚠ THE TIP IS `AspectCard`, NOT `InfoTip text=`. A string handed to `text=` renders as prose, so
 * the what / where / how would run together into one paragraph — the failure the Tables tab
 * already paid for once, where a typeset builder passed to `text=` printed its own LaTeX source.
 *
 * ⚠ THE COPY IS NOT HERE. It lives in `longEquityCopy`, beside the titles, because it is
 * translated and because a heading and its explanation drifting apart is exactly what a shared
 * lookup prevents — see the notes on `CHART_INFO`.
 */
import { AspectCard } from '../../../lib/tipCard';
import { useLang } from '../../../lib/i18n';
import InfoTip from '../InfoTip';
import { chartInfo, chartTitle, type ChartKey } from './longEquityCopy';

export default function CardHeading({ chartKey, sbc = false, className = '' }: {
  /** Which card this is. Also the key its heading and its ⓘ are both looked up by. */
  chartKey: ChartKey;
  /** The SBC-correction checkbox. Three headings and three tips change with it. */
  sbc?: boolean;
  /** Layout the CALLER's header row needs — `truncate min-w-0` where the title shares its line. */
  className?: string;
}) {
  // ⚠ A TUPLE — `useLang` is an external store, and the setter is its second element.
  const [lang] = useLang();
  const info = chartInfo(lang, chartKey, sbc);
  return (
    <h4 className={`text-base font-semibold text-fg-strong ${className}`}>
      {chartTitle(lang, chartKey, sbc)}
      <InfoTip className="ml-1" content={<AspectCard
        what={info.what} where={info.where} how={info.how} />} />
    </h4>
  );
}
