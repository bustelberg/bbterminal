/**
 * The filename a scheduled strategy's holdings export gets.
 *
 * `MomentumTopSelectie Neutraal September` — the strategy's own name, then the month the
 * portfolio is FOR.
 *
 * ⚠⚠ THE MONTH COMES FROM THE SNAPSHOT'S `as_of_date`, NEVER FROM `today`. `as_of_date` is
 * the GRID date the period is anchored to — the rebalance date the picks were decided for
 * (see `momentum/schedule.compute_next_due_at`, which reads exactly this column as its
 * reference). Those two disagree for several days every month by DESIGN: the rebalance for
 * the first Monday of September is decided on the preceding Friday's close and the tick
 * fires from the Saturday, so a file downloaded on 5 September is the SEPTEMBER portfolio.
 * Stamping it "August" would misfile the one export somebody keeps.
 *
 * ⚠ ENGLISH MONTH NAMES, matching every other date this app renders (`LongEquityUniverse`,
 * `FrozenUniversesPanel`, `AirsPortfolioUpload` all format `en-GB`/`en-US`). The strategy
 * NAME is whatever the user typed and is passed through untouched, Dutch or otherwise.
 *
 * ⚠ NO YEAR, as specified. Two Septembers a year apart therefore collide in a downloads
 * folder — the browser will suffix "(1)", which is a worse label than a year would be. Say
 * so before changing it: the name was asked for in exactly this shape.
 */

const MONTHS = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
];

/**
 * `(strategyName, asOfDate)` → the export basename, WITHOUT extension or date stamp.
 *
 * ⚠ PARSED OFF THE STRING, NOT THROUGH `new Date()`. `as_of_date` is a plain `YYYY-MM-DD`
 * calendar date with no time and no zone; `new Date('2026-09-01')` is parsed as UTC
 * MIDNIGHT and then read back in the viewer's local zone, so anyone west of Greenwich gets
 * the 31st of August — the month boundary is precisely where a scheduled strategy's file
 * lands, so this would be wrong on the first of every month for half the world.
 *
 * Falls back to the name alone when the date is missing or unparseable, and to
 * `current-portfolio` when there is no name either — a download must never fail over its
 * own label.
 */
export function holdingsExportName(
  strategyName: string | null | undefined,
  asOfDate: string | null | undefined,
): string {
  const name = (strategyName ?? '').trim();
  const m = /^(\d{4})-(\d{2})-\d{2}/.exec(String(asOfDate ?? ''));
  const month = m ? MONTHS[Number(m[2]) - 1] : undefined;
  if (!name) return month ? `current-portfolio ${month}` : 'current-portfolio';
  return month ? `${name} ${month}` : name;
}
