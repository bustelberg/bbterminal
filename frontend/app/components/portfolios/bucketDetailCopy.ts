'use client';

import { useLang, type Lang } from '../../../lib/i18n';

/**
 * The bucket drill-down — the pane that opens under a composition or attribution bar and lists
 * your names in that bucket beside the index's.
 *
 * ⚠ THE TWO LONG COLUMN HINTS ARE NOT HERE. `WEIGHT_HINT` / `WEIGHT_NOW_HINT` are paragraph-length
 * `title=` prose about which weight each column is and what Return and Contribution are built from;
 * they belong with the ⓘ-card body sweep, not with the labels. Same line `quickValuationCopy` and
 * `longEquityCopy` draw: what a reader SCANS is translated first.
 */
export type BucketDetailCopy = {
  axis: { sector: string; region: string; currency: string };
  yourHoldings: string;
  constituents: (benchmark: string) => string;
  inBoth: (n: string) => string;
  weightedAtOpen: string;
  startOfYear: string;
  colName: string;
  colWeight: string;
  colWeightNow: string;
  colReturn: string;
  colContrib: string;
  total: string;
  totalTitle: (n: string) => string;
  inBothTitle: string;
  computing: string;
  noHoldings: string;
  notDecomposed: string;
};

const EN: BucketDetailCopy = {
  axis: { sector: 'Sector', region: 'Region', currency: 'Currency' },
  yourHoldings: 'Your holdings',
  constituents: (benchmark) => `${benchmark} constituents`,
  inBoth: (n) => `${n} in both`,
  weightedAtOpen: 'weighted at window open',
  startOfYear: 'Start of year',
  colName: 'Name',
  colWeight: 'Weight',
  colWeightNow: 'now',
  colReturn: 'Return',
  colContrib: 'Contrib.',
  total: 'Total',
  totalTitle: (n) => `All ${n} names in this bucket`,
  inBothTitle: 'Held in both your portfolio and the benchmark',
  computing: 'Computing attribution…',
  noHoldings: 'No holdings behind this bucket in the YTD window.',
  notDecomposed: 'Funds, cash and unclassified holdings are not a sector bet, so this bucket is '
    + 'not decomposed — just the holdings in it.',
};

/**
 * ⚠ TRANSLATED FROM THE ENGLISH ABOVE, never authored here.
 *
 * ⚠ `Contrib.` BECOMES `Bijdr.` AND STAYS ABBREVIATED. The column is `w-[3.6rem]` under
 * `table-fixed`; the full `Bijdrage` is ~55px at 11px uppercase and would spill over the column
 * beside it, which is the exact failure the `Weight (Start of year)` header note in
 * `BucketDetailPanel` records.
 */
const NL: BucketDetailCopy = {
  axis: { sector: 'Sector', region: 'Regio', currency: 'Valuta' },
  yourHoldings: 'Uw posities',
  constituents: (benchmark) => `Bestanddelen ${benchmark}`,
  inBoth: (n) => `${n} in beide`,
  weightedAtOpen: 'gewogen bij aanvang van de periode',
  startOfYear: 'Begin van het jaar',
  colName: 'Naam',
  colWeight: 'Weging',
  colWeightNow: 'nu',
  colReturn: 'Rendement',
  colContrib: 'Bijdr.',
  total: 'Totaal',
  totalTitle: (n) => `Alle ${n} namen in deze categorie`,
  inBothTitle: 'Aangehouden in zowel uw portefeuille als de benchmark',
  computing: 'Attributie berekenen…',
  noHoldings: 'Geen posities achter deze categorie in de YTD-periode.',
  notDecomposed: 'Fondsen, liquiditeiten en niet-geclassificeerde posities zijn geen sectorkeuze, '
    + 'dus deze categorie wordt niet uitgesplitst; alleen de posities erin worden getoond.',
};

export const BUCKET_DETAIL_COPY: Record<Lang, BucketDetailCopy> = { en: EN, nl: NL };

export function useBucketDetailCopy(): BucketDetailCopy {
  const [lang] = useLang();
  return BUCKET_DETAIL_COPY[lang];
}
