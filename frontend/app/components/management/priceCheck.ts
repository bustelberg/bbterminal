import type { AirsAccountIsins } from '../../../lib/types/api';

// Explicit dashboard comparison exclusions; do not change the underlying instrument mapping.
const PRICE_CHECK_EXCLUDED_ISINS = new Set(['CH1369849273']); // High Income Quality fund

// Yahoo labels London closes as GBp (pence), while AIRS reports the same native price in GBP.
// A quote unit is not a different currency: normalize both before comparing or displaying a range.
const QUOTE_UNIT_NORMALIZATION: Record<string, { currency: string; divisor: number }> = {
  GBp: { currency: 'GBP', divisor: 100 },
  GBX: { currency: 'GBP', divisor: 100 },
};

function normalizeQuoteUnit(value: number | null, currency: string | null) {
  const unit = currency ? QUOTE_UNIT_NORMALIZATION[currency] : undefined;
  return {
    value: value == null || !unit ? value : value / unit.divisor,
    currency: unit?.currency ?? currency,
    divisor: unit?.divisor ?? 1,
  };
}

export type PriceStatus = 'matched' | 'warning' | 'difference' | 'stale' | 'linked' | 'unavailable';
export type RangeAssessment = 'within' | 'outside' | 'insufficient' | 'notComparable' | 'staleRange';
export type PriceItem = {
  key: string; name: string; isin: string | null; instrument: string | null;
  yahooSymbol: string | null;
  certificate: string | null;
  airs: number | null; yahoo: number | null; difference: number | null;
  airsCurrency: string | null; yahooCurrency: string | null;
  recentRange: { low: number; high: number; start: string; end: string; count: number } | null;
  rangeAssessment: RangeAssessment;
  airsDate: string | null; yahooDate: string | null; status: PriceStatus;
};
export type PriceSummary = {
  items: PriceItem[]; checked: number; total: number; differences: number; warnings: number;
  stale: number; unavailable: number; linked: number;
};

export function summarizePrices(data: AirsAccountIsins): PriceSummary {
  // This is an exception list for listed equities only. Bonds and other instruments need their
  // own quote-unit-aware checks; comparing their clean price to an issuer's share price is noise.
  const items = (data.rows ?? []).filter((row) => row.bucket === 'Equity'
    && !PRICE_CHECK_EXCLUDED_ISINS.has(row.isin?.trim().toUpperCase() ?? '')).map((row): PriceItem => {
    const rawAirsCurrency = row.currency?.trim() || null;
    const rawYahooCurrency = row.our_price_currency?.trim() || null;
    const normalizedAirs = normalizeQuoteUnit(row.airs_price_local ?? null, rawAirsCurrency);
    const normalizedYahoo = normalizeQuoteUnit(row.our_price_local ?? null, rawYahooCurrency);
    const airs = normalizedAirs.value;
    const yahoo = normalizedYahoo.value;
    const airsCurrency = normalizedAirs.currency;
    const yahooCurrency = normalizedYahoo.currency;
    const airsDate = row.airs_price_date?.slice(0, 10) ?? data.as_of?.slice(0, 10) ?? null;
    const yahooDate = row.our_price_date?.slice(0, 10) ?? null;
    const valid = !!airsCurrency && airsCurrency === yahooCurrency
      && typeof airs === 'number' && Number.isFinite(airs) && airs > 0
      && typeof yahoo === 'number' && Number.isFinite(yahoo) && yahoo > 0;
    const difference = valid ? (airs / yahoo - 1) * 100 : null;
    const rawRange = row.recent_close_range;
    const recentRange = rawRange && typeof rawRange.low === 'number' && Number.isFinite(rawRange.low)
      && typeof rawRange.high === 'number' && Number.isFinite(rawRange.high) && rawRange.low > 0 && rawRange.high >= rawRange.low
      && typeof rawRange.start === 'string' && typeof rawRange.end === 'string' && typeof rawRange.count === 'number'
      ? { low: rawRange.low / normalizedYahoo.divisor, high: rawRange.high / normalizedYahoo.divisor,
        start: rawRange.start, end: rawRange.end, count: rawRange.count } : null;
    const rangeAge = recentRange && airsDate ? (Date.parse(airsDate) - Date.parse(recentRange.end)) / 86400000 : NaN;
    // A missing five-session range is distinct from a linked listing or an incompatible currency.
    // Calling all of those “not enough history” hid the real reason when five closes were plainly
    // displayed in the adjacent column.
    const rangeAssessment: RangeAssessment = !valid || row.served_by || row.verdict === 'cross_listed'
      ? 'notComparable' : !recentRange || recentRange.count !== 5
        ? 'insufficient' : !(rangeAge > 0 && rangeAge <= 7)
          ? 'staleRange' : airs >= recentRange.low - 0.005 && airs <= recentRange.high + 0.005 ? 'within' : 'outside';
    let status: PriceStatus;
    if (row.served_by || row.verdict === 'cross_listed') status = 'linked';
    else if (!valid || !airsDate || !yahooDate) status = 'unavailable';
    else if (airsDate !== yahooDate || row.verdict === 'stale_price') status = 'stale';
    else if (Math.abs(difference!) >= 5 - 1e-9) status = 'difference';
    else if (Math.abs(difference!) > 2 + 1e-9) status = 'warning';
    else status = 'matched';
    return {
      key: JSON.stringify([row.isin ?? '', row.holding_name]), name: row.holding_name,
      isin: row.isin ?? null, instrument: row.our_instrument ?? null, yahooSymbol: row.yahoo_symbol ?? null,
      certificate: row.certificate_holding ?? null,
      airs, yahoo, difference, airsCurrency, yahooCurrency, airsDate, yahooDate, status, recentRange, rangeAssessment,
    };
  });
  const count = (status: PriceStatus) => items.filter((row) => row.status === status).length;
  return { items, total: items.length, checked: count('matched') + count('warning') + count('difference'),
    differences: count('difference'), warnings: count('warning'), stale: count('stale'),
    unavailable: count('unavailable'), linked: count('linked') };
}

/** A difference requires comparable quote dates; stale gaps need a refresh first. */
export const isExtremeDifference = (row: PriceItem) =>
  row.status === 'difference' && row.difference != null
  && Math.abs(row.difference) >= 5 - 1e-9;

export const isStalePriceGap = (row: PriceItem) =>
  row.status === 'stale' && row.difference != null
  && Math.abs(row.difference) >= 5 - 1e-9;

export const needsPriceReview = (summary?: PriceSummary | null) => !!summary &&
  summary.items.some(isExtremeDifference);

/** Alert only when a holding newly crosses the extreme-gap threshold. */
export function newPriceDifferences(before: PriceSummary | null | undefined, after: PriceSummary): number {
  const previous = new Set(before?.items.filter(isExtremeDifference).map((row) => row.key) ?? []);
  return after.items.filter((row) => isExtremeDifference(row) && !previous.has(row.key)).length;
}
