import type { DrawdownPeriod } from '../../../lib/stores/momentum';

// Palette for series lines (index 0 = active strategy).
export const SERIES_COLORS = [
  '#818cf8', // indigo
  '#f59e0b', // amber
  '#34d399', // emerald
  '#f472b6', // pink
  '#60a5fa', // sky
  '#a78bfa', // violet
  '#fb7185', // rose
  '#22d3ee', // cyan
];

export const fmtPct = (v: number | null) =>
  v != null ? `${v >= 0 ? '+' : ''}${v.toFixed(2)}%` : '—';

/** Annualized return derived from a compound return + duration in months:
 * (1 + compound_return/100)^(12/months) − 1, expressed as %. Returns null
 * when not computable (non-positive factor, zero months). The math
 * extrapolates short windows to a full year — useful for comparing
 * different-duration runs on the same scale, but treat sub-12-month
 * CAGRs with caution since they project a short observation forward. */
export function annualize(compoundReturnPct: number | null, months: number): number | null {
  if (compoundReturnPct == null || months <= 0) return null;
  const factor = 1 + compoundReturnPct / 100;
  if (factor <= 0) return null;
  return (Math.pow(factor, 12 / months) - 1) * 100;
}

export const fmtPrice = (v: number | null | undefined) => {
  if (v == null) return '—';
  const d = Math.abs(v) >= 1000 ? 0 : Math.abs(v) >= 10 ? 2 : 4;
  return v.toFixed(d);
};

// US-listed exchanges per GuruFocus's URL convention: `/stock/TICKER/summary`
// without a prefix. Non-US (XTER, HKSE, LSE, TSE, …) need `EXCHANGE:TICKER`.
// Aligned with `backend/ingest/prices.py::US_EXCHANGES` so the click-through
// link and the actual API request resolve to the same security. `US` is
// kept as a catch-all some loaders use for OTC/pink sheets.
const GURUFOCUS_US_EXCHANGES = new Set(['NYSE', 'NASDAQ', 'AMEX', 'CBOE', 'US']);

// Strings that show up in the frontend payload when an exchange link is
// missing on the backend side (pandas/JSON sometimes serializes None/NaN
// as the literal "None"/"nan", and saved variant bundles from before the
// snapshot-normalization fix still carry them). Treat them as no-exchange
// so we don't synthesize broken `None:TICKER`-style URLs.
const EMPTY_EXCHANGE_TOKENS = new Set(['', 'NONE', 'NAN', 'NULL', 'UNDEFINED']);

/** Heuristic: when the exchange link is genuinely missing (saved-bundle
 * universes from before the snapshot-normalization fix carry empty or
 * "None" strings), guess the exchange from the ticker's shape so the
 * GuruFocus URL still resolves to the right security. Returns '' when no
 * confident guess is possible — caller should fall back to the bare
 * ticker.
 *  - 4-5 digit numeric → HKSE (Hong Kong: `01988` Bank of China,
 *    `00700` Tencent, `03690` Meituan, etc.).
 * Other patterns (LSE `.L` suffixes, SHSE/SZSE 6-digit numerics, …) are
 * left to expand later — most real-world breakage comes from HKSE.
 */
function inferExchangeFromTicker(ticker: string): string {
  if (/^\d{4,5}$/.test(ticker)) return 'HKSE';
  return '';
}

export function guruFocusUrl(ticker: string, exchange: string): string {
  const t = ticker.toUpperCase();
  let e = (exchange ?? '').toUpperCase();
  if (GURUFOCUS_US_EXCHANGES.has(e)) {
    return `https://www.gurufocus.com/stock/${t}/summary`;
  }
  if (EMPTY_EXCHANGE_TOKENS.has(e)) {
    e = inferExchangeFromTicker(t);
    if (!e) {
      // No exchange and no confident guess — bare ticker is the safest
      // bet (works for US-listed names).
      return `https://www.gurufocus.com/stock/${t}/summary`;
    }
  }
  return `https://www.gurufocus.com/stock/${e}:${t}/summary`;
}

/** Returns the exchange label fit for display (e.g. inside `(NYSE)` parens
 * after a ticker, or as the value in an Exchange column). When the raw
 * exchange is missing/junk, falls through to the same ticker-shape
 * inference `guruFocusUrl` uses, so the column matches the link's
 * resolution. Pass the ticker when you want that fallback; omit for a
 * strict "what was on the row" reading. */
export function displayExchange(exchange: string | null | undefined, ticker?: string): string {
  const e = (exchange ?? '').trim();
  if (e && !EMPTY_EXCHANGE_TOKENS.has(e.toUpperCase())) return e;
  if (ticker) {
    const inferred = inferExchangeFromTicker(ticker.toUpperCase());
    if (inferred) return inferred;
  }
  return '';
}

export const EXCHANGE_NAMES: Record<string, string> = {
  NYSE: 'New York Stock Exchange',
  NAS: 'NASDAQ',
  NASDAQ: 'NASDAQ',
  AMEX: 'NYSE American',
  OTCPK: 'OTC Markets Pink',
  OTCBB: 'OTC Bulletin Board',
  LSE: 'London Stock Exchange',
  XETR: 'Xetra (Deutsche Börse)',
  XETRA: 'Xetra (Deutsche Börse)',
  FRA: 'Frankfurt Stock Exchange',
  GER: 'Deutsche Börse',
  EPA: 'Euronext Paris',
  XPAR: 'Euronext Paris',
  AMS: 'Euronext Amsterdam',
  XAMS: 'Euronext Amsterdam',
  BRU: 'Euronext Brussels',
  XBRU: 'Euronext Brussels',
  LIS: 'Euronext Lisbon',
  XLIS: 'Euronext Lisbon',
  MIL: 'Borsa Italiana (Milan)',
  BIT: 'Borsa Italiana (Milan)',
  MCE: 'Bolsa de Madrid',
  BME: 'Bolsa de Madrid',
  SWX: 'SIX Swiss Exchange',
  SIX: 'SIX Swiss Exchange',
  VIE: 'Vienna Stock Exchange',
  WBO: 'Vienna Stock Exchange',
  WAR: 'Warsaw Stock Exchange',
  WSE: 'Warsaw Stock Exchange',
  IST: 'Borsa Istanbul',
  XIST: 'Borsa Istanbul',
  HEL: 'Nasdaq Helsinki',
  CPH: 'Nasdaq Copenhagen',
  STO: 'Nasdaq Stockholm',
  OSL: 'Oslo Stock Exchange',
  ICE: 'Nasdaq Iceland',
  DUB: 'Euronext Dublin',
  ATH: 'Athens Stock Exchange',
  BUD: 'Budapest Stock Exchange',
  PRA: 'Prague Stock Exchange',
  BUC: 'Bucharest Stock Exchange',
  MOEX: 'Moscow Exchange',
  TSX: 'Toronto Stock Exchange',
  TSXV: 'TSX Venture Exchange',
  CVE: 'TSX Venture Exchange',
  CNSX: 'Canadian Securities Exchange',
  MEX: 'Bolsa Mexicana de Valores',
  BCBA: 'Buenos Aires Stock Exchange',
  BVMF: 'B3 (São Paulo)',
  SAO: 'B3 (São Paulo)',
  TSE: 'Tokyo Stock Exchange',
  HKSE: 'Hong Kong Stock Exchange',
  SHSE: 'Shanghai Stock Exchange',
  SSE: 'Shanghai Stock Exchange',
  SZSE: 'Shenzhen Stock Exchange',
  TPE: 'Taiwan Stock Exchange',
  TWSE: 'Taiwan Stock Exchange',
  ROCO: 'Taipei Exchange',
  XKRX: 'Korea Exchange',
  KRX: 'Korea Exchange',
  NSE: 'National Stock Exchange of India',
  BSE: 'Bombay Stock Exchange',
  SGX: 'Singapore Exchange',
  XKLS: 'Bursa Malaysia',
  KLSE: 'Bursa Malaysia',
  BKK: 'Stock Exchange of Thailand',
  SET: 'Stock Exchange of Thailand',
  PHS: 'Philippine Stock Exchange',
  IDX: 'Indonesia Stock Exchange',
  ASX: 'Australian Securities Exchange',
  NZSE: 'New Zealand Exchange',
  NZX: 'New Zealand Exchange',
  JSE: 'Johannesburg Stock Exchange',
  TASE: 'Tel Aviv Stock Exchange',
  SAU: 'Saudi Stock Exchange (Tadawul)',
  DFM: 'Dubai Financial Market',
  ADX: 'Abu Dhabi Securities Exchange',
  QSE: 'Qatar Stock Exchange',
};

/** Compute top N non-overlapping drawdown periods from (date, value) pairs. */
export function computeTopDrawdowns(
  values: { date: string; value: number }[],
  n: number = 3,
): DrawdownPeriod[] {
  if (values.length < 2) return [];

  const periods: DrawdownPeriod[] = [];
  let peakVal = values[0].value;
  let peakDate = values[0].date;
  let troughVal = peakVal;
  let troughDate = peakDate;
  let inDrawdown = false;

  for (let i = 1; i < values.length; i++) {
    const { date: dt, value: val } = values[i];
    if (val >= peakVal) {
      if (inDrawdown) {
        periods.push({
          drawdown_pct: Math.round((troughVal / peakVal - 1) * 10000) / 100,
          peak_date: peakDate,
          trough_date: troughDate,
          recovery_date: dt,
        });
        inDrawdown = false;
      }
      peakVal = val;
      peakDate = dt;
      troughVal = val;
      troughDate = dt;
    } else {
      inDrawdown = true;
      if (val < troughVal) {
        troughVal = val;
        troughDate = dt;
      }
    }
  }
  if (inDrawdown) {
    periods.push({
      drawdown_pct: Math.round((troughVal / peakVal - 1) * 10000) / 100,
      peak_date: peakDate,
      trough_date: troughDate,
      recovery_date: null,
    });
  }

  // Pick top N non-overlapping
  const sorted = [...periods].sort((a, b) => a.drawdown_pct - b.drawdown_pct);
  const selected: DrawdownPeriod[] = [];
  for (const p of sorted) {
    if (selected.length >= n) break;
    const pEnd = p.recovery_date ?? '9999-99';
    const overlaps = selected.some(s => {
      const sEnd = s.recovery_date ?? '9999-99';
      return p.peak_date <= sEnd && pEnd >= s.peak_date;
    });
    if (!overlaps) selected.push(p);
  }
  return selected;
}

export const tooltipStyle = {
  contentStyle: { background: '#1a1d27', border: '1px solid rgba(75,85,99,0.4)', borderRadius: 8, fontSize: 13 },
  labelStyle: { color: '#9ca3af' },
  itemStyle: { color: '#e5e7eb' },
};
