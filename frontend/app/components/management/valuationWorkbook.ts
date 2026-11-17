export type WorkbookParameters = { years: number; safety_margin: number; bear_growth_delta: number;
  bear_multiple_delta: number; bull_growth_delta: number; bull_multiple_delta: number };
export type CompanyAssumption = {
  growth?: number | null; fair_multiple?: number | null; exit_multiple?: number | null;
  risk_note?: string; review_required?: boolean; basis_kind?: 'forward_eps' | 'manual';
  basis_label?: string; manual_basis?: number | null; basis_currency?: string | null;
  basis_date?: string | null; basis_source?: string | null; basis_fx?: number | null; months_fy1?: number | null;
};
export type WorkbookRow = { isin: string | null; ticker: string | null; selections: string[];
  membership_profiles?: Record<string, string[]>; membership_complete?: boolean; membership_unknown_codes?: string[];
  assumptions: CompanyAssumption; seed_source: string | null;
  quote?: { symbol?: string | null; price?: number | null; currency?: string | null; price_date?: string | null; source?: string | null };
  forecast: { fy1?: number; fy2?: number; fy1_date?: string; fy2_date?: string; currency?: string;
    basis_label?: string; metric_code?: string; reason?: string; source?: string } };
export type MembershipDefinition = { code: string; name: string; scope: string; source: string;
  status: 'complete' | 'partial' | 'unavailable';
  portfolios: { name: string; as_of: string | null; retrieved_at?: string | null; available: boolean; reason?: string }[] };
export type Workbook = { portfolio_id: number; as_of?: string; parameters: WorkbookParameters; rows: WorkbookRow[]; updated_at?: string | null;
  membership_legend?: MembershipDefinition[] };

const finite = (n: unknown): n is number => typeof n === 'number' && Number.isFinite(n);
const positive = (n: unknown): n is number => finite(n) && n > 0;
const unit = (code?: string | null) => code === 'GBp' || code === 'GBX' ? .01 : 1;
const currency = (code?: string | null) => code === 'GBp' || code === 'GBX' ? 'GBP' : code ?? null;

/** Only blue source fields and green assumptions enter the formula engine.
 * Calculated cell values are deliberately absent from this type. */
export function valuationInputs(row: WorkbookRow, quote: NonNullable<WorkbookRow['quote']>, p: WorkbookParameters, asOf?: string) {
  const a = row.assumptions, f = row.forecast;
  return { raw_price: quote.price, quote_currency: quote.currency, price_date: quote.price_date, as_of: asOf,
    fy1: f.fy1, fy2: f.fy2, fy1_date: f.fy1_date, fy2_date: f.fy2_date, forecast_currency: f.currency,
    manual_basis: a.manual_basis, manual_currency: a.basis_currency,
    basis_kind: a.basis_kind, basis_fx: a.basis_fx, months_fy1: a.months_fy1,
    growth: a.growth, fair_multiple: a.fair_multiple, exit_multiple: a.exit_multiple,
    review_required: a.review_required, ...p } as const;
}
type Inputs = Readonly<ReturnType<typeof valuationInputs>>;
type InputKey = keyof Inputs;

function calendarMonths(i: Inputs) {
  if (!i.fy1_date || !i.price_date) return null;
  const end = new Date(`${i.fy1_date}T12:00:00Z`), when = new Date(`${i.price_date}T12:00:00Z`);
  const count = (end.getUTCFullYear() - when.getUTCFullYear()) * 12 + end.getUTCMonth() - when.getUTCMonth() + 1;
  return finite(count) ? Math.max(0, Math.min(12, count)) : null;
}
function monthsFromInputs(i: Inputs) {
  const months = i.months_fy1 ?? calendarMonths(i);
  return finite(months) && months >= 0 && months <= 12 ? months : null;
}
function rawBasis(i: Inputs) {
  if (i.basis_kind === 'manual') return finite(i.manual_basis) ? i.manual_basis : null;
  const months = monthsFromInputs(i);
  return finite(i.fy1) && finite(i.fy2) && months != null ? i.fy1 * months / 12 + i.fy2 * (1 - months / 12) : null;
}
// Pure subexpressions receive primitive inputs, never another displayed yellow cell.
function basisFromInputs(i: Inputs) {
  const raw = rawBasis(i), from = currency(i.basis_kind === 'manual' ? i.manual_currency : i.forecast_currency);
  const to = currency(i.quote_currency);
  if (raw == null || !from || !to) return null;
  const scale = unit(i.basis_kind === 'manual' ? i.manual_currency : i.forecast_currency);
  if (from === to) return raw * scale;
  return positive(i.basis_fx) ? raw * scale * i.basis_fx : null;
}
function fresh(i: Inputs) {
  if (!i.as_of) return true;
  if (!i.price_date) return false;
  const age = new Date(i.as_of).getTime() - new Date(i.price_date).getTime();
  return finite(age) && age >= 0 && age <= 7 * 86400000;
}
function priceFromInputs(i: Inputs) {
  return fresh(i) && positive(i.raw_price) && i.quote_currency ? i.raw_price * unit(i.quote_currency) : null;
}
function fairFromInputs(i: Inputs) {
  const basis = basisFromInputs(i);
  return positive(basis) && positive(i.fair_multiple) ? basis * i.fair_multiple : null;
}
function buyFromInputs(i: Inputs) {
  const basis = basisFromInputs(i);
  return positive(basis) && positive(i.fair_multiple) && finite(i.safety_margin) && i.safety_margin >= 0 && i.safety_margin < 1
    ? basis * i.fair_multiple * (1 - i.safety_margin) : null;
}
function scenarioFromInputs(i: Inputs, growthDelta: number, multipleDelta: number, bear = false) {
  const basis = basisFromInputs(i);
  if (!positive(basis) || !finite(i.growth) || i.growth <= -1 || !positive(i.exit_multiple) || !positive(i.years)) return null;
  const growth = bear ? Math.max(-.9, i.growth + growthDelta) : i.growth + growthDelta;
  if (growth <= -1 || multipleDelta <= -1) return null;
  const value = basis * (1 + growth) ** i.years * i.exit_multiple * (1 + multipleDelta);
  return finite(value) ? value : null;
}

const basisInputs = ['basis_kind', 'manual_basis', 'manual_currency', 'fy1', 'fy2', 'fy1_date', 'price_date',
  'months_fy1', 'forecast_currency', 'quote_currency', 'basis_fx'] as const;
const priceInputs = ['raw_price', 'quote_currency', 'price_date', 'as_of'] as const;
const scenarioInputs = [...basisInputs, 'growth', 'exit_multiple', 'years'] as const;
const B = 'IF(basis_kind = manual, manual_basis × UNIT(manual_currency), (fy1 × M / 12 + fy2 × (1 − M / 12)) × UNIT(forecast_currency)) × FX';
const P = '(raw_price × UNIT(quote_currency))';
const M = 'IF(months_fy1 is entered, months_fy1, CLAMP(calendar months from price_date through fy1_date, 0, 12))';
const FX = 'FX = 1 for matching normalized source/quote currencies; otherwise basis_fx';
type Formula = { inputs: readonly InputKey[]; expression: string; evaluate: (inputs: Inputs) => number | string | null };

/** Expressions expand source subexpressions: no formula references a yellow key.
 * Every evaluator has the same input-only signature, so evaluation order is irrelevant. */
export const VALUATION_FORMULAS = {
  valuationCurrency: { inputs: ['quote_currency'], expression: 'NORMALIZE_CURRENCY(quote_currency)', evaluate: (i: Inputs) => currency(i.quote_currency) },
  unitScale: { inputs: ['quote_currency'], expression: 'UNIT(quote_currency): 0.01 for GBp/GBX, otherwise 1', evaluate: (i: Inputs) => i.quote_currency ? unit(i.quote_currency) : null },
  price: { inputs: priceInputs, expression: P, evaluate: priceFromInputs },
  calendarMonths: { inputs: ['fy1_date', 'price_date'], expression: 'CLAMP(calendar months from price_date through fy1_date, 0, 12)', evaluate: calendarMonths },
  months: { inputs: ['months_fy1', 'fy1_date', 'price_date'], expression: M, evaluate: monthsFromInputs },
  basis: { inputs: basisInputs, expression: `${B}; M = ${M}; ${FX}`, evaluate: basisFromInputs },
  currentMultiple: { inputs: [...basisInputs, ...priceInputs], expression: `${P} / (${B}); M = ${M}; ${FX}`, evaluate: (i: Inputs) => {
    const basis = basisFromInputs(i), price = priceFromInputs(i); return positive(basis) && price != null ? price / basis : null;
  } },
  fair: { inputs: [...basisInputs, 'fair_multiple'], expression: `(${B}) × fair_multiple; M = ${M}; ${FX}`, evaluate: fairFromInputs },
  fairUpside: { inputs: [...basisInputs, ...priceInputs, 'fair_multiple'], expression: `((${B}) × fair_multiple / ${P} − 1) × 100; M = ${M}; ${FX}`, evaluate: (i: Inputs) => {
    const target = fairFromInputs(i), price = priceFromInputs(i); return positive(target) && price != null ? (target / price - 1) * 100 : null;
  } },
  buyMultiple: { inputs: ['fair_multiple', 'safety_margin'], expression: 'fair_multiple × (1 − safety_margin)', evaluate: (i: Inputs) =>
    positive(i.fair_multiple) && i.safety_margin >= 0 && i.safety_margin < 1 ? i.fair_multiple * (1 - i.safety_margin) : null },
  buyPrice: { inputs: [...basisInputs, 'fair_multiple', 'safety_margin'], expression: `(${B}) × fair_multiple × (1 − safety_margin); M = ${M}; ${FX}`, evaluate: buyFromInputs },
  buyUpside: { inputs: [...basisInputs, ...priceInputs, 'fair_multiple', 'safety_margin'], expression: `((${B}) × fair_multiple × (1 − safety_margin) / ${P} − 1) × 100; M = ${M}; ${FX}`, evaluate: (i: Inputs) => {
    const target = buyFromInputs(i), price = priceFromInputs(i); return positive(target) && price != null ? (target / price - 1) * 100 : null;
  } },
  signal: { inputs: [...basisInputs, ...priceInputs, 'fair_multiple', 'safety_margin', 'review_required'], expression: `REVIEW if review_required or inputs unavailable; BUY if ${P} ≤ (${B}) × fair_multiple × (1 − safety_margin); CONSIDER if ${P} ≤ (${B}) × fair_multiple; otherwise DO NOT BUY. M = ${M}; ${FX}`, evaluate: (i: Inputs) => {
    const price = priceFromInputs(i), fair = fairFromInputs(i), buy = buyFromInputs(i);
    return i.review_required || price == null || !positive(fair) || !positive(buy) ? 'REVIEW'
      : price <= buy ? 'BUY' : price <= fair ? 'CONSIDER' : 'DO NOT BUY';
  } },
  bear: { inputs: [...scenarioInputs, 'bear_growth_delta', 'bear_multiple_delta'], expression: `(${B}) × (1 + MAX(−0.9, growth + bear_growth_delta)) ^ years × exit_multiple × (1 + bear_multiple_delta); M = ${M}; ${FX}`, evaluate: (i: Inputs) => scenarioFromInputs(i, i.bear_growth_delta, i.bear_multiple_delta, true) },
  base: { inputs: scenarioInputs, expression: `(${B}) × (1 + growth) ^ years × exit_multiple; M = ${M}; ${FX}`, evaluate: (i: Inputs) => scenarioFromInputs(i, 0, 0) },
  bull: { inputs: [...scenarioInputs, 'bull_growth_delta', 'bull_multiple_delta'], expression: `(${B}) × (1 + growth + bull_growth_delta) ^ years × exit_multiple × (1 + bull_multiple_delta); M = ${M}; ${FX}`, evaluate: (i: Inputs) => scenarioFromInputs(i, i.bull_growth_delta, i.bull_multiple_delta) },
  cagr: { inputs: [...scenarioInputs, ...priceInputs], expression: `(((${B}) × (1 + growth) ^ years × exit_multiple / ${P}) ^ (1 / years) − 1) × 100; M = ${M}; ${FX}`, evaluate: (i: Inputs) => {
    const target = scenarioFromInputs(i, 0, 0), price = priceFromInputs(i);
    return positive(target) && price != null && positive(i.years) ? ((target / price) ** (1 / i.years) - 1) * 100 : null;
  } },
} satisfies Record<string, Formula>;
export type CalculationKey = keyof typeof VALUATION_FORMULAS;
type Values = { [K in CalculationKey]: ReturnType<typeof VALUATION_FORMULAS[K]['evaluate']> };

export function workbookValuation(row: WorkbookRow, quote: NonNullable<WorkbookRow['quote']>, p: WorkbookParameters, asOf?: string) {
  const inputs = valuationInputs(row, quote, p, asOf);
  const values = Object.fromEntries(Object.entries(VALUATION_FORMULAS).map(([key, formula]) => [key, formula.evaluate(inputs)])) as Values;
  let reason: string | null = null;
  if (!fresh(inputs)) reason = 'Latest saved Yahoo close is more than seven days old or undated.';
  else if (rawBasis(inputs) == null) reason = row.assumptions.basis_kind === 'manual'
    ? 'Enter a per-share source basis and its currency.' : row.forecast.reason ?? 'Two annual forecasts and fiscal dates are required.';
  else if (basisFromInputs(inputs) == null) {
    const from = currency(inputs.basis_kind === 'manual' ? inputs.manual_currency : inputs.forecast_currency);
    const to = currency(inputs.quote_currency);
    reason = from && to ? `Enter the ${from} → ${to} basis conversion rate; no FX or ADR ratio is assumed.`
      : 'Basis currency or quote currency is missing.';
  }
  return { ...values, reason, basisLabel: row.assumptions.basis_kind === 'manual'
    ? row.assumptions.basis_label || 'Manual source basis' : `NTM ${row.forecast.basis_label || 'Forward EPS'}` };
}
