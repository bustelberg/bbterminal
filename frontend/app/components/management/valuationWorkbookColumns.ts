import { VALUATION_FORMULAS, type CalculationKey } from './valuationWorkbook';

export type ColumnKind = 'data' | 'assumption' | 'calculation';
export const COLUMN_STYLE: Record<ColumnKind, string> = {
  data: 'bg-blue-500/10 border-blue-500',
  assumption: 'bg-green-500/10 border-green-500',
  calculation: 'bg-yellow-500/10 border-yellow-500',
};
export type WorkbookColumn = { key: string; en: string; nl: string; kind: ColumnKind; formula?: string };
const data = [
  ['company', 'Company', 'Bedrijf'], ['ticker', 'Guru ticker', 'Guru ticker'], ['selections', 'Portfolio membership', 'Portefeuillelidmaatschap'],
  ['raw_price', 'Raw Yahoo close', 'Ruwe Yahoo-slotkoers'], ['quote_currency', 'Yahoo quote currency/unit', 'Yahoo-koersvaluta/eenheid'],
  ['price_date', 'Price date', 'Koersdatum'], ['price_source', 'Price source', 'Koersbron'],
  ['fy1', 'GuruFocus FY1 EPS', 'GuruFocus FY1 EPS'], ['fy1_date', 'FY1 fiscal date', 'Boekjaardatum FY1'],
  ['fy2', 'GuruFocus FY2 EPS', 'GuruFocus FY2 EPS'], ['fy2_date', 'FY2 fiscal date', 'Boekjaardatum FY2'],
  ['forecast_currency', 'Forecast currency/unit', 'Prognosevaluta/eenheid'], ['forecast_basis', 'Forecast earnings measure', 'Winstmaatstaf prognose'],
  ['forecast_source', 'Forecast source', 'Prognosebron'], ['manual_basis', 'Manual source basis per share', 'Handmatige bronbasis per aandeel'],
  ['manual_currency', 'Manual source currency/unit', 'Handmatige bronvaluta/eenheid'], ['manual_label', 'Manual source measure', 'Handmatige bronmaatstaf'],
  ['manual_date', 'Manual source date', 'Handmatige bronpeildatum'], ['manual_source', 'Manual source URL', 'Handmatige bron-URL'],
] as const;
const assumptions = [
  ['basis_kind', 'Basis selection', 'Keuze waarderingsbasis'], ['months_fy1', 'FY1 months override', 'Maanden FY1 invoer'],
  ['basis_fx', 'Basis → quote FX input', 'FX-invoer basis → koers'], ['growth', 'Base growth p.a.', 'Groei basis p.j.'],
  ['fair_multiple', 'Fair multiple', 'Faire multiple'], ['exit_multiple', 'Exit multiple', 'Exitmultiple'],
  ['review_required', 'Review required', 'Nader toetsen'], ['risk_note', 'Key risk / consideration', 'Kernrisico / aandachtspunt'],
] as const;
const calculations: readonly (readonly [CalculationKey, string, string])[] = [
  ['valuationCurrency', 'Valuation currency', 'Waarderingsvaluta'], ['unitScale', 'Price unit factor', 'Koerseenheidfactor'],
  ['price', 'Price in valuation currency', 'Koers in waarderingsvaluta'], ['calendarMonths', 'FY1 calendar months', 'Kalendermaanden FY1'],
  ['months', 'FY1 months used', 'Gebruikte maanden FY1'], ['basis', 'Calculated earnings / NAV / BV per share', 'Berekende winst / NAV / BV p.a.'],
  ['currentMultiple', 'Current multiple', 'Actuele multiple'], ['fair', 'Fair price', 'Faire koers'], ['fairUpside', 'Fair vs price', 'Verschil fair vs koers'],
  ['buyMultiple', 'Buy multiple', 'Koopmultiple'], ['buyPrice', 'Buy price', 'Koopprijs'], ['buyUpside', 'To buy price', 'Tot koopprijs'],
  ['signal', 'Buy signal', 'Koopsignaal'], ['bear', 'Bear', 'Negatief'], ['base', 'Base', 'Basis'], ['bull', 'Bull', 'Positief'], ['cagr', 'Base CAGR', 'CAGR basis'],
];
export const WORKBOOK_COLUMNS: readonly WorkbookColumn[] = [
  ...data.map(([key, en, nl]) => ({ key, en, nl, kind: 'data' as const })),
  ...assumptions.map(([key, en, nl]) => ({ key, en, nl, kind: 'assumption' as const })),
  ...calculations.map(([key, en, nl]) => ({ key, en, nl, kind: 'calculation' as const, formula: VALUATION_FORMULAS[key].expression })),
  { key: 'safety_margin', en: 'Safety margin', nl: 'Veiligheidsmarge', kind: 'assumption' },
  { key: 'years', en: 'Projection years', nl: 'Projectiejaren', kind: 'assumption' },
];

export const WORKBOOK_PANELS = [
  { key: 'company', en: 'Onderneming', nl: 'Onderneming', columns: [
    'company', 'ticker', 'selections', 'valuationCurrency',
  ] },
  { key: 'earnings', en: 'Koers & winst', nl: 'Koers & winst', columns: [
    'company', 'raw_price', 'quote_currency', 'unitScale', 'price', 'basis_kind', 'fy1', 'fy1_date', 'fy2', 'fy2_date', 'forecast_currency', 'forecast_basis',
    'months_fy1', 'calendarMonths', 'months', 'manual_basis', 'manual_currency', 'manual_label', 'basis_fx', 'basis', 'currentMultiple', 'growth',
  ] },
  { key: 'valuation', en: 'Waardering & koopzone', nl: 'Waardering & koopzone', columns: [
    'company', 'valuationCurrency', 'price', 'fair_multiple', 'fair', 'fairUpside', 'safety_margin', 'buyMultiple', 'buyPrice', 'buyUpside', 'signal',
  ] },
  { key: 'scenario', en: 'Scenario', nl: 'Scenario', columns: [
    'company', 'valuationCurrency', 'growth', 'exit_multiple', 'years', 'bear', 'base', 'bull', 'cagr',
  ] },
  { key: 'sources', en: 'Bron & controle', nl: 'Bron & controle', columns: [
    'company', 'ticker', 'price_date', 'price_source', 'forecast_source', 'forecast_basis', 'manual_date', 'manual_source', 'review_required', 'risk_note',
  ] },
  { key: 'memberships', en: 'Afkortingen', nl: 'Afkortingen', columns: [] },
  { key: 'parameters', en: 'Parameters', nl: 'Parameters', columns: [] },
] as const;
export type WorkbookPanelKey = typeof WORKBOOK_PANELS[number]['key'];
export function columnsForPanel(panel: WorkbookPanelKey): WorkbookColumn[] {
  return WORKBOOK_PANELS.find(item => item.key === panel)!.columns.map(key => WORKBOOK_COLUMNS.find(column => column.key === key)!);
}
export function panelForInput(key: string): WorkbookPanelKey {
  if (['years', 'safety_margin', 'bear_growth_delta', 'bear_multiple_delta', 'bull_growth_delta', 'bull_multiple_delta'].includes(key)) return 'parameters';
  if (key === 'fair_multiple') return 'valuation';
  if (key === 'exit_multiple') return 'scenario';
  if (key === 'as_of') return 'sources';
  return WORKBOOK_PANELS.find(panel => panel.key !== 'company' && (panel.columns as readonly string[]).includes(key))?.key ?? 'sources';
}
