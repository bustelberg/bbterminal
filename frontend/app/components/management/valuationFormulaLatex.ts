import { basisFx, workbookValuation, type CalculationKey, type Workbook, type WorkbookRow } from './valuationWorkbook';

const text = (value: string) => `\\text{${value.replace(/[\\{}$&#_%^~]/g, ' ')}}`;
const formatNumber = (value: unknown) => {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '\\text{missing}';
  const rounded = value !== 0 && Math.abs(value) < .01 ? Number(value.toPrecision(3)).toExponential() : String(Number(value.toFixed(2)));
  const literal = rounded.replace(/e([+-]?\d+)/i, '\\times10^{$1}');
  return value < 0 ? `\\left(${literal}\\right)` : literal;
};
const scale = (currency?: string | null) => currency === 'GBp' || currency === 'GBX' ? .01 : 1;

const VARIABLES = [
  ['EPS_1', 'Earnings per share for the first forecast year.', 'Winst per aandeel in het eerste prognosejaar.'],
  ['EPS_2', 'Earnings per share for the second forecast year.', 'Winst per aandeel in het tweede prognosejaar.'],
  ['M', 'First-year weight in months (0–12): your override, otherwise calendar months from the price date through FY1 year-end.', 'Weging van het eerste jaar in maanden (0–12): uw invoer, anders kalendermaanden vanaf de koersdatum tot het einde van FY1.'],
  ['B_{\\mathrm{manual}}', 'Entered earnings, NAV or book value per share.', 'Ingevoerde winst, NAV of boekwaarde per aandeel.'],
  ['s_B', 'Source-value unit factor: 0.01 for pence, otherwise 1.', 'Eenheidfactor bronwaarde: 0,01 voor pence, anders 1.'],
  ['FX', 'Source-to-valuation currency rate; 1 for matching currencies.', 'Omrekenkoers van bron- naar waarderingsvaluta; 1 bij gelijke valuta.'],
  ['P_Y', 'Raw Yahoo closing price.', 'Ruwe Yahoo-slotkoers.'],
  ['s_P', 'Price unit factor: 0.01 for pence, otherwise 1.', 'Koerseenheidfactor: 0,01 voor pence, anders 1.'],
  ['C_Y', 'Yahoo quote currency; GBp and GBX normalize to GBP.', 'Yahoo-koersvaluta; GBp en GBX worden GBP.'],
  ['m_f', 'Fair valuation multiple.', 'Faire waarderingsmultiple.'],
  ['h', 'Safety margin as a fraction, e.g. 0.20 means 20%.', 'Veiligheidsmarge als fractie, bijvoorbeeld 0,20 is 20%.'],
  ['g', 'Annual base growth as a fraction.', 'Jaarlijkse basisgroei als fractie.'],
  ['T', 'Projection period in years.', 'Projectieperiode in jaren.'],
  ['m_e', 'Exit valuation multiple.', 'Waarderingsmultiple aan het einde van de projectie.'],
  ['\\Delta g_{\\mathrm{bear}}', 'Bear adjustment to annual growth.', 'Negatieve aanpassing van de jaarlijkse groei.'],
  ['\\Delta g_{\\mathrm{bull}}', 'Bull adjustment to annual growth.', 'Positieve aanpassing van de jaarlijkse groei.'],
  ['\\Delta m_{\\mathrm{bear}}', 'Bear adjustment to the exit multiple, as a fraction.', 'Negatieve aanpassing van de exitmultiple, als fractie.'],
  ['\\Delta m_{\\mathrm{bull}}', 'Bull adjustment to the exit multiple, as a fraction.', 'Positieve aanpassing van de exitmultiple, als fractie.'],
  ['Y_1', 'Year of the first forecast fiscal year-end.', 'Jaar van het eerste voorspelde boekjaareinde.'],
  ['Y_P', 'Year of the price date.', 'Jaar van de koersdatum.'],
  ['M_1', 'Month of the first forecast fiscal year-end (1–12).', 'Maand van het eerste voorspelde boekjaareinde (1–12).'],
  ['M_P', 'Month of the price date (1–12).', 'Maand van de koersdatum (1–12).'],
  ['M_{\\mathrm{override}}', 'Entered first-year month weight (0–12).', 'Ingevoerde maandweging van het eerste jaar (0–12).'],
  ['M_{\\mathrm{calendar}}', 'Calendar months from the price date through FY1 year-end, limited to 0–12.', 'Kalendermaanden vanaf de koersdatum tot het einde van FY1, begrensd op 0–12.'],
] as const;

export function valuationFormulaLatex(calculation: CalculationKey, workbook: Workbook, row?: WorkbookRow) {
  let roundedInputs = false;
  const n = (value: unknown) => {
    if (typeof value === 'number' && Number.isFinite(value) && Math.abs(value) >= .01
      && Math.abs(value - Number(value.toFixed(2))) > 1e-9) roundedInputs = true;
    return formatNumber(value);
  };
  const p = workbook.parameters, a = row?.assumptions, f = row?.forecast, q = row?.quote;
  const result = row ? workbookValuation(row, q ?? {}, p, workbook.as_of) : null;
  const manual = a?.basis_kind === 'manual';
  const from = manual ? a?.basis_currency : f?.currency;
  const fx = row ? basisFx(row, q ?? {}) : undefined;
  const basis = (numeric: boolean) => manual
    ? `${numeric ? n(a?.manual_basis) : 'B_{\\mathrm{manual}}'} \\times ${numeric ? n(scale(from)) : 's_B'} \\times ${numeric ? n(fx) : 'FX'}`
    : `\\left(${numeric ? n(f?.fy1) : 'EPS_1'}\\frac{${numeric ? n(result?.months) : 'M'}}{12}
      + ${numeric ? n(f?.fy2) : 'EPS_2'}\\frac{12-${numeric ? n(result?.months) : 'M'}}{12}\\right)
      \\times ${numeric ? n(scale(from)) : 's_B'} \\times ${numeric ? n(fx) : 'FX'}`;
  const expression = (numeric: boolean): string => {
    const v = (symbol: string, value: unknown) => numeric ? n(value) : symbol;
    const B = `\\left(${basis(numeric)}\\right)`;
    const P = `\\left(${v('P_Y', q?.price)}\\times ${v('s_P', scale(q?.currency))}\\right)`;
    const fair = `${B}\\times ${v('m_f', a?.fair_multiple)}`;
    const margin = `\\left(1-${v('h', p.safety_margin)}\\right)`;
    const buy = `${fair}\\times ${margin}`;
    const scenario = (kind: 'bear' | 'base' | 'bull') => {
      const delta = kind === 'base' ? '' : `+${v(`\\Delta g_{\\mathrm{${kind}}}`, p[`${kind}_growth_delta`])}`;
      const growth = kind === 'bear' ? `\\max\\left(-0.9,${v('g', a?.growth)}${delta}\\right)` : `${v('g', a?.growth)}${delta}`;
      const multiple = kind === 'base' ? '' : `\\times\\left(1+${v(`\\Delta m_{\\mathrm{${kind}}}`, p[`${kind}_multiple_delta`])}\\right)`;
      return `${B}\\times\\left(1+${growth}\\right)^{${v('T', p.years)}}\\times ${v('m_e', a?.exit_multiple)}${multiple}`;
    };
    switch (calculation) {
      case 'valuationCurrency': return `\\operatorname{normalize}\\left(${numeric ? text(q?.currency ?? 'missing') : 'C_Y'}\\right)`;
      case 'unitScale': return numeric ? n(scale(q?.currency)) : '\\begin{cases}0.01 & C_Y\\in\\{\\mathrm{GBp},\\mathrm{GBX}\\}\\\\1&\\text{otherwise}\\end{cases}';
      case 'price': return P;
      case 'calendarMonths': return `\\min\\left(12,\\max\\left(0,12\\left(${v('Y_1', f?.fy1_date ? Number(f.fy1_date.slice(0, 4)) : null)}-${v('Y_P', q?.price_date ? Number(q.price_date.slice(0, 4)) : null)}\\right)+${v('M_1', f?.fy1_date ? Number(f.fy1_date.slice(5, 7)) : null)}-${v('M_P', q?.price_date ? Number(q.price_date.slice(5, 7)) : null)}+1\\right)\\right)`;
      case 'months': return numeric ? n(result?.months) : 'M_{\\mathrm{override}}\\;\\text{if entered, otherwise }M_{\\mathrm{calendar}}';
      case 'basis': return basis(numeric);
      case 'currentMultiple': return `\\frac{${P}}{${B}}`;
      case 'fair': return fair;
      case 'fairUpside': return `\\left(\\frac{${fair}}{${P}}-1\\right)\\times100`;
      case 'buyMultiple': return `${v('m_f', a?.fair_multiple)}\\times${margin}`;
      case 'buyPrice': return buy;
      case 'buyUpside': return `\\left(\\frac{${buy}}{${P}}-1\\right)\\times100`;
      case 'bear': return scenario('bear');
      case 'base': return scenario('base');
      case 'bull': return scenario('bull');
      case 'cagr': return `\\left[\\left(\\frac{${scenario('base')}}{${P}}\\right)^{\\frac{1}{${v('T', p.years)}}}-1\\right]\\times100`;
      case 'signal': return `\\begin{cases}\\text{?}&\\text{missing or unusable inputs}\\\\\\text{REVIEW}&\\text{review required}\\\\\\text{BUY}&${P}\\leq ${buy}\\\\\\text{CONSIDER}&${P}\\leq ${fair}\\\\\\text{DO NOT BUY}&\\text{otherwise}\\end{cases}`;
    }
  };
  const value = result?.[calculation];
  const unit = ['fairUpside', 'buyUpside', 'cagr'].includes(calculation) ? '\\%'
    : ['currentMultiple', 'buyMultiple'].includes(calculation) ? '\\times'
    : ['price', 'basis', 'fair', 'buyPrice', 'bear', 'base', 'bull'].includes(calculation) ? text(result?.valuationCurrency ?? '') : '';
  const answer = typeof value === 'number' ? `${value.toFixed(2)}\\;${unit}` : typeof value === 'string' ? text(value) : null;
  const labels: Record<CalculationKey, string> = {
    valuationCurrency: 'Currency', unitScale: 'Price unit factor', price: 'Price', calendarMonths: 'Calendar months',
    months: 'Months used', basis: 'Basis per share', currentMultiple: 'Current multiple', fair: 'Fair price',
    fairUpside: 'Fair upside', buyMultiple: 'Buy multiple', buyPrice: 'Buy price', buyUpside: 'Buy upside',
    signal: 'Signal', bear: 'Bear', base: 'Base', bull: 'Bull', cagr: 'Base CAGR',
  };
  const lhs = text(labels[calculation]);
  const symbolic = `${lhs} = ${expression(false)}`;
  const variables = VARIABLES.filter(([symbol]) => {
    if (symbol.length > 1) return symbolic.includes(symbol);
    return new RegExp(`(^|[^A-Za-z_])${symbol}(?![A-Za-z_])`).test(symbolic);
  }).map(([symbol, en, nl]) => ({ symbol, en, nl }));
  const numeric = row && answer != null ? expression(true) : null;
  const roundedResult = typeof value === 'number' && Math.abs(value - Number(value.toFixed(2))) > 1e-9;
  return { symbolic, variables, substituted: numeric != null ? `${lhs} = ${numeric} ${roundedInputs || roundedResult ? '\\approx' : '='} ${answer}` : null,
    reason: result?.reason ?? 'Required inputs are missing or invalid.' };
}
