import { describe, expect, it } from 'vitest';
import { savedContributionCalculation, type MonthlyPosition, type MonthlyReport } from './monthlyReport';

const position: MonthlyPosition = { name: 'Euronext', isin: 'NL0006294274', status: 'bought',
  return_pct: 2.516654, start_date: '2026-03-25', end_date: '2026-03-31' };
const fixture = () => ({ month: '2026-03', months: [], snapshot: { sections: {
  opening: { status: 'ok', data: { as_of: '2026-02-28', rows: [] } },
  closing: { status: 'ok', data: { as_of: '2026-03-31', rows: [{ name: 'Euronext', value_eur: 14404 }] } },
  transactions: { status: 'ok', data: { readable: true, activity: [
    { name: 'Euronext', date: '2026-03-25', kind: 'buy', amount_eur: 14050.4 },
  ] } },
  performance: { status: 'ok', data: { beginvermogen: 1120508.18, stortingen: 0, onttrekkingen: 0 } },
} } }) as unknown as MonthlyReport;

describe('saved AIRS contribution operands', () => {
  it('recovers the actual Euronext calculation from an older saved response', () => {
    const calculation = savedContributionCalculation(fixture(), position)!;
    expect(calculation.opening_value_eur).toBe(0);
    expect(calculation.purchases_eur).toBe(14050.4);
    expect(calculation.result_eur).toBeCloseTo(353.6);
    expect(100 * calculation.result_eur / calculation.portfolio_opening_capital_eur).toBeCloseTo(.03155711);
    expect(calculation.trades[0].date).toBe('2026-03-25');
  });
  it('never interprets missing source data or external flows as a valid zero', () => {
    const report = fixture();
    report.snapshot!.sections.opening.status = 'error';
    expect(savedContributionCalculation(report, position)).toBeNull();
    const flows = fixture();
    flows.snapshot!.sections.performance.data!.stortingen = 1000;
    expect(savedContributionCalculation(flows, position)).toBeNull();
    expect(savedContributionCalculation(null, position)).toBeNull();
  });
});
