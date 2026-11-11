import { expect, it } from 'vitest';
import { recentWeightPlan, type TransactionBundle } from './recentWeightPlan';

function bundle(): TransactionBundle {
  return {
    success: true, from: '2026-11-04', to: '2026-11-10', retrieved_at: '2026-11-10T12:00:00Z', rules: [],
    transactions: [{ id: '1:1', date: '2026-11-10', event: 'Buy', name: 'Apple', isin: 'US0378331005', quantity: 10, price: 100, currency: 'USD' }],
    leonteq: { success: true, status: 'success', groups: [{ name: 'Share', weight: '100%', positions: [
      { name: 'Apple', isin: 'US0378331005', weight: '5.20%', price_date: '2026-11-10', total_value: null, units: null, price: null, currency: null, performance: null },
      { name: 'EURO', isin: null, weight: '94.80%', price_date: '2026-11-10', total_value: null, units: null, price: null, currency: null, performance: null },
    ] }] },
    airs: { portfolio: 'Fixed', portfolio_id: 1, datum: '2026-11-10', dates: ['2026-11-10'], rows: [
      { id: 1, Fonds: 'Apple', ISINCode: 'US0378331005', Percentage: 4, valuta: null, Beleggingscategorie: null, Beleggingssector: null, regio: null, afmCategorie: null },
    ] },
  };
}

it('groups trades and uses the actual current weight, independent of transaction amounts', () => {
  const data = bundle();
  data.transactions.push({ ...data.transactions[0], id: '1:2', event: 'Sell', quantity: 99999 });
  const rows = recentWeightPlan(data);
  expect(rows).toHaveLength(1);
  expect(rows[0].trades).toHaveLength(2);
  expect(rows[0].target).toBe(5.2);
  expect(rows[0].current).toBe(4);
  expect(rows[0].reason).toBeNull();
});

it('proposes zero only after a verified full sale and complete dated source snapshot', () => {
  const data = bundle();
  data.transactions[0].event = 'Sell';
  data.leonteq.groups![0].positions.shift();
  expect(recentWeightPlan(data)[0].target).toBe(0);
  data.leonteq.groups![0].positions[0].price_date = '2026-11-09';
  expect(recentWeightPlan(data)[0].reason).toContain('full sale');
  data.leonteq.groups![0].positions[0].price_date = undefined;
  expect(recentWeightPlan(data)[0].target).toBeNull();
});

it('does not turn a missing buy holding into a zero target', () => {
  const data = bundle();
  data.leonteq.groups![0].positions.shift();
  expect(recentWeightPlan(data)[0].target).toBeNull();
  expect(recentWeightPlan(data)[0].reason).toContain('full sale');
});

it('honours manual connections and disconnects from the shared matcher', () => {
  const data = bundle();
  data.airs.rows[0].ISINCode = 'US5949181045';
  data.airs.rows[0].Fonds = 'Different name';
  expect(recentWeightPlan(data)[0].operation).toBe('add');
  data.rules = [{ leonteq_key: 'isin:US0378331005', airs_key: 'isin:US5949181045' }];
  expect(recentWeightPlan(data)[0].reason).toBeNull();
  data.rules[0].airs_key = null;
  expect(recentWeightPlan(data)[0].reason).toContain('Needs connection');
});

it('offers an add at the current source weight when the instrument is absent from AIRS Fixed', () => {
  const data = bundle();
  data.airs.rows = [{ ...data.airs.rows[0], id: 2, Fonds: 'Liquiditeiten', ISINCode: null, Percentage: 100 }];
  const row = recentWeightPlan(data)[0];
  expect(row.operation).toBe('add');
  expect(row.current).toBe(0);
  expect(row.target).toBe(5.2);
  expect(row.airsIsin).toBe('US0378331005');
  expect(row.rowId).toBeNull();
  expect(row.reason).toBeNull();
  data.rules = [{ leonteq_key: 'isin:US0378331005', airs_key: null }];
  expect(recentWeightPlan(data)[0].reason).toContain('Needs connection');
});

it('marks aligned weights and refuses missing row metadata or duplicate positions', () => {
  const data = bundle();
  data.airs.rows[0].Percentage = 5.2;
  expect(recentWeightPlan(data)[0].aligned).toBe(true);
  data.airs.rows[0].id = null;
  expect(recentWeightPlan(data)[0].reason).toContain('metadata');
  data.airs.rows.push({ ...data.airs.rows[0], id: 2 });
  expect(recentWeightPlan(data)[0].reason).toContain('Ambiguous');
});
