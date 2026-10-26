export type Performance = { periode: string; rendement: number | null; beginvermogen: number | null;
  eindvermogen: number | null; beleggingsresultaat: number | null; koersresultaat: number | null;
  opbrengsten: number | null; kosten: number | null; stortingen: number | null;
  onttrekkingen: number | null; mutatie_opgelopen_rente: number | null; fetched_at?: string };
export type Holding = { name: string; isin: string | null; currency: string; quantity: number | null;
  value_eur: number | null; weight_pct: number | null };
export type Valuation = { as_of: string; requested_date: string; rows: Holding[] };
export type Activity = { name: string; kind: 'buy' | 'sell'; date: string; amount_eur: number | null; quantity: number };
export type Sheet = { columns: string[]; rows: Record<string, unknown>[]; kinds: Record<string, string>;
  readable: boolean; activity: Activity[] };
export type Journal = { boekdatum: string | null; fonds: string; grootboek: string; omschrijving: string; amount_eur: number };
export type MonthlyPosition = { name: string; isin: string | null; return_pct: number | null;
  opening_weight_pct?: number | null; contribution_pp?: number | null; is_cash?: boolean;
  contribution_calculation?: { opening_value_eur: number; closing_value_eur: number; purchases_eur: number;
    sales_eur: number; result_eur: number; portfolio_opening_capital_eur: number;
    opening_date: string | null; closing_date: string | null;
    trades: { date: string; kind: 'buy' | 'sell'; amount_eur: number }[] } | null;
  start_price_eur?: number | null; end_price_eur?: number | null;
  start_date: string | null; end_date: string | null; status: 'held' | 'bought' | 'sold' | 'unavailable'; reason?: string };
type Section<T> = { status: 'ok' | 'no_data' | 'error'; data: T | null; message?: string };
export type MonthlyReport = { portefeuille: string; month: string; start: string; end: string; opening_date: string;
  weighted_benchmark?: Record<string, { return_pct: number; start_date: string; end_date: string; source: string;
    components?: { ticker: string; weight_pct: number; return_pct: number; start_date: string; end_date: string;
      start_price_eur?: number; end_price_eur?: number }[] }>;
  months: (Performance & { month: string })[]; fetched_at: string | null; needs_refresh: boolean; warning: string | null;
  snapshot: { start: string; end: string; fetched_at: string; positions?: MonthlyPosition[];
    reconciliation?: { status: 'reconciled' | 'not_reconciled' | 'unavailable'; monthly_return_pct: number | null;
      contribution_sum_pp: number | null; difference_pp: number | null; issues: string[];
      adjustments: { kind: string; contribution_pp: number }[] };
    sections: {
    performance: Section<Performance>; opening: Section<Valuation>; closing: Section<Valuation>;
    transactions: Section<Sheet>; journal?: Section<Journal[]>;
  } } | null };

/** Older responses already carry the source reports, but not the worked calculation.
 * Reconstruct its operands for the popover without declaring the month reconciled.
 */
export function savedContributionCalculation(report: MonthlyReport | null, position: MonthlyPosition): MonthlyPosition['contribution_calculation'] {
  if (position.contribution_calculation) return position.contribution_calculation;
  const sections = report?.snapshot?.sections;
  if (!sections || position.is_cash || position.status === 'unavailable') return null;
  const { opening, closing, transactions, performance } = sections;
  if (opening?.status !== 'ok' || closing?.status !== 'ok' || transactions?.status !== 'ok'
    || !opening.data || !closing.data || !transactions.data?.readable) return null;
  const source = report?.months.find((row) => row.month === report.month && row.periode === closing.data?.as_of)
    ?? (performance?.status === 'ok' ? performance.data : null);
  const capital = source?.beginvermogen;
  if (capital == null || !Number.isFinite(capital) || capital <= 0 || source?.stortingen !== 0 || source?.onttrekkingen !== 0) return null;
  const before = opening.data.rows.filter((row) => row.name === position.name);
  const after = closing.data.rows.filter((row) => row.name === position.name);
  if (before.length > 1 || after.length > 1) return null;
  const first = before.length ? before[0].value_eur : 0;
  const last = after.length ? after[0].value_eur : 0;
  if (first == null || last == null || !Number.isFinite(first) || !Number.isFinite(last)) return null;
  const trades = transactions.data.activity.filter((trade) => trade.name === position.name);
  if (trades.some((trade) => trade.amount_eur == null || !Number.isFinite(trade.amount_eur) || trade.amount_eur < 0)) return null;
  const purchases = trades.filter((trade) => trade.kind === 'buy').reduce((sum, trade) => sum + trade.amount_eur!, 0);
  const sales = trades.filter((trade) => trade.kind === 'sell').reduce((sum, trade) => sum + trade.amount_eur!, 0);
  return { opening_value_eur: first, closing_value_eur: last, purchases_eur: purchases, sales_eur: sales,
    result_eur: last - first + sales - purchases, portfolio_opening_capital_eur: capital,
    opening_date: opening.data.as_of, closing_date: closing.data.as_of,
    trades: trades.map((trade) => ({ date: trade.date, kind: trade.kind, amount_eur: trade.amount_eur! })) };
}
