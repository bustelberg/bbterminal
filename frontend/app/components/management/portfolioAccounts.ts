import type { AirsPortfolioOverview } from '../../../lib/types/api';

// Match the AIRS scan's minimum for real books; an explicit model link remains eligible.
export const MIN_REAL_HOLDINGS = 5;
export const canAnalyseAccount = (row: AirsPortfolioOverview) =>
  row.fixed_portfolio_id != null || (row.holdings ?? 0) >= MIN_REAL_HOLDINGS;
