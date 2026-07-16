/**
 * Curated re-exports of generated API types.
 *
 * The raw generated file at `lib/api-types.ts` exposes everything as
 * `components['schemas']['Foo']`, which is verbose at call sites. This
 * file re-exports the schemas we actually use with friendlier names —
 * so downstream code does `import { BacktestRequest } from '../types/api'`
 * instead of `import type { components } from '../api-types'` and then
 * `components['schemas']['BacktestRequest']`.
 *
 * **Rule of thumb**: when you find yourself hand-typing a request body
 * or a server-side response shape, check whether the Pydantic model
 * already exists in `backend/openapi.json` first — if it does, re-export
 * it here and import from this file rather than duplicating the shape.
 *
 * Generator flags worth knowing about:
 *   - `--default-non-nullable=false` (set in `package.json`): fields
 *     with Pydantic defaults are marked optional in TS. Matches how
 *     the frontend builds requests (omit a field → backend fills the
 *     default). Without this flag, every defaulted field becomes
 *     required and TS rejects the partial-construction style.
 */
import type { components } from '../api-types';

// ── Request bodies (POST/PUT bodies the backend accepts) ─────────────
export type BacktestRequest = components['schemas']['BacktestRequest'];
export type VariantSpec = components['schemas']['VariantSpec'];
export type SaveBacktestRequest = components['schemas']['SaveBacktestRequest'];
export type RenameBacktestRequest = components['schemas']['RenameBacktestRequest'];
export type RenameCurrentPicksRequest = components['schemas']['RenameCurrentPicksRequest'];
export type CreateCompanyRequest = components['schemas']['CreateCompanyRequest'];
export type UpdateCompanyRequest = components['schemas']['UpdateCompanyRequest'];
export type ScheduledStrategyCreate = components['schemas']['ScheduledStrategyCreate'];
export type ScheduledStrategyPatch = components['schemas']['ScheduledStrategyPatch'];
export type ScreenRequest = components['schemas']['ScreenRequest'];
export type BuildUniverseRequest = components['schemas']['BuildUniverseRequest'];
export type DeriveUniverseRequest = components['schemas']['DeriveUniverseRequest'];
export type CreateBenchmarkRequest = components['schemas']['CreateBenchmarkRequest'];
export type UpdateBenchmarkRequest = components['schemas']['UpdateBenchmarkRequest'];

// ── Diversifier (correlation + blend analysis) ───────────────────────
export type CorrelationRequest = components['schemas']['CorrelationRequest'];
export type CorrelationResponse = components['schemas']['CorrelationResponse'];
export type DiversifierResult = components['schemas']['DiversifierResult'];
export type DiversifierStrategyStats = components['schemas']['StrategyStats'];
export type ResolveNameResponse = components['schemas']['ResolveNameResponse'];
export type BacktestStats = components['schemas']['BacktestStats'];
export type OptimizeResponse = components['schemas']['OptimizeResponse'];
export type AssetWeight = components['schemas']['AssetWeight'];
export type DrawdownInfo = components['schemas']['DrawdownInfo'];
export type YearStat = components['schemas']['YearStat'];
export type SavedPortfolio = components['schemas']['SavedPortfolio'];
export type PortfolioStateResponse = components['schemas']['PortfolioStateResponse'];
export type HoldingStateInfo = components['schemas']['HoldingStateInfo'];
export type ExchangeFeeIn = components['schemas']['ExchangeFeeIn'];
export type LongEquitySaveUniverseRequest = components['schemas']['LongEquitySaveUniverseRequest'];
export type RecomputeRequest = components['schemas']['RecomputeRequest'];
export type SignalBreakdownRequest = components['schemas']['SignalBreakdownRequest'];
export type IndicatorRequest = components['schemas']['IndicatorRequest'];
export type UniverseRenameRequest = components['schemas']['UniverseRenameRequest'];
export type CreateUserRequest = components['schemas']['CreateUserRequest'];
export type SetRoleRequest = components['schemas']['SetRoleRequest'];
export type ImpersonateRequest = components['schemas']['ImpersonateRequest'];

// Asset pipeline (flat per-ISIN grid)
export type AssetGridRow = components['schemas']['AssetGridRow'];
// GuruFocus dividends, bridged onto the grid by ISIN (only ~13% of rows resolve).
export type DividendCoverageEntry = components['schemas']['DividendCoverageEntry'];
export type DividendSeriesResponse = components['schemas']['DividendSeriesResponse'];
// The live per-payment feed. The fiscal-year series only gains a point when a
// fiscal year closes, so a mid-year dividend hike is invisible for up to a year.
export type DividendPaymentsResponse = components['schemas']['DividendPaymentsResponse'];
// Revenue, in MILLIONS of the LISTING's trading currency — GuruFocus FX-converts
// financials per fiscal period, so a non-home listing reports a different number
// (CSX FY2025: 14,092 USD on Nasdaq vs 12,034.6 EUR on Xetra). The opposite of the
// dividend feed, which reports the declaration currency on every listing.
export type FinancialSeriesResponse = components['schemas']['FinancialSeriesResponse'];

// AIRS model-portfolio positions — the XLS export that DOES carry an ISIN (the AIRS
// holdings sheet only has a fund name, which is why name-matching was never safe).
export type ModelPortfolioPositions = components['schemas']['ModelPortfolioPositions'];
export type ModelPortfolioPosition = components['schemas']['ModelPortfolioPosition'];
export type StoredModelPortfolio = components['schemas']['StoredModelPortfolio'];
export type ModelPortfolioPerformance = components['schemas']['ModelPortfolioPerformance'];
// Pairwise correlation of the listed (>5-holding) models' daily EUR returns — YTD + trailing
// 12m, NxN over portfolio_ids. Null cell = <min_overlap_days common returns. Same return series
// the YTD column is read off, so the matrix cannot disagree with the table above it.
export type PortfolioCorrelationMatrix = components['schemas']['PortfolioCorrelationMatrix'];
// Composition of a model portfolio beside a benchmark's, on ONE set of buckets. Funds are NOT
// looked through — they land in a single "Fund (not looked through)" bucket on every axis,
// because an ETF's listing tells you nothing about what it holds.
export type ModelPortfolioAnalysis = components['schemas']['ModelPortfolioAnalysis'];
// Brinson-Fachler: WHY a model beat or lagged the index. Allocation (the right buckets?) vs
// selection (the right names inside them?) — different mistakes with different fixes. The three
// effects SUM to the excess; `reconciles` carries the proof.
export type ModelPortfolioAttribution = components['schemas']['ModelPortfolioAttribution'];
export type ReconstructedIndex = components['schemas']['ReconstructedIndex'];
export type IndexMember = components['schemas']['IndexMember'];

/** The four soundness charts — price vs fair value, yield, ROIC vs WACC, safety.
 *  One payload off one cached GuruFocus blob + our own daily yfinance price in EUR. */
export type FundamentalsResponse = components['schemas']['FundamentalsResponse'];
export type FundamentalSeries = components['schemas']['FundamentalSeries'];

/** One of the four quality numbers + its verdict (ok | fail | n_a | unknown). */
export type QualityMetric = components['schemas']['QualityMetric'];

/** An AIRS ACCOUNT — what a book actually made, on AIRS's own EUR values.
 *  A different object from a model portfolio (which is a composition of weights). */
export type AirsAccount = components['schemas']['AirsAccount'];
export type AirsAccountDetail = components['schemas']['AirsAccountDetail'];
