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
/** Active share: `½ Σ|wᵖ − wᵇ|` over the book's INDIVIDUAL STOCKS, taken as 100% of the portfolio.
 *  ⚠ A STRUCTURAL measure, not a return one — the size of the bet, not whether it paid. Its
 *  companion `ModelPortfolioAttribution` answers the other question. */
export type ActiveShare = components['schemas']['ActiveShare'];
/** ⚠ ONE ISSUER, not one holding — two share classes fold into one row with their weights summed,
 *  so this list is legitimately shorter than the Holdings table. See `_active_share._issuer_key`. */
export type ActiveShareRow = components['schemas']['ActiveShareRow'];
/** REALISED (ex-post) tracking error: the annualised volatility of the active return.
 *  ⚠ NOT ex-ante — there is no covariance forecast here, and the two routinely disagree, so every
 *  label says "realised". ⚠ And it is the spread of the active return, never the active return
 *  itself: a book can have a large tracking error and no excess at all. */
export type TrackingError = components['schemas']['TrackingError'];
/** Correlation as a RISK measure: ρ to the benchmark (the other side of the tracking error, via
 *  `σₐ² = σₚ² + σᵇ² − 2ρσₚσᵇ`) and ρ between the positions (the diversification check).
 *  ⚠ NOT attribution — that DECOMPOSES the active return into terms that sum to it, and
 *  correlation appears in none of them. They are separate panels on purpose. */
export type RiskCorrelation = components['schemas']['RiskCorrelation'];
/** σ of the stock sleeve's OWN returns, plus its downside half.
 *  ⚠ THE SAME `σₚ` the correlation view puts inside `σₐ² = σₚ² + σᵇ² − 2ρσₚσᵇ` — one series,
 *  one function. ⚠ No cash-flow contamination BY CONSTRUCTION: it is a weighted basket of
 *  instrument returns, not an account value, so there are no flows in it to chain-link out. */
export type PortfolioVolatility = components['schemas']['PortfolioVolatility'];
/** Max drawdown of the RECONSTRUCTED sleeve — `MDD = min(Wₜ/Mₜ − 1)` over today's holdings.
 *  ⚠⚠ NOT the client's realised drawdown: look-ahead bias (today's weights, chosen with
 *  hindsight) and survivorship bias (names since sold are absent). The client's own figure comes
 *  from the AIRS returns. ⚠ Defaults to DAILY — unlike the other risk views, which default to
 *  weekly to dodge a two-series closing-time bias that a drawdown cannot have. */
export type PortfolioDrawdown = components['schemas']['PortfolioDrawdown'];
/** The paired book's cumulative return through the year (`returns`, 0% on `return_from`) and its
 *  value on every date we hold a snapshot for (`points`) — see `BookReturnChart`.
 *  ⚠⚠ TWO QUANTITIES, ONE OF THEM PERFORMANCE. `returns` is AIRS's own flow-aware
 *  `cumulatief_rendement`, read not derived; `points` is VALUE, which a funding moves — so
 *  `flows` rides along and a return is never computed from two of them. */
export type BookValueSeries = components['schemas']['BookValueSeries'];
/** `C₁₀ = Σ w₍ᵢ₎` and `HHI = Σ wᵢ²`, with `N_eff = 1/HHI`.
 *  ⚠ ON ISSUERS, not lines — the same folding `ActiveShare` uses, so the two views cannot
 *  disagree about how many positions the book holds. ⚠ BOTH denominators are returned (of the
 *  stock sleeve, and of the whole book including cash) because the choice changes the number. */
export type PortfolioConcentration = components['schemas']['PortfolioConcentration'];
/** Effective positions — `Eᵢ = qᵢ·Pᵢ·Xᵢ` — per issuer, plus the currency split.
 *  ⚠⚠ WE DO NOT COMPUTE THAT PRODUCT: `Eᵢ` is AIRS's own `current_value_eur`, the figure on the
 *  client's statement. A second derivation from our close and our FX would disagree with it on most
 *  rows with nothing able to say which was right. ⚠ Currency is the LISTING's — the exposure borne,
 *  not the one the company earns in. */
export type PortfolioExposure = components['schemas']['PortfolioExposure'];
export type ReconstructedIndex = components['schemas']['ReconstructedIndex'];
/** Per-constituent Long Equity measures for a benchmark — a second, slower call than the index
 *  itself, so the price table renders first and these fill in. */
export type ConstituentFundamentals = components['schemas']['ConstituentFundamentals'];
/** The VALUES behind `ConstituentFundamentals`' spans: every constituent x every line x every
 *  period, in EUR, with the period's own market cap so a cross-section can be weighted.
 *  ⚠ `cadence: 'quarterly'` is TRAILING TWELVE MONTHS, not the raw quarter — both slider axes are
 *  12-month figures, so moving the quarter changes the as-of date and never the unit. */
export type FundamentalGrid = components['schemas']['FundamentalGrid'];
export type FundamentalGridColumn = components['schemas']['FundamentalGridColumn'];
export type FundamentalGridRow = components['schemas']['FundamentalGridRow'];
export type FundamentalGridPeriod = components['schemas']['FundamentalGridPeriod'];
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
export type AirsPortfolioOverview = components['schemas']['AirsPortfolioOverview'];
export type AirsAccountIsins = components['schemas']['AirsAccountIsins'];
/** One account's AIRS Transacties, as the SHEET — `columns` are DATA, not a contract. No column of
 *  the TRANS report has been measured yet, so the backend imposes no schema on it; see
 *  `backend/airs_transacties.py` for why guessing one is the failure to avoid. */
export type AirsAccountTransactions = components['schemas']['AirsAccountTransactions'];
/** A book's year built from its positions — held AND sold — set against AIRS's own figure.
 *  ⚠ `realised_ytd_eur` of null is NOT zero: it means no Transacties sheet is cached, so the
 *  realised leg is unknown and there is no total to show. */
export type AirsAccountReconciliation = components['schemas']['AirsAccountReconciliation'];
/** One holding's year split into buy-and-hold + the effect of each trade. ⚠ `actual_eur` is the
 *  ECONOMIC result and is not the table's `Result` column — see `restatement_eur`. */
export type HoldingTiming = components['schemas']['HoldingTiming'];
export type AirsHoldingSegment = components['schemas']['AirsHoldingSegment'];
export type AirsHoldingIsin = components['schemas']['AirsHoldingIsin'];
export type AirsAccountModelLink = components['schemas']['AirsAccountModelLink'];
export type AirsAccountModelLinks = components['schemas']['AirsAccountModelLinks'];
export type AirsModelChoice = components['schemas']['AirsModelChoice'];
