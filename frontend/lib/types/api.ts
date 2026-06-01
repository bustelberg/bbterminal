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
export type UpdateBenchmarkSectorRequest = components['schemas']['UpdateBenchmarkSectorRequest'];
export type ExchangeFeeIn = components['schemas']['ExchangeFeeIn'];
export type LongEquitySaveUniverseRequest = components['schemas']['LongEquitySaveUniverseRequest'];
export type RecomputeRequest = components['schemas']['RecomputeRequest'];
export type SignalBreakdownRequest = components['schemas']['SignalBreakdownRequest'];
export type IndicatorRequest = components['schemas']['IndicatorRequest'];
export type UniverseRenameRequest = components['schemas']['UniverseRenameRequest'];
export type CreateUserRequest = components['schemas']['CreateUserRequest'];
export type SetRoleRequest = components['schemas']['SetRoleRequest'];
export type ImpersonateRequest = components['schemas']['ImpersonateRequest'];
