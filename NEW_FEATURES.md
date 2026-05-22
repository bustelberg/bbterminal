# NEW_FEATURES.md

Spec for the eight new features that landed in uncommitted work and are about
to be reverted. Use this to re-implement deliberately, one feature at a time.

## Where to find the verbatim source

Two reference artifacts are deliberately preserved across the revert so you
can cross-check anything the prose summary glosses over:

- **`.uncommitted.diff`** — `git diff` of every TRACKED change. Use this when
  re-implementing JSX (the spec describes structure + class intent in prose;
  the diff has the exact Tailwind classes), Pydantic models, and Python
  endpoint bodies.
- **`.uncommitted-newfiles/`** — verbatim copies of the two NEW (untracked)
  files. `git diff` does not include untracked files, so without this backup
  they'd be lost on revert:
  - `SavedRunsDropdown.tsx` — full Feature 7 component (~290 lines).
  - `20260522050000_company_gurufocus_lookup_failed_at.sql` — full Feature 1 migration.

After reverting (note: the two `.uncommitted-*` paths are kept):

```powershell
git checkout -- .
Remove-Item frontend/app/components/momentum/SavedRunsDropdown.tsx
Remove-Item supabase/migrations/20260522050000_company_gurufocus_lookup_failed_at.sql
# DO NOT delete .uncommitted.diff or .uncommitted-newfiles/ — keep them as
# reference until re-implementation is finished. Delete both at the end.
```

Suggested implementation order (each is independent except where noted):

1. Feature 1 — GuruFocus lookup-failed tracking (migration first, then ingest, then UI).
2. Feature 2 — CompanyManager multi-select filters (purely frontend; uncoupled from 1).
3. Feature 3 — AirSPMS IP-forbidden classification (small, isolated).
4. Feature 4 — Sortino / win rate / median (backend + small frontend display changes).
5. Feature 5 — Universe equal-weight overlay (frontend only; consumes existing field).
6. Feature 6 — Variants cross-product sweep (largest; touches the JSX that broke).
7. Feature 7 — SavedRunsDropdown extraction (uses the `config` field added to /api/momentum/backtests).
8. Feature 8 — Template universes card + auto-bootstrap.

Features 6 and 7 are the ones whose careless interleaving in `MomentumBacktester.tsx`
caused the parse error. Implement them in two clean passes, not at once.

---

## Feature 1 — GuruFocus lookup-failed tracking + diagnostic endpoint

### Database

New column on `company` (full migration body below — paste verbatim):

```sql
-- supabase/migrations/<TIMESTAMP>_company_gurufocus_lookup_failed_at.sql

-- `company.gurufocus_lookup_failed_at` — last timestamp at which the
-- GuruFocus price/volume ingest got "Stock not found" for this company's
-- (gurufocus_ticker, exchange) pair AFTER trying every entry in the
-- exchange fallback list. NULL = lookups working (or never attempted).
--
-- Distinct from `delisted_at`:
--   * delisted_at      = real listing existed once, GuruFocus now reports
--                        "Delisted stocks are available for Professional
--                        plan" (a paywall, but the listing was real).
--   * gurufocus_lookup_failed_at = (ticker, exchange) doesn't resolve at
--                        all on GuruFocus. Usually the row's exchange is
--                        wrong (e.g. NYSE:ASND when the listing is actually
--                        NASDAQ:ASND), or the ticker symbol is stale.
--
-- The /companies UI surfaces non-null rows with a red badge so the user
-- can investigate before the next backtest fires the same "N companies
-- have NO price data" warning.

ALTER TABLE company
  ADD COLUMN IF NOT EXISTS gurufocus_lookup_failed_at TIMESTAMPTZ NULL;

-- Partial index — only the rows we'd ever want to surface are the
-- non-null ones (a tiny fraction of the table). Cuts index size vs. a
-- full-column index.
CREATE INDEX IF NOT EXISTS company_gurufocus_lookup_failed_at_idx
  ON company (gurufocus_lookup_failed_at)
  WHERE gurufocus_lookup_failed_at IS NOT NULL;

NOTIFY pgrst, 'reload schema';
```

### Backend

**`backend/ingest/prices.py` (`ensure_prices_for_company`)** — after the fallback
chain has been exhausted and the response was "stock not found", stamp the
column; after a successful load (price rows inserted), clear it. Both wrapped
in `try/except` so write failures degrade gracefully:

```python
# After the data is None / api_log check, but BEFORE the stale-cache fallback:
if data is None and api_log and "stock not found" in api_log.lower():
    try:
        import datetime as _dt
        supabase.table("company").update(
            {"gurufocus_lookup_failed_at": _dt.datetime.now(_dt.timezone.utc).isoformat()}
        ).eq("company_id", company_id).execute()
    except Exception as e:
        logging.getLogger(__name__).warning(
            "[ensure_prices] failed to stamp lookup_failed cid=%s: %s: %s",
            company_id, type(e).__name__, e,
        )

# Right before `return result` at the end of the function (after rows_loaded):
try:
    supabase.table("company").update(
        {"gurufocus_lookup_failed_at": None}
    ).eq("company_id", company_id).not_.is_("gurufocus_lookup_failed_at", "null").execute()
except Exception as e:
    logging.getLogger(__name__).warning(
        "[ensure_prices] failed to clear lookup_failed cid=%s: %s: %s",
        company_id, type(e).__name__, e,
    )
```

**`backend/routers/companies.py` (`list_companies`)** — include the new column in
the select string so `/api/companies` returns it. Just change the inner select:

```python
.select(
    "company_id,company_name,gurufocus_ticker,exchange_id,"
    "delisted_at,gurufocus_lookup_failed_at,"
    "gurufocus_exchange:gurufocus_exchange("
    "exchange_code,country:country(country_name))"
)
```

**`backend/routers/admin.py`** — new endpoint
`POST /api/admin/gurufocus-exchange-search`. Takes `{tickers: [{ticker, current_exchange?}], candidate_exchanges?: string[]}`,
probes the GF price endpoint for each candidate exchange (current_exchange first
when given), returns per-ticker `{ticker, current_exchange, found_exchange, status: 'found'|'not_found', candidates_tried: [...]}`.
Admin auth required (`_require_admin`).

Key implementation points:
- Default candidates: `["NAS","NYSE","AMEX","OTCBB","XTER","XPAR","AMS","OBOM","MIL","MAD","WBO","STO","OSL","HEL","CSE","LSE","SWX","TSE","HKEX","KSE","BSE","NSE","SGX","TPE"]` (matches `FEASIBLE_GF_EXCHANGES`).
- Symbol building mirrors `ingest.prices._build_symbol`: US exchanges (`NAS/NASDAQ/NYSE/AMEX/CBOE`) use just the ticker; everything else uses `{EXCH}:{TICKER}`.
- Uses `ingest._gurufocus_http.cf_get`. Hit `{base_url}/public/user/{api_key}/stock/{symbol}/price`. `base_url` derived from `GURUFOCUS_BASE_URL`, stripping trailing `/data`.
- Treats a 200 response whose body starts with `[` as the positive signal (GF sometimes returns 200 with an error string).
- Returns `await asyncio.to_thread(_q)` since `cf_get` is sync.
- Pydantic models: `_GuruFocusExchangeSearchRequest`, `_GuruFocusExchangeSearchBody`.

### Frontend

**`frontend/lib/hooks/apiData.ts`** — extend `CompanyRow` type:

```ts
gurufocus_lookup_failed_at?: string | null;
```

**`frontend/app/components/CompanyManager.tsx`**:

1. Add the same field to the local `Company` type.
2. New `findCorrectExchange(c: Company)` method — POSTs to `/api/admin/gurufocus-exchange-search` with `[{ticker, current_exchange}]`, parses the single result. If `status === 'found' && found_exchange !== current_exchange`, shows a confirm dialog ("Update the row's exchange to X?") that on accept calls `handleSave(c.company_id, { gurufocus_exchange: r.found_exchange })`. If found but already matches, friendly "should clear on next ingest" alert. If not_found, alert with the tried list.
3. Render a red "GF LOOKUP" button next to the row's name when `c.gurufocus_lookup_failed_at && !c.delisted_at`. Click triggers `findCorrectExchange(c)`. Tooltip includes the timestamp.

---

## Feature 2 — CompanyManager multi-select filters

Pure frontend, in `frontend/app/components/CompanyManager.tsx`.

Replace the three single-select `<select>` filters (Exchange / Country / Universe)
with a new internal component `MultiSelectFilter` (a search-filterable checkbox
dropdown).

**Combine semantics shown in the UI:**
- Exchange + Country → `combineMode="OR"` (a company has exactly one of each).
- Universe → `combineMode="AND"` (intersection of memberships, e.g. ACWI ∩ LEONTEQ).

The `combineMode` chip is purely cosmetic — actual AND/OR is applied in the
caller's `filteredCompanies` useMemo.

**State changes:**
- `filterExchange`, `filterCountry`, `filterUniverse` go from `string` → `string[]`, all starting `[]`.
- Filter applications:
  - Exchange: `list = list.filter((c) => filterExchange.includes(c.gurufocus_exchange))` when `length > 0`.
  - Country: same with `c.country != null && filterCountry.includes(c.country)`.
  - Universe: `filterUniverse.every((u) => (c.universes ?? []).includes(u))` — AND.
- "Clear filters" pill checks `length > 0` on each array.
- Universe chip on each row toggles inclusion: `setFilterUniverse((cur) => (cur.includes(u) ? cur.filter((x) => x !== u) : [...cur, u]))`.

**`MultiSelectFilter` component skeleton (props):**

```ts
function MultiSelectFilter({
  label,
  options,
  selected,
  onChange,
  combineMode,
}: {
  label: string;
  options: string[];
  selected: string[];
  onChange: (next: string[]) => void;
  combineMode?: 'AND' | 'OR';
}) { /* … */ }
```

Behavior:
- Button trigger: shows "All <label>" when empty, comma-joined when ≤2 selected, otherwise "N <label>". Indigo border + text when any selected.
- Popover (uses `useClickOutside` + `useEscapeKey`): header row with label + optional combineMode chip + "Clear" button when any selected.
- Search input shown only when `options.length > 8`.
- Checkbox list with `accent-indigo-500`, text white when checked else gray-300.
- `max-h-80` + scrollable; "No matches" placeholder.

---

## Feature 3 — AirSPMS IP-forbidden classification

### Backend (`backend/airs_scanner.py`)

Add a discriminated exception class + a sniff helper:

```python
class AirsAccessForbiddenError(RuntimeError):
    """AirSPMS returned a 403 Forbidden / bot-block page on the login URL.
    Almost always means this server's egress IP is not on AirSPMS's allowlist.
    Distinct from a generic login failure so the SSE error event can carry a
    `kind` discriminator and the frontend can render IP-whitelist guidance."""
    def __init__(self, detail: str):
        super().__init__("AirSPMS blocked the login request with 403 Forbidden")
        self.detail = detail


def _looks_forbidden(diag: str) -> bool:
    lower = diag.lower()
    return (
        "403 forbidden" in lower
        or "'forbidden'" in lower
        or "'access denied'" in lower
    )
```

In `_login(page)`, when the `#username` fill fails, run `_capture_login_diagnostics`
as today, then if `_looks_forbidden(diag)`, `raise AirsAccessForbiddenError(diag) from e`
*before* the generic `RuntimeError`.

In `scan_portfolios_sync`'s `try/except` chain, add a clause **before** the
catch-all `Exception`:

```python
except AirsAccessForbiddenError as e:
    send_event(
        "error",
        kind="ip_forbidden",
        message=(
            "AirSPMS responded with HTTP 403 Forbidden. This server's "
            "outbound IP address is likely not on the AirSPMS allowlist "
            "— ask your AirSPMS administrator to whitelist it, or wait "
            "and retry if the egress IP just rotated."
        ),
        detail=e.detail,
    )
    return []
```

### Frontend

**`frontend/lib/stores/airsScan.ts`** — extend `AirsScanState`:

```ts
export type AirsScanErrorKind = 'ip_forbidden';
// in state:
errorKind: AirsScanErrorKind | null;
errorDetail: string | null;
```

Initial state sets both `null`. `startAirsScan` resets both. The SSE `error`
handler reads `data.kind` + `data.detail`, only sets `errorKind` to
`'ip_forbidden'` when `data.kind === 'ip_forbidden'`, else null. `es.onerror`
("Connection lost") resets kind/detail to null.

**`frontend/app/components/AirsPortfolioUpload.tsx`** — read `errorKind` +
`errorDetail` from the store. The `setError(null)` helper resets all three.
Replace the single error banner with two branches:

- `error && errorKind === 'ip_forbidden'`: bespoke banner with title
  "AirSPMS access denied (403 Forbidden)", body text, and a `<details>`
  block with `summary="Show technical details"` rendering `errorDetail` in a
  monospace `<pre>`.
- `error && errorKind !== 'ip_forbidden'`: the existing generic red banner.

---

## Feature 4 — Sortino + win rate + median period return

### Backend

**`backend/momentum/backtest/types.py`** — extend `BacktestSummary` with three
optional fields (must come AFTER the existing required fields because dataclass
default-ordering — give the existing trailing fields explicit defaults too so the
class stays valid):

```python
sortino_ratio: float | None = None
win_rate_pct: float | None = None
median_period_return_pct: float | None = None
avg_monthly_turnover_pct: float = 0.0
total_months: int = 0
avg_holdings: float = 0.0
```

`BacktestResult.dump()` must include all three in the `summary` dict.

**`backend/momentum/backtest/_summary.py`** — in `build_backtest_result`, after
the Sharpe computation:

```python
sortino = None
# In the same `if len(closed_daily_returns) >= 21:` block:
arr = np.array(closed_daily_returns)
downside = arr[arr < 0]
if len(downside) > 1:
    downside_std = float(downside.std())
    if downside_std > 0:
        sortino = round((d_mean / downside_std) * (252 ** 0.5), 2)

# In the period-frequency `elif` fallback, same shape but using `period_mean` and
# `_periods_per_year(rebalance_frequency)`:
arr = np.array(accumulators.all_period_returns)
downside = arr[arr < 0]
if len(downside) > 1:
    downside_std = float(downside.std())
    if downside_std > 0:
        sortino = round((period_mean / downside_std) * (_periods_per_year(rebalance_frequency) ** 0.5), 2)

# After both Sharpe/Sortino computations, win rate + median (use period returns
# not daily — "win rate" lines up with rebalance cadence):
win_rate_pct = None
median_period_return_pct = None
if accumulators.all_period_returns:
    pr_arr = np.array(accumulators.all_period_returns)
    wins = int((pr_arr > 0).sum())
    total = int(pr_arr.size)
    if total > 0:
        win_rate_pct = round(100.0 * wins / total, 2)
    median_period_return_pct = round(float(np.median(pr_arr)), 2)
```

Pass all three into the `BacktestSummary(...)` constructor call.

**`backend/momentum/backtest/runner.py` (`run_multi_trial_backtest`)** — aggregate
the new fields via `_mean_std` and pass them into the synthesized
`BacktestSummary`:

```python
sortino_mean, _sortino_std = _mean_std("sortino_ratio")
win_mean, _win_std = _mean_std("win_rate_pct")
median_mean, _median_std = _mean_std("median_period_return_pct")
# ...
summary = BacktestSummary(
    ...,
    sharpe_ratio=sharpe_mean,
    sortino_ratio=sortino_mean,
    win_rate_pct=win_mean,
    median_period_return_pct=median_mean,
    ...
)
```

### Frontend

**`frontend/lib/stores/momentum.ts`** — extend `Summary`:

```ts
sortino_ratio?: number | null;
win_rate_pct?: number | null;
median_period_return_pct?: number | null;
```

**`frontend/app/components/momentum/equityCurve/SummaryStats.tsx`** — inside the
"Strategy (full range)" line below the aligned-stats table, conditionally
render three more spans when the field is non-null. Each wraps a `font-mono`
value; the median span colorizes emerald/rose. Tooltips:
- Sortino: "like Sharpe but only penalizes downside vol (std of negative daily returns × √252). Higher than Sharpe → upside vol dominates."
- Win rate: "% of closed periods with strictly positive return."
- Median period: "Median of closed-period returns. Far below the headline mean → return is carried by a few outlier months."

**`frontend/app/components/momentum/VariantSummaryTable.tsx`** — add three new
columns between Sharpe and Total return:

- Column header `Sortino` (title="Same as Sharpe but volatility only counts negative daily returns. Higher than Sharpe = upside vol dominates the variance.").
- Column header `Win rate` (title="% of closed periods with strictly positive return").
- Column header `Median period` (title="Median of closed-period returns. Far below the mean → headline return is carried by a few outlier months.").

`SummaryCells` placeholders for non-OK rows go from 6 (`[0..5]`) → 9 (`[0..8]`).
The export-rows + export-columns arrays get matching `sortino`, `win_rate_pct`,
`median_period_return_pct` entries.

---

## Feature 5 — Universe equal-weight baseline overlay on the equity curve

Pure frontend (consumes the existing `monthly_records[i].universe_cumulative_return_pct`
field, which already ships from the backend — verify before re-implementing).

**`frontend/app/components/momentum/equityCurve/seriesMath.ts`** — new helper:

```ts
export function seriesFromUniverseBaseline(
  monthly: PeriodRecord[],
): { map: Map<string, number>; months: string[] } {
  const map = new Map<string, number>();
  const months: string[] = [];
  for (const r of monthly) {
    const v = r.universe_cumulative_return_pct;
    if (v == null) continue;
    const key = endOfMonth(r.date);
    map.set(key, 1 + v / 100);
    months.push(key);
  }
  months.sort();
  return { map, months };
}
```

In `resolveSeries(...)`, immediately after the active strategy series is pushed
into `out`, also push a non-removable gray "Universe (equal-weight)" series when
the baseline map is non-empty:

```ts
const universe = seriesFromUniverseBaseline(result.monthly_records);
if (universe.map.size > 0) {
  out.push({
    id: 'universe',
    label: 'Universe (equal-weight)',
    color: '#9ca3af', // gray-400
    kind: 'benchmark',
    removable: false,
    factorByMonth: universe.map,
    months: universe.months,
  });
}
```

---

## Feature 6 — Variants cross-product sweep (5 axes)

### Concept

Replace the popover-style "pick which legacy 2-segment variants to run" with a
permanent inline panel that lets the user sweep five axes simultaneously, taking
the cross-product:

1. **Frequency**: multi-select over `RebalanceFrequency`.
2. **Strategy**: multi-select over `StrategyType` (long_only / long_short).
3. **Universe**: multi-select over `indexUniverses[].index_name`.
4. **Grouping**: multi-select over `sector | industry`.
5. **Top sectors / Per sector / Min price score**: three comma-separated text
   inputs. Empty axis = "inherit base, don't sweep". For min price score, the
   literal `none` / `off` means "filter disabled for this variant".

Below the axes, a permutations preview list shows every cross-product variant
with a per-row enable checkbox (so the user can carve out specific combos).
The Run button uses `eligibleCount = totalPerms - userDisabled - blockedByMode`.

The old top-row inputs for Top Sectors / Per Sector / Group By / Min Price Score
go away — those dials are now per-variant via the sweep axes. The base-value
state (`topSectors`, `topPerSector`, `grouping`, `minPriceScore`) is **kept**
in component state and still drives single Run-backtest calls (with their
historical defaults).

The legacy universe `<select>` dropdown at the top is also gone — the variants
picker's universe column is now the single source of truth. The single-run
`startBacktest` should derive its `index_universe` + `grouping` from the first
selected variant (since base config doesn't carry these inputs anymore).

### Backend

**`backend/routers/momentum/backtest_stream/models.py`** — extend `VariantSpec`:

```python
class VariantSpec(BaseModel):
    frequency: Literal[ ... ]  # unchanged
    strategy_type: Literal["long_only", "long_short"]
    # All None = "inherit base":
    top_n_sectors: int | None = None
    top_n_per_sector: int | None = None
    min_price_score: float | None = None
    universe_label: str | None = None
    index_universe: str | None = None
    grouping: Literal["sector", "industry"] | None = None
```

**`backend/routers/momentum/backtest_stream/universe_loader.py`** — split the
`load_monthly_eligible(req)` async generator into a decoupled triple-arg
version + a back-compat wrapper:

```python
async def load_monthly_eligible_for(
    universe_label: str | None,
    index_universe: str | None,
    grouping_field: str,
    *,
    require_universe: bool = True,
):
    """Same body as the old load_monthly_eligible, but takes the inputs
    as explicit args instead of pulling them off `req`. Final yield is
    the sentinel ('__result__', monthly_eligible_or_None, did_error)."""
    if grouping_field not in ("sector", "industry"):
        grouping_field = "sector"
    # ... same logic as before, just substitute universe_label / index_universe
    # for req.universe_label / req.index_universe, and use `require_universe`
    # instead of `(req.top_n_sectors or 0) > 0` for the final fail-when-None
    # check.


async def load_monthly_eligible(req):
    """Back-compat wrapper around load_monthly_eligible_for. Used by the
    single-run path."""
    grouping_field = getattr(req, "grouping", "sector") or "sector"
    require = (req.top_n_sectors or 0) > 0
    async for evt in load_monthly_eligible_for(
        req.universe_label, req.index_universe, grouping_field,
        require_universe=require,
    ):
        yield evt
```

**`backend/routers/momentum/backtest_stream/stream.py`** — in
`_momentum_backtest_stream`, replace the single `load_monthly_eligible(req)`
call with a per-combo loop:

```python
base_grouping = (getattr(req, "grouping", None) or "sector")
base_combo = (req.universe_label, req.index_universe, base_grouping)
combos = [base_combo]
seen = {base_combo}
for v in (req.variants or []):
    c = (
        v.universe_label if v.universe_label is not None else req.universe_label,
        v.index_universe if v.index_universe is not None else req.index_universe,
        (v.grouping if v.grouping is not None else base_grouping),
    )
    if c not in seen:
        combos.append(c); seen.add(c)

require_universe = (req.top_n_sectors or 0) > 0
monthly_eligible_by_combo: dict[tuple, dict] = {}
for combo in combos:
    u_label, u_idx, grp = combo
    me = None
    did_err = False
    async for evt in load_monthly_eligible_for(u_label, u_idx, grp, require_universe=require_universe):
        if isinstance(evt, tuple) and evt[0] == "__result__":
            _, me, did_err = evt
            continue
        yield evt
    if did_err:
        return
    if me is not None:
        monthly_eligible_by_combo[combo] = me

monthly_eligible = monthly_eligible_by_combo.get(base_combo)

# Union over all combos — drives the shared signal-panel build:
union_monthly_eligible: dict = {}
for me in monthly_eligible_by_combo.values():
    for month, cmap in me.items():
        tgt = union_monthly_eligible.setdefault(month, {})
        for cid, v in cmap.items():
            if cid not in tgt:
                tgt[cid] = v

if not union_monthly_eligible and require_universe:
    yield _emit({"type": "error", "message": "No universe selected. ..."})
    return

# Optional progress message when there's more than one combo:
if combos[1:] and union_monthly_eligible:
    n_uniq = len(monthly_eligible_by_combo)
    cids = set()
    for me in monthly_eligible_by_combo.values():
        for cmap in me.values():
            cids.update(cmap.keys())
    yield _emit({"type": "progress", "pct": 7, "message": f"Multi-universe sweep: {n_uniq} unique (universe, grouping) combos -> union of {len(cids):,} companies will be priced once and reused across variants."})
```

The universe_df filtering should now use `union_monthly_eligible` (loops
`union_monthly_eligible.values()` for `eligible_ids`) instead of
`monthly_eligible`.

The call into `run_variants_sweep` gets two extra kwargs:
`monthly_eligible_by_combo=monthly_eligible_by_combo`,
`union_monthly_eligible=union_monthly_eligible`.

**`backend/routers/momentum/backtest_stream/variants.py`** —
`run_variants_sweep` signature accepts the two new kwargs as keyword-only.
The shared signal-panel build (`build_shared_backtest_inputs(...)`) passes
`monthly_eligible=(union_monthly_eligible if union_monthly_eligible else monthly_eligible)`.

Inside the per-variant loop, resolve effective dials + the per-variant monthly_eligible:

```python
base_grouping = getattr(req, "grouping", "sector") or "sector"
for v_idx, vspec in enumerate(req.variants):
    v_top_sectors = vspec.top_n_sectors if vspec.top_n_sectors is not None else req.top_n_sectors
    v_top_per_sector = vspec.top_n_per_sector if vspec.top_n_per_sector is not None else req.top_n_per_sector
    v_min_score = vspec.min_price_score if vspec.min_price_score is not None else req.min_price_score
    v_universe_label = vspec.universe_label if vspec.universe_label is not None else req.universe_label
    v_index_universe = vspec.index_universe if vspec.index_universe is not None else req.index_universe
    v_grouping_field = vspec.grouping if vspec.grouping is not None else base_grouping
    v_monthly_eligible = monthly_eligible
    if monthly_eligible_by_combo is not None:
        v_combo = (v_universe_label, v_index_universe, v_grouping_field)
        v_monthly_eligible = monthly_eligible_by_combo.get(v_combo, monthly_eligible)

    # Extended variant_key — only include a dial when vspec overrode it,
    # so legacy 2-segment keys keep their old form:
    key_parts = [vspec.frequency, vspec.strategy_type]
    if vspec.top_n_sectors is not None: key_parts.append(f"s{vspec.top_n_sectors}")
    if vspec.top_n_per_sector is not None: key_parts.append(f"p{vspec.top_n_per_sector}")
    if vspec.min_price_score is not None:
        score_str = f"{vspec.min_price_score:g}" if isinstance(vspec.min_price_score, float) else str(vspec.min_price_score)
        key_parts.append(f"m{score_str}")
    if vspec.index_universe is not None:
        key_parts.append(f"u{vspec.index_universe}")
    elif vspec.universe_label is not None:
        key_parts.append(f"u{vspec.universe_label}")
    if vspec.grouping is not None:
        key_parts.append(f"g{vspec.grouping}")
    variant_key = "__".join(key_parts)

    # Build v_config using v_top_sectors / v_top_per_sector / v_min_score
    # instead of req.top_n_sectors / req.top_n_per_sector / req.min_price_score.
    # Pass me=v_monthly_eligible into the closure that runs run_backtest /
    # run_multi_trial_backtest.
```

`_v_run(cfg=v_config, prepared=v_prepared, me=v_monthly_eligible)` — the runner
calls use `monthly_eligible=me` instead of the captured shared one.

### Frontend store (`frontend/lib/stores/momentum.ts`)

```ts
// Loosen the strict template literal:
export type VariantKey = string;

export type VariantParams = {
  frequency: RebalanceFrequency;
  strategy: StrategyType;
  top_n_sectors?: number;
  top_n_per_sector?: number;
  min_price_score?: number | null;  // null = "off for this variant"; undefined = "inherit"
  universe?: string;                 // index_universe value
  grouping?: 'sector' | 'industry';
};

export function makeVariantKey(p: VariantParams): VariantKey {
  const parts: string[] = [p.frequency, p.strategy];
  if (p.top_n_sectors != null) parts.push(`s${p.top_n_sectors}`);
  if (p.top_n_per_sector != null) parts.push(`p${p.top_n_per_sector}`);
  if (p.min_price_score != null) parts.push(`m${p.min_price_score}`);
  if (p.universe != null) parts.push(`u${p.universe}`);
  if (p.grouping != null) parts.push(`g${p.grouping}`);
  return parts.join('__');
}

export function parseVariantKey(key: VariantKey): VariantParams | null {
  const parts = key.split('__');
  if (parts.length < 2) return null;
  const out: VariantParams = {
    frequency: parts[0] as RebalanceFrequency,
    strategy: parts[1] as StrategyType,
  };
  for (let i = 2; i < parts.length; i++) {
    const seg = parts[i];
    const tag = seg[0]; const rest = seg.slice(1);
    if (tag === 's') { const n = Number(rest); if (Number.isFinite(n)) out.top_n_sectors = n; }
    else if (tag === 'p') { const n = Number(rest); if (Number.isFinite(n)) out.top_n_per_sector = n; }
    else if (tag === 'm') { const n = Number(rest); if (Number.isFinite(n)) out.min_price_score = n; }
    else if (tag === 'u') { out.universe = rest; }
    else if (tag === 'g') { if (rest === 'sector' || rest === 'industry') out.grouping = rest; }
  }
  return out;
}

export function variantLabel(p: VariantParams): string {
  const freqLabel = (
    p.frequency === 'daily' ? 'Daily' :
    p.frequency === 'weekly' ? 'Weekly' :
    p.frequency === 'monthly' ? 'Monthly' :
    p.frequency.replace(/^every_(\d+)_months$/, 'Every $1 months')
  );
  const stratLabel = p.strategy === 'long_only' ? 'Long-only' : 'Long-short';
  const parts: string[] = [freqLabel, stratLabel];
  if (p.universe != null) parts.push(p.universe);
  if (p.grouping != null) parts.push(p.grouping === 'industry' ? 'by industry' : 'by sector');
  const bucketSingular = p.grouping === 'industry' ? 'industry' : 'sector';
  const bucketPlural = p.grouping === 'industry' ? 'industries' : 'sectors';
  if (p.top_n_sectors != null) parts.push(`top ${p.top_n_sectors} ${p.top_n_sectors === 1 ? bucketSingular : bucketPlural}`);
  if (p.top_n_per_sector != null) parts.push(`${p.top_n_per_sector} per ${bucketSingular}`);
  if (p.min_price_score != null) parts.push(`min ${p.min_price_score}`);
  return parts.join(' · ');
}
```

`BacktestStartConfig.variants` array entries gain optional `top_n_sectors`,
`top_n_per_sector`, `min_price_score`, `universe_label`, `index_universe`,
`grouping` fields.

`startVariantsBacktest` signature changes:
- Old: `(base, keys?: readonly VariantKey[])` → resolved against VARIANT_DEFS.
- New: `(base, variants: VariantParams[])` — required. Caller already filtered.
- Initial pending state: `for (const v of targets) initial[makeVariantKey(v)] = { status: 'pending' };`
- Outbound `variants` payload: `targets.map((v) => ({ frequency: v.frequency, strategy_type: v.strategy, top_n_sectors, top_n_per_sector, min_price_score, index_universe: v.universe, grouping }))`.
- Error/cancel finalization keys variants by `makeVariantKey(v)` instead of `v.key`.

### Frontend UI (`frontend/app/components/MomentumBacktester.tsx`)

State for the inline picker:

```ts
const ALL_FREQS = useMemo(() => Array.from(new Set(VARIANT_DEFS.map((v) => v.frequency))), []);
const ALL_STRATEGIES = useMemo(() => Array.from(new Set(VARIANT_DEFS.map((v) => v.strategy))), []);
const [selectedFreqs, setSelectedFreqs] = useState<Set<RebalanceFrequency>>(
  () => new Set<RebalanceFrequency>(['monthly', 'every_3_months']),
);
const [selectedStrategies, setSelectedStrategies] = useState<Set<StrategyType>>(
  () => new Set<StrategyType>(['long_only']),
);
const [selectedUniverses, setSelectedUniverses] = useState<Set<string>>(
  () => new Set<string>(['ACWI_LEONTEQ']),
);
const [selectedGroupings, setSelectedGroupings] = useState<Set<'sector' | 'industry'>>(
  () => new Set<'sector' | 'industry'>(['sector']),
);
const [topSectorsSweep, setTopSectorsSweep] = useState<string>('');
const [perSectorSweep, setPerSectorSweep] = useState<string>('');
const [minScoreSweep, setMinScoreSweep] = useState<string>('');
const [disabledPerms, setDisabledPerms] = useState<Set<VariantKey>>(() => new Set());
```

Helper functions:

```ts
const toggleInSet = <T extends string>(setter, value) => setter((prev) => {
  const next = new Set(prev);
  if (next.has(value)) next.delete(value); else next.add(value);
  return next;
});
const togglePermDisabled = (key) => setDisabledPerms((prev) => {
  const next = new Set(prev);
  if (next.has(key)) next.delete(key); else next.add(key);
  return next;
});
const parseNumList = (s) => { /* ',' split, trim, Number(), filter finite, dedup */ };
const parseMinScoreList = (s) => { /* same, plus "none"/"off" → null token, dedup with null distinct */ };
```

Derived `allPermutations` memo (cross-product):

```ts
const allPermutations = useMemo<VariantParams[]>(() => {
  const topList = parseNumList(topSectorsSweep);
  const perList = parseNumList(perSectorSweep);
  const minList = parseMinScoreList(minScoreSweep);
  const uniList = Array.from(selectedUniverses);
  const grpList = Array.from(selectedGroupings);
  const topAxis = topList.length === 0 ? [undefined] : topList;
  const perAxis = perList.length === 0 ? [undefined] : perList;
  const minAxis = minList.length === 0 ? [undefined] : minList;
  const uniAxis = uniList.length === 0 ? [undefined] : uniList;
  const grpAxis = grpList.length === 0 ? [undefined] : grpList;
  const out: VariantParams[] = [];
  for (const v of VARIANT_DEFS) {
    if (!selectedFreqs.has(v.frequency)) continue;
    if (!selectedStrategies.has(v.strategy)) continue;
    for (const t of topAxis) for (const p of perAxis) for (const m of minAxis)
    for (const u of uniAxis) for (const g of grpAxis) {
      out.push({
        frequency: v.frequency,
        strategy: v.strategy,
        ...(t !== undefined ? { top_n_sectors: t } : {}),
        ...(p !== undefined ? { top_n_per_sector: p } : {}),
        ...(m !== undefined ? { min_price_score: m } : {}),
        ...(u !== undefined ? { universe: u } : {}),
        ...(g !== undefined ? { grouping: g } : {}),
      });
    }
  }
  return out;
}, [selectedFreqs, selectedStrategies, selectedUniverses, selectedGroupings, topSectorsSweep, perSectorSweep, minScoreSweep]);

const variantsToRun = useMemo(
  () => allPermutations.filter((p) => !disabledPerms.has(makeVariantKey(p))),
  [allPermutations, disabledPerms],
);
const longShortBlocked = selectionMode === 'random' || selectionMode === 'all' || selectionMode === 'sector_etf';
const eligibleVariants = variantsToRun.filter((v) => !longShortBlocked || v.strategy !== 'long_short');
const eligibleCount = eligibleVariants.length;
const totalPerms = allPermutations.length;
const LARGE_VARIANTS_THRESHOLD = 30;
const variantsBlockReason: string | null = selectedUniverses.size === 0 ? 'Pick at least one universe.' : null;
```

`runVariantsBacktest()`:
- `targets = variantsToRun.filter((v) => !longShortBlocked || v.strategy !== 'long_short')`
- Bail when `targets.length === 0`.
- `universeFromVariants = targets[0]?.universe ?? null`
- `groupingFromVariants = targets[0]?.grouping ?? 'sector'`
- Pass these as `index_universe` and `grouping` in the base config to `startVariantsBacktest(base, targets)`.

`saveVariantsBundle()`:
- Iterate `Object.entries(variants)` (not VARIANT_DEFS); parse each key via `parseVariantKey`; skip when null or status !== 'ok'.
- The persisted entry shape per variant: `{key, label: variantLabel(params), frequency, strategy, top_n_sectors, top_n_per_sector, min_price_score, summary, monthly_records: r.monthly_records.map(rec => ({...rec, holdings: []})), daily_records, ...}`.
- ALWAYS strip per-period `holdings` from variant bundles (not just in `selectionMode === 'all'`) — the cross-product blows up the payload otherwise.

Auto-save handler: iterate `Object.entries(variants)` rather than VARIANT_DEFS,
split into `okKeys` / `errKeys` lists.

`triggerActiveVariantLabel`: rename local `variantLabel` → `activeVariantSuffix`
to avoid shadowing the imported `variantLabel` function. Fallback chain:
`VARIANT_DEFS.find(...).label ?? (parseVariantKey(key) ? variantLabel(parseVariantKey(key)!) : undefined)`.

**JSX layout** — the inline panel goes immediately below the Run buttons row,
above the "Strategy parameters" card. Structure:

```
<div className="bg-[#151821] rounded-xl border border-gray-800/40 p-4">
  <h2>Variants</h2>
  {variantsBlockReason && <red banner>}
  <SweepAxesRow>           {/* 3-column grid: Top sectors / Per sector / Min price score text inputs */}
  {longShortBlocked && <amber notice>}
  <FrequencyStrategyRow>   {/* 2-column grid: Frequency + Strategy checkbox lists */}
  <UniverseGroupingRow>    {/* 2-column grid: Universe + Grouping checkbox lists */}
  <PermutationsPreview>    {/* count chip + Enable-all button + scrollable checkbox list */}
</div>
```

Each multi-select column has:
- Header row: label + "All" / "None" link buttons.
- Bordered scrollable `<ul>` (`max-h-56` or `max-h-32`) with checkbox `<label>` per option.

Permutations preview: count chip amber when `eligibleCount > LARGE_VARIANTS_THRESHOLD`,
"Enable all" button visible only when `totalPerms > eligibleCount`, each `<li>`
shows checkbox + `variantLabel(p)`. Disabled when `longShortBlocked && p.strategy === 'long_short'`.

**Old removed UI**:
- Top-of-config Universe `<select>` (`indexUniverses` dropdown).
- Top-row Top Sectors / Per Sector / Group By / Min Price Score inputs (these moved to sweep axes).
- The Run-variants split-button + popover that toggled `variantsPickerOpen`. Now the Run button is a plain button and the panel below is always visible.

`useClickOutside(variantsPickerRef, ...)` is gone (no popover).

### Frontend (`VariantSummaryTable.tsx`)

Stable display order across legacy + cross-product keys:

```ts
const orderedVariantRows = useMemo<{ key: VariantKey; params: VariantParams; label: string }[]>(() => {
  const freqOrder = new Map<RebalanceFrequency, number>();
  VARIANT_DEFS.forEach((v, i) => { if (!freqOrder.has(v.frequency)) freqOrder.set(v.frequency, i); });
  const stratOrder: Record<StrategyType, number> = { long_only: 0, long_short: 1 };
  const entries: { key: VariantKey; params: VariantParams; label: string }[] = [];
  for (const k of Object.keys(variants)) {
    if (variants[k as VariantKey] == null) continue;
    const p = parseVariantKey(k as VariantKey);
    if (!p) continue;
    const canonical = VARIANT_DEFS.find((v) => v.key === k)?.label;
    entries.push({ key: k as VariantKey, params: p, label: canonical ?? variantLabel(p) });
  }
  entries.sort((a, b) => {
    let d = (freqOrder.get(a.params.frequency) ?? 999) - (freqOrder.get(b.params.frequency) ?? 999);
    if (d !== 0) return d;
    d = stratOrder[a.params.strategy] - stratOrder[b.params.strategy];
    if (d !== 0) return d;
    d = (a.params.top_n_sectors ?? -1) - (b.params.top_n_sectors ?? -1);
    if (d !== 0) return d;
    d = (a.params.top_n_per_sector ?? -1) - (b.params.top_n_per_sector ?? -1);
    if (d !== 0) return d;
    return ((a.params.min_price_score ?? -Infinity) as number) - ((b.params.min_price_score ?? -Infinity) as number);
  });
  return entries;
}, [variants]);
```

Iterate `orderedVariantRows` in the body and in the export-rows memo (instead
of `VARIANT_DEFS.filter(...)`).

---

## Feature 7 — SavedRunsDropdown extraction

### Backend

**`backend/routers/momentum/backtest_crud.py` (`list_backtests`)** — change the
select from `"run_id, name, created_at"` to `"run_id, name, created_at, config"`
so the dropdown can render discriminating subtext. Update the docstring to note
that `config` is included (small payload, not the result blob).

### Frontend

Create a new file `frontend/app/components/momentum/SavedRunsDropdown.tsx`
(see actual code in the saved diff or the new-file blob). Key API:

```ts
export type SavedRunsDropdownProps = {
  savedRuns: SavedRun[];
  loading: boolean;
  loadedRunId: number | null;
  loadingRunId: number | null;
  deletingRunId: number | null;
  renamingRunId: number | null;
  bulkDeleting: boolean;
  onLoad: (runId: number) => void;
  onDelete: (runId: number) => void;
  onRename: (runId: number, currentName: string) => void;
  onBulkDelete: (ids: number[]) => Promise<void>;
};
```

Behavior:
- Owns `open` + `selectedIds` state internally; uses `useClickOutside` to close.
- Clears selection on close (useEffect).
- Trigger button label: spinner during `loading || loadingRunId != null`; "Loading
  saved backtests" when first-fetch; "No saved backtests yet" when empty;
  loaded-run name when active; "Load saved backtest..." otherwise.
- Multi-select header (bulk delete bar) appears when `selectedIds.size > 0`.
- Per-row inline `paramsLine` + `signalsLine` subtext:

```ts
function describeBacktestParams(cfg: Record<string, unknown> | undefined): string {
  // builds "3×6 · 2024-01 → 2026-05 · random×5@42 · group:industry · price≥30 · P70/V30"
  // from cfg.top_n_sectors / cfg.top_n_per_sector, start_date/end_date, selection_mode/n_trials/random_seed,
  // grouping (when not 'sector'), min_price_score, category_weights.
}

function describeBacktestSignals(cfg: Record<string, unknown> | undefined): string {
  // "12-1:3 6m:2 DD" — abbreviations from SIGNAL_ABBREV, skips weight=0,
  // omits ":N" suffix when weight === 1.
}

const SIGNAL_ABBREV: Record<string, string> = {
  mom_12_1: '12-1', mom_6m: '6m',
  volatility_adjusted_return_6m: 'vAdj',
  drawdown_from_recent_high_pct: 'DD',
  above_200ma: '200ma',
  vol_20d_vs_60d: 'vSrg',
  vol_trend_3m: 'vT3m',
};
```

### Frontend (`MomentumBacktester.tsx`) integration

Replace the existing inline `(() => { ... saved runs dropdown IIFE ... })()` block
with `<SavedRunsDropdown ... />` (passing the props above).

Remove the now-dead state:
- `savedDropdownOpen`, `savedDropdownRef`
- `selectedRunIds`, `toggleRunSelected`
- The matching `useClickOutside(savedDropdownRef, ...)` call
- The `useEffect(() => { if (!savedDropdownOpen) setSelectedRunIds(new Set()) })`

`bulkDeleteRuns` signature changes from `async () => { const ids = Array.from(selectedRunIds); ... }`
to `async (ids: number[]) => { ... }`. Local `idSet = new Set(ids)`. The
existing list-pruning + loadedRunId-clear logic uses `idSet` instead.

Keep the `loadingRunId` / `deletingRunId` / `renamingRunId` state in the parent
— it's set by the parent's CRUD handlers and forwarded to the dropdown so the
matching row can spinner-up.

---

## Feature 8 — Template universes section on /schedule + auto-bootstrap

### Backend

**`backend/routers/universe_templates.py` (`_summary`)** — add `last_refreshed_at`
to the response:

```python
last_refreshed_at = template.last_refreshed_at(supabase) if uid is not None else None
# ...
return {
    ...
    "last_refreshed_at": last_refreshed_at,
}
```

(`UniverseTemplate.last_refreshed_at()` already exists in
`backend/index_universe/templates/base.py`.)

**`backend/routers/ingest_runs.py`** — add `"bootstrap_template_refresh"` to
`_VALID_JOB_NAMES` with a docstring explaining it's fired by `scheduler.py` on
app start when a template has never been refreshed:

```python
"bootstrap_template_refresh",
```

**`backend/scheduler.py`** — three helpers + a hook into `register_scheduler`:

```python
_BOOTSTRAP_DELAY_SECONDS = 30
_PIPELINE_STALE_AFTER_SECONDS = 3600


def _unrefreshed_templates() -> list[str]:
    """Return template_keys for every registered template that's never
    been refreshed in this env (universe row missing, or last_refreshed_at IS NULL)."""
    from deps import supabase
    from index_universe.templates import all_templates
    unrefreshed = []
    for template in all_templates():
        try:
            uid = template.universe_id(supabase)
            if uid is None:
                unrefreshed.append(template.template_key); continue
            if template.last_refreshed_at(supabase) is None:
                unrefreshed.append(template.template_key)
        except Exception as e:
            _log.warning("[scheduler] bootstrap check failed for %s: %s: %s",
                         template.template_key, type(e).__name__, e)
    return unrefreshed


def _pipeline_already_running() -> bool:
    """True if an `ingest_run` row in `running` state was started within
    the last hour. Guards against duplicate bootstraps."""
    from deps import supabase
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=_PIPELINE_STALE_AFTER_SECONDS)).isoformat()
    try:
        resp = supabase.table("ingest_run").select("run_id").eq("status", "running").gte("started_at", cutoff).limit(1).execute()
        return bool(resp.data)
    except Exception as e:
        _log.warning("[scheduler] running-pipeline probe failed (%s: %s) — skipping bootstrap to be safe", type(e).__name__, e)
        return True   # fail-safe: if we can't query, don't double-fire


def _maybe_bootstrap_templates(sched: BackgroundScheduler) -> None:
    """Schedule a one-shot full-pipeline run via DateTrigger when a template
    has never been refreshed. Idempotent (replace_existing=True)."""
    try:
        unrefreshed = _unrefreshed_templates()
    except Exception as e:
        _log.warning("[scheduler] bootstrap-templates probe failed: %s: %s", type(e).__name__, e)
        return
    if not unrefreshed:
        _log.info("[scheduler] bootstrap-templates: all templates refreshed — no-op"); return
    if _pipeline_already_running():
        _log.info("[scheduler] bootstrap-templates: %s unrefreshed (%s) but a pipeline is already running — skipping", len(unrefreshed), unrefreshed); return
    run_at = datetime.now(timezone.utc) + timedelta(seconds=_BOOTSTRAP_DELAY_SECONDS)
    _log.warning("[scheduler] bootstrap-templates: %s unrefreshed (%s) — firing full pipeline at %s", len(unrefreshed), unrefreshed, run_at.isoformat())
    sched.add_job(
        _fire_job,
        DateTrigger(run_date=run_at),
        args=["bootstrap_template_refresh"],
        id="bootstrap_template_refresh",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=600,
    )
```

Imports needed: `from datetime import datetime, timedelta, timezone` and
`from apscheduler.triggers.date import DateTrigger`.

In `register_scheduler`, after `sched.start()` (still inside the `try/except` shell):

```python
try:
    _maybe_bootstrap_templates(sched)
except Exception as e:
    _log.warning("[scheduler] bootstrap-templates wrapper failed: %s: %s", type(e).__name__, e)
```

### Frontend (`frontend/app/components/Schedule.tsx`)

Add a `UniverseTemplateSummary` type matching the backend payload, then add a
`TemplateUniversesCard` component placed **above** the Misc jobs section in
the main `Schedule` component's JSX:

```tsx
{/* Template universes — visibility into the canonical universes and whether
    any of them need an initial refresh in this env. */}
<TemplateUniversesCard />
```

`TemplateUniversesCard` responsibilities:
- `useEffect` fetch from `GET /api/universe-templates` on mount; store in `templates` state.
- Trigger button posts to `/api/ingest/scheduled-refresh/trigger?job_name=manual`
  via `apiFetch`; on success, refetch after 5s `setTimeout`.
- Amber banner shown when `unrefreshed = templates.filter(t => t.last_refreshed_at == null)`
  has any entries. Banner body: "<N> template(s) never refreshed in this environment"
  + a "Run pipeline now" button. Shows `triggerError` inline when applicable.
- Table-style list: every template gets a row showing `label`, `template_key`
  chip, "never refreshed" chip (amber) when applicable, and a status line either
  "no membership data yet" (amber) or "<N> members · latest month <YYYY-MM> · last refresh <timestamp>".
- Uses `LoadingDots`, `fmtTimestamp` from the existing Schedule.tsx helpers.

---

## Stretch / nice-to-have notes

- **PostgREST cache reload**: the migration ends with `NOTIFY pgrst, 'reload schema';` so the new column is queryable immediately without a Supabase restart. Keep this when re-applying.

- **`config` payload size**: `/api/momentum/backtests` returning `config` adds ~1-3 KB per row. For ~50 saved runs that's ~150 KB worst case — acceptable. If it ever grows past 1 MB, consider a lightweight `config_summary` projection instead.

- **`monthly_records` strip**: in the variants-bundle save, stripping holdings unconditionally means a saved bundle reload can't drill into per-period holdings for the *inactive* variants. The *active* variant's holdings are still computed live from `r.monthly_records` (server-side, attached when the user clicks a variant tab). Worth verifying this still works after the change — if the variant tab needs the persisted holdings, you have a problem.

- **Multi-universe sweep error path**: the old "mixed-universe rejection" error message was removed since the backend now supports it. Sanity-check that `run_variants_sweep` no longer raises on mixed universes (search for any leftover gate).

- **Bootstrap timing**: 30s after startup is a guess. If Railway's import-time + Supabase client init goes longer, bump `_BOOTSTRAP_DELAY_SECONDS`. The fallback (next Tuesday tick) still works.
