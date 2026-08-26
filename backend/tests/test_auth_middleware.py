"""Regression guard for the `/api/*` auth gate (`routers/_auth_middleware.py`).

Pins the security properties the audit cared about so they can't silently
regress:
  * M1 — EVERY non-public /api/* request needs a valid token (reads too).
  * H1 — earnings-refresh is no longer unauthenticated; it requires a
    logged-in user (and is intentionally allowed for non-admins).
  * Role tiers — public (health/cron), self-auth (/api/auth), user reads,
    user writes, and "everything else is admin-only".

We drive the real `enforce_api_auth` middleware with a stubbed
`verify_token` (injected via sys.modules so no Supabase client is needed)
and a constructed Starlette Request, asserting both the HTTP status and
whether the request was allowed through to the handler.
"""
from __future__ import annotations

import asyncio
import sys
import types

from fastapi.responses import JSONResponse
from starlette.requests import Request

from routers import _auth_middleware as mw


def _request(method: str, path: str, auth: str | None = "Bearer t") -> Request:
    headers = [(b"authorization", auth.encode())] if auth is not None else []
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": headers,
        "query_string": b"",
        "scheme": "http",
        "server": ("testserver", 80),
    }
    return Request(scope)


class _FakeAuthBackendUnavailable(RuntimeError):
    """Stands in for `routers.auth.AuthBackendUnavailable`.

    The middleware does `from routers.auth import AuthBackendUnavailable`, so it
    binds whatever the injected fake module exposes — meaning raising *this*
    class is caught by the middleware's `except` for real. Defining it here
    instead of importing the genuine one keeps this suite free of the Supabase
    client, which is the whole reason routers.auth is faked (see module docstring).
    """


def _run(monkeypatch, method: str, path: str, role: str | None, auth: str | None = "Bearer t",
         raises: BaseException | None = None):
    """Returns (status_code, reached_handler). `role=None` simulates an
    invalid/absent token (verify_token returns None); `raises` simulates
    verify_token blowing up instead of returning a verdict."""
    def _verify(_authz):
        if raises is not None:
            raise raises
        return None if role is None else {"role": role}

    fake_auth = types.SimpleNamespace(
        verify_token=_verify,
        # !! MUST BE PRESENT OR EVERY TEST IN THIS FILE ImportErrors -- the
        # middleware imports this name alongside verify_token.
        AuthBackendUnavailable=_FakeAuthBackendUnavailable,
    )
    monkeypatch.setitem(sys.modules, "routers.auth", fake_auth)

    reached = {"v": False}

    async def call_next(_req):
        reached["v"] = True
        return JSONResponse({"ok": True})

    resp = asyncio.run(mw.enforce_api_auth(_request(method, path, auth), call_next))
    return resp.status_code, reached["v"]


class TestPublicAndPassthrough:
    def test_options_preflight_passes(self, monkeypatch):
        assert _run(monkeypatch, "OPTIONS", "/api/companies", None) == (200, True)

    def test_non_api_path_passes(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/openapi.json", None) == (200, True)

    def test_health_is_public(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/health", None) == (200, True)

    def test_both_cron_endpoints_are_public(self, monkeypatch):
        # Cron endpoints self-gate on X-Cron-Secret, so the JWT gate lets
        # them through.
        assert _run(monkeypatch, "POST", "/api/ingest/scheduled-refresh/cron", None) == (200, True)
        assert _run(monkeypatch, "POST", "/api/momentum/current-picks/cron", None) == (200, True)

    def test_auth_router_is_self_gated(self, monkeypatch):
        assert _run(monkeypatch, "POST", "/api/auth/login", None) == (200, True)


class TestM1_ReadsRequireAuth:
    def test_unauthenticated_read_is_401(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/companies", None) == (401, False)

    def test_unauthenticated_arbitrary_read_is_401(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/momentum/saved", None) == (401, False)

    def test_authenticated_user_read_allowed(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/companies", "user") == (200, True)

    def test_non_admin_read_of_admin_path_is_403(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/admin/health", "user") == (403, False)

    def test_scheduled_strategies_read_allowed_for_user(self, monkeypatch):
        # The read-only /schedule view: users may GET (the endpoint filters to
        # user_visible rows); writes stay admin-only (see TestWriteTiers).
        assert _run(monkeypatch, "GET", "/api/scheduled-strategies", "user") == (200, True)

    def test_fx_and_benchmarks_reads_allowed_for_user(self, monkeypatch):
        # Reference data the read-only /schedule portfolio card needs (FX
        # conversion + ETF benchmark identity).
        assert _run(monkeypatch, "GET", "/api/fx/latest", "user") == (200, True)
        assert _run(monkeypatch, "GET", "/api/benchmarks", "user") == (200, True)


class TestScheduleDetailResourceReads:
    """The read-only /schedule strategy-detail panel loads its current
    portfolio + source backtest from two otherwise-admin `/api/momentum/*`
    GET-by-id routes. Those EXACT paths are allow-listed for users (the
    endpoints then authorize the id via `user_visible`); the list-all + write
    forms stay admin-only."""

    def test_current_picks_by_id_read_allowed_for_user(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/momentum/current-picks/42", "user") == (200, True)

    def test_backtest_by_id_read_allowed_for_user(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/momentum/backtests/107", "user") == (200, True)

    def test_current_picks_list_still_admin_only(self, monkeypatch):
        # The bare list (no id) would leak every snapshot — not allow-listed.
        assert _run(monkeypatch, "GET", "/api/momentum/current-picks", "user") == (403, False)

    def test_backtests_list_still_admin_only(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/momentum/backtests", "user") == (403, False)

    def test_current_picks_subroute_still_admin_only(self, monkeypatch):
        # e.g. /{id}/refresh-mtd must not be reachable via the id allow-list.
        assert _run(monkeypatch, "GET", "/api/momentum/current-picks/42/refresh-mtd", "user") == (403, False)

    def test_current_picks_by_id_write_still_admin_only(self, monkeypatch):
        assert _run(monkeypatch, "DELETE", "/api/momentum/current-picks/42", "user") == (403, False)

    def test_resource_read_still_needs_auth(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/momentum/backtests/107", None) == (401, False)

    def test_airs_scan_is_admin_only_even_though_airs_reads_are_not(self, monkeypatch):
        # See TestManagementDashboardForUsers: /api/airs/ is a user read tier now, but `scan` is a
        # GET that runs a Playwright scrape — the deny-list has to beat the prefix.
        assert _run(monkeypatch, "GET", "/api/airs/scan", "user") == (403, False)


class TestManagementDashboardForUsers:
    """The Management Dashboard is user-visible, so the API behind it must be readable — and
    REFRESHABLE (2026-08-19). The line is refresh vs. mutate: a user may make the page's own
    figures current, and may not change what it says. See TestManagementDashboardRefreshesAreOpen
    for the first half; the deny tests in this class are the second."""

    def test_the_page_s_reads_are_allowed(self, monkeypatch):
        for path in ("/api/airs/portfolios/overview",
                     # ⚠ `/accounts/{p}/isins`, NOT `/holdings` — see
                     # TestExpandingAnAccountIsAdminOnly. This list said `/holdings` until
                     # 2026-08-06, which is exactly the access the Overview row now withholds.
                     "/api/airs/accounts/BUS_X/isins",
                     "/api/airs/model-portfolios/correlations",
                     "/api/airs/model-portfolios/7/analysis",
                     "/api/asset-pipeline/fundamentals/isin/US0378331005",
                     "/api/asset-pipeline/latest-close/isin/US0378331005",
                     "/api/asset-pipeline/risk/isin/US0378331005"):
            assert _run(monkeypatch, "GET", path, "user") == (200, True), path

    def test_a_read_that_arrives_as_post_is_allowed(self, monkeypatch):
        """⚠ These POST because a basket of ISINs does not fit in a URL. They mutate nothing, and
        the method-based write tier would 403 a user on a page they may open."""
        for path in ("/api/airs/basket/analysis",
                     "/api/asset-pipeline/basket/performance",
                     "/api/earnings/margin-inputs",
                     "/api/earnings/fundamental-blend-metrics",
                     "/api/earnings/fundamental-coverage"):
            assert _run(monkeypatch, "POST", path, "user") == (200, True), path

    def test_the_ingest_sibling_one_segment_down_is_named_in_full(self, monkeypatch):
        """⚠ WHY BOTH LISTS ARE EXACT-MATCH, NEVER PREFIX. `fundamental-coverage` reports what we
        hold; `fundamental-coverage/ingest` spends GuruFocus quota to go and fetch it. A user may
        now fire both — but only because each is written out by name, and this test is the
        difference between that and a prefix that would also hand over whatever lands under
        `/api/earnings/` next."""
        assert _run(monkeypatch, "POST", "/api/earnings/fundamental-coverage/ingest",
                    "user") == (200, True)
        assert _run(monkeypatch, "POST", "/api/earnings/fundamental-coverage/anything-else",
                    "user") == (403, False)

    def test_the_request_held_sse_scrapes_stay_admin_only(self, monkeypatch):
        """⚠ THE JOB FORM IS OPEN, THE SSE FORM IS NOT — see
        TestManagementDashboardRefreshesAreOpen. These GETs hold a request open for the whole
        Playwright scrape and belong to the admin-only /airs-portfolio page; the Dashboard starts
        the same work as a cancellable background job. The blocking POST twins are likewise not the
        Dashboard's — it fires `/job` for both."""
        assert _run(monkeypatch, "GET", "/api/airs/scan", "user") == (403, False)
        assert _run(monkeypatch, "GET", "/api/airs/model-portfolios/scan", "user") == (403, False)
        assert _run(monkeypatch, "POST", "/api/airs/vermogen/refresh", "user") == (403, False)
        assert _run(monkeypatch, "POST", "/api/airs/portfolios/BUS_X/refresh", "user") == (403, False)

    def test_client_crm_records_stay_admin_only(self, monkeypatch):
        """A genuine read, but of relations — a different subject from the portfolios page, and it
        sits inside the now-readable /api/airs/ prefix."""
        assert _run(monkeypatch, "GET", "/api/airs/crm-relaties", "user") == (403, False)

    def test_deleting_an_account_is_admin_only(self, monkeypatch):
        assert _run(monkeypatch, "DELETE", "/api/airs/portfolios/BUS_X", "user") == (403, False)

    def test_the_three_pins_are_admin_only(self, monkeypatch):
        """Class, ISIN and Link are data curation — one user's fix would move every book."""
        assert _run(monkeypatch, "POST", "/api/airs/asset-bucket-override", "user") == (403, False)
        assert _run(monkeypatch, "POST", "/api/airs/holding-isin-override", "user") == (403, False)
        assert _run(monkeypatch, "PUT", "/api/airs/accounts/BUS_X/link", "user") == (403, False)
        assert _run(monkeypatch, "PUT", "/api/airs/model-portfolios/7/display-name",
                    "user") == (403, False)

    def test_the_rest_of_asset_pipeline_is_not_opened_up(self, monkeypatch):
        """Only the three by-ISIN reads the modals need — the instruments grid is admin's."""
        assert _run(monkeypatch, "GET", "/api/asset-pipeline/grid", "user") == (403, False)
        assert _run(monkeypatch, "POST", "/api/asset-pipeline/store", "user") == (403, False)

    def test_these_reads_still_need_a_token(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/airs/portfolios/overview", None) == (401, False)
        assert _run(monkeypatch, "POST", "/api/airs/basket/analysis", None) == (401, False)


class TestManagementDashboardRefreshesAreOpen:
    """Every refresh on /management-dashboard is a non-admin's to fire (2026-08-19, on request).

    ⚠ THE PAGE WAS READABLE BUT FROZEN. A user could see that the AIRS scrape was days old, that an
    index had not been rebuilt, that a constituent had no fundamentals — and could do nothing about
    any of it. The judgement recorded in `_USER_REFRESH_PATHS` is that a stale dashboard nobody can
    refresh costs more than the GuruFocus quota and AirSPMS sessions these spend.

    ⚠ THESE ARE THE RULE, NOT THE VISIBLE BUTTONS. The panels no longer gate the controls on
    `isAdmin`, which is presentation. If this class goes red the buttons 403 whatever they look
    like; if only the panels change, the permission is a suggestion."""

    # Exactly what the three tabs fire, by the button that fires it.
    REFRESHES = (
        # Overview → "Refresh all from AIRS", both halves.
        ("POST", "/api/airs/vermogen/refresh/job"),
        ("POST", "/api/airs/model-portfolios/scan/job"),
        # Overview → one row's Refresh, and the identical one in the Analyse modal's header.
        ("POST", "/api/airs/portfolios/BUS_WTS_StMerken_Dyn/refresh/job"),
        # Analyse modal → the fundamentals fill, over a paired model or a bare basket of ISINs.
        ("POST", "/api/airs/model-portfolios/7/fundamentals/ingest/job"),
        ("POST", "/api/airs/basket/fundamentals/ingest/job"),
        # Analyse modal input tables → one `no_data` holding's financials.
        ("POST", "/api/earnings/fundamental-coverage/ingest"),
        # Benchmarks → per-index Refresh and "Refresh all" (both halves of each).
        ("POST", "/api/benchmarks/index/S%26P%20500/refresh/job"),
        ("POST", "/api/benchmarks/index/S%26P%20500/fundamentals/ingest/job"),
        # Benchmarks grid → one constituent's Fetch cell.
        ("POST", "/api/benchmarks/company/1234/fundamentals/ingest/job"),
    )

    def test_a_user_may_start_every_one_of_them(self, monkeypatch):
        for method, path in self.REFRESHES:
            assert _run(monkeypatch, method, path, "user") == (200, True), path

    def test_they_all_still_need_a_token(self, monkeypatch):
        for method, path in self.REFRESHES:
            assert _run(monkeypatch, method, path, None) == (401, False), path

    def test_a_user_can_watch_and_stop_what_they_started(self, monkeypatch):
        """⚠ THE TRANSPORT COMES WITH THE PERMISSION. Every path above returns a job handle and
        reports through `/api/jobs`; without the list, the stream and the Cancel a user would start
        minutes of work with no progress and no way out — the state this panel kept being reported
        as "stuck"."""
        assert _run(monkeypatch, "GET", "/api/jobs", "user") == (200, True)
        assert _run(monkeypatch, "GET", "/api/jobs/abc123/stream", "user") == (200, True)
        assert _run(monkeypatch, "POST", "/api/jobs/abc123/cancel", "user") == (200, True)

    def test_starting_a_job_is_still_owned_by_the_endpoint(self, monkeypatch):
        """There is no generic `POST /api/jobs` — a generic starter is a registry of kinds mapped
        to callables, i.e. arbitrary work by name — and opening the read tier must not invent one."""
        assert _run(monkeypatch, "POST", "/api/jobs", "user") == (403, False)

    def test_the_job_suffix_is_not_a_skeleton_key(self, monkeypatch):
        """⚠ ANCHORED PATTERNS, NOT "anything ending in /job". Every allowed path is written out; a
        job starter that is not the Dashboard's stays admin-only."""
        assert _run(monkeypatch, "POST", "/api/asset-pipeline/ingest/job", "user") == (403, False)
        assert _run(monkeypatch, "POST", "/api/airs/portfolios/BUS_X/delete/job",
                    "user") == (403, False)

    def test_the_refresh_tier_did_not_open_the_mutations_beside_it(self, monkeypatch):
        """⚠ THE WHOLE REASON THIS IS A LIST OF PATHS AND NOT AN `/api/airs/` WRITE PREFIX. These
        sit one segment from a refresh a user may now fire, and each CHANGES what the page says
        rather than making it current."""
        for method, path in (("DELETE", "/api/airs/portfolios/BUS_X"),
                             ("DELETE", "/api/benchmarks/index/S%26P%20500"),
                             ("POST", "/api/airs/asset-bucket-override"),
                             ("POST", "/api/airs/holding-isin-override"),
                             ("PUT", "/api/airs/accounts/BUS_X/display-name"),
                             ("PUT", "/api/airs/accounts/BUS_X/link")):
            assert _run(monkeypatch, method, path, "user") == (403, False), path


class TestExpandingAnAccountIsAdminOnly:
    """Opening a row in the /management-dashboard Overview is admin-only (2026-08-06).

    The summary table stays user-readable — what is withheld is the book behind a row: its
    positions and their EUR values, its mutations, the reconciliation, the link picker.

    ⚠ THESE ARE THE RULE, NOT THE HIDDEN `<tr>`. The frontend drops the row's click handler for a
    non-admin, which is presentation; every one of these URLs is still sitting in a bundle that
    user downloads. If this class goes green while the panel is restricted, the restriction is
    real; if only the panel changes, it is a suggestion."""

    EXPANDED = ("/api/airs/accounts/BUS_X/holdings",
                "/api/airs/accounts/BUS_X/transactions",
                "/api/airs/accounts/BUS_X/return-reconciliation",
                "/api/airs/accounts/BUS_X/linkable")

    def test_every_sub_resource_the_expanded_row_opens_is_denied(self, monkeypatch):
        for path in self.EXPANDED:
            assert _run(monkeypatch, "GET", path, "user") == (403, False), path

    def test_an_admin_still_gets_all_of_them(self, monkeypatch):
        for path in self.EXPANDED:
            assert _run(monkeypatch, "GET", path, "admin") == (200, True), path

    def test_isins_stays_readable_because_analyse_shares_it(self, monkeypatch):
        """⚠ THE ONE THAT MUST NOT BE SWEPT UP, and the reason this is a pattern list rather than
        an `/api/airs/accounts/` prefix. A non-admin keeps the Analyse button, and for a book with
        no paired model portfolio `/isins` is the ONLY way it gets a basket to analyse
        (`openModal` in PortfolioOverviewPanel). A prefix would take that away silently."""
        assert _run(monkeypatch, "GET", "/api/airs/accounts/BUS_X/isins", "user") == (200, True)

    def test_the_account_name_may_contain_anything_url_safe(self, monkeypatch):
        """⚠ AIRS's `Portefeuille` is a 24-char legacy code with underscores and digits, and it
        arrives percent-encoded. A pattern anchored on a narrower character class would match the
        tidy test id and miss the real ones — i.e. pass here and allow in production."""
        for pid in ("BUS_WTS_StMerken_Dyn", "BUS_BM_AAN_kw_USD_2026_d", "MoTopSelectie_FX", "7"):
            assert _run(monkeypatch, "GET", f"/api/airs/accounts/{pid}/holdings",
                        "user") == (403, False), pid

    def test_the_deny_does_not_leak_onto_neighbouring_paths(self, monkeypatch):
        """The patterns are anchored at both ends: they must not catch the accounts LIST, nor a
        deeper path that merely starts the same way."""
        assert _run(monkeypatch, "GET", "/api/airs/accounts", "user") == (200, True)
        assert _run(monkeypatch, "GET", "/api/airs/model-portfolios/7/positions",
                    "user") == (200, True)


class TestH1_EarningsRefreshNeedsAuth:
    def test_unauthenticated_refresh_is_401(self, monkeypatch):
        # The whole point of the fix: no longer reachable without a token.
        assert _run(monkeypatch, "POST", "/api/earnings/1/refresh/financials", None) == (401, False)
        assert _run(monkeypatch, "POST", "/api/earnings/1/refresh-all", None) == (401, False)

    def test_authenticated_user_may_refresh(self, monkeypatch):
        # Intentionally allowed for non-admins (it's a user-facing page).
        assert _run(monkeypatch, "POST", "/api/earnings/1/refresh/financials", "user") == (200, True)


class TestWriteTiers:
    def test_non_admin_write_to_protected_path_is_403(self, monkeypatch):
        # /api/companies is a user READ surface but not a user WRITE one.
        assert _run(monkeypatch, "POST", "/api/companies", "user") == (403, False)

    def test_portfolio_parse_now_admin_only(self, monkeypatch):
        # The AIRS upload page is admin-only now; its write left the user tier.
        assert _run(monkeypatch, "POST", "/api/portfolios/parse", "user") == (403, False)

    def test_scheduled_strategies_write_is_admin_only(self, monkeypatch):
        # Users get read-only schedule: mutations stay admin-only.
        assert _run(monkeypatch, "POST", "/api/scheduled-strategies", "user") == (403, False)

    def test_admin_may_write_anything(self, monkeypatch):
        assert _run(monkeypatch, "POST", "/api/momentum/backtest", "admin") == (200, True)

    def test_admin_may_read_anything(self, monkeypatch):
        assert _run(monkeypatch, "GET", "/api/admin/health", "admin") == (200, True)


class TestUnreachableIdentityProviderIsNot401:
    """An outage must not be reported as a credentials problem.

    2026-08-11: a REINDEX saturated prod's disk I/O, GoTrue's own DB lookup
    timed out, `verify_token` swallowed the exception into None, and the whole
    app answered **401 Authentication required**. Every session was valid. The
    401 pointed the investigation at expired logins and auth config for hours
    while the actual cause was one maintenance command eating the disk.

    401 is also actively harmful here: it invites the frontend to discard a
    good session in response to a transient fault.
    """

    def test_transport_failure_is_503_not_401(self, monkeypatch):
        status, reached = _run(
            monkeypatch, "GET", "/api/airs/portfolios/overview", "user",
            raises=_FakeAuthBackendUnavailable("ReadTimeout"),
        )
        assert status == 503, "an unreachable identity provider must not read as bad credentials"
        assert reached is False, "the request must still not reach the handler"

    def test_503_carries_retry_after(self, monkeypatch):
        """Transient means retryable, and the response should say so."""
        fake_auth = types.SimpleNamespace(
            verify_token=_raise_unavailable,
            AuthBackendUnavailable=_FakeAuthBackendUnavailable,
        )
        monkeypatch.setitem(sys.modules, "routers.auth", fake_auth)

        async def call_next(_req):
            return JSONResponse({"ok": True})

        resp = asyncio.run(
            mw.enforce_api_auth(_request("GET", "/api/airs/portfolios/overview"), call_next)
        )
        assert resp.status_code == 503
        assert resp.headers.get("Retry-After") == "5"

    def test_a_genuinely_invalid_token_is_still_401(self, monkeypatch):
        """The fix must not turn real rejections into 503s — that would make
        every unauthenticated probe look like an outage."""
        assert _run(monkeypatch, "GET", "/api/airs/portfolios/overview", None) == (401, False)

    def test_an_unexpected_error_is_still_500(self, monkeypatch):
        """Only transport faults become 503. A programming error must stay a
        500, or a real bug hides behind 'try again later' forever."""
        status, reached = _run(
            monkeypatch, "GET", "/api/airs/portfolios/overview", "user",
            raises=ValueError("boom"),
        )
        assert (status, reached) == (500, False)


def _raise_unavailable(_authz):
    raise _FakeAuthBackendUnavailable("ReadTimeout")


class TestTheOneAssetPipelineWriteAUserMayMake:
    """`/api/asset-pipeline/latest-close/isin/{isin}/refresh` — bring ONE instrument's stored
    closes up to date, from the Deep Valuation tab's share-price row.

    ⚠⚠ THE READ IT REPAIRS IS ALREADY A USER READ. `/api/asset-pipeline/latest-close/` sits in
    `_USER_READ_PREFIXES` because the Management Dashboard needs it, so the refresh button beside
    that figure is on screen for every authenticated user. Left in the write tier it 403s for all
    of them — a visible control that fails for most of the people who can see it.

    ⚠⚠ AND IT IS A PATTERN, NOT A PREFIX. `/api/asset-pipeline/` also holds the bulk ingest, the
    OpenFIGI resolve and the row refresh. Widening this to a prefix hands every one of those to
    any logged-in user, which is the trap `_USER_POST_READ_PATHS` states in its own note.
    """

    def test_a_user_may_refresh_one_instruments_close(self, monkeypatch):
        assert _run(monkeypatch, "POST",
                    "/api/asset-pipeline/latest-close/isin/US0378331005/refresh",
                    "user") == (200, True)

    def test_an_admin_may_too(self, monkeypatch):
        assert _run(monkeypatch, "POST",
                    "/api/asset-pipeline/latest-close/isin/US0378331005/refresh",
                    "admin") == (200, True)

    def test_anonymous_still_gets_401(self, monkeypatch):
        assert _run(monkeypatch, "POST",
                    "/api/asset-pipeline/latest-close/isin/US0378331005/refresh",
                    None) == (401, False)

    def test_it_does_NOT_open_the_rest_of_the_asset_pipeline(self, monkeypatch):
        # The expensive neighbours: a bulk ingest, an identity resolve, a whole-row refresh.
        assert _run(monkeypatch, "POST", "/api/asset-pipeline/rows/refresh", "user") == (403, False)
        assert _run(monkeypatch, "POST", "/api/asset-pipeline/ingest", "user") == (403, False)
        assert _run(monkeypatch, "POST", "/api/asset-pipeline/existing", "user") == (403, False)

    def test_it_does_not_open_a_deeper_or_wider_path(self, monkeypatch):
        # ⚠ ANCHORED AT BOTH ENDS. A trailing segment past `/refresh`, or a second ISIN segment,
        # is a different endpoint — and `[^/]+` is what stops one being smuggled through.
        assert _run(monkeypatch, "POST",
                    "/api/asset-pipeline/latest-close/isin/US0378331005/refresh/all",
                    "user") == (403, False)
        assert _run(monkeypatch, "POST",
                    "/api/asset-pipeline/latest-close/isin/A/B/refresh", "user") == (403, False)

    def test_the_plain_GET_is_untouched(self, monkeypatch):
        assert _run(monkeypatch, "GET",
                    "/api/asset-pipeline/latest-close/isin/US0378331005", "user") == (200, True)
