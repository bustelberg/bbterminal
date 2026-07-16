"""A name WE choose for a model portfolio.

AIRS's `Portefeuille` is a 24-char code — "BUS_BM_AAN_kw_USD_2026_d", "TOPS_OFF_BEH" — an
identifier squeezed through a legacy form field, not a label anyone would choose. `display_name`
is the label, shown beside the code rather than instead of it.

⚠ THE ONE THING THAT MUST NEVER HAPPEN. `display_name` is a column on `airs_model_portfolio`, a
table the scan rewrites on every rescan. That is only safe because `save_portfolios` builds an
EXPLICIT payload and upserts `on_conflict="id"`, so PostgREST SETs only the columns it names.

Add `display_name` to that payload — the obvious edit, "for consistency", the kind that looks like
tidying — and every chosen name in the table is destroyed on the next scan. Silently: the scan has
no idea what the old value was, there is no second copy, and nothing would fail. The names would
simply be gone, and the first sign would be a human noticing the column went blank.

That is what the first test here exists to prevent. The rest is behaviour.
"""
from __future__ import annotations

import inspect

from routers import airs
from routers import _airs_portfolio_store as store


class TestTheScanMustNeverWriteTheChosenName:
    """⚠ Read the module docstring before touching this."""

    def test_the_upsert_payload_does_not_carry_display_name(self):
        src = inspect.getsource(store.save_portfolios)
        assert "display_name" not in src, (
            "`save_portfolios` names `display_name` in its payload. Every chosen name will be "
            "wiped on the next rescan, silently and with no second copy. The scan writes what "
            "AIRS said; a chosen name is not something AIRS said.")

    def test_the_payload_is_explicit_not_a_splat(self):
        """The guard above only holds while the payload is an explicit column list. A `**row`
        splat would let any future scraped key through — including one named display_name."""
        src = inspect.getsource(store.save_portfolios)
        assert "**r" not in src
        assert '"id": r["id"]' in src

    def test_the_upsert_is_on_conflict_id_not_delete_then_insert(self):
        """`save_positions` is deliberately delete-then-insert (a vanished holding must vanish).
        If the PORTFOLIO writer ever became that, the column would be dropped and recreated with
        every scan and the name would go with it."""
        src = inspect.getsource(store.save_portfolios)
        assert 'on_conflict="id"' in src
        assert "delete()" not in src


class TestBlankMeansNoNameChosen:
    """NULL is "none chosen — fall back to AIRS's code". "" is not a second way of saying that: a
    stored empty string is a CHOSEN label that happens to be blank, and it renders an empty cell
    that reads as a bug rather than as a choice. So the API collapses blank -> NULL on write.

    (Contrast `airs_model_portfolio_link`, where a stored NULL genuinely means "explicitly not a
    portfolio" and MUST stay separable from "never decided". Same column type, opposite rule —
    because there the absence is itself a decision, and here it isn't.)
    """

    def test_the_endpoint_collapses_blank_to_null(self):
        src = inspect.getsource(airs.airs_set_portfolio_display_name)
        assert '(body.display_name or "").strip() or None' in src

    def test_whitespace_only_is_blank(self):
        """"   " is a user clearing the field, not naming a model with three spaces."""
        src = inspect.getsource(airs.airs_set_portfolio_display_name)
        assert ".strip()" in src

    def test_an_unknown_portfolio_404s_rather_than_silently_doing_nothing(self):
        src = inspect.getsource(airs.airs_set_portfolio_display_name)
        assert "HTTPException(404" in src
        assert "if not res.data" in src


class TestItIsKeyedOnTheIdNotTheName:
    """The ask was "a mapping from portfolio name to a name we choose". The NAME is the wrong key:
    `id` is AirSPMS's own and documented as the stable PK, while `name` is exactly what someone
    might edit in AIRS — and a rename would orphan the alias, reverting the row to its code with
    nothing to say why. Key on the fact that does not move."""

    def test_the_write_is_by_id(self):
        src = inspect.getsource(airs.airs_set_portfolio_display_name)
        assert '.eq("id", portfolio_id)' in src
        assert '.eq("name"' not in src


class TestItRidesTheGridRead:
    def test_the_model_exposes_it(self):
        """The page opens on one instant DB read; the name must arrive with it rather than cost a
        second query or a join."""
        assert "display_name" in airs.StoredModelPortfolio.model_fields

    def test_it_defaults_to_none_not_empty_string(self):
        f = airs.StoredModelPortfolio.model_fields["display_name"]
        assert f.default is None

    def test_the_grid_read_is_select_star(self):
        """`load_portfolios` selects *, so a new view column needs no change here — but if that
        ever narrows to a column list, `display_name` has to be on it."""
        src = inspect.getsource(store.load_portfolios)
        assert 'select("*")' in src
        assert "airs_model_portfolio_grid" in src


class TestTheChosenNameReachesEverySingleSlotSurface:
    """A surface with room for ONE name shows the chosen one. `portfolio_label` is that rule, in
    one place — "which name do we show" is exactly the sort of question that gets re-answered
    slightly differently in each file until two screens disagree about what a portfolio is
    called."""

    def test_the_label_prefers_the_chosen_name(self):
        from routers._airs_portfolio_store import portfolio_label

        assert portfolio_label({"name": "BUS_Neutraal_FX",
                                "display_name": "Business Neutral"}) == "Business Neutral"

    def test_it_falls_back_to_the_airs_code(self):
        """An axis needs a label. This is where the fallback DIFFERS from the /portfolios table,
        which shows a muted "—" precisely so the unnamed models stay visible."""
        from routers._airs_portfolio_store import portfolio_label

        assert portfolio_label({"name": "BUS_Neutraal_FX", "display_name": None}) == "BUS_Neutraal_FX"
        assert portfolio_label({"name": "BUS_Neutraal_FX", "display_name": ""}) == "BUS_Neutraal_FX"
        assert portfolio_label({"name": "BUS_Neutraal_FX", "display_name": "   "}) == "BUS_Neutraal_FX"

    def test_the_correlation_matrix_uses_it(self):
        import inspect

        from routers import _airs_portfolio_correlation as c

        src = inspect.getsource(c.compute_portfolio_correlations)
        assert "portfolio_label(g)" in src
        assert 'g["name"] for g in ports' not in src        # no second, inline rule

    def test_the_matrix_sorts_by_what_it_shows(self):
        """Ordering by AIRS's code while displaying the chosen name renders a matrix whose axes
        look shuffled — alphabetical by a key the reader cannot see is indistinguishable from
        unsorted. Measured: naming one model "Aardvark Fund" must move it to index 0."""
        import inspect

        from routers import _airs_portfolio_correlation as c

        src = inspect.getsource(c.compute_portfolio_correlations)
        assert "ports.sort(key=lambda g: portfolio_label(g).lower())" in src

    def test_the_matrix_keeps_the_airs_code_alongside(self):
        """The label is for reading; the code is for finding the model in AIRS. The axis clips at
        22 chars and a chosen name hides the code entirely, so it has to ride along."""
        import inspect

        from routers import _airs_portfolio_correlation as c

        src = inspect.getsource(c.compute_portfolio_correlations)
        assert '"codes"' in src
        assert "codes" in c.__dict__ or 'codes = {' in src
