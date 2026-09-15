from routers import _airs_portfolio_analysis as analysis


class _AssetExecution:
    def select(self, *_args):
        return self

    def ilike(self, *_args):
        return self

    def limit(self, *_args):
        return self

    def execute(self):
        return type("Result", (), {"data": [
            {"isin": "DK0062498333", "name": "Novo Nordisk A/S"},
            {"isin": "US6701002056", "name": "Novo Nordisk ADR"},
        ]})()


class _Supabase:
    def table(self, name):
        assert name == "asset_execution"
        return _AssetExecution()


def test_one_word_company_name_matches_its_legal_suffix():
    assert analysis._instrument_names_match("Euronext", "Euronext NV")
    assert analysis._instrument_names_match("Euronext", "Euronext N.V.")


def test_ordinary_share_does_not_match_its_adr():
    assert not analysis._instrument_names_match("Novo Nordisk", "Novo Nordisk ADR")
    assert analysis._instrument_names_match("Novo Nordisk", "Novo Nordisk A/S")


def test_company_suffix_does_not_block_marsh_mclennan():
    assert analysis._instrument_names_match("Marsh & McLennan", "Marsh & McLennan Companies Inc")


def test_novo_nordisk_sold_row_resolves_to_its_ordinary_share(monkeypatch):
    monkeypatch.setattr(analysis, "supabase", _Supabase())
    assert analysis._asset_execution_isins_by_name(["Novo Nordisk"]) == {
        "Novo Nordisk": "DK0062498333",
    }
