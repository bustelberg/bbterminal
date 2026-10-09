"""Status semantics for the frozen Leonteq Yahoo mapping audit."""

from routers.leonteq import _mapping_status


def test_yahoo_mapping_requires_a_healthy_symbol() -> None:
    assert _mapping_status(None) == "unmapped"
    assert _mapping_status({"status": "ok"}) == "unmapped"
    assert _mapping_status({"status": "error", "analysis_symbol": "ANDR.VI"}) == "unmapped"


def test_yahoo_mapping_only_marks_checked_identity_verified() -> None:
    assert _mapping_status({"status": "ok", "analysis_symbol": "ANDR.VI", "identity_status": "verified"}) == "verified"
    assert _mapping_status({"status": "ok", "analysis_symbol": "XOM", "identity_status": "mismatch"}) == "review"


def test_human_verified_leonteq_identity_exception_is_verified() -> None:
    assert _mapping_status({
        "isin": "DK0010244508", "status": "ok", "analysis_symbol": "MAERSK-B.CO", "identity_status": "mismatch",
    }) == "verified"
