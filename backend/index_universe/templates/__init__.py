"""Universe template registry.

Each template is a class subclassing `UniverseTemplate` that knows how to
keep one canonical universe row continuously up to date. The pipeline's
ACWI/templates phase iterates this registry and calls `refresh()` on
each, producing one `current_picks_snapshot`-style diff entry per
template per run (assembled into `ingest_run.templates_summary`).

Adding a new template
---------------------
1. Implement a subclass of `UniverseTemplate` in a new module here
   (e.g. `sp500.py::SP500Template`).
2. Register it in the `TEMPLATES` dict below.
3. The pipeline + UI pick it up automatically — no other wiring.
"""
from __future__ import annotations

from .acwi import ACWITemplate
from .acwi_leonteq import ACWILeonteqTemplate
from .base import (
    ProgressCallback,
    RefreshResult,
    TemplateDiff,
    UniverseTemplate,
)
from .leonteq import LeonteqTemplate

# Order matters: ACWI_LEONTEQ is a derived template that reads from
# the ACWI and LEONTEQ universe_membership rows. Register it AFTER its
# two parents so the pipeline's templates phase refreshes the parents
# first; otherwise the intersection lags by one tick.
TEMPLATES: dict[str, type[UniverseTemplate]] = {
    ACWITemplate.template_key: ACWITemplate,
    LeonteqTemplate.template_key: LeonteqTemplate,
    ACWILeonteqTemplate.template_key: ACWILeonteqTemplate,
}


def get_template(template_key: str) -> UniverseTemplate:
    """Look up a template by its key. Raises `KeyError` (mapped to 404 by
    the HTTP layer) if unknown."""
    cls = TEMPLATES.get(template_key)
    if cls is None:
        raise KeyError(template_key)
    return cls()


def all_templates() -> list[UniverseTemplate]:
    """Instantiate every registered template, in registry order. Used by
    the pipeline's templates phase + the `/api/universe-templates` list
    endpoint."""
    return [cls() for cls in TEMPLATES.values()]


__all__ = [
    "ACWITemplate",
    "ACWILeonteqTemplate",
    "LeonteqTemplate",
    "ProgressCallback",
    "RefreshResult",
    "TEMPLATES",
    "TemplateDiff",
    "UniverseTemplate",
    "all_templates",
    "get_template",
]
