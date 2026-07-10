"""One declaration of every signal, shared by both engines.

Phase 2 of the engine unification. `momentum/signals.py` (daily as-of cadence)
and `asset_pipeline/signals.py` (month-end cadence) both derive their signal
definitions from `registry.SIGNALS` instead of declaring their own, so the two
batteries cannot drift apart unnoticed again.

Read `registry.py`'s docstring before adding a signal — especially the part about
`key` vs `name`, and the two measured collisions.
"""
from .context import MonthEndCtx
from .registry import (
    PARITY,
    SIGNALS,
    Cadence,
    Group,
    Parity,
    SignalSpec,
    Units,
    by_cadence,
    by_name,
    colliding_names,
    legacy_defs,
)

__all__ = [
    "PARITY",
    "SIGNALS",
    "Cadence",
    "Group",
    "MonthEndCtx",
    "Parity",
    "SignalSpec",
    "Units",
    "by_cadence",
    "by_name",
    "colliding_names",
    "legacy_defs",
]
