"""Request-scoped narration for portfolio refresh jobs; ordinary reads stay quiet."""
from contextlib import contextmanager
from contextvars import ContextVar

_report = ContextVar('leonteq_refresh_progress', default=None)


def report(english: str, dutch: str | None = None):
    callback = _report.get()
    if callback:
        callback(english, dutch or english)


@contextmanager
def observe(callback):
    token = _report.set(callback)
    try:
        yield
    finally:
        _report.reset(token)
