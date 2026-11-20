"""Environment boundary for automatic work; manual operations remain explicit."""
from __future__ import annotations

import os


def production_automation_enabled() -> bool:
    """Fail closed locally, including when a local DB has production defaults.

    APP_ENV explicitly overrides deployment detection. A Railway environment
    name takes precedence over its public domain so staging stays quiet too.
    DISABLE_SCHEDULER remains the kill switch on non-worker replicas.
    """
    if os.environ.get('DISABLE_SCHEDULER', '').strip().lower() in {'1', 'true', 'yes'}:
        return False
    environment = os.environ.get('APP_ENV') or os.environ.get('RAILWAY_ENVIRONMENT_NAME')
    if environment:
        return environment.strip().lower() == 'production'
    return bool(os.environ.get('RAILWAY_PUBLIC_DOMAIN', '').strip())
