"""Hit the real ASGI app (routing, body parsing, serialization) for the AEX overlay."""
import json
import deps  # noqa: F401
from fastapi.testclient import TestClient

import routers.auth as auth
auth.verify_token = lambda *a, **k: {"id": "probe", "email": "probe@x", "app_metadata": {"role": "admin"}}
import routers._authz as authz
authz.is_admin_request = lambda *a, **k: True

import main
c = TestClient(main.app)
H = {"Authorization": "Bearer probe"}

for path in ("margin-inputs", "fcf-sbc-yield-inputs", "gross-margin-inputs"):
    for body in ({"universe": "AEX", "cadence": "annual"},
                 {"universe": "SP500", "cadence": "annual"}):
        r = c.post(f"/api/earnings/{path}", json=body, headers=H)
        if r.status_code != 200:
            print(f"{path:22s} {body['universe']:6s} -> {r.status_code} {r.text[:160]}")
            continue
        d = r.json()
        rows = d.get("rows", [])
        withdata = sum(1 for x in rows if x.get("revenue") or x.get("fcf"))
        print(f"{path:22s} {body['universe']:6s} -> 200, {len(rows)} rows, "
              f"{withdata} with data, years={len(d.get('years', []))}")
