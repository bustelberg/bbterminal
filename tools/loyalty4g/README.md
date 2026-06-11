# loyalty4g logo upload

One-off operational tooling (not part of the app, not run by CI) to push the
Leonteq company logos into the **loyalty4g** Sonata admin as "shares" (Aandelen).
Uses the logo data already produced by the backend in
`backend/leonteq_logos/` (`companies.json` manifest + `images/`).

Run everything **from the repo root** — the scripts default to root-relative
paths (`backend/leonteq_logos/...`, `loyalty4g.creds`).

## Pipeline

1. **`upscale_logos.py`** — ensure every logo PNG is ≥ 400×400 (the upload
   rejects smaller). Upscales real logos; regenerates tiny favicons as
   first-letter badges.
   ```sh
   uv run --with pillow python tools/loyalty4g/upscale_logos.py --check   # report
   uv run --with pillow python tools/loyalty4g/upscale_logos.py           # fix in place
   ```

2. **`login.py`** — log in to the Symfony `form_login` site and (optionally)
   save the session cookies.
   ```sh
   uv run --project backend python tools/loyalty4g/login.py --browser
   ```

3. **`upload.py`** — Playwright automation that creates a share + attaches the
   logo, driven by the logged-in session. `--explore` is read-only.
   ```sh
   uv run --project backend python tools/loyalty4g/upload.py --name "2G Energy AG" \
       --logo backend/leonteq_logos/images/86505899.png
   ```

## Credentials

Resolved in order: CLI args → `LOYALTY4G_USERNAME`/`LOYALTY4G_PASSWORD` env →
a `KEY=VALUE` creds file (default `./loyalty4g.creds`, gitignored).
