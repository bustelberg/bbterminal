# Local environment setup

For job failures, stale data, and partial results, see
[Refresh troubleshooting](troubleshooting.md).

Start with a local Supabase stack and an isolated local database. Do not copy
production environment files or connect a development server to production.
Even with the scheduler disabled, startup hooks can update mappings, enqueue
resolver entries, and reset interrupted backfills. Manual refresh actions also
write data and may contact external providers.

## First setup

1. Install the prerequisites in the [README](../README.md#local-development),
   including a running Docker-compatible container runtime for local Supabase.
2. From the repository root, run `supabase start`, then `supabase status`.
3. Copy [backend/.env.example](../backend/.env.example) to `backend/.env.local`
   and [frontend/.env.example](../frontend/.env.example) to `frontend/.env.local`.
   Use `cp` in Bash or `Copy-Item` in PowerShell; keep existing local files if
   already configured, and compare their values with the templates instead.
4. Replace the two key placeholders with the local service-role key (backend)
   and local anon key (frontend) shown by `supabase status`. Confirm the API and
   database URLs match that same stack; custom port mappings require edits.
5. Start the backend and frontend using the README commands. Check that the
   backend startup line `[deps] SUPABASE_URL` names your local instance, and that
   the scheduler reports it is disabled. Restart the dev servers after env edits.

The backend loads `backend/.env` first and `backend/.env.local` second, with
the latter overriding existing values, including shell variables. Paths are
relative to the backend module, independent of the launch directory. Optional
blank values in the template deliberately clear matching defaults from `.env`.
Next.js loads the frontend's `.env.local`; `NEXT_PUBLIC_*` values are public
browser configuration, not secrets. A root `.env.local` is not a substitute for
these two files. Remove inherited deployment markers such as
`RAILWAY_PUBLIC_DOMAIN` from your local shell.

## Required configuration

| Variable | Location | Purpose / source |
| --- | --- | --- |
| `SUPABASE_URL` | Backend | Local API URL from `supabase status`. |
| `SUPABASE_SERVICE_KEY` | Backend only | Local service-role key; privileged database access. |
| `NEXT_PUBLIC_SUPABASE_URL` | Frontend | Same local API URL as the backend. |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | Frontend | Local anon key; never the service-role key. |
| `NEXT_PUBLIC_API_URL` | Frontend | Backend origin, normally `http://localhost:8000`. |
| `DATABASE_URL` | Backend | Local Postgres connection for COPY reads and job locks. Some routes fall back without it; controlled Analyse preparation needs it. |
| `SUPABASE_DB_URL` | Backend | Optional alternative; takes precedence over `DATABASE_URL`. Leave blank with the template's local `DATABASE_URL`. |

Sign in with a user belonging to the local Supabase Auth instance. Empty local
tables do not contain production holdings or provider snapshots; arrange an
approved development dataset when working on those features. Admin access is
determined by the application's auth policy, not by an env flag. Unit tests use
fakes/frozen fixtures and do not need live credentials or a running database;
see [Testing](testing.md).

## Background work disabled locally

| Setting | Template value | Effect |
| --- | --- | --- |
| `DISABLE_SCHEDULER` | `1` | Stops the in-process scheduler, including daily jobs, startup kickstart, retry timers, and scheduled queue draining. |
| `DISABLE_BOOT_HEAL` | `1` | Disables boot-time retries of broken jobs if the scheduler is later enabled. |
| `ASSET_QUEUE_INPROCESS` | `0` | Keeps the scheduled asset resolver queue off if the scheduler is later enabled. |
| `SECTOR_TIMELINE_PREWARM` | `0` | Disables sector-timeline startup prewarming. |
| `BLEND_PREWARM` | Empty | Disables blend prewarming; an empty value is intentional. |
| `DISABLE_LAZY_REPRICE` | `1` | Disables lazy strategy snapshot repricing. This flag checks presence: remove it or leave it empty to re-enable; `0` still disables it. |

Enable only the functionality needed for the task, against development services.
To exercise the scheduler, set `DISABLE_SCHEDULER=0`; retain the other switches
unless intentionally testing their work. Disabling the scheduler does not block
manual job endpoints or every startup hook, and is not an offline mode.

## Optional integrations

Obtain development credentials from the project owner through the team's secret
sharing process. Keep them in backend configuration only. Ordinary UI work and
unit tests do not require these integrations.

| Variables | Needed for |
| --- | --- |
| `BROKER_USERNAME`, `BROKER_PASSWORD` | AIRS/AirSPMS scans. Browser-based scans also require `uv run playwright install chromium` from `backend/`; Playwright is a runtime scraper dependency, not a test suite. |
| `GURUFOCUS_API_KEY` | GuruFocus fundamentals and related ingestion. |
| `OPENFIGI_API_KEY` | Authenticated OpenFIGI resolution; anonymous requests are supported with lower limits. |
| `LEONTEQ_USER`, `LEONTEQ_PASSWORD` | Leonteq portal features; legacy aliases are `LYNQS_USER` / `LYNQS_PASS`. |
| `LONGEQUITY_BASE_URL`, `LONGEQUITY_FILENAME_TEMPLATE` | LongEquity source report acquisition; request the development URL/template from the owner. |
| `ANTHROPIC_API_KEY` | Assisted index listing-override discovery. |
| `CRON_SECRET` | Authenticated external cron calls; leave blank when not testing cron. |
| `BLEND_CACHE_REDIS_URL` | Optional shared blend cache; blank uses the in-memory fallback. |
| `CORS_ORIGINS` | Additional frontend origins; standard localhost/127.0.0.1 ports 3000 and 3001 are already allowed. |

Yahoo price downloads do not require an API key but still use the network and
provider rate limits. The templates do not authorize production access.
