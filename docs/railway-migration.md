# Migrating BBTerminal to Railway

Status: **plan only, nothing started.** Written 2026-08-11.

---

## 1. What "migrate to Railway" actually means

> ⚠ **The trap: this is NOT "move Postgres to Railway".** Railway gives you Postgres and
> containers. Supabase gives you Postgres **plus three HTTP services**, and the codebase talks to
> all three — not to Postgres directly.

Measured surface in this repo:

| Supabase service | Who depends on it | Size of the dependency |
|---|---|---|
| **PostgREST** (`/rest/v1`) | backend | **169 files** import the client; 14 RPCs |
| **GoTrue / Auth** (`/auth/v1`) | backend + frontend | 62 backend call sites; **all 26** frontend `supabase.*` calls |
| **Storage** (`/storage/v1`) | backend | 3 buckets: `gurufocus-raw`, `longequity-raw`, `backtest-results` |

So there are two real options, and only one is sane:

- **A. Run the Supabase OSS services on Railway.** They are published Docker images; Railway runs
  Docker images. `SUPABASE_URL` + `SUPABASE_SERVICE_KEY` repoint and **the application code does
  not change**. ← *recommended*
- **B. Rewrite off Supabase** (SQLAlchemy/asyncpg + hand-rolled JWT auth + S3). That is 169 files
  of data access, an auth system, and a storage layer. Months, and every one of the traps recorded
  in `CLAUDE.md` gets re-litigated against new code.

**This document plans option A.**

### Why it is genuinely feasible here

Verified against the repo, not assumed:

- **No `CREATE EXTENSION` in any of the 109 migrations.** No pg_cron, pg_net or vault usage. The
  schema is ordinary Postgres.
- **No Realtime.** No Edge Functions (`supabase/` holds only `config.toml`, `migrations`,
  `seed.sql`, `snippets`).
- **The frontend uses supabase-js for AUTH ONLY** — 26 calls, every one `auth.*`, zero `.from()`,
  `.rpc()` or `.storage`. Data goes through the backend via `apiFetch`. So the frontend needs
  GoTrue and nothing else.
- **The backend client is built from two env vars** (`deps.py`: `create_client(SUPABASE_URL,
  SUPABASE_SERVICE_KEY)`), behind a lazy proxy. One place to repoint.
- Supabase-specific SQL is limited to GRANTs to `service_role` / `anon` / `authenticated` and a
  single `auth.users` reference.

### What is genuinely new work

> ⚠ **A GATEWAY IS REQUIRED, AND IT IS THE ONE PIECE THAT DOES NOT EXIST TODAY.** Both supabase-py
> and supabase-js derive `/rest/v1`, `/auth/v1` and `/storage/v1` from **one base URL**. Railway
> routes by **domain per service**, not by path. So something must map paths to services, or
> `SUPABASE_URL` cannot be a single value and the "no code changes" property is lost.
> Supabase uses Kong; a ~20-line Caddy service is simpler and enough.

---

## 2. Phases

### Phase 0 — Prove the stack locally first (do not skip)

Build the exact service set as a `docker-compose.railway.yml` on your machine and point your
**local** backend at it. Everything below gets de-risked here, where a mistake costs nothing.

Success criterion: `cd backend && uv run uvicorn main:app` with `SUPABASE_URL` pointing at the
local gateway, and `/management-dashboard` works — log in, load portfolios, open the Analyse modal,
fetch a dividend blob from Storage.

⚠ This is also where you find out whether a service version is incompatible, which is a very
different experience at 2pm locally than at 2am against production.

### Phase 1 — Postgres

Use the **`supabase/postgres` image**, not Railway's stock Postgres template. It ships the roles
(`anon`, `authenticated`, `service_role`, `authenticator`), the `auth`/`storage` schemas, and the
search-path/GRANT conventions the 109 migrations already assume. Stock Postgres means recreating
all of that by hand, and the failure mode is the one already recorded in memory: *"permission
denied 42501" with BYPASSRLS=true means a missing table GRANT, not RLS*.

- Attach a Railway **volume**; set `PGDATA` onto it.
- Size it with headroom: prod is ~27 GB today and a clone peaks well above that (see the disk
  preflight in `scripts/clone-local-to-prod.ps1`).

### Phase 2 — The three services + gateway

| service | image | notes |
|---|---|---|
| `postgrest` | `postgrest/postgrest` | `PGRST_DB_URI`, `PGRST_DB_SCHEMA=public`, `PGRST_DB_ANON_ROLE=anon`, `PGRST_JWT_SECRET` |
| `auth` | `supabase/gotrue` | `GOTRUE_DB_DATABASE_URL`, `GOTRUE_SITE_URL`, `GOTRUE_JWT_SECRET`, SMTP |
| `storage` | `supabase/storage-api` | backend = S3 or file; see Phase 4 |
| `gateway` | `caddy` | path-routes the three under one hostname |

⚠ **Set `PGRST_DB_MAX_ROWS` deliberately.** The silent 1,000-row truncation that has bitten this
codebase repeatedly (`project_postgrest_max_rows_trap`) is *this setting*. Self-hosting means you
choose it — but **do not "fix" the trap by raising it**, because every reader that pages correctly
is written against a cap existing. Match production's 1,000 so local, staging and prod behave
identically; a reader that only works at 10,000 is a reader that will fail in prod.

Caddy sketch:

```
:8000 {
    handle /rest/v1/*    { uri strip_prefix /rest/v1    ; reverse_proxy postgrest:3000 }
    handle /auth/v1/*    { uri strip_prefix /auth/v1    ; reverse_proxy auth:9999 }
    handle /storage/v1/* { uri strip_prefix /storage/v1 ; reverse_proxy storage:5000 }
}
```

### Phase 3 — JWT keys

> ⚠ **YOUR CURRENT ANON AND SERVICE KEYS WILL NOT WORK AND CANNOT BE CARRIED OVER.** They are JWTs
> signed with the hosted project's `JWT_SECRET`. Self-hosting means minting your own: choose a new
> `JWT_SECRET`, then sign an `anon` and a `service_role` token with it. Every service
> (`PGRST_JWT_SECRET`, `GOTRUE_JWT_SECRET`, storage) must share that secret, and
> `NEXT_PUBLIC_SUPABASE_ANON_KEY` + `SUPABASE_SERVICE_KEY` must be regenerated.

⚠ **Existing user sessions are invalidated by this** — everyone is logged out once at cutover.
That is expected; say so rather than debugging it. Passwords are **not** affected (bcrypt hashes
live in `auth.users` and migrate with the data).

### Phase 4 — Data + Storage

**Database.** `pg_dump` → restore. Include the **`auth` schema**, or every user has to re-register.

```
pg_dump --no-owner --no-acl -n public -n auth -n storage -Fc <prod> > bb.dump
pg_restore --no-owner --no-acl -d <railway> bb.dump
```

⚠ **Restore, then `REINDEX`.** A restore builds indexes by insertion — the same bloat that took
`metric_data`'s indexes to 14.8 GB after the aborted clone. Reindex once at the end, on the new
box, before cutover, while nobody is watching.

**Storage.** ⚠ **This is the decision with no default.** Supabase-hosted Storage is S3-backed;
Railway has no object store. Two options:

- **External S3** (Cloudflare R2, Backblaze B2) — `storage-api` in S3 mode. Recommended: object
  storage is what this is, egress from R2 is free, and it survives a Railway service being
  recreated.
- **Railway volume** in file mode — simpler, but it is now a stateful service you must back up
  yourself, and volumes are not shared across environments.

Copy the bytes with the same name-diff-then-move approach `clone-local-to-prod.ps1` already uses
for the `backtest-results` bucket (names over SQL, bytes over HTTP).

### Phase 5 — Migrations

`npx supabase db push` targets a Supabase project. Against Railway you need a runner. Cheapest
path that keeps the existing files and ordering: keep `supabase/migrations/*.sql` as the source of
truth and apply them with `psql` in a deploy step, tracking applied versions in
`supabase_migrations.schema_migrations` exactly as today (the clone script already writes that
table, so the format is known).

⚠ Do not let a migration run concurrently on two replicas — take an advisory lock in the runner.

### Phase 6 — Frontend

Move Next.js off Vercel only if you want to; **it is independent of everything above** and Vercel
is genuinely good at it. If you do move it, Railway serves Next fine (`npm run build` +
`npm start`, or the standalone output). Env vars are the same three
(`NEXT_PUBLIC_SUPABASE_URL` → the gateway, `NEXT_PUBLIC_SUPABASE_ANON_KEY` → the new anon JWT,
`NEXT_PUBLIC_API_URL` → the backend service).

**Domains — yes, you get the Vercel equivalent.** Railway gives every service a
`*.up.railway.app` subdomain, and custom domains are per-service **per environment**, e.g.
`bbterminal.up.railway.app` (production) and `bbterminal-staging.up.railway.app` (staging).

### Phase 7 — Environments

Railway **Environments** are the actual prize here. Create `production` and `staging` in one
project; each gets its own full service set and its own variables. Staging gets a smaller Postgres
volume and a periodically-restored prod dump.

⚠ **Point staging at its own database, and prove it.** A staging backend still holding
production's `SUPABASE_URL` is the single most dangerous configuration in this whole plan — it
looks exactly like a working staging environment right up until it writes.

### Phase 8 — Cutover

1. Freeze writes (disable the scheduler: `DISABLE_SCHEDULER=1`).
2. Final `pg_dump` → restore → **REINDEX** → `ANALYZE`.
3. Copy any Storage objects added since the bulk copy.
4. Repoint `NEXT_PUBLIC_*` and redeploy the frontend.
5. Verify: log in, `/management-dashboard`, one Analyse modal, one dividend fetch, one scheduler
   tick, one ingest run.
6. **Keep the Supabase project alive and paid for two weeks.** Rollback = repointing env vars back,
   and that only works while the old database still exists.

---

## 3. What you gain, what you take on

**Gain**

- **Co-location.** Backend and DB in one region on one network. Today every PostgREST call crosses
  the internet — ~40 ms laptop→Supabase, and the endpoints that make 100+ round trips pay it every
  time. This is the largest real win and it is a latency win, not a reliability one.
- **A staging environment**, which does not exist today at all.
- One bill, one dashboard, one deploy story.
- No Supavisor. (Which also removes the pooler quirks recorded in
  `project_supavisor_strips_startup_options` — `PGAPPNAME` overwritten, `PGOPTIONS` stripped.)

**Take on**

- ⚠ **Backups become your job.** This is the biggest item and the easiest to defer until it
  matters. Supabase does daily backups (and PITR on Pro) today. On Railway you need a scheduled
  `pg_dump` to off-box storage **and a restore you have actually tested**. An untested backup is a
  belief, not a backup.
- Upgrades of Postgres / GoTrue / PostgREST / Storage become yours.
- No SQL editor, no auth UI, no dashboard — today's incident was diagnosed partly through them.
- More moving parts to be down at once.

---

## 4. Honest recommendation

**Today's outage was not Supabase's fault.** An aborted 60M-row transfer into a nearly-full
database, leaving 4M dead tuples and 14.8 GB of bloated indexes, behaves the same on any host.
Migrating for reliability reasons would be solving the wrong problem, and self-hosting *adds*
reliability surface.

Migrate for the reasons that are real: **co-location and staging**.

⚠ **The cheaper path to the same two wins is worth pricing first**: Supabase Pro (bigger disk,
PITR, daily backups) + a second Supabase project as staging, with the backend moved to Railway's
region nearest the database. That gets staging and most of the latency win, keeps managed backups,
and takes days rather than weeks.

If you want everything in one place and are willing to own backups, the plan above is sound and
the codebase is unusually well-suited to it — no extensions, no Realtime, auth-only frontend, and
one env var to repoint.

---

## 5. Suggested order

1. Phase 0 locally — a weekend, and it answers most unknowns.
2. Railway project, `staging` environment only. Restore a prod dump. Run it in parallel for a week.
3. Set up and **test a restore** from the automated backup before production ever moves.
4. Only then, Phase 8.
