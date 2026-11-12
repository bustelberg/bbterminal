# BBTerminal

[![CI](https://github.com/bustelberg/bbterminal/actions/workflows/ci.yml/badge.svg)](https://github.com/bustelberg/bbterminal/actions/workflows/ci.yml)

A full-stack app with a **Next.js** frontend, **FastAPI** backend, and **Supabase** for auth/database.

---

## Stack

| Layer    | Tech                        | Deploy        |
|----------|-----------------------------|---------------|
| Frontend | Next.js 16 + Tailwind CSS   | Vercel        |
| Backend  | FastAPI + uvicorn (Python)  | Railway       |
| Database | Supabase (Postgres + auth)  | Supabase      |

---

## Local Development

### Prerequisites

- Node.js 20+
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- [Supabase CLI](https://supabase.com/docs/guides/cli)
- A running Docker-compatible container runtime for local Supabase

### 1. Clone & install

```bash
git clone https://github.com/<your-org>/bbterminal.git
cd bbterminal
```

### 2. Start Supabase locally

```bash
supabase start
```

This spins up a local Postgres instance, auth server, and Studio UI at `http://localhost:54323`.

### 3. Configure the local environment

Copy [backend/.env.example](backend/.env.example) to `backend/.env.local` and
[frontend/.env.example](frontend/.env.example) to `frontend/.env.local` if these
files do not already exist. Fill the key placeholders from `supabase status`.
Both apps must point to the same local Supabase stack. The backend template
disables scheduled jobs, queue draining, and prewarming by default.

See [Environment setup](docs/environment.md) for required credentials, optional
integrations, and how to enable jobs locally. Startup hooks and manual actions
can still write data, so use an isolated development database.

### 4. Start the backend

```bash
cd backend
uv sync                   # install dependencies via uv tool
uv run uvicorn main:app --reload --port 8000
```

Backend runs at `http://localhost:8000`.

### 5. Start the frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend runs at `http://localhost:3000`.

### Environment variables

The checked-in `.env.example` files contain placeholders and local defaults;
the `.env.local` files contain your actual configuration and are ignored by Git.
The backend loads `backend/.env` first, then overrides it with
`backend/.env.local`. Keep service-role keys, database passwords, and provider
credentials on the backend. `NEXT_PUBLIC_*` values are visible in the browser.
See the [configuration reference](docs/environment.md) for details.

For failed jobs, stale data, lock contention, or partial reports, use the
[refresh troubleshooting guide](docs/troubleshooting.md).
Before changing refresh orchestration, read the
[AIRS and scheduler execution map](docs/execution-paths.md).

---

## Useful commands

Run each command below from the repository root. Tests are unit-only: backend
pytest uses in-memory fakes or frozen fixtures, and frontend Vitest includes
component tests with a simulated DOM. Browser/e2e, live-service, and database-backed
tests are not part of this project. See [Testing](docs/testing.md) for the policy,
test tiers, and CI checks.

```bash
# Fast development checks (frontend and backend; excludes slow CPU tests)
npm run test:fast

# Frontend: lint / typecheck / unit and component tests
npm --prefix frontend run lint
(cd frontend && npx tsc --noEmit)
npm --prefix frontend test

# Backend: full unit suite and lint (same lint invocation as CI)
(cd backend && uv run pytest tests/)
(cd backend && uvx ruff check .)

# Production frontend build (packaging, separate from test verification)
npm --prefix frontend run build

# Refresh the FastAPI contract and generated frontend types after an API change
npm run api:sync

# Verify both generated contract artifacts are committed
npm run api:check

# Stop local Supabase
supabase stop

# Destructive: ask before resetting the local Supabase database
supabase db reset

# Generate a new Supabase migration
supabase migration new <migration-name>
```

---

## Deploy to Production

The frontend deploys to **Vercel**, the backend to **Railway**. Both auto-deploy on push to `main` via their respective GitHub integrations.

### Push to prod via Git

```bash
git add .
git commit -m "your message"
git push origin main
```

### Manual deploy

```bash
# Frontend (Vercel CLI)
npm i -g vercel
cd frontend && vercel --prod

# Backend (Railway CLI)
npm i -g @railway/cli
cd backend && railway up
```

Environment variables are managed in each platform's dashboard, not in `.env` files.

For multiple backend replicas, provision a managed Redis instance and set
`BLEND_CACHE_REDIS_URL` on the backend service. Benchmark Graphs will then share their gzipped
responses across replicas and an ingest invalidates all of them through a shared generation key.
