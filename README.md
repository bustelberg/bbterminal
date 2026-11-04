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

### 3. Start the backend

```bash
cd backend
uv sync                   # install dependencies via uv tool
uv run uvicorn main:app --reload --port 8000
```

Backend runs at `http://localhost:8000`.

### 4. Start the frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend runs at `http://localhost:3000`.

### Environment variables

Use the local project values from `supabase status`. The backend loads `backend/.env` first and then overrides it with `backend/.env.local`. Use the latter for local development so a production default does not select the hosted database.

```bash
# frontend — create frontend/.env.local
NEXT_PUBLIC_SUPABASE_URL=http://127.0.0.1:54321
NEXT_PUBLIC_SUPABASE_ANON_KEY=...
NEXT_PUBLIC_API_URL=http://localhost:8000

# backend — create backend/.env.local
SUPABASE_URL=http://127.0.0.1:54321
SUPABASE_SERVICE_KEY=...
# Optional: a shared Redis connection for benchmark Graphs responses across Railway replicas.
# Leave unset for local/single-process development; it falls back to the in-memory cache.
BLEND_CACHE_REDIS_URL=redis://...
```

---

## Useful commands

```bash
# Frontend: lint / typecheck / unit and component tests
cd frontend && npm run lint
cd frontend && npx tsc --noEmit
cd frontend && npm test          # vitest
cd frontend && npm run e2e        # Playwright (runs a production build)

# Backend: unit tests (momentum engine)
cd backend && uv run pytest tests/

# Build frontend for production (local check)
cd frontend && npm run build

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
