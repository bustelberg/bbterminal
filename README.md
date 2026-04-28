# BBTerminal

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

Copy and fill in your env files:

```bash
# frontend — create frontend/.env.local
NEXT_PUBLIC_SUPABASE_URL=...
NEXT_PUBLIC_SUPABASE_ANON_KEY=...

# backend — create backend/.env
SUPABASE_URL=...
SUPABASE_SERVICE_ROLE_KEY=...
```

---

## Useful commands

```bash
# Run frontend linter
cd frontend && npm run lint

# Build frontend for production (local check)
cd frontend && npm run build

# Stop local Supabase
supabase stop

# Reset local Supabase DB and re-run migrations
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