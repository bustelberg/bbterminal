-- Win #2: DB-persisted panel cache for /signal-breakdown.
--
-- The in-process `_BREAKDOWN_PANEL_CACHE` LRU in signals.py serves
-- /signal-breakdown clicks in <500ms once warmed, but loses every
-- entry on Railway redeploy. This table persists computed panels
-- across restarts AND lets them be shared between concurrent backend
-- replicas if we ever scale out.
--
-- Cache key: (universe_label, index_universe, cutoff_date). Both
-- label columns are nullable — a universe can be addressed by one or
-- the other (or both); the unique index uses COALESCE so NULL +
-- NULL counts as duplicate.
--
-- Panels are stored as JSONB list-of-records (one entry per company
-- in the universe at the cutoff). Typical size 30-60 KB serialized
-- per panel. No TTL column: panels are pure functions of data
-- BEFORE cutoff_date, so a panel for 2026-04-01 stays valid forever
-- under normal operation. Backfills that retroactively add prices
-- pre-cutoff are the rare invalidation event; in that case the
-- operator can `DELETE FROM panel_cache WHERE cutoff_date <= X`
-- manually. Cheaper than carrying a freshness check on every read.

CREATE TABLE public.panel_cache (
    cache_id bigserial PRIMARY KEY,
    universe_label text,
    index_universe text,
    cutoff_date date NOT NULL,
    panel_jsonb jsonb NOT NULL,
    n_companies integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

-- Uniqueness over the (label, index_universe, cutoff) tuple. COALESCE
-- so NULL collisions are caught (PG's default treats NULL as
-- distinct, which would let duplicate entries accumulate).
CREATE UNIQUE INDEX panel_cache_key_uniq ON public.panel_cache (
    COALESCE(universe_label, ''),
    COALESCE(index_universe, ''),
    cutoff_date
);

-- Secondary index for "what's stored under this universe?" diagnostic
-- queries (ordered scan of all entries for a given universe).
CREATE INDEX panel_cache_universe_idx ON public.panel_cache (
    COALESCE(universe_label, ''),
    COALESCE(index_universe, ''),
    cutoff_date DESC
);

-- RLS: panel data is non-sensitive (it's derived from public-domain
-- prices + universe membership), but follow the project's deny-all
-- default + service-role grant pattern. Reads/writes are via the
-- service key from the backend; users never hit this table directly.
ALTER TABLE public.panel_cache ENABLE ROW LEVEL SECURITY;
CREATE POLICY panel_cache_deny_all ON public.panel_cache FOR ALL USING (false);

GRANT SELECT, INSERT, UPDATE, DELETE ON public.panel_cache TO service_role;
GRANT USAGE, SELECT ON SEQUENCE public.panel_cache_cache_id_seq TO service_role;
