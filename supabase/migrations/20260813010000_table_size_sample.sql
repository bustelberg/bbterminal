-- HOW FAST THE DATABASE IS GROWING, AND WHICH TABLES ARE DOING IT.
--
-- ⚠⚠ BYTES, NOT ROWS WRITTEN — AND THE DIFFERENCE INVERTS THE ANSWER. The obvious instrumentation
-- is "count what each job inserts", and it would rank the CRM scrape (which OVERWRITES its whole
-- table: thousands of rows written, zero growth) above the month-end price refresh. Several jobs
-- here are delete-then-insert snapshots or upserts, so rows-written and disk-used are only loosely
-- related. Worse, a row count cannot see INDEXES or BLOAT, which on an 18 GB table are most of the
-- disk. `pg_total_relation_size` is exact, includes both, and is what the hosting bills on.
--
-- ⚠ MEASURED FROM OUTSIDE THE JOBS, ON PURPOSE. Nothing here asks a job to report anything, so no
-- job can forget to, none can drift, and a job added next month is covered the day it ships. The
-- cost is that this says WHAT grew and not WHO grew it; per-job attribution is a separate question
-- and a separate (lossier) measurement.
--
-- ⚠ POSTGRES ONLY. Supabase STORAGE — the `gurufocus-raw` bucket of cached vendor JSON — is not in
-- the database and is invisible here. A reader comparing this against the hosting's disk figure
-- will find a gap, and that is the gap.
create table if not exists table_size_sample (
  id            bigserial primary key,

  sampled_at    timestamptz not null default now(),
  table_name    text        not null,

  -- Heap + indexes + TOAST — the whole cost of the table on disk.
  total_bytes   bigint      not null,
  -- Split out so a table that is growing INDEXES rather than data is visible as such.
  table_bytes   bigint,
  index_bytes   bigint,

  -- ⚠ AN ESTIMATE, AND NAMED AS ONE. It is `pg_stat_user_tables.n_live_tup`, which is maintained
  -- by autovacuum and can be wildly stale — measured on this database it reported 48 rows for an
  -- 18 GB table. The honest alternative is `count(*)`, which on that table is minutes of I/O every
  -- day for a number nobody bills on. Bytes are the measure; this is a hint.
  rows_estimate bigint
);

-- The only query shape: one table's history, newest first.
create index if not exists table_size_sample_name_time
  on table_size_sample (table_name, sampled_at desc);

grant select, insert, update, delete on table_size_sample to anon, authenticated, service_role;
grant usage, select on sequence table_size_sample_id_seq to anon, authenticated, service_role;

comment on table table_size_sample is
  'Daily snapshot of every public table''s size on disk. Bytes, not rows written — several jobs '
  'overwrite or upsert, so rows written and growth are different questions. Excludes Supabase '
  'Storage.';


-- ⚠ A FUNCTION BECAUSE PostgREST CANNOT READ THE CATALOG. `pg_total_relation_size` and
-- `pg_stat_user_tables` are not tables the REST layer exposes, and reaching them would otherwise
-- mean a direct-Postgres connection — which the backend has only when SUPABASE_DB_URL is set, and
-- which silently falls back to PostgREST when it is not. A `security definer` RPC works on every
-- deployment through the one client the app already has.
create or replace function public.table_sizes()
returns table (
  table_name    text,
  total_bytes   bigint,
  table_bytes   bigint,
  index_bytes   bigint,
  rows_estimate bigint
)
language sql
stable
security definer
set search_path = public, pg_catalog
as $$
  select c.relname::text,
         pg_total_relation_size(c.oid)::bigint,
         pg_table_size(c.oid)::bigint,
         pg_indexes_size(c.oid)::bigint,
         coalesce(s.n_live_tup, 0)::bigint
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  left join pg_stat_user_tables s on s.relid = c.oid
  -- 'r' ordinary + 'p' partitioned. Views, indexes and sequences are excluded: an index's bytes
  -- are already inside its table's `pg_total_relation_size`, so listing them would double-count.
  where n.nspname = 'public' and c.relkind in ('r', 'p')
$$;

grant execute on function public.table_sizes() to anon, authenticated, service_role;


-- Growth over a window, computed in Postgres.
--
-- ⚠⚠ IT IS DONE HERE RATHER THAN IN PYTHON BECAUSE THE NAIVE READ TRUNCATES. Fetching the raw
-- samples and reducing them client-side is ~50 tables x N days of rows — over 1,000 within a
-- fortnight — and PostgREST silently caps a response at 1,000 rows on cloud. The growth figure
-- would quietly start being computed from a partial window, which is the exact failure mode this
-- codebase has been bitten by twice (see `_fx`, `_closes_paged`). One row per table, always.
--
-- ⚠ `earlier` IS THE NEWEST SAMPLE AT OR BEFORE THE CUTOFF, and it is NULL when the history does
-- not reach back that far. The caller must report that as "no baseline yet" — a NULL baseline read
-- as zero would present a brand-new install as a database that has not grown at all.
create or replace function public.table_growth(days integer default 7)
returns table (
  table_name    text,
  latest_bytes  bigint,
  latest_at     timestamptz,
  earlier_bytes bigint,
  earlier_at    timestamptz,
  rows_estimate bigint
)
language sql
stable
security definer
set search_path = public, pg_catalog
as $$
  with latest as (
    select distinct on (s.table_name)
           s.table_name, s.total_bytes, s.sampled_at, s.rows_estimate
    from table_size_sample s
    order by s.table_name, s.sampled_at desc
  ),
  earlier as (
    select distinct on (s.table_name) s.table_name, s.total_bytes, s.sampled_at
    from table_size_sample s
    where s.sampled_at <= now() - make_interval(days => days)
    order by s.table_name, s.sampled_at desc
  )
  select l.table_name, l.total_bytes, l.sampled_at,
         e.total_bytes, e.sampled_at, l.rows_estimate
  from latest l
  left join earlier e on e.table_name = l.table_name
$$;

grant execute on function public.table_growth(integer) to anon, authenticated, service_role;
