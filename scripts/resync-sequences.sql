-- Find (and fix) every identity/serial sequence in `public` that has fallen behind its table.
--
-- WHY THIS EXISTS
-- ---------------
-- A sequence that has fallen behind its table does not fail at read time and does not fail
-- at deploy time. It fails on the NEXT INSERT, with
--
--     duplicate key value violates unique constraint "<table>_pkey"
--     Key (id)=(2899) already exists.
--
-- and everything that inserts into that table stops working at once. Measured in production
-- 2026-09-07: `ingest_run` drifted, so `_create_run` raised on every call -- which meant the
-- scheduler could not open a run row and NEITHER COULD ANY BUTTON. The visible symptom was a
-- /schedule page reporting `daily_pipeline` 26 days stale beside a Rebalance button that
-- flashed "Starting..." and did nothing: two surfaces both looking like a scheduler fault
-- when the schedule was never the problem.
--
-- HOW IT HAPPENS: rows inserted with EXPLICIT ids do not advance the sequence. A
-- local-to-prod clone does exactly that (`scripts/clone-local-to-prod.ps1` step [7b] is the
-- reset that is supposed to follow it), as does any hand-written INSERT naming the id
-- column, and any restore from a data-only dump.
--
-- HOW TO RUN -- two statements, both plain SELECTs. Run [1]; if it returns rows, run [2].
--
-- !! THEY RETURN ROWS RATHER THAN RAISING NOTICES, AND THAT IS THE WHOLE POINT. The obvious
-- shape for this is a DO block with RAISE NOTICE, and it is silently useless in the place it
-- actually gets run: the Supabase SQL editor DOES NOT DISPLAY NOTICES. It reports
-- "Success. 0 rows returned" for a database with a drifted sequence and for a healthy one
-- alike -- the same answer for the two opposite states, which is worse than no tool at all.
-- (psql shows them, which is exactly how this shipped: it was verified in the wrong client.)
--
-- !! AND NO psql BACKSLASH COMMANDS. `\if` / `\set` are a syntax error in the SQL editor, and
-- psql will not interpolate a :var inside a dollar-quoted block anyway. Plain SQL is the only
-- dialect both front-ends share.


-- ---------------------------------------------------------------------------------------
-- [1] REPORT -- read-only. Every sequence about to hand out an id that already exists.
--     No rows returned = nothing is drifted. That is the healthy answer.
-- ---------------------------------------------------------------------------------------
WITH cols AS (
    -- `pg_get_serial_sequence` MISSES a manually-created sequence (CREATE SEQUENCE + DEFAULT
    -- nextval, not OWNED BY -- e.g. company_id_seq), so fall back to parsing the default
    -- expression. Same discovery as `clone-local-to-prod.ps1`.
    SELECT c.table_name AS tbl,
           c.column_name AS col,
           COALESCE(
               pg_get_serial_sequence(format('public.%I', c.table_name), c.column_name),
               (regexp_match(c.column_default, 'nextval\(''([^'']+)'''))[1]
           ) AS seq
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema
     AND t.table_name   = c.table_name
     AND t.table_type   = 'BASE TABLE'          -- a VIEW has no MAX() to take
    WHERE c.table_schema = 'public'
      AND (c.is_identity = 'YES' OR c.column_default LIKE 'nextval(%')
), state AS (
    SELECT tbl, col, seq,
           -- Dynamic SQL inside a SELECT, without needing a function: `query_to_xml` runs the
           -- per-table MAX() and the xpath pulls the scalar back out.
           (xpath('/row/m/text()',
                  query_to_xml(format('SELECT COALESCE(MAX(%I), 0) AS m FROM public.%I', col, tbl),
                               false, true, '')))[1]::text::bigint AS max_id,
           -- The id this sequence would hand out NEXT. `pg_sequence_last_value` returns NULL
           -- when the sequence has never been called, in which case the next id is its start
           -- value -- not last_value + 1. Getting that wrong reports a fresh sequence as
           -- drifted by one, on every table, for ever.
           COALESCE(pg_sequence_last_value(seq::regclass) + 1,
                    (SELECT seqstart FROM pg_sequence WHERE seqrelid = seq::regclass)) AS next_id
    FROM cols
    WHERE seq IS NOT NULL
)
SELECT tbl                        AS "table",
       col                        AS "column",
       seq                        AS sequence,
       next_id                    AS would_hand_out,
       max_id                     AS max_id_in_table,
       (max_id - next_id + 1)     AS inserts_that_would_fail
FROM state
WHERE next_id <= max_id
ORDER BY tbl, col;


-- ---------------------------------------------------------------------------------------
-- [2] REPAIR -- run only if [1] returned rows. Returns one row per sequence it fixed.
--
--     SAFE: it only ever moves a sequence FORWARD to MAX(id). It never reassigns an id and
--     never touches a row, the WHERE means a sequence already ahead of its table is left
--     alone, and re-running it returns zero rows. Moving a sequence BACKWARDS would be the
--     dangerous direction (ids of deleted rows would be reissued), which is why the guard is
--     a WHERE on the set rather than a blanket setval over every sequence.
-- ---------------------------------------------------------------------------------------
WITH cols AS (
    SELECT c.table_name AS tbl,
           c.column_name AS col,
           COALESCE(
               pg_get_serial_sequence(format('public.%I', c.table_name), c.column_name),
               (regexp_match(c.column_default, 'nextval\(''([^'']+)'''))[1]
           ) AS seq
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema
     AND t.table_name   = c.table_name
     AND t.table_type   = 'BASE TABLE'
    WHERE c.table_schema = 'public'
      AND (c.is_identity = 'YES' OR c.column_default LIKE 'nextval(%')
), state AS (
    SELECT tbl, col, seq,
           (xpath('/row/m/text()',
                  query_to_xml(format('SELECT COALESCE(MAX(%I), 0) AS m FROM public.%I', col, tbl),
                               false, true, '')))[1]::text::bigint AS max_id,
           COALESCE(pg_sequence_last_value(seq::regclass) + 1,
                    (SELECT seqstart FROM pg_sequence WHERE seqrelid = seq::regclass)) AS next_id
    FROM cols
    WHERE seq IS NOT NULL
)
SELECT tbl     AS "table",
       col     AS "column",
       seq     AS sequence,
       next_id AS was_handing_out,
       max_id  AS max_id_in_table,
       -- GREATEST(max_id, 1) because a sequence cannot be set below its minimum;
       -- is_called := max_id > 0 so an EMPTY table's next insert gets id 1, not 2.
       setval(seq::regclass, GREATEST(max_id, 1), max_id > 0) AS sequence_set_to
FROM state
WHERE next_id <= max_id
ORDER BY tbl, col;
