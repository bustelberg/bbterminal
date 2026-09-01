-- RETIRE THE CRM "ALLE RELATIES" FEATURE ENTIRELY (2026-09-01, on request).
--
-- ⚠⚠ THIS IS A REAL DATA LOSS AND NOTHING LEFT IN THIS REPO CAN UNDO IT. The job that filled these
-- tables goes in the same change (`crm_relaties_refresh`, `backend/airs_crm.py`,
-- `airs_scanner.download_crm_relaties_sync`), so there is no code left that could refill them.
-- Recovering the data means re-exporting it from AirSPMS by hand.
--
-- ⚠ `airs_crm_relatie` HAD NO READERS AT ALL — grepped repo-wide before dropping. It was written
-- daily by the refresh job and read by nothing: no endpoint, no view, no script. Its parsed, typed
-- columns existed for a query nobody ever wrote.
--
-- ⚠⚠ `airs_crm_relaties_raw` DID HAVE ONE, AND POSSIBLY ONE MORE THAN THIS REPO CAN SEE. In here it
-- backed `GET /api/airs/crm-relaties` and the "CRM Alle relaties" card on the AIRS page, both
-- removed in this change. Its own creating migration (`20260623050000`) also recorded an intent
-- this codebase cannot verify — "so another site can read it straight from Supabase and decode the
-- original file". Nothing in this repo is that reader. If one exists it is external, it
-- authenticates with the service key (RLS here is deny-all), and it starts getting nothing the
-- moment this runs.
--
-- ⚠⚠ THE RUN HISTORY IS DELIBERATELY LEFT BEHIND. The obvious tidy-up is
-- `delete from scheduled_job_run where job_id = 'crm_relaties_refresh'`, and that table's own
-- migration refuses it in as many words: `job_id` is text rather than a foreign key precisely so
-- that "a job removed from the code must leave its history behind (that IS the evidence it used to
-- run), not cascade it away". It also cannot strand a card: `/schedule` renders one row per
-- DECLARED job and one per REGISTERED job, and an id in neither list is simply never looked up.

drop table if exists airs_crm_relatie;
drop table if exists airs_crm_relaties_raw;

notify pgrst, 'reload schema';
