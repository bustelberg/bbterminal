UPDATE ingest_run
  SET status = 'error',
      current_phase = 'done',
      finished_at = NOW(),
      error_summary = 'Orphaned by backend restart (manually marked, was stuck for 5d 18h)'
  WHERE run_id = 6;