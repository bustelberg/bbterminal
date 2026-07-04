update ingest_run set status='error', finished_at=now(), error_summary='killed'
where status='running';