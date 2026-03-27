CREATE OR REPLACE FUNCTION get_snapshot_company_ids(p_snapshot_id int)
RETURNS TABLE(company_id int) AS $$
  SELECT DISTINCT company_id
  FROM facts_number
  WHERE snapshot_id = p_snapshot_id;
$$ LANGUAGE sql STABLE;
