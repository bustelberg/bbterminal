-- Index supporting GROUP BY company_id used by company_universe_labels() and
-- any future per-company lookups. Without this the aggregate becomes a full
-- sequential scan over the (universe, company, month) rows and the
-- /api/companies/memberships endpoint hangs for tens of seconds.

CREATE INDEX IF NOT EXISTS idx_universe_membership_company
  ON universe_membership (company_id);
