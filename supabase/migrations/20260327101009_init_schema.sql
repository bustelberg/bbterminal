-- ------------------------------------------------------------
-- Sequences
-- ------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS company_id_seq;
CREATE SEQUENCE IF NOT EXISTS metric_id_seq;
CREATE SEQUENCE IF NOT EXISTS snapshot_id_seq;
CREATE SEQUENCE IF NOT EXISTS source_id_seq;
CREATE SEQUENCE IF NOT EXISTS portfolio_id_seq;

-- ------------------------------------------------------------
-- Dimensions
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS company (
  company_id        INTEGER PRIMARY KEY DEFAULT nextval('company_id_seq'),
  longequity_ticker VARCHAR,
  primary_ticker    VARCHAR NOT NULL,
  primary_exchange  VARCHAR NOT NULL DEFAULT 'UNKNOWN',
  country           VARCHAR,
  company_name      VARCHAR,
  sector            VARCHAR,
  UNIQUE (primary_ticker, primary_exchange)
);

CREATE TABLE IF NOT EXISTS metric (
  metric_id   INTEGER PRIMARY KEY DEFAULT nextval('metric_id_seq'),
  metric_code VARCHAR UNIQUE NOT NULL,
  value_type  VARCHAR NOT NULL
    CHECK (value_type IN ('number', 'text', 'bool', 'date'))
);

CREATE TABLE IF NOT EXISTS snapshot (
  snapshot_id   INTEGER PRIMARY KEY DEFAULT nextval('snapshot_id_seq'),
  target_date   DATE NOT NULL,
  published_at  DATE NOT NULL,
  imported_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (target_date, published_at)
);

CREATE TABLE IF NOT EXISTS source (
  source_id   INTEGER PRIMARY KEY DEFAULT nextval('source_id_seq'),
  source_code VARCHAR UNIQUE NOT NULL
);

-- ------------------------------------------------------------
-- Facts
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS facts_number (
  company_id    INTEGER NOT NULL,
  metric_id     INTEGER NOT NULL,
  snapshot_id   INTEGER NOT NULL,
  source_id     INTEGER NOT NULL,
  metric_value  DOUBLE PRECISION NOT NULL,
  is_prediction BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (company_id, metric_id, snapshot_id, source_id),
  FOREIGN KEY (company_id)  REFERENCES company(company_id),
  FOREIGN KEY (metric_id)   REFERENCES metric(metric_id),
  FOREIGN KEY (snapshot_id) REFERENCES snapshot(snapshot_id),
  FOREIGN KEY (source_id)   REFERENCES source(source_id)
);

CREATE TABLE IF NOT EXISTS facts_text (
  company_id   INTEGER NOT NULL,
  metric_id    INTEGER NOT NULL,
  snapshot_id  INTEGER NOT NULL,
  source_id    INTEGER NOT NULL,
  metric_value VARCHAR,
  PRIMARY KEY (company_id, metric_id, snapshot_id, source_id),
  FOREIGN KEY (company_id)  REFERENCES company(company_id),
  FOREIGN KEY (metric_id)   REFERENCES metric(metric_id),
  FOREIGN KEY (snapshot_id) REFERENCES snapshot(snapshot_id),
  FOREIGN KEY (source_id)   REFERENCES source(source_id)
);

-- ------------------------------------------------------------
-- Portfolio (snapshot-aligned)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS portfolio (
  portfolio_id   INTEGER PRIMARY KEY DEFAULT nextval('portfolio_id_seq'),
  portfolio_name VARCHAR NOT NULL,
  snapshot_id    INTEGER NOT NULL,
  UNIQUE (portfolio_name, snapshot_id),
  FOREIGN KEY (snapshot_id) REFERENCES snapshot(snapshot_id)
);

CREATE TABLE IF NOT EXISTS portfolio_weight (
  portfolio_id INTEGER NOT NULL,
  company_id   INTEGER NOT NULL,
  weight_value DOUBLE PRECISION NOT NULL CHECK (weight_value >= 0 AND weight_value <= 1),
  PRIMARY KEY (portfolio_id, company_id),
  FOREIGN KEY (portfolio_id) REFERENCES portfolio(portfolio_id),
  FOREIGN KEY (company_id)   REFERENCES company(company_id)
);