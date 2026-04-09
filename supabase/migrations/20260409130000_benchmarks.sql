-- ============================================================
-- Benchmarks — ETF/index benchmarks for backtest comparison
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS benchmark_id_seq;

CREATE TABLE IF NOT EXISTS benchmark (
  benchmark_id    INTEGER PRIMARY KEY DEFAULT nextval('benchmark_id_seq'),
  ticker          VARCHAR NOT NULL UNIQUE,
  name            VARCHAR NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Benchmark prices stored separately from company metric_data
CREATE TABLE IF NOT EXISTS benchmark_price (
  benchmark_id    INTEGER NOT NULL REFERENCES benchmark(benchmark_id) ON DELETE CASCADE,
  target_date     DATE NOT NULL,
  price           DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (benchmark_id, target_date)
);
