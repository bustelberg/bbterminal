-- Exchange to currency mapping (from GuruFocus API)
CREATE TABLE IF NOT EXISTS exchange_currency (
  exchange_code  VARCHAR PRIMARY KEY,
  country        VARCHAR NOT NULL,
  currency       VARCHAR NOT NULL,
  source         VARCHAR NOT NULL DEFAULT 'gurufocus'
);
