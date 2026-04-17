-- ============================================================
-- Normalized schema: country → currency → gurufocus_exchange
-- + fx_rate, universe, universe_membership, company_source
-- + company column renames, backtest_run consolidation
-- + drop legacy tables
-- ============================================================

-- ─── Country ───────────��────────────────────────────────────
CREATE TABLE IF NOT EXISTS country (
  country_code VARCHAR(2) PRIMARY KEY,
  country_name VARCHAR NOT NULL
);

INSERT INTO country (country_code, country_name) VALUES
  ('AE', 'United Arab Emirates'),
  ('AT', 'Austria'),
  ('AU', 'Australia'),
  ('BE', 'Belgium'),
  ('BR', 'Brazil'),
  ('CA', 'Canada'),
  ('CH', 'Switzerland'),
  ('CL', 'Chile'),
  ('CN', 'China'),
  ('CO', 'Colombia'),
  ('CZ', 'Czech Republic'),
  ('DE', 'Germany'),
  ('DK', 'Denmark'),
  ('EG', 'Egypt'),
  ('ES', 'Spain'),
  ('FI', 'Finland'),
  ('FR', 'France'),
  ('GB', 'United Kingdom'),
  ('GR', 'Greece'),
  ('HK', 'Hong Kong'),
  ('HU', 'Hungary'),
  ('ID', 'Indonesia'),
  ('IE', 'Ireland'),
  ('IL', 'Israel'),
  ('IN', 'India'),
  ('IS', 'Iceland'),
  ('IT', 'Italy'),
  ('JP', 'Japan'),
  ('KR', 'South Korea'),
  ('KW', 'Kuwait'),
  ('MX', 'Mexico'),
  ('MY', 'Malaysia'),
  ('NL', 'Netherlands'),
  ('NO', 'Norway'),
  ('NZ', 'New Zealand'),
  ('PH', 'Philippines'),
  ('PL', 'Poland'),
  ('PT', 'Portugal'),
  ('QA', 'Qatar'),
  ('RO', 'Romania'),
  ('RU', 'Russia'),
  ('SA', 'Saudi Arabia'),
  ('SE', 'Sweden'),
  ('SG', 'Singapore'),
  ('TH', 'Thailand'),
  ('TR', 'Turkey'),
  ('TW', 'Taiwan'),
  ('US', 'United States'),
  ('ZA', 'South Africa')
ON CONFLICT (country_code) DO NOTHING;

-- ─── Currency ─────────���─────────────────────────���───────────
CREATE TABLE IF NOT EXISTS currency (
  currency_code VARCHAR(3) PRIMARY KEY,
  currency_name VARCHAR NOT NULL,
  source        VARCHAR NOT NULL DEFAULT 'ecb',
  peg_to_usd   DOUBLE PRECISION
);

INSERT INTO currency (currency_code, currency_name, source, peg_to_usd) VALUES
  ('AED', 'UAE Dirham',           'pegged', 3.6725),
  ('AUD', 'Australian Dollar',    'ecb',    NULL),
  ('BRL', 'Brazilian Real',       'ecb',    NULL),
  ('CAD', 'Canadian Dollar',      'ecb',    NULL),
  ('CHF', 'Swiss Franc',          'ecb',    NULL),
  ('CLP', 'Chilean Peso',         'ecb',    NULL),
  ('CNY', 'Chinese Yuan',         'ecb',    NULL),
  ('COP', 'Colombian Peso',       'ecb',    NULL),
  ('CZK', 'Czech Koruna',         'ecb',    NULL),
  ('DKK', 'Danish Krone',         'ecb',    NULL),
  ('EGP', 'Egyptian Pound',       'ecb',    NULL),
  ('EUR', 'Euro',                 'ecb',    NULL),
  ('GBP', 'British Pound',        'ecb',    NULL),
  ('HKD', 'Hong Kong Dollar',     'ecb',    NULL),
  ('HUF', 'Hungarian Forint',     'ecb',    NULL),
  ('IDR', 'Indonesian Rupiah',    'ecb',    NULL),
  ('ILS', 'Israeli Shekel',       'ecb',    NULL),
  ('INR', 'Indian Rupee',         'ecb',    NULL),
  ('ISK', 'Icelandic Krona',      'ecb',    NULL),
  ('JPY', 'Japanese Yen',         'ecb',    NULL),
  ('KRW', 'South Korean Won',     'ecb',    NULL),
  ('KWD', 'Kuwaiti Dinar',        'pegged', 0.306),
  ('MXN', 'Mexican Peso',         'ecb',    NULL),
  ('MYR', 'Malaysian Ringgit',    'ecb',    NULL),
  ('NOK', 'Norwegian Krone',      'ecb',    NULL),
  ('NZD', 'New Zealand Dollar',   'ecb',    NULL),
  ('PHP', 'Philippine Peso',      'ecb',    NULL),
  ('PLN', 'Polish Zloty',         'ecb',    NULL),
  ('QAR', 'Qatari Riyal',         'pegged', 3.64),
  ('RON', 'Romanian Leu',         'ecb',    NULL),
  ('RUB', 'Russian Ruble',        'ecb',    NULL),
  ('SAR', 'Saudi Riyal',          'pegged', 3.75),
  ('SEK', 'Swedish Krona',        'ecb',    NULL),
  ('SGD', 'Singapore Dollar',     'ecb',    NULL),
  ('THB', 'Thai Baht',            'ecb',    NULL),
  ('TRY', 'Turkish Lira',         'ecb',    NULL),
  ('TWD', 'New Taiwan Dollar',    'yahoo',  NULL),
  ('USD', 'US Dollar',            'ecb',    NULL),
  ('ZAR', 'South African Rand',   'ecb',    NULL)
ON CONFLICT (currency_code) DO NOTHING;

-- ─── GuruFocus Exchange ─────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS exchange_id_seq;

CREATE TABLE IF NOT EXISTS gurufocus_exchange (
  exchange_id    INTEGER PRIMARY KEY DEFAULT nextval('exchange_id_seq'),
  exchange_code  VARCHAR NOT NULL UNIQUE,
  exchange_name  VARCHAR NOT NULL,
  is_us          BOOLEAN NOT NULL DEFAULT false,
  country_code   VARCHAR(2) NOT NULL REFERENCES country(country_code),
  currency_code  VARCHAR(3) NOT NULL REFERENCES currency(currency_code)
);

INSERT INTO gurufocus_exchange (exchange_code, exchange_name, is_us, country_code, currency_code) VALUES
  -- United States
  ('NYSE',    'New York Stock Exchange',      true,  'US', 'USD'),
  ('NASDAQ',  'NASDAQ',                       true,  'US', 'USD'),
  ('CBOE',    'Cboe BZX',                     true,  'US', 'USD'),
  -- Europe
  ('LSE',     'London Stock Exchange',         false, 'GB', 'GBP'),
  ('XTER',    'Xetra',                         false, 'DE', 'EUR'),
  ('XPAR',    'Euronext Paris',                false, 'FR', 'EUR'),
  ('XAMS',    'Euronext Amsterdam',            false, 'NL', 'EUR'),
  ('XBRU',    'Euronext Brussels',             false, 'BE', 'EUR'),
  ('XLIS',    'Euronext Lisbon',               false, 'PT', 'EUR'),
  ('MIL',     'Borsa Italiana',                false, 'IT', 'EUR'),
  ('XMAD',    'Bolsa de Madrid',               false, 'ES', 'EUR'),
  ('XSWX',    'SIX Swiss Exchange',            false, 'CH', 'CHF'),
  ('OSTO',    'Nasdaq Stockholm',              false, 'SE', 'SEK'),
  ('OCSE',    'Nasdaq Copenhagen',             false, 'DK', 'DKK'),
  ('OSL',     'Oslo Bors',                     false, 'NO', 'NOK'),
  ('OHEL',    'Nasdaq Helsinki',               false, 'FI', 'EUR'),
  ('WAR',     'Warsaw Stock Exchange',         false, 'PL', 'PLN'),
  ('XPRA',    'Vienna Stock Exchange',         false, 'AT', 'EUR'),
  ('ATH',     'Athens Exchange',               false, 'GR', 'EUR'),
  ('ISE',     'Irish Stock Exchange',          false, 'IE', 'EUR'),
  ('BDP',     'Budapest Stock Exchange',       false, 'HU', 'HUF'),
  ('PRA',     'Prague Stock Exchange',         false, 'CZ', 'CZK'),
  ('IST',     'Istanbul Stock Exchange',       false, 'TR', 'TRY'),
  -- Americas
  ('TSX',     'Toronto Stock Exchange',        false, 'CA', 'CAD'),
  ('TSXV',    'TSX Venture Exchange',          false, 'CA', 'CAD'),
  ('MEX',     'Bolsa Mexicana de Valores',     false, 'MX', 'MXN'),
  ('BMV',     'Bolsa Mexicana (alt)',          false, 'MX', 'MXN'),
  ('BSP',     'B3 (Brazil)',                   false, 'BR', 'BRL'),
  ('SGO',     'Santiago Stock Exchange',       false, 'CL', 'CLP'),
  ('BOG',     'Bolsa de Colombia',             false, 'CO', 'COP'),
  -- Asia-Pacific
  ('TSE',     'Tokyo Stock Exchange',          false, 'JP', 'JPY'),
  ('HKSE',    'Hong Kong Stock Exchange',      false, 'HK', 'HKD'),
  ('SSE',     'Shanghai Stock Exchange',       false, 'CN', 'CNY'),
  ('SZSE',    'Shenzhen Stock Exchange',       false, 'CN', 'CNY'),
  ('TWSE',    'Taiwan Stock Exchange',         false, 'TW', 'TWD'),
  ('GTSM',    'Gretai Securities Market',      false, 'TW', 'TWD'),
  ('XKRX',    'Korea Exchange',                false, 'KR', 'KRW'),
  ('NSE',     'National Stock Exchange India',  false, 'IN', 'INR'),
  ('BSE',     'BSE India',                     false, 'IN', 'INR'),
  ('ASX',     'Australian Stock Exchange',     false, 'AU', 'AUD'),
  ('NZSE',    'New Zealand Exchange',          false, 'NZ', 'NZD'),
  ('SGX',     'Singapore Exchange',            false, 'SG', 'SGD'),
  ('KLSE',    'Bursa Malaysia',                false, 'MY', 'MYR'),
  ('IDX',     'Indonesia Stock Exchange',      false, 'ID', 'IDR'),
  ('SET',     'Stock Exchange of Thailand',    false, 'TH', 'THB'),
  ('PSE',     'Philippine Stock Exchange',     false, 'PH', 'PHP'),
  -- Middle East / Africa
  ('TADAWUL', 'Saudi Stock Exchange',          false, 'SA', 'SAR'),
  ('ADX',     'Abu Dhabi Securities Exchange', false, 'AE', 'AED'),
  ('DFM',     'Dubai Financial Market',        false, 'AE', 'AED'),
  ('QSE',     'Qatar Exchange',                false, 'QA', 'QAR'),
  ('KSE',     'Kuwait Stock Exchange',         false, 'KW', 'KWD'),
  ('TASE',    'Tel Aviv Stock Exchange',       false, 'IL', 'ILS'),
  ('JSE',     'Johannesburg Stock Exchange',   false, 'ZA', 'ZAR'),
  ('EGX',     'Egyptian Exchange',             false, 'EG', 'EGP'),
  ('MCX',     'Moscow Exchange',               false, 'RU', 'RUB'),
  -- Extra (from OpenFIGI resolutions)
  ('FRA',     'Frankfurt Stock Exchange',      false, 'DE', 'EUR')
ON CONFLICT (exchange_code) DO NOTHING;

-- ─── FX Rate ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fx_rate (
  currency_code VARCHAR(3) NOT NULL REFERENCES currency(currency_code),
  rate_date     DATE       NOT NULL,
  rate          DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (currency_code, rate_date)
);

CREATE INDEX IF NOT EXISTS idx_fx_rate_date ON fx_rate (rate_date);

-- ─── Universe ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS universe (
  universe_id SERIAL PRIMARY KEY,
  label       VARCHAR NOT NULL UNIQUE,
  description VARCHAR,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS universe_membership (
  universe_id      INTEGER NOT NULL REFERENCES universe(universe_id) ON DELETE CASCADE,
  company_id       INTEGER NOT NULL REFERENCES company(company_id) ON DELETE CASCADE,
  target_month     VARCHAR NOT NULL,
  universe_ticker  VARCHAR,
  sector           VARCHAR,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (universe_id, company_id, target_month)
);

CREATE INDEX IF NOT EXISTS idx_universe_membership_month
  ON universe_membership (universe_id, target_month);

-- ─── Company Source ───────���─────────────────────────────────
CREATE TABLE IF NOT EXISTS company_source (
  company_id  INTEGER NOT NULL REFERENCES company(company_id) ON DELETE CASCADE,
  source_code VARCHAR NOT NULL,
  first_seen  DATE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (company_id, source_code)
);

-- ─── Alter company table ────────────────────────────────────

-- Rename primary_ticker → gurufocus_ticker
ALTER TABLE company RENAME COLUMN primary_ticker TO gurufocus_ticker;

-- Add exchange_id FK (nullable for unresolved companies)
ALTER TABLE company ADD COLUMN IF NOT EXISTS exchange_id
  INTEGER REFERENCES gurufocus_exchange(exchange_id);

-- Backfill exchange_id from existing primary_exchange values
UPDATE company c
SET exchange_id = e.exchange_id
FROM gurufocus_exchange e
WHERE c.primary_exchange = e.exchange_code
  AND c.exchange_id IS NULL;

-- Migrate source TEXT[] → company_source table
INSERT INTO company_source (company_id, source_code, first_seen, created_at)
SELECT company_id, unnest(source), CURRENT_DATE, now()
FROM company
WHERE source IS NOT NULL AND array_length(source, 1) > 0
ON CONFLICT DO NOTHING;

-- Drop old unique constraint and create new one
ALTER TABLE company DROP CONSTRAINT IF EXISTS company_primary_ticker_primary_exchange_key;
ALTER TABLE company ADD CONSTRAINT company_gurufocus_ticker_exchange_id_key
  UNIQUE (gurufocus_ticker, exchange_id);

-- Drop columns that moved elsewhere
ALTER TABLE company DROP COLUMN IF EXISTS primary_exchange;
ALTER TABLE company DROP COLUMN IF EXISTS country;
ALTER TABLE company DROP COLUMN IF EXISTS sector;
ALTER TABLE company DROP COLUMN IF EXISTS longequity_ticker;
ALTER TABLE company DROP COLUMN IF EXISTS source;

-- ─── Alter ticker_override ──────────────────────────────────
ALTER TABLE ticker_override RENAME COLUMN primary_ticker TO gurufocus_ticker;
ALTER TABLE ticker_override RENAME COLUMN primary_exchange TO gurufocus_exchange;

-- ─── Alter backtest_run: consolidate into config + result ───
ALTER TABLE backtest_run ADD COLUMN IF NOT EXISTS result JSONB;

UPDATE backtest_run
SET result = jsonb_build_object(
  'summary', summary,
  'monthly_records', monthly_records,
  'universe', universe
)
WHERE result IS NULL
  AND summary IS NOT NULL;

ALTER TABLE backtest_run DROP COLUMN IF EXISTS summary;
ALTER TABLE backtest_run DROP COLUMN IF EXISTS monthly_records;
ALTER TABLE backtest_run DROP COLUMN IF EXISTS universe;

-- Make result NOT NULL for new rows (existing rows already backfilled)
-- Can't add NOT NULL if column might have NULLs, so use a default
ALTER TABLE backtest_run ALTER COLUMN result SET DEFAULT '{}'::jsonb;

-- ─── Drop legacy tables ─────────────────────────────────────
DROP TABLE IF EXISTS exchange_currency;
DROP TABLE IF EXISTS index_membership;
DROP TABLE IF EXISTS universe_snapshot;

-- ─── Drop legacy RPC functions ──────────────────────────────
DROP FUNCTION IF EXISTS universe_month_counts(text);
DROP FUNCTION IF EXISTS universe_labels();
DROP FUNCTION IF EXISTS index_membership_indexes();
DROP FUNCTION IF EXISTS index_membership_cumulative(text);
DROP FUNCTION IF EXISTS index_membership_months(text);

-- ─── Reload PostgREST schema cache ──────��──────────────────
NOTIFY pgrst, 'reload schema';
