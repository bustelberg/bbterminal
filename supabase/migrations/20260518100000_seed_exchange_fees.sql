-- Seed `exchange_fee` with the operator's broker-cost defaults so the
-- /fees page comes up populated rather than blank, and backtests
-- immediately render `gross (net)` parens using realistic figures.
--
-- Source: the operator's IBKR Fixed-Smart entry-tier mapping (May 2026).
-- Single one-way bps per exchange — round-trip cost is 2× the value
-- below. Midpoint of each band is used where a range was given. Tweak
-- any row via /fees in the UI; the page just upserts back into this
-- same table.
--
--   Western Europe         →  5    bps   supported
--   Southern/Eastern EU    →  12   bps   supported   (10-15 band midpoint)
--   US                     →  0.5  bps   supported
--   Canada                 →  0.8  bps   supported
--   APAC majors            →  7    bps   supported   (5-8 band midpoint)
--   India                  →  1    bps   supported
--   MENA                   →  18   bps   supported   (10-25 band midpoint)
--   Broker-unsupported     →  0    bps   UNSUPPORTED — excluded from backtest universe
--
-- ON CONFLICT/UPDATE makes the migration safely re-runnable; manual
-- /fees edits made after the first application are preserved by the
-- service running the migration only once (Supabase tracks applied
-- migrations in supabase_migrations.schema_migrations), so the
-- ON CONFLICT branch is just defensive.

INSERT INTO exchange_fee (exchange_code, fee_bps, is_broker_supported) VALUES
  -- ─── Western Europe (5 bps) ─────────────────────────────
  ('XAMS',  5,    TRUE),  -- Euronext Amsterdam
  ('XPAR',  5,    TRUE),  -- Euronext Paris
  ('XBRU',  5,    TRUE),  -- Euronext Brussels
  ('XTER',  5,    TRUE),  -- Xetra (DE)
  ('FRA',   5,    TRUE),  -- Frankfurt Stock Exchange
  ('MIL',   5,    TRUE),  -- Borsa Italiana
  ('XMAD',  5,    TRUE),  -- Bolsa de Madrid
  ('XSWX',  5,    TRUE),  -- SIX Swiss
  ('OCSE',  5,    TRUE),  -- Nasdaq Copenhagen
  ('OHEL',  5,    TRUE),  -- Nasdaq Helsinki
  ('OSTO',  5,    TRUE),  -- Nasdaq Stockholm
  ('OSL',   5,    TRUE),  -- Oslo Bors
  ('WBO',   5,    TRUE),  -- Wiener Boerse (Vienna)
  ('LSE',   5,    TRUE),  -- London Stock Exchange
  ('DUB',   5,    TRUE),  -- Irish Stock Exchange / Euronext Dublin

  -- ─── Southern/Eastern Europe (12 bps midpoint) ──────────
  ('XLIS',  12,   TRUE),  -- Euronext Lisbon
  ('XPRA',  12,   TRUE),  -- Prague
  ('BUD',   12,   TRUE),  -- Budapest
  ('WAR',   12,   TRUE),  -- Warsaw
  -- ATH (Athens) intentionally not in operator's list; left at default
  -- (supported, 0 bps) — adjust via /fees if needed.

  -- ─── US (0.5 bps proxy for Fixed-Smart entry tier) ──────
  ('NYSE',    0.5, TRUE),
  ('NASDAQ',  0.5, TRUE),
  ('CBOE',    0.5, TRUE),

  -- ─── Canada (0.8 bps proxy) ─────────────────────────────
  ('TSX',   0.8,  TRUE),
  ('TSXV',  0.8,  TRUE),

  -- ─── APAC majors (7 bps midpoint of 5-8 band) ───────────
  ('TSE',   7,    TRUE),  -- Tokyo
  ('HKSE',  7,    TRUE),  -- Hong Kong
  ('SGX',   7,    TRUE),  -- Singapore
  ('ASX',   7,    TRUE),  -- Sydney
  ('XKRX',  7,    TRUE),  -- Korea Exchange
  ('XKLS',  7,    TRUE),  -- Bursa Malaysia
  ('TPE',   7,    TRUE),  -- Taiwan
  -- China mainland (SHSE, SZSE), Taiwan OTC (ROCO), NZ (NZSE) intentionally
  -- not in operator's list — left at default (supported, 0 bps). Tweak via /fees.

  -- ─── India (1 bps cap INR 20) ───────────────────────────
  ('NSE',   1,    TRUE),
  ('BSE',   1,    TRUE),

  -- ─── MENA (18 bps midpoint of 10-25 band) ───────────────
  ('ADX',   18,   TRUE),  -- Abu Dhabi
  ('DFM',   18,   TRUE),  -- Dubai
  ('SAU',   18,   TRUE),  -- Saudi (Tadawul)
  ('XTAE',  18,   TRUE),  -- Tel Aviv

  -- ─── Broker-unsupported (excluded from backtest universe) ─
  ('MIC',   0,    FALSE), -- Moscow (sanctioned anyway)
  ('BKK',   0,    FALSE), -- Bangkok (Thailand)
  ('BOG',   0,    FALSE), -- Bolsa de Colombia
  ('CAI',   0,    FALSE), -- Cairo (Egypt)
  ('ISX',   0,    FALSE), -- Indonesia
  ('JSE',   0,    FALSE), -- Johannesburg (South Africa)
  ('KUW',   0,    FALSE), -- Kuwait
  ('PHS',   0,    FALSE), -- Philippines
  ('XSGO',  0,    FALSE), -- Santiago (Chile)
  ('IST',   0,    FALSE), -- Istanbul (Turkey)
  ('DSMD',  0,    FALSE)  -- Qatar
ON CONFLICT (exchange_code) DO UPDATE SET
  fee_bps             = EXCLUDED.fee_bps,
  is_broker_supported = EXCLUDED.is_broker_supported,
  updated_at          = now();

NOTIFY pgrst, 'reload schema';
