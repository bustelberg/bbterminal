CREATE TABLE IF NOT EXISTS airs_performance (
    portefeuille         TEXT NOT NULL,
    periode              DATE NOT NULL,
    beginvermogen        NUMERIC,
    koersresultaat       NUMERIC,
    opbrengsten          NUMERIC,
    beleggingsresultaat  NUMERIC,
    eindvermogen         NUMERIC,
    rendement            NUMERIC,
    cumulatief_rendement NUMERIC,
    fetched_at           TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (portefeuille, periode)
);
