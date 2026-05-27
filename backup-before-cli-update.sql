


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


CREATE EXTENSION IF NOT EXISTS "pg_net" WITH SCHEMA "extensions";






COMMENT ON SCHEMA "public" IS 'standard public schema';



CREATE EXTENSION IF NOT EXISTS "pg_graphql" WITH SCHEMA "graphql";






CREATE EXTENSION IF NOT EXISTS "pg_stat_statements" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "supabase_vault" WITH SCHEMA "vault";






CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA "extensions";






CREATE OR REPLACE FUNCTION "public"."company_universe_labels"() RETURNS TABLE("company_id" integer, "labels" "text"[])
    LANGUAGE "sql" STABLE
    AS $$
  SELECT
    m.company_id,
    array_agg(DISTINCT u.label ORDER BY u.label) AS labels
  FROM universe_membership m
  JOIN universe u USING (universe_id)
  GROUP BY m.company_id;
$$;


ALTER FUNCTION "public"."company_universe_labels"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_company_ids_for_date"("p_source_code" "text", "p_target_date" "date") RETURNS TABLE("company_id" integer)
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  SELECT DISTINCT md.company_id
  FROM metric_data md
  WHERE md.source_code = p_source_code
    AND md.target_date = p_target_date;
$$;


ALTER FUNCTION "public"."get_company_ids_for_date"("p_source_code" "text", "p_target_date" "date") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_distinct_dates"("p_source_code" "text") RETURNS TABLE("target_date" "date")
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  SELECT DISTINCT md.target_date
  FROM metric_data md
  WHERE md.source_code = p_source_code
  ORDER BY md.target_date;
$$;


ALTER FUNCTION "public"."get_distinct_dates"("p_source_code" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."increment_api_usage"("p_month" "text", "p_region" "text", "p_count" integer) RETURNS "void"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
BEGIN
  INSERT INTO api_usage (month, region, request_count)
  VALUES (p_month, p_region, p_count)
  ON CONFLICT (month, region)
  DO UPDATE SET request_count = api_usage.request_count + p_count;
END;
$$;


ALTER FUNCTION "public"."increment_api_usage"("p_month" "text", "p_region" "text", "p_count" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."merge_company_data"("p_from_id" integer, "p_to_id" integer) RETURNS "void"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
BEGIN
  -- Move metric_data rows that won't conflict
  UPDATE metric_data
  SET company_id = p_to_id
  WHERE company_id = p_from_id
    AND (metric_code, source_code, target_date) NOT IN (
      SELECT metric_code, source_code, target_date
      FROM metric_data WHERE company_id = p_to_id
    );
  -- Delete remaining rows from the source company
  DELETE FROM metric_data WHERE company_id = p_from_id;
END;
$$;


ALTER FUNCTION "public"."merge_company_data"("p_from_id" integer, "p_to_id" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_admin_role_on_signup"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
DECLARE
  admin_hashes constant text[] := ARRAY[
    '9fe083c7c1b2b6273a30b369870280d9cdfd3a89e165e6c2d68035cf1f7f144f',
    '5db5e75947119ef23451bc46919479a90b6bd51cd2e81815f2c7083e20fde36f'
  ];
BEGIN
  IF NEW.email IS NOT NULL
     AND encode(extensions.digest(lower(NEW.email), 'sha256'), 'hex') = ANY(admin_hashes)
  THEN
    NEW.raw_app_meta_data := COALESCE(NEW.raw_app_meta_data, '{}'::jsonb)
                          || jsonb_build_object('role', 'admin');
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."set_admin_role_on_signup"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."universe_full_stats"() RETURNS TABLE("universe_id" integer, "total_rows" integer, "unique_companies" integer, "unique_tickers" integer, "month_count" integer, "start_month" "text", "end_month" "text", "monthly_counts" "jsonb", "sector_counts" "jsonb")
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  WITH monthly AS (
    SELECT
      universe_id,
      jsonb_agg(
        jsonb_build_object('month', target_month, 'count', member_count)
        ORDER BY target_month
      ) AS arr
    FROM universe_monthly_counts
    GROUP BY universe_id
  ),
  sectors AS (
    SELECT
      universe_id,
      jsonb_agg(
        jsonb_build_object('sector', sector, 'count', member_count)
        ORDER BY member_count DESC
      ) AS arr
    FROM universe_sector_counts
    GROUP BY universe_id
  )
  SELECT
    s.universe_id,
    s.total_rows,
    s.unique_companies,
    s.unique_tickers,
    s.month_count,
    s.start_month::text,
    s.end_month::text,
    COALESCE(m.arr, '[]'::jsonb) AS monthly_counts,
    COALESCE(sec.arr, '[]'::jsonb) AS sector_counts
  FROM universe_summary s
  LEFT JOIN monthly m USING (universe_id)
  LEFT JOIN sectors sec USING (universe_id);
$$;


ALTER FUNCTION "public"."universe_full_stats"() OWNER TO "postgres";

SET default_tablespace = '';

SET default_table_access_method = "heap";


CREATE TABLE IF NOT EXISTS "public"."airs_performance" (
    "portefeuille" "text" NOT NULL,
    "periode" "date" NOT NULL,
    "beginvermogen" numeric,
    "koersresultaat" numeric,
    "opbrengsten" numeric,
    "beleggingsresultaat" numeric,
    "eindvermogen" numeric,
    "rendement" numeric,
    "cumulatief_rendement" numeric,
    "fetched_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."airs_performance" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."api_usage" (
    "id" integer NOT NULL,
    "month" "text" NOT NULL,
    "region" "text" NOT NULL,
    "request_count" integer DEFAULT 0 NOT NULL
);


ALTER TABLE "public"."api_usage" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."api_usage_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."api_usage_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."api_usage_id_seq" OWNED BY "public"."api_usage"."id";



CREATE SEQUENCE IF NOT EXISTS "public"."backtest_cache_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."backtest_cache_id_seq" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."backtest_cache" (
    "cache_id" integer DEFAULT "nextval"('"public"."backtest_cache_id_seq"'::"regclass") NOT NULL,
    "strategy_hash" "text" NOT NULL,
    "data_date" "date" DEFAULT CURRENT_DATE NOT NULL,
    "config" "jsonb" NOT NULL,
    "payload" "jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."backtest_cache" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."backtest_run_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."backtest_run_id_seq" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."backtest_run" (
    "run_id" integer DEFAULT "nextval"('"public"."backtest_run_id_seq"'::"regclass") NOT NULL,
    "name" character varying NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "config" "jsonb" NOT NULL,
    "result" "jsonb" DEFAULT '{}'::"jsonb"
);


ALTER TABLE "public"."backtest_run" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."benchmark_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."benchmark_id_seq" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."benchmark" (
    "benchmark_id" integer DEFAULT "nextval"('"public"."benchmark_id_seq"'::"regclass") NOT NULL,
    "ticker" character varying NOT NULL,
    "name" character varying NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "sector" "text"
);


ALTER TABLE "public"."benchmark" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."benchmark_price" (
    "benchmark_id" integer NOT NULL,
    "target_date" "date" NOT NULL,
    "price" double precision NOT NULL
);


ALTER TABLE "public"."benchmark_price" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."company_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."company_id_seq" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."company" (
    "company_id" integer DEFAULT "nextval"('"public"."company_id_seq"'::"regclass") NOT NULL,
    "gurufocus_ticker" character varying NOT NULL,
    "company_name" character varying,
    "exchange_id" integer
);


ALTER TABLE "public"."company" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."company_source" (
    "company_id" integer NOT NULL,
    "source_code" character varying NOT NULL,
    "first_seen" "date",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."company_source" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."country" (
    "country_code" character varying(2) NOT NULL,
    "country_name" character varying NOT NULL
);


ALTER TABLE "public"."country" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."currency" (
    "currency_code" character varying(3) NOT NULL,
    "currency_name" character varying NOT NULL,
    "source" character varying DEFAULT 'ecb'::character varying NOT NULL,
    "peg_to_usd" double precision
);


ALTER TABLE "public"."currency" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."current_picks_day" (
    "strategy_hash" "text" NOT NULL,
    "target_date" "date" NOT NULL,
    "as_of_date" "date" NOT NULL,
    "holdings" "jsonb" NOT NULL,
    "portfolio_return_pct" numeric,
    "next_day_return_pct" numeric,
    "turnover_abs" integer,
    "turnover_pct" numeric,
    "config" "jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."current_picks_day" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."current_picks_snapshot_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."current_picks_snapshot_id_seq" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."current_picks_snapshot" (
    "snapshot_id" integer DEFAULT "nextval"('"public"."current_picks_snapshot_id_seq"'::"regclass") NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "triggered_by" "text" NOT NULL,
    "as_of_date" "date" NOT NULL,
    "latest_price_date" "date",
    "config" "jsonb" NOT NULL,
    "holdings" "jsonb" NOT NULL,
    "daily_picks" "jsonb" DEFAULT '[]'::"jsonb" NOT NULL,
    "strategy_hash" "text",
    "name" "text",
    CONSTRAINT "current_picks_snapshot_triggered_by_check" CHECK (("triggered_by" = ANY (ARRAY['auto'::"text", 'manual'::"text"])))
);


ALTER TABLE "public"."current_picks_snapshot" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."exchange_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."exchange_id_seq" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."fx_rate" (
    "currency_code" character varying(3) NOT NULL,
    "rate_date" "date" NOT NULL,
    "rate" double precision NOT NULL
);


ALTER TABLE "public"."fx_rate" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."gurufocus_exchange" (
    "exchange_id" integer DEFAULT "nextval"('"public"."exchange_id_seq"'::"regclass") NOT NULL,
    "exchange_code" character varying NOT NULL,
    "exchange_name" character varying NOT NULL,
    "is_us" boolean DEFAULT false NOT NULL,
    "country_code" character varying(2) NOT NULL,
    "currency_code" character varying(3) NOT NULL
);


ALTER TABLE "public"."gurufocus_exchange" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."ingest_run_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."ingest_run_id_seq" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."ingest_run" (
    "run_id" integer DEFAULT "nextval"('"public"."ingest_run_id_seq"'::"regclass") NOT NULL,
    "job_name" "text" NOT NULL,
    "triggered_by" "text" NOT NULL,
    "started_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "finished_at" timestamp with time zone,
    "status" "text" DEFAULT 'running'::"text" NOT NULL,
    "companies_processed" integer DEFAULT 0 NOT NULL,
    "prices_refreshed" integer DEFAULT 0 NOT NULL,
    "volumes_refreshed" integer DEFAULT 0 NOT NULL,
    "forbidden_count" integer DEFAULT 0 NOT NULL,
    "delisted_count" integer DEFAULT 0 NOT NULL,
    "error_count" integer DEFAULT 0 NOT NULL,
    "error_summary" "text",
    CONSTRAINT "ingest_run_status_check" CHECK (("status" = ANY (ARRAY['running'::"text", 'ok'::"text", 'error'::"text"]))),
    CONSTRAINT "ingest_run_triggered_by_check" CHECK (("triggered_by" = ANY (ARRAY['auto'::"text", 'manual'::"text"])))
);


ALTER TABLE "public"."ingest_run" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."metric_data" (
    "company_id" integer NOT NULL,
    "metric_code" character varying NOT NULL,
    "source_code" character varying NOT NULL,
    "target_date" "date" NOT NULL,
    "numeric_value" double precision,
    "text_value" character varying,
    "is_prediction" boolean DEFAULT false NOT NULL,
    "recorded_at" timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE "public"."metric_data" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."portfolio_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."portfolio_id_seq" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."portfolio" (
    "portfolio_id" integer DEFAULT "nextval"('"public"."portfolio_id_seq"'::"regclass") NOT NULL,
    "portfolio_name" character varying NOT NULL,
    "target_date" "date" NOT NULL,
    "published_at" "date"
);


ALTER TABLE "public"."portfolio" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."portfolio_weight" (
    "portfolio_id" integer NOT NULL,
    "company_id" integer NOT NULL,
    "weight_value" double precision NOT NULL,
    CONSTRAINT "portfolio_weight_weight_value_check" CHECK ((("weight_value" >= (0)::double precision) AND ("weight_value" <= (1)::double precision)))
);


ALTER TABLE "public"."portfolio_weight" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."ticker_override" (
    "ticker" character varying NOT NULL,
    "gurufocus_ticker" character varying NOT NULL,
    "gurufocus_exchange" character varying NOT NULL,
    "source" character varying DEFAULT 'openfigi'::character varying NOT NULL,
    "created_at" timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE "public"."ticker_override" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."universe" (
    "universe_id" integer NOT NULL,
    "label" character varying NOT NULL,
    "description" character varying,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "parent_universe_id" integer,
    "filter_config" "jsonb"
);


ALTER TABLE "public"."universe" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."universe_membership" (
    "universe_id" integer NOT NULL,
    "company_id" integer NOT NULL,
    "target_month" character varying NOT NULL,
    "universe_ticker" character varying,
    "sector" character varying,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."universe_membership" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."universe_monthly_counts" WITH ("security_invoker"='on') AS
 SELECT "universe_id",
    "target_month",
    ("count"(*))::integer AS "member_count"
   FROM "public"."universe_membership"
  GROUP BY "universe_id", "target_month";


ALTER VIEW "public"."universe_monthly_counts" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."universe_sector_counts" WITH ("security_invoker"='on') AS
 SELECT "universe_id",
    COALESCE(NULLIF(("sector")::"text", ''::"text"), '(unknown)'::"text") AS "sector",
    ("count"(*))::integer AS "member_count"
   FROM "public"."universe_membership"
  GROUP BY "universe_id", COALESCE(NULLIF(("sector")::"text", ''::"text"), '(unknown)'::"text");


ALTER VIEW "public"."universe_sector_counts" OWNER TO "postgres";


CREATE MATERIALIZED VIEW "public"."universe_stats" AS
 SELECT "u"."universe_id",
    "u"."label",
    "u"."description",
    "u"."created_at",
    "min"(("m"."target_month")::"text") AS "start_month",
    "max"(("m"."target_month")::"text") AS "end_month",
    "count"(DISTINCT "m"."target_month") AS "month_count",
    "count"(DISTINCT "m"."universe_ticker") AS "total_unique_tickers"
   FROM ("public"."universe" "u"
     LEFT JOIN "public"."universe_membership" "m" ON (("m"."universe_id" = "u"."universe_id")))
  GROUP BY "u"."universe_id", "u"."label", "u"."description", "u"."created_at"
  WITH NO DATA;


ALTER MATERIALIZED VIEW "public"."universe_stats" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."universe_summary" WITH ("security_invoker"='on') AS
 SELECT "universe_id",
    ("count"(*))::integer AS "total_rows",
    ("count"(DISTINCT "company_id"))::integer AS "unique_companies",
    ("count"(DISTINCT "universe_ticker"))::integer AS "unique_tickers",
    ("count"(DISTINCT "target_month"))::integer AS "month_count",
    "min"(("target_month")::"text") AS "start_month",
    "max"(("target_month")::"text") AS "end_month"
   FROM "public"."universe_membership"
  GROUP BY "universe_id";


ALTER VIEW "public"."universe_summary" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."universe_universe_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."universe_universe_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."universe_universe_id_seq" OWNED BY "public"."universe"."universe_id";



ALTER TABLE ONLY "public"."api_usage" ALTER COLUMN "id" SET DEFAULT "nextval"('"public"."api_usage_id_seq"'::"regclass");



ALTER TABLE ONLY "public"."universe" ALTER COLUMN "universe_id" SET DEFAULT "nextval"('"public"."universe_universe_id_seq"'::"regclass");



ALTER TABLE ONLY "public"."airs_performance"
    ADD CONSTRAINT "airs_performance_pkey" PRIMARY KEY ("portefeuille", "periode");



ALTER TABLE ONLY "public"."api_usage"
    ADD CONSTRAINT "api_usage_month_region_key" UNIQUE ("month", "region");



ALTER TABLE ONLY "public"."api_usage"
    ADD CONSTRAINT "api_usage_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."backtest_cache"
    ADD CONSTRAINT "backtest_cache_pkey" PRIMARY KEY ("cache_id");



ALTER TABLE ONLY "public"."backtest_run"
    ADD CONSTRAINT "backtest_run_pkey" PRIMARY KEY ("run_id");



ALTER TABLE ONLY "public"."benchmark"
    ADD CONSTRAINT "benchmark_pkey" PRIMARY KEY ("benchmark_id");



ALTER TABLE ONLY "public"."benchmark_price"
    ADD CONSTRAINT "benchmark_price_pkey" PRIMARY KEY ("benchmark_id", "target_date");



ALTER TABLE ONLY "public"."benchmark"
    ADD CONSTRAINT "benchmark_ticker_key" UNIQUE ("ticker");



ALTER TABLE ONLY "public"."company"
    ADD CONSTRAINT "company_gurufocus_ticker_exchange_id_key" UNIQUE ("gurufocus_ticker", "exchange_id");



ALTER TABLE ONLY "public"."company"
    ADD CONSTRAINT "company_pkey" PRIMARY KEY ("company_id");



ALTER TABLE ONLY "public"."company_source"
    ADD CONSTRAINT "company_source_pkey" PRIMARY KEY ("company_id", "source_code");



ALTER TABLE ONLY "public"."country"
    ADD CONSTRAINT "country_pkey" PRIMARY KEY ("country_code");



ALTER TABLE ONLY "public"."currency"
    ADD CONSTRAINT "currency_pkey" PRIMARY KEY ("currency_code");



ALTER TABLE ONLY "public"."current_picks_day"
    ADD CONSTRAINT "current_picks_day_pkey" PRIMARY KEY ("strategy_hash", "target_date");



ALTER TABLE ONLY "public"."current_picks_snapshot"
    ADD CONSTRAINT "current_picks_snapshot_pkey" PRIMARY KEY ("snapshot_id");



ALTER TABLE ONLY "public"."fx_rate"
    ADD CONSTRAINT "fx_rate_pkey" PRIMARY KEY ("currency_code", "rate_date");



ALTER TABLE ONLY "public"."gurufocus_exchange"
    ADD CONSTRAINT "gurufocus_exchange_exchange_code_key" UNIQUE ("exchange_code");



ALTER TABLE ONLY "public"."gurufocus_exchange"
    ADD CONSTRAINT "gurufocus_exchange_pkey" PRIMARY KEY ("exchange_id");



ALTER TABLE ONLY "public"."ingest_run"
    ADD CONSTRAINT "ingest_run_pkey" PRIMARY KEY ("run_id");



ALTER TABLE ONLY "public"."metric_data"
    ADD CONSTRAINT "metric_data_pkey" PRIMARY KEY ("company_id", "metric_code", "source_code", "target_date");



ALTER TABLE ONLY "public"."portfolio"
    ADD CONSTRAINT "portfolio_pkey" PRIMARY KEY ("portfolio_id");



ALTER TABLE ONLY "public"."portfolio"
    ADD CONSTRAINT "portfolio_portfolio_name_target_date_key" UNIQUE ("portfolio_name", "target_date");



ALTER TABLE ONLY "public"."portfolio_weight"
    ADD CONSTRAINT "portfolio_weight_pkey" PRIMARY KEY ("portfolio_id", "company_id");



ALTER TABLE ONLY "public"."ticker_override"
    ADD CONSTRAINT "ticker_override_pkey" PRIMARY KEY ("ticker");



ALTER TABLE ONLY "public"."universe"
    ADD CONSTRAINT "universe_label_key" UNIQUE ("label");



ALTER TABLE ONLY "public"."universe_membership"
    ADD CONSTRAINT "universe_membership_pkey" PRIMARY KEY ("universe_id", "company_id", "target_month");



ALTER TABLE ONLY "public"."universe"
    ADD CONSTRAINT "universe_pkey" PRIMARY KEY ("universe_id");



CREATE UNIQUE INDEX "benchmark_sector_unique" ON "public"."benchmark" USING "btree" ("sector") WHERE ("sector" IS NOT NULL);



CREATE INDEX "idx_backtest_cache_created_at_desc" ON "public"."backtest_cache" USING "btree" ("created_at" DESC);



CREATE UNIQUE INDEX "idx_backtest_cache_hash_date" ON "public"."backtest_cache" USING "btree" ("strategy_hash", "data_date");



CREATE INDEX "idx_current_picks_created_at_desc" ON "public"."current_picks_snapshot" USING "btree" ("created_at" DESC);



CREATE INDEX "idx_current_picks_day_hash_month" ON "public"."current_picks_day" USING "btree" ("strategy_hash", "as_of_date");



CREATE INDEX "idx_current_picks_snapshot_hash_asof" ON "public"."current_picks_snapshot" USING "btree" ("strategy_hash", "as_of_date" DESC);



CREATE INDEX "idx_fx_rate_date" ON "public"."fx_rate" USING "btree" ("rate_date");



CREATE INDEX "idx_ingest_run_job_started" ON "public"."ingest_run" USING "btree" ("job_name", "started_at" DESC);



CREATE INDEX "idx_ingest_run_started_at_desc" ON "public"."ingest_run" USING "btree" ("started_at" DESC);



CREATE INDEX "idx_metric_data_source_date" ON "public"."metric_data" USING "btree" ("source_code", "target_date");



CREATE INDEX "idx_universe_membership_company" ON "public"."universe_membership" USING "btree" ("company_id");



CREATE INDEX "idx_universe_membership_month" ON "public"."universe_membership" USING "btree" ("universe_id", "target_month");



CREATE INDEX "idx_universe_parent" ON "public"."universe" USING "btree" ("parent_universe_id");



CREATE UNIQUE INDEX "universe_stats_universe_id_uniq" ON "public"."universe_stats" USING "btree" ("universe_id");



ALTER TABLE ONLY "public"."benchmark_price"
    ADD CONSTRAINT "benchmark_price_benchmark_id_fkey" FOREIGN KEY ("benchmark_id") REFERENCES "public"."benchmark"("benchmark_id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."company"
    ADD CONSTRAINT "company_exchange_id_fkey" FOREIGN KEY ("exchange_id") REFERENCES "public"."gurufocus_exchange"("exchange_id");



ALTER TABLE ONLY "public"."company_source"
    ADD CONSTRAINT "company_source_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."company"("company_id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."fx_rate"
    ADD CONSTRAINT "fx_rate_currency_code_fkey" FOREIGN KEY ("currency_code") REFERENCES "public"."currency"("currency_code");



ALTER TABLE ONLY "public"."gurufocus_exchange"
    ADD CONSTRAINT "gurufocus_exchange_country_code_fkey" FOREIGN KEY ("country_code") REFERENCES "public"."country"("country_code");



ALTER TABLE ONLY "public"."gurufocus_exchange"
    ADD CONSTRAINT "gurufocus_exchange_currency_code_fkey" FOREIGN KEY ("currency_code") REFERENCES "public"."currency"("currency_code");



ALTER TABLE ONLY "public"."metric_data"
    ADD CONSTRAINT "metric_data_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."company"("company_id");



ALTER TABLE ONLY "public"."portfolio_weight"
    ADD CONSTRAINT "portfolio_weight_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."company"("company_id");



ALTER TABLE ONLY "public"."portfolio_weight"
    ADD CONSTRAINT "portfolio_weight_portfolio_id_fkey" FOREIGN KEY ("portfolio_id") REFERENCES "public"."portfolio"("portfolio_id");



ALTER TABLE ONLY "public"."universe_membership"
    ADD CONSTRAINT "universe_membership_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."company"("company_id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."universe_membership"
    ADD CONSTRAINT "universe_membership_universe_id_fkey" FOREIGN KEY ("universe_id") REFERENCES "public"."universe"("universe_id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."universe"
    ADD CONSTRAINT "universe_parent_universe_id_fkey" FOREIGN KEY ("parent_universe_id") REFERENCES "public"."universe"("universe_id") ON DELETE SET NULL;



ALTER TABLE "public"."airs_performance" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."api_usage" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."backtest_cache" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."backtest_run" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."benchmark" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."benchmark_price" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."company" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."company_source" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."country" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."currency" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."current_picks_day" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."current_picks_snapshot" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."fx_rate" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."gurufocus_exchange" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."ingest_run" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."metric_data" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."portfolio" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."portfolio_weight" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."ticker_override" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."universe" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."universe_membership" ENABLE ROW LEVEL SECURITY;




ALTER PUBLICATION "supabase_realtime" OWNER TO "postgres";





GRANT USAGE ON SCHEMA "public" TO "postgres";
GRANT USAGE ON SCHEMA "public" TO "anon";
GRANT USAGE ON SCHEMA "public" TO "authenticated";
GRANT USAGE ON SCHEMA "public" TO "service_role";































































































































































GRANT ALL ON FUNCTION "public"."company_universe_labels"() TO "anon";
GRANT ALL ON FUNCTION "public"."company_universe_labels"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."company_universe_labels"() TO "service_role";



GRANT ALL ON FUNCTION "public"."get_company_ids_for_date"("p_source_code" "text", "p_target_date" "date") TO "anon";
GRANT ALL ON FUNCTION "public"."get_company_ids_for_date"("p_source_code" "text", "p_target_date" "date") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_company_ids_for_date"("p_source_code" "text", "p_target_date" "date") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_distinct_dates"("p_source_code" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."get_distinct_dates"("p_source_code" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_distinct_dates"("p_source_code" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."increment_api_usage"("p_month" "text", "p_region" "text", "p_count" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."increment_api_usage"("p_month" "text", "p_region" "text", "p_count" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."increment_api_usage"("p_month" "text", "p_region" "text", "p_count" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."merge_company_data"("p_from_id" integer, "p_to_id" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."merge_company_data"("p_from_id" integer, "p_to_id" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."merge_company_data"("p_from_id" integer, "p_to_id" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."set_admin_role_on_signup"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_admin_role_on_signup"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_admin_role_on_signup"() TO "service_role";



GRANT ALL ON FUNCTION "public"."universe_full_stats"() TO "anon";
GRANT ALL ON FUNCTION "public"."universe_full_stats"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."universe_full_stats"() TO "service_role";


















GRANT ALL ON TABLE "public"."airs_performance" TO "anon";
GRANT ALL ON TABLE "public"."airs_performance" TO "authenticated";
GRANT ALL ON TABLE "public"."airs_performance" TO "service_role";



GRANT ALL ON TABLE "public"."api_usage" TO "anon";
GRANT ALL ON TABLE "public"."api_usage" TO "authenticated";
GRANT ALL ON TABLE "public"."api_usage" TO "service_role";



GRANT ALL ON SEQUENCE "public"."api_usage_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."api_usage_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."api_usage_id_seq" TO "service_role";



GRANT ALL ON SEQUENCE "public"."backtest_cache_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."backtest_cache_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."backtest_cache_id_seq" TO "service_role";



GRANT ALL ON TABLE "public"."backtest_cache" TO "anon";
GRANT ALL ON TABLE "public"."backtest_cache" TO "authenticated";
GRANT ALL ON TABLE "public"."backtest_cache" TO "service_role";



GRANT ALL ON SEQUENCE "public"."backtest_run_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."backtest_run_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."backtest_run_id_seq" TO "service_role";



GRANT ALL ON TABLE "public"."backtest_run" TO "anon";
GRANT ALL ON TABLE "public"."backtest_run" TO "authenticated";
GRANT ALL ON TABLE "public"."backtest_run" TO "service_role";



GRANT ALL ON SEQUENCE "public"."benchmark_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."benchmark_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."benchmark_id_seq" TO "service_role";



GRANT ALL ON TABLE "public"."benchmark" TO "anon";
GRANT ALL ON TABLE "public"."benchmark" TO "authenticated";
GRANT ALL ON TABLE "public"."benchmark" TO "service_role";



GRANT ALL ON TABLE "public"."benchmark_price" TO "anon";
GRANT ALL ON TABLE "public"."benchmark_price" TO "authenticated";
GRANT ALL ON TABLE "public"."benchmark_price" TO "service_role";



GRANT ALL ON SEQUENCE "public"."company_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."company_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."company_id_seq" TO "service_role";



GRANT ALL ON TABLE "public"."company" TO "anon";
GRANT ALL ON TABLE "public"."company" TO "authenticated";
GRANT ALL ON TABLE "public"."company" TO "service_role";



GRANT ALL ON TABLE "public"."company_source" TO "anon";
GRANT ALL ON TABLE "public"."company_source" TO "authenticated";
GRANT ALL ON TABLE "public"."company_source" TO "service_role";



GRANT ALL ON TABLE "public"."country" TO "anon";
GRANT ALL ON TABLE "public"."country" TO "authenticated";
GRANT ALL ON TABLE "public"."country" TO "service_role";



GRANT ALL ON TABLE "public"."currency" TO "anon";
GRANT ALL ON TABLE "public"."currency" TO "authenticated";
GRANT ALL ON TABLE "public"."currency" TO "service_role";



GRANT ALL ON TABLE "public"."current_picks_day" TO "anon";
GRANT ALL ON TABLE "public"."current_picks_day" TO "authenticated";
GRANT ALL ON TABLE "public"."current_picks_day" TO "service_role";



GRANT ALL ON SEQUENCE "public"."current_picks_snapshot_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."current_picks_snapshot_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."current_picks_snapshot_id_seq" TO "service_role";



GRANT ALL ON TABLE "public"."current_picks_snapshot" TO "anon";
GRANT ALL ON TABLE "public"."current_picks_snapshot" TO "authenticated";
GRANT ALL ON TABLE "public"."current_picks_snapshot" TO "service_role";



GRANT ALL ON SEQUENCE "public"."exchange_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."exchange_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."exchange_id_seq" TO "service_role";



GRANT ALL ON TABLE "public"."fx_rate" TO "anon";
GRANT ALL ON TABLE "public"."fx_rate" TO "authenticated";
GRANT ALL ON TABLE "public"."fx_rate" TO "service_role";



GRANT ALL ON TABLE "public"."gurufocus_exchange" TO "anon";
GRANT ALL ON TABLE "public"."gurufocus_exchange" TO "authenticated";
GRANT ALL ON TABLE "public"."gurufocus_exchange" TO "service_role";



GRANT ALL ON SEQUENCE "public"."ingest_run_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."ingest_run_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."ingest_run_id_seq" TO "service_role";



GRANT ALL ON TABLE "public"."ingest_run" TO "anon";
GRANT ALL ON TABLE "public"."ingest_run" TO "authenticated";
GRANT ALL ON TABLE "public"."ingest_run" TO "service_role";



GRANT ALL ON TABLE "public"."metric_data" TO "anon";
GRANT ALL ON TABLE "public"."metric_data" TO "authenticated";
GRANT ALL ON TABLE "public"."metric_data" TO "service_role";



GRANT ALL ON SEQUENCE "public"."portfolio_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."portfolio_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."portfolio_id_seq" TO "service_role";



GRANT ALL ON TABLE "public"."portfolio" TO "anon";
GRANT ALL ON TABLE "public"."portfolio" TO "authenticated";
GRANT ALL ON TABLE "public"."portfolio" TO "service_role";



GRANT ALL ON TABLE "public"."portfolio_weight" TO "anon";
GRANT ALL ON TABLE "public"."portfolio_weight" TO "authenticated";
GRANT ALL ON TABLE "public"."portfolio_weight" TO "service_role";



GRANT ALL ON TABLE "public"."ticker_override" TO "anon";
GRANT ALL ON TABLE "public"."ticker_override" TO "authenticated";
GRANT ALL ON TABLE "public"."ticker_override" TO "service_role";



GRANT ALL ON TABLE "public"."universe" TO "anon";
GRANT ALL ON TABLE "public"."universe" TO "authenticated";
GRANT ALL ON TABLE "public"."universe" TO "service_role";



GRANT ALL ON TABLE "public"."universe_membership" TO "anon";
GRANT ALL ON TABLE "public"."universe_membership" TO "authenticated";
GRANT ALL ON TABLE "public"."universe_membership" TO "service_role";



GRANT ALL ON TABLE "public"."universe_monthly_counts" TO "anon";
GRANT ALL ON TABLE "public"."universe_monthly_counts" TO "authenticated";
GRANT ALL ON TABLE "public"."universe_monthly_counts" TO "service_role";



GRANT ALL ON TABLE "public"."universe_sector_counts" TO "anon";
GRANT ALL ON TABLE "public"."universe_sector_counts" TO "authenticated";
GRANT ALL ON TABLE "public"."universe_sector_counts" TO "service_role";



GRANT ALL ON TABLE "public"."universe_stats" TO "anon";
GRANT ALL ON TABLE "public"."universe_stats" TO "authenticated";
GRANT ALL ON TABLE "public"."universe_stats" TO "service_role";



GRANT ALL ON TABLE "public"."universe_summary" TO "anon";
GRANT ALL ON TABLE "public"."universe_summary" TO "authenticated";
GRANT ALL ON TABLE "public"."universe_summary" TO "service_role";



GRANT ALL ON SEQUENCE "public"."universe_universe_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."universe_universe_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."universe_universe_id_seq" TO "service_role";









ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "service_role";































