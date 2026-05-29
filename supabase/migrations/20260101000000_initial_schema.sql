--
-- PostgreSQL database dump
--


-- Dumped from database version 17.6
-- Dumped by pg_dump version 17.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA IF NOT EXISTS public;


--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA public IS 'standard public schema';


--
-- Name: company_latest_close_price_dates(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.company_latest_close_price_dates() RETURNS TABLE(company_id integer, latest_target_date text)
    LANGUAGE sql STABLE
    AS $$
  SELECT
    md.company_id,
    MAX(md.target_date::TEXT) AS latest_target_date
  FROM metric_data md
  WHERE md.metric_code = 'close_price'
    AND md.source_code = 'gurufocus'
  GROUP BY md.company_id;
$$;


--
-- Name: company_universe_labels(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.company_universe_labels() RETURNS TABLE(company_id integer, labels text[])
    LANGUAGE sql STABLE
    AS $$
  SELECT
    m.company_id,
    array_agg(DISTINCT u.label ORDER BY u.label) AS labels
  FROM universe_membership m
  JOIN universe u USING (universe_id)
  GROUP BY m.company_id;
$$;


--
-- Name: get_company_ids_for_date(text, date); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_company_ids_for_date(p_source_code text, p_target_date date) RETURNS TABLE(company_id integer)
    LANGUAGE sql STABLE
    SET search_path TO 'public', 'pg_temp'
    AS $$
  SELECT DISTINCT md.company_id
  FROM metric_data md
  WHERE md.source_code = p_source_code
    AND md.target_date = p_target_date;
$$;


--
-- Name: get_distinct_dates(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_distinct_dates(p_source_code text) RETURNS TABLE(target_date date)
    LANGUAGE sql STABLE
    SET search_path TO 'public', 'pg_temp'
    AS $$
  SELECT DISTINCT md.target_date
  FROM metric_data md
  WHERE md.source_code = p_source_code
  ORDER BY md.target_date;
$$;


--
-- Name: increment_api_usage(text, text, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.increment_api_usage(p_month text, p_region text, p_count integer) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'public', 'pg_temp'
    AS $$
BEGIN
  INSERT INTO api_usage (month, region, request_count)
  VALUES (p_month, p_region, p_count)
  ON CONFLICT (month, region)
  DO UPDATE SET request_count = api_usage.request_count + p_count;
END;
$$;


--
-- Name: merge_company_data(integer, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.merge_company_data(p_from_id integer, p_to_id integer) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'public', 'pg_temp'
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


--
-- Name: set_admin_role_on_signup(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.set_admin_role_on_signup() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO ''
    AS $$
DECLARE
  -- SHA-256(lower(email)) hex. Keeps admin emails out of source.
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


--
-- Name: universe_all_companies_ever(integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.universe_all_companies_ever(p_universe_id integer) RETURNS TABLE(company_id integer, gurufocus_ticker text, exchange_code text, exchange_name text, company_name text, sector text, first_month text, last_month text, months_count integer, still_current boolean)
    LANGUAGE sql STABLE
    AS $$
  WITH agg AS (
    SELECT
      um.company_id,
      MIN(um.target_month) AS first_month,
      MAX(um.target_month) AS last_month,
      COUNT(DISTINCT um.target_month)::INTEGER AS months_count
    FROM universe_membership um
    WHERE um.universe_id = p_universe_id
    GROUP BY um.company_id
  ),
  -- Single scalar: the latest month this universe has any membership row
  -- for. Used to flag `still_current`.
  latest_month AS (
    SELECT MAX(target_month) AS m
    FROM universe_membership
    WHERE universe_id = p_universe_id
  ),
  -- For each company, take the most-recent universe_membership row.
  -- DISTINCT ON pattern is Postgres's idiomatic "top-1 per group".
  latest_per_company AS (
    SELECT DISTINCT ON (um.company_id)
      um.company_id,
      um.sector
    FROM universe_membership um
    WHERE um.universe_id = p_universe_id
    ORDER BY um.company_id, um.target_month DESC
  )
  SELECT
    a.company_id,
    c.gurufocus_ticker,
    ge.exchange_code,
    ge.exchange_name,
    c.company_name,
    l.sector,
    a.first_month,
    a.last_month,
    a.months_count,
    (a.last_month = (SELECT m FROM latest_month)) AS still_current
  FROM agg a
  JOIN latest_per_company l ON l.company_id = a.company_id
  LEFT JOIN company c ON c.company_id = a.company_id
  LEFT JOIN gurufocus_exchange ge ON ge.exchange_id = c.exchange_id
  ORDER BY c.gurufocus_ticker NULLS LAST;
$$;


--
-- Name: universe_available_months(integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.universe_available_months(p_universe_id integer) RETURNS TABLE(target_month text)
    LANGUAGE sql STABLE
    AS $$
  SELECT DISTINCT um.target_month
  FROM universe_membership um
  WHERE um.universe_id = p_universe_id
  ORDER BY um.target_month;
$$;


--
-- Name: universe_full_stats(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.universe_full_stats() RETURNS TABLE(universe_id integer, total_rows integer, unique_companies integer, unique_tickers integer, month_count integer, start_month text, end_month text, monthly_counts jsonb, sector_counts jsonb)
    LANGUAGE sql STABLE
    SET search_path TO 'public', 'pg_temp'
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


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: airs_performance; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.airs_performance (
    portefeuille text NOT NULL,
    periode date NOT NULL,
    beginvermogen numeric,
    koersresultaat numeric,
    opbrengsten numeric,
    beleggingsresultaat numeric,
    eindvermogen numeric,
    rendement numeric,
    cumulatief_rendement numeric,
    fetched_at timestamp with time zone DEFAULT now()
);


--
-- Name: api_usage; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.api_usage (
    id integer NOT NULL,
    month text NOT NULL,
    region text NOT NULL,
    request_count integer DEFAULT 0 NOT NULL
);


--
-- Name: api_usage_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.api_usage_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: api_usage_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.api_usage_id_seq OWNED BY public.api_usage.id;


--
-- Name: backtest_cache_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.backtest_cache_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: backtest_cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backtest_cache (
    cache_id integer DEFAULT nextval('public.backtest_cache_id_seq'::regclass) NOT NULL,
    strategy_hash text NOT NULL,
    data_date date DEFAULT CURRENT_DATE NOT NULL,
    config jsonb NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: backtest_run_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.backtest_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: backtest_run; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backtest_run (
    run_id integer DEFAULT nextval('public.backtest_run_id_seq'::regclass) NOT NULL,
    name character varying NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    config jsonb NOT NULL,
    result jsonb DEFAULT '{}'::jsonb
);


--
-- Name: benchmark_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.benchmark_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: benchmark; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.benchmark (
    benchmark_id integer DEFAULT nextval('public.benchmark_id_seq'::regclass) NOT NULL,
    ticker character varying NOT NULL,
    name character varying NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    sector text
);


--
-- Name: benchmark_price; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.benchmark_price (
    benchmark_id integer NOT NULL,
    target_date date NOT NULL,
    price double precision NOT NULL
);


--
-- Name: company_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.company_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: company; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company (
    company_id integer DEFAULT nextval('public.company_id_seq'::regclass) NOT NULL,
    gurufocus_ticker character varying NOT NULL,
    company_name character varying,
    exchange_id integer
);


--
-- Name: company_source; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_source (
    company_id integer NOT NULL,
    source_code character varying NOT NULL,
    first_seen date,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: country; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.country (
    country_code character varying(2) NOT NULL,
    country_name character varying NOT NULL
);


--
-- Name: currency; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.currency (
    currency_code character varying(3) NOT NULL,
    currency_name character varying NOT NULL,
    source character varying DEFAULT 'ecb'::character varying NOT NULL,
    peg_to_usd double precision
);


--
-- Name: current_picks_day; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.current_picks_day (
    strategy_hash text NOT NULL,
    target_date date NOT NULL,
    as_of_date date NOT NULL,
    holdings jsonb NOT NULL,
    portfolio_return_pct numeric,
    next_day_return_pct numeric,
    turnover_abs integer,
    turnover_pct numeric,
    config jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: current_picks_snapshot_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.current_picks_snapshot_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: current_picks_snapshot; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.current_picks_snapshot (
    snapshot_id integer DEFAULT nextval('public.current_picks_snapshot_id_seq'::regclass) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    triggered_by text NOT NULL,
    as_of_date date NOT NULL,
    latest_price_date date,
    config jsonb NOT NULL,
    holdings jsonb NOT NULL,
    daily_picks jsonb DEFAULT '[]'::jsonb NOT NULL,
    strategy_hash text,
    name text,
    ingest_run_id integer,
    backtest_run_id integer,
    scheduled_strategy_id integer,
    kind text,
    is_backfill boolean DEFAULT false NOT NULL,
    period_return_pct double precision,
    CONSTRAINT current_picks_snapshot_kind_check CHECK (((kind IS NULL) OR (kind = ANY (ARRAY['rebalance'::text, 'price_update'::text])))),
    CONSTRAINT current_picks_snapshot_triggered_by_check CHECK ((triggered_by = ANY (ARRAY['auto'::text, 'manual'::text])))
);


--
-- Name: exchange_fee; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.exchange_fee (
    exchange_code character varying NOT NULL,
    fee_bps numeric(10,4) DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    is_broker_supported boolean DEFAULT true NOT NULL
);


--
-- Name: exchange_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.exchange_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fx_rate; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fx_rate (
    currency_code character varying(3) NOT NULL,
    rate_date date NOT NULL,
    rate double precision NOT NULL
);


--
-- Name: gurufocus_exchange; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gurufocus_exchange (
    exchange_id integer DEFAULT nextval('public.exchange_id_seq'::regclass) NOT NULL,
    exchange_code character varying NOT NULL,
    exchange_name character varying NOT NULL,
    is_us boolean DEFAULT false NOT NULL,
    country_code character varying(2) NOT NULL,
    currency_code character varying(3) NOT NULL
);


--
-- Name: ingest_run_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ingest_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ingest_run; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ingest_run (
    run_id integer DEFAULT nextval('public.ingest_run_id_seq'::regclass) NOT NULL,
    job_name text NOT NULL,
    triggered_by text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    status text DEFAULT 'running'::text NOT NULL,
    companies_processed integer DEFAULT 0 NOT NULL,
    prices_refreshed integer DEFAULT 0 NOT NULL,
    volumes_refreshed integer DEFAULT 0 NOT NULL,
    forbidden_count integer DEFAULT 0 NOT NULL,
    delisted_count integer DEFAULT 0 NOT NULL,
    error_count integer DEFAULT 0 NOT NULL,
    error_summary text,
    current_phase text,
    momentum_summary jsonb,
    current_message text,
    companies_total integer,
    templates_summary jsonb,
    CONSTRAINT ingest_run_status_check CHECK ((status = ANY (ARRAY['running'::text, 'ok'::text, 'error'::text]))),
    CONSTRAINT ingest_run_triggered_by_check CHECK ((triggered_by = ANY (ARRAY['auto'::text, 'manual'::text])))
);


--
-- Name: COLUMN ingest_run.templates_summary; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.ingest_run.templates_summary IS 'Array of per-template refresh results: [{template_key, universe_id, this_month, prev_month, additions_count, removals_count, renames_count, additions[], removals[], renames[]}]. Empty array on runs with no enabled templates.';


--
-- Name: leonteq_equity_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.leonteq_equity_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: leonteq_equity; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.leonteq_equity (
    id integer DEFAULT nextval('public.leonteq_equity_id_seq'::regclass) NOT NULL,
    name text NOT NULL,
    ticker text,
    isin text,
    sector text,
    industry text,
    gurufocus_url text,
    company_id integer,
    scraped_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: metric_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metric_data (
    company_id integer NOT NULL,
    metric_code character varying NOT NULL,
    source_code character varying NOT NULL,
    target_date date NOT NULL,
    numeric_value double precision,
    text_value character varying,
    is_prediction boolean DEFAULT false NOT NULL,
    recorded_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: portfolio_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.portfolio_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: portfolio; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.portfolio (
    portfolio_id integer DEFAULT nextval('public.portfolio_id_seq'::regclass) NOT NULL,
    portfolio_name character varying NOT NULL,
    target_date date NOT NULL,
    published_at date
);


--
-- Name: portfolio_weight; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.portfolio_weight (
    portfolio_id integer NOT NULL,
    company_id integer NOT NULL,
    weight_value double precision NOT NULL,
    CONSTRAINT portfolio_weight_weight_value_check CHECK (((weight_value >= (0)::double precision) AND (weight_value <= (1)::double precision)))
);


--
-- Name: scheduled_strategy_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.scheduled_strategy_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: scheduled_strategy; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scheduled_strategy (
    id integer DEFAULT nextval('public.scheduled_strategy_id_seq'::regclass) NOT NULL,
    backtest_run_id integer,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    name text,
    frequency text,
    config jsonb,
    last_run_at timestamp with time zone,
    next_due_at timestamp with time zone,
    backfill_status text,
    backfill_progress_pct integer,
    backfill_message text,
    backfill_error text,
    backfill_started_at timestamp with time zone,
    backfill_finished_at timestamp with time zone,
    CONSTRAINT scheduled_strategy_backfill_status_check CHECK (((backfill_status IS NULL) OR (backfill_status = ANY (ARRAY['running'::text, 'done'::text, 'error'::text])))),
    CONSTRAINT scheduled_strategy_frequency_check CHECK (((frequency IS NULL) OR (frequency = ANY (ARRAY['daily'::text, 'weekly'::text, 'monthly'::text, 'bimonthly'::text, 'quarterly'::text]))))
);


--
-- Name: ticker_override; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ticker_override (
    ticker character varying NOT NULL,
    gurufocus_ticker character varying NOT NULL,
    gurufocus_exchange character varying NOT NULL,
    source character varying DEFAULT 'openfigi'::character varying NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: universe; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.universe (
    universe_id integer NOT NULL,
    label character varying NOT NULL,
    description character varying,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    parent_universe_id integer,
    filter_config jsonb,
    template_key text,
    last_refreshed_at timestamp with time zone
);


--
-- Name: COLUMN universe.template_key; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.universe.template_key IS 'Non-NULL on template-managed universes (one canonical row per template_key, self-updating via the pipeline). NULL on user-created criteria universes.';


--
-- Name: COLUMN universe.last_refreshed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.universe.last_refreshed_at IS 'Set by UniverseTemplate.refresh() on every successful write. Used as the cache-invalidation key + HTTP ETag input. NULL on universes that predate the template abstraction.';


--
-- Name: universe_membership; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.universe_membership (
    universe_id integer NOT NULL,
    company_id integer NOT NULL,
    target_month character varying NOT NULL,
    universe_ticker character varying,
    sector character varying,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    industry character varying
);


--
-- Name: universe_monthly_counts; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.universe_monthly_counts WITH (security_invoker='on') AS
 SELECT universe_id,
    target_month,
    (count(*))::integer AS member_count
   FROM public.universe_membership
  GROUP BY universe_id, target_month;


--
-- Name: universe_sector_counts; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.universe_sector_counts WITH (security_invoker='on') AS
 SELECT universe_id,
    COALESCE(NULLIF((sector)::text, ''::text), '(unknown)'::text) AS sector,
    (count(*))::integer AS member_count
   FROM public.universe_membership
  GROUP BY universe_id, COALESCE(NULLIF((sector)::text, ''::text), '(unknown)'::text);


--
-- Name: universe_stats; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.universe_stats AS
 SELECT u.universe_id,
    u.label,
    u.description,
    u.created_at,
    min((m.target_month)::text) AS start_month,
    max((m.target_month)::text) AS end_month,
    count(DISTINCT m.target_month) AS month_count,
    count(DISTINCT m.universe_ticker) AS total_unique_tickers
   FROM (public.universe u
     LEFT JOIN public.universe_membership m ON ((m.universe_id = u.universe_id)))
  GROUP BY u.universe_id, u.label, u.description, u.created_at
  WITH NO DATA;


--
-- Name: universe_summary; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.universe_summary WITH (security_invoker='on') AS
 SELECT universe_id,
    (count(*))::integer AS total_rows,
    (count(DISTINCT company_id))::integer AS unique_companies,
    (count(DISTINCT universe_ticker))::integer AS unique_tickers,
    (count(DISTINCT target_month))::integer AS month_count,
    min((target_month)::text) AS start_month,
    max((target_month)::text) AS end_month
   FROM public.universe_membership
  GROUP BY universe_id;


--
-- Name: universe_universe_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.universe_universe_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: universe_universe_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.universe_universe_id_seq OWNED BY public.universe.universe_id;


--
-- Name: api_usage id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_usage ALTER COLUMN id SET DEFAULT nextval('public.api_usage_id_seq'::regclass);


--
-- Name: universe universe_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.universe ALTER COLUMN universe_id SET DEFAULT nextval('public.universe_universe_id_seq'::regclass);


--
-- Name: airs_performance airs_performance_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.airs_performance
    ADD CONSTRAINT airs_performance_pkey PRIMARY KEY (portefeuille, periode);


--
-- Name: api_usage api_usage_month_region_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_usage
    ADD CONSTRAINT api_usage_month_region_key UNIQUE (month, region);


--
-- Name: api_usage api_usage_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_usage
    ADD CONSTRAINT api_usage_pkey PRIMARY KEY (id);


--
-- Name: backtest_cache backtest_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backtest_cache
    ADD CONSTRAINT backtest_cache_pkey PRIMARY KEY (cache_id);


--
-- Name: backtest_run backtest_run_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backtest_run
    ADD CONSTRAINT backtest_run_pkey PRIMARY KEY (run_id);


--
-- Name: benchmark benchmark_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.benchmark
    ADD CONSTRAINT benchmark_pkey PRIMARY KEY (benchmark_id);


--
-- Name: benchmark_price benchmark_price_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.benchmark_price
    ADD CONSTRAINT benchmark_price_pkey PRIMARY KEY (benchmark_id, target_date);


--
-- Name: benchmark benchmark_ticker_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.benchmark
    ADD CONSTRAINT benchmark_ticker_key UNIQUE (ticker);


--
-- Name: company company_gurufocus_ticker_exchange_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company
    ADD CONSTRAINT company_gurufocus_ticker_exchange_id_key UNIQUE (gurufocus_ticker, exchange_id);


--
-- Name: company company_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company
    ADD CONSTRAINT company_pkey PRIMARY KEY (company_id);


--
-- Name: company_source company_source_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_source
    ADD CONSTRAINT company_source_pkey PRIMARY KEY (company_id, source_code);


--
-- Name: country country_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.country
    ADD CONSTRAINT country_pkey PRIMARY KEY (country_code);


--
-- Name: currency currency_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.currency
    ADD CONSTRAINT currency_pkey PRIMARY KEY (currency_code);


--
-- Name: current_picks_day current_picks_day_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.current_picks_day
    ADD CONSTRAINT current_picks_day_pkey PRIMARY KEY (strategy_hash, target_date);


--
-- Name: current_picks_snapshot current_picks_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.current_picks_snapshot
    ADD CONSTRAINT current_picks_snapshot_pkey PRIMARY KEY (snapshot_id);


--
-- Name: exchange_fee exchange_fee_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchange_fee
    ADD CONSTRAINT exchange_fee_pkey PRIMARY KEY (exchange_code);


--
-- Name: fx_rate fx_rate_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fx_rate
    ADD CONSTRAINT fx_rate_pkey PRIMARY KEY (currency_code, rate_date);


--
-- Name: gurufocus_exchange gurufocus_exchange_exchange_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gurufocus_exchange
    ADD CONSTRAINT gurufocus_exchange_exchange_code_key UNIQUE (exchange_code);


--
-- Name: gurufocus_exchange gurufocus_exchange_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gurufocus_exchange
    ADD CONSTRAINT gurufocus_exchange_pkey PRIMARY KEY (exchange_id);


--
-- Name: ingest_run ingest_run_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ingest_run
    ADD CONSTRAINT ingest_run_pkey PRIMARY KEY (run_id);


--
-- Name: leonteq_equity leonteq_equity_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.leonteq_equity
    ADD CONSTRAINT leonteq_equity_pkey PRIMARY KEY (id);


--
-- Name: metric_data metric_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metric_data
    ADD CONSTRAINT metric_data_pkey PRIMARY KEY (company_id, metric_code, source_code, target_date);


--
-- Name: portfolio portfolio_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.portfolio
    ADD CONSTRAINT portfolio_pkey PRIMARY KEY (portfolio_id);


--
-- Name: portfolio portfolio_portfolio_name_target_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.portfolio
    ADD CONSTRAINT portfolio_portfolio_name_target_date_key UNIQUE (portfolio_name, target_date);


--
-- Name: portfolio_weight portfolio_weight_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.portfolio_weight
    ADD CONSTRAINT portfolio_weight_pkey PRIMARY KEY (portfolio_id, company_id);


--
-- Name: scheduled_strategy scheduled_strategy_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheduled_strategy
    ADD CONSTRAINT scheduled_strategy_pkey PRIMARY KEY (id);


--
-- Name: ticker_override ticker_override_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ticker_override
    ADD CONSTRAINT ticker_override_pkey PRIMARY KEY (ticker);


--
-- Name: universe universe_label_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.universe
    ADD CONSTRAINT universe_label_key UNIQUE (label);


--
-- Name: universe_membership universe_membership_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.universe_membership
    ADD CONSTRAINT universe_membership_pkey PRIMARY KEY (universe_id, company_id, target_month);


--
-- Name: universe universe_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.universe
    ADD CONSTRAINT universe_pkey PRIMARY KEY (universe_id);


--
-- Name: universe universe_template_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.universe
    ADD CONSTRAINT universe_template_key_key UNIQUE (template_key);


--
-- Name: benchmark_sector_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX benchmark_sector_unique ON public.benchmark USING btree (sector) WHERE (sector IS NOT NULL);


--
-- Name: idx_backtest_cache_created_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_backtest_cache_created_at_desc ON public.backtest_cache USING btree (created_at DESC);


--
-- Name: idx_backtest_cache_hash_date; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_backtest_cache_hash_date ON public.backtest_cache USING btree (strategy_hash, data_date);


--
-- Name: idx_current_picks_backtest_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_current_picks_backtest_run ON public.current_picks_snapshot USING btree (backtest_run_id, created_at DESC);


--
-- Name: idx_current_picks_created_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_current_picks_created_at_desc ON public.current_picks_snapshot USING btree (created_at DESC);


--
-- Name: idx_current_picks_day_hash_month; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_current_picks_day_hash_month ON public.current_picks_day USING btree (strategy_hash, as_of_date);


--
-- Name: idx_current_picks_ingest_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_current_picks_ingest_run ON public.current_picks_snapshot USING btree (ingest_run_id);


--
-- Name: idx_current_picks_scheduled_strategy; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_current_picks_scheduled_strategy ON public.current_picks_snapshot USING btree (scheduled_strategy_id, created_at DESC);


--
-- Name: idx_current_picks_snapshot_hash_asof; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_current_picks_snapshot_hash_asof ON public.current_picks_snapshot USING btree (strategy_hash, as_of_date DESC);


--
-- Name: idx_current_picks_strategy_rebalance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_current_picks_strategy_rebalance ON public.current_picks_snapshot USING btree (scheduled_strategy_id, created_at DESC) WHERE (kind = 'rebalance'::text);


--
-- Name: idx_fx_rate_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_fx_rate_date ON public.fx_rate USING btree (rate_date);


--
-- Name: idx_ingest_run_job_started; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ingest_run_job_started ON public.ingest_run USING btree (job_name, started_at DESC);


--
-- Name: idx_ingest_run_started_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ingest_run_started_at_desc ON public.ingest_run USING btree (started_at DESC);


--
-- Name: idx_leonteq_equity_company_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_leonteq_equity_company_id ON public.leonteq_equity USING btree (company_id);


--
-- Name: idx_leonteq_equity_scraped_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_leonteq_equity_scraped_at ON public.leonteq_equity USING btree (scraped_at DESC);


--
-- Name: idx_leonteq_equity_sector; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_leonteq_equity_sector ON public.leonteq_equity USING btree (sector, industry);


--
-- Name: idx_metric_data_source_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_metric_data_source_date ON public.metric_data USING btree (source_code, target_date);


--
-- Name: idx_scheduled_strategy_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scheduled_strategy_due ON public.scheduled_strategy USING btree (enabled, next_due_at);


--
-- Name: idx_scheduled_strategy_enabled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scheduled_strategy_enabled ON public.scheduled_strategy USING btree (enabled, created_at);


--
-- Name: idx_universe_membership_company; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_universe_membership_company ON public.universe_membership USING btree (company_id);


--
-- Name: idx_universe_membership_month; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_universe_membership_month ON public.universe_membership USING btree (universe_id, target_month);


--
-- Name: idx_universe_parent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_universe_parent ON public.universe USING btree (parent_universe_id);


--
-- Name: universe_stats_universe_id_uniq; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX universe_stats_universe_id_uniq ON public.universe_stats USING btree (universe_id);


--
-- Name: benchmark_price benchmark_price_benchmark_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.benchmark_price
    ADD CONSTRAINT benchmark_price_benchmark_id_fkey FOREIGN KEY (benchmark_id) REFERENCES public.benchmark(benchmark_id) ON DELETE CASCADE;


--
-- Name: company company_exchange_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company
    ADD CONSTRAINT company_exchange_id_fkey FOREIGN KEY (exchange_id) REFERENCES public.gurufocus_exchange(exchange_id);


--
-- Name: company_source company_source_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_source
    ADD CONSTRAINT company_source_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.company(company_id) ON DELETE CASCADE;


--
-- Name: current_picks_snapshot current_picks_snapshot_backtest_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.current_picks_snapshot
    ADD CONSTRAINT current_picks_snapshot_backtest_run_id_fkey FOREIGN KEY (backtest_run_id) REFERENCES public.backtest_run(run_id) ON DELETE SET NULL;


--
-- Name: current_picks_snapshot current_picks_snapshot_ingest_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.current_picks_snapshot
    ADD CONSTRAINT current_picks_snapshot_ingest_run_id_fkey FOREIGN KEY (ingest_run_id) REFERENCES public.ingest_run(run_id) ON DELETE SET NULL;


--
-- Name: current_picks_snapshot current_picks_snapshot_scheduled_strategy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.current_picks_snapshot
    ADD CONSTRAINT current_picks_snapshot_scheduled_strategy_id_fkey FOREIGN KEY (scheduled_strategy_id) REFERENCES public.scheduled_strategy(id) ON DELETE SET NULL;


--
-- Name: exchange_fee exchange_fee_exchange_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchange_fee
    ADD CONSTRAINT exchange_fee_exchange_code_fkey FOREIGN KEY (exchange_code) REFERENCES public.gurufocus_exchange(exchange_code) ON DELETE CASCADE;


--
-- Name: fx_rate fx_rate_currency_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fx_rate
    ADD CONSTRAINT fx_rate_currency_code_fkey FOREIGN KEY (currency_code) REFERENCES public.currency(currency_code);


--
-- Name: gurufocus_exchange gurufocus_exchange_country_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gurufocus_exchange
    ADD CONSTRAINT gurufocus_exchange_country_code_fkey FOREIGN KEY (country_code) REFERENCES public.country(country_code);


--
-- Name: gurufocus_exchange gurufocus_exchange_currency_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gurufocus_exchange
    ADD CONSTRAINT gurufocus_exchange_currency_code_fkey FOREIGN KEY (currency_code) REFERENCES public.currency(currency_code);


--
-- Name: leonteq_equity leonteq_equity_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.leonteq_equity
    ADD CONSTRAINT leonteq_equity_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.company(company_id) ON DELETE SET NULL;


--
-- Name: metric_data metric_data_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metric_data
    ADD CONSTRAINT metric_data_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.company(company_id);


--
-- Name: portfolio_weight portfolio_weight_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.portfolio_weight
    ADD CONSTRAINT portfolio_weight_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.company(company_id);


--
-- Name: portfolio_weight portfolio_weight_portfolio_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.portfolio_weight
    ADD CONSTRAINT portfolio_weight_portfolio_id_fkey FOREIGN KEY (portfolio_id) REFERENCES public.portfolio(portfolio_id);


--
-- Name: scheduled_strategy scheduled_strategy_backtest_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheduled_strategy
    ADD CONSTRAINT scheduled_strategy_backtest_run_id_fkey FOREIGN KEY (backtest_run_id) REFERENCES public.backtest_run(run_id) ON DELETE CASCADE;


--
-- Name: universe_membership universe_membership_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.universe_membership
    ADD CONSTRAINT universe_membership_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.company(company_id) ON DELETE CASCADE;


--
-- Name: universe_membership universe_membership_universe_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.universe_membership
    ADD CONSTRAINT universe_membership_universe_id_fkey FOREIGN KEY (universe_id) REFERENCES public.universe(universe_id) ON DELETE CASCADE;


--
-- Name: universe universe_parent_universe_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.universe
    ADD CONSTRAINT universe_parent_universe_id_fkey FOREIGN KEY (parent_universe_id) REFERENCES public.universe(universe_id) ON DELETE SET NULL;


--
-- Name: airs_performance; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.airs_performance ENABLE ROW LEVEL SECURITY;

--
-- Name: api_usage; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.api_usage ENABLE ROW LEVEL SECURITY;

--
-- Name: backtest_cache; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.backtest_cache ENABLE ROW LEVEL SECURITY;

--
-- Name: backtest_run; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.backtest_run ENABLE ROW LEVEL SECURITY;

--
-- Name: benchmark; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.benchmark ENABLE ROW LEVEL SECURITY;

--
-- Name: benchmark_price; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.benchmark_price ENABLE ROW LEVEL SECURITY;

--
-- Name: company; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.company ENABLE ROW LEVEL SECURITY;

--
-- Name: company_source; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.company_source ENABLE ROW LEVEL SECURITY;

--
-- Name: country; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.country ENABLE ROW LEVEL SECURITY;

--
-- Name: currency; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.currency ENABLE ROW LEVEL SECURITY;

--
-- Name: current_picks_day; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.current_picks_day ENABLE ROW LEVEL SECURITY;

--
-- Name: current_picks_snapshot; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.current_picks_snapshot ENABLE ROW LEVEL SECURITY;

--
-- Name: exchange_fee; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.exchange_fee ENABLE ROW LEVEL SECURITY;

--
-- Name: fx_rate; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.fx_rate ENABLE ROW LEVEL SECURITY;

--
-- Name: gurufocus_exchange; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.gurufocus_exchange ENABLE ROW LEVEL SECURITY;

--
-- Name: ingest_run; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.ingest_run ENABLE ROW LEVEL SECURITY;

--
-- Name: metric_data; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.metric_data ENABLE ROW LEVEL SECURITY;

--
-- Name: portfolio; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.portfolio ENABLE ROW LEVEL SECURITY;

--
-- Name: portfolio_weight; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.portfolio_weight ENABLE ROW LEVEL SECURITY;

--
-- Name: scheduled_strategy; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.scheduled_strategy ENABLE ROW LEVEL SECURITY;

--
-- Name: ticker_override; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.ticker_override ENABLE ROW LEVEL SECURITY;

--
-- Name: universe; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.universe ENABLE ROW LEVEL SECURITY;

--
-- Name: universe_membership; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.universe_membership ENABLE ROW LEVEL SECURITY;

--
-- PostgreSQL database dump complete
--


--
-- PostgreSQL database dump
--


-- Dumped from database version 17.6
-- Dumped by pg_dump version 17.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: country; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.country (country_code, country_name) FROM stdin;
AE	United Arab Emirates
AT	Austria
AU	Australia
BE	Belgium
BR	Brazil
CA	Canada
CH	Switzerland
CL	Chile
CN	China
CO	Colombia
CZ	Czech Republic
DE	Germany
DK	Denmark
EG	Egypt
ES	Spain
FI	Finland
FR	France
GB	United Kingdom
GR	Greece
HK	Hong Kong
HU	Hungary
ID	Indonesia
IE	Ireland
IL	Israel
IN	India
IS	Iceland
IT	Italy
JP	Japan
KR	South Korea
KW	Kuwait
MX	Mexico
MY	Malaysia
NL	Netherlands
NO	Norway
NZ	New Zealand
PH	Philippines
PL	Poland
PT	Portugal
QA	Qatar
RO	Romania
RU	Russia
SA	Saudi Arabia
SE	Sweden
SG	Singapore
TH	Thailand
TR	Turkey
TW	Taiwan
US	United States
ZA	South Africa
\.


--
-- Data for Name: currency; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.currency (currency_code, currency_name, source, peg_to_usd) FROM stdin;
AED	UAE Dirham	pegged	3.6725
AUD	Australian Dollar	ecb	\N
BRL	Brazilian Real	ecb	\N
CAD	Canadian Dollar	ecb	\N
CHF	Swiss Franc	ecb	\N
CLP	Chilean Peso	ecb	\N
CNY	Chinese Yuan	ecb	\N
COP	Colombian Peso	ecb	\N
CZK	Czech Koruna	ecb	\N
DKK	Danish Krone	ecb	\N
EGP	Egyptian Pound	ecb	\N
EUR	Euro	ecb	\N
GBP	British Pound	ecb	\N
HKD	Hong Kong Dollar	ecb	\N
HUF	Hungarian Forint	ecb	\N
IDR	Indonesian Rupiah	ecb	\N
ILS	Israeli Shekel	ecb	\N
INR	Indian Rupee	ecb	\N
ISK	Icelandic Krona	ecb	\N
JPY	Japanese Yen	ecb	\N
KRW	South Korean Won	ecb	\N
KWD	Kuwaiti Dinar	pegged	0.306
MXN	Mexican Peso	ecb	\N
MYR	Malaysian Ringgit	ecb	\N
NOK	Norwegian Krone	ecb	\N
NZD	New Zealand Dollar	ecb	\N
PHP	Philippine Peso	ecb	\N
PLN	Polish Zloty	ecb	\N
QAR	Qatari Riyal	pegged	3.64
RON	Romanian Leu	ecb	\N
RUB	Russian Ruble	ecb	\N
SAR	Saudi Riyal	pegged	3.75
SEK	Swedish Krona	ecb	\N
SGD	Singapore Dollar	ecb	\N
THB	Thai Baht	ecb	\N
TRY	Turkish Lira	ecb	\N
TWD	New Taiwan Dollar	yahoo	\N
USD	US Dollar	ecb	\N
ZAR	South African Rand	ecb	\N
\.


--
-- Data for Name: gurufocus_exchange; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.gurufocus_exchange (exchange_id, exchange_code, exchange_name, is_us, country_code, currency_code) FROM stdin;
1	NYSE	New York Stock Exchange	t	US	USD
2	NASDAQ	NASDAQ	t	US	USD
3	CBOE	Cboe BZX	t	US	USD
4	LSE	London Stock Exchange	f	GB	GBP
5	XTER	Xetra	f	DE	EUR
6	XPAR	Euronext Paris	f	FR	EUR
7	XAMS	Euronext Amsterdam	f	NL	EUR
8	XBRU	Euronext Brussels	f	BE	EUR
9	XLIS	Euronext Lisbon	f	PT	EUR
10	MIL	Borsa Italiana	f	IT	EUR
11	XMAD	Bolsa de Madrid	f	ES	EUR
12	XSWX	SIX Swiss Exchange	f	CH	CHF
13	OSTO	Nasdaq Stockholm	f	SE	SEK
14	OCSE	Nasdaq Copenhagen	f	DK	DKK
15	OSL	Oslo Bors	f	NO	NOK
16	OHEL	Nasdaq Helsinki	f	FI	EUR
17	WAR	Warsaw Stock Exchange	f	PL	PLN
19	ATH	Athens Exchange	f	GR	EUR
23	IST	Istanbul Stock Exchange	f	TR	TRY
24	TSX	Toronto Stock Exchange	f	CA	CAD
25	TSXV	TSX Venture Exchange	f	CA	CAD
26	MEX	Bolsa Mexicana de Valores	f	MX	MXN
27	BMV	Bolsa Mexicana (alt)	f	MX	MXN
28	BSP	B3 (Brazil)	f	BR	BRL
30	BOG	Bolsa de Colombia	f	CO	COP
31	TSE	Tokyo Stock Exchange	f	JP	JPY
32	HKSE	Hong Kong Stock Exchange	f	HK	HKD
34	SZSE	Shenzhen Stock Exchange	f	CN	CNY
37	XKRX	Korea Exchange	f	KR	KRW
38	NSE	National Stock Exchange India	f	IN	INR
39	BSE	BSE India	f	IN	INR
40	ASX	Australian Stock Exchange	f	AU	AUD
41	NZSE	New Zealand Exchange	f	NZ	NZD
42	SGX	Singapore Exchange	f	SG	SGD
48	ADX	Abu Dhabi Securities Exchange	f	AE	AED
49	DFM	Dubai Financial Market	f	AE	AED
53	JSE	Johannesburg Stock Exchange	f	ZA	ZAR
56	FRA	Frankfurt Stock Exchange	f	DE	EUR
18	WBO	Wiener Boerse	f	AT	EUR
22	XPRA	Prague Stock Exchange	f	CZ	CZK
20	DUB	Irish Stock Exchange	f	IE	EUR
21	BUD	Budapest Stock Exchange	f	HU	HUF
33	SHSE	Shanghai Stock Exchange	f	CN	CNY
35	TPE	Taiwan Stock Exchange	f	TW	TWD
36	ROCO	Gretai Securities Market	f	TW	TWD
43	XKLS	Bursa Malaysia	f	MY	MYR
44	ISX	Indonesia Stock Exchange	f	ID	IDR
45	BKK	Stock Exchange of Thailand	f	TH	THB
46	PHS	Philippine Stock Exchange	f	PH	PHP
47	SAU	Saudi Stock Exchange	f	SA	SAR
50	DSMD	Qatar Exchange	f	QA	QAR
51	KUW	Kuwait Stock Exchange	f	KW	KWD
52	XTAE	Tel Aviv Stock Exchange	f	IL	ILS
54	CAI	Egyptian Exchange	f	EG	EGP
55	MIC	Moscow Exchange	f	RU	RUB
29	XSGO	Santiago Stock Exchange	f	CL	CLP
\.


--
-- Data for Name: exchange_fee; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.exchange_fee (exchange_code, fee_bps, updated_at, is_broker_supported) FROM stdin;
NYSE	10.0000	2026-05-15 11:32:39.101822+00	t
NASDAQ	10.0000	2026-05-15 11:32:39.101822+00	t
CBOE	10.0000	2026-05-15 11:32:39.101822+00	t
LSE	10.0000	2026-05-15 11:32:39.101822+00	t
XTER	10.0000	2026-05-15 11:32:39.101822+00	t
XPAR	10.0000	2026-05-15 11:32:39.101822+00	t
XAMS	10.0000	2026-05-15 11:32:39.101822+00	t
XBRU	10.0000	2026-05-15 11:32:39.101822+00	t
XLIS	10.0000	2026-05-15 11:32:39.101822+00	t
MIL	10.0000	2026-05-15 11:32:39.101822+00	t
XMAD	10.0000	2026-05-15 11:32:39.101822+00	t
XSWX	10.0000	2026-05-15 11:32:39.101822+00	t
OSTO	10.0000	2026-05-15 11:32:39.101822+00	t
OCSE	10.0000	2026-05-15 11:32:39.101822+00	t
OSL	10.0000	2026-05-15 11:32:39.101822+00	t
OHEL	10.0000	2026-05-15 11:32:39.101822+00	t
WAR	10.0000	2026-05-15 11:32:39.101822+00	t
ATH	10.0000	2026-05-20 10:43:57.10074+00	t
IST	10.0000	2026-05-15 11:32:39.101822+00	t
TSX	10.0000	2026-05-15 11:32:39.101822+00	t
TSXV	10.0000	2026-05-15 11:32:39.101822+00	t
MEX	10.0000	2026-05-20 10:43:57.10074+00	t
BMV	10.0000	2026-05-20 10:43:57.10074+00	t
BSP	10.0000	2026-05-20 10:43:57.10074+00	t
BOG	10.0000	2026-05-15 11:32:39.101822+00	t
TSE	10.0000	2026-05-15 11:32:39.101822+00	t
HKSE	10.0000	2026-05-15 11:32:39.101822+00	t
SZSE	10.0000	2026-05-20 10:43:57.10074+00	t
XKRX	10.0000	2026-05-15 11:32:39.101822+00	t
NSE	10.0000	2026-05-15 11:32:39.101822+00	t
BSE	10.0000	2026-05-15 11:32:39.101822+00	t
ASX	10.0000	2026-05-15 11:32:39.101822+00	t
NZSE	10.0000	2026-05-20 10:43:57.10074+00	t
SGX	10.0000	2026-05-15 11:32:39.101822+00	t
ADX	10.0000	2026-05-15 11:32:39.101822+00	t
DFM	10.0000	2026-05-15 11:32:39.101822+00	t
JSE	10.0000	2026-05-15 11:32:39.101822+00	t
FRA	10.0000	2026-05-15 11:32:39.101822+00	t
WBO	10.0000	2026-05-15 11:32:39.101822+00	t
XPRA	10.0000	2026-05-15 11:32:39.101822+00	t
DUB	10.0000	2026-05-15 11:32:39.101822+00	t
BUD	10.0000	2026-05-15 11:32:39.101822+00	t
SHSE	10.0000	2026-05-20 10:43:57.10074+00	t
TPE	10.0000	2026-05-15 11:32:39.101822+00	t
ROCO	10.0000	2026-05-20 10:43:57.10074+00	t
XKLS	10.0000	2026-05-15 11:32:39.101822+00	t
ISX	10.0000	2026-05-15 11:32:39.101822+00	t
BKK	10.0000	2026-05-15 11:32:39.101822+00	t
PHS	10.0000	2026-05-15 11:32:39.101822+00	t
SAU	10.0000	2026-05-15 11:32:39.101822+00	t
DSMD	10.0000	2026-05-15 11:32:39.101822+00	t
KUW	10.0000	2026-05-15 11:32:39.101822+00	t
XTAE	10.0000	2026-05-15 11:32:39.101822+00	t
CAI	10.0000	2026-05-15 11:32:39.101822+00	t
MIC	10.0000	2026-05-15 11:32:39.101822+00	t
XSGO	10.0000	2026-05-15 11:32:39.101822+00	t
\.


--
-- PostgreSQL database dump complete
--


