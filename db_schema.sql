--
-- PostgreSQL database dump
--


-- Dumped from database version 16.10
-- Dumped by pg_dump version 16.10

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

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: entities; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.entities (
    entity_id integer NOT NULL,
    canonical_id character varying(100) NOT NULL,
    name character varying(200) NOT NULL,
    symbol character varying(50),
    entity_type character varying(50) DEFAULT 'token'::character varying NOT NULL,
    asset_class character varying(50) DEFAULT 'crypto'::character varying NOT NULL,
    sector character varying(100),
    parent_chain character varying(100),
    coingecko_id character varying(100),
    created_at timestamp without time zone DEFAULT now(),
    is_active boolean DEFAULT true
);


--
-- Name: entities_entity_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.entities_entity_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: entities_entity_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.entities_entity_id_seq OWNED BY public.entities.entity_id;


--
-- Name: entity_source_ids; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.entity_source_ids (
    id integer NOT NULL,
    entity_id integer NOT NULL,
    source character varying(100) NOT NULL,
    source_id character varying(200) NOT NULL,
    source_id_type character varying(50)
);


--
-- Name: entity_source_ids_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.entity_source_ids_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: entity_source_ids_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.entity_source_ids_id_seq OWNED BY public.entity_source_ids.id;


--
-- Name: metrics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metrics (
    id integer NOT NULL,
    pulled_at timestamp without time zone NOT NULL,
    source character varying(100) NOT NULL,
    asset character varying(100) NOT NULL,
    metric_name character varying(200) NOT NULL,
    value double precision,
    domain character varying(50),
    exchange character varying(100),
    entity_id integer,
    granularity character varying(20) DEFAULT 'daily'::character varying
);


--
-- Name: metrics_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.metrics_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: metrics_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.metrics_id_seq OWNED BY public.metrics.id;


--
-- Name: pulls; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pulls (
    pull_id integer NOT NULL,
    source_name character varying(100) NOT NULL,
    pulled_at timestamp without time zone DEFAULT now() NOT NULL,
    status character varying(50) NOT NULL,
    records_count integer DEFAULT 0,
    notes text
);


--
-- Name: pulls_pull_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pulls_pull_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pulls_pull_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pulls_pull_id_seq OWNED BY public.pulls.pull_id;


--
-- Name: v_derivatives_summary; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_derivatives_summary AS
 SELECT e.canonical_id,
    e.symbol,
    m.exchange,
    date_trunc('hour'::text, m.pulled_at) AS ts,
    max(
        CASE
            WHEN ((m.metric_name)::text = 'FUNDING_RATE'::text) THEN m.value
            ELSE NULL::double precision
        END) AS funding_rate,
    max(
        CASE
            WHEN ((m.metric_name)::text = 'DOLLAR_OI_CLOSE'::text) THEN m.value
            ELSE NULL::double precision
        END) AS open_interest_usd,
    max(
        CASE
            WHEN ((m.metric_name)::text = 'LIQ_DOLLAR_VOL'::text) THEN m.value
            ELSE NULL::double precision
        END) AS liquidations_usd
   FROM ((public.metrics m
     JOIN public.entity_source_ids esi ON ((((m.source)::text = (esi.source)::text) AND ((m.asset)::text = (esi.source_id)::text))))
     JOIN public.entities e ON ((esi.entity_id = e.entity_id)))
  WHERE ((m.domain)::text = 'derivative'::text)
  GROUP BY e.canonical_id, e.symbol, m.exchange, (date_trunc('hour'::text, m.pulled_at));


--
-- Name: v_velo_daily; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_velo_daily AS
 SELECT date_trunc('day'::text, pulled_at) AS ts,
    source,
    asset,
    entity_id,
    metric_name,
    exchange,
    domain,
        CASE
            WHEN ((metric_name)::text = 'OPEN_PRICE'::text) THEN (array_agg(value ORDER BY pulled_at))[1]
            WHEN ((metric_name)::text = 'CLOSE_PRICE'::text) THEN (array_agg(value ORDER BY pulled_at DESC))[1]
            WHEN ((metric_name)::text ~~ '%_HIGH'::text) THEN max(value)
            WHEN ((metric_name)::text ~~ '%_LOW'::text) THEN min(value)
            WHEN ((metric_name)::text ~~ 'HIGH_%'::text) THEN max(value)
            WHEN ((metric_name)::text ~~ 'LOW_%'::text) THEN min(value)
            WHEN ((metric_name)::text ~~ '%OI_CLOSE'::text) THEN (array_agg(value ORDER BY pulled_at DESC))[1]
            WHEN ((metric_name)::text ~~ '%OI_HIGH'::text) THEN max(value)
            WHEN ((metric_name)::text ~~ '%OI_LOW'::text) THEN min(value)
            WHEN ((metric_name)::text ~~ '%VOLUME%'::text) THEN sum(value)
            WHEN ((metric_name)::text ~~ '%LIQ%'::text) THEN sum(value)
            WHEN ((metric_name)::text ~~ '%TRADES%'::text) THEN sum(value)
            WHEN ((metric_name)::text ~~ '%LIQUIDATIONS%'::text) THEN sum(value)
            WHEN ((metric_name)::text = ANY ((ARRAY['FUNDING_RATE'::character varying, 'FUNDING_RATE_AVG'::character varying, 'PREMIUM'::character varying])::text[])) THEN avg(value)
            ELSE (array_agg(value ORDER BY pulled_at DESC))[1]
        END AS value,
    count(*) AS hours_in_day
   FROM public.metrics
  WHERE (((source)::text = 'velo'::text) AND ((granularity)::text = 'hourly'::text))
  GROUP BY (date_trunc('day'::text, pulled_at)), source, asset, entity_id, metric_name, exchange, domain;


--
-- Name: v_metrics_daily; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_metrics_daily AS
 SELECT ts,
    source,
    asset,
    entity_id,
    metric_name,
    value,
    domain,
    exchange
   FROM ( SELECT date_trunc('day'::text, metrics.pulled_at) AS ts,
            metrics.source,
            metrics.asset,
            metrics.entity_id,
            metrics.metric_name,
            metrics.value,
            metrics.domain,
            metrics.exchange
           FROM public.metrics
          WHERE ((metrics.granularity)::text = 'daily'::text)
        UNION ALL
         SELECT v_velo_daily.ts,
            v_velo_daily.source,
            v_velo_daily.asset,
            v_velo_daily.entity_id,
            v_velo_daily.metric_name,
            v_velo_daily.value,
            v_velo_daily.domain,
            v_velo_daily.exchange
           FROM public.v_velo_daily) combined;


--
-- Name: v_normalized_metrics; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_normalized_metrics AS
 SELECT ts,
    source,
    asset,
    entity_id,
        CASE
            WHEN (((source)::text = 'velo'::text) AND ((metric_name)::text = 'CLOSE_PRICE'::text)) THEN 'PRICE'::character varying
            WHEN (((source)::text = 'velo'::text) AND ((metric_name)::text = 'DOLLAR_OI_CLOSE'::text)) THEN 'OPEN_INTEREST'::character varying
            WHEN (((source)::text = 'velo'::text) AND ((metric_name)::text = 'DOLLAR_VOLUME'::text)) THEN 'TRADING_VOLUME_24H'::character varying
            WHEN (((source)::text = 'velo'::text) AND ((metric_name)::text = 'LIQ_DOLLAR_VOL'::text)) THEN 'LIQUIDATIONS_24H'::character varying
            ELSE metric_name
        END AS metric_name,
    value,
    domain,
    exchange
   FROM public.v_metrics_daily;


--
-- Name: v_entity_comparison; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_entity_comparison AS
 SELECT e.canonical_id,
    e.name,
    e.symbol,
    e.entity_type,
    e.sector,
    m.ts,
    m.source,
    m.metric_name,
    m.value,
    m.domain,
    m.exchange
   FROM ((public.v_normalized_metrics m
     JOIN public.entity_source_ids esi ON ((((m.source)::text = (esi.source)::text) AND ((m.asset)::text = (esi.source_id)::text))))
     JOIN public.entities e ON ((esi.entity_id = e.entity_id)));


--
-- Name: v_entity_metrics; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_entity_metrics AS
 SELECT e.canonical_id,
    e.name,
    e.symbol,
    e.entity_type,
    e.sector,
    m.pulled_at AS ts,
    m.source,
    m.metric_name,
    m.value,
    m.domain,
    m.exchange
   FROM ((public.metrics m
     JOIN public.entity_source_ids esi ON ((((m.source)::text = (esi.source)::text) AND ((m.asset)::text = (esi.source_id)::text))))
     JOIN public.entities e ON ((esi.entity_id = e.entity_id)));


--
-- Name: v_latest_metrics; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_latest_metrics AS
 SELECT DISTINCT ON (m.source, m.asset, m.metric_name, m.exchange) e.canonical_id,
    m.source,
    m.asset,
    m.metric_name,
    m.exchange,
    m.pulled_at AS ts,
    m.value,
    m.domain
   FROM ((public.metrics m
     LEFT JOIN public.entity_source_ids esi ON ((((m.source)::text = (esi.source)::text) AND ((m.asset)::text = (esi.source_id)::text))))
     LEFT JOIN public.entities e ON ((esi.entity_id = e.entity_id)))
  ORDER BY m.source, m.asset, m.metric_name, m.exchange, m.pulled_at DESC;


--
-- Name: entities entity_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entities ALTER COLUMN entity_id SET DEFAULT nextval('public.entities_entity_id_seq'::regclass);


--
-- Name: entity_source_ids id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_source_ids ALTER COLUMN id SET DEFAULT nextval('public.entity_source_ids_id_seq'::regclass);


--
-- Name: metrics id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metrics ALTER COLUMN id SET DEFAULT nextval('public.metrics_id_seq'::regclass);


--
-- Name: pulls pull_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pulls ALTER COLUMN pull_id SET DEFAULT nextval('public.pulls_pull_id_seq'::regclass);


--
-- Name: entities entities_canonical_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_canonical_id_key UNIQUE (canonical_id);


--
-- Name: entities entities_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_pkey PRIMARY KEY (entity_id);


--
-- Name: entity_source_ids entity_source_ids_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_source_ids
    ADD CONSTRAINT entity_source_ids_pkey PRIMARY KEY (id);


--
-- Name: entity_source_ids entity_source_ids_source_source_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_source_ids
    ADD CONSTRAINT entity_source_ids_source_source_id_key UNIQUE (source, source_id);


--
-- Name: metrics metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metrics
    ADD CONSTRAINT metrics_pkey PRIMARY KEY (id);


--
-- Name: pulls pulls_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pulls
    ADD CONSTRAINT pulls_pkey PRIMARY KEY (pull_id);


--
-- Name: idx_entities_sector; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_entities_sector ON public.entities USING btree (sector);


--
-- Name: idx_entities_symbol; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_entities_symbol ON public.entities USING btree (symbol);


--
-- Name: idx_entities_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_entities_type ON public.entities USING btree (entity_type);


--
-- Name: idx_entity_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_entity_source ON public.entity_source_ids USING btree (source, source_id);


--
-- Name: idx_metrics_domain; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_metrics_domain ON public.metrics USING btree (domain, pulled_at);


--
-- Name: idx_metrics_entity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_metrics_entity ON public.metrics USING btree (entity_id, metric_name, pulled_at);


--
-- Name: idx_metrics_exchange; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_metrics_exchange ON public.metrics USING btree (exchange, pulled_at) WHERE (exchange IS NOT NULL);


--
-- Name: idx_metrics_granularity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_metrics_granularity ON public.metrics USING btree (granularity, pulled_at);


--
-- Name: idx_metrics_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_metrics_lookup ON public.metrics USING btree (source, asset, metric_name, pulled_at);


--
-- Name: idx_metrics_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_metrics_ts ON public.metrics USING btree (pulled_at);


--
-- Name: idx_metrics_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_metrics_unique ON public.metrics USING btree (source, asset, metric_name, pulled_at, COALESCE(exchange, ''::character varying));


--
-- Name: idx_source_ids_entity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_source_ids_entity ON public.entity_source_ids USING btree (entity_id);


--
-- Name: idx_source_ids_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_source_ids_lookup ON public.entity_source_ids USING btree (source, source_id);


--
-- Name: metrics_unique_with_exchange; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX metrics_unique_with_exchange ON public.metrics USING btree (source, asset, metric_name, pulled_at, COALESCE(exchange, ''::character varying));


--
-- Name: entity_source_ids entity_source_ids_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_source_ids
    ADD CONSTRAINT entity_source_ids_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(entity_id) ON DELETE CASCADE;


--
-- Name: metrics metrics_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metrics
    ADD CONSTRAINT metrics_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(entity_id);


--
-- PostgreSQL database dump complete
--


