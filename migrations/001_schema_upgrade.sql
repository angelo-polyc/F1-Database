-- =============================================================================
-- SCHEMA MIGRATION 001: Add Entity Master + Enhanced Metrics for Velo
-- =============================================================================
-- SAFETY: Entire migration wrapped in transaction - rolls back on any error
-- Run this ONCE before deploying Velo integration
-- Safe to run multiple times (uses IF NOT EXISTS and ON CONFLICT DO NOTHING)
-- =============================================================================

BEGIN;

-- =============================================================================
-- PART 1: ENTITY MASTER TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS entities (
    entity_id SERIAL PRIMARY KEY,
    canonical_id VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    symbol VARCHAR(50),
    entity_type VARCHAR(50) NOT NULL DEFAULT 'token',
    asset_class VARCHAR(50) NOT NULL DEFAULT 'crypto',
    sector VARCHAR(100),
    parent_chain VARCHAR(100),
    coingecko_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_symbol ON entities(symbol);
CREATE INDEX IF NOT EXISTS idx_entities_sector ON entities(sector);

-- =============================================================================
-- PART 2: SOURCE ID MAPPING TABLE (Rosetta Stone)
-- =============================================================================

CREATE TABLE IF NOT EXISTS entity_source_ids (
    id SERIAL PRIMARY KEY,
    entity_id INTEGER NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    source VARCHAR(100) NOT NULL,
    source_id VARCHAR(200) NOT NULL,
    source_id_type VARCHAR(50),
    UNIQUE(source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_source_ids_lookup ON entity_source_ids(source, source_id);
CREATE INDEX IF NOT EXISTS idx_source_ids_entity ON entity_source_ids(entity_id);

-- =============================================================================
-- PART 3: ENHANCE METRICS TABLE (Safe column additions)
-- =============================================================================

ALTER TABLE metrics ADD COLUMN IF NOT EXISTS domain VARCHAR(50);
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS exchange VARCHAR(100);
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS entity_id INTEGER REFERENCES entities(entity_id);
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS granularity VARCHAR(20) DEFAULT 'daily';

-- Backfill existing data as 'daily' (small update, safe)
UPDATE metrics SET granularity = 'daily' WHERE granularity IS NULL AND source IN ('artemis', 'defillama');

-- =============================================================================
-- PART 4: UPDATE UNIQUE CONSTRAINT (Safe index operations)
-- =============================================================================

DROP INDEX IF EXISTS idx_metrics_unique;

CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_unique 
ON metrics (source, asset, metric_name, pulled_at, COALESCE(exchange, ''));

-- =============================================================================
-- PART 5: ADD PERFORMANCE INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_metrics_ts ON metrics(pulled_at);
CREATE INDEX IF NOT EXISTS idx_metrics_domain ON metrics(domain, pulled_at);
CREATE INDEX IF NOT EXISTS idx_metrics_entity ON metrics(entity_id, metric_name, pulled_at);
CREATE INDEX IF NOT EXISTS idx_metrics_exchange ON metrics(exchange, pulled_at) WHERE exchange IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_metrics_granularity ON metrics(granularity, pulled_at);

-- =============================================================================
-- PART 6: PULLS TABLE ENHANCEMENT
-- =============================================================================

ALTER TABLE pulls ADD COLUMN IF NOT EXISTS notes TEXT;

-- =============================================================================
-- PART 7: VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW v_entity_metrics AS
SELECT 
    e.canonical_id,
    e.name,
    e.symbol,
    e.entity_type,
    e.sector,
    m.pulled_at as ts,
    m.source,
    m.metric_name,
    m.value,
    m.domain,
    m.exchange
FROM metrics m
JOIN entity_source_ids esi ON m.source = esi.source AND m.asset = esi.source_id
JOIN entities e ON esi.entity_id = e.entity_id;

CREATE OR REPLACE VIEW v_derivatives_summary AS
SELECT 
    e.canonical_id,
    e.symbol,
    m.exchange,
    DATE_TRUNC('hour', m.pulled_at) as ts,
    MAX(CASE WHEN m.metric_name = 'FUNDING_RATE' THEN m.value END) as funding_rate,
    MAX(CASE WHEN m.metric_name = 'DOLLAR_OI_CLOSE' THEN m.value END) as open_interest_usd,
    MAX(CASE WHEN m.metric_name = 'LIQ_DOLLAR_VOL' THEN m.value END) as liquidations_usd
FROM metrics m
JOIN entity_source_ids esi ON m.source = esi.source AND m.asset = esi.source_id
JOIN entities e ON esi.entity_id = e.entity_id
WHERE m.domain = 'derivative'
GROUP BY e.canonical_id, e.symbol, m.exchange, DATE_TRUNC('hour', m.pulled_at);

CREATE OR REPLACE VIEW v_velo_daily AS
SELECT 
    DATE_TRUNC('day', pulled_at) as ts,
    source,
    asset,
    entity_id,
    metric_name,
    exchange,
    domain,
    CASE 
        WHEN metric_name = 'OPEN_PRICE' THEN (ARRAY_AGG(value ORDER BY pulled_at ASC))[1]
        WHEN metric_name = 'CLOSE_PRICE' THEN (ARRAY_AGG(value ORDER BY pulled_at DESC))[1]
        WHEN metric_name LIKE '%_HIGH' THEN MAX(value)
        WHEN metric_name LIKE '%_LOW' THEN MIN(value)
        WHEN metric_name LIKE 'HIGH_%' THEN MAX(value)
        WHEN metric_name LIKE 'LOW_%' THEN MIN(value)
        WHEN metric_name LIKE '%OI_CLOSE' THEN (ARRAY_AGG(value ORDER BY pulled_at DESC))[1]
        WHEN metric_name LIKE '%OI_HIGH' THEN MAX(value)
        WHEN metric_name LIKE '%OI_LOW' THEN MIN(value)
        WHEN metric_name LIKE '%VOLUME%' THEN SUM(value)
        WHEN metric_name LIKE '%LIQ%' THEN SUM(value)
        WHEN metric_name LIKE '%TRADES%' THEN SUM(value)
        WHEN metric_name LIKE '%LIQUIDATIONS%' THEN SUM(value)
        WHEN metric_name IN ('FUNDING_RATE', 'FUNDING_RATE_AVG', 'PREMIUM') THEN AVG(value)
        ELSE (ARRAY_AGG(value ORDER BY pulled_at DESC))[1]
    END as value,
    COUNT(*) as hours_in_day
FROM metrics
WHERE source = 'velo' AND granularity = 'hourly'
GROUP BY DATE_TRUNC('day', pulled_at), source, asset, entity_id, metric_name, exchange, domain;

CREATE OR REPLACE VIEW v_metrics_daily AS
SELECT ts, source, asset, entity_id, metric_name, value, domain, exchange
FROM (
    SELECT 
        DATE_TRUNC('day', pulled_at) as ts,
        source, asset, entity_id, metric_name, value, domain, exchange
    FROM metrics 
    WHERE granularity = 'daily'
    
    UNION ALL
    
    SELECT ts, source, asset, entity_id, metric_name, value, domain, exchange
    FROM v_velo_daily
) combined;

CREATE OR REPLACE VIEW v_latest_metrics AS
SELECT DISTINCT ON (m.source, m.asset, m.metric_name, m.exchange)
    e.canonical_id,
    m.source,
    m.asset,
    m.metric_name,
    m.exchange,
    m.pulled_at as ts,
    m.value,
    m.domain
FROM metrics m
LEFT JOIN entity_source_ids esi ON m.source = esi.source AND m.asset = esi.source_id
LEFT JOIN entities e ON esi.entity_id = e.entity_id
ORDER BY m.source, m.asset, m.metric_name, m.exchange, m.pulled_at DESC;

-- =============================================================================
-- PART 8: METRIC NORMALIZATION VIEW
-- =============================================================================
-- Maps different metric names across sources to canonical names for cross-source queries
-- Velo CLOSE_PRICE -> PRICE (to match Artemis)
-- Handles granularity differences automatically via v_metrics_daily

CREATE OR REPLACE VIEW v_normalized_metrics AS
SELECT 
    ts,
    source,
    asset,
    entity_id,
    CASE 
        WHEN source = 'velo' AND metric_name = 'CLOSE_PRICE' THEN 'PRICE'
        WHEN source = 'velo' AND metric_name = 'DOLLAR_OI_CLOSE' THEN 'OPEN_INTEREST'
        WHEN source = 'velo' AND metric_name = 'DOLLAR_VOLUME' THEN 'TRADING_VOLUME_24H'
        WHEN source = 'velo' AND metric_name = 'LIQ_DOLLAR_VOL' THEN 'LIQUIDATIONS_24H'
        ELSE metric_name
    END as metric_name,
    value,
    domain,
    exchange
FROM v_metrics_daily;

-- =============================================================================
-- PART 9: CROSS-SOURCE COMPARISON VIEW
-- =============================================================================
-- Joins entities across sources for unified querying

CREATE OR REPLACE VIEW v_entity_comparison AS
SELECT 
    e.canonical_id,
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
FROM v_normalized_metrics m
JOIN entity_source_ids esi ON m.source = esi.source AND m.asset = esi.source_id
JOIN entities e ON esi.entity_id = e.entity_id;

COMMIT;

-- =============================================================================
-- VERIFICATION (run manually after migration)
-- =============================================================================
-- SELECT COUNT(*) FROM entities;
-- SELECT COUNT(*) FROM entity_source_ids;
-- SELECT column_name FROM information_schema.columns WHERE table_name = 'metrics';
-- \d+ metrics
