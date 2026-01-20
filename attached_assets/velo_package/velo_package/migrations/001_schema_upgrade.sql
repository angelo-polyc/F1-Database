-- =============================================================================
-- SCHEMA MIGRATION 001: Add Entity Master + Enhanced Metrics for Velo
-- =============================================================================
-- Run this ONCE before deploying Velo integration
-- Safe to run multiple times (uses IF NOT EXISTS)
-- =============================================================================

-- =============================================================================
-- PART 1: ENTITY MASTER TABLE
-- =============================================================================
-- Single source of truth for all tracked entities across all sources

CREATE TABLE IF NOT EXISTS entities (
    entity_id SERIAL PRIMARY KEY,
    
    -- Canonical identifier (your internal standard)
    canonical_id VARCHAR(100) NOT NULL UNIQUE,  -- e.g., 'solana', 'bitcoin', 'aave'
    
    -- Display info
    name VARCHAR(200) NOT NULL,                  -- e.g., 'Solana', 'Bitcoin', 'Aave'
    symbol VARCHAR(50),                          -- e.g., 'SOL', 'BTC', 'AAVE'
    
    -- Classification
    entity_type VARCHAR(50) NOT NULL DEFAULT 'token',  -- 'chain', 'protocol', 'token', 'equity'
    asset_class VARCHAR(50) NOT NULL DEFAULT 'crypto', -- 'crypto', 'equity', 'commodity'
    sector VARCHAR(100),                         -- 'l1', 'defi_lending', 'defi_dex', 'meme'
    
    -- Relationships
    parent_chain VARCHAR(100),                   -- For protocols: which chain?
    
    -- Metadata
    coingecko_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

-- Indexes for entities
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_symbol ON entities(symbol);
CREATE INDEX IF NOT EXISTS idx_entities_sector ON entities(sector);

-- =============================================================================
-- PART 2: SOURCE ID MAPPING TABLE (Rosetta Stone)
-- =============================================================================
-- Maps each source's ID format to canonical entity

CREATE TABLE IF NOT EXISTS entity_source_ids (
    id SERIAL PRIMARY KEY,
    entity_id INTEGER NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    source VARCHAR(100) NOT NULL,               -- 'artemis', 'defillama', 'velo'
    source_id VARCHAR(200) NOT NULL,            -- The ID that source uses
    source_id_type VARCHAR(50),                 -- 'ticker', 'slug', 'gecko_id', 'coin'
    
    UNIQUE(source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_source_ids_lookup ON entity_source_ids(source, source_id);
CREATE INDEX IF NOT EXISTS idx_source_ids_entity ON entity_source_ids(entity_id);

-- =============================================================================
-- PART 3: ENHANCE METRICS TABLE
-- =============================================================================
-- Add new columns for domain, exchange, and entity linking

-- Rename pulled_at to ts if it exists (optional - skip if you prefer pulled_at)
-- ALTER TABLE metrics RENAME COLUMN pulled_at TO ts;

-- Add domain column (fundamental, trading, derivative, options)
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS domain VARCHAR(50);

-- Add exchange column (for derivatives where exchange matters)
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS exchange VARCHAR(100);

-- Add entity_id link (nullable for gradual migration)
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS entity_id INTEGER REFERENCES entities(entity_id);

-- =============================================================================
-- PART 4: UPDATE UNIQUE CONSTRAINT
-- =============================================================================
-- The new constraint includes exchange (with COALESCE for NULL handling)
-- This allows: BTC + FUNDING_RATE + binance-futures AND BTC + FUNDING_RATE + bybit
-- as separate rows

-- Drop old constraint if exists (may fail if different name - that's OK)
DROP INDEX IF EXISTS idx_metrics_unique;

-- Create new constraint that handles exchange
CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_unique 
ON metrics (source, asset, metric_name, pulled_at, COALESCE(exchange, ''));

-- =============================================================================
-- PART 5: ADD TIMESTAMP INDEX
-- =============================================================================
-- Critical for hourly data queries - avoids mixing hourly and daily data

CREATE INDEX IF NOT EXISTS idx_metrics_ts ON metrics(pulled_at);
CREATE INDEX IF NOT EXISTS idx_metrics_domain ON metrics(domain, pulled_at);
CREATE INDEX IF NOT EXISTS idx_metrics_entity ON metrics(entity_id, metric_name, pulled_at);
CREATE INDEX IF NOT EXISTS idx_metrics_exchange ON metrics(exchange, pulled_at) WHERE exchange IS NOT NULL;

-- =============================================================================
-- PART 6: UPDATE PULLS TABLE (Optional enhancement)
-- =============================================================================

ALTER TABLE pulls ADD COLUMN IF NOT EXISTS notes TEXT;

-- =============================================================================
-- PART 7: USEFUL VIEWS
-- =============================================================================

-- Cross-source entity view: Get all data for an entity regardless of source
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

-- Derivatives summary view
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

-- Latest metrics per entity (useful for dashboards)
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
-- VERIFICATION QUERIES (run after migration)
-- =============================================================================
-- SELECT COUNT(*) FROM entities;
-- SELECT COUNT(*) FROM entity_source_ids;
-- SELECT column_name FROM information_schema.columns WHERE table_name = 'metrics';
-- \d+ metrics
