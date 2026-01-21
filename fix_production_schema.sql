-- Run this SQL in your Production database panel to fix missing schema

-- Add missing columns to metrics table
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS domain VARCHAR(50);
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS exchange VARCHAR(100);
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS entity_id INTEGER;
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS granularity VARCHAR(20);

-- Add notes column to pulls table
ALTER TABLE pulls ADD COLUMN IF NOT EXISTS notes TEXT;

-- Create entity tables
CREATE TABLE IF NOT EXISTS entities (
    entity_id SERIAL PRIMARY KEY,
    canonical_id VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(200),
    symbol VARCHAR(50),
    entity_type VARCHAR(50),
    sector VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS entity_source_ids (
    id SERIAL PRIMARY KEY,
    entity_id INTEGER REFERENCES entities(entity_id),
    source VARCHAR(50) NOT NULL,
    source_id VARCHAR(100) NOT NULL,
    UNIQUE(source, source_id)
);

-- Create missing indexes
CREATE INDEX IF NOT EXISTS idx_metrics_lookup ON metrics (source, asset, metric_name, pulled_at);
CREATE INDEX IF NOT EXISTS idx_metrics_ts ON metrics (pulled_at);
CREATE INDEX IF NOT EXISTS idx_metrics_domain ON metrics (domain, pulled_at);
CREATE INDEX IF NOT EXISTS idx_metrics_entity ON metrics (entity_id, metric_name, pulled_at);
CREATE INDEX IF NOT EXISTS idx_metrics_exchange ON metrics (exchange, pulled_at) WHERE exchange IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_metrics_granularity ON metrics (granularity, pulled_at);
CREATE INDEX IF NOT EXISTS idx_entity_source ON entity_source_ids (source, source_id);
