-- Views must be created in dependency order

-- v_velo_daily: Aggregates hourly Velo data to daily
CREATE OR REPLACE VIEW v_velo_daily AS
SELECT 
    date_trunc('day', pulled_at) AS ts,
    source, asset, entity_id, metric_name, exchange, domain,
    CASE
        WHEN metric_name = 'OPEN_PRICE' THEN (array_agg(value ORDER BY pulled_at))[1]
        WHEN metric_name = 'CLOSE_PRICE' THEN (array_agg(value ORDER BY pulled_at DESC))[1]
        WHEN metric_name LIKE '%_HIGH' THEN max(value)
        WHEN metric_name LIKE '%_LOW' THEN min(value)
        WHEN metric_name LIKE 'HIGH_%' THEN max(value)
        WHEN metric_name LIKE 'LOW_%' THEN min(value)
        WHEN metric_name LIKE '%OI_CLOSE' THEN (array_agg(value ORDER BY pulled_at DESC))[1]
        WHEN metric_name LIKE '%OI_HIGH' THEN max(value)
        WHEN metric_name LIKE '%OI_LOW' THEN min(value)
        WHEN metric_name LIKE '%VOLUME%' THEN sum(value)
        WHEN metric_name LIKE '%LIQ%' THEN sum(value)
        WHEN metric_name LIKE '%TRADES%' THEN sum(value)
        WHEN metric_name LIKE '%LIQUIDATIONS%' THEN sum(value)
        WHEN metric_name IN ('FUNDING_RATE', 'FUNDING_RATE_AVG', 'PREMIUM') THEN avg(value)
        ELSE (array_agg(value ORDER BY pulled_at DESC))[1]
    END AS value,
    count(*) AS hours_in_day
FROM metrics
WHERE source = 'velo' AND granularity = 'hourly'
GROUP BY date_trunc('day', pulled_at), source, asset, entity_id, metric_name, exchange, domain;

-- v_metrics_daily: Combines daily metrics with aggregated Velo
CREATE OR REPLACE VIEW v_metrics_daily AS
SELECT ts, source, asset, entity_id, metric_name, value, domain, exchange
FROM (
    SELECT date_trunc('day', pulled_at) AS ts, source, asset, entity_id, metric_name, value, domain, exchange
    FROM metrics WHERE granularity = 'daily'
    UNION ALL
    SELECT ts, source, asset, entity_id, metric_name, value, domain, exchange
    FROM v_velo_daily
) combined;

-- v_normalized_metrics: Standardizes metric names across sources
CREATE OR REPLACE VIEW v_normalized_metrics AS
SELECT 
    ts, source, asset, entity_id,
    CASE
        WHEN source = 'velo' AND metric_name = 'CLOSE_PRICE' THEN 'PRICE'
        WHEN source = 'velo' AND metric_name = 'DOLLAR_OI_CLOSE' THEN 'OPEN_INTEREST'
        WHEN source = 'velo' AND metric_name = 'DOLLAR_VOLUME' THEN 'TRADING_VOLUME_24H'
        WHEN source = 'velo' AND metric_name = 'LIQ_DOLLAR_VOL' THEN 'LIQUIDATIONS_24H'
        ELSE metric_name
    END AS metric_name,
    value, domain, exchange
FROM v_metrics_daily;

-- v_entity_metrics: Joins metrics with entity master
CREATE OR REPLACE VIEW v_entity_metrics AS
SELECT 
    e.canonical_id, e.name, e.symbol, e.entity_type, e.sector,
    m.pulled_at AS ts, m.source, m.metric_name, m.value, m.domain, m.exchange
FROM metrics m
JOIN entity_source_ids esi ON m.source = esi.source AND m.asset = esi.source_id
JOIN entities e ON esi.entity_id = e.entity_id;

-- v_entity_comparison: Cross-source comparison using normalized metrics
CREATE OR REPLACE VIEW v_entity_comparison AS
SELECT 
    e.canonical_id, e.name, e.symbol, e.entity_type, e.sector,
    m.ts, m.source, m.metric_name, m.value, m.domain, m.exchange
FROM v_normalized_metrics m
JOIN entity_source_ids esi ON m.source = esi.source AND m.asset = esi.source_id
JOIN entities e ON esi.entity_id = e.entity_id;

-- v_latest_metrics: Most recent value per metric
CREATE OR REPLACE VIEW v_latest_metrics AS
SELECT DISTINCT ON (m.source, m.asset, m.metric_name, m.exchange)
    e.canonical_id, m.source, m.asset, m.metric_name, m.exchange,
    m.pulled_at AS ts, m.value, m.domain
FROM metrics m
LEFT JOIN entity_source_ids esi ON m.source = esi.source AND m.asset = esi.source_id
LEFT JOIN entities e ON esi.entity_id = e.entity_id
ORDER BY m.source, m.asset, m.metric_name, m.exchange, m.pulled_at DESC;

-- v_derivatives_summary: Pivoted derivatives data
CREATE OR REPLACE VIEW v_derivatives_summary AS
SELECT 
    e.canonical_id, e.symbol, m.exchange,
    date_trunc('hour', m.pulled_at) AS ts,
    max(CASE WHEN m.metric_name = 'FUNDING_RATE' THEN m.value END) AS funding_rate,
    max(CASE WHEN m.metric_name = 'DOLLAR_OI_CLOSE' THEN m.value END) AS open_interest_usd,
    max(CASE WHEN m.metric_name = 'LIQ_DOLLAR_VOL' THEN m.value END) AS liquidations_usd
FROM metrics m
JOIN entity_source_ids esi ON m.source = esi.source AND m.asset = esi.source_id
JOIN entities e ON esi.entity_id = e.entity_id
WHERE m.domain = 'derivative'
GROUP BY e.canonical_id, e.symbol, m.exchange, date_trunc('hour', m.pulled_at);
