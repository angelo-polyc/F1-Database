# CoinGecko Data Pipeline - LLM Handover Document

## Overview

This document provides comprehensive instructions for managing the CoinGecko cryptocurrency price and volume data pipeline. The pipeline pulls hourly data from CoinGecko's API and stores it in PostgreSQL.

---

## System Architecture

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│   CoinGecko     │─────▶│  coingecko_pull  │─────▶│   PostgreSQL    │
│   API           │      │  .py             │      │   Database      │
└─────────────────┘      └──────────────────┘      └─────────────────┘
                                │
                                ▼
                         ┌──────────────────┐
                         │ coingecko_config │
                         │ .csv             │
                         └──────────────────┘
```

---

## File Locations

| File | Path | Description |
|------|------|-------------|
| Main Script | `/path/to/coingecko_pull.py` | Primary data pull script |
| Config File | `/path/to/coingecko_config.csv` | Symbol → CoinGecko ID mappings |
| ID Cache | `/path/to/coingecko_id_cache.json` | Cached API ID lookups |
| Logs | `/path/to/logs/coingecko_YYYYMMDD.log` | Daily log files |

---

## Database Schema

### Table: `coingecko_hourly`

Primary data table storing hourly price and volume snapshots.

```sql
CREATE TABLE coingecko_hourly (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,           -- Hour-aligned UTC timestamp
    symbol VARCHAR(50) NOT NULL,              -- Ticker symbol (e.g., 'BTC')
    coingecko_id VARCHAR(100) NOT NULL,       -- CoinGecko API ID (e.g., 'bitcoin')
    name VARCHAR(200),                        -- Full coin name
    price_usd NUMERIC(28, 12),                -- Current price in USD
    volume_24h_usd NUMERIC(28, 2),            -- 24-hour trading volume
    market_cap_usd NUMERIC(28, 2),            -- Market capitalization
    market_cap_rank INTEGER,                  -- Rank by market cap
    high_24h NUMERIC(28, 12),                 -- 24h high price
    low_24h NUMERIC(28, 12),                  -- 24h low price
    price_change_24h NUMERIC(28, 12),         -- Absolute price change (24h)
    price_change_pct_1h NUMERIC(12, 6),       -- % change (1h)
    price_change_pct_24h NUMERIC(12, 6),      -- % change (24h)
    price_change_pct_7d NUMERIC(12, 6),       -- % change (7d)
    circulating_supply NUMERIC(28, 4),
    total_supply NUMERIC(28, 4),
    max_supply NUMERIC(28, 4),
    ath_usd NUMERIC(28, 12),                  -- All-time high price
    ath_date TIMESTAMPTZ,
    atl_usd NUMERIC(28, 12),                  -- All-time low price
    atl_date TIMESTAMPTZ,
    last_updated TIMESTAMPTZ,                 -- CoinGecko's last update time
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(timestamp, coingecko_id)
);
```

**Indexes:**
- `idx_coingecko_hourly_symbol_ts` - For symbol-based time series queries
- `idx_coingecko_hourly_cgid_ts` - For CoinGecko ID lookups
- `idx_coingecko_hourly_ts` - For time-range queries

### Table: `coingecko_pull_log`

Metadata table tracking each data pull execution.

```sql
CREATE TABLE coingecko_pull_log (
    id SERIAL PRIMARY KEY,
    pull_timestamp TIMESTAMPTZ NOT NULL,
    symbols_requested INTEGER,
    symbols_retrieved INTEGER,
    symbols_failed TEXT[],
    duration_seconds NUMERIC(10, 2),
    api_calls INTEGER,
    status VARCHAR(50),                       -- SUCCESS, PARTIAL, FAILED
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

---

## Common Queries

### Get Latest Prices for All Symbols

```sql
SELECT DISTINCT ON (symbol) 
    symbol, 
    price_usd, 
    volume_24h_usd, 
    market_cap_usd,
    price_change_pct_24h,
    timestamp
FROM coingecko_hourly
ORDER BY symbol, timestamp DESC;
```

### Get Price History for a Symbol

```sql
SELECT 
    timestamp,
    price_usd,
    volume_24h_usd,
    market_cap_usd
FROM coingecko_hourly
WHERE symbol = 'BTC'
  AND timestamp >= NOW() - INTERVAL '7 days'
ORDER BY timestamp ASC;
```

### Get Hourly Price for Specific Date Range

```sql
SELECT 
    timestamp,
    symbol,
    price_usd,
    volume_24h_usd
FROM coingecko_hourly
WHERE symbol IN ('BTC', 'ETH', 'SOL')
  AND timestamp BETWEEN '2026-01-01' AND '2026-01-15'
ORDER BY symbol, timestamp;
```

### Calculate Daily OHLCV from Hourly Data

```sql
SELECT 
    DATE(timestamp) AS date,
    symbol,
    (ARRAY_AGG(price_usd ORDER BY timestamp ASC))[1] AS open,
    MAX(price_usd) AS high,
    MIN(price_usd) AS low,
    (ARRAY_AGG(price_usd ORDER BY timestamp DESC))[1] AS close,
    AVG(volume_24h_usd) AS avg_volume_24h
FROM coingecko_hourly
WHERE symbol = 'BTC'
  AND timestamp >= NOW() - INTERVAL '30 days'
GROUP BY DATE(timestamp), symbol
ORDER BY date;
```

### Check Data Freshness

```sql
SELECT 
    symbol,
    MAX(timestamp) AS latest_data,
    COUNT(*) AS total_records,
    NOW() - MAX(timestamp) AS data_age
FROM coingecko_hourly
GROUP BY symbol
ORDER BY latest_data DESC;
```

### Monitor Pull Health

```sql
SELECT 
    pull_timestamp,
    symbols_requested,
    symbols_retrieved,
    duration_seconds,
    status,
    CARDINALITY(symbols_failed) AS failed_count
FROM coingecko_pull_log
ORDER BY pull_timestamp DESC
LIMIT 24;
```

### Find Missing Hours

```sql
WITH expected_hours AS (
    SELECT generate_series(
        DATE_TRUNC('hour', NOW() - INTERVAL '24 hours'),
        DATE_TRUNC('hour', NOW()),
        '1 hour'::interval
    ) AS hour
),
actual_hours AS (
    SELECT DISTINCT timestamp AS hour
    FROM coingecko_hourly
    WHERE timestamp >= NOW() - INTERVAL '24 hours'
)
SELECT e.hour AS missing_hour
FROM expected_hours e
LEFT JOIN actual_hours a ON e.hour = a.hour
WHERE a.hour IS NULL
ORDER BY e.hour;
```

---

## Configuration Management

### Config File Format (`coingecko_config.csv`)

```csv
symbol,coingecko_id,name,enabled,notes
BTC,bitcoin,Bitcoin,true,
ETH,ethereum,Ethereum,true,
LUNA,terra-luna-2,Terra Luna 2.0,true,AMBIGUOUS - VERIFY
```

**Fields:**
- `symbol` - Your internal ticker symbol (uppercase)
- `coingecko_id` - CoinGecko's unique identifier (critical!)
- `name` - Human-readable name
- `enabled` - `true` or `false` to include/exclude
- `notes` - Any notes, especially for ambiguous symbols

### Handling Ambiguous Symbols

Many ticker symbols map to multiple coins. For example:
- `LUNA` → Could be Terra Luna Classic, Terra Luna 2.0, etc.
- `HONEY` → Multiple tokens use this symbol

**To resolve:**
1. Check the `notes` column for entries marked `AMBIGUOUS - VERIFY`
2. Look up the correct CoinGecko ID at: https://www.coingecko.com/
3. The API ID is shown on each coin's page under "Info"
4. Update the `coingecko_id` in the config file

---

## Environment Variables

```bash
# CoinGecko API Key (Pro API for higher rate limits)
export COINGECKO_API_KEY="your-api-key-here"

# PostgreSQL Connection
export PGHOST="localhost"
export PGPORT="5432"
export PGDATABASE="crypto_analytics"
export PGUSER="postgres"
export PGPASSWORD="your-password"
```

---

## Operational Commands

### Initial Setup

```bash
# 1. Install dependencies
pip install requests psycopg2-binary --break-system-packages

# 2. Set environment variables
export COINGECKO_API_KEY="your-key"
export PGDATABASE="crypto_analytics"
# ... (other DB vars)

# 3. Generate initial config with ID mappings
python coingecko_pull.py --refresh-ids

# 4. Review and edit the config file for ambiguous symbols
nano coingecko_config.csv

# 5. Test run (no DB write)
python coingecko_pull.py --dry-run

# 6. Full run
python coingecko_pull.py
```

### Schedule Hourly Runs (cron)

```bash
# Edit crontab
crontab -e

# Add this line (runs at minute 5 of every hour)
5 * * * * cd /path/to/scripts && /usr/bin/python3 coingecko_pull.py >> logs/cron.log 2>&1
```

### Manual Operations

```bash
# Standard data pull
python coingecko_pull.py

# Refresh ID mappings (after adding new symbols)
python coingecko_pull.py --refresh-ids

# Test without database write
python coingecko_pull.py --dry-run

# Override symbols list
python coingecko_pull.py --refresh-ids --symbols "BTC,ETH,SOL"
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Missing data for symbol | Wrong CoinGecko ID | Verify ID in config, refresh with `--refresh-ids` |
| Rate limit errors (429) | Too many API calls | Script auto-retries; consider Pro API |
| DB connection failed | Wrong credentials | Check `PGHOST`, `PGUSER`, `PGPASSWORD` |
| Stale data | Cron not running | Check `crontab -l`, verify script permissions |
| Null prices | Coin delisted or no data | CoinGecko may not have data for this coin |

### Checking Logs

```bash
# View today's log
cat logs/coingecko_$(date +%Y%m%d).log

# Tail live logs during a run
tail -f logs/coingecko_$(date +%Y%m%d).log

# Search for errors
grep -i error logs/coingecko_*.log
```

### Validating Data Quality

```sql
-- Check for null prices (might indicate API issues)
SELECT symbol, COUNT(*) AS null_price_count
FROM coingecko_hourly
WHERE price_usd IS NULL
GROUP BY symbol
ORDER BY null_price_count DESC;

-- Check for duplicate timestamps
SELECT timestamp, coingecko_id, COUNT(*)
FROM coingecko_hourly
GROUP BY timestamp, coingecko_id
HAVING COUNT(*) > 1;
```

---

## Adding New Symbols

1. **Add to DEFAULT_SYMBOLS list in script** (or specify via `--symbols`)

2. **Refresh ID mappings:**
   ```bash
   python coingecko_pull.py --refresh-ids
   ```

3. **Review generated config file** - check for `AMBIGUOUS` entries

4. **Verify the CoinGecko ID** at https://www.coingecko.com/

5. **Next hourly run will include the new symbol**

---

## Removing Symbols

1. Edit `coingecko_config.csv`
2. Set `enabled` to `false` for the symbol
3. **OR** delete the row entirely
4. Historical data remains in the database

---

## API Rate Limits

| API Tier | Rate Limit | Recommended Delay |
|----------|------------|-------------------|
| Demo (Free) | 30 calls/min | 2.1 seconds |
| Analyst | 500 calls/min | 0.15 seconds |
| Lite | 500 calls/min | 0.15 seconds |
| Pro | 1000 calls/min | 0.07 seconds |

The script auto-detects Pro API when `COINGECKO_API_KEY` is set.

---

## Data Retention

Consider adding a data retention policy for large datasets:

```sql
-- Delete data older than 1 year
DELETE FROM coingecko_hourly
WHERE timestamp < NOW() - INTERVAL '1 year';

-- Or create a monthly archive job
CREATE TABLE coingecko_hourly_archive_2025 AS
SELECT * FROM coingecko_hourly
WHERE timestamp >= '2025-01-01' AND timestamp < '2026-01-01';
```

---

## Integration with Existing Infrastructure

### Joining with Artemis Data

```sql
-- Join CoinGecko price data with Artemis metrics
SELECT 
    cg.timestamp,
    cg.symbol,
    cg.price_usd,
    cg.volume_24h_usd,
    a.dau,
    a.tvl
FROM coingecko_hourly cg
JOIN artemis_daily a 
    ON cg.symbol = a.symbol 
    AND DATE(cg.timestamp) = a.date
WHERE cg.symbol = 'ETH'
ORDER BY cg.timestamp;
```

### Exporting to CSV

```sql
COPY (
    SELECT timestamp, symbol, price_usd, volume_24h_usd, market_cap_usd
    FROM coingecko_hourly
    WHERE timestamp >= '2026-01-01'
    ORDER BY timestamp, symbol
) TO '/tmp/coingecko_export.csv' WITH CSV HEADER;
```

---

## Contact & Resources

- **CoinGecko API Docs:** https://docs.coingecko.com/
- **CoinGecko Status:** https://status.coingecko.com/
- **Rate Limit Info:** https://www.coingecko.com/en/api/pricing

---

## Changelog

| Date | Change |
|------|--------|
| 2026-01-25 | Initial pipeline creation |

---

*Document generated for Angelo's crypto analytics infrastructure.*
