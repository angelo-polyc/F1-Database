# Data Pipeline Architecture

## Overview

This is a Python data pipeline for recurring cryptocurrency and equity data API pulls with PostgreSQL storage. The system pulls data from multiple API sources, stores it in a standardized time-series format, and runs automated scheduled pulls.

## Core Design Principles

1. **Only pull direct API data** - No client-side calculations or aggregations
2. **Modular sources** - Each API source is a self-contained class
3. **Standardized storage** - All data goes into a single `metrics` table with consistent schema
4. **Idempotent operations** - Backfills and live pulls handle duplicates gracefully
5. **Configuration-driven** - CSV files define what assets/metrics to pull

## Directory Structure

```
.
├── main.py                  # CLI entry point
├── scheduler.py             # Automated pull scheduler
├── backfill_<source>.py     # Historical data backfill scripts
├── query_data.py            # Query helper functions
├── <source>_config.csv      # Configuration files
├── db/
│   ├── __init__.py
│   └── setup.py             # Database connection and schema
└── sources/
    ├── __init__.py          # Source registry
    ├── base.py              # BaseSource abstract class
    ├── artemis.py           # Artemis implementation
    └── defillama.py         # DefiLlama implementation
```

## Database Schema

### Tables

```sql
-- Logs each API pull operation
CREATE TABLE pulls (
    pull_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    pulled_at TIMESTAMP NOT NULL DEFAULT NOW(),
    status VARCHAR(50) NOT NULL,        -- 'success', 'error', 'no_data'
    records_count INTEGER DEFAULT 0
);

-- Time series data storage
CREATE TABLE metrics (
    id SERIAL PRIMARY KEY,
    pulled_at TIMESTAMP NOT NULL,       -- Date of the data point
    source VARCHAR(100) NOT NULL,       -- e.g., 'artemis', 'defillama'
    asset VARCHAR(100) NOT NULL,        -- Entity identifier
    metric_name VARCHAR(200) NOT NULL,  -- e.g., 'PRICE', 'TVL', 'FEES'
    value DOUBLE PRECISION
);

-- Unique constraint for deduplication
CREATE UNIQUE INDEX idx_metrics_unique 
ON metrics (source, asset, metric_name, pulled_at);
```

### Key Concepts

- **source**: Identifies which API the data came from
- **asset**: Entity identifier (may differ between sources - e.g., Artemis uses `sol`, DefiLlama uses `solana`)
- **metric_name**: Standardized uppercase metric name
- **pulled_at**: Truncated to midnight UTC for daily data

## Data Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Config CSV     │────▶│  Source Class   │────▶│  PostgreSQL     │
│  (assets/       │     │  (fetch & parse)│     │  (metrics table)│
│   metrics)      │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Live Pulls (Scheduler)

1. Read config CSV to get asset/metric pairs
2. Make API calls to fetch current values
3. Truncate timestamp to midnight UTC
4. Insert with `ON CONFLICT DO UPDATE` (upsert today's values)

### Backfills

1. Read config CSV to get entities
2. Make API calls to historical endpoints
3. Keep original API timestamps
4. Insert with `ON CONFLICT DO NOTHING` (skip duplicates)

## Duplicate Prevention Strategy

| Operation | Timestamp | Conflict Handling | Purpose |
|-----------|-----------|-------------------|---------|
| Live pull | Truncated to midnight | `DO UPDATE` | Update today's value if re-pulled |
| Backfill | Original from API | `DO NOTHING` | Preserve historical data |

## Scheduler Behavior

The scheduler (`scheduler.py`) runs continuously:

1. On startup with `--fresh`: Clears all data, runs full backfills
2. On startup without `--fresh`: Runs backfills (idempotent), then initial pulls
3. Continuous loop: Checks every 30 seconds for scheduled pull times
   - Artemis: Daily at 00:05 UTC
   - DefiLlama: Hourly at XX:05 UTC

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| DATABASE_URL | Yes | PostgreSQL connection string |
| ARTEMIS_API_KEY | For Artemis | API key for Artemis |
| DEFILLAMA_API_KEY | For DefiLlama Pro | API key for Pro endpoints |

## Rate Limiting

Each source has a `REQUEST_DELAY` constant:
- DefiLlama: 0.08s (~12 req/sec, limit is 16.6/sec for Pro)
- Artemis: 0.12s (~8 req/sec)

Implement exponential backoff on 429 responses.
