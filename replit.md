# Cryptocurrency & Equity Data Pipeline

## Overview
A Python data pipeline system for recurring API pulls of cryptocurrency and equity data with PostgreSQL storage. The system is designed to be modular and extensible for adding multiple data sources.

## Project Structure
```
.
├── main.py                  # CLI entry point
├── scheduler.py             # Automated pull scheduler (1h DefiLlama, 24h Artemis)
├── backfill_defillama.py   # Historical data backfill script
├── query_data.py           # Query helper functions with CSV export
├── artemis_config.csv      # Configuration for Artemis API pulls (matrix format)
├── defillama_config.csv    # Configuration for DefiLlama API pulls
├── db/
│   ├── __init__.py
│   └── setup.py            # Database connection and schema setup
└── sources/
    ├── __init__.py         # Source registry
    ├── base.py             # BaseSource abstract class
    ├── artemis.py          # Artemis API source implementation
    └── defillama.py        # DefiLlama API source implementation
```

## Database Schema
- **pulls**: Logs each API pull (pull_id, source_name, pulled_at, status, records_count)
- **metrics**: Time series data (id, pulled_at, source, asset, metric_name, value)
- Unique index on (source, asset, metric_name, pulled_at) for fast queries and deduplication

## Usage

### Setup Database
```bash
python main.py setup
```

### Pull Data
```bash
python main.py pull artemis     # Pull from Artemis
python main.py pull defillama   # Pull from DefiLlama
```

### Query Data (Interactive)
```bash
python main.py query
```

### List Available Sources
```bash
python main.py sources
```

## Data Sources

### Artemis
- **Assets:** 287 crypto assets and equities
- **Metrics:** 25 (PRICE, MC, FEES, REVENUE, DAU, TXNS, etc.)
- **ID Format:** Short IDs (e.g., `sol`, `eth`, `aave`)
- **Requires:** ARTEMIS_API_KEY secret

### DefiLlama
- **Assets:** 323 protocols and chains
- **Metrics:** 42 (TVL, CHAIN_TVL, DEX_VOLUME_24H, FEES_24H, etc.)
- **ID Format:** CoinGecko IDs (e.g., `solana`, `ethereum`, `aave`)
- **Requires:** No API key (free API)

## Adding New Sources
1. Create a new file in `sources/` (e.g., `sources/newsource.py`)
2. Extend `BaseSource` class and implement:
   - `source_name` property
   - `pull()` method
3. Register in `sources/__init__.py` by adding to the `SOURCES` dict

## Environment Variables
- `DATABASE_URL`: PostgreSQL connection string (auto-configured)
- `ARTEMIS_API_KEY`: API key for Artemis data (required for artemis pulls)

## Entity ID Mapping
Artemis and DefiLlama use different ID systems:
- Artemis: `sol` → DefiLlama: `solana`
- Artemis: `eth` → DefiLlama: `ethereum`

Cross-source queries require joining on the `source` column or using entity mapping.

## Automated Scheduling
The `scheduler.py` runs continuously and executes pulls at different intervals:
- **DefiLlama:** Every 1 hour
- **Artemis:** Every 24 hours

Both pulls run immediately on startup, then follow their respective intervals.

Run manually: `python scheduler.py`

## Historical Backfill
The `backfill_defillama.py` script fetches historical time series data from DefiLlama summary endpoints:
- **Endpoints:** fees, revenue, dexs, derivatives, aggregators, tvl (protocol)
- **Data:** Daily time series going back to 2011 for some assets
- **Idempotent:** Uses ON CONFLICT DO NOTHING to prevent duplicates

Run backfill: `python backfill_defillama.py`

### Historical Data Coverage (as of Jan 2026)
- **Total Records:** ~500,000+ across 323 assets
- **TVL:** 228k records from 2019
- **FEES:** 139k records from 2011
- **REVENUE:** 88k records from 2020
- **DEX_VOLUME:** 30k records from 2020
- **DERIVATIVES_VOLUME:** 7k records from 2021
