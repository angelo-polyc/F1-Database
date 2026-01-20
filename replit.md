# Cryptocurrency & Equity Data Pipeline

## User Preferences
- **Do NOT restart workflows automatically** - Only restart when explicitly instructed

## Overview
A Python data pipeline system for recurring API pulls of cryptocurrency and equity data with PostgreSQL storage. The system is designed to be modular and extensible for adding multiple data sources.

## Project Structure
```
.
├── main.py                  # CLI entry point
├── scheduler.py             # Automated pull scheduler (1h DefiLlama, 24h Artemis)
├── backfill_defillama.py   # DefiLlama historical data backfill
├── backfill_artemis.py     # Artemis historical data backfill
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

## Duplicate Prevention
- **Backfills**: Use original API timestamps with ON CONFLICT DO NOTHING (skip exact duplicates)
- **Live pulls**: Truncate timestamp to midnight UTC with ON CONFLICT DO UPDATE (upsert today's values)
- This ensures historical data integrity while preventing scheduler duplicates

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
- **Metrics:** PRICE, MC, FEES, REVENUE, DAU, TXNS, 24H_VOLUME, AVG_TXN_FEE, PERP_FEES, PERP_TXNS, LENDING_DEPOSITS, LENDING_BORROWS, SPOT_VOLUME, etc.
- **Excluded Metrics:** VOLATILITY_90D_ANN, STABLECOIN_AVG_DAU, TOKENIZED_SHARES_TRADING_VOLUME, FDMV_NAV_RATIO
- **ID Format:** Short IDs (e.g., `sol`, `eth`, `aave`)
- **Requires:** ARTEMIS_API_KEY secret

### DefiLlama
- **Assets:** 323+ entities (96 chains, 227 protocols) - auto-categorized via /chains API
- **Chain Metrics:** CHAIN_TVL, CHAIN_FEES_24H, CHAIN_REVENUE_24H, CHAIN_DEX_VOLUME_24H, CHAIN_PERPS_VOLUME_24H, CHAIN_OPTIONS_VOLUME_24H
- **Protocol Metrics:** TVL, FEES_24H, REVENUE_24H, DEX_VOLUME_24H, DERIVATIVES_VOLUME_24H, EARNINGS, INFLOW, OUTFLOW
- **Stablecoin Metrics:** STABLECOIN_SUPPLY (circulating) - uses dedicated stablecoins.llama.fi endpoint for 13 stablecoins (dai, usds, ethena-usde, etc.)
- **Bridge Metrics:** BRIDGE_VOLUME_24H, BRIDGE_VOLUME_7D, BRIDGE_VOLUME_30D - uses bridges.llama.fi for 10+ bridge protocols
- **Pro API Metrics:** INFLOW, OUTFLOW, OPEN_INTEREST (require DEFILLAMA_API_KEY)
- **Removed:** PRICE, MCAP, FDV (use CoinGecko integration), dailyTokenIncentives (unstable 500 errors)
- **Chain vs Protocol:** Auto-detected using DefiLlama /chains API (599 chains) + EXTRA_CHAINS override (ronin, stride, babylon)
- **Slug Resolution:** Chain fetchers use slug > name > gecko_id fallback order to handle entities with blank names
- **ID Format:** CoinGecko IDs (e.g., `solana`, `ethereum`, `aave`)
- **Requires:** DEFILLAMA_API_KEY secret (Pro API)

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
The `scheduler.py` runs continuously and executes pulls at specific times (5 min after API updates):
- **DefiLlama:** Hourly at XX:05 UTC (APIs update on the hour)
- **Artemis:** Daily at 00:05 UTC (APIs update at midnight UTC)

Both pulls run immediately on startup, then follow their scheduled times.

Run manually: `python scheduler.py`
Fresh start (clears all data first): `python scheduler.py --fresh`

## Performance Optimizations
- **Parallel API requests:** Uses ThreadPoolExecutor with 10 workers for concurrent fetches
- **Connection pooling:** HTTP sessions with HTTPAdapter for connection reuse
- **Thread-safe sessions:** Each worker thread gets its own requests.Session via threading.local()
- **Bulk fetching:** All lookup endpoints fetched in single parallel batch (~11 URLs in ~2s vs ~9s sequential)
- **Pre-built entity URLs:** Per-entity API calls gathered upfront and fetched in single parallel batch
- **Unified rate limiting:** All HTTP calls (fetch_json, fetch_json_threadsafe, fetch_pro_json) go through a global token bucket limiter
- **Rate limit guarantee:** Thread-safe lock ensures max 15 req/sec (900/min), well under the 1000/min API limit

## Historical Backfill

### DefiLlama Backfill
The `backfill_defillama.py` script fetches historical time series data from DefiLlama summary endpoints:
- **Chain Endpoints:** historicalChainTvl, overview/fees/{chain}, overview/fees/{chain}?dataType=dailyRevenue, overview/dexs/{chain}, overview/derivatives/{chain}, overview/options/{chain}
- **Protocol Endpoints:** fees, revenue, dexs, derivatives, aggregators, tvl
- **Stablecoin Endpoints:** stablecoins.llama.fi/stablecoincharts for 13 stablecoins (dai, usds, ethena-usde, etc.)
- **Bridge Endpoints:** bridges.llama.fi/bridgevolume for 10+ bridge protocols
- **Note:** Inflows/outflows are NOT fetched during backfill (only 30 days available via Pro API) - collected by scheduled pulls instead
- **Data:** Daily time series going back to 2011 for some assets
- **Idempotent:** Uses ON CONFLICT DO NOTHING to prevent duplicates

Run: `python backfill_defillama.py`

### Artemis Backfill
The `backfill_artemis.py` script fetches 5 years of historical data from Artemis API:
- **Method:** Uses startDate/endDate parameters with symbol batching
- **Data:** Daily time series going back to 2021
- **Idempotent:** Uses ON CONFLICT DO NOTHING to prevent duplicates

Run: `python backfill_artemis.py`

### Historical Data Coverage (as of Jan 2026)

**DefiLlama:** ~500K records across 323 assets
- TVL: 228k records from 2019
- FEES: 139k records from 2011
- REVENUE: 88k records from 2020
- DEX_VOLUME: 30k records from 2020

**Artemis:** ~1.87M records across 287 assets
- PRICE: 255k records from 2021
- CIRCULATING_SUPPLY: 192k records from 2021
- FEES: 154k records from 2021
- MC: 153k records from 2021
- REVENUE: 145k records from 2021
- DAU: 144k records from 2021
