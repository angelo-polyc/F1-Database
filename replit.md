# Cryptocurrency & Equity Data Pipeline

## User Preferences
- **Do NOT restart workflows automatically** - Only restart when explicitly instructed

## Overview
A Python data pipeline system for recurring API pulls of cryptocurrency and equity data with PostgreSQL storage. The system is designed to be modular and extensible for adding multiple data sources.

## Project Structure
```
.
├── main.py                  # CLI entry point
├── scheduler.py             # Automated pull scheduler (1h DefiLlama/Velo, 24h Artemis)
├── backfill_defillama.py   # DefiLlama historical data backfill
├── backfill_artemis.py     # Artemis historical data backfill
├── backfill_velo.py        # Velo historical data backfill
├── query_data.py           # Query helper functions with CSV export
├── artemis_config.csv      # Configuration for Artemis API pulls (matrix format)
├── defillama_config.csv    # Configuration for DefiLlama API pulls
├── velo_config.csv         # Velo config - full (1069 pairs)
├── velo_config_top20.csv   # Velo config - top 20 coins (~71 pairs)
├── velo_config_top50.csv   # Velo config - top 50 coins (~164 pairs)
├── velo_config_top100.csv  # Velo config - top 100 coins (~258 pairs)
├── migrations/
│   ├── 001_schema_upgrade.sql    # Entity master + enhanced metrics schema
│   ├── 002_entity_seed_data.sql  # Entity seed with Velo coins + initial mappings
│   ├── 003_complete_entity_mappings.sql  # Complete Artemis/DefiLlama mappings
│   └── 004_entity_corrections.sql  # User corrections: merge duplicates, add mappings
├── db/
│   ├── __init__.py
│   └── setup.py            # Database connection and schema setup
└── sources/
    ├── __init__.py         # Source registry
    ├── base.py             # BaseSource abstract class
    ├── artemis.py          # Artemis API source implementation
    ├── defillama.py        # DefiLlama API source implementation
    └── velo.py             # Velo.xyz API source implementation
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

### Velo.xyz
- **Assets:** 481 coins with derivatives data across 4 exchanges
- **Exchanges:** Binance Futures, Bybit, OKX Swap, Hyperliquid
- **Metrics:** CLOSE_PRICE, DOLLAR_VOLUME, DOLLAR_OI_CLOSE, FUNDING_RATE_AVG, LIQ_DOLLAR_VOL (5 essential metrics)
- **Granularity:** Hourly (vs daily for Artemis/DefiLlama)
- **Config Options:**
  - `velo_config.csv` - Full 1,069 pairs (23M rows/month)
  - `velo_config_top100.csv` - Top 100 coins (~258 pairs, 6.5M rows/month)
  - `velo_config_top50.csv` - Top 50 coins (~164 pairs, 3.2M rows/month)
  - `velo_config_top20.csv` - Top 20 coins (~71 pairs, 1.4M rows/month)
- **ID Format:** Uppercase symbols (e.g., `BTC`, `ETH`, `SOL`)
- **Requires:** VELO_API_KEY secret

## Adding New Sources
1. Create a new file in `sources/` (e.g., `sources/newsource.py`)
2. Extend `BaseSource` class and implement:
   - `source_name` property
   - `pull()` method
3. Register in `sources/__init__.py` by adding to the `SOURCES` dict

## Environment Variables
- `DATABASE_URL`: PostgreSQL connection string (auto-configured)
- `ARTEMIS_API_KEY`: API key for Artemis data (required for artemis pulls)
- `DEFILLAMA_API_KEY`: API key for DefiLlama Pro API
- `VELO_API_KEY`: API key for Velo.xyz API (required for velo pulls)

## Entity Master System
The entity master system provides cross-source canonical IDs for unified queries:

### Tables
- **entities**: Canonical entity master (entity_id, canonical_id, name, symbol, entity_type, sector)
- **entity_source_ids**: Maps source-specific IDs to canonical entities (Rosetta Stone)

### ID Mappings
Each source uses different ID formats, mapped via entity_source_ids:
- Artemis: `sol`, `eth`, `btc` (short tickers) - **364 assets mapped**
- DefiLlama: `solana`, `ethereum`, `bitcoin` (CoinGecko IDs / slugs) - **343 entities mapped**
- Velo: `SOL`, `ETH`, `BTC` (uppercase symbols) - **482 coins mapped**
- **54 entities** have all 3 sources mapped for full cross-source analysis
- **971 total entities** after deduplication (merged ALT/AltLayer, Arkham/ARKM, Polygon/POL, ACX/Across, XPL/Plasma, etc.)

### Cross-Source Query Example
```sql
SELECT e.canonical_id, m.source, m.metric_name, m.value
FROM v_entity_comparison
WHERE canonical_id = 'bitcoin'
  AND ts = '2025-01-20';
```

### Metric Normalization View
The `v_normalized_metrics` view maps different metric names to canonical names:
- Velo `CLOSE_PRICE` → `PRICE` (matches Artemis)
- Velo `DOLLAR_OI_CLOSE` → `OPEN_INTEREST`
- Velo `DOLLAR_VOLUME` → `TRADING_VOLUME_24H`

## Automated Scheduling
The `scheduler.py` runs continuously and executes pulls at specific times (5 min after API updates):
- **Artemis:** Daily at 00:05 UTC (APIs update at midnight UTC)
- **DefiLlama:** Hourly at XX:05 UTC (APIs update on the hour)
- **Velo:** Hourly at XX:05 UTC (real-time data, pulled alongside DefiLlama)

All pulls run immediately on startup, then follow their scheduled times.

Run manually: `python scheduler.py`
Fresh start (clears all data first): `python scheduler.py --fresh`
Production mode (no backfills): `python scheduler.py --no-backfill`

## Production Deployment

The project is configured for VM deployment with `--no-backfill` flag:
- Starts pulling data immediately on the scheduled intervals
- Skips historical backfills (run these manually in development first)
- Uses the same production database (Neon-backed PostgreSQL)

**To deploy:** Click the "Publish" button in Replit. The production scheduler will:
1. Run initial pulls for all 3 sources immediately
2. Continue on schedule: DefiLlama/Velo hourly at XX:05, Artemis daily at 00:05

**To run backfills later:** After deploying, run backfills manually from the dev environment or add them to production when ready.

## Performance Optimizations
- **Parallel API requests:** Uses ThreadPoolExecutor with 10 workers for concurrent fetches
- **Connection pooling:** HTTP sessions with HTTPAdapter for connection reuse
- **Thread-safe sessions:** Each worker thread gets its own requests.Session via threading.local()
- **Bulk fetching:** All lookup endpoints fetched in single parallel batch (~11 URLs in ~2s vs ~9s sequential)
- **Pre-built entity URLs:** Per-entity API calls gathered upfront and fetched in single parallel batch
- **Unified rate limiting:** All HTTP calls (fetch_json, fetch_json_threadsafe, fetch_pro_json) go through a global token bucket limiter
- **Rate limit guarantee:** Thread-safe lock ensures max 15 req/sec (900/min), well under the 1000/min API limit
- **Velo dynamic column batching:** API has 22,500 cell limit per request. For large time gaps (>6 hours), uses 5 essential columns (CLOSE_PRICE, DOLLAR_VOLUME, DOLLAR_OI_CLOSE, FUNDING_RATE, LIQ_DOLLAR_VOL). For normal hourly operation (<6 hours), uses all 30 columns. Column batch size is calculated dynamically based on time range and number of coins per batch.
- **Source-specific timeouts:** Scheduler uses 15-minute timeout for Velo (handles large catch-ups), 10-minute timeout for other sources

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

### Velo Backfill
The `backfill_velo.py` script fetches hourly historical data from Velo API:
- **Method:** Uses 72-hour time windows with 5 essential metrics (CLOSE_PRICE, DOLLAR_VOLUME, DOLLAR_OI_CLOSE, FUNDING_RATE, PREMIUM)
- **API Limitation:** Returns minute-level data despite resolution=1h parameter - client aggregates to hourly
- **Time Estimate:** ~90 minutes for full 30-day backfill of all 1080 coin-exchange pairs
- **Data:** Hourly time series (configurable days, default 30)
- **Idempotent:** Uses ON CONFLICT DO NOTHING to prevent duplicates

Run: `python backfill_velo.py --days 30`
Options: `--coin BTC` (filter to one coin), `--exchange bybit` (filter to one exchange)

**Note:** The Velo API returns minute-level data which is memory-intensive. The backfill aggregates this to hourly records before inserting. Live hourly pulls insert only 100-200 new records because most hours already exist from backfill.

### Historical Data Coverage (as of Jan 21, 2026)

**Artemis:** ~1.4M records across 178 assets, 23 metrics
- PRICE: 214k records (178 assets)
- MC: 152k records (137 assets)
- CIRCULATING_SUPPLY_NATIVE: 152k records (122 assets)
- FEES: 115k records (105 assets)
- DAU: 111k records (96 assets)
- REVENUE: 103k records (97 assets)
- TXNS: 93k records (81 assets)
- FDMC_FEES_RATIO: 85k records (80 assets)
- FDMC_REVENUE_RATIO: 71k records (65 assets)
- TOTAL_SUPPLY_NATIVE: 62k records (49 assets)
- 24H_VOLUME: 61k records (55 assets)
- NEW_USERS: 26k records (19 assets)
- SPOT_VOLUME: 24k records (19 assets)
- Note: BATCH_SIZE reduced to 50 (from 250) to avoid HTTP 500 errors on on-chain metrics

**DefiLlama:** ~210K records across 1,177 assets, 61 metrics
- TVL: 63k records (228 assets)
- CHAIN_TVL: 32k records (99 chains)
- CHAIN_DEX_VOLUME: 27k records (29 chains)
- CHAIN_FEES: 22k records (30 chains)
- CHAIN_REVENUE: 19k records (26 chains)
- FEES/REVENUE: 30k+ records (protocol-level)

**Velo:** ~130K records across 485 assets, 30 metrics
- Hourly derivatives data (funding rates, open interest, liquidations)
- 4 exchanges: Binance Futures, Bybit, OKX Swap, Hyperliquid
