# Database Schema Documentation

## Overview
This PostgreSQL schema stores time-series financial data from 5 API sources for cryptocurrency and equity markets. Designed for recurring data pulls with cross-source entity matching.

## Core Tables

### `entities` - Canonical Entity Master
Central registry of all assets (coins, chains, protocols, stocks) with a single canonical ID.

| Column | Description |
|--------|-------------|
| entity_id | Auto-increment primary key |
| canonical_id | Unique human-readable ID (e.g., "bitcoin", "ethereum", "solana") |
| name | Display name |
| symbol | Trading symbol (BTC, ETH, SOL) |
| entity_type | token, chain, protocol, stablecoin, bridge, stock |
| asset_class | crypto, equity |
| sector | DeFi, L1, L2, CEX, Mining, etc. |
| coingecko_id | Optional CoinGecko ID for API lookups |

### `entity_source_ids` - Cross-Source ID Mapping (Rosetta Stone)
Maps source-specific IDs to canonical entities. Critical for joining data across sources.

| Source | ID Format | Example |
|--------|-----------|---------|
| artemis | Short lowercase | btc, eth, sol |
| defillama | CoinGecko IDs/slugs | bitcoin, ethereum, solana |
| velo | Uppercase symbols | BTC, ETH, SOL |
| coingecko | CoinGecko IDs | bitcoin, ethereum, solana |
| alphavantage | Stock tickers | MSTR, COIN, SPY |

### `metrics` - Time Series Data
All metric values from all sources in a unified format.

| Column | Description |
|--------|-------------|
| pulled_at | Timestamp of the data point |
| source | artemis, defillama, velo, coingecko, alphavantage |
| asset | Source-specific asset ID |
| metric_name | Standardized metric name (PRICE, TVL, FEES, etc.) |
| value | Numeric value |
| domain | Optional: spot, derivative, chain, protocol |
| exchange | Optional: For Velo derivatives (binance_futures, bybit, etc.) |
| entity_id | FK to entities table (optional, for direct joins) |
| granularity | daily or hourly |

**Unique constraint:** (source, asset, metric_name, pulled_at, exchange) prevents duplicates.

### `pulls` - Pull Log
Audit trail of all API pulls.

| Column | Description |
|--------|-------------|
| source_name | Which API was called |
| pulled_at | When the pull occurred |
| status | success, error, partial |
| records_count | How many records were inserted |

## Key Views

### `v_entity_comparison`
Cross-source analysis using canonical IDs. Joins metrics through entity mappings.
```sql
SELECT * FROM v_entity_comparison 
WHERE canonical_id = 'bitcoin' AND metric_name = 'PRICE';
```

### `v_normalized_metrics`
Normalizes Velo metric names to match other sources:
- CLOSE_PRICE → PRICE
- DOLLAR_OI_CLOSE → OPEN_INTEREST
- DOLLAR_VOLUME → TRADING_VOLUME_24H

### `v_velo_daily`
Aggregates hourly Velo data to daily (sums volumes, averages funding rates, takes close prices).

### `v_metrics_daily`
Union of daily metrics + aggregated Velo hourly data for unified daily analysis.

### `v_latest_metrics`
Most recent value for each (source, asset, metric, exchange) combination.

## Data Sources & Metrics

### Artemis (Daily)
- PRICE, MC, FDMC, FEES, REVENUE, DAU, TXNS, NEW_USERS
- CIRCULATING_SUPPLY_NATIVE, TOTAL_SUPPLY_NATIVE
- SPOT_VOLUME, PERP_VOLUME, OPEN_INTEREST
- Equity metrics: EQ_EV, NAV, EQ_CASH, EQ_TOTAL_DEBT

### DefiLlama (Hourly)
- Chain metrics: CHAIN_TVL, CHAIN_FEES_24H, CHAIN_REVENUE_24H, CHAIN_DEX_VOLUME_24H
- Protocol metrics: TVL, FEES_24H, REVENUE_24H, DEX_VOLUME_24H
- Stablecoin metrics: STABLECOIN_SUPPLY
- Bridge metrics: BRIDGE_VOLUME_24H/7D/30D

### Velo (Hourly)
- Per-exchange derivatives data: binance_futures, bybit, okx_swap, hyperliquid
- CLOSE_PRICE, DOLLAR_VOLUME, DOLLAR_OI_CLOSE, FUNDING_RATE_AVG, LIQ_DOLLAR_VOL

### CoinGecko (Hourly)
- PRICE, VOLUME_24H, MARKET_CAP
- CIRCULATING_SUPPLY, TOTAL_SUPPLY, MAX_SUPPLY, FDV

### AlphaVantage (Daily)
- Stock data: OPEN, HIGH, LOW, CLOSE, VOLUME, DOLLAR_VOLUME
- 22 crypto/fintech equities (MSTR, COIN, MARA, SPY, QQQ, etc.)

## Common Query Patterns

### Get latest BTC price from all sources
```sql
SELECT source, value, ts 
FROM v_latest_metrics 
WHERE canonical_id = 'bitcoin' AND metric_name = 'PRICE';
```

### Compare TVL across DeFi protocols
```sql
SELECT canonical_id, value 
FROM v_entity_comparison 
WHERE metric_name = 'TVL' AND ts = CURRENT_DATE
ORDER BY value DESC;
```

### Get derivatives data for a coin across exchanges
```sql
SELECT exchange, funding_rate, open_interest_usd 
FROM v_derivatives_summary 
WHERE canonical_id = 'bitcoin' AND ts > NOW() - INTERVAL '24 hours';
```

### Cross-source price comparison
```sql
SELECT e.canonical_id, m.source, m.metric_name, m.value
FROM metrics m
JOIN entity_source_ids esi ON m.source = esi.source AND m.asset = esi.source_id
JOIN entities e ON esi.entity_id = e.entity_id
WHERE e.canonical_id = 'ethereum'
  AND m.metric_name IN ('PRICE', 'CLOSE_PRICE')
  AND m.pulled_at > NOW() - INTERVAL '1 day';
```

## Storage Estimates
- ~500 bytes per row average
- Velo: ~47M rows/year (~22 GB)
- DefiLlama: ~41M rows/year (~19 GB)
- CoinGecko: ~12M rows/year (~6 GB)
- Artemis: ~500K rows/year (~0.25 GB)
- AlphaVantage: ~48K rows/year (~0.02 GB)
- **Total: ~75-100 GB/year with indexes**

## Key Design Decisions
1. **Source-specific IDs preserved** - Each source uses different ID formats; entity_source_ids maps them
2. **Upsert on conflict** - Live pulls update today's values; backfills skip existing data
3. **Granularity column** - Distinguishes hourly Velo data from daily data for aggregation
4. **Exchange column** - Nullable; only used for Velo derivatives (4 exchanges)
5. **Domain column** - Optional categorization (spot, derivative, chain, protocol)
