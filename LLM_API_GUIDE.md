# Financial Data API - LLM Query Guide

This document contains everything you need to query the Crypto & Equity Data API for financial analysis.

## API Access

| Item | Value |
|------|-------|
| **Base URL** | `https://8acc5ab4-40f6-4976-b1be-4973166c8566-00-28hrjot7q1pu2.riker.replit.dev/api` |
| **Authentication** | Header: `X-API-Key: Polychain2030!#` |
| **Format** | JSON |

**URL Structure:**
- Dashboard: `https://domain.replit.dev/` (Streamlit UI)
- REST API: `https://domain.replit.dev/api/...` (Data endpoints)

**All requests require the API key header:**
```
X-API-Key: Polychain2030!#
```

---

## LLM-Friendly Features

The API is optimized for LLM usage with these convenience features:

### 1. Case-Insensitive Parameters
All parameter values are case-insensitive:
- `metric=TVL`, `metric=tvl`, `metric=Tvl` all work
- `source=CoinGecko`, `source=coingecko` both work
- `canonical_id=BITCOIN`, `canonical_id=Bitcoin` both work

### 2. Canonical ID Resolution
Use simple canonical IDs like `bitcoin`, `ethereum`, `solana` instead of source-specific IDs. The API automatically resolves them:
```bash
# Instead of knowing source-specific IDs:
GET /api/time-series?asset=bitcoin&metric=FEES
# Auto-resolves to 'btc' for Artemis, 'bitcoin' for DefiLlama, etc.
```

### 3. Auto-Source Selection
When you omit the `source` parameter, the API automatically selects the best source for each metric:

| Metric | Auto-Selected Source |
|--------|---------------------|
| `TVL`, `CHAIN_TVL`, `FEES_24H`, `DEX_VOLUME_24H` | DefiLlama |
| `FEES`, `REVENUE`, `DAU`, `TXNS` | Artemis |
| `PRICE`, `MARKET_CAP`, `VOLUME_24H` | CoinGecko |
| `FUNDING_RATE_AVG`, `DOLLAR_OI_CLOSE` | Velo |
| `OPEN`, `HIGH`, `LOW`, `CLOSE` | AlphaVantage |

```bash
# Source auto-selected based on metric:
GET /api/time-series?asset=bitcoin&metric=TVL      # Uses DefiLlama
GET /api/time-series?asset=bitcoin&metric=FEES     # Uses Artemis
GET /api/time-series?asset=bitcoin&metric=PRICE    # Uses CoinGecko
```

### 4. Available Metrics Per Entity
The `/entities/{canonical_id}` endpoint now returns `available_metrics` showing exactly which metrics exist for each source:
```json
{
  "canonical_id": "bitcoin",
  "source_mappings": {"artemis": "btc", "defillama": "bitcoin", ...},
  "available_metrics": {
    "artemis": ["DAU", "FEES", "REVENUE", "TXNS"],
    "defillama": ["CHAIN_TVL", "CHAIN_DEX_VOLUME"]
  }
}
```

### 5. Resolution Metadata
When canonical IDs are resolved, responses include `_meta` showing what was resolved:
```json
{
  "asset": "btc",
  "metric": "FEES",
  "_meta": {
    "requested_asset": "bitcoin",
    "resolved_asset": "btc",
    "resolved_source": "artemis"
  }
}
```

---

## Data Sources

The database contains 3 years of historical data from 5 sources:

| Source | Data Type | Frequency | Coverage |
|--------|-----------|-----------|----------|
| **Artemis** | Crypto fundamentals (fees, revenue, DAU) | Daily | ~364 assets |
| **DefiLlama** | DeFi metrics (TVL, volume, stablecoins) | Hourly | ~371 protocols |
| **Velo** | Derivatives (funding rates, open interest) | Hourly | ~490 pairs |
| **CoinGecko** | Market data (price, market cap) | Hourly | ~201 tokens |
| **AlphaVantage** | Equities (OHLCV stock prices) | Hourly | ~52 tickers |

---

## Available Metrics by Source

### Artemis (Crypto Fundamentals)
- `PRICE` - Token price in USD
- `MC` - Market capitalization
- `FEES` - Protocol fees generated
- `REVENUE` - Protocol revenue
- `DAU` - Daily active users
- `TXNS` - Transaction count
- `24H_VOLUME` - 24-hour trading volume
- `AVG_TXN_FEE` - Average transaction fee
- `NEW_USERS` - New users count
- `SPOT_VOLUME` - Spot trading volume

### DefiLlama (DeFi Metrics)
- `TVL` - Total value locked (protocol)
- `CHAIN_TVL` - Total value locked (chain)
- `FEES_24H` - 24-hour fees
- `REVENUE_24H` - 24-hour revenue
- `DEX_VOLUME_24H` - DEX trading volume
- `DERIVATIVES_VOLUME_24H` - Derivatives volume
- `STABLECOIN_SUPPLY` - Stablecoin market cap
- `BRIDGE_VOLUME_24H/7D/30D` - Bridge volumes

### Velo (Derivatives Data)
- `CLOSE_PRICE` - Futures close price
- `DOLLAR_VOLUME` - Trading volume in USD
- `DOLLAR_OI_CLOSE` - Open interest at close
- `FUNDING_RATE_AVG` - Average funding rate
- `LIQ_DOLLAR_VOL` - Liquidation volume

### CoinGecko (Market Data)
- `PRICE` - Current price
- `MARKET_CAP` - Market capitalization
- `VOLUME_24H` - 24-hour volume
- `PRICE_CHANGE_24H_PCT` - 24h price change %
- `ATH` - All-time high price
- `ATL` - All-time low price

### AlphaVantage (Equities)
- `OPEN`, `HIGH`, `LOW`, `CLOSE` - OHLC prices
- `ADJUSTED_CLOSE` - Split/dividend adjusted close
- `VOLUME` - Trading volume
- `DIVIDEND_AMOUNT` - Dividend payments

---

## API Endpoints

### 1. GET /api/data-dictionary
Returns complete schema documentation. **Call this first** to understand available data.

```bash
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/data-dictionary"
```

### 2. GET /api/entities
Search and filter entities (assets).

**Parameters:**
- `type` - Filter: token, chain, protocol, equity, etf
- `sector` - Filter: layer-1, defi, meme, btc_mining, crypto_etf, etc.
- `source` - Filter: artemis, defillama, velo, coingecko, alphavantage
- `search` - Search by name or symbol
- `limit` - Max results (default 50)

**Examples:**
```bash
# Find all layer-1 chains
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/entities?sector=layer-1&limit=20"

# Search for Bitcoin-related entities
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/entities?search=bitcoin"

# List all equities
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/entities?type=equity"
```

### 3. GET /api/entities/{canonical_id}
Get full details for a specific entity including all source ID mappings and available metrics.

```bash
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/entities/bitcoin"
```

**Response includes:**
- Entity metadata (name, symbol, type, sector)
- Source mappings (what ID to use for each source)
- Available metrics per source (shows exactly what data exists)

### 4. GET /api/metrics
List available metrics with record counts.

**Parameters:**
- `source` - Filter by source
- `asset` - Filter by asset

```bash
# All metrics from Artemis
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/metrics?source=artemis"
```

### 5. GET /api/latest
Get the most recent value for a metric across all assets. **Best for rankings and comparisons.**

**Parameters:**
- `metric` (required) - Metric name (case-insensitive: PRICE, tvl, Fees all work)
- `source` - Filter by source (optional - auto-selected based on metric)
- `assets` - Comma-separated list of asset IDs or canonical IDs (bitcoin, ethereum)
- `limit` - Max results (default 100)

**Examples:**
```bash
# Top 20 assets by TVL (source auto-selected: DefiLlama)
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/latest?metric=TVL&limit=20"

# Latest prices (source auto-selected: CoinGecko)
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/latest?metric=PRICE&limit=50"

# Specific assets using canonical IDs
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/latest?metric=FEES&assets=bitcoin,ethereum,solana"
```

### 6. GET /api/time-series
Get historical time series data. **Best for trends and charts.**

**Parameters:**
- `asset` (required) - Canonical ID (bitcoin, ethereum) or source-specific ID - auto-resolved
- `metric` (required) - Metric name (case-insensitive)
- `source` - Source name (optional - auto-selected based on metric)
- `start_date` - Start date (YYYY-MM-DD)
- `end_date` - End date (YYYY-MM-DD)
- `days` - Days of history (default 30)

**Examples:**
```bash
# Simplified: Just use canonical ID and metric (source auto-selected)
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/time-series?asset=bitcoin&metric=PRICE&days=30"

# TVL trend - auto-selects DefiLlama
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/time-series?asset=solana&metric=TVL&days=90"

# Fee revenue - auto-selects Artemis
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/time-series?asset=ethereum&metric=FEES&days=365"

# Stock prices still need explicit source (AlphaVantage uses ticker symbols)
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/time-series?asset=COIN&metric=CLOSE&source=alphavantage&days=60"
```

### 7. GET /api/cross-source
Compare data for an entity across different sources. **Best for data validation and multi-source analysis.**

**Parameters:**
- `canonical_id` (required) - Canonical entity ID
- `metric` - Filter by metric
- `days` - Days to look back (default 7)

**Examples:**
```bash
# Compare Bitcoin data across all sources
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/cross-source?canonical_id=bitcoin"

# Compare Ethereum prices specifically
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/cross-source?canonical_id=ethereum&metric=PRICE"
```

### 8. GET /api/sources
List all data sources with record counts.

```bash
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/sources"
```

### 9. GET /api/stats
Get database statistics and health info.

```bash
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/stats"
```

---

## Entity ID Formats

Different sources use different ID formats. Use `/entities/{canonical_id}` to find the correct ID for each source.

| Source | ID Format | Examples |
|--------|-----------|----------|
| Artemis | Lowercase short | `sol`, `eth`, `btc`, `aave` |
| DefiLlama | CoinGecko IDs | `solana`, `ethereum`, `aave` |
| Velo | SYMBOL_EXCHANGE | `BTC_binance-futures`, `ETH_bybit` |
| CoinGecko | Lowercase slugs | `bitcoin`, `ethereum`, `solana` |
| AlphaVantage | Stock tickers | `COIN`, `MSTR`, `SPY`, `AAPL` |

---

## Common Query Patterns

### Price Analysis
```bash
# Get current prices for top assets
GET /api/latest?metric=PRICE&source=coingecko&limit=20

# Get Bitcoin 90-day price history
GET /api/time-series?asset=bitcoin&metric=PRICE&source=coingecko&days=90

# Compare Bitcoin price across sources
GET /api/cross-source?canonical_id=bitcoin&metric=PRICE
```

### DeFi Analysis
```bash
# Top protocols by TVL
GET /api/latest?metric=TVL&source=defillama&limit=20

# Aave TVL trend over 6 months
GET /api/time-series?asset=aave&metric=TVL&source=defillama&days=180

# Top fee-generating protocols
GET /api/latest?metric=FEES_24H&source=defillama&limit=10
```

### Derivatives Analysis
```bash
# Bitcoin funding rates across exchanges
GET /api/time-series?asset=BTC_binance-futures&metric=FUNDING_RATE_AVG&source=velo&days=30

# Open interest for ETH
GET /api/time-series?asset=ETH_binance-futures&metric=DOLLAR_OI_CLOSE&source=velo&days=30
```

### Equity/Crypto Correlation
```bash
# COIN stock price
GET /api/time-series?asset=COIN&metric=CLOSE&source=alphavantage&days=90

# Bitcoin miners (MARA, RIOT)
GET /api/time-series?asset=MARA&metric=CLOSE&source=alphavantage&days=90

# Compare with Bitcoin
GET /api/time-series?asset=bitcoin&metric=PRICE&source=coingecko&days=90
```

### Fundamental Analysis
```bash
# Protocol revenue leaders
GET /api/latest?metric=REVENUE&source=artemis&limit=20

# Ethereum daily active users trend
GET /api/time-series?asset=eth&metric=DAU&source=artemis&days=90

# Transaction volume by chain
GET /api/latest?metric=TXNS&source=artemis&limit=20
```

---

## Sample Entities

### Layer-1 Chains
- `bitcoin` / `btc` - Bitcoin
- `ethereum` / `eth` - Ethereum
- `solana` / `sol` - Solana
- `avalanche-2` / `avax` - Avalanche
- `cardano` / `ada` - Cardano

### DeFi Protocols
- `aave` - Aave lending
- `uniswap` - Uniswap DEX
- `lido` - Lido staking
- `maker` / `mkr` - MakerDAO
- `curve-dao-token` / `crv` - Curve

### Crypto Equities
- `COIN` - Coinbase
- `MSTR` - MicroStrategy
- `MARA` - Marathon Digital
- `RIOT` - Riot Platforms
- `HOOD` - Robinhood

### Crypto ETFs
- `IBIT` - iShares Bitcoin Trust
- `FBTC` - Fidelity Bitcoin
- `GBTC` - Grayscale Bitcoin
- `ETHA` - iShares Ethereum Trust

---

## Tips for Financial Analysis

1. **Always check entity mappings first** - Use `/entities/{canonical_id}` to find the right asset ID for each source

2. **Cross-validate with multiple sources** - Use `/cross-source` to compare data from different providers

3. **Match time periods** - When comparing assets, use the same `days` parameter

4. **Use the right source for each metric:**
   - Price/Market Cap: CoinGecko or Artemis
   - TVL/DeFi metrics: DefiLlama
   - Derivatives/Funding: Velo
   - Fundamentals (fees, revenue, DAU): Artemis
   - Equities: AlphaVantage

5. **Rate limiting** - The API handles rate limiting internally, but avoid rapid successive calls

---

## Error Handling

| Status | Meaning |
|--------|---------|
| 200 | Success |
| 401 | Invalid or missing API key |
| 404 | Entity or resource not found |
| 422 | Invalid parameters |
| 503 | Database temporarily unavailable |

---

## Quick Start Workflow

1. **Understand the schema:** `GET /api/data-dictionary`
2. **Find your entity:** `GET /api/entities?search=bitcoin`
3. **Get entity details (with available metrics):** `GET /api/entities/bitcoin`
4. **Query the data (source auto-selected):** `GET /api/time-series?asset=bitcoin&metric=PRICE&days=30`

**Simplified Example:**
```bash
# Just ask for what you want - no need to specify source
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/api/time-series?asset=bitcoin&metric=TVL&days=30"
# API automatically: resolves 'bitcoin' to 'bitcoin' (DefiLlama ID), selects DefiLlama as source
```
