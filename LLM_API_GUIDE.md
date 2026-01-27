# Financial Data API - LLM Query Guide

This document contains everything you need to query the Crypto & Equity Data API for financial analysis.

## API Access

| Item | Value |
|------|-------|
| **Base URL** | `https://8acc5ab4-40f6-4976-b1be-4973166c8566-00-28hrjot7q1pu2.riker.replit.dev:8000` |
| **Authentication** | Header: `X-API-Key: Polychain2030!#` |
| **Format** | JSON |

**IMPORTANT:** The URL must include `:8000` - the main URL without the port goes to the Dashboard, not the API.

**All requests require the API key header:**
```
X-API-Key: Polychain2030!#
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

### 1. GET /data-dictionary
Returns complete schema documentation. **Call this first** to understand available data.

```bash
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/data-dictionary"
```

### 2. GET /entities
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
  "https://BASE_URL/entities?sector=layer-1&limit=20"

# Search for Bitcoin-related entities
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/entities?search=bitcoin"

# List all equities
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/entities?type=equity"
```

### 3. GET /entities/{canonical_id}
Get full details for a specific entity including all source ID mappings.

```bash
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/entities/bitcoin"
```

**Response includes:**
- Entity metadata (name, symbol, type, sector)
- Source mappings (what ID to use for each source)

### 4. GET /metrics
List available metrics with record counts.

**Parameters:**
- `source` - Filter by source
- `asset` - Filter by asset

```bash
# All metrics from Artemis
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/metrics?source=artemis"
```

### 5. GET /latest
Get the most recent value for a metric across all assets. **Best for rankings and comparisons.**

**Parameters:**
- `metric` (required) - Metric name (PRICE, TVL, FEES, etc.)
- `source` - Filter by source
- `assets` - Comma-separated list of assets
- `limit` - Max results (default 100)

**Examples:**
```bash
# Top 20 assets by TVL
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/latest?metric=TVL&source=defillama&limit=20"

# Latest prices from CoinGecko
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/latest?metric=PRICE&source=coingecko&limit=50"

# Top fee-generating protocols
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/latest?metric=FEES&source=artemis&limit=10"
```

### 6. GET /time-series
Get historical time series data. **Best for trends and charts.**

**Parameters:**
- `asset` (required) - Asset ID (format varies by source)
- `metric` (required) - Metric name
- `source` - Source name (recommended)
- `start_date` - Start date (YYYY-MM-DD)
- `end_date` - End date (YYYY-MM-DD)
- `days` - Days of history (default 30)

**Examples:**
```bash
# Bitcoin price history (30 days)
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/time-series?asset=bitcoin&metric=PRICE&source=coingecko&days=30"

# Solana TVL trend (90 days)
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/time-series?asset=solana&metric=TVL&source=defillama&days=90"

# Ethereum fee revenue (1 year)
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/time-series?asset=eth&metric=FEES&source=artemis&days=365"

# COIN stock price history
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/time-series?asset=COIN&metric=CLOSE&source=alphavantage&days=60"
```

### 7. GET /cross-source
Compare data for an entity across different sources. **Best for data validation and multi-source analysis.**

**Parameters:**
- `canonical_id` (required) - Canonical entity ID
- `metric` - Filter by metric
- `days` - Days to look back (default 7)

**Examples:**
```bash
# Compare Bitcoin data across all sources
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/cross-source?canonical_id=bitcoin"

# Compare Ethereum prices specifically
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/cross-source?canonical_id=ethereum&metric=PRICE"
```

### 8. GET /sources
List all data sources with record counts.

```bash
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/sources"
```

### 9. GET /stats
Get database statistics and health info.

```bash
curl -H "X-API-Key: Polychain2030!#" \
  "https://BASE_URL/stats"
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
GET /latest?metric=PRICE&source=coingecko&limit=20

# Get Bitcoin 90-day price history
GET /time-series?asset=bitcoin&metric=PRICE&source=coingecko&days=90

# Compare Bitcoin price across sources
GET /cross-source?canonical_id=bitcoin&metric=PRICE
```

### DeFi Analysis
```bash
# Top protocols by TVL
GET /latest?metric=TVL&source=defillama&limit=20

# Aave TVL trend over 6 months
GET /time-series?asset=aave&metric=TVL&source=defillama&days=180

# Top fee-generating protocols
GET /latest?metric=FEES_24H&source=defillama&limit=10
```

### Derivatives Analysis
```bash
# Bitcoin funding rates across exchanges
GET /time-series?asset=BTC_binance-futures&metric=FUNDING_RATE_AVG&source=velo&days=30

# Open interest for ETH
GET /time-series?asset=ETH_binance-futures&metric=DOLLAR_OI_CLOSE&source=velo&days=30
```

### Equity/Crypto Correlation
```bash
# COIN stock price
GET /time-series?asset=COIN&metric=CLOSE&source=alphavantage&days=90

# Bitcoin miners (MARA, RIOT)
GET /time-series?asset=MARA&metric=CLOSE&source=alphavantage&days=90

# Compare with Bitcoin
GET /time-series?asset=bitcoin&metric=PRICE&source=coingecko&days=90
```

### Fundamental Analysis
```bash
# Protocol revenue leaders
GET /latest?metric=REVENUE&source=artemis&limit=20

# Ethereum daily active users trend
GET /time-series?asset=eth&metric=DAU&source=artemis&days=90

# Transaction volume by chain
GET /latest?metric=TXNS&source=artemis&limit=20
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

1. **Understand the schema:** `GET /data-dictionary`
2. **Find your entity:** `GET /entities?search=bitcoin`
3. **Get entity details:** `GET /entities/bitcoin`
4. **Query the data:** `GET /time-series?asset=bitcoin&metric=PRICE&source=coingecko&days=30`
