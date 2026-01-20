# Existing Data Sources

## Artemis

### Overview
Artemis provides institutional-grade crypto metrics including on-chain data, market data, and financial metrics.

### Configuration
- **Config file**: `artemis_config.csv`
- **Format**: Matrix (assets × metrics) with `Pull` column and metric columns
- **API key**: `ARTEMIS_API_KEY`

### Asset ID Format
Short tickers: `sol`, `eth`, `btc`, `aave`, `uni`

### Metrics Available
| Metric | API ID | Description |
|--------|--------|-------------|
| Price | PRICE | Current token price |
| Market Cap | MC | Market capitalization |
| Fully Diluted MC | FDMC | Fully diluted market cap |
| Fees | FEES | Protocol fees |
| Revenue | REVENUE | Protocol revenue |
| Daily Active Users | DAU | Unique active addresses |
| Transactions | TXNS | Transaction count |
| 24H Volume | 24H_VOLUME | Trading volume |
| Average Txn Fee | AVG_TXN_FEE | Average transaction fee |
| Perpetual Fees | PERP_FEES | Perpetual trading fees |
| Perpetual Txns | PERP_TXNS | Perpetual transaction count |
| Lending Deposits | LENDING_DEPOSITS | Total lending deposits |
| Lending Borrows | LENDING_BORROWS | Total lending borrows |
| Spot Volume | SPOT_VOLUME | Spot trading volume |

### Excluded Metrics
These are excluded due to data quality issues:
- VOLATILITY_90D_ANN
- STABLECOIN_AVG_DAU
- TOKENIZED_SHARES_TRADING_VOLUME
- FDMV_NAV_RATIO

### API Details
- **Base URL**: `https://api.artemisxyz.com/data`
- **Rate limit**: ~8 req/sec recommended
- **Batch size**: 250 symbols per request
- **Endpoint format**: `/data/{metric}/?symbols={symbol1},{symbol2}&APIKey={key}`

### Code Structure
```python
# sources/artemis.py
class ArtemisSource(BaseSource):
    def pull(self) -> int:
        # 1. Load config from CSV (matrix format)
        # 2. Group assets by metric
        # 3. Batch requests (250 per call)
        # 4. Parse nested response structure
        # 5. Insert with upsert
```

---

## DefiLlama

### Overview
DefiLlama provides DeFi-focused data including TVL, fees, volumes, and protocol metrics. Has both free and Pro API tiers.

### Configuration
- **Config file**: `defillama_config.csv`
- **Format**: Simple list with `gecko_id`, `name`, `slug`, `Pull` columns
- **API key**: `DEFILLAMA_API_KEY` (for Pro endpoints)

### Asset ID Format
CoinGecko IDs: `solana`, `ethereum`, `bitcoin`, `aave`, `uniswap`

### Chain vs Protocol Detection
Entities are automatically categorized using DefiLlama's `/chains` API:
- If entity name matches a chain from the API → chain metrics
- Otherwise → protocol metrics

### Metrics Available

**Chain Metrics** (for L1/L2 chains):
| Metric | Endpoint | Description |
|--------|----------|-------------|
| CHAIN_TVL | `/v2/historicalChainTvl/{chain}` | Chain total TVL |
| CHAIN_FEES_24H | `/overview/fees/{chain}` | 24h chain fees |
| CHAIN_REVENUE_24H | `/overview/fees/{chain}?dataType=dailyRevenue` | 24h chain revenue |
| CHAIN_DEX_VOLUME_24H | `/overview/dexs/{chain}` | 24h DEX volume on chain |
| CHAIN_PERPS_VOLUME_24H | `/overview/derivatives/{chain}` | 24h perps volume |
| CHAIN_OPTIONS_VOLUME_24H | `/overview/options/{chain}` | 24h options volume |

**Protocol Metrics** (for DeFi protocols):
| Metric | Endpoint | Description |
|--------|----------|-------------|
| TVL | `/protocol/{slug}` | Protocol TVL |
| FEES_24H | `/summary/fees/{slug}` | 24h fees |
| REVENUE_24H | `/summary/fees/{slug}?dataType=dailyRevenue` | 24h revenue |
| DEX_VOLUME_24H | `/summary/dexs/{slug}` | 24h DEX volume |
| DERIVATIVES_VOLUME_24H | `/summary/derivatives/{slug}` | 24h derivatives volume |
| EARNINGS | `/earnings/{slug}` | Protocol earnings |
| INCENTIVES | `/earnings/{slug}` | Token incentives |

**Pro API Metrics** (require DEFILLAMA_API_KEY):
| Metric | Endpoint | Description |
|--------|----------|-------------|
| INFLOW | `/api/inflows/{slug}/{timestamp}` | 30-day inflows |
| OUTFLOW | `/api/inflows/{slug}/{timestamp}` | 30-day outflows |
| OPEN_INTEREST | `/yields/perps` | Perpetual open interest |

### API Details
- **Free URL**: `https://api.llama.fi`
- **Pro URL**: `https://pro-api.llama.fi`
- **Rate limit**: 1000 req/min for Pro (~16 req/sec), 500/min for free
- **Recommended delay**: 0.08s (~12 req/sec)

### Slug Resolution
Chain endpoints use a fallback order to find the correct slug:
1. `slug` field from config
2. `name` field (lowercased, spaces to dashes)
3. `gecko_id` field

### Code Structure
```python
# sources/defillama.py
class DefiLlamaSource(BaseSource):
    def pull(self) -> int:
        # 1. Fetch chain list from /chains API
        # 2. Load config from CSV
        # 3. Fetch bulk data (protocols, fees, volumes, etc.)
        # 4. For each entity:
        #    - Determine if chain or protocol
        #    - Fetch appropriate metrics
        #    - Chain: call fetch_chain_metrics()
        #    - Protocol: call fetch_protocol_earnings()
        # 5. Fetch open interest from Pro API
        # 6. Insert with upsert
```

### Metric Mapping
Raw API fields are mapped to standardized names:
```python
METRIC_MAP = {
    'tvl': 'TVL',
    'fees_total24h': 'FEES_24H',
    'revenue_total24h': 'REVENUE_24H',
    'dexs_total24h': 'DEX_VOLUME_24H',
    'chain_tvl': 'CHAIN_TVL',
    'chain_fees_24h': 'CHAIN_FEES_24H',
    # ... etc
}
```

---

## Cross-Source Considerations

### Different ID Systems
| Source | Bitcoin | Ethereum | Solana |
|--------|---------|----------|--------|
| Artemis | btc | eth | sol |
| DefiLlama | bitcoin | ethereum | solana |

Cross-source queries require joining on the `source` column or maintaining an entity mapping table.

### Metric Name Consistency
Both sources use standardized uppercase metric names:
- `PRICE`, `TVL`, `FEES`, `REVENUE`, `DAU`, etc.

This allows cross-source comparisons when the same metric is available.
