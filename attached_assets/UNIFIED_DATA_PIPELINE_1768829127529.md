# Unified Artemis + DefiLlama Data Pipeline

## Overview

This document provides specifications for integrating DefiLlama data pulls into your existing Artemis pipeline, creating a unified crypto analytics database.

## Key Findings: Format Comparison

### 1. Data Structure

| Aspect | Artemis | DefiLlama (converted) |
|--------|---------|-----------|
| **Format** | Long/normalized (EAV) | Long/normalized (EAV) ✓ |
| **Schema** | `asset, metric_name, value, pulled_at, source` | `asset, metric_name, value, pulled_at, source` ✓ |
| **Values** | Strings representing numbers | Strings representing numbers ✓ |
| **Precision** | Full decimal precision | Full decimal precision ✓ |

**Result**: DefiLlama pull script outputs **exact same format** as Artemis. Can be directly UNIONed or appended.

### 2. Entity Identification

| Source | Primary ID | Secondary ID | Example |
|--------|-----------|--------------|---------|
| Artemis | `asset` (artemis_id) | `coingecko_id` | `sol` → `solana` |
| DefiLlama | `gecko_id` | `slug` | `solana` |

**Critical Issue**: Only ~30% of Artemis assets have direct gecko_id mapping.
- Artemis uses short IDs: `sol`, `eth`, `btc`  
- DefiLlama uses CoinGecko IDs: `solana`, `ethereum`, `bitcoin`
- Equities (`eq-*`) have no gecko_id equivalent

**Recommendation**: Use `gecko_id` as the universal join key. Create a mapping table for translation.

### 3. Metric Naming

| Metric Type | Artemis | DefiLlama |
|-------------|---------|-----------|
| TVL | `TVL` | `protocols_tvl`, `chains_tvl` |
| Fees | `FEES` | `fees_total24h` |
| Revenue | `REVENUE` | `fees_dailyRevenue` |
| Market Cap | `MC` | `protocols_mcap` |
| FDV | `FDMC` | `protocols_fdv` |
| Volume | `SPOT_VOLUME` | `dexs_total24h` |
| DAU | `DAU` | *(not available)* |
| Price | `PRICE` | `stablecoins_price` (stables only) |

**Recommendation**: Standardize to Artemis naming convention since it's already established.

### 4. Value Formats

Both sources output raw numeric values as strings. No transformation needed.

```
Artemis:    FEES: 1885566.5592120884
DefiLlama:  fees_total24h: 394231
```

Both maintain full decimal precision. Both use standard decimal notation (no scientific notation observed).

## Recommended Database Schema

### Option A: Unified Long Format (Recommended)

```sql
CREATE TABLE metrics_latest (
    asset VARCHAR(100),            -- gecko_id for DefiLlama, artemis_id for Artemis
    metric_name VARCHAR(100),      -- standardized metric name
    value DECIMAL(30, 10),         -- numeric value
    source VARCHAR(20),            -- 'artemis' or 'defillama'
    pulled_at TIMESTAMP,
    PRIMARY KEY (asset, metric_name, source)
);

-- Optional: entity mapping for cross-source joins
CREATE TABLE entity_mapping (
    gecko_id VARCHAR(100) PRIMARY KEY,
    artemis_id VARCHAR(50),
    name VARCHAR(200),
    category VARCHAR(50)
);
```

### Option B: Source-Specific Tables with View

```sql
CREATE TABLE artemis_metrics (
    asset VARCHAR(50),
    metric_name VARCHAR(100),
    value DECIMAL(30, 10),
    pulled_at TIMESTAMP
);

CREATE TABLE defillama_metrics (
    asset VARCHAR(100),  -- uses gecko_id
    metric_name VARCHAR(100),
    value DECIMAL(30, 10),
    pulled_at TIMESTAMP
);

-- Unified view with entity mapping
CREATE VIEW unified_metrics AS
SELECT 
    COALESCE(em.artemis_id, a.asset) as asset,
    a.asset as artemis_id,
    a.metric_name,
    a.value,
    'artemis' as source,
    a.pulled_at
FROM artemis_metrics a
LEFT JOIN entity_mapping em ON a.asset = em.artemis_id
UNION ALL
SELECT 
    d.asset as asset,  -- gecko_id
    em.artemis_id,
    d.metric_name,
    d.value,
    'defillama' as source,
    d.pulled_at
FROM defillama_metrics d
LEFT JOIN entity_mapping em ON d.asset = em.gecko_id;
```

## Metric Mapping Reference

```python
METRIC_MAP = {
    # DefiLlama metric -> Artemis equivalent
    'protocols_tvl': 'TVL',
    'chains_tvl': 'CHAIN_TVL',
    'protocols_mcap': 'MC',
    'protocols_fdv': 'FDMC',
    'fees_total24h': 'FEES_24H',
    'fees_dailyRevenue': 'REVENUE_24H',
    'fees_dailyProtocolRevenue': 'PROTOCOL_REVENUE_24H',
    'fees_dailySupplySideRevenue': 'SUPPLY_SIDE_REVENUE_24H',
    'fees_dailyUserFees': 'USER_FEES_24H',
    'fees_total7d': 'FEES_7D',
    'fees_total30d': 'FEES_30D',
    'fees_totalAllTime': 'FEES_ALL_TIME',
    'dexs_total24h': 'DEX_VOLUME_24H',
    'dexs_total7d': 'DEX_VOLUME_7D',
    'derivatives_total24h': 'DERIVATIVES_VOLUME_24H',
    'aggregators_total24h': 'AGGREGATOR_VOLUME_24H',
    'bridges_lastDailyVolume': 'BRIDGE_VOLUME_24H',
    'stablecoins_circulating_peggedUSD': 'STABLECOIN_SUPPLY',
    'stablecoins_price': 'STABLECOIN_PRICE',
    'options_total24h': 'OPTIONS_VOLUME_24H',
    'options_dailyNotionalVolume': 'OPTIONS_NOTIONAL_24H',
}
```

## Implementation Files

1. **`defillama_pull.py`** - Main pull script (converts to long format)
2. **`entity_mapping.json`** - Artemis ID ↔ gecko_id mapping
3. **`metric_mapping.json`** - DefiLlama → standardized metric names

## Usage

```bash
# Pull DefiLlama data
python defillama_pull.py

# Output: defillama_latest_pull.csv
# Columns: asset, metric_name, value, pulled_at, source
```

## Combining Outputs

Both Artemis and DefiLlama outputs now have identical schema:

```
asset,metric_name,value,pulled_at,source
aave,FEES,1885566.55,2026-01-15 17:21:08,artemis
aave,TVL,14239037522.65,2026-01-19 13:22:40,defillama
```

### Simple Concatenation (Bash)
```bash
# Combine CSVs (skip header on second file)
cat artemis_latest_pull.csv > unified_metrics.csv
tail -n +2 defillama_latest_pull.csv >> unified_metrics.csv
```

### Database Insert (SQL)
```sql
-- Both can go into same table
INSERT INTO metrics_latest (asset, metric_name, value, pulled_at, source)
SELECT asset, metric_name, value::decimal, pulled_at::timestamp, source
FROM artemis_staging
UNION ALL
SELECT asset, metric_name, value::decimal, pulled_at::timestamp, source  
FROM defillama_staging;
```

### Cross-Source Query Example
```sql
-- Compare FEES from both sources for same asset
SELECT 
    a.asset,
    a.value as artemis_fees,
    d.value as defillama_fees,
    a.value - d.value as difference
FROM metrics_latest a
JOIN metrics_latest d ON a.asset = d.asset
WHERE a.source = 'artemis' AND a.metric_name = 'FEES'
  AND d.source = 'defillama' AND d.metric_name = 'FEES_24H';
```

## Entity Coverage

| Source | Entities with Data |
|--------|-------------------|
| Artemis | 287 |
| DefiLlama | 323 |
| **Overlap** (via gecko_id mapping) | ~180-200 |
| **Artemis-only** (mostly equities) | ~100 |
| **DefiLlama-only** | ~140 |

## Notes

1. **Timestamps**: Artemis includes `pulled_at`. DefiLlama data is point-in-time from their API (add timestamp at pull time).

2. **NULL handling**: Empty strings in CSVs should be treated as NULL in database.

3. **Duplicate metrics**: Some entities appear in multiple DefiLlama endpoints (e.g., Ethereum in both `protocols` and `chains`). The pull script uses the higher value.

4. **Rate limits**: DefiLlama has no published rate limits but recommend 1 req/sec for bulk pulls.
