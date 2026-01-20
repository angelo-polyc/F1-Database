# Velo.xyz Integration Package

Complete integration of Velo derivatives data into your crypto data pipeline.

## What's Included

```
velo_package/
├── README.md                           # This file
├── migrations/
│   ├── 001_schema_upgrade.sql          # Entity master + enhanced metrics schema
│   └── 002_entity_seed_data.sql        # Seed data for 481 coins
├── sources/
│   └── velo.py                         # VeloSource class
├── velo_config.csv                     # 1,069 coin-exchange pairs (>$1M OI)
└── backfill_velo.py                    # Historical data backfill script
```

## Quick Start

### 1. Run Schema Migration

```bash
# In Replit, connect to your Postgres and run:
psql $DATABASE_URL < migrations/001_schema_upgrade.sql
psql $DATABASE_URL < migrations/002_entity_seed_data.sql
```

Or via Python:
```python
import psycopg2
import os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

with open('migrations/001_schema_upgrade.sql', 'r') as f:
    cur.execute(f.read())
with open('migrations/002_entity_seed_data.sql', 'r') as f:
    cur.execute(f.read())

conn.commit()
cur.close()
conn.close()
```

### 2. Add Files to Your Project

```bash
# Copy source class
cp sources/velo.py your_project/sources/

# Copy config and backfill
cp velo_config.csv your_project/
cp backfill_velo.py your_project/
```

### 3. Register VeloSource

Edit `sources/__init__.py`:

```python
from sources.base import BaseSource
from sources.artemis import ArtemisSource
from sources.defillama import DefiLlamaSource
from sources.velo import VeloSource  # Add this

SOURCES = {
    "artemis": ArtemisSource,
    "defillama": DefiLlamaSource,
    "velo": VeloSource,  # Add this
}
```

### 4. Add API Key

In Replit Secrets:
- Key: `VELO_API_KEY`
- Value: `e1d6ee67e6724c0281c02e02b5c131d5`

### 5. Run Backfill

```bash
python backfill_velo.py --days 30
```

### 6. Update Scheduler

Add to `scheduler.py`:

```python
# At top with other constants
VELO_MINUTE = 10
last_velo_hour = None

# Add function
def should_run_velo(now_utc):
    global last_velo_hour
    if now_utc.minute >= VELO_MINUTE:
        current_hour = (now_utc.date(), now_utc.hour)
        if last_velo_hour != current_hour:
            return True
    return False

# In main loop
if should_run_velo(now_utc):
    print(f"[{now_utc.strftime('%H:%M')}] Running Velo hourly pull...")
    try:
        run_pull("velo")
        last_velo_hour = (now_utc.date(), now_utc.hour)
    except Exception as e:
        print(f"[Velo] Error: {e}")
```

---

## Schema Changes

### New Tables

**`entities`** - Canonical entity master
```sql
entity_id | canonical_id | name    | symbol | entity_type | sector
----------|--------------|---------|--------|-------------|--------
1         | bitcoin      | Bitcoin | BTC    | chain       | l1
2         | solana       | Solana  | SOL    | chain       | l1
3         | aave         | Aave    | AAVE   | protocol    | defi_lending
```

**`entity_source_ids`** - Maps source IDs to canonical entities
```sql
entity_id | source  | source_id | source_id_type
----------|---------|-----------|---------------
1         | artemis | btc       | ticker
1         | velo    | BTC       | coin
2         | artemis | sol       | ticker
2         | velo    | SOL       | coin
```

### Enhanced `metrics` Table

New columns added:
- `entity_id` - Links to canonical entity (nullable)
- `domain` - 'fundamental', 'trading', 'derivative', 'options'
- `exchange` - For per-exchange data (NULL for non-exchange sources)

New unique constraint:
```sql
UNIQUE (source, asset, metric_name, pulled_at, COALESCE(exchange, ''))
```

This allows storing BTC funding rate for Binance AND Bybit separately.

---

## Data Model

### Asset Identifier

Velo uses the coin symbol directly: `BTC`, `ETH`, `SOL`

The exchange is stored in a separate column, not concatenated into the asset.

### Metrics Stored

| Metric | Description |
|--------|-------------|
| `OPEN_PRICE`, `HIGH_PRICE`, `LOW_PRICE`, `CLOSE_PRICE` | OHLC prices |
| `COIN_VOLUME`, `DOLLAR_VOLUME` | Trading volume |
| `BUY_COIN_VOLUME`, `SELL_COIN_VOLUME` | Taker volume breakdown |
| `BUY_DOLLAR_VOLUME`, `SELL_DOLLAR_VOLUME` | Taker volume in USD |
| `BUY_TRADES`, `SELL_TRADES`, `TOTAL_TRADES` | Trade counts |
| `COIN_OI_HIGH`, `COIN_OI_LOW`, `COIN_OI_CLOSE` | Open interest in coins |
| `DOLLAR_OI_HIGH`, `DOLLAR_OI_LOW`, `DOLLAR_OI_CLOSE` | Open interest in USD |
| `FUNDING_RATE` | Funding rate (8h normalized) |
| `FUNDING_RATE_AVG` | Rolling average funding |
| `PREMIUM` | Spot-futures premium |
| `BUY_LIQUIDATIONS`, `SELL_LIQUIDATIONS` | Liquidation counts |
| `BUY_LIQ_COIN_VOL`, `SELL_LIQ_COIN_VOL` | Liquidation volume (coins) |
| `BUY_LIQ_DOLLAR_VOL`, `SELL_LIQ_DOLLAR_VOL` | Liquidation volume (USD) |
| `LIQ_COIN_VOL`, `LIQ_DOLLAR_VOL` | Total liquidation volume |

### Timestamp Granularity

Velo data is **hourly**, truncated to the hour:
- `2025-01-20 14:00:00` ✓
- `2025-01-20 14:30:00` ✗

This differs from Artemis/DefiLlama which are daily.

---

## Query Examples

### Funding rates across exchanges
```sql
SELECT exchange, pulled_at, value as funding_rate
FROM metrics
WHERE source = 'velo'
  AND asset = 'BTC'
  AND metric_name = 'FUNDING_RATE'
ORDER BY pulled_at DESC, exchange;
```

### Total OI for a coin (aggregated across exchanges)
```sql
SELECT 
    asset,
    pulled_at,
    SUM(value) as total_oi_usd
FROM metrics
WHERE source = 'velo'
  AND asset = 'SOL'
  AND metric_name = 'DOLLAR_OI_CLOSE'
GROUP BY asset, pulled_at
ORDER BY pulled_at DESC;
```

### Using entity master for cross-source queries
```sql
SELECT 
    e.canonical_id,
    m.source,
    m.metric_name,
    m.value,
    m.exchange
FROM metrics m
JOIN entity_source_ids esi ON m.source = esi.source AND m.asset = esi.source_id
JOIN entities e ON esi.entity_id = e.entity_id
WHERE e.canonical_id = 'bitcoin'
  AND m.domain = 'derivative'
ORDER BY m.pulled_at DESC;
```

### OI-weighted funding rate
```sql
WITH data AS (
    SELECT 
        asset,
        pulled_at,
        exchange,
        MAX(CASE WHEN metric_name = 'FUNDING_RATE' THEN value END) as funding,
        MAX(CASE WHEN metric_name = 'DOLLAR_OI_CLOSE' THEN value END) as oi
    FROM metrics
    WHERE source = 'velo' AND asset = 'BTC'
    GROUP BY asset, pulled_at, exchange
)
SELECT 
    asset,
    pulled_at,
    SUM(funding * oi) / NULLIF(SUM(oi), 0) as oi_weighted_funding,
    SUM(oi) as total_oi
FROM data
GROUP BY asset, pulled_at
ORDER BY pulled_at DESC;
```

---

## Storage Estimates

| Config | Pairs | Metrics | Rows/Hour | Rows/Day | Rows/Month |
|--------|-------|---------|-----------|----------|------------|
| Full (default) | 1,069 | 30 | 32,070 | 769,680 | 23M |
| Top 100 coins | ~300 | 30 | 9,000 | 216,000 | 6.5M |
| Top 50 coins | ~150 | 30 | 4,500 | 108,000 | 3.2M |

Estimated storage: ~150 bytes/row → Full config ≈ 3.5 GB/month

---

## Troubleshooting

### "VELO_API_KEY environment variable not set"
Add the API key to Replit Secrets.

### Rate limit errors
The code handles rate limits with exponential backoff. If persistent, increase `REQUEST_DELAY` in velo.py.

### "duplicate key value violates unique constraint"
This is expected during backfills - means data already exists. Uses `ON CONFLICT DO NOTHING`.

### Entity mappings not working
Run `migrations/002_entity_seed_data.sql` to populate entity_source_ids.

### Missing entity for a coin
Add to entities table:
```sql
INSERT INTO entities (canonical_id, name, symbol, entity_type, asset_class, sector)
VALUES ('newcoin', 'New Coin', 'NEWCOIN', 'token', 'crypto', 'other');

INSERT INTO entity_source_ids (entity_id, source, source_id, source_id_type)
SELECT entity_id, 'velo', 'NEWCOIN', 'coin' FROM entities WHERE symbol = 'NEWCOIN';
```

---

## API Reference

**Base URL:** `https://api.velo.xyz/api/v1`

**Auth:** HTTP Basic (username: `api`, password: API_KEY)

**Rate Limit:** 120 requests per 30 seconds

**Endpoints:**
- `/futures` - List available futures products
- `/rows` - Get time series data

**Example:**
```bash
curl --user "api:YOUR_KEY" \
  "https://api.velo.xyz/api/v1/rows?type=futures&exchanges=binance-futures&coins=BTC&columns=funding_rate&begin=1705700000000&end=1705800000000&resolution=1h"
```
