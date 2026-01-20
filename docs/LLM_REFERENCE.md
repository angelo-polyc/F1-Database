# LLM Reference: Crypto Data Pipeline

This is a comprehensive reference document for an LLM to understand and work on this data pipeline codebase.

## Quick Start for Adding a New Source

1. Create `sources/<name>.py` extending `BaseSource`
2. Register in `sources/__init__.py`
3. Create `<name>_config.csv` for configuration
4. Create `backfill_<name>.py` for historical data
5. (Optional) Add to `scheduler.py` for automated pulls

## Core Files Reference

### `sources/base.py` - Abstract Base Class

```python
from abc import ABC, abstractmethod
from datetime import datetime
from db.setup import get_connection

class BaseSource(ABC):
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return unique lowercase identifier (e.g., 'artemis', 'defillama')"""
        pass
    
    @abstractmethod
    def pull(self) -> int:
        """Fetch current data from API and insert. Return record count."""
        pass
    
    def log_pull(self, status: str, records_count: int) -> int:
        """Log pull to 'pulls' table. Status: 'success', 'error', 'no_data'"""
        # Inserts into pulls table with timestamp
        pass
    
    def insert_metrics(self, records: list) -> int:
        """
        Insert records with upsert behavior.
        
        Records format:
        [{"asset": "bitcoin", "metric_name": "PRICE", "value": 45000.0}, ...]
        
        - Truncates timestamp to midnight UTC
        - Uses ON CONFLICT DO UPDATE (upsert)
        """
        pass
```

### `sources/__init__.py` - Source Registry

```python
from sources.base import BaseSource
from sources.artemis import ArtemisSource
from sources.defillama import DefiLlamaSource

SOURCES = {
    "artemis": ArtemisSource,
    "defillama": DefiLlamaSource,
}

def get_source(name):
    """Get source instance by name. Raises ValueError if not found."""
    if name not in SOURCES:
        raise ValueError(f"Unknown source: {name}. Available: {list(SOURCES.keys())}")
    return SOURCES[name]()
```

### `db/setup.py` - Database Schema

```python
def get_connection():
    """Return psycopg2 connection using DATABASE_URL env var."""
    return psycopg2.connect(DATABASE_URL)

def setup_database():
    """Create tables and indexes if not exist."""
    # Creates: pulls, metrics tables
    # Creates: idx_metrics_unique on (source, asset, metric_name, pulled_at)
```

## Database Schema

```sql
CREATE TABLE pulls (
    pull_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    pulled_at TIMESTAMP NOT NULL DEFAULT NOW(),
    status VARCHAR(50) NOT NULL,
    records_count INTEGER DEFAULT 0
);

CREATE TABLE metrics (
    id SERIAL PRIMARY KEY,
    pulled_at TIMESTAMP NOT NULL,
    source VARCHAR(100) NOT NULL,
    asset VARCHAR(100) NOT NULL,
    metric_name VARCHAR(200) NOT NULL,
    value DOUBLE PRECISION
);

CREATE UNIQUE INDEX idx_metrics_unique 
ON metrics (source, asset, metric_name, pulled_at);
```

## Duplicate Handling

| Context | SQL | Purpose |
|---------|-----|---------|
| Live pulls | `ON CONFLICT DO UPDATE SET value = EXCLUDED.value` | Update today's value |
| Backfills | `ON CONFLICT DO NOTHING` | Preserve historical data |

## Standard Patterns

### Live Pull Pattern

```python
class NewSource(BaseSource):
    def pull(self) -> int:
        # 1. Load config
        config = self.load_config()
        
        # 2. Fetch from API
        all_records = []
        for entity in config:
            data = self.fetch_data(entity)
            for metric, value in data.items():
                all_records.append({
                    "asset": entity['id'],
                    "metric_name": metric,
                    "value": float(value)
                })
            time.sleep(REQUEST_DELAY)
        
        # 3. Insert (inherited method handles timestamp and upsert)
        total = self.insert_metrics(all_records)
        
        # 4. Log the pull
        self.log_pull("success" if total > 0 else "no_data", total)
        
        return total
```

### Backfill Pattern

```python
def insert_historical(records):
    """Backfill insert with DO NOTHING."""
    conn = get_connection()
    cur = conn.cursor()
    
    rows = [(r['pulled_at'], 'source_name', r['asset'], r['metric_name'], r['value']) 
            for r in records]
    
    cur.executemany("""
        INSERT INTO metrics (pulled_at, source, asset, metric_name, value)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (source, asset, metric_name, pulled_at) DO NOTHING
    """, rows)
    
    conn.commit()
    cur.close()
    conn.close()
```

### Rate Limiting Pattern

```python
REQUEST_DELAY = 0.12  # seconds between requests

# With retry on 429
def fetch_with_retry(url, max_retries=3):
    for attempt in range(max_retries):
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            wait = 2 ** (attempt + 1)
            time.sleep(wait)
        else:
            return None
    return None
```

## Config File Formats

### Simple List Format
```csv
gecko_id,name,slug,Pull
solana,Solana,solana,1
ethereum,Ethereum,ethereum,1
aave,Aave,aave,1
```

### Matrix Format (assets × metrics)
```csv
asset,Pull,PRICE,TVL,FEES,REVENUE
bitcoin,1,1,1,1,1
ethereum,1,1,1,1,0
solana,1,1,0,1,1
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| DATABASE_URL | PostgreSQL connection string |
| ARTEMIS_API_KEY | Artemis API key |
| DEFILLAMA_API_KEY | DefiLlama Pro API key |
| <SOURCE>_API_KEY | Pattern for new sources |

## Scheduler Integration

To add scheduled pulls for a new source, edit `scheduler.py`:

```python
# Add constants
NEWSOURCE_HOUR = 6
NEWSOURCE_MINUTE = 5
last_newsource_date = None

# Add check function
def should_run_newsource(now_utc):
    global last_newsource_date
    if now_utc.hour == NEWSOURCE_HOUR and now_utc.minute >= NEWSOURCE_MINUTE:
        if last_newsource_date != now_utc.date():
            return True
    return False

# In main loop
if should_run_newsource(now_utc):
    run_pull("newsource")
    last_newsource_date = now_utc.date()

# Add to backfill section
run_backfill("newsource")
```

## CLI Usage

```bash
python main.py setup              # Initialize database
python main.py pull artemis       # Pull from Artemis
python main.py pull defillama     # Pull from DefiLlama
python main.py sources            # List available sources
python main.py query              # Interactive query mode
python backfill_artemis.py        # Run Artemis backfill
python backfill_defillama.py      # Run DefiLlama backfill
python scheduler.py               # Run scheduler
python scheduler.py --fresh       # Clear data and restart
```

## Common Gotchas

1. **Asset ID differences**: Artemis uses `sol`, DefiLlama uses `solana`
2. **Metric naming**: Always uppercase (e.g., `TVL`, `FEES_24H`)
3. **Timestamp handling**: Live pulls truncate to midnight, backfills preserve original
4. **Chain detection**: DefiLlama auto-detects chains vs protocols via /chains API
5. **Rate limits**: DefiLlama Pro = 1000/min, Artemis = conservative 8/sec
6. **Pro API**: Some endpoints require API key in URL or as Bearer token

## Metric Naming Conventions

| Pattern | Example | Description |
|---------|---------|-------------|
| Simple | `TVL`, `PRICE`, `FEES` | Point-in-time values |
| 24H suffix | `FEES_24H`, `VOLUME_24H` | 24-hour aggregates |
| CHAIN_ prefix | `CHAIN_TVL`, `CHAIN_FEES` | Chain-level metrics |
| Native suffix | `SUPPLY_NATIVE` | Non-USD denominated |

## Testing a New Source

1. Run setup: `python main.py setup`
2. Test pull: `python main.py pull <source>`
3. Check data: `python main.py query`
4. Test backfill: `python backfill_<source>.py`
5. Verify: `SELECT COUNT(*), source FROM metrics GROUP BY source;`
