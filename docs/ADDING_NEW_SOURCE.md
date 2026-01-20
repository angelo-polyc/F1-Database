# Adding a New Data Source

This guide explains how to add a new API source to the data pipeline.

## Step 1: Create the Source Class

Create a new file `sources/<sourcename>.py`:

```python
import os
import csv
import time
import requests
from sources.base import BaseSource

REQUEST_DELAY = 0.12  # Adjust based on API rate limits

class NewSource(BaseSource):
    
    @property
    def source_name(self) -> str:
        """Must return a unique lowercase identifier for this source."""
        return "newsource"
    
    def __init__(self):
        self.api_key = os.environ.get("NEWSOURCE_API_KEY")
        if not self.api_key:
            raise ValueError("NEWSOURCE_API_KEY environment variable not set")
        self.config_path = "newsource_config.csv"
        self.base_url = "https://api.newsource.com"
    
    def pull(self) -> int:
        """
        Main entry point for live data pulls.
        Must return the number of records inserted.
        """
        # 1. Load configuration
        config = self.load_config()
        
        # 2. Fetch data from API
        all_records = []
        for asset in config['assets']:
            data = self.fetch_asset_data(asset)
            
            for metric_name, value in data.items():
                if value is not None:
                    all_records.append({
                        "asset": asset,
                        "metric_name": metric_name,
                        "value": float(value)
                    })
            
            time.sleep(REQUEST_DELAY)
        
        # 3. Insert into database (inherited method)
        total_records = self.insert_metrics(all_records)
        
        # 4. Log the pull (inherited method)
        status = "success" if total_records > 0 else "no_data"
        self.log_pull(status, total_records)
        
        return total_records
    
    def load_config(self) -> dict:
        """Load assets/metrics from CSV config file."""
        # Implementation depends on your config format
        pass
    
    def fetch_asset_data(self, asset: str) -> dict:
        """Fetch data for a single asset from the API."""
        url = f"{self.base_url}/data/{asset}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            print(f"Error fetching {asset}: {e}")
        
        return {}
```

## Step 2: Register the Source

Edit `sources/__init__.py`:

```python
from sources.base import BaseSource
from sources.artemis import ArtemisSource
from sources.defillama import DefiLlamaSource
from sources.newsource import NewSource  # Add import

SOURCES = {
    "artemis": ArtemisSource,
    "defillama": DefiLlamaSource,
    "newsource": NewSource,  # Add to registry
}

def get_source(name):
    if name not in SOURCES:
        raise ValueError(f"Unknown source: {name}. Available: {list(SOURCES.keys())}")
    return SOURCES[name]()
```

## Step 3: Create Configuration File

Create `newsource_config.csv` with your assets. Format depends on your needs:

**Simple format (one asset per row):**
```csv
asset,pull
bitcoin,1
ethereum,1
solana,1
```

**Matrix format (assets × metrics):**
```csv
asset,Pull,PRICE,TVL,VOLUME
bitcoin,1,1,1,1
ethereum,1,1,1,0
solana,1,1,0,1
```

## Step 4: Create Backfill Script

Create `backfill_newsource.py`:

```python
import os
import time
import requests
import psycopg2
from datetime import datetime

REQUEST_DELAY = 0.08
BATCH_SIZE = 1000

DATABASE_URL = os.environ.get("DATABASE_URL")
API_KEY = os.environ.get("NEWSOURCE_API_KEY")

def get_connection():
    return psycopg2.connect(DATABASE_URL)

def fetch_historical(asset: str) -> list:
    """Fetch historical data for an asset."""
    records = []
    url = f"https://api.newsource.com/history/{asset}"
    
    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            for entry in data:
                records.append({
                    'asset': asset,
                    'metric_name': entry['metric'],
                    'value': float(entry['value']),
                    'pulled_at': datetime.utcfromtimestamp(entry['timestamp'])
                })
    except Exception as e:
        print(f"Error: {e}")
    
    return records

def insert_batch(records: list):
    """Insert records with ON CONFLICT DO NOTHING for backfills."""
    if not records:
        return 0
    
    conn = get_connection()
    cur = conn.cursor()
    
    rows = [(r['pulled_at'], 'newsource', r['asset'], r['metric_name'], r['value']) 
            for r in records]
    
    cur.executemany("""
        INSERT INTO metrics (pulled_at, source, asset, metric_name, value)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (source, asset, metric_name, pulled_at) DO NOTHING
    """, rows)
    
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    
    return inserted

def main():
    print("=" * 60)
    print("NEWSOURCE HISTORICAL BACKFILL")
    print("=" * 60)
    
    # Load assets from config
    assets = load_assets_from_config()
    
    total_records = 0
    for i, asset in enumerate(assets):
        print(f"[{i+1:3d}/{len(assets)}] {asset}...", end=" ", flush=True)
        
        records = fetch_historical(asset)
        inserted = insert_batch(records)
        total_records += inserted
        
        print(f"+{inserted:5d} records")
        time.sleep(REQUEST_DELAY)
    
    print("=" * 60)
    print(f"Total records: {total_records}")
    print("=" * 60)

if __name__ == "__main__":
    main()
```

## Step 5: Update Scheduler (Optional)

If your source needs scheduled pulls, edit `scheduler.py`:

```python
# Add schedule constants
NEWSOURCE_HOUR = 6  # Run at 06:XX UTC
NEWSOURCE_MINUTE = 5

# Add tracking variable
last_newsource_date = None

# Add check function
def should_run_newsource(now_utc):
    global last_newsource_date
    if now_utc.hour == NEWSOURCE_HOUR and now_utc.minute >= NEWSOURCE_MINUTE:
        today = now_utc.date()
        if last_newsource_date != today:
            return True
    return False

# Add to main loop
if should_run_newsource(now_utc):
    run_pull("newsource")
    last_newsource_date = now_utc.date()

# Add to backfill section
run_backfill("newsource")
```

## Step 6: Add Secret

Add the API key as a secret:
- Key: `NEWSOURCE_API_KEY`
- Value: Your API key

## Inherited Methods from BaseSource

Your source class inherits these methods:

### `insert_metrics(records: list) -> int`
Inserts records into the database with upsert behavior.

**Input format:**
```python
records = [
    {"asset": "bitcoin", "metric_name": "PRICE", "value": 45000.0},
    {"asset": "bitcoin", "metric_name": "TVL", "value": 1000000.0},
]
```

**Behavior:**
- Truncates timestamp to midnight UTC
- Uses `ON CONFLICT DO UPDATE` (upsert)
- Returns count of inserted records

### `log_pull(status: str, records_count: int) -> int`
Logs the pull operation to the `pulls` table.

**Status values:** `"success"`, `"error"`, `"no_data"`

## Best Practices

1. **Rate limiting**: Always include `REQUEST_DELAY` and respect API limits
2. **Error handling**: Catch and log errors, don't crash on single failures
3. **Metric naming**: Use uppercase, descriptive names (e.g., `TVL`, `FEES_24H`)
4. **Asset IDs**: Document your ID format (slug, ticker, CoinGecko ID, etc.)
5. **Batch operations**: For large datasets, batch API calls and database inserts
6. **Idempotency**: Backfills should use `DO NOTHING`, live pulls use `DO UPDATE`
7. **Progress output**: Print progress so operators can monitor long runs
