"""
Velo.xyz Data Source

Pulls hourly futures derivatives data (funding, OI, liquidations) from Velo API.
Integrates with entity master table and uses domain='derivative'.
"""

import os
import csv
import time
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.auth import HTTPBasicAuth
from sources.base import BaseSource
from db.setup import get_connection

# Rate limit: 120 requests per 30 seconds = 4/sec max
# Use 2/sec to stay safely under limit
REQUEST_DELAY = 0.5  # 2 req/sec to avoid rate limits

# Velo API returns 22,500 values max per request
# API returns ~120 hours (5 days) of data regardless of begin/end params
# 30 columns * 120 rows = 3600 cells per coin per exchange
# 22,500 / 3600 = 6 coins max per request
BATCH_SIZE = 5

# Essential metrics for quick pulls (used when catching up large gaps)
ESSENTIAL_COLUMNS = [
    'close_price', 'dollar_volume', 'dollar_open_interest_close',
    'funding_rate', 'liquidations_dollar_volume'
]

# All futures columns to pull (used for normal hourly operation)
FUTURES_COLUMNS = [
    'open_price', 'high_price', 'low_price', 'close_price',
    'coin_volume', 'dollar_volume',
    'buy_coin_volume', 'sell_coin_volume',
    'buy_dollar_volume', 'sell_dollar_volume',
    'buy_trades', 'sell_trades', 'total_trades',
    'coin_open_interest_high', 'coin_open_interest_low', 'coin_open_interest_close',
    'dollar_open_interest_high', 'dollar_open_interest_low', 'dollar_open_interest_close',
    'funding_rate', 'funding_rate_avg', 'premium',
    'buy_liquidations', 'sell_liquidations',
    'buy_liquidations_coin_volume', 'sell_liquidations_coin_volume',
    'buy_liquidations_dollar_volume', 'sell_liquidations_dollar_volume',
    'liquidations_coin_volume', 'liquidations_dollar_volume'
]

# Map Velo column names to standardized DB metric names
METRIC_MAP = {
    'open_price': 'OPEN_PRICE',
    'high_price': 'HIGH_PRICE',
    'low_price': 'LOW_PRICE',
    'close_price': 'CLOSE_PRICE',
    'coin_volume': 'COIN_VOLUME',
    'dollar_volume': 'DOLLAR_VOLUME',
    'buy_coin_volume': 'BUY_COIN_VOLUME',
    'sell_coin_volume': 'SELL_COIN_VOLUME',
    'buy_dollar_volume': 'BUY_DOLLAR_VOLUME',
    'sell_dollar_volume': 'SELL_DOLLAR_VOLUME',
    'buy_trades': 'BUY_TRADES',
    'sell_trades': 'SELL_TRADES',
    'total_trades': 'TOTAL_TRADES',
    'coin_open_interest_high': 'COIN_OI_HIGH',
    'coin_open_interest_low': 'COIN_OI_LOW',
    'coin_open_interest_close': 'COIN_OI_CLOSE',
    'dollar_open_interest_high': 'DOLLAR_OI_HIGH',
    'dollar_open_interest_low': 'DOLLAR_OI_LOW',
    'dollar_open_interest_close': 'DOLLAR_OI_CLOSE',
    'funding_rate': 'FUNDING_RATE',
    'funding_rate_avg': 'FUNDING_RATE_AVG',
    'premium': 'PREMIUM',
    'buy_liquidations': 'BUY_LIQUIDATIONS',
    'sell_liquidations': 'SELL_LIQUIDATIONS',
    'buy_liquidations_coin_volume': 'BUY_LIQ_COIN_VOL',
    'sell_liquidations_coin_volume': 'SELL_LIQ_COIN_VOL',
    'buy_liquidations_dollar_volume': 'BUY_LIQ_DOLLAR_VOL',
    'sell_liquidations_dollar_volume': 'SELL_LIQ_DOLLAR_VOL',
    'liquidations_coin_volume': 'LIQ_COIN_VOL',
    'liquidations_dollar_volume': 'LIQ_DOLLAR_VOL',
}

EXCHANGES = ['binance-futures', 'bybit', 'okex-swap', 'hyperliquid']


class VeloSource(BaseSource):
    """
    Velo.xyz derivatives data source.
    
    Pulls hourly futures data including funding rates, open interest,
    liquidations, and OHLCV for all configured coin-exchange pairs.
    
    Uses enhanced metrics schema with:
    - domain='derivative' 
    - exchange column for per-exchange data
    - entity_id linking to entity master
    """
    
    @property
    def source_name(self) -> str:
        return "velo"
    
    def __init__(self, config_path: str = "velo_config.csv"):
        self.api_key = os.environ.get("VELO_API_KEY")
        if not self.api_key:
            raise ValueError("VELO_API_KEY environment variable not set")
        self.config_path = config_path
        self.base_url = "https://api.velo.xyz/api/v1"
        self.auth = HTTPBasicAuth("api", self.api_key)
        self._entity_cache = None  # Cache for entity_id lookups
    
    def pull(self) -> int:
        """
        Pull data for all configured coin-exchange pairs using timestamp-based fetching.
        Only fetches data newer than the last recorded timestamp per pair.
        Returns number of records inserted.
        """
        print(f"[Velo] Starting timestamp-based pull...")
        
        # Load entity mappings
        self._load_entity_cache()
        
        # Load configuration
        config = self.load_config()
        if not config:
            print("[Velo] No coin-exchange pairs configured")
            self.log_pull("no_data", 0)
            return 0
        
        # Get last timestamps per coin-exchange pair from database
        last_timestamps = self._get_last_timestamps()
        print(f"[Velo] Found last timestamps for {len(last_timestamps)} coin-exchange pairs")
        
        # Current time as end
        now = datetime.now(timezone.utc)
        end_ts = int(now.timestamp() * 1000)
        
        # Default begin: 2 hours ago for pairs with no history
        default_begin = end_ts - (2 * 60 * 60 * 1000)
        
        # Build fetch tasks with per-pair begin timestamps
        # Group pairs by exchange and begin timestamp for efficient batching
        fetch_tasks = []
        for item in config:
            coin = item['coin']
            exchange = item['exchange']
            pair_key = (coin, exchange)
            
            # Use last timestamp + 1 hour if exists, else default
            if pair_key in last_timestamps:
                last_ts = last_timestamps[pair_key]
                # Start from 1 hour after last recorded to avoid overlap
                begin_ts = int((last_ts.timestamp() + 3600) * 1000)
                # Skip if already up to date (begin would be in the future)
                if begin_ts >= end_ts:
                    continue
            else:
                begin_ts = default_begin
            
            fetch_tasks.append((exchange, coin, begin_ts, end_ts))
        
        if not fetch_tasks:
            print("[Velo] All pairs are up to date, nothing to fetch")
            self.log_pull("success", 0)
            return 0
        
        # Group by (exchange, begin_ts) for efficient batching
        # In normal hourly operation, all pairs share the same begin_ts
        from collections import defaultdict
        groups = defaultdict(list)
        for exchange, coin, begin_ts, end_ts_pair in fetch_tasks:
            groups[(exchange, begin_ts, end_ts_pair)].append(coin)
        
        # Build batched fetch tasks (5 coins per batch)
        batched_tasks = []
        for (exchange, begin_ts, end_ts_pair), coins in groups.items():
            for i in range(0, len(coins), BATCH_SIZE):
                batch = coins[i:i+BATCH_SIZE]
                batched_tasks.append((exchange, batch, begin_ts, end_ts_pair))
        
        print(f"[Velo] Fetching {len(fetch_tasks)} pairs in {len(batched_tasks)} batches...")
        
        all_records = []
        completed = 0
        
        # Sequential fetch with batched API calls
        for exchange, coins_batch, begin_ts, end_ts_pair in batched_tasks:
            try:
                records = self._fetch_batch(exchange, coins_batch, begin_ts, end_ts_pair)
                all_records.extend(records)
                completed += 1
                if completed % 50 == 0:
                    print(f"[Velo] Progress: {completed}/{len(batched_tasks)} batches fetched...")
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                print(f"[Velo] Error fetching {exchange}: {e}")
        
        # Deduplicate records before insert
        if all_records:
            seen = set()
            unique_records = []
            for r in all_records:
                key = (r['pulled_at'], r['asset'], r['metric_name'], r.get('exchange', ''))
                if key not in seen:
                    seen.add(key)
                    unique_records.append(r)
            
            print(f"[Velo] Deduplicated {len(all_records)} -> {len(unique_records)} records")
            total = self._insert_metrics(unique_records)
            print(f"[Velo] Inserted {total} new records")
            self.log_pull("success", total)
            return total
        else:
            print("[Velo] No new data received")
            self.log_pull("no_data", 0)
            return 0
    
    def _get_last_timestamps(self) -> dict:
        """Get the last recorded timestamp for each coin-exchange pair."""
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT asset, exchange, MAX(pulled_at) as last_ts
            FROM metrics
            WHERE source = 'velo' AND exchange IS NOT NULL
            GROUP BY asset, exchange
        """)
        
        result = {}
        for row in cur.fetchall():
            result[(row[0], row[1])] = row[2]
        
        cur.close()
        conn.close()
        return result
    
    def _fetch_batch(self, exchange: str, coins: list, begin_ts: int, end_ts: int) -> list:
        """Fetch data for a batch of coins from a single exchange.
        
        Splits column requests into chunks of 10 to stay under Velo's 22,500 cell limit.
        """
        records = []
        coins_str = ','.join(coins)
        
        # API limit: 22,500 cells max per request
        # Calculate time range to decide which columns to fetch
        time_range_hours = (end_ts - begin_ts) / (1000 * 60 * 60)
        estimated_rows = int(time_range_hours * 60)  # minute-level data
        num_coins = len(coins)
        
        # For large gaps (>6 hours), use essential columns only for speed
        # For normal hourly operation (<6 hours), use all columns
        if time_range_hours > 6:
            columns_to_fetch = ESSENTIAL_COLUMNS
        else:
            columns_to_fetch = FUTURES_COLUMNS
        
        # Calculate optimal column batch size
        if estimated_rows > 0 and num_coins > 0:
            max_columns = int(20000 / (estimated_rows * num_coins))
            COLUMN_BATCH_SIZE = max(2, min(len(columns_to_fetch), max_columns))
        else:
            COLUMN_BATCH_SIZE = len(columns_to_fetch)
        
        column_batches = [
            columns_to_fetch[i:i+COLUMN_BATCH_SIZE] 
            for i in range(0, len(columns_to_fetch), COLUMN_BATCH_SIZE)
        ]
        
        # Collect all parsed rows, keyed by (time, coin, exchange)
        row_data = {}  # key: (ts_hour, coin, exchange) -> {metric: value, ...}
        
        for col_batch in column_batches:
            columns_str = ','.join(col_batch)
            
            url = (
                f"{self.base_url}/rows?"
                f"type=futures&"
                f"exchanges={exchange}&"
                f"coins={coins_str}&"
                f"columns={columns_str}&"
                f"begin={begin_ts}&"
                f"end={end_ts}&"
                f"resolution=1h"
            )
            
            try:
                resp = requests.get(url, auth=self.auth, timeout=60)
                
                if resp.status_code == 429:
                    time.sleep(5)
                    resp = requests.get(url, auth=self.auth, timeout=60)
                
                if resp.status_code != 200:
                    continue
                
                lines = resp.text.strip().split('\n')
                if len(lines) < 2:
                    continue
                
                headers = lines[0].split(',')
                
                for line in lines[1:]:
                    values = line.split(',')
                    if len(values) != len(headers):
                        continue
                    
                    row = dict(zip(headers, values))
                    
                    try:
                        ts_ms = int(row['time'])
                        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                        ts_hour = ts.replace(minute=0, second=0, microsecond=0)
                    except (ValueError, KeyError):
                        continue
                    
                    coin_val = row['coin']
                    exch = row['exchange']
                    key = (ts_hour, coin_val, exch)
                    
                    if key not in row_data:
                        row_data[key] = {}
                    
                    # Store metric values for this row
                    for velo_col in col_batch:
                        if velo_col in row and row[velo_col]:
                            try:
                                row_data[key][velo_col] = float(row[velo_col])
                            except ValueError:
                                pass
                
                time.sleep(REQUEST_DELAY)
                
            except requests.RequestException as e:
                print(f"[Velo] Request error for {exchange}: {e}")
        
        # Convert collected data to records
        for (ts_hour, coin_val, exch), metrics in row_data.items():
            entity_id = self._entity_cache.get(coin_val) if self._entity_cache else None
            
            for velo_col, value in metrics.items():
                db_metric = METRIC_MAP.get(velo_col)
                if db_metric:
                    records.append({
                        'pulled_at': ts_hour,
                        'asset': coin_val,
                        'entity_id': entity_id,
                        'metric_name': db_metric,
                        'value': value,
                        'domain': 'derivative',
                        'exchange': exch,
                        'granularity': 'hourly'
                    })
        
        return records
    
    def load_config(self) -> list:
        """Load coin-exchange pairs from CSV config."""
        if not os.path.exists(self.config_path):
            print(f"[Velo] Config file not found: {self.config_path}")
            return []
        
        config = []
        with open(self.config_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('pull', '1') == '1':
                    config.append({
                        'coin': row['coin'].upper(),
                        'exchange': row['exchange'].lower()
                    })
        
        print(f"[Velo] Loaded {len(config)} coin-exchange pairs from config")
        return config
    
    def _load_entity_cache(self):
        """Load entity_id mappings from database."""
        self._entity_cache = {}
        
        try:
            conn = get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                SELECT source_id, entity_id 
                FROM entity_source_ids 
                WHERE source = 'velo'
            """)
            
            for row in cur.fetchall():
                self._entity_cache[row[0]] = row[1]
            
            cur.close()
            conn.close()
            print(f"[Velo] Loaded {len(self._entity_cache)} entity mappings")
        except Exception as e:
            print(f"[Velo] Warning: Could not load entity cache: {e}")
            self._entity_cache = {}
    
    def _insert_metrics(self, records: list) -> int:
        """
        Insert records with hourly timestamps using upsert.
        
        Uses enhanced schema with domain, exchange, and entity_id.
        """
        if not records:
            return 0
        
        conn = get_connection()
        cur = conn.cursor()
        
        # Use execute_values for efficient bulk insert
        from psycopg2.extras import execute_values
        
        # Prepare rows: (pulled_at, source, asset, entity_id, metric_name, value, domain, exchange, granularity)
        rows = [
            (
                r['pulled_at'], 
                self.source_name, 
                r['asset'], 
                r.get('entity_id'),  # May be None
                r['metric_name'], 
                r['value'],
                r.get('domain', 'derivative'),
                r.get('exchange'),
                r.get('granularity', 'hourly')  # Default to hourly for Velo
            )
            for r in records
        ]
        
        # Upsert query with new columns including granularity
        query = """
            INSERT INTO metrics (pulled_at, source, asset, entity_id, metric_name, value, domain, exchange, granularity)
            VALUES %s
            ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) 
            DO UPDATE SET 
                value = EXCLUDED.value,
                entity_id = COALESCE(EXCLUDED.entity_id, metrics.entity_id),
                domain = COALESCE(EXCLUDED.domain, metrics.domain),
                granularity = COALESCE(EXCLUDED.granularity, metrics.granularity)
        """
        
        try:
            execute_values(cur, query, rows, page_size=1000)
            inserted = cur.rowcount
            conn.commit()
        except Exception as e:
            print(f"[Velo] Database error: {e}")
            conn.rollback()
            inserted = 0
        finally:
            cur.close()
            conn.close()
        
        return inserted


def fetch_available_products() -> dict:
    """
    Utility function to fetch all available futures products from Velo.
    Returns dict of {exchange: [products]}
    
    Usage:
        products = fetch_available_products()
        for ex, prods in products.items():
            print(f"{ex}: {len(prods)} products")
    """
    url = "https://api.velo.xyz/api/v1/futures"
    
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            return {}
        
        data = resp.json()
        by_exchange = {}
        
        for item in data:
            ex = item.get('exchange', 'unknown')
            if ex not in by_exchange:
                by_exchange[ex] = []
            by_exchange[ex].append({
                'coin': item.get('coin'),
                'product': item.get('product'),
                'begin': item.get('begin')
            })
        
        return by_exchange
    
    except Exception as e:
        print(f"Error fetching products: {e}")
        return {}


def update_entity_mappings():
    """
    Utility to sync Velo coins to entity_source_ids table.
    Run this when adding new coins to track.
    """
    products = fetch_available_products()
    
    all_coins = set()
    for exchange, prods in products.items():
        for p in prods:
            all_coins.add(p['coin'])
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get existing mappings
    cur.execute("SELECT source_id FROM entity_source_ids WHERE source = 'velo'")
    existing = set(row[0] for row in cur.fetchall())
    
    # Find new coins
    new_coins = all_coins - existing
    
    if new_coins:
        print(f"Found {len(new_coins)} new coins to add")
        # Would need to first add to entities table, then add mappings
        # For now, just report
        for coin in sorted(new_coins)[:20]:
            print(f"  {coin}")
        if len(new_coins) > 20:
            print(f"  ... and {len(new_coins) - 20} more")
    else:
        print("All coins already mapped")
    
    cur.close()
    conn.close()
