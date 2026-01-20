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
from requests.auth import HTTPBasicAuth
from sources.base import BaseSource
from db.setup import get_connection

# Rate limit: 120 requests per 30 seconds = 4/sec max
# Use conservative 0.3s delay (3.3 req/sec)
REQUEST_DELAY = 0.3

# Velo API returns 22,500 values max per request
# For live pulls (2 hours of data), we can batch ~25 coins safely
# 30 columns * 2 hours * 25 coins = 1,500 values per batch
BATCH_SIZE = 5  # Reduced from 25 - API limit is 22500 cells per request

# All futures columns to pull
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
        Pull current hour's data for all configured coin-exchange pairs.
        Returns number of records inserted.
        """
        print(f"[Velo] Starting hourly pull...")
        
        # Load entity mappings
        self._load_entity_cache()
        
        # Load configuration
        config = self.load_config()
        if not config:
            print("[Velo] No coin-exchange pairs configured")
            self.log_pull("no_data", 0)
            return 0
        
        # Group by exchange for efficient batching
        by_exchange = {}
        for item in config:
            ex = item['exchange']
            if ex not in by_exchange:
                by_exchange[ex] = []
            by_exchange[ex].append(item['coin'])
        
        # Time range: last 2 hours to ensure we get complete hour
        now = datetime.now(timezone.utc)
        end_ts = int(now.timestamp() * 1000)
        begin_ts = end_ts - (2 * 60 * 60 * 1000)  # 2 hours ago
        
        all_records = []
        
        for exchange, coins in by_exchange.items():
            print(f"[Velo] Fetching {len(coins)} coins from {exchange}...")
            
            # Batch coins
            for i in range(0, len(coins), BATCH_SIZE):
                batch = coins[i:i+BATCH_SIZE]
                records = self._fetch_batch(exchange, batch, begin_ts, end_ts)
                all_records.extend(records)
                
                if i + BATCH_SIZE < len(coins):
                    time.sleep(REQUEST_DELAY)
            
            time.sleep(REQUEST_DELAY)
        
        # Deduplicate records before insert (same coin/exchange/metric/timestamp)
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
            print(f"[Velo] Inserted {total} records")
            self.log_pull("success", total)
            return total
        else:
            print("[Velo] No data received")
            self.log_pull("no_data", 0)
            return 0
    
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
    
    def _fetch_batch(self, exchange: str, coins: list, begin_ts: int, end_ts: int) -> list:
        """Fetch data for a batch of coins from a single exchange."""
        records = []
        
        coins_str = ','.join(coins)
        columns_str = ','.join(FUTURES_COLUMNS)
        
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
                print(f"[Velo] Rate limited, waiting 5s...")
                time.sleep(5)
                resp = requests.get(url, auth=self.auth, timeout=60)
            
            if resp.status_code != 200:
                print(f"[Velo] Error {resp.status_code}: {resp.text[:200]}")
                return []
            
            # Parse CSV response
            lines = resp.text.strip().split('\n')
            if len(lines) < 2:
                return []
            
            headers = lines[0].split(',')
            
            for line in lines[1:]:
                values = line.split(',')
                if len(values) != len(headers):
                    continue
                
                row = dict(zip(headers, values))
                
                # Parse timestamp
                try:
                    ts_ms = int(row['time'])
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    # Truncate to hour
                    ts_hour = ts.replace(minute=0, second=0, microsecond=0)
                except (ValueError, KeyError):
                    continue
                
                coin = row['coin']
                exch = row['exchange']
                
                # Look up entity_id
                entity_id = self._entity_cache.get(coin) if self._entity_cache else None
                
                # Extract all metrics
                for velo_col, db_metric in METRIC_MAP.items():
                    if velo_col in row and row[velo_col]:
                        try:
                            value = float(row[velo_col])
                            records.append({
                                'pulled_at': ts_hour,
                                'asset': coin,
                                'entity_id': entity_id,
                                'metric_name': db_metric,
                                'value': value,
                                'domain': 'derivative',
                                'exchange': exch,
                                'granularity': 'hourly'  # Critical: distinguishes from daily data
                            })
                        except ValueError:
                            pass
        
        except requests.RequestException as e:
            print(f"[Velo] Request error for {exchange}: {e}")
        
        return records
    
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
