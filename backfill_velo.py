"""
Velo Historical Backfill Script

Backfills hourly futures data from Velo API.
Uses ON CONFLICT DO NOTHING to preserve existing data.

Usage:
    python backfill_velo.py                    # Backfill last 30 days
    python backfill_velo.py --days 90          # Backfill last 90 days
    python backfill_velo.py --coin BTC         # Backfill only BTC
    python backfill_velo.py --exchange bybit   # Backfill only Bybit
"""

import os
import sys
import csv
import time
import argparse
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.auth import HTTPBasicAuth
from collections import defaultdict

# Configuration
DATABASE_URL = os.environ.get("DATABASE_URL")
API_KEY = os.environ.get("VELO_API_KEY")
BASE_URL = "https://api.velo.xyz/api/v1"
CONFIG_PATH = "velo_config.csv"

# Rate limiting: 120 req/30s = 4/sec
REQUEST_DELAY = 0.25
MAX_WORKERS = 4  # Parallel workers for fetching

# Velo returns max 22,500 values per request
# With 30 columns, 750 rows = ~31 days at hourly
# Use 20 days per request to be safe
DAYS_PER_REQUEST = 20

# Batch size for coins per request
BACKFILL_BATCH_SIZE = 5

# All metrics to pull
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


def get_connection():
    """Get database connection."""
    return psycopg2.connect(DATABASE_URL)


def load_entity_cache():
    """Load entity_id mappings from database."""
    cache = {}
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT source_id, entity_id 
            FROM entity_source_ids 
            WHERE source = 'velo'
        """)
        for row in cur.fetchall():
            cache[row[0]] = row[1]
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Warning: Could not load entity cache: {e}")
    return cache


def load_config(coin_filter=None, exchange_filter=None):
    """Load coin-exchange pairs from config, optionally filtered."""
    if not os.path.exists(CONFIG_PATH):
        print(f"Error: Config file not found: {CONFIG_PATH}")
        sys.exit(1)
    
    config = []
    with open(CONFIG_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('pull', '1') != '1':
                continue
            
            coin = row['coin'].upper()
            exchange = row['exchange'].lower()
            
            if coin_filter and coin != coin_filter.upper():
                continue
            if exchange_filter and exchange != exchange_filter.lower():
                continue
            
            config.append({'coin': coin, 'exchange': exchange})
    
    return config


def fetch_historical(exchange, coins, begin_ts, end_ts, auth, entity_cache):
    """Fetch historical data for a batch of coins from one exchange."""
    records = []
    
    coins_str = ','.join(coins)
    columns_str = ','.join(FUTURES_COLUMNS)
    
    url = (
        f"{BASE_URL}/rows?"
        f"type=futures&"
        f"exchanges={exchange}&"
        f"coins={coins_str}&"
        f"columns={columns_str}&"
        f"begin={begin_ts}&"
        f"end={end_ts}&"
        f"resolution=1h"
    )
    
    for attempt in range(3):
        try:
            resp = requests.get(url, auth=auth, timeout=120)
            
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            
            if resp.status_code != 200:
                if attempt < 2:
                    time.sleep(1)
                    continue
                print(f"    API error {resp.status_code}: {resp.text[:100]}")
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
                
                try:
                    ts_ms = int(row['time'])
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    ts_hour = ts.replace(minute=0, second=0, microsecond=0)
                except (ValueError, KeyError):
                    continue
                
                coin = row['coin']
                exch = row['exchange']
                entity_id = entity_cache.get(coin)
                
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
            
            break
        
        except requests.RequestException as e:
            if attempt < 2:
                time.sleep(1)
                continue
            print(f"    Request error: {e}")
    
    return records


def insert_batch(records):
    """Insert records with ON CONFLICT DO NOTHING for backfills."""
    if not records:
        return 0
    
    conn = get_connection()
    cur = conn.cursor()
    
    rows = [
        (
            r['pulled_at'], 
            'velo', 
            r['asset'], 
            r.get('entity_id'),
            r['metric_name'], 
            r['value'],
            r.get('domain', 'derivative'),
            r.get('exchange'),
            r.get('granularity', 'hourly')  # Default to hourly for Velo
        )
        for r in records
    ]
    
    query = """
        INSERT INTO metrics (pulled_at, source, asset, entity_id, metric_name, value, domain, exchange, granularity)
        VALUES %s
        ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) DO NOTHING
    """
    
    try:
        execute_values(cur, query, rows, page_size=1000)
        inserted = cur.rowcount
        conn.commit()
    except Exception as e:
        print(f"    Database error: {e}")
        conn.rollback()
        inserted = 0
    finally:
        cur.close()
        conn.close()
    
    return inserted


def main():
    parser = argparse.ArgumentParser(description='Backfill Velo historical data')
    parser.add_argument('--days', type=int, default=30, help='Days of history to backfill')
    parser.add_argument('--coin', type=str, help='Filter to specific coin')
    parser.add_argument('--exchange', type=str, help='Filter to specific exchange')
    args = parser.parse_args()
    
    if not DATABASE_URL:
        print("Error: DATABASE_URL not set")
        sys.exit(1)
    if not API_KEY:
        print("Error: VELO_API_KEY not set")
        sys.exit(1)
    
    print("=" * 70)
    print("VELO HISTORICAL BACKFILL")
    print("=" * 70)
    print(f"Days to backfill: {args.days}")
    if args.coin:
        print(f"Filtering to coin: {args.coin}")
    if args.exchange:
        print(f"Filtering to exchange: {args.exchange}")
    print()
    
    # Load entity cache
    entity_cache = load_entity_cache()
    print(f"Loaded {len(entity_cache)} entity mappings")
    
    # Load config
    config = load_config(args.coin, args.exchange)
    print(f"Loaded {len(config)} coin-exchange pairs")
    
    # Group by exchange
    by_exchange = defaultdict(list)
    for item in config:
        by_exchange[item['exchange']].append(item['coin'])
    
    auth = HTTPBasicAuth("api", API_KEY)
    
    # Time range
    now = datetime.now(timezone.utc)
    end_dt = now.replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=args.days)
    
    print(f"Time range: {start_dt.isoformat()} to {end_dt.isoformat()}")
    print()
    
    total_records = 0
    total_inserted = 0
    
    # Build all fetch tasks: (exchange, coins, begin_ts, end_ts)
    fetch_tasks = []
    for exchange, coins in by_exchange.items():
        for i in range(0, len(coins), BACKFILL_BATCH_SIZE):
            batch_coins = coins[i:i+BACKFILL_BATCH_SIZE]
            
            window_start = start_dt
            while window_start < end_dt:
                window_end = min(window_start + timedelta(days=DAYS_PER_REQUEST), end_dt)
                begin_ts = int(window_start.timestamp() * 1000)
                end_ts = int(window_end.timestamp() * 1000)
                fetch_tasks.append((exchange, batch_coins, begin_ts, end_ts))
                window_start = window_end
    
    print(f"Total fetch tasks: {len(fetch_tasks)}")
    print(f"Using {MAX_WORKERS} parallel workers")
    print()
    
    # Process in parallel with rate limiting
    completed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for task in fetch_tasks:
            exchange, batch_coins, begin_ts, end_ts = task
            future = executor.submit(fetch_historical, exchange, batch_coins, begin_ts, end_ts, auth, entity_cache)
            futures[future] = task
            time.sleep(REQUEST_DELAY)  # Rate limit submissions
        
        for future in as_completed(futures):
            completed += 1
            try:
                records = future.result()
                total_records += len(records)
                
                if records:
                    inserted = insert_batch(records)
                    total_inserted += inserted
                
                if completed % 10 == 0:
                    print(f"  Progress: {completed}/{len(fetch_tasks)} tasks, "
                          f"fetched: {total_records:,}, inserted: {total_inserted:,}")
            except Exception as e:
                task = futures[future]
                print(f"  Error on {task[0]}: {e}")
    
    print()
    print("=" * 70)
    print(f"COMPLETE")
    print(f"  Total records fetched:  {total_records:,}")
    print(f"  Total records inserted: {total_inserted:,}")
    print(f"  (Difference is duplicates skipped)")
    print("=" * 70)


if __name__ == "__main__":
    main()
