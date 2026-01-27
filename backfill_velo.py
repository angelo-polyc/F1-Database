"""
Velo Historical Backfill Script

Backfills hourly futures data from Velo API.
Uses ON CONFLICT DO NOTHING to preserve existing data.

Usage:
    python backfill_velo.py                         # Default: 3 years
    python backfill_velo.py --days 30               # Last 30 days
    python backfill_velo.py --start-date 2023-01-26 --end-date 2026-01-25
    python backfill_velo.py --coin BTC              # Filter to specific coin
    python backfill_velo.py --exchange bybit        # Filter to specific exchange
    python backfill_velo.py --dry-run               # Preview without inserting

Note: Velo API returns minute-level data which is aggregated to hourly.
      72-hour windows used to stay under 22,500 cell API limit.
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

# Rate limiting: 120 req/30s = 4/sec, but use 2/sec to be safe
REQUEST_DELAY = 0.5
MAX_WORKERS = 1  # Sequential fetching to avoid rate limits

# Velo returns max 22,500 values per request
# API returns MINUTE-level data (1440 rows/day) despite resolution=1h
# With 5 columns, 1 coin: 22500 / (1440 * 5) = 3.1 days max
# Use 3 days per request with 1 coin to stay under limit
HOURS_PER_REQUEST = 72  # 3 days = 72 hours

# Batch size: 1 coin per request (API gives minute data, aggregated hourly client-side)
BACKFILL_BATCH_SIZE = 1

# Essential metrics only for faster backfill (5 columns = 3.1 days/request max)
FUTURES_COLUMNS = [
    'close_price',
    'dollar_volume',
    'dollar_open_interest_close',
    'funding_rate',
    'premium'
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
    """Fetch historical data for a batch of coins from one exchange.
    
    Aggregates minute-level API data to hourly records.
    """
    hourly_data = {}  # Key: (ts_hour, coin, exchange, metric) -> record
    
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
                            # Key for hourly aggregation
                            key = (ts_hour, coin, exch, db_metric)
                            hourly_data[key] = {
                                'pulled_at': ts_hour,
                                'asset': coin,
                                'entity_id': entity_id,
                                'metric_name': db_metric,
                                'value': value,  # Use last value for the hour
                                'domain': 'derivative',
                                'exchange': exch,
                                'granularity': 'hourly'
                            }
                        except ValueError:
                            pass
            
            # Convert aggregated data to records list
            records = list(hourly_data.values())
            break
        
        except requests.RequestException as e:
            if attempt < 2:
                time.sleep(1)
                continue
            print(f"    Request error: {e}")
    
    # Return aggregated hourly records
    return list(hourly_data.values())


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
    parser.add_argument('--days', type=int, help='Days of history to backfill (default: 1095 = 3 years)')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD, default: now)')
    parser.add_argument('--coin', type=str, help='Filter to specific coin')
    parser.add_argument('--exchange', type=str, help='Filter to specific exchange')
    parser.add_argument('--dry-run', action='store_true', help='Preview without inserting')
    args = parser.parse_args()
    
    if not DATABASE_URL:
        print("Error: DATABASE_URL not set")
        sys.exit(1)
    if not API_KEY:
        print("Error: VELO_API_KEY not set")
        sys.exit(1)
    
    # Calculate date range
    now = datetime.now(timezone.utc)
    
    if args.end_date:
        end_dt = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_dt = end_dt.replace(hour=23, minute=0, second=0, microsecond=0)
    else:
        end_dt = now.replace(minute=0, second=0, microsecond=0)
    
    if args.start_date:
        start_dt = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    elif args.days:
        start_dt = end_dt - timedelta(days=args.days)
    else:
        start_dt = end_dt - timedelta(days=365 * 3)  # Default: 3 years
    
    print("=" * 70)
    print("VELO HISTORICAL BACKFILL")
    print("=" * 70)
    print(f"Date range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
    if args.coin:
        print(f"Filtering to coin: {args.coin}")
    if args.exchange:
        print(f"Filtering to exchange: {args.exchange}")
    if args.dry_run:
        print("DRY RUN - no data will be inserted")
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
    
    total_records = 0
    total_inserted = 0
    
    # Build all fetch tasks: (exchange, coins, begin_ts, end_ts)
    fetch_tasks = []
    for exchange, coins in by_exchange.items():
        for i in range(0, len(coins), BACKFILL_BATCH_SIZE):
            batch_coins = coins[i:i+BACKFILL_BATCH_SIZE]
            
            window_start = start_dt
            while window_start < end_dt:
                window_end = min(window_start + timedelta(hours=HOURS_PER_REQUEST), end_dt)
                begin_ts = int(window_start.timestamp() * 1000)
                end_ts = int(window_end.timestamp() * 1000)
                fetch_tasks.append((exchange, batch_coins, begin_ts, end_ts))
                window_start = window_end
    
    print(f"Total fetch tasks: {len(fetch_tasks)}")
    print(f"Using {MAX_WORKERS} parallel workers")
    print("=" * 70)
    
    if args.dry_run:
        print("\n[DRY RUN] Would process the above configuration")
        return
    
    # Process in parallel with rate limiting
    completed = 0
    start_time = time.time()
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
    
    elapsed = time.time() - start_time
    
    print()
    print("=" * 70)
    print("BACKFILL COMPLETE")
    print("=" * 70)
    print(f"  Date range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
    print(f"  Total records fetched:  {total_records:,}")
    print(f"  Total records inserted: {total_inserted:,}")
    print(f"  Time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    if elapsed > 0 and total_records > 0:
        print(f"  Rate: {total_records / elapsed:.0f} records/sec")
    print(f"  (Difference is duplicates skipped via ON CONFLICT DO NOTHING)")
    print("=" * 70)


if __name__ == "__main__":
    main()
