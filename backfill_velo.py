"""
Velo Historical Backfill Script (Optimized)

Backfills futures data from Velo API using 15m resolution for efficiency.
Aggregates to hourly for storage. Uses ON CONFLICT DO NOTHING for backfills.

Key optimizations:
- Uses resolution=15m (15x fewer rows than 1m, API doesn't support 1h properly)
- ~46 days per request (vs 3 days before) = 15x fewer API calls
- Estimated time: ~13 hours for full 3-year backfill (vs 9+ days)

Usage:
    python backfill_velo.py                         # Default: 3 years
    python backfill_velo.py --days 30               # Last 30 days
    python backfill_velo.py --start-date 2023-01-26 --end-date 2026-01-25
    python backfill_velo.py --coin BTC              # Filter to specific coin
    python backfill_velo.py --exchange bybit        # Filter to specific exchange
    python backfill_velo.py --dry-run               # Preview without inserting
    python backfill_velo.py --top N                 # Only top N coins by volume
"""

import os
import sys
import csv
import time
import argparse
import threading
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone, timedelta
from requests.auth import HTTPBasicAuth
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

_lock = threading.Lock()

DATABASE_URL = os.environ.get("DATABASE_URL")
API_KEY = os.environ.get("VELO_API_KEY")
BASE_URL = "https://api.velo.xyz/api/v1"
CONFIG_PATH = "velo_config.csv"

REQUEST_DELAY = 0.35
MAX_RETRIES = 5
BASE_BACKOFF = 1.5

RESOLUTION = '15m'
INTERVALS_PER_DAY = 96
VALUES_LIMIT = 22500

FUTURES_COLUMNS = [
    'close_price',
    'dollar_volume',
    'dollar_open_interest_close',
    'funding_rate',
    'liquidations_dollar_volume'
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
    return psycopg2.connect(DATABASE_URL)


def load_entity_cache():
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


def load_config(coin_filter=None, exchange_filter=None, top_n=None):
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
    
    if top_n:
        priority_coins = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA', 'AVAX', 'LINK', 
                         'DOT', 'MATIC', 'UNI', 'ATOM', 'LTC', 'BCH', 'NEAR', 'APT',
                         'OP', 'ARB', 'FIL', 'INJ', 'SUI', 'SEI', 'TIA', 'JUP',
                         'PEPE', 'WIF', 'BONK', 'SHIB', 'HYPE', 'AAVE', 'MKR', 'CRV',
                         'FTM', 'RUNE', 'IMX', 'SAND', 'MANA', 'AXS', 'GALA', 'ENS',
                         'RENDER', 'FET', 'AGIX', 'OCEAN', 'TAO', 'WLD', 'RNDR', 'GRT']
        
        priority_set = set(priority_coins[:top_n])
        priority_config = [c for c in config if c['coin'] in priority_set]
        other_config = [c for c in config if c['coin'] not in priority_set]
        
        remaining = top_n - len(set(c['coin'] for c in priority_config))
        if remaining > 0:
            seen = set(c['coin'] for c in priority_config)
            for c in other_config:
                if c['coin'] not in seen:
                    priority_config.append(c)
                    seen.add(c['coin'])
                    if len(seen) >= top_n:
                        break
                elif c['coin'] in seen:
                    priority_config.append(c)
        
        config = priority_config
        unique_coins = len(set(c['coin'] for c in config))
        print(f"Filtered to top {unique_coins} coins ({len(config)} pairs)")
    
    return config


def calculate_days_per_request(num_columns):
    max_intervals = VALUES_LIMIT // num_columns
    days = max_intervals // INTERVALS_PER_DAY
    return max(1, days - 1)


def fetch_historical(exchange, coin, begin_ts, end_ts, auth, entity_cache):
    records = []
    hourly_data = {}
    
    columns_str = ','.join(FUTURES_COLUMNS)
    
    url = (
        f"{BASE_URL}/rows?"
        f"type=futures&"
        f"exchanges={exchange}&"
        f"coins={coin}&"
        f"columns={columns_str}&"
        f"begin={begin_ts}&"
        f"end={end_ts}&"
        f"resolution={RESOLUTION}"
    )
    
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, auth=auth, timeout=180)
            
            if resp.status_code == 429:
                wait = BASE_BACKOFF ** (attempt + 1)
                print(f"    Rate limited, waiting {wait}s (attempt {attempt + 1}/{MAX_RETRIES})...")
                time.sleep(wait)
                continue
            
            if resp.status_code != 200:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(BASE_BACKOFF)
                    continue
                print(f"    API error {resp.status_code}: {resp.text[:100]}")
                return []
            
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
                
                coin_val = row.get('coin', coin)
                exch = row.get('exchange', exchange)
                entity_id = entity_cache.get(coin_val)
                
                for velo_col, db_metric in METRIC_MAP.items():
                    if velo_col in row and row[velo_col]:
                        try:
                            value = float(row[velo_col])
                            key = (ts_hour, coin_val, exch, db_metric)
                            hourly_data[key] = {
                                'pulled_at': ts_hour,
                                'asset': coin_val,
                                'entity_id': entity_id,
                                'metric_name': db_metric,
                                'value': value,
                                'domain': 'derivative',
                                'exchange': exch,
                                'granularity': 'hourly'
                            }
                        except ValueError:
                            pass
            
            records = list(hourly_data.values())
            break
        
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = BASE_BACKOFF ** (attempt + 1)
                print(f"    Request error, retrying in {wait}s: {e}")
                time.sleep(wait)
                continue
            print(f"    Request error after {MAX_RETRIES} attempts: {e}")
    
    return records


def insert_batch(records):
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
            r.get('granularity', 'hourly')
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
    parser = argparse.ArgumentParser(description='Backfill Velo historical data (optimized)')
    parser.add_argument('--days', type=int, help='Days of history to backfill (default: 1095 = 3 years)')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD, default: now)')
    parser.add_argument('--coin', type=str, help='Filter to specific coin')
    parser.add_argument('--exchange', type=str, help='Filter to specific exchange')
    parser.add_argument('--top', type=int, help='Only backfill top N coins by importance')
    parser.add_argument('--dry-run', action='store_true', help='Preview without inserting')
    parser.add_argument('--delay', type=float, default=REQUEST_DELAY, help='Delay between requests in seconds')
    parser.add_argument('--workers', type=int, default=2, help='Number of parallel workers (default: 2)')
    args = parser.parse_args()
    
    if not DATABASE_URL:
        print("Error: DATABASE_URL not set")
        sys.exit(1)
    if not API_KEY:
        print("Error: VELO_API_KEY not set")
        sys.exit(1)
    
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
        start_dt = end_dt - timedelta(days=365 * 3)
    
    days_per_request = calculate_days_per_request(len(FUTURES_COLUMNS))
    
    print("=" * 70)
    print("VELO HISTORICAL BACKFILL (Optimized)")
    print("=" * 70)
    print(f"Date range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
    print(f"Resolution: {RESOLUTION} (aggregated to hourly)")
    print(f"Days per request: {days_per_request} (optimized from 3)")
    print(f"Request delay: {args.delay}s")
    print(f"Workers: {args.workers}")
    if args.coin:
        print(f"Filtering to coin: {args.coin}")
    if args.exchange:
        print(f"Filtering to exchange: {args.exchange}")
    if args.top:
        print(f"Limiting to top {args.top} coins")
    if args.dry_run:
        print("DRY RUN - no data will be inserted")
    print()
    
    entity_cache = load_entity_cache()
    print(f"Loaded {len(entity_cache)} entity mappings")
    
    config = load_config(args.coin, args.exchange, args.top)
    print(f"Loaded {len(config)} coin-exchange pairs")
    
    by_exchange = defaultdict(list)
    for item in config:
        by_exchange[item['exchange']].append(item['coin'])
    
    auth = HTTPBasicAuth("api", API_KEY)
    
    fetch_tasks = []
    for exchange, coins in by_exchange.items():
        unique_coins = list(set(coins))
        for coin in unique_coins:
            window_start = start_dt
            while window_start < end_dt:
                window_end = min(window_start + timedelta(days=days_per_request), end_dt)
                begin_ts = int(window_start.timestamp() * 1000)
                end_ts = int(window_end.timestamp() * 1000)
                fetch_tasks.append((exchange, coin, begin_ts, end_ts))
                window_start = window_end
    
    total_days = (end_dt - start_dt).days
    estimated_time_sec = len(fetch_tasks) * args.delay / args.workers
    estimated_time_hr = estimated_time_sec / 3600
    
    print(f"\nTotal fetch tasks: {len(fetch_tasks)}")
    print(f"Estimated time: {estimated_time_hr:.1f} hours ({estimated_time_sec/60:.0f} minutes)")
    print("=" * 70)
    
    if args.dry_run:
        print("\n[DRY RUN] Would process the above configuration")
        print(f"\nSample tasks (first 5):")
        for task in fetch_tasks[:5]:
            exchange, coin, begin_ts, end_ts = task
            begin_dt = datetime.fromtimestamp(begin_ts/1000, tz=timezone.utc)
            end_dt_task = datetime.fromtimestamp(end_ts/1000, tz=timezone.utc)
            print(f"  {exchange} / {coin}: {begin_dt.strftime('%Y-%m-%d')} to {end_dt_task.strftime('%Y-%m-%d')}")
        return
    
    total_records = 0
    total_inserted = 0
    completed = 0
    start_time = time.time()
    last_progress = start_time
    
    def process_task(task):
        exchange, coin, begin_ts, end_ts = task
        time.sleep(args.delay)
        records = fetch_historical(exchange, coin, begin_ts, end_ts, auth, entity_cache)
        inserted = 0
        if records:
            inserted = insert_batch(records)
        return len(records), inserted
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_task, task): task for task in fetch_tasks}
        
        for future in as_completed(futures):
            try:
                fetched, inserted = future.result()
                with _lock:
                    total_records += fetched
                    total_inserted += inserted
                    completed += 1
                    
                    now_time = time.time()
                    if now_time - last_progress >= 30 or completed == len(fetch_tasks):
                        elapsed = now_time - start_time
                        rate = completed / elapsed if elapsed > 0 else 0
                        eta_sec = (len(fetch_tasks) - completed) / rate if rate > 0 else 0
                        eta_min = eta_sec / 60
                        
                        print(f"  [{completed}/{len(fetch_tasks)}] "
                              f"Records: {total_records:,} fetched, {total_inserted:,} inserted | "
                              f"Rate: {rate:.1f} req/s | ETA: {eta_min:.0f} min")
                        last_progress = now_time
            except Exception as e:
                with _lock:
                    completed += 1
                print(f"    Task error: {e}")
    
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
