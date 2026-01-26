#!/usr/bin/env python3
"""
CoinGecko Historical Backfill Script

Backfills historical price, market cap, and volume data from CoinGecko API.

Usage:
    python backfill_coingecko.py                         # Default: 3 years
    python backfill_coingecko.py --days 365              # Last year
    python backfill_coingecko.py --start-date 2023-01-01
    python backfill_coingecko.py --coins bitcoin,ethereum,solana
    python backfill_coingecko.py --dry-run

API Notes:
    - Uses /coins/{id}/market_chart/range endpoint
    - Returns daily data for ranges >90 days (00:00 UTC)
    - Pro API: 500 calls/min, Demo: 30 calls/min
"""

import os
import csv
import time
import argparse
import threading
import requests
import psycopg2
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

COINGECKO_API_KEY = os.environ.get('COINGECKO_API_KEY', '')
USE_PRO_API = bool(COINGECKO_API_KEY)

DEMO_BASE_URL = "https://api.coingecko.com/api/v3"
PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"
BASE_URL = PRO_BASE_URL if USE_PRO_API else DEMO_BASE_URL

RATE_LIMIT_PER_SEC = 8 if USE_PRO_API else 0.5
MAX_WORKERS = 4 if USE_PRO_API else 1
MAX_RETRIES = 3
RETRY_DELAY = 5
MAX_DAYS_PER_CHUNK = 365
BATCH_SIZE = 500

METRIC_MAP = {
    'prices': 'PRICE',
    'market_caps': 'MARKET_CAP',
    'total_volumes': 'VOLUME_24H',
}

_rate_lock = threading.Lock()
_last_request_time = [0.0]
_min_interval = 1.0 / RATE_LIMIT_PER_SEC

_thread_local = threading.local()


def get_thread_session():
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = requests.Session()
        _thread_local.session.headers['Accept'] = 'application/json'
        if USE_PRO_API:
            _thread_local.session.headers['x-cg-pro-api-key'] = COINGECKO_API_KEY
    return _thread_local.session


def _wait_for_rate_limit():
    with _rate_lock:
        now = time.time()
        elapsed = now - _last_request_time[0]
        if elapsed < _min_interval:
            time.sleep(_min_interval - elapsed)
        _last_request_time[0] = time.time()


def fetch_market_chart_range(coin_id, start_date, end_date):
    session = get_thread_session()
    
    params = {
        'vs_currency': 'usd',
        'from': start_date.strftime('%Y-%m-%d'),
        'to': end_date.strftime('%Y-%m-%d'),
        'precision': 'full'
    }
    
    if not USE_PRO_API and COINGECKO_API_KEY:
        params['x_cg_demo_api_key'] = COINGECKO_API_KEY
    
    url = f"{BASE_URL}/coins/{coin_id}/market_chart/range"
    
    for attempt in range(MAX_RETRIES):
        _wait_for_rate_limit()
        try:
            resp = session.get(url, params=params, timeout=60)
            
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait_time = 60 if attempt == 0 else 120
                time.sleep(wait_time)
                continue
            elif resp.status_code == 404:
                return None
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                continue
        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        except Exception:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    
    return None


def parse_timeseries(data, asset, start_ts, end_ts):
    records = []
    
    if not data:
        return records
    
    for api_field, metric_name in METRIC_MAP.items():
        timeseries = data.get(api_field, [])
        
        for point in timeseries:
            if len(point) >= 2 and point[1] is not None:
                try:
                    timestamp_ms = point[0]
                    ts = timestamp_ms / 1000
                    
                    if start_ts is not None and ts < start_ts:
                        continue
                    if end_ts is not None and ts > end_ts:
                        continue
                    
                    value = float(point[1])
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    pulled_at = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    records.append({
                        'pulled_at': pulled_at,
                        'asset': asset,
                        'metric_name': metric_name,
                        'value': value
                    })
                except (ValueError, TypeError, OSError):
                    continue
    
    return records


def chunk_date_range(start_date, end_date, max_days=MAX_DAYS_PER_CHUNK):
    chunks = []
    current = start_date
    
    while current < end_date:
        chunk_end = min(current + timedelta(days=max_days), end_date)
        chunks.append((current, chunk_end))
        current = chunk_end
    
    return chunks


def get_db_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])


def load_config():
    config_path = 'coingecko_config.csv'
    if not os.path.exists(config_path):
        return {}
    
    mappings = {}
    with open(config_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('enabled', 'true').lower() == 'true':
                cg_id = row.get('coingecko_id', '').strip()
                if cg_id and 'VERIFY' not in row.get('notes', ''):
                    mappings[cg_id] = cg_id
    return mappings


def load_entity_cache():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT source_id, entity_id
            FROM entity_source_ids
            WHERE source = 'coingecko'
        """)
        cache = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        conn.close()
        return cache
    except Exception:
        return {}


def insert_records_batch(conn, records):
    if not records:
        return 0
    
    cur = conn.cursor()
    inserted = 0
    
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        values = []
        for r in batch:
            values.append((
                r['pulled_at'],
                'coingecko',
                r['asset'],
                r['metric_name'],
                r['value']
            ))
        
        cur.executemany("""
            INSERT INTO metrics (pulled_at, source, asset, metric_name, value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) DO NOTHING
        """, values)
        inserted += len(batch)
    
    conn.commit()
    cur.close()
    return inserted


def backfill_coin(coin_id, asset, start_date, end_date, start_ts, end_ts):
    all_records = []
    chunks = chunk_date_range(start_date, end_date)
    
    for chunk_start, chunk_end in chunks:
        data = fetch_market_chart_range(coin_id, chunk_start, chunk_end)
        if data:
            records = parse_timeseries(data, asset, start_ts, end_ts)
            all_records.extend(records)
    
    return all_records


def main():
    parser = argparse.ArgumentParser(description='CoinGecko Historical Backfill')
    parser.add_argument('--days', type=int, help='Number of days to backfill (default: 1095 = 3 years)')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD, default: yesterday)')
    parser.add_argument('--coins', type=str, help='Comma-separated list of coin IDs')
    parser.add_argument('--dry-run', action='store_true', help='Preview without inserting')
    args = parser.parse_args()
    
    end_date = datetime.now(timezone.utc) - timedelta(days=1)
    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    elif args.days:
        start_date = end_date - timedelta(days=args.days)
    else:
        start_date = end_date - timedelta(days=365 * 3)
    
    start_ts = start_date.timestamp()
    end_ts = end_date.replace(hour=23, minute=59, second=59).timestamp()
    
    print("=" * 70)
    print("COINGECKO HISTORICAL BACKFILL")
    print("=" * 70)
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"API: {'Pro' if USE_PRO_API else 'Demo'} ({RATE_LIMIT_PER_SEC * 60:.0f} calls/min)")
    
    if args.dry_run:
        print("DRY RUN - no data will be inserted")
    
    entity_cache = load_entity_cache()
    print(f"Entity mappings: {len(entity_cache)}")
    
    if args.coins:
        coin_ids = [c.strip() for c in args.coins.split(',')]
        coin_list = [(cid, entity_cache.get(cid, cid)) for cid in coin_ids]
    else:
        config = load_config()
        coin_list = [(cid, entity_cache.get(cid, cid)) for cid in config.keys()]
    
    print(f"Coins to backfill: {len(coin_list)}")
    
    date_chunks = chunk_date_range(start_date, end_date)
    print(f"Date chunks: {len(date_chunks)} (max {MAX_DAYS_PER_CHUNK} days each)")
    print("=" * 70)
    
    if args.dry_run:
        print("\n[DRY RUN] Would process the above configuration")
        return
    
    conn = get_db_connection()
    
    total_records = 0
    successful = 0
    failed = []
    api_calls = 0
    start_time = time.time()
    
    def process_coin(coin_data):
        coin_id, asset = coin_data
        return coin_id, asset, backfill_coin(coin_id, asset, start_date, end_date, start_ts, end_ts)
    
    print(f"\nProcessing {len(coin_list)} coins with {MAX_WORKERS} workers...")
    print("-" * 70)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_coin, coin): coin for coin in coin_list}
        
        for i, future in enumerate(as_completed(futures), 1):
            try:
                coin_id, asset, records = future.result()
                api_calls += len(date_chunks)
                
                if records:
                    inserted = insert_records_batch(conn, records)
                    total_records += inserted
                    successful += 1
                    unique_dates = len(set(r['pulled_at'] for r in records))
                    print(f"[{i:3d}/{len(coin_list)}] {coin_id:30s} -> {len(records):5d} records ({unique_dates} days)")
                else:
                    print(f"[{i:3d}/{len(coin_list)}] {coin_id:30s} -> No data")
                    failed.append(coin_id)
                    
            except Exception as e:
                coin = futures[future]
                print(f"[{i:3d}/{len(coin_list)}] {coin[0]:30s} -> Error: {e}")
                failed.append(coin[0])
    
    conn.close()
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("BACKFILL COMPLETE")
    print("=" * 70)
    print(f"  Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"  Coins: {successful}/{len(coin_list)}")
    print(f"  Records inserted: {total_records:,}")
    print(f"  API calls: ~{api_calls}")
    print(f"  Time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    if elapsed > 0:
        print(f"  Rate: {total_records / elapsed:.0f} records/sec")
    if failed:
        print(f"  Failed: {failed[:10]}{'...' if len(failed) > 10 else ''}")
    print("=" * 70)


if __name__ == "__main__":
    main()
