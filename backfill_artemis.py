#!/usr/bin/env python3
"""
Artemis Historical Backfill Script

Backfills historical data from Artemis API for all configured assets and metrics.
Uses parallel workers for speed and date chunking for long ranges.

Usage:
    python backfill_artemis.py                    # Default: 3 years
    python backfill_artemis.py --days 365         # Last 365 days
    python backfill_artemis.py --start-date 2022-01-01 --end-date 2024-12-31
    python backfill_artemis.py --dry-run          # Preview without inserting
"""

import os
import csv
import time
import argparse
import requests
import psycopg2
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Rate limiting: 2 requests per second max (enforced at execution time)
REQUEST_DELAY = 0.5  # Minimum delay between API calls
BATCH_SIZE = 50
MAX_WORKERS = 2  # Reduced to avoid burst
MAX_DAYS_PER_CHUNK = 365
MAX_RETRIES = 2  # Reduced from 3
RETRY_DELAY = 2
REQUEST_TIMEOUT = 30  # Reduced from 120s to avoid hung tasks
DB_BATCH_SIZE = 10  # Commit every N tasks instead of every task

# Thread-safe rate limiter
_rate_lock = threading.Lock()
_last_request_time = 0

EXCLUDED_METRICS = {
    "VOLATILITY_90D_ANN",
    "90-Day ANN Volatility",
    "STABLECOIN_AVG_DAU",
    "Stablecoins Average DAUs",
    "TOKENIZED_SHARES_TRADING_VOLUME",
    "Stock Trading Volume",
    "FDMV_NAV_RATIO",
    "FDMV / NAV",
}

FRIENDLY_TO_API_ID = {
    "Fees": "FEES",
    "Open Interest": "OPEN_INTEREST",
    "Perpetuals Volume": "PERP_VOLUME",
    "Revenue": "REVENUE",
    "Daily Active Users": "DAU",
    "New Users": "NEW_USERS",
    "Transactions": "TXNS",
    "Perpetual Liquidations": "PERP_LIQUIDATION",
    "Circulating Supply": "CIRCULATING_SUPPLY_NATIVE",
    "Total Supply Native": "TOTAL_SUPPLY_NATIVE",
    "Spot Volume": "SPOT_VOLUME",
    "Spot Fees": "SPOT_FEES",
    "Price": "PRICE",
    "FDMC / Fees": "FDMC_FEES_RATIO",
    "FDMC / Revenue": "FDMC_REVENUE_RATIO",
    "Market Cap": "MC",
    "Stablecoin Transfer Volume": "STABLECOIN_TRANSFER_VOLUME",
    "Gross Emissions": "GROSS_EMISSIONS",
    "Fully Diluted Market Cap": "FDMC",
    "Enterprise Value": "EQ_EV",
    "Latest Cash": "EQ_CASH_AND_CASH_EQUIVALENTS",
    "Debt": "EQ_TOTAL_DEBT",
    "Stablecoin Supply (USD)": "STABLECOIN_MC",
    "Filtered Stablecoin Transactions": "STABLECOIN_DAILY_TXNS",
    "Daily Token Trading Volume": "24H_VOLUME",
    "Net Asset Value": "NAV",
    "Stablecoin Average Transaction Value": "AVERAGE_TRANSACTION_VALUE",
    "Average Transaction Fee": "AVG_TXN_FEE",
    "Perpetual Fees": "PERP_FEES",
    "Perpetual Transactions": "PERP_TXNS",
    "Lending Deposits": "LENDING_DEPOSITS",
    "Lending Borrows": "LENDING_BORROWS",
}


def load_config():
    pull_config = {}
    
    with open('artemis_config.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        
        pull_idx = headers.index('Pull') if 'Pull' in headers else -1
        metric_cols = headers[pull_idx + 1:] if pull_idx >= 0 else []
        
        for row in reader:
            if row['Pull'] in ['1', '1.0', 1, 1.0]:
                asset_id = row['asset']
                
                for metric in metric_cols:
                    if metric in EXCLUDED_METRICS:
                        continue
                    if row.get(metric) in ['1', '1.0', 1, 1.0]:
                        api_id = FRIENDLY_TO_API_ID.get(metric, metric)
                        if api_id in EXCLUDED_METRICS:
                            continue
                        if api_id not in pull_config:
                            pull_config[api_id] = []
                        if asset_id not in pull_config[api_id]:
                            pull_config[api_id].append(asset_id)
    
    return pull_config


def rate_limit():
    """Enforce rate limiting between API calls (thread-safe)."""
    global _last_request_time
    with _rate_lock:
        now = time.time()
        elapsed = now - _last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        _last_request_time = time.time()


def fetch_historical(api_key, metric, symbols, start_date, end_date):
    # Enforce rate limit BEFORE making the request
    rate_limit()
    
    url = f"https://api.artemisxyz.com/data/{metric.lower()}/"
    params = {
        'symbols': ','.join(symbols),
        'startDate': start_date,
        'endDate': end_date,
        'APIKey': api_key
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            
            if resp.status_code == 200 and resp.text.strip():
                return resp.json()
            elif resp.status_code == 429:
                wait_time = RETRY_DELAY * (attempt + 2)  # Longer backoff for rate limit
                print(f"    Rate limited on {metric}, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            elif resp.status_code == 400:
                print(f"    Bad request for {metric}: {resp.text[:100]}")
                return None
            else:
                print(f"    API error {resp.status_code} for {metric}, retrying...")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                continue
                
        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * 2)
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    
    return None


def extract_records(data, metric_name):
    records = []
    if not data:
        return records
    
    data_section = data.get('data', data)
    symbols_data = data_section.get('symbols', data_section)
    
    for asset, asset_data in symbols_data.items():
        if not isinstance(asset_data, dict):
            continue
            
        metric_key = metric_name.lower()
        metric_values = asset_data.get(metric_key, asset_data.get(metric_name, []))
        
        if isinstance(metric_values, list):
            for entry in metric_values:
                if isinstance(entry, dict):
                    date_str = entry.get('date') or entry.get('timestamp')
                    value = entry.get('val') or entry.get('value')
                    
                    if date_str and value is not None:
                        try:
                            if isinstance(value, str):
                                if value in ("Metric not found.", "Metric not available for asset."):
                                    continue
                                value = float(value)
                            dt = datetime.strptime(date_str[:10], '%Y-%m-%d')
                            records.append((dt, 'artemis', asset, metric_name, float(value), dt.date()))
                        except (ValueError, TypeError):
                            continue
                            
        elif isinstance(metric_values, dict):
            for date_str, value in metric_values.items():
                if len(date_str) >= 10 and date_str[4] == '-':
                    try:
                        if value is None:
                            continue
                        if isinstance(value, str):
                            if value in ("Metric not found.", "Metric not available for asset."):
                                continue
                            value = float(value)
                        dt = datetime.strptime(date_str[:10], '%Y-%m-%d')
                        records.append((dt, 'artemis', asset, metric_name, float(value), dt.date()))
                    except (ValueError, TypeError):
                        continue
    
    return records


def generate_date_chunks(start_date, end_date, max_days=MAX_DAYS_PER_CHUNK):
    chunks = []
    current = start_date
    
    while current < end_date:
        chunk_end = min(current + timedelta(days=max_days), end_date)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    
    return chunks


def main():
    parser = argparse.ArgumentParser(description='Artemis Historical Backfill')
    parser.add_argument('--days', type=int, help='Number of days to backfill (default: 1095 = 3 years)')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD, default: yesterday)')
    parser.add_argument('--dry-run', action='store_true', help='Preview without inserting')
    parser.add_argument('--metric', type=str, help='Backfill only specific metric')
    args = parser.parse_args()
    
    api_key = os.environ.get('ARTEMIS_API_KEY')
    if not api_key:
        print("ERROR: ARTEMIS_API_KEY not set")
        return
    
    end_date = datetime.now() - timedelta(days=1)
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    elif args.days:
        start_date = end_date - timedelta(days=args.days)
    else:
        start_date = end_date - timedelta(days=365 * 3)
    
    print("=" * 70)
    print("ARTEMIS HISTORICAL BACKFILL")
    print("=" * 70)
    
    pull_config = load_config()
    
    if args.metric:
        if args.metric in pull_config:
            pull_config = {args.metric: pull_config[args.metric]}
        else:
            print(f"ERROR: Metric {args.metric} not found in config")
            return
    
    all_metrics = list(pull_config.keys())
    all_assets = sorted(set(a for assets in pull_config.values() for a in assets))
    
    print(f"Metrics to backfill: {len(all_metrics)}")
    print(f"Assets to backfill: {len(all_assets)}")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    date_chunks = generate_date_chunks(start_date, end_date)
    print(f"Date chunks: {len(date_chunks)} (max {MAX_DAYS_PER_CHUNK} days each)")
    
    if args.dry_run:
        print("\n[DRY RUN] Would process the above configuration")
        return
    
    print("=" * 70)
    
    conn = psycopg2.connect(os.environ['DATABASE_URL'], connect_timeout=30)
    cur = conn.cursor()
    # Set statement timeout to prevent indefinite hangs (60 seconds)
    cur.execute("SET statement_timeout = '60s'")
    
    total_records = 0
    api_calls = 0
    errors = 0
    start_time = time.time()
    
    fetch_tasks = []
    for chunk_start, chunk_end in date_chunks:
        for metric in all_metrics:
            assets_for_metric = pull_config[metric]
            for batch_start in range(0, len(assets_for_metric), BATCH_SIZE):
                batch = assets_for_metric[batch_start:batch_start + BATCH_SIZE]
                fetch_tasks.append((metric, batch, chunk_start.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')))
    
    print(f"Total fetch tasks: {len(fetch_tasks)}", flush=True)
    print(f"Using {MAX_WORKERS} parallel workers, {REQUEST_TIMEOUT}s timeout", flush=True)
    print("=" * 70, flush=True)
    
    def fetch_task(task):
        metric, batch, start_str, end_str = task
        data = fetch_historical(api_key, metric, batch, start_str, end_str)
        if data:
            return extract_records(data, metric)
        return []
    
    completed = 0
    failed_tasks = 0
    pending_records = []  # Buffer for batch commits
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for task in fetch_tasks:
            future = executor.submit(fetch_task, task)
            futures[future] = task
            # No delay here - rate limiting is enforced in fetch_historical()
        
        for future in as_completed(futures):
            completed += 1
            api_calls += 1
            task_info = futures[future]
            try:
                # Timeout on result to avoid hung futures
                records = future.result(timeout=REQUEST_TIMEOUT + 10)
                if records:
                    pending_records.extend(records)
                    total_records += len(records)
                else:
                    failed_tasks += 1
                
                # Batch commit every DB_BATCH_SIZE tasks or at the end
                if len(pending_records) >= 500 or completed == len(fetch_tasks):
                    if pending_records:
                        cur.executemany('''
                            INSERT INTO metrics (pulled_at, source, asset, metric_name, value, metric_date)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) DO NOTHING
                        ''', pending_records)
                        conn.commit()
                        pending_records = []
                
                # Progress logging every 10 tasks for visibility
                if completed % 10 == 0 or completed == len(fetch_tasks):
                    elapsed = time.time() - start_time
                    rate = total_records / elapsed if elapsed > 0 else 0
                    pct = completed / len(fetch_tasks) * 100
                    print(f"  [{pct:5.1f}%] {completed}/{len(fetch_tasks)} tasks, "
                          f"{total_records:,} records, {failed_tasks} empty, {rate:.0f} rec/sec", flush=True)
            except TimeoutError:
                errors += 1
                failed_tasks += 1
                print(f"  TIMEOUT on {task_info[0]} ({task_info[2]})", flush=True)
            except Exception as e:
                errors += 1
                failed_tasks += 1
                print(f"  Error on {task_info[0]}: {e}", flush=True)
    
    conn.close()
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 70, flush=True)
    print("BACKFILL COMPLETE", flush=True)
    print("=" * 70, flush=True)
    print(f"Total records: {total_records:,}", flush=True)
    print(f"API calls: {api_calls}", flush=True)
    print(f"Tasks with no data: {failed_tasks}", flush=True)
    print(f"Errors: {errors}", flush=True)
    print(f"Time: {elapsed:.0f}s ({elapsed/60:.1f} min)", flush=True)
    if elapsed > 0:
        print(f"Rate: {total_records/elapsed:.0f} records/sec", flush=True)
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
