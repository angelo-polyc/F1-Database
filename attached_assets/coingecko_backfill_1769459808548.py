#!/usr/bin/env python3
"""
CoinGecko Historical Data Backfill Script

Backfills historical price, market cap, and volume data from CoinGecko API
into the PostgreSQL database.

Usage:
    python coingecko_backfill.py --start-date 2024-01-01 --end-date 2024-06-30
    python coingecko_backfill.py --days 90
    python coingecko_backfill.py --start-date 2024-01-01 --coins bitcoin,ethereum,solana
    python coingecko_backfill.py --start-date 2024-01-01 --dry-run

Environment:
    COINGECKO_API_KEY - CoinGecko API key (Pro recommended for higher rate limits)
    DATABASE_URL - PostgreSQL connection string

API Endpoints:
    /coins/{id}/market_chart/range - Historical chart data with from/to timestamps
    
Rate Limits:
    Pro API: 500 calls/min
    Demo API: 30 calls/min
"""

import os
import sys
import csv
import time
import argparse
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

# =============================================================================
# CONFIGURATION
# =============================================================================

COINGECKO_API_KEY = os.environ.get('COINGECKO_API_KEY', '')
USE_PRO_API = bool(COINGECKO_API_KEY)

DEMO_BASE_URL = "https://api.coingecko.com/api/v3"
PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"
BASE_URL = PRO_BASE_URL if USE_PRO_API else DEMO_BASE_URL

# Rate limits per minute
RATE_LIMIT_CALLS_PER_MIN = 500 if USE_PRO_API else 30
REQUEST_DELAY = 60 / RATE_LIMIT_CALLS_PER_MIN + 0.05  # Small buffer

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5

# Date chunking - max days per request to avoid timeouts
MAX_DAYS_PER_CHUNK = 365

# Metric mapping from API response fields to database metric names
METRIC_MAP = {
    'prices': 'PRICE',
    'market_caps': 'MARKET_CAP',
    'total_volumes': 'VOLUME_24H',
}

SOURCE_NAME = "coingecko"

# =============================================================================
# DATABASE FUNCTIONS (Standalone - no external dependencies)
# =============================================================================

def get_connection():
    """Get database connection using DATABASE_URL environment variable."""
    import psycopg2
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    return psycopg2.connect(database_url)


def insert_historical_metrics(records: list, dry_run: bool = False) -> int:
    """Insert historical metric records with upsert logic."""
    if not records:
        return 0
    
    if dry_run:
        print(f"[DRY RUN] Would insert {len(records)} records")
        return len(records)
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Records should have: pulled_at, asset, metric_name, value
    rows = [
        (r["pulled_at"], SOURCE_NAME, r["asset"], r["metric_name"], r["value"], None)
        for r in records
    ]
    
    cur.executemany("""
        INSERT INTO metrics (pulled_at, source, asset, metric_name, value, exchange)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) 
        DO UPDATE SET value = EXCLUDED.value
    """, rows)
    
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    
    return inserted


def log_pull(status: str, records_count: int) -> int:
    """Log the backfill operation to the pulls table."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pulls (source_name, pulled_at, status, records_count)
        VALUES (%s, %s, %s, %s)
        RETURNING pull_id
        """,
        (f"{SOURCE_NAME}_backfill", datetime.utcnow(), status, records_count)
    )
    pull_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return pull_id


def load_entity_cache() -> dict:
    """Load entity ID mappings from database."""
    try:
        conn = get_connection()
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
    except Exception as e:
        print(f"[CoinGecko] Warning: Could not load entity cache: {e}")
        return {}


# =============================================================================
# API FUNCTIONS
# =============================================================================

class CoinGeckoBackfill:
    """CoinGecko historical data backfill handler."""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.session = requests.Session()
        self.session.headers['Accept'] = 'application/json'
        if USE_PRO_API:
            self.session.headers['x-cg-pro-api-key'] = COINGECKO_API_KEY
        self.last_request_time = 0
        self.entity_cache = {}
        self.api_calls = 0
    
    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: dict = None, retries: int = 0):
        """Make API request with retry logic."""
        self._rate_limit()
        url = f"{BASE_URL}/{endpoint}"
        
        # For demo API, pass key as query param
        if not USE_PRO_API and COINGECKO_API_KEY:
            params = params or {}
            params['x_cg_demo_api_key'] = COINGECKO_API_KEY
        
        try:
            response = self.session.get(url, params=params, timeout=60)
            self.api_calls += 1
            
            if response.status_code == 429:
                wait_time = 60 if retries == 0 else 120
                print(f"[CoinGecko] Rate limited - waiting {wait_time} seconds")
                time.sleep(wait_time)
                if retries < MAX_RETRIES:
                    return self._make_request(endpoint, params, retries + 1)
                return None
            
            if response.status_code == 404:
                return None  # Coin not found
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.Timeout:
            print(f"[CoinGecko] Timeout on {endpoint}")
            if retries < MAX_RETRIES:
                time.sleep(RETRY_DELAY * (retries + 1))
                return self._make_request(endpoint, params, retries + 1)
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"[CoinGecko] API error: {e}")
            if retries < MAX_RETRIES:
                time.sleep(RETRY_DELAY * (retries + 1))
                return self._make_request(endpoint, params, retries + 1)
            return None
    
    def load_config(self, config_path: str = None) -> dict:
        """Load coin configuration from CSV file."""
        if config_path is None:
            # Try common locations
            candidates = [
                'coingecko_config.csv',
                os.path.join(os.path.dirname(__file__), 'coingecko_config.csv'),
                os.path.join(os.path.dirname(__file__), '..', 'coingecko_config.csv'),
            ]
            for path in candidates:
                if os.path.exists(path):
                    config_path = path
                    break
        
        if not config_path or not os.path.exists(config_path):
            print(f"[CoinGecko] Config file not found")
            return {}
        
        mappings = {}
        try:
            with open(config_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('enabled', 'true').lower() == 'true':
                        symbol = row.get('symbol', '').upper().strip()
                        cg_id = row.get('coingecko_id', '').strip()
                        if symbol and cg_id:
                            mappings[symbol] = cg_id
            print(f"[CoinGecko] Loaded {len(mappings)} coins from config: {config_path}")
            return mappings
        except Exception as e:
            print(f"[CoinGecko] Failed to load config: {e}")
            return {}
    
    def fetch_market_chart_range(self, coin_id: str, start_date: datetime, end_date: datetime) -> dict:
        """
        Fetch historical market chart data for a coin within a date range.
        
        Uses /coins/{id}/market_chart/range endpoint.
        Returns dict with 'prices', 'market_caps', 'total_volumes' arrays.
        """
        # Format dates as ISO strings (recommended by CoinGecko)
        from_str = start_date.strftime('%Y-%m-%d')
        to_str = end_date.strftime('%Y-%m-%d')
        
        params = {
            'vs_currency': 'usd',
            'from': from_str,
            'to': to_str,
            'precision': 'full'  # Full precision for values
        }
        
        endpoint = f"coins/{coin_id}/market_chart/range"
        return self._make_request(endpoint, params)
    
    def parse_timeseries(self, data: dict, asset: str) -> list:
        """
        Parse CoinGecko market chart response into metric records.
        
        Response format:
        {
            "prices": [[timestamp_ms, price], ...],
            "market_caps": [[timestamp_ms, market_cap], ...],
            "total_volumes": [[timestamp_ms, volume], ...]
        }
        """
        records = []
        
        for api_field, metric_name in METRIC_MAP.items():
            timeseries = data.get(api_field, [])
            
            for point in timeseries:
                if len(point) >= 2:
                    timestamp_ms = point[0]
                    value = point[1]
                    
                    if value is None:
                        continue
                    
                    try:
                        # Convert millisecond timestamp to datetime
                        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                        # Normalize to daily granularity (00:00 UTC)
                        pulled_at = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                        
                        records.append({
                            'pulled_at': pulled_at,
                            'asset': asset,
                            'metric_name': metric_name,
                            'value': float(value)
                        })
                    except (ValueError, TypeError, OSError) as e:
                        continue
        
        return records
    
    def chunk_date_range(self, start_date: datetime, end_date: datetime) -> list:
        """Split date range into chunks to avoid API timeouts."""
        chunks = []
        current = start_date
        
        while current < end_date:
            chunk_end = min(current + timedelta(days=MAX_DAYS_PER_CHUNK), end_date)
            chunks.append((current, chunk_end))
            current = chunk_end
        
        return chunks
    
    def backfill_coin(self, coin_id: str, asset: str, start_date: datetime, end_date: datetime) -> list:
        """Backfill historical data for a single coin."""
        all_records = []
        chunks = self.chunk_date_range(start_date, end_date)
        
        for chunk_start, chunk_end in chunks:
            data = self.fetch_market_chart_range(coin_id, chunk_start, chunk_end)
            
            if data:
                records = self.parse_timeseries(data, asset)
                all_records.extend(records)
            else:
                print(f"[CoinGecko]   No data for {coin_id} ({chunk_start.date()} - {chunk_end.date()})")
        
        return all_records
    
    def run_backfill(
        self,
        start_date: datetime,
        end_date: datetime,
        coins: list = None,
        config_path: str = None
    ) -> int:
        """
        Run the full backfill process.
        
        Args:
            start_date: Start of backfill period
            end_date: End of backfill period
            coins: Optional list of coin IDs to backfill (overrides config)
            config_path: Optional path to config CSV
            
        Returns:
            Total number of records inserted
        """
        print("\n" + "=" * 70)
        print("COINGECKO HISTORICAL BACKFILL")
        print("=" * 70)
        print(f"Period: {start_date.date()} to {end_date.date()}")
        print(f"API: {'Pro' if USE_PRO_API else 'Demo'} ({RATE_LIMIT_CALLS_PER_MIN} calls/min)")
        if self.dry_run:
            print("Mode: DRY RUN (no data will be inserted)")
        print()
        
        # Load entity cache for ID resolution
        self.entity_cache = load_entity_cache()
        if self.entity_cache:
            print(f"[CoinGecko] Loaded {len(self.entity_cache)} entity mappings")
        
        # Determine which coins to backfill
        if coins:
            # Direct list of coin IDs provided
            coin_list = [(cid, cid) for cid in coins]  # (coingecko_id, asset_name)
            print(f"[CoinGecko] Processing {len(coin_list)} coins from command line")
        else:
            # Load from config
            symbol_to_id = self.load_config(config_path)
            if not symbol_to_id:
                print("[CoinGecko] No coins configured. Use --coins or provide config file.")
                return 0
            # Build list: (coingecko_id, resolved_asset_name)
            coin_list = []
            for symbol, cg_id in symbol_to_id.items():
                # Resolve to entity_id if available, else use coingecko_id
                asset = self.entity_cache.get(cg_id, cg_id)
                coin_list.append((cg_id, asset))
        
        total_records = 0
        successful_coins = 0
        failed_coins = []
        
        print(f"\n[CoinGecko] Starting backfill for {len(coin_list)} coins...")
        print("-" * 70)
        
        for i, (cg_id, asset) in enumerate(coin_list, 1):
            print(f"[{i}/{len(coin_list)}] {cg_id} -> {asset}")
            
            try:
                records = self.backfill_coin(cg_id, asset, start_date, end_date)
                
                if records:
                    inserted = insert_historical_metrics(records, self.dry_run)
                    total_records += inserted
                    successful_coins += 1
                    
                    # Count unique dates
                    unique_dates = len(set(r['pulled_at'] for r in records))
                    print(f"         {len(records)} records ({unique_dates} days)")
                else:
                    print(f"         No data available")
                    failed_coins.append(cg_id)
                    
            except Exception as e:
                print(f"         Error: {e}")
                failed_coins.append(cg_id)
        
        # Log the backfill operation
        if not self.dry_run:
            status = "success" if total_records > 0 else "no_data"
            log_pull(status, total_records)
        
        # Summary
        print("\n" + "=" * 70)
        print("BACKFILL COMPLETE")
        print("=" * 70)
        print(f"  Period: {start_date.date()} to {end_date.date()}")
        print(f"  Coins processed: {successful_coins}/{len(coin_list)}")
        print(f"  Total records: {total_records}")
        print(f"  API calls: {self.api_calls}")
        if failed_coins:
            print(f"  Failed coins: {failed_coins[:10]}{'...' if len(failed_coins) > 10 else ''}")
        print("=" * 70)
        
        return total_records


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill historical data from CoinGecko API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python coingecko_backfill.py --start-date 2024-01-01 --end-date 2024-06-30
  python coingecko_backfill.py --days 90
  python coingecko_backfill.py --start-date 2024-01-01 --coins bitcoin,ethereum
  python coingecko_backfill.py --start-date 2024-01-01 --dry-run
        """
    )
    
    # Date range options
    date_group = parser.add_argument_group('Date Range')
    date_group.add_argument(
        '--start-date',
        type=str,
        help='Start date for backfill (YYYY-MM-DD)'
    )
    date_group.add_argument(
        '--end-date',
        type=str,
        help='End date for backfill (YYYY-MM-DD, default: today)'
    )
    date_group.add_argument(
        '--days',
        type=int,
        help='Number of days to backfill (alternative to --start-date)'
    )
    
    # Filter options
    filter_group = parser.add_argument_group('Filters')
    filter_group.add_argument(
        '--coins',
        type=str,
        help='Comma-separated list of CoinGecko coin IDs to backfill'
    )
    filter_group.add_argument(
        '--config',
        type=str,
        help='Path to coingecko_config.csv file'
    )
    
    # Execution options
    exec_group = parser.add_argument_group('Execution')
    exec_group.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without inserting data'
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Determine date range
    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    
    if args.days:
        start_date = end_date - timedelta(days=args.days)
    elif args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    else:
        print("Error: Must specify either --start-date or --days")
        sys.exit(1)
    
    # Validate date range
    if start_date >= end_date:
        print("Error: Start date must be before end date")
        sys.exit(1)
    
    # Parse coins filter
    coins = None
    if args.coins:
        coins = [c.strip().lower() for c in args.coins.split(',') if c.strip()]
    
    # Run backfill
    backfiller = CoinGeckoBackfill(dry_run=args.dry_run)
    
    try:
        total = backfiller.run_backfill(
            start_date=start_date,
            end_date=end_date,
            coins=coins,
            config_path=args.config
        )
        sys.exit(0 if total > 0 else 1)
    except KeyboardInterrupt:
        print("\n[CoinGecko] Backfill interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n[CoinGecko] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
