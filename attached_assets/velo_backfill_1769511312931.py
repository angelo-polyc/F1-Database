#!/usr/bin/env python3
"""
Velo.xyz Historical Backfill Script

Backfills hourly derivatives data (funding rates, OI, liquidations, OHLCV)
from Velo API for specified date ranges.

Usage:
    python velo_backfill.py --start-date 2024-01-01 --end-date 2024-06-30
    python velo_backfill.py --days 90
    python velo_backfill.py --start-date 2024-01-01 --entities BTC,ETH --exchanges binance-futures,bybit
    python velo_backfill.py --start-date 2024-01-01 --dry-run

Note: Velo monthly plan provides 3-month history. For 3-year backfill, 
      yearly subscription with full history access is required.
"""

import os
import csv
import time
import argparse
import requests
from datetime import datetime, timezone, timedelta
from requests.auth import HTTPBasicAuth
from typing import Optional, List, Dict, Tuple

# Import database connection - adjust path as needed for your setup
try:
    from db.setup import get_connection
except ImportError:
    # Fallback for standalone testing
    def get_connection():
        import psycopg2
        return psycopg2.connect(
            host=os.environ.get("DB_HOST", "localhost"),
            database=os.environ.get("DB_NAME", "crypto_db"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", "")
        )

# ============================================================================
# CONFIGURATION
# ============================================================================

# Rate limit: 120 requests per 30 seconds = 4/sec max
# Use 2/sec to stay safely under limit
REQUEST_DELAY = 0.5

# Velo API returns 22,500 values max per request
# With 5 columns * 24 hours * 30 days = 3600 cells per coin per exchange
# Safe batch size: 5 coins per request for monthly chunks
BATCH_SIZE = 5

# Date chunking: process 7 days at a time for hourly data
# 7 days * 24 hours * 5 columns * 5 coins = 4200 cells (well under 22,500)
DATE_CHUNK_DAYS = 7

# Columns to fetch for backfill (same as live pull)
BACKFILL_COLUMNS = [
    'close_price',                   # CLOSE_PRICE
    'dollar_volume',                 # DOLLAR_VOLUME
    'dollar_open_interest_close',    # DOLLAR_OI_CLOSE
    'funding_rate_avg',              # FUNDING_RATE_AVG
    'liquidations_dollar_volume'     # LIQ_DOLLAR_VOL
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

# Supported exchanges
EXCHANGES = ['binance-futures', 'bybit', 'okex-swap', 'hyperliquid']

# Maximum lookback (3 years)
MAX_LOOKBACK_DAYS = 365 * 3  # 1095 days


# ============================================================================
# VELO BACKFILL SOURCE CLASS
# ============================================================================

class VeloBackfillSource:
    """
    Velo.xyz historical backfill source.
    
    Pulls hourly futures data including funding rates, open interest,
    liquidations, and OHLCV for all configured coin-exchange pairs
    within a specified date range.
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
        self._entity_cache = {}
        self._request_count = 0
        self._last_request_time = 0
    
    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self._last_request_time = time.time()
    
    def load_config(self, filter_entities: List[str] = None, 
                    filter_exchanges: List[str] = None) -> List[Dict]:
        """
        Load coin-exchange pairs from CSV config.
        
        Args:
            filter_entities: Optional list of coins to include (uppercase)
            filter_exchanges: Optional list of exchanges to include (lowercase)
        
        Returns:
            List of {coin, exchange} dicts
        """
        if not os.path.exists(self.config_path):
            print(f"[Velo Backfill] Config file not found: {self.config_path}")
            return []
        
        config = []
        with open(self.config_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('pull', '1') == '1':
                    coin = row['coin'].upper()
                    exchange = row['exchange'].lower()
                    
                    # Apply filters if provided
                    if filter_entities and coin not in filter_entities:
                        continue
                    if filter_exchanges and exchange not in filter_exchanges:
                        continue
                    
                    config.append({
                        'coin': coin,
                        'exchange': exchange
                    })
        
        print(f"[Velo Backfill] Loaded {len(config)} coin-exchange pairs from config")
        return config
    
    def load_entity_cache(self):
        """Load entity_id mappings from database."""
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
            print(f"[Velo Backfill] Loaded {len(self._entity_cache)} entity mappings")
        except Exception as e:
            print(f"[Velo Backfill] Warning: Could not load entity cache: {e}")
            self._entity_cache = {}
    
    def fetch_historical_data(self, exchange: str, coins: List[str], 
                               begin_ts: int, end_ts: int,
                               columns: List[str] = None) -> List[Dict]:
        """
        Fetch historical data for a batch of coins from a single exchange.
        
        Args:
            exchange: Exchange name (e.g., 'binance-futures')
            coins: List of coin symbols (e.g., ['BTC', 'ETH'])
            begin_ts: Start timestamp in milliseconds
            end_ts: End timestamp in milliseconds
            columns: List of columns to fetch (defaults to BACKFILL_COLUMNS)
        
        Returns:
            List of record dicts ready for database insertion
        """
        if columns is None:
            columns = BACKFILL_COLUMNS
        
        records = []
        coins_str = ','.join(coins)
        columns_str = ','.join(columns)
        
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
        
        self._rate_limit()
        self._request_count += 1
        
        try:
            resp = requests.get(url, auth=self.auth, timeout=60)
            
            if resp.status_code == 429:
                print(f"[Velo Backfill] Rate limited, waiting 30 seconds...")
                time.sleep(30)
                resp = requests.get(url, auth=self.auth, timeout=60)
            
            if resp.status_code != 200:
                print(f"[Velo Backfill] API error {resp.status_code} for {exchange}")
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
                
                coin_val = row.get('coin', '')
                exch = row.get('exchange', '')
                entity_id = self._entity_cache.get(coin_val)
                
                # Create records for each metric column
                for velo_col in columns:
                    if velo_col in row and row[velo_col]:
                        try:
                            value = float(row[velo_col])
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
                        except ValueError:
                            pass
            
        except requests.RequestException as e:
            print(f"[Velo Backfill] Request error for {exchange}: {e}")
        
        return records
    
    def insert_historical_metrics(self, records: List[Dict]) -> int:
        """
        Insert historical records with upsert logic.
        
        Args:
            records: List of record dicts
        
        Returns:
            Number of records inserted/updated
        """
        if not records:
            return 0
        
        conn = get_connection()
        cur = conn.cursor()
        
        try:
            from psycopg2.extras import execute_values
        except ImportError:
            # Fallback to executemany if execute_values not available
            execute_values = None
        
        rows = [
            (
                r['pulled_at'],
                self.source_name,
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
            ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) 
            DO UPDATE SET 
                value = EXCLUDED.value,
                entity_id = COALESCE(EXCLUDED.entity_id, metrics.entity_id),
                domain = COALESCE(EXCLUDED.domain, metrics.domain),
                granularity = COALESCE(EXCLUDED.granularity, metrics.granularity)
        """
        
        try:
            if execute_values:
                execute_values(cur, query, rows, page_size=1000)
            else:
                # Fallback
                cur.executemany(query.replace('%s', '(%s, %s, %s, %s, %s, %s, %s, %s, %s)'), rows)
            
            inserted = len(rows)
            conn.commit()
        except Exception as e:
            print(f"[Velo Backfill] Database error: {e}")
            conn.rollback()
            inserted = 0
        finally:
            cur.close()
            conn.close()
        
        return inserted
    
    def log_pull(self, status: str, records_count: int) -> int:
        """Log the backfill operation to the pulls table."""
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pulls (source_name, pulled_at, status, records_count)
            VALUES (%s, %s, %s, %s)
            RETURNING pull_id
            """,
            ("velo_backfill", datetime.utcnow(), status, records_count)
        )
        pull_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return pull_id
    
    def generate_date_chunks(self, start_date: datetime, end_date: datetime, 
                              chunk_days: int = DATE_CHUNK_DAYS) -> List[Tuple[datetime, datetime]]:
        """
        Generate date chunks for processing.
        
        Args:
            start_date: Start of backfill range
            end_date: End of backfill range
            chunk_days: Number of days per chunk
        
        Returns:
            List of (chunk_start, chunk_end) tuples
        """
        chunks = []
        current = start_date
        
        while current < end_date:
            chunk_end = min(current + timedelta(days=chunk_days), end_date)
            chunks.append((current, chunk_end))
            current = chunk_end
        
        return chunks
    
    def backfill(self, start_date: datetime, end_date: datetime,
                 entities: List[str] = None, exchanges: List[str] = None,
                 dry_run: bool = False) -> int:
        """
        Main backfill orchestration method.
        
        Args:
            start_date: Start of backfill range (UTC)
            end_date: End of backfill range (UTC)
            entities: Optional list of coins to backfill (uppercase)
            exchanges: Optional list of exchanges to backfill (lowercase)
            dry_run: If True, don't insert data, just show what would be done
        
        Returns:
            Total number of records inserted
        """
        print("=" * 70)
        print("VELO HISTORICAL BACKFILL")
        print("=" * 70)
        print(f"Date range: {start_date.date()} to {end_date.date()}")
        print(f"Dry run: {dry_run}")
        
        # Validate date range
        days_diff = (end_date - start_date).days
        if days_diff > MAX_LOOKBACK_DAYS:
            print(f"[Velo Backfill] Warning: Requested {days_diff} days exceeds max lookback of {MAX_LOOKBACK_DAYS} days")
            print("[Velo Backfill] Note: Full 3-year history requires Velo yearly subscription")
        
        # Load configuration
        config = self.load_config(filter_entities=entities, filter_exchanges=exchanges)
        if not config:
            print("[Velo Backfill] No coin-exchange pairs to process")
            if not dry_run:
                self.log_pull("no_data", 0)
            return 0
        
        # Load entity mappings
        if not dry_run:
            self.load_entity_cache()
        
        # Group by exchange for efficient batching
        exchange_coins = {}
        for item in config:
            ex = item['exchange']
            if ex not in exchange_coins:
                exchange_coins[ex] = []
            if item['coin'] not in exchange_coins[ex]:
                exchange_coins[ex].append(item['coin'])
        
        print(f"Exchanges: {list(exchange_coins.keys())}")
        print(f"Total coins per exchange: {', '.join(f'{ex}={len(coins)}' for ex, coins in exchange_coins.items())}")
        
        # Generate date chunks
        date_chunks = self.generate_date_chunks(start_date, end_date)
        print(f"Date chunks: {len(date_chunks)} (processing {DATE_CHUNK_DAYS} days at a time)")
        
        # Calculate estimated API calls
        total_batches = sum(
            (len(coins) + BATCH_SIZE - 1) // BATCH_SIZE 
            for coins in exchange_coins.values()
        )
        total_api_calls = total_batches * len(date_chunks)
        estimated_time = total_api_calls * REQUEST_DELAY / 60
        
        print(f"Estimated API calls: {total_api_calls}")
        print(f"Estimated time: {estimated_time:.1f} minutes")
        print("=" * 70)
        
        if dry_run:
            print("\n[DRY RUN] Would process the following:")
            for ex, coins in exchange_coins.items():
                print(f"  {ex}: {', '.join(coins[:10])}{'...' if len(coins) > 10 else ''}")
            print(f"\n[DRY RUN] Date chunks:")
            for i, (chunk_start, chunk_end) in enumerate(date_chunks[:5]):
                print(f"  Chunk {i+1}: {chunk_start.date()} to {chunk_end.date()}")
            if len(date_chunks) > 5:
                print(f"  ... and {len(date_chunks) - 5} more chunks")
            return 0
        
        # Process data
        total_records = 0
        all_records = []
        chunk_count = 0
        
        for chunk_idx, (chunk_start, chunk_end) in enumerate(date_chunks):
            chunk_count += 1
            begin_ts = int(chunk_start.timestamp() * 1000)
            end_ts = int(chunk_end.timestamp() * 1000)
            
            print(f"\n[Chunk {chunk_count}/{len(date_chunks)}] {chunk_start.date()} to {chunk_end.date()}")
            
            for exchange, coins in exchange_coins.items():
                # Batch coins
                for batch_start in range(0, len(coins), BATCH_SIZE):
                    batch = coins[batch_start:batch_start + BATCH_SIZE]
                    
                    records = self.fetch_historical_data(
                        exchange=exchange,
                        coins=batch,
                        begin_ts=begin_ts,
                        end_ts=end_ts
                    )
                    
                    all_records.extend(records)
                    
                    if self._request_count % 50 == 0:
                        print(f"  Progress: {self._request_count} API calls, {len(all_records)} records collected...")
            
            # Insert in batches to avoid memory issues
            if len(all_records) >= 50000:
                # Deduplicate before insert
                seen = set()
                unique_records = []
                for r in all_records:
                    key = (r['pulled_at'], r['asset'], r['metric_name'], r.get('exchange', ''))
                    if key not in seen:
                        seen.add(key)
                        unique_records.append(r)
                
                inserted = self.insert_historical_metrics(unique_records)
                total_records += inserted
                print(f"  Batch insert: {inserted} records")
                all_records = []
        
        # Final insert
        if all_records:
            seen = set()
            unique_records = []
            for r in all_records:
                key = (r['pulled_at'], r['asset'], r['metric_name'], r.get('exchange', ''))
                if key not in seen:
                    seen.add(key)
                    unique_records.append(r)
            
            inserted = self.insert_historical_metrics(unique_records)
            total_records += inserted
        
        # Log the pull
        status = "success" if total_records > 0 else "no_data"
        pull_id = self.log_pull(status, total_records)
        
        print("\n" + "=" * 70)
        print("BACKFILL COMPLETE")
        print(f"  Pull ID: {pull_id}")
        print(f"  API calls: {self._request_count}")
        print(f"  Total records inserted: {total_records}")
        print(f"  Date range: {start_date.date()} to {end_date.date()}")
        print("=" * 70)
        
        return total_records


# ============================================================================
# CLI
# ============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill Velo.xyz historical derivatives data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Backfill specific date range
    python velo_backfill.py --start-date 2024-01-01 --end-date 2024-06-30
    
    # Backfill last 90 days
    python velo_backfill.py --days 90
    
    # Backfill specific coins and exchanges
    python velo_backfill.py --start-date 2024-01-01 --entities BTC,ETH,SOL --exchanges binance-futures,bybit
    
    # Dry run to see what would be processed
    python velo_backfill.py --start-date 2024-01-01 --dry-run
    
    # Backfill 3 years (requires Velo yearly subscription)
    python velo_backfill.py --start-date 2022-01-27 --end-date 2025-01-27

Note: Velo monthly subscription provides 3-month history.
      For full 3-year backfill, yearly subscription with full history access is required.
        """
    )
    
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for backfill (YYYY-MM-DD format)"
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for backfill (YYYY-MM-DD format). Defaults to today."
    )
    
    parser.add_argument(
        "--days",
        type=int,
        help="Number of days to backfill from today (alternative to --start-date/--end-date)"
    )
    
    parser.add_argument(
        "--entities",
        type=str,
        help="Comma-separated list of coins to backfill (e.g., BTC,ETH,SOL)"
    )
    
    parser.add_argument(
        "--exchanges",
        type=str,
        help="Comma-separated list of exchanges (e.g., binance-futures,bybit)"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default="velo_config.csv",
        help="Path to config CSV file (default: velo_config.csv)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually fetching/inserting data"
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Determine date range
    if args.days:
        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=args.days)
    elif args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        print("Error: Must specify either --start-date or --days")
        return 1
    
    # Validate date range
    if start_date >= end_date:
        print(f"Error: Start date ({start_date.date()}) must be before end date ({end_date.date()})")
        return 1
    
    # Parse entity and exchange filters
    entities = None
    if args.entities:
        entities = [e.strip().upper() for e in args.entities.split(',')]
    
    exchanges = None
    if args.exchanges:
        exchanges = [e.strip().lower() for e in args.exchanges.split(',')]
        # Validate exchanges
        invalid = set(exchanges) - set(EXCHANGES)
        if invalid:
            print(f"Warning: Unknown exchanges: {invalid}")
            print(f"Valid exchanges: {EXCHANGES}")
    
    # Initialize and run backfill
    try:
        source = VeloBackfillSource(config_path=args.config)
        total = source.backfill(
            start_date=start_date,
            end_date=end_date,
            entities=entities,
            exchanges=exchanges,
            dry_run=args.dry_run
        )
        
        if args.dry_run:
            print("\n[DRY RUN] No data was inserted")
        else:
            print(f"\nBackfill complete: {total} records inserted")
        
        return 0
    
    except ValueError as e:
        print(f"Configuration error: {e}")
        return 1
    except Exception as e:
        print(f"Error during backfill: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
