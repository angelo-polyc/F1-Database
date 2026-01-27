#!/usr/bin/env python3
"""
Alpha Vantage Historical Backfill Script

Backfills daily OHLCV data for equities (crypto-related stocks like MSTR, COIN, etc.)
from Alpha Vantage API for specified date ranges.

Usage:
    python alphavantage_backfill.py --start-date 2022-01-01 --end-date 2025-01-27
    python alphavantage_backfill.py --days 365
    python alphavantage_backfill.py --start-date 2024-01-01 --entities MSTR,COIN,MARA
    python alphavantage_backfill.py --start-date 2024-01-01 --dry-run

Note: 
    - Free tier: 25 API calls/day (very limited for backfill)
    - Premium tiers provide higher rate limits
    - Each ticker requires 1 API call (full history returned per call)
    - 20+ years of historical data available
"""

import os
import csv
import time
import argparse
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

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

# Rate limiting
# Free tier: 25 calls/day, Premium: up to 1200 calls/min depending on plan
# Using conservative delay for premium tier (30 req/min = 2s between requests)
REQUEST_DELAY = 2.5  # seconds between requests

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 60  # seconds to wait on rate limit

# Metrics to extract
METRICS = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'ADJUSTED_CLOSE', 'VOLUME', 'DOLLAR_VOLUME']

# Maximum lookback (Alpha Vantage provides 20+ years)
MAX_LOOKBACK_DAYS = 365 * 20


# ============================================================================
# ALPHA VANTAGE BACKFILL SOURCE CLASS
# ============================================================================

class AlphaVantageBackfillSource:
    """
    Alpha Vantage historical backfill source.
    
    Pulls daily OHLCV data for equities using TIME_SERIES_DAILY_ADJUSTED
    endpoint with full output size (20+ years of history).
    """
    
    @property
    def source_name(self) -> str:
        return "alphavantage"
    
    def __init__(self, config_path: str = "alphavantage_config.csv"):
        self.api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY environment variable not set")
        self.config_path = config_path
        self.base_url = "https://www.alphavantage.co/query"
        self._request_count = 0
        self._last_request_time = 0
    
    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self._last_request_time = time.time()
    
    def load_tickers(self, filter_entities: List[str] = None) -> List[Dict]:
        """
        Load tickers from CSV config.
        
        Args:
            filter_entities: Optional list of symbols to include (uppercase)
        
        Returns:
            List of ticker dicts with symbol, exchange, category
        """
        if not os.path.exists(self.config_path):
            print(f"[AlphaVantage Backfill] Config file not found: {self.config_path}")
            return []
        
        tickers = []
        with open(self.config_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('pull', '').strip() == '1':
                    symbol = row['symbol'].strip().upper()
                    
                    # Apply filter if provided
                    if filter_entities and symbol not in filter_entities:
                        continue
                    
                    tickers.append({
                        'symbol': symbol,
                        'exchange': row.get('exchange', '').strip(),
                        'category': row.get('category', '').strip()
                    })
        
        print(f"[AlphaVantage Backfill] Loaded {len(tickers)} tickers from config")
        return tickers
    
    def fetch_full_history(self, symbol: str) -> Optional[Dict]:
        """
        Fetch full historical daily data for a symbol.
        
        Uses TIME_SERIES_DAILY_ADJUSTED with outputsize=full to get
        20+ years of history including split/dividend adjustments.
        
        Args:
            symbol: Stock ticker symbol (e.g., 'MSTR')
        
        Returns:
            Dict with date keys and OHLCV values, or None on error
        """
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol,
            "apikey": self.api_key,
            "outputsize": "full"  # Get full history (20+ years)
        }
        
        for attempt in range(MAX_RETRIES):
            self._rate_limit()
            self._request_count += 1
            
            try:
                response = requests.get(self.base_url, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                # Check for API errors
                if "Error Message" in data:
                    print(f"  {symbol}: API Error - {data['Error Message']}")
                    return None
                
                if "Note" in data:
                    # Rate limit hit
                    print(f"  {symbol}: Rate limit hit, waiting {RETRY_DELAY}s...")
                    time.sleep(RETRY_DELAY)
                    continue
                
                if "Information" in data:
                    print(f"  {symbol}: API Info - {data['Information']}")
                    # This usually means rate limit or invalid key
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                        continue
                    return None
                
                # Extract time series data
                time_series = data.get("Time Series (Daily)", {})
                if not time_series:
                    print(f"  {symbol}: No time series data")
                    return None
                
                return time_series
                
            except requests.exceptions.RequestException as e:
                print(f"  {symbol}: Request failed - {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
                    continue
                return None
        
        return None
    
    def parse_historical_data(self, symbol: str, time_series: Dict, 
                               start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Parse time series data into records for database insertion.
        
        Args:
            symbol: Stock ticker symbol
            time_series: Dict of date -> OHLCV data from API
            start_date: Start of date range to include
            end_date: End of date range to include
        
        Returns:
            List of record dicts ready for database insertion
        """
        records = []
        
        for date_str, values in time_series.items():
            try:
                # Parse date
                date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                
                # Filter by date range
                if date < start_date or date > end_date:
                    continue
                
                # Extract values
                open_price = float(values["1. open"])
                high_price = float(values["2. high"])
                low_price = float(values["3. low"])
                close_price = float(values["4. close"])
                adjusted_close = float(values.get("5. adjusted close", close_price))
                volume = int(values.get("6. volume", values.get("5. volume", 0)))
                dollar_volume = close_price * volume
                
                # Normalize timestamp to midnight UTC
                pulled_at = date.replace(hour=0, minute=0, second=0, microsecond=0)
                
                # Create records for each metric
                metric_values = {
                    'OPEN': open_price,
                    'HIGH': high_price,
                    'LOW': low_price,
                    'CLOSE': close_price,
                    'ADJUSTED_CLOSE': adjusted_close,
                    'VOLUME': volume,
                    'DOLLAR_VOLUME': dollar_volume
                }
                
                for metric_name, value in metric_values.items():
                    if value is not None:
                        records.append({
                            'pulled_at': pulled_at,
                            'asset': symbol.lower(),
                            'metric_name': metric_name,
                            'value': value,
                            'granularity': 'daily'
                        })
                
            except (KeyError, ValueError) as e:
                # Skip malformed entries
                continue
        
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
        
        # Prepare rows for insertion
        # Alpha Vantage doesn't use exchange column (NULL for stocks)
        rows = [
            (
                r['pulled_at'],
                self.source_name,
                r['asset'],
                r['metric_name'],
                r['value'],
                None,  # exchange is NULL for Alpha Vantage
                r.get('granularity', 'daily')
            )
            for r in records
        ]
        
        # Use executemany with upsert
        query = """
            INSERT INTO metrics (pulled_at, source, asset, metric_name, value, exchange, granularity)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) 
            DO UPDATE SET 
                value = EXCLUDED.value,
                granularity = COALESCE(EXCLUDED.granularity, metrics.granularity)
        """
        
        try:
            # Try to use execute_values for efficiency
            try:
                from psycopg2.extras import execute_values
                insert_query = """
                    INSERT INTO metrics (pulled_at, source, asset, metric_name, value, exchange, granularity)
                    VALUES %s
                    ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) 
                    DO UPDATE SET 
                        value = EXCLUDED.value,
                        granularity = COALESCE(EXCLUDED.granularity, metrics.granularity)
                """
                execute_values(cur, insert_query, rows, page_size=1000)
            except ImportError:
                cur.executemany(query, rows)
            
            inserted = len(rows)
            conn.commit()
        except Exception as e:
            print(f"[AlphaVantage Backfill] Database error: {e}")
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
            ("alphavantage_backfill", datetime.utcnow(), status, records_count)
        )
        pull_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return pull_id
    
    def backfill(self, start_date: datetime, end_date: datetime,
                 entities: List[str] = None, dry_run: bool = False) -> int:
        """
        Main backfill orchestration method.
        
        Args:
            start_date: Start of backfill range (UTC)
            end_date: End of backfill range (UTC)
            entities: Optional list of symbols to backfill (uppercase)
            dry_run: If True, don't insert data, just show what would be done
        
        Returns:
            Total number of records inserted
        """
        print("=" * 70)
        print("ALPHA VANTAGE HISTORICAL BACKFILL")
        print("=" * 70)
        print(f"Date range: {start_date.date()} to {end_date.date()}")
        print(f"Dry run: {dry_run}")
        
        # Validate date range
        days_diff = (end_date - start_date).days
        print(f"Days to backfill: {days_diff}")
        
        # Load tickers
        tickers = self.load_tickers(filter_entities=entities)
        if not tickers:
            print("[AlphaVantage Backfill] No tickers to process")
            if not dry_run:
                self.log_pull("no_data", 0)
            return 0
        
        print(f"Tickers: {len(tickers)}")
        print(f"Symbols: {', '.join(t['symbol'] for t in tickers[:10])}{'...' if len(tickers) > 10 else ''}")
        
        # Estimate API calls and time
        estimated_time = len(tickers) * REQUEST_DELAY / 60
        print(f"Estimated API calls: {len(tickers)}")
        print(f"Estimated time: {estimated_time:.1f} minutes")
        print("=" * 70)
        
        if dry_run:
            print("\n[DRY RUN] Would process the following tickers:")
            for t in tickers:
                print(f"  {t['symbol']} ({t.get('category', 'N/A')})")
            return 0
        
        # Process each ticker
        total_records = 0
        success_count = 0
        error_count = 0
        all_records = []
        
        for i, ticker_info in enumerate(tickers, 1):
            symbol = ticker_info['symbol']
            print(f"\n[{i:2}/{len(tickers)}] {symbol}...", end=" ")
            
            # Fetch full history
            time_series = self.fetch_full_history(symbol)
            
            if time_series:
                # Parse and filter by date range
                records = self.parse_historical_data(
                    symbol=symbol,
                    time_series=time_series,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if records:
                    all_records.extend(records)
                    success_count += 1
                    
                    # Calculate date range found
                    dates = [r['pulled_at'] for r in records if r['metric_name'] == 'CLOSE']
                    if dates:
                        min_date = min(dates).date()
                        max_date = max(dates).date()
                        print(f"found {len(dates)} days ({min_date} to {max_date})")
                    else:
                        print(f"found {len(records)} records")
                else:
                    print("no data in date range")
                    error_count += 1
            else:
                error_count += 1
            
            # Batch insert periodically to manage memory
            if len(all_records) >= 10000:
                inserted = self.insert_historical_metrics(all_records)
                total_records += inserted
                print(f"  [Batch insert: {inserted} records]")
                all_records = []
        
        # Final insert
        if all_records:
            inserted = self.insert_historical_metrics(all_records)
            total_records += inserted
        
        # Log the pull
        status = "success" if total_records > 0 else ("partial" if success_count > 0 else "no_data")
        pull_id = self.log_pull(status, total_records)
        
        print("\n" + "=" * 70)
        print("BACKFILL COMPLETE")
        print(f"  Pull ID: {pull_id}")
        print(f"  Tickers: {success_count}/{len(tickers)} successful")
        print(f"  API calls: {self._request_count}")
        print(f"  Total records inserted: {total_records}")
        print(f"  Errors: {error_count}")
        print("=" * 70)
        
        return total_records


# ============================================================================
# CLI
# ============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill Alpha Vantage historical equity data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Backfill specific date range
    python alphavantage_backfill.py --start-date 2022-01-01 --end-date 2025-01-27
    
    # Backfill last 365 days
    python alphavantage_backfill.py --days 365
    
    # Backfill specific tickers only
    python alphavantage_backfill.py --start-date 2024-01-01 --entities MSTR,COIN,MARA
    
    # Dry run to see what would be processed
    python alphavantage_backfill.py --start-date 2024-01-01 --dry-run
    
    # Backfill 3 years
    python alphavantage_backfill.py --start-date 2022-01-27 --end-date 2025-01-27

Note: 
    - Free tier only allows 25 API calls/day
    - Each ticker requires 1 API call (full history per call)
    - For 38 tickers, you need premium API or ~2 days with free tier
    - 20+ years of historical data available
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
        help="Comma-separated list of ticker symbols to backfill (e.g., MSTR,COIN,MARA)"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default="alphavantage_config.csv",
        help="Path to config CSV file (default: alphavantage_config.csv)"
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
    
    # Parse entity filter
    entities = None
    if args.entities:
        entities = [e.strip().upper() for e in args.entities.split(',')]
    
    # Initialize and run backfill
    try:
        source = AlphaVantageBackfillSource(config_path=args.config)
        total = source.backfill(
            start_date=start_date,
            end_date=end_date,
            entities=entities,
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
