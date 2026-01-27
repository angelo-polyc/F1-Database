#!/usr/bin/env python3
"""
Alpha Vantage Historical Backfill Script

Backfills daily OHLCV data for equities (crypto-related stocks like MSTR, COIN, etc.)
from Alpha Vantage API for specified date ranges.

Usage:
    python backfill_alphavantage.py                              # Default: 3 years
    python backfill_alphavantage.py --days 365                   # Last 365 days
    python backfill_alphavantage.py --start-date 2022-01-01 --end-date 2025-01-27
    python backfill_alphavantage.py --entities MSTR,COIN,MARA    # Filter to specific tickers
    python backfill_alphavantage.py --dry-run                    # Preview without inserting

Note: 
    - Free tier: 25 API calls/day (very limited for backfill)
    - Premium tiers provide higher rate limits
    - Each ticker requires 1 API call (full history returned per call)
    - 20+ years of historical data available

API Metrics (direct from TIME_SERIES_DAILY_ADJUSTED):
    - OPEN, HIGH, LOW, CLOSE, ADJUSTED_CLOSE, VOLUME
    - DIVIDEND_AMOUNT, SPLIT_COEFFICIENT
"""

import os
import csv
import time
import argparse
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

from db.setup import get_connection


# ============================================================================
# CONFIGURATION
# ============================================================================

# Rate limiting (conservative for premium tier: ~24 req/min)
REQUEST_DELAY = 2.5  # seconds between requests

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 60  # seconds to wait on rate limit

# Metrics to extract (all directly from API - no client-side calculations)
METRICS = [
    'OPEN', 'HIGH', 'LOW', 'CLOSE', 'ADJUSTED_CLOSE', 
    'VOLUME', 'DIVIDEND_AMOUNT', 'SPLIT_COEFFICIENT'
]

# Default backfill: 3 years
DEFAULT_DAYS = 365 * 3


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
        """Load tickers from CSV config."""
        if not os.path.exists(self.config_path):
            print(f"Config file not found: {self.config_path}")
            return []
        
        tickers = []
        with open(self.config_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('pull', '').strip() == '1':
                    symbol = row['symbol'].strip().upper()
                    
                    if filter_entities and symbol not in filter_entities:
                        continue
                    
                    tickers.append({
                        'symbol': symbol,
                        'exchange': row.get('exchange', '').strip(),
                        'category': row.get('category', '').strip()
                    })
        
        return tickers
    
    def fetch_full_history(self, symbol: str) -> Optional[Dict]:
        """
        Fetch full historical daily data for a symbol.
        
        Uses TIME_SERIES_DAILY_ADJUSTED with outputsize=full to get
        20+ years of history including split/dividend adjustments.
        """
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol,
            "apikey": self.api_key,
            "outputsize": "full"
        }
        
        for attempt in range(MAX_RETRIES):
            self._rate_limit()
            self._request_count += 1
            
            try:
                response = requests.get(self.base_url, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                if "Error Message" in data:
                    print(f"API Error - {data['Error Message']}")
                    return None
                
                if "Note" in data:
                    print(f"Rate limit hit, waiting {RETRY_DELAY}s...")
                    time.sleep(RETRY_DELAY)
                    continue
                
                if "Information" in data:
                    print(f"API Info - {data['Information']}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                        continue
                    return None
                
                time_series = data.get("Time Series (Daily)", {})
                if not time_series:
                    print("No time series data")
                    return None
                
                return time_series
                
            except requests.exceptions.RequestException as e:
                print(f"Request failed - {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
                    continue
                return None
        
        return None
    
    def parse_historical_data(self, symbol: str, time_series: Dict, 
                               start_date: datetime, end_date: datetime) -> List[Dict]:
        """Parse time series data into records for database insertion."""
        records = []
        
        for date_str, values in time_series.items():
            try:
                date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                
                if date < start_date or date > end_date:
                    continue
                
                # Extract all API-provided values (no calculations)
                open_price = float(values["1. open"])
                high_price = float(values["2. high"])
                low_price = float(values["3. low"])
                close_price = float(values["4. close"])
                adjusted_close = float(values.get("5. adjusted close", close_price))
                volume = int(values.get("6. volume", 0))
                dividend_amount = float(values.get("7. dividend amount", 0))
                split_coefficient = float(values.get("8. split coefficient", 1))
                
                pulled_at = date.replace(hour=0, minute=0, second=0, microsecond=0)
                
                metric_values = {
                    'OPEN': open_price,
                    'HIGH': high_price,
                    'LOW': low_price,
                    'CLOSE': close_price,
                    'ADJUSTED_CLOSE': adjusted_close,
                    'VOLUME': volume,
                    'DIVIDEND_AMOUNT': dividend_amount,
                    'SPLIT_COEFFICIENT': split_coefficient
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
                
            except (KeyError, ValueError):
                continue
        
        return records
    
    def insert_historical_metrics(self, records: List[Dict]) -> int:
        """Insert historical records with ON CONFLICT DO NOTHING (safe for backfills)."""
        if not records:
            return 0
        
        conn = get_connection()
        cur = conn.cursor()
        
        from psycopg2.extras import execute_values
        
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
        
        query = """
            INSERT INTO metrics (pulled_at, source, asset, metric_name, value, exchange, granularity)
            VALUES %s
            ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) 
            DO NOTHING
        """
        
        try:
            execute_values(cur, query, rows, page_size=1000)
            conn.commit()
            inserted = cur.rowcount if cur.rowcount >= 0 else len(rows)
        except Exception as e:
            print(f"Database error: {e}")
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
            dry_run: If True, don't insert data
        
        Returns:
            Total number of records inserted
        """
        print("=" * 70)
        print("ALPHA VANTAGE HISTORICAL BACKFILL")
        print("=" * 70)
        print(f"Date range: {start_date.date()} to {end_date.date()}")
        if dry_run:
            print("DRY RUN - no data will be inserted")
        
        days_diff = (end_date - start_date).days
        print(f"Days to backfill: {days_diff}")
        
        tickers = self.load_tickers(filter_entities=entities)
        if not tickers:
            print("No tickers to process")
            if not dry_run:
                self.log_pull("no_data", 0)
            return 0
        
        print(f"Tickers: {len(tickers)}")
        print(f"Metrics: {', '.join(METRICS)}")
        
        estimated_time = len(tickers) * REQUEST_DELAY / 60
        print(f"Estimated time: {estimated_time:.1f} minutes")
        print("=" * 70)
        
        if dry_run:
            print("\n[DRY RUN] Would process the following tickers:")
            for t in tickers:
                print(f"  {t['symbol']} ({t.get('category', 'N/A')})")
            return 0
        
        total_records = 0
        success_count = 0
        all_records = []
        start_time = time.time()
        
        for i, ticker_info in enumerate(tickers, 1):
            symbol = ticker_info['symbol']
            print(f"[{i:2}/{len(tickers)}] {symbol}...", end=" ", flush=True)
            
            time_series = self.fetch_full_history(symbol)
            
            if time_series:
                records = self.parse_historical_data(symbol, time_series, start_date, end_date)
                
                if records:
                    all_records.extend(records)
                    success_count += 1
                    
                    dates = [r['pulled_at'] for r in records if r['metric_name'] == 'CLOSE']
                    if dates:
                        min_date = min(dates).date()
                        max_date = max(dates).date()
                        print(f"{len(dates)} days ({min_date} to {max_date})")
                    else:
                        print(f"{len(records)} records")
                else:
                    print("no data in range")
            else:
                print("failed")
            
            if len(all_records) >= 10000:
                inserted = self.insert_historical_metrics(all_records)
                total_records += inserted
                print(f"  [Batch insert: {inserted:,} records]")
                all_records = []
        
        if all_records:
            inserted = self.insert_historical_metrics(all_records)
            total_records += inserted
        
        status = "success" if total_records > 0 else "no_data"
        self.log_pull(status, total_records)
        
        elapsed = time.time() - start_time
        
        print()
        print("=" * 70)
        print("BACKFILL COMPLETE")
        print("=" * 70)
        print(f"  Date range: {start_date.date()} to {end_date.date()}")
        print(f"  Tickers processed: {success_count}/{len(tickers)}")
        print(f"  Total records inserted: {total_records:,}")
        print(f"  Time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
        if elapsed > 0 and total_records > 0:
            print(f"  Rate: {total_records / elapsed:.0f} records/sec")
        print(f"  (Duplicates skipped via ON CONFLICT DO NOTHING)")
        print("=" * 70)
        
        return total_records


# ============================================================================
# CLI MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Backfill Alpha Vantage historical equity data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python backfill_alphavantage.py                               # Default: 3 years
  python backfill_alphavantage.py --days 365                    # Last year
  python backfill_alphavantage.py --start-date 2022-01-01
  python backfill_alphavantage.py --entities MSTR,COIN,MARA
  python backfill_alphavantage.py --dry-run
        """
    )
    
    parser.add_argument('--start-date', type=str, 
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, 
                        help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--days', type=int, default=DEFAULT_DAYS,
                        help=f'Number of days to backfill (default: {DEFAULT_DAYS})')
    parser.add_argument('--entities', type=str, 
                        help='Comma-separated list of ticker symbols (e.g., MSTR,COIN)')
    parser.add_argument('--config', type=str, default='alphavantage_config.csv',
                        help='Path to config CSV file')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without inserting data')
    
    args = parser.parse_args()
    
    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start_date = end_date - timedelta(days=args.days)
    
    if start_date >= end_date:
        print(f"Error: Start date ({start_date.date()}) must be before end date ({end_date.date()})")
        return 1
    
    entities = None
    if args.entities:
        entities = [e.strip().upper() for e in args.entities.split(',')]
    
    try:
        source = AlphaVantageBackfillSource(config_path=args.config)
        source.backfill(
            start_date=start_date,
            end_date=end_date,
            entities=entities,
            dry_run=args.dry_run
        )
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
