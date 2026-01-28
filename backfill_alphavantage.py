#!/usr/bin/env python3
"""
Alpha Vantage Historical Backfill Script (Hourly)

Backfills hourly OHLCV data for equities (crypto-related stocks like MSTR, COIN, etc.)
from Alpha Vantage API using TIME_SERIES_INTRADAY with 60min interval.

Usage:
    python backfill_alphavantage.py                              # Default: 3 years
    python backfill_alphavantage.py --days 365                   # Last 365 days
    python backfill_alphavantage.py --start-date 2022-01-01 --end-date 2025-01-27
    python backfill_alphavantage.py --entities MSTR,COIN,MARA    # Filter to specific tickers
    python backfill_alphavantage.py --dry-run                    # Preview without inserting

Note: 
    - TIME_SERIES_INTRADAY is a premium endpoint
    - Each month requires a separate API call per ticker
    - 3 years = 36 months per ticker
    - Premium tiers provide higher rate limits

API Metrics (from TIME_SERIES_INTRADAY):
    - OPEN, HIGH, LOW, CLOSE, VOLUME
"""

import os
import csv
import time
import gc
import argparse
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict
from dateutil.relativedelta import relativedelta

from db.setup import get_connection


REQUEST_DELAY = 2.5
MAX_RETRIES = 3
RETRY_DELAY = 60

METRICS = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']

DEFAULT_DAYS = 365 * 3


class AlphaVantageBackfillSource:
    """
    Alpha Vantage historical backfill source for hourly data.
    
    Uses TIME_SERIES_INTRADAY with interval=60min and month parameter
    to fetch historical hourly data month by month.
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
    
    def fetch_intraday_month(self, symbol: str, month: str) -> Optional[Dict]:
        """
        Fetch hourly intraday data for a symbol for a specific month.
        
        Uses TIME_SERIES_INTRADAY with interval=60min and month parameter.
        
        Args:
            symbol: Stock ticker symbol
            month: Month in YYYY-MM format
        
        Returns:
            Dict of timestamp -> OHLCV data, or None on error
        """
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": "60min",
            "month": month,
            "outputsize": "full",
            "adjusted": "true",
            "extended_hours": "true",
            "apikey": self.api_key
        }
        
        for attempt in range(MAX_RETRIES):
            self._rate_limit()
            self._request_count += 1
            
            try:
                response = requests.get(self.base_url, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                if "Error Message" in data:
                    print(f"    API Error for {month}: {data['Error Message'][:50]}")
                    return None
                
                if "Note" in data:
                    print(f"    Rate limit hit, waiting {RETRY_DELAY}s...")
                    time.sleep(RETRY_DELAY)
                    continue
                
                if "Information" in data:
                    print(f"    API Info: {data['Information'][:50]}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                        continue
                    return None
                
                time_series = data.get("Time Series (60min)", {})
                if not time_series:
                    return None
                
                return time_series
                
            except requests.exceptions.RequestException as e:
                print(f"    Request failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
                    continue
                return None
        
        return None
    
    def parse_intraday_data(self, symbol: str, time_series: Dict, 
                            start_date: datetime, end_date: datetime) -> List[Dict]:
        """Parse intraday time series data into records for database insertion."""
        records = []
        
        for timestamp_str, values in time_series.items():
            try:
                dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                
                if dt < start_date or dt > end_date:
                    continue
                
                open_price = float(values["1. open"])
                high_price = float(values["2. high"])
                low_price = float(values["3. low"])
                close_price = float(values["4. close"])
                volume = int(values.get("5. volume", 0))
                
                pulled_at = dt.replace(minute=0, second=0, microsecond=0)
                
                metric_values = {
                    'OPEN': open_price,
                    'HIGH': high_price,
                    'LOW': low_price,
                    'CLOSE': close_price,
                    'VOLUME': volume
                }
                
                for metric_name, value in metric_values.items():
                    if value is not None:
                        records.append({
                            'pulled_at': pulled_at,
                            'asset': symbol.lower(),
                            'metric_name': metric_name,
                            'value': value,
                            'granularity': 'hourly'
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
                None,
                r.get('granularity', 'hourly'),
                r['pulled_at'].date() if hasattr(r['pulled_at'], 'date') else r['pulled_at']
            )
            for r in records
        ]
        
        query = """
            INSERT INTO metrics (pulled_at, source, asset, metric_name, value, exchange, granularity, metric_date)
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
    
    def generate_months(self, start_date: datetime, end_date: datetime) -> List[str]:
        """Generate list of YYYY-MM strings between start and end dates."""
        months = []
        current = start_date.replace(day=1)
        
        while current <= end_date:
            months.append(current.strftime("%Y-%m"))
            current = current + relativedelta(months=1)
        
        return months
    
    def backfill(self, start_date: datetime, end_date: datetime,
                 entities: List[str] = None, dry_run: bool = False) -> int:
        """
        Main backfill orchestration method for hourly data.
        
        Args:
            start_date: Start of backfill range (UTC)
            end_date: End of backfill range (UTC)
            entities: Optional list of symbols to backfill (uppercase)
            dry_run: If True, don't insert data
        
        Returns:
            Total number of records inserted
        """
        print("=" * 70)
        print("ALPHA VANTAGE HISTORICAL BACKFILL (HOURLY)")
        print("=" * 70)
        print(f"Date range: {start_date.date()} to {end_date.date()}")
        print(f"Granularity: hourly (60min intervals)")
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
        
        months = self.generate_months(start_date, end_date)
        
        print(f"Tickers: {len(tickers)}")
        print(f"Months to fetch: {len(months)} ({months[0]} to {months[-1]})")
        print(f"Metrics: {', '.join(METRICS)}")
        
        total_api_calls = len(tickers) * len(months)
        estimated_time = total_api_calls * REQUEST_DELAY / 60
        print(f"Total API calls: {total_api_calls}")
        print(f"Estimated time: {estimated_time:.1f} minutes")
        print("=" * 70)
        
        if dry_run:
            print("\n[DRY RUN] Would process the following tickers:")
            for t in tickers:
                print(f"  {t['symbol']} ({t.get('category', 'N/A')})")
            print(f"\nMonths: {', '.join(months[:6])}{'...' if len(months) > 6 else ''}")
            return 0
        
        total_records = 0
        success_count = 0
        start_time = time.time()
        api_calls_made = 0
        
        for i, ticker_info in enumerate(tickers, 1):
            symbol = ticker_info['symbol']
            ticker_records = 0
            ticker_start = time.time()
            
            print(f"\n[{i:2}/{len(tickers)}] {symbol} ({len(months)} months):")
            
            # Process months in small batches and insert per batch
            MONTHS_PER_BATCH = 6
            for batch_start in range(0, len(months), MONTHS_PER_BATCH):
                batch_months = months[batch_start:batch_start + MONTHS_PER_BATCH]
                batch_records = []
                
                for month in batch_months:
                    api_calls_made += 1
                    time_series = self.fetch_intraday_month(symbol, month)
                    
                    if time_series:
                        records = self.parse_intraday_data(symbol, time_series, start_date, end_date)
                        if records:
                            batch_records.extend(records)
                            ticker_records += len(records)
                
                # Insert after each batch of months
                if batch_records:
                    inserted = self.insert_historical_metrics(batch_records)
                    total_records += inserted
                    del batch_records
                    gc.collect()
            
            if ticker_records > 0:
                success_count += 1
                ticker_elapsed = time.time() - ticker_start
                print(f"  -> {ticker_records:,} records in {ticker_elapsed:.0f}s")
            else:
                print(f"  -> no data")
            
            elapsed = time.time() - start_time
            remaining_calls = total_api_calls - api_calls_made
            if api_calls_made > 0:
                avg_time_per_call = elapsed / api_calls_made
                eta_minutes = (remaining_calls * avg_time_per_call) / 60
                print(f"  Progress: {api_calls_made}/{total_api_calls} calls, ETA: {eta_minutes:.0f} min")
        
        status = "success" if total_records > 0 else "no_data"
        self.log_pull(status, total_records)
        
        elapsed = time.time() - start_time
        
        print()
        print("=" * 70)
        print("BACKFILL COMPLETE")
        print("=" * 70)
        print(f"  Date range: {start_date.date()} to {end_date.date()}")
        print(f"  Granularity: hourly")
        print(f"  Tickers processed: {success_count}/{len(tickers)}")
        print(f"  Total records inserted: {total_records:,}")
        print(f"  API calls: {api_calls_made}")
        print(f"  Time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
        if elapsed > 0 and total_records > 0:
            print(f"  Rate: {total_records / elapsed:.0f} records/sec")
        print(f"  (Duplicates skipped via ON CONFLICT DO NOTHING)")
        print("=" * 70)
        
        return total_records


def main():
    parser = argparse.ArgumentParser(
        description='Backfill Alpha Vantage historical hourly equity data',
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
    
    end_date = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59, microsecond=0)
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc
            )
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
