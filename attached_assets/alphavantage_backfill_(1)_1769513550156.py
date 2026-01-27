"""
Alpha Vantage Historical Backfill Module

Backfills daily OHLCV data for equities (crypto-related stocks like MSTR, COIN, etc.)
from Alpha Vantage API. Designed to be imported and used programmatically.

Usage:
    from sources.alphavantage_backfill import AlphaVantageBackfillSource
    
    source = AlphaVantageBackfillSource()
    records = source.backfill(
        start_date=datetime(2022, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2025, 1, 27, tzinfo=timezone.utc),
        entities=['MSTR', 'COIN', 'MARA']
    )

Note: 
    - Free tier: 25 API calls/day (very limited)
    - Each ticker = 1 API call (returns full history)
    - 20+ years of historical data available
"""

import os
import csv
import time
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict
from sources.base import BaseSource
from db.setup import get_connection


# ============================================================================
# CONFIGURATION
# ============================================================================

# Rate limiting (conservative for premium tier)
REQUEST_DELAY = 2.5  # seconds between requests

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 60  # seconds to wait on rate limit

# Metrics to extract
METRICS = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'ADJUSTED_CLOSE', 'VOLUME', 'DOLLAR_VOLUME']


# ============================================================================
# ALPHA VANTAGE BACKFILL SOURCE CLASS
# ============================================================================

class AlphaVantageBackfillSource(BaseSource):
    """
    Alpha Vantage historical backfill source.
    
    Inherits from BaseSource to use common logging functionality.
    Pulls daily OHLCV data for equities using TIME_SERIES_DAILY_ADJUSTED.
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
    
    def pull(self) -> int:
        """
        Required by BaseSource. For backfill, use backfill() method instead.
        This method performs a default 30-day backfill.
        """
        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=30)
        return self.backfill(start_date, end_date)
    
    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self._last_request_time = time.time()
    
    def load_tickers(self, filter_entities: List[str] = None) -> List[Dict]:
        """Load tickers from CSV config."""
        if not os.path.exists(self.config_path):
            print(f"[AlphaVantage Backfill] Config file not found: {self.config_path}")
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
        
        print(f"[AlphaVantage Backfill] Loaded {len(tickers)} tickers from config")
        return tickers
    
    def fetch_full_history(self, symbol: str) -> Optional[Dict]:
        """
        Fetch full historical daily data for a symbol.
        
        Uses TIME_SERIES_DAILY_ADJUSTED with outputsize=full.
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
                    print(f"  {symbol}: API Error - {data['Error Message']}")
                    return None
                
                if "Note" in data:
                    print(f"  {symbol}: Rate limit hit, waiting {RETRY_DELAY}s...")
                    time.sleep(RETRY_DELAY)
                    continue
                
                if "Information" in data:
                    print(f"  {symbol}: API Info - {data['Information']}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                        continue
                    return None
                
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
        """Parse time series data into records for database insertion."""
        records = []
        
        for date_str, values in time_series.items():
            try:
                date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                
                if date < start_date or date > end_date:
                    continue
                
                open_price = float(values["1. open"])
                high_price = float(values["2. high"])
                low_price = float(values["3. low"])
                close_price = float(values["4. close"])
                adjusted_close = float(values.get("5. adjusted close", close_price))
                volume = int(values.get("6. volume", values.get("5. volume", 0)))
                dollar_volume = close_price * volume
                
                pulled_at = date.replace(hour=0, minute=0, second=0, microsecond=0)
                
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
                
            except (KeyError, ValueError):
                continue
        
        return records
    
    def insert_historical_metrics(self, records: List[Dict]) -> int:
        """Insert historical records with upsert logic."""
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
                None,  # exchange
                r.get('granularity', 'daily')
            )
            for r in records
        ]
        
        query = """
            INSERT INTO metrics (pulled_at, source, asset, metric_name, value, exchange, granularity)
            VALUES %s
            ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) 
            DO UPDATE SET 
                value = EXCLUDED.value,
                granularity = COALESCE(EXCLUDED.granularity, metrics.granularity)
        """
        
        try:
            execute_values(cur, query, rows, page_size=1000)
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
    
    def log_backfill(self, status: str, records_count: int) -> int:
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
                 entities: List[str] = None, dry_run: bool = False,
                 verbose: bool = True) -> int:
        """
        Main backfill orchestration method.
        
        Args:
            start_date: Start of backfill range (UTC)
            end_date: End of backfill range (UTC)
            entities: Optional list of symbols to backfill (uppercase)
            dry_run: If True, don't insert data
            verbose: If True, print progress messages
        
        Returns:
            Total number of records inserted
        """
        if verbose:
            print("=" * 70)
            print("ALPHA VANTAGE HISTORICAL BACKFILL")
            print("=" * 70)
            print(f"Date range: {start_date.date()} to {end_date.date()}")
        
        # Load tickers
        tickers = self.load_tickers(filter_entities=entities)
        if not tickers:
            if not dry_run:
                self.log_backfill("no_data", 0)
            return 0
        
        if verbose:
            print(f"Tickers: {len(tickers)}")
            print("=" * 70)
        
        if dry_run:
            return 0
        
        # Process each ticker
        total_records = 0
        success_count = 0
        all_records = []
        
        for i, ticker_info in enumerate(tickers, 1):
            symbol = ticker_info['symbol']
            if verbose:
                print(f"[{i:2}/{len(tickers)}] {symbol}...", end=" ")
            
            time_series = self.fetch_full_history(symbol)
            
            if time_series:
                records = self.parse_historical_data(symbol, time_series, start_date, end_date)
                
                if records:
                    all_records.extend(records)
                    success_count += 1
                    
                    if verbose:
                        dates = [r['pulled_at'] for r in records if r['metric_name'] == 'CLOSE']
                        if dates:
                            print(f"found {len(dates)} days")
            
            # Batch insert periodically
            if len(all_records) >= 10000:
                inserted = self.insert_historical_metrics(all_records)
                total_records += inserted
                all_records = []
        
        # Final insert
        if all_records:
            inserted = self.insert_historical_metrics(all_records)
            total_records += inserted
        
        # Log
        status = "success" if total_records > 0 else "no_data"
        self.log_backfill(status, total_records)
        
        if verbose:
            print("=" * 70)
            print(f"COMPLETE: {total_records} records inserted")
            print("=" * 70)
        
        return total_records
