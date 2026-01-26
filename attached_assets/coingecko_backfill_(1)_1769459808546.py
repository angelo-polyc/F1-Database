"""
CoinGecko Historical Data Backfill Source Module

Integrates with the existing source infrastructure to backfill historical
price, market cap, and volume data from CoinGecko API.

Usage:
    from sources.coingecko_backfill import CoinGeckoBackfillSource
    
    source = CoinGeckoBackfillSource()
    source.backfill(
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 6, 30, tzinfo=timezone.utc)
    )
"""

import os
import csv
import time
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Tuple, Dict
from sources.base import BaseSource
from db.setup import get_connection

# =============================================================================
# CONFIGURATION
# =============================================================================

COINGECKO_API_KEY = os.environ.get('COINGECKO_API_KEY', '')
USE_PRO_API = bool(COINGECKO_API_KEY)

DEMO_BASE_URL = "https://api.coingecko.com/api/v3"
PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"
BASE_URL = PRO_BASE_URL if USE_PRO_API else DEMO_BASE_URL

# Rate limits
RATE_LIMIT_CALLS_PER_MIN = 500 if USE_PRO_API else 30
REQUEST_DELAY = 60 / RATE_LIMIT_CALLS_PER_MIN + 0.05

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5

# Max days per API request
MAX_DAYS_PER_CHUNK = 365

# Metric mapping
METRIC_MAP = {
    'prices': 'PRICE',
    'market_caps': 'MARKET_CAP',
    'total_volumes': 'VOLUME_24H',
}


class CoinGeckoBackfillSource(BaseSource):
    """
    CoinGecko historical data backfill source.
    
    Fetches historical price, market cap, and volume data using the
    /coins/{id}/market_chart/range endpoint.
    """
    
    @property
    def source_name(self) -> str:
        return "coingecko"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers['Accept'] = 'application/json'
        if USE_PRO_API:
            self.session.headers['x-cg-pro-api-key'] = COINGECKO_API_KEY
        self.last_request_time = 0
        self.entity_cache = {}
        self.api_calls = 0
    
    def _rate_limit(self):
        """Enforce rate limiting."""
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: dict = None, retries: int = 0):
        """Make API request with retry logic."""
        self._rate_limit()
        url = f"{BASE_URL}/{endpoint}"
        
        if not USE_PRO_API and COINGECKO_API_KEY:
            params = params or {}
            params['x_cg_demo_api_key'] = COINGECKO_API_KEY
        
        try:
            response = self.session.get(url, params=params, timeout=60)
            self.api_calls += 1
            
            if response.status_code == 429:
                wait_time = 60 if retries == 0 else 120
                print(f"[CoinGecko Backfill] Rate limited - waiting {wait_time}s")
                time.sleep(wait_time)
                if retries < MAX_RETRIES:
                    return self._make_request(endpoint, params, retries + 1)
                return None
            
            if response.status_code == 404:
                return None
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.Timeout:
            if retries < MAX_RETRIES:
                time.sleep(RETRY_DELAY * (retries + 1))
                return self._make_request(endpoint, params, retries + 1)
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"[CoinGecko Backfill] API error: {e}")
            if retries < MAX_RETRIES:
                time.sleep(RETRY_DELAY * (retries + 1))
                return self._make_request(endpoint, params, retries + 1)
            return None
    
    def _load_entity_cache(self):
        """Load entity ID mappings from database."""
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT source_id, entity_id
                FROM entity_source_ids
                WHERE source = 'coingecko'
            """)
            self.entity_cache = {row[0]: row[1] for row in cur.fetchall()}
            cur.close()
            conn.close()
            print(f"[CoinGecko Backfill] Loaded {len(self.entity_cache)} entity mappings")
        except Exception as e:
            print(f"[CoinGecko Backfill] Could not load entity cache: {e}")
            self.entity_cache = {}
    
    def load_config(self, config_path: str = None) -> Dict[str, str]:
        """Load coin configuration from CSV."""
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'coingecko_config.csv'
            )
        
        if not os.path.exists(config_path):
            print(f"[CoinGecko Backfill] Config not found: {config_path}")
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
            print(f"[CoinGecko Backfill] Loaded {len(mappings)} coins from config")
            return mappings
        except Exception as e:
            print(f"[CoinGecko Backfill] Config load error: {e}")
            return {}
    
    def fetch_market_chart_range(
        self,
        coin_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[dict]:
        """
        Fetch historical chart data for a coin.
        
        Uses /coins/{id}/market_chart/range with ISO date strings.
        """
        params = {
            'vs_currency': 'usd',
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'precision': 'full'
        }
        
        endpoint = f"coins/{coin_id}/market_chart/range"
        return self._make_request(endpoint, params)
    
    def parse_timeseries(self, data: dict, asset: str) -> List[dict]:
        """
        Parse market chart response into metric records.
        
        Converts [[timestamp_ms, value], ...] arrays to database records.
        """
        records = []
        
        for api_field, metric_name in METRIC_MAP.items():
            timeseries = data.get(api_field, [])
            
            for point in timeseries:
                if len(point) >= 2 and point[1] is not None:
                    try:
                        timestamp_ms = point[0]
                        value = float(point[1])
                        
                        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
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
    
    def insert_historical_metrics(self, records: List[dict]) -> int:
        """Insert historical records with upsert."""
        if not records:
            return 0
        
        conn = get_connection()
        cur = conn.cursor()
        
        rows = [
            (r["pulled_at"], self.source_name, r["asset"], r["metric_name"], r["value"], None)
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
    
    def chunk_date_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Tuple[datetime, datetime]]:
        """Split date range into chunks."""
        chunks = []
        current = start_date
        
        while current < end_date:
            chunk_end = min(current + timedelta(days=MAX_DAYS_PER_CHUNK), end_date)
            chunks.append((current, chunk_end))
            current = chunk_end
        
        return chunks
    
    def backfill_coin(
        self,
        coin_id: str,
        asset: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[dict]:
        """Backfill data for a single coin."""
        all_records = []
        chunks = self.chunk_date_range(start_date, end_date)
        
        for chunk_start, chunk_end in chunks:
            data = self.fetch_market_chart_range(coin_id, chunk_start, chunk_end)
            
            if data:
                records = self.parse_timeseries(data, asset)
                all_records.extend(records)
        
        return all_records
    
    def backfill(
        self,
        start_date: datetime,
        end_date: datetime = None,
        coins: List[str] = None,
        config_path: str = None
    ) -> int:
        """
        Run historical backfill.
        
        Args:
            start_date: Start of backfill period
            end_date: End of backfill period (default: now)
            coins: List of CoinGecko IDs to backfill (overrides config)
            config_path: Path to config CSV
            
        Returns:
            Total records inserted
        """
        if end_date is None:
            end_date = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        
        print("\n" + "=" * 70)
        print("COINGECKO HISTORICAL BACKFILL")
        print("=" * 70)
        print(f"Period: {start_date.date()} to {end_date.date()}")
        print(f"API: {'Pro' if USE_PRO_API else 'Demo'} ({RATE_LIMIT_CALLS_PER_MIN}/min)")
        print()
        
        self._load_entity_cache()
        
        # Build coin list
        if coins:
            coin_list = [(cid, self.entity_cache.get(cid, cid)) for cid in coins]
            print(f"[CoinGecko Backfill] {len(coin_list)} coins from argument")
        else:
            symbol_to_id = self.load_config(config_path)
            if not symbol_to_id:
                print("[CoinGecko Backfill] No coins configured")
                return 0
            coin_list = [
                (cg_id, self.entity_cache.get(cg_id, cg_id))
                for cg_id in set(symbol_to_id.values())
            ]
        
        total_records = 0
        successful = 0
        failed = []
        
        print(f"\n[CoinGecko Backfill] Processing {len(coin_list)} coins...")
        print("-" * 70)
        
        for i, (cg_id, asset) in enumerate(coin_list, 1):
            print(f"[{i}/{len(coin_list)}] {cg_id} -> {asset}", end=" ")
            
            try:
                records = self.backfill_coin(cg_id, asset, start_date, end_date)
                
                if records:
                    inserted = self.insert_historical_metrics(records)
                    total_records += inserted
                    successful += 1
                    unique_dates = len(set(r['pulled_at'] for r in records))
                    print(f"-> {len(records)} records ({unique_dates} days)")
                else:
                    print("-> No data")
                    failed.append(cg_id)
                    
            except Exception as e:
                print(f"-> Error: {e}")
                failed.append(cg_id)
        
        # Log to pulls table
        status = "success" if total_records > 0 else "no_data"
        self.log_pull(status, total_records)
        
        print("\n" + "=" * 70)
        print("BACKFILL COMPLETE")
        print(f"  Coins: {successful}/{len(coin_list)}")
        print(f"  Records: {total_records}")
        print(f"  API calls: {self.api_calls}")
        if failed:
            print(f"  Failed: {failed[:10]}{'...' if len(failed) > 10 else ''}")
        print("=" * 70)
        
        return total_records
    
    def pull(self) -> int:
        """
        Default pull method - backfills last 30 days.
        
        For regular live pulls, use the CoinGeckoSource class instead.
        """
        end_date = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        start_date = end_date - timedelta(days=30)
        
        return self.backfill(start_date, end_date)
    
    def log_pull(self, status: str, records_count: int) -> int:
        """Log backfill operation with distinct source name."""
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pulls (source_name, pulled_at, status, records_count)
            VALUES (%s, %s, %s, %s)
            RETURNING pull_id
            """,
            (f"{self.source_name}_backfill", datetime.utcnow(), status, records_count)
        )
        pull_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return pull_id


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def backfill_coingecko(
    start_date: datetime,
    end_date: datetime = None,
    coins: List[str] = None
) -> int:
    """
    Convenience function for backfilling CoinGecko data.
    
    Example:
        from sources.coingecko_backfill import backfill_coingecko
        from datetime import datetime, timezone
        
        backfill_coingecko(
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 6, 30, tzinfo=timezone.utc),
            coins=['bitcoin', 'ethereum', 'solana']
        )
    """
    source = CoinGeckoBackfillSource()
    return source.backfill(start_date, end_date, coins)
