"""
Velo.xyz Historical Backfill Module

Backfills hourly derivatives data (funding rates, OI, liquidations, OHLCV)
from Velo API. Designed to be imported and used programmatically.

Usage:
    from sources.velo_backfill import VeloBackfillSource
    
    source = VeloBackfillSource()
    records = source.backfill(
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 6, 30, tzinfo=timezone.utc),
        entities=['BTC', 'ETH'],
        exchanges=['binance-futures', 'bybit']
    )

Note: Velo monthly plan provides 3-month history. For 3-year backfill, 
      yearly subscription with full history access is required.
"""

import os
import csv
import time
import requests
from datetime import datetime, timezone, timedelta
from requests.auth import HTTPBasicAuth
from typing import Optional, List, Dict, Tuple
from sources.base import BaseSource
from db.setup import get_connection


# ============================================================================
# CONFIGURATION
# ============================================================================

# Rate limit: 120 requests per 30 seconds = 4/sec max
# Use 2/sec to stay safely under limit
REQUEST_DELAY = 0.5

# Batch size for coins per request (to stay under 22,500 cell limit)
BATCH_SIZE = 5

# Date chunking: process 7 days at a time for hourly data
DATE_CHUNK_DAYS = 7

# Columns to fetch for backfill
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
MAX_LOOKBACK_DAYS = 365 * 3


# ============================================================================
# VELO BACKFILL SOURCE CLASS
# ============================================================================

class VeloBackfillSource(BaseSource):
    """
    Velo.xyz historical backfill source.
    
    Inherits from BaseSource to use common logging functionality.
    Pulls hourly futures data including funding rates, open interest,
    liquidations, and OHLCV for configured coin-exchange pairs.
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
    
    def pull(self) -> int:
        """
        Required by BaseSource. For backfill, use backfill() method instead.
        This method performs a default 7-day backfill.
        """
        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=7)
        return self.backfill(start_date, end_date)
    
    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self._last_request_time = time.time()
    
    def load_config(self, filter_entities: List[str] = None, 
                    filter_exchanges: List[str] = None) -> List[Dict]:
        """Load coin-exchange pairs from CSV config."""
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
                    
                    if filter_entities and coin not in filter_entities:
                        continue
                    if filter_exchanges and exchange not in filter_exchanges:
                        continue
                    
                    config.append({'coin': coin, 'exchange': exchange})
        
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
        """Fetch historical data for a batch of coins from a single exchange."""
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
            execute_values(cur, query, rows, page_size=1000)
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
            ("velo_backfill", datetime.utcnow(), status, records_count)
        )
        pull_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return pull_id
    
    def generate_date_chunks(self, start_date: datetime, end_date: datetime, 
                              chunk_days: int = DATE_CHUNK_DAYS) -> List[Tuple[datetime, datetime]]:
        """Generate date chunks for processing."""
        chunks = []
        current = start_date
        while current < end_date:
            chunk_end = min(current + timedelta(days=chunk_days), end_date)
            chunks.append((current, chunk_end))
            current = chunk_end
        return chunks
    
    def backfill(self, start_date: datetime, end_date: datetime,
                 entities: List[str] = None, exchanges: List[str] = None,
                 dry_run: bool = False, verbose: bool = True) -> int:
        """
        Main backfill orchestration method.
        
        Args:
            start_date: Start of backfill range (UTC)
            end_date: End of backfill range (UTC)
            entities: Optional list of coins to backfill (uppercase)
            exchanges: Optional list of exchanges to backfill (lowercase)
            dry_run: If True, don't insert data
            verbose: If True, print progress messages
        
        Returns:
            Total number of records inserted
        """
        if verbose:
            print("=" * 70)
            print("VELO HISTORICAL BACKFILL")
            print("=" * 70)
            print(f"Date range: {start_date.date()} to {end_date.date()}")
        
        # Validate date range
        days_diff = (end_date - start_date).days
        if days_diff > MAX_LOOKBACK_DAYS:
            print(f"[Velo Backfill] Warning: {days_diff} days exceeds {MAX_LOOKBACK_DAYS} day max")
        
        # Load configuration
        config = self.load_config(filter_entities=entities, filter_exchanges=exchanges)
        if not config:
            if not dry_run:
                self.log_backfill("no_data", 0)
            return 0
        
        if not dry_run:
            self.load_entity_cache()
        
        # Group by exchange
        exchange_coins = {}
        for item in config:
            ex = item['exchange']
            if ex not in exchange_coins:
                exchange_coins[ex] = []
            if item['coin'] not in exchange_coins[ex]:
                exchange_coins[ex].append(item['coin'])
        
        # Generate date chunks
        date_chunks = self.generate_date_chunks(start_date, end_date)
        
        if verbose:
            print(f"Exchanges: {list(exchange_coins.keys())}")
            print(f"Date chunks: {len(date_chunks)}")
            print("=" * 70)
        
        if dry_run:
            return 0
        
        # Process data
        total_records = 0
        all_records = []
        
        for chunk_idx, (chunk_start, chunk_end) in enumerate(date_chunks):
            begin_ts = int(chunk_start.timestamp() * 1000)
            end_ts = int(chunk_end.timestamp() * 1000)
            
            if verbose:
                print(f"[Chunk {chunk_idx+1}/{len(date_chunks)}] {chunk_start.date()} to {chunk_end.date()}")
            
            for exchange, coins in exchange_coins.items():
                for batch_start in range(0, len(coins), BATCH_SIZE):
                    batch = coins[batch_start:batch_start + BATCH_SIZE]
                    records = self.fetch_historical_data(exchange, batch, begin_ts, end_ts)
                    all_records.extend(records)
            
            # Batch insert to avoid memory issues
            if len(all_records) >= 50000:
                seen = set()
                unique_records = []
                for r in all_records:
                    key = (r['pulled_at'], r['asset'], r['metric_name'], r.get('exchange', ''))
                    if key not in seen:
                        seen.add(key)
                        unique_records.append(r)
                
                inserted = self.insert_historical_metrics(unique_records)
                total_records += inserted
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
        
        # Log
        status = "success" if total_records > 0 else "no_data"
        self.log_backfill(status, total_records)
        
        if verbose:
            print("=" * 70)
            print(f"COMPLETE: {total_records} records inserted")
            print("=" * 70)
        
        return total_records
