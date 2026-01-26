"""
DefiLlama Historical Backfill Script

Backfills historical data from DefiLlama API for protocols, chains, stablecoins, and bridges.
Limited to 3 years maximum lookback.

Usage:
    python defillama_backfill.py --start-date 2024-01-01 --end-date 2024-06-30
    python defillama_backfill.py --start-date 2024-01-01  # defaults end to yesterday
    python defillama_backfill.py --days 90  # last 90 days
    python defillama_backfill.py --start-date 2024-01-01 --dry-run  # preview without inserting

Environment:
    DEFILLAMA_API_KEY - Your DefiLlama Pro API key (required for full historical data)
"""

import os
import sys
import csv
import time
import argparse
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Import from existing structure
try:
    from sources.base import BaseSource
    from db.setup import get_connection
    HAS_DB = True
except ImportError:
    HAS_DB = False
    print("Warning: Could not import db modules. Running in standalone mode.")

# ============================================================================
# CONFIGURATION
# ============================================================================

MAX_LOOKBACK_YEARS = 3  # Maximum 3 years historical data
MAX_WORKERS = 8
RATE_LIMIT_PER_SEC = 12  # Stay under 1000/min for Pro API
REQUEST_DELAY = 0.1
RETRY_DELAY = 2
MAX_RETRIES = 3

_thread_local = threading.local()

# Metric mapping for historical data
HISTORICAL_METRIC_MAP = {
    # Protocol TVL
    'protocol_tvl': 'TVL',
    
    # Chain TVL
    'chain_tvl': 'CHAIN_TVL',
    
    # Fees & Revenue (from /summary/fees endpoint)
    'fees_daily': 'FEES_DAILY',
    'revenue_daily': 'REVENUE_DAILY',
    'holders_revenue_daily': 'HOLDERS_REVENUE_DAILY',
    
    # DEX Volume (from /summary/dexs endpoint)
    'dex_volume_daily': 'DEX_VOLUME_DAILY',
    
    # Derivatives (from /summary/derivatives endpoint)
    'derivatives_volume_daily': 'DERIVATIVES_VOLUME_DAILY',
    
    # Options (from /summary/options endpoint)
    'options_premium_daily': 'OPTIONS_PREMIUM_DAILY',
    'options_notional_daily': 'OPTIONS_NOTIONAL_DAILY',
    
    # Stablecoin supply
    'stablecoin_supply': 'STABLECOIN_SUPPLY',
    
    # Bridge volume
    'bridge_volume_daily': 'BRIDGE_VOLUME_DAILY',
}


# ============================================================================
# BACKFILL CLASS
# ============================================================================

class DefiLlamaBackfill:
    """Handles historical data backfill from DefiLlama API."""
    
    def __init__(self, api_key: str = None, config_path: str = "defillama_config.csv"):
        self.api_key = api_key or os.environ.get("DEFILLAMA_API_KEY")
        self.config_path = config_path
        self.source_name = "defillama"
        
        # Set base URL based on API key availability
        if self.api_key:
            self.base_url = "https://pro-api.llama.fi"
        else:
            self.base_url = "https://api.llama.fi"
            print("Warning: No API key - using free tier with limited rate limits")
        
        # Session with connection pooling
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        
        # Stats
        self.stats = {
            "api_calls": 0,
            "records_inserted": 0,
            "errors": 0,
            "protocols_processed": 0,
            "chains_processed": 0,
        }
    
    def _build_url(self, endpoint: str) -> str:
        """Build URL with API key if available."""
        if self.api_key:
            return f"{self.base_url}/{self.api_key}{endpoint}"
        return f"{self.base_url}{endpoint}"
    
    def _get_thread_session(self):
        """Get thread-local session for parallel requests."""
        if not hasattr(_thread_local, 'session'):
            _thread_local.session = requests.Session()
            adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5)
            _thread_local.session.mount('https://', adapter)
            _thread_local.session.mount('http://', adapter)
        return _thread_local.session
    
    def fetch_json(self, url: str, use_thread_session: bool = False) -> Optional[dict]:
        """Fetch JSON from URL with retries."""
        session = self._get_thread_session() if use_thread_session else self.session
        
        for attempt in range(MAX_RETRIES):
            try:
                resp = session.get(url, timeout=60)
                self.stats["api_calls"] += 1
                
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 429:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    print(f"    Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                elif resp.status_code == 404:
                    return None
                else:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                    continue
                    
            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * 2)
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        
        self.stats["errors"] += 1
        return None
    
    def load_config(self) -> List[Dict[str, Any]]:
        """Load entities from config CSV."""
        entities = []
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('pull', '').strip() in ['1', '1.0', 'true', 'True']:
                        entities.append({
                            'gecko_id': row.get('gecko_id', '').strip().lower(),
                            'slug': row.get('slug', '').strip().lower(),
                            'name': row.get('name', '').strip(),
                            'entity_type': row.get('entity_type', 'protocol').strip().lower(),
                        })
        except FileNotFoundError:
            print(f"Config file not found: {self.config_path}")
        return entities
    
    def _filter_by_date_range(self, data_points: List, 
                              start_ts: int, end_ts: int) -> List:
        """Filter data points to date range."""
        filtered = []
        for point in data_points:
            if isinstance(point, dict):
                ts = point.get('date') or point.get('timestamp')
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                ts = point[0]
            else:
                continue
            
            if ts and start_ts <= ts <= end_ts:
                filtered.append(point)
        
        return filtered
    
    def fetch_protocol_tvl_history(self, slug: str) -> List[Dict]:
        """Fetch historical TVL for a protocol."""
        url = self._build_url(f"/api/protocol/{slug}")
        data = self.fetch_json(url)
        
        if not data:
            return []
        
        # TVL is in 'tvl' array with {date, totalLiquidityUSD}
        tvl_data = data.get('tvl', [])
        return tvl_data
    
    def fetch_chain_tvl_history(self, chain: str) -> List[Dict]:
        """Fetch historical TVL for a chain."""
        url = self._build_url(f"/api/v2/historicalChainTvl/{chain}")
        data = self.fetch_json(url)
        
        if not data or not isinstance(data, list):
            return []
        
        return data
    
    def fetch_fees_history(self, slug: str) -> Optional[Dict]:
        """Fetch historical fees/revenue for a protocol."""
        url = self._build_url(f"/api/summary/fees/{slug}?dataType=dailyFees")
        data = self.fetch_json(url)
        
        if not data:
            return None
        
        return {
            'fees': data.get('totalDataChart', []),
            'fees_breakdown': data.get('totalDataChartBreakdown', []),
        }
    
    def fetch_revenue_history(self, slug: str) -> Optional[List]:
        """Fetch historical revenue for a protocol."""
        url = self._build_url(f"/api/summary/fees/{slug}?dataType=dailyRevenue")
        data = self.fetch_json(url)
        
        if not data:
            return None
        
        return data.get('totalDataChart', [])
    
    def fetch_dex_volume_history(self, slug: str) -> Optional[List]:
        """Fetch historical DEX volume for a protocol."""
        url = self._build_url(f"/api/summary/dexs/{slug}")
        data = self.fetch_json(url)
        
        if not data:
            return None
        
        return data.get('totalDataChart', [])
    
    def fetch_derivatives_history(self, slug: str) -> Optional[List]:
        """Fetch historical derivatives volume."""
        url = self._build_url(f"/api/summary/derivatives/{slug}")
        data = self.fetch_json(url)
        
        if not data:
            return None
        
        return data.get('totalDataChart', [])
    
    def fetch_stablecoin_history(self, stablecoin_id: int) -> Optional[Dict]:
        """Fetch historical stablecoin supply data."""
        url = self._build_url(f"/stablecoins/stablecoin/{stablecoin_id}")
        data = self.fetch_json(url)
        
        if not data:
            return None
        
        # Extract chain-level circulating data over time
        chain_circulating = data.get('chainCirculating', {})
        return chain_circulating
    
    def fetch_bridge_history(self, bridge_id: int) -> Optional[List]:
        """Fetch historical bridge volume."""
        url = self._build_url(f"/bridges/bridgevolume/all")
        data = self.fetch_json(url)
        
        if not data or not isinstance(data, list):
            return None
        
        return data
    
    def parse_timeseries(self, data: List, metric_name: str, 
                         asset_id: str, start_ts: int, end_ts: int) -> List[Dict]:
        """Parse time series data into records."""
        records = []
        
        for point in data:
            ts = None
            value = None
            
            # Handle different formats
            if isinstance(point, dict):
                ts = point.get('date') or point.get('timestamp')
                value = (point.get('totalLiquidityUSD') or 
                        point.get('tvl') or 
                        point.get('value') or
                        point.get('totalCirculating', {}).get('peggedUSD'))
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                ts, value = point[0], point[1]
            
            if ts is None or value is None:
                continue
            
            # Filter by date range
            if not (start_ts <= ts <= end_ts):
                continue
            
            try:
                numeric_value = float(value)
                dt = datetime.utcfromtimestamp(ts)
                records.append({
                    "date": dt,
                    "asset": asset_id,
                    "metric_name": metric_name,
                    "value": numeric_value,
                })
            except (ValueError, TypeError, OSError):
                continue
        
        return records
    
    def insert_historical_metrics(self, records: List[Dict]) -> int:
        """Insert historical records into database."""
        if not records or not HAS_DB:
            return len(records) if records else 0
        
        conn = get_connection()
        cur = conn.cursor()
        
        rows = []
        for r in records:
            pulled_at = r["date"].replace(hour=0, minute=0, second=0, microsecond=0)
            rows.append((
                pulled_at,
                self.source_name,
                r["asset"],
                r["metric_name"],
                r["value"],
                None  # exchange
            ))
        
        cur.executemany("""
            INSERT INTO metrics (pulled_at, source, asset, metric_name, value, exchange)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) 
            DO UPDATE SET value = EXCLUDED.value
        """, rows)
        
        conn.commit()
        cur.close()
        conn.close()
        
        return len(rows)
    
    def log_pull(self, status: str, records_count: int, notes: str = None):
        """Log backfill to pulls table."""
        if not HAS_DB:
            return
        
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pulls (source_name, pulled_at, status, records_count, notes)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (f"{self.source_name}_backfill", datetime.utcnow(), status, records_count, notes)
        )
        conn.commit()
        cur.close()
        conn.close()
    
    def backfill_protocol(self, entity: Dict, start_ts: int, end_ts: int) -> List[Dict]:
        """Backfill all historical data for a single protocol."""
        records = []
        slug = entity.get('slug') or entity.get('gecko_id')
        gecko_id = entity.get('gecko_id')
        
        if not slug:
            return records
        
        # 1. Protocol TVL
        tvl_data = self.fetch_protocol_tvl_history(slug)
        if tvl_data:
            tvl_records = self.parse_timeseries(tvl_data, 'TVL', gecko_id, start_ts, end_ts)
            records.extend(tvl_records)
        time.sleep(REQUEST_DELAY)
        
        # 2. Fees
        fees_data = self.fetch_fees_history(slug)
        if fees_data and fees_data.get('fees'):
            fees_records = self.parse_timeseries(fees_data['fees'], 'FEES_DAILY', gecko_id, start_ts, end_ts)
            records.extend(fees_records)
        time.sleep(REQUEST_DELAY)
        
        # 3. Revenue
        revenue_data = self.fetch_revenue_history(slug)
        if revenue_data:
            rev_records = self.parse_timeseries(revenue_data, 'REVENUE_DAILY', gecko_id, start_ts, end_ts)
            records.extend(rev_records)
        time.sleep(REQUEST_DELAY)
        
        # 4. DEX Volume (if applicable)
        dex_data = self.fetch_dex_volume_history(slug)
        if dex_data:
            dex_records = self.parse_timeseries(dex_data, 'DEX_VOLUME_DAILY', gecko_id, start_ts, end_ts)
            records.extend(dex_records)
        time.sleep(REQUEST_DELAY)
        
        # 5. Derivatives Volume (if applicable)
        deriv_data = self.fetch_derivatives_history(slug)
        if deriv_data:
            deriv_records = self.parse_timeseries(deriv_data, 'DERIVATIVES_VOLUME_DAILY', gecko_id, start_ts, end_ts)
            records.extend(deriv_records)
        time.sleep(REQUEST_DELAY)
        
        return records
    
    def backfill_chain(self, entity: Dict, start_ts: int, end_ts: int) -> List[Dict]:
        """Backfill historical data for a chain."""
        records = []
        chain_name = entity.get('name') or entity.get('gecko_id')
        gecko_id = entity.get('gecko_id')
        
        if not chain_name:
            return records
        
        # Chain TVL
        tvl_data = self.fetch_chain_tvl_history(chain_name)
        if tvl_data:
            tvl_records = self.parse_timeseries(tvl_data, 'CHAIN_TVL', gecko_id, start_ts, end_ts)
            records.extend(tvl_records)
        
        return records
    
    def run_backfill(self, start_date: datetime, end_date: datetime,
                     dry_run: bool = False,
                     entity_filter: List[str] = None,
                     entity_types: List[str] = None) -> Dict:
        """
        Run the backfill process.
        
        Args:
            start_date: Start date for backfill
            end_date: End date for backfill  
            dry_run: If True, don't insert data
            entity_filter: Optional list of specific gecko_ids to backfill
            entity_types: Optional list of entity types ('protocol', 'chain', 'stablecoin')
        """
        # Enforce 3-year maximum lookback
        max_lookback = datetime.now() - timedelta(days=MAX_LOOKBACK_YEARS * 365)
        if start_date < max_lookback:
            print(f"Warning: Adjusting start date from {start_date.strftime('%Y-%m-%d')} "
                  f"to {max_lookback.strftime('%Y-%m-%d')} (3-year limit)")
            start_date = max_lookback
        
        print("=" * 70)
        print("DEFILLAMA HISTORICAL BACKFILL")
        print("=" * 70)
        print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"API Key: {'Present' if self.api_key else 'Not set (free tier)'}")
        print(f"Dry run: {dry_run}")
        print()
        
        # Convert to timestamps
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        
        # Load config
        entities = self.load_config()
        if not entities:
            print("No entities found in config")
            return self.stats
        
        # Apply filters
        if entity_filter:
            entity_filter_lower = [e.lower() for e in entity_filter]
            entities = [e for e in entities 
                       if e['gecko_id'] in entity_filter_lower or e['slug'] in entity_filter_lower]
        
        if entity_types:
            entity_types_lower = [t.lower() for t in entity_types]
            entities = [e for e in entities if e.get('entity_type', 'protocol') in entity_types_lower]
        
        # Separate by type
        protocols = [e for e in entities if e.get('entity_type', 'protocol') == 'protocol']
        chains = [e for e in entities if e.get('entity_type') == 'chain']
        stablecoins = [e for e in entities if e.get('entity_type') == 'stablecoin']
        
        print(f"Entities to backfill:")
        print(f"  Protocols: {len(protocols)}")
        print(f"  Chains: {len(chains)}")
        print(f"  Stablecoins: {len(stablecoins)}")
        print("=" * 70)
        
        all_records = []
        
        # Process protocols
        if protocols:
            print(f"\nBackfilling {len(protocols)} protocols...")
            for i, entity in enumerate(protocols, 1):
                gecko_id = entity.get('gecko_id')
                print(f"  [{i:3d}/{len(protocols)}] {gecko_id}...", end=" ", flush=True)
                
                if dry_run:
                    print("[DRY RUN]")
                    continue
                
                records = self.backfill_protocol(entity, start_ts, end_ts)
                
                if records:
                    all_records.extend(records)
                    print(f"{len(records)} records")
                    self.stats["protocols_processed"] += 1
                else:
                    print("no data")
        
        # Process chains
        if chains:
            print(f"\nBackfilling {len(chains)} chains...")
            for i, entity in enumerate(chains, 1):
                gecko_id = entity.get('gecko_id')
                print(f"  [{i:3d}/{len(chains)}] {gecko_id}...", end=" ", flush=True)
                
                if dry_run:
                    print("[DRY RUN]")
                    continue
                
                records = self.backfill_chain(entity, start_ts, end_ts)
                
                if records:
                    all_records.extend(records)
                    print(f"{len(records)} records")
                    self.stats["chains_processed"] += 1
                else:
                    print("no data")
                
                time.sleep(REQUEST_DELAY)
        
        # Insert all records
        total_inserted = 0
        if all_records and not dry_run:
            print(f"\nInserting {len(all_records)} records...")
            total_inserted = self.insert_historical_metrics(all_records)
            self.stats["records_inserted"] = total_inserted
        
        # Log the operation
        if not dry_run and HAS_DB:
            notes = f"Backfill {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            status = "success" if total_inserted > 0 else "no_data"
            self.log_pull(status, total_inserted, notes)
        
        # Summary
        print("\n" + "=" * 70)
        print("BACKFILL COMPLETE")
        print("=" * 70)
        print(f"  API calls:           {self.stats['api_calls']}")
        print(f"  Protocols processed: {self.stats['protocols_processed']}")
        print(f"  Chains processed:    {self.stats['chains_processed']}")
        print(f"  Records inserted:    {self.stats['records_inserted']}")
        print(f"  Errors:              {self.stats['errors']}")
        print("=" * 70)
        
        return self.stats


# ============================================================================
# CLI INTERFACE
# ============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Backfill historical data from DefiLlama API (max 3 years)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill specific date range
  python defillama_backfill.py --start-date 2024-01-01 --end-date 2024-06-30

  # Backfill last 90 days
  python defillama_backfill.py --days 90

  # Backfill with dry run
  python defillama_backfill.py --start-date 2024-01-01 --dry-run

  # Backfill specific entities
  python defillama_backfill.py --start-date 2024-01-01 --entities aave,uniswap,compound

  # Backfill only chains
  python defillama_backfill.py --start-date 2024-01-01 --types chain

Environment Variables:
  DEFILLAMA_API_KEY    Your DefiLlama Pro API key (recommended)

Note: Maximum lookback is 3 years from current date.
        """
    )
    
    parser.add_argument(
        "--start-date", "-s",
        type=str,
        help="Start date in YYYY-MM-DD format"
    )
    
    parser.add_argument(
        "--end-date", "-e",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format (default: yesterday)"
    )
    
    parser.add_argument(
        "--days", "-d",
        type=int,
        default=None,
        help="Number of days to backfill (alternative to start/end dates)"
    )
    
    parser.add_argument(
        "--config", "-c",
        type=str,
        default="defillama_config.csv",
        help="Path to config CSV file"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without inserting data"
    )
    
    parser.add_argument(
        "--entities",
        type=str,
        default=None,
        help="Comma-separated list of specific gecko_ids to backfill"
    )
    
    parser.add_argument(
        "--types",
        type=str,
        default=None,
        help="Comma-separated list of entity types (protocol, chain, stablecoin)"
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Determine date range
    if args.days:
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=args.days - 1)
    elif args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        else:
            end_date = datetime.now() - timedelta(days=1)
    else:
        print("ERROR: Must specify either --start-date or --days")
        sys.exit(1)
    
    # Validate dates
    if start_date > end_date:
        print("ERROR: Start date must be before end date")
        sys.exit(1)
    
    # Parse filters
    entity_filter = None
    if args.entities:
        entity_filter = [e.strip() for e in args.entities.split(",")]
    
    entity_types = None
    if args.types:
        entity_types = [t.strip() for t in args.types.split(",")]
    
    # Run backfill
    backfill = DefiLlamaBackfill(config_path=args.config)
    stats = backfill.run_backfill(
        start_date=start_date,
        end_date=end_date,
        dry_run=args.dry_run,
        entity_filter=entity_filter,
        entity_types=entity_types,
    )
    
    sys.exit(0 if stats["errors"] == 0 else 1)


if __name__ == "__main__":
    main()
