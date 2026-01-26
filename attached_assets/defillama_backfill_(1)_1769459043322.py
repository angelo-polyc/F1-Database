"""
DefiLlama Historical Backfill Source Module

Integrates with the existing sources/ structure for historical data backfills.
Place this file in your sources/ directory.

Usage:
    from sources.defillama_backfill import DefiLlamaBackfillSource
    
    source = DefiLlamaBackfillSource()
    source.backfill(start_date="2024-01-01", end_date="2024-06-30")
    
Note: Maximum lookback is 3 years from the start date.
"""

import os
import csv
import time
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from requests.adapters import HTTPAdapter
from sources.base import BaseSource
from db.setup import get_connection

# ============================================================================
# CONFIGURATION
# ============================================================================

MAX_LOOKBACK_YEARS = 3  # Maximum 3 years historical data
REQUEST_DELAY = 0.1
RETRY_DELAY = 2
MAX_RETRIES = 3


class DefiLlamaBackfillSource(BaseSource):
    """DefiLlama historical backfill source."""
    
    @property
    def source_name(self) -> str:
        return "defillama"
    
    def __init__(self):
        self.config_path = "defillama_config.csv"
        self.api_key = os.environ.get("DEFILLAMA_API_KEY")
        
        if self.api_key:
            self.base_url = "https://pro-api.llama.fi"
        else:
            self.base_url = "https://api.llama.fi"
            print("[DefiLlama] Warning: No API key - using free tier")
        
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        
        self._api_calls = 0
        self._errors = 0
    
    def _build_url(self, endpoint: str) -> str:
        """Build URL with API key if available."""
        if self.api_key:
            return f"{self.base_url}/{self.api_key}{endpoint}"
        return f"{self.base_url}{endpoint}"
    
    def fetch_json(self, url: str) -> Optional[dict]:
        """Fetch JSON with retries."""
        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(url, timeout=60)
                self._api_calls += 1
                
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 429:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                elif resp.status_code == 404:
                    return None
                else:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                    continue
            except Exception:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        
        self._errors += 1
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
            print(f"[DefiLlama] Config file not found: {self.config_path}")
        return entities
    
    def fetch_protocol_tvl_history(self, slug: str) -> List[Dict]:
        """Fetch historical TVL for a protocol."""
        url = self._build_url(f"/api/protocol/{slug}")
        data = self.fetch_json(url)
        return data.get('tvl', []) if data else []
    
    def fetch_chain_tvl_history(self, chain: str) -> List[Dict]:
        """Fetch historical TVL for a chain."""
        url = self._build_url(f"/api/v2/historicalChainTvl/{chain}")
        data = self.fetch_json(url)
        return data if isinstance(data, list) else []
    
    def fetch_fees_history(self, slug: str) -> Optional[List]:
        """Fetch historical fees for a protocol."""
        url = self._build_url(f"/api/summary/fees/{slug}?dataType=dailyFees")
        data = self.fetch_json(url)
        return data.get('totalDataChart', []) if data else None
    
    def fetch_revenue_history(self, slug: str) -> Optional[List]:
        """Fetch historical revenue for a protocol."""
        url = self._build_url(f"/api/summary/fees/{slug}?dataType=dailyRevenue")
        data = self.fetch_json(url)
        return data.get('totalDataChart', []) if data else None
    
    def fetch_dex_volume_history(self, slug: str) -> Optional[List]:
        """Fetch historical DEX volume for a protocol."""
        url = self._build_url(f"/api/summary/dexs/{slug}")
        data = self.fetch_json(url)
        return data.get('totalDataChart', []) if data else None
    
    def fetch_derivatives_history(self, slug: str) -> Optional[List]:
        """Fetch historical derivatives volume."""
        url = self._build_url(f"/api/summary/derivatives/{slug}")
        data = self.fetch_json(url)
        return data.get('totalDataChart', []) if data else None
    
    def parse_timeseries(self, data: List, metric_name: str,
                         asset_id: str, start_ts: int, end_ts: int) -> List[Dict]:
        """Parse time series data into records."""
        records = []
        
        for point in data:
            ts = None
            value = None
            
            if isinstance(point, dict):
                ts = point.get('date') or point.get('timestamp')
                value = (point.get('totalLiquidityUSD') or
                        point.get('tvl') or
                        point.get('value'))
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                ts, value = point[0], point[1]
            
            if ts is None or value is None:
                continue
            
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
        """Insert historical records."""
        if not records:
            return 0
        
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
                None
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
    
    def pull(self) -> int:
        """Standard pull - use backfill() for historical data."""
        print("Use backfill() method for historical data")
        return 0
    
    def backfill(self, start_date: str, end_date: str = None,
                 entities: List[str] = None, types: List[str] = None) -> int:
        """
        Backfill historical data from DefiLlama.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD), defaults to yesterday
            entities: Optional list of specific gecko_ids
            types: Optional list of entity types ('protocol', 'chain')
            
        Returns:
            int: Total records inserted
        """
        # Parse dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now() - timedelta(days=1)
        
        # Enforce 3-year limit
        max_lookback = datetime.now() - timedelta(days=MAX_LOOKBACK_YEARS * 365)
        if start_dt < max_lookback:
            print(f"[DefiLlama] Adjusting start date to {max_lookback.strftime('%Y-%m-%d')} (3-year limit)")
            start_dt = max_lookback
        
        print("\n" + "=" * 70)
        print("DEFILLAMA HISTORICAL BACKFILL")
        print("=" * 70)
        print(f"Date range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
        
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        
        # Load config
        config_entities = self.load_config()
        
        # Apply filters
        if entities:
            entities_lower = [e.lower() for e in entities]
            config_entities = [e for e in config_entities
                            if e['gecko_id'] in entities_lower or e['slug'] in entities_lower]
        
        if types:
            types_lower = [t.lower() for t in types]
            config_entities = [e for e in config_entities
                            if e.get('entity_type', 'protocol') in types_lower]
        
        protocols = [e for e in config_entities if e.get('entity_type', 'protocol') == 'protocol']
        chains = [e for e in config_entities if e.get('entity_type') == 'chain']
        
        print(f"Protocols: {len(protocols)}, Chains: {len(chains)}")
        print("=" * 70)
        
        all_records = []
        
        # Process protocols
        if protocols:
            print(f"\nBackfilling {len(protocols)} protocols...")
            for i, entity in enumerate(protocols, 1):
                gecko_id = entity.get('gecko_id')
                slug = entity.get('slug') or gecko_id
                print(f"  [{i:3d}/{len(protocols)}] {gecko_id}...", end=" ", flush=True)
                
                records = []
                
                # TVL
                tvl_data = self.fetch_protocol_tvl_history(slug)
                if tvl_data:
                    records.extend(self.parse_timeseries(tvl_data, 'TVL', gecko_id, start_ts, end_ts))
                time.sleep(REQUEST_DELAY)
                
                # Fees
                fees_data = self.fetch_fees_history(slug)
                if fees_data:
                    records.extend(self.parse_timeseries(fees_data, 'FEES_DAILY', gecko_id, start_ts, end_ts))
                time.sleep(REQUEST_DELAY)
                
                # Revenue
                rev_data = self.fetch_revenue_history(slug)
                if rev_data:
                    records.extend(self.parse_timeseries(rev_data, 'REVENUE_DAILY', gecko_id, start_ts, end_ts))
                time.sleep(REQUEST_DELAY)
                
                # DEX Volume
                dex_data = self.fetch_dex_volume_history(slug)
                if dex_data:
                    records.extend(self.parse_timeseries(dex_data, 'DEX_VOLUME_DAILY', gecko_id, start_ts, end_ts))
                time.sleep(REQUEST_DELAY)
                
                # Derivatives
                deriv_data = self.fetch_derivatives_history(slug)
                if deriv_data:
                    records.extend(self.parse_timeseries(deriv_data, 'DERIVATIVES_VOLUME_DAILY', gecko_id, start_ts, end_ts))
                time.sleep(REQUEST_DELAY)
                
                if records:
                    all_records.extend(records)
                    print(f"{len(records)} records")
                else:
                    print("no data")
        
        # Process chains
        if chains:
            print(f"\nBackfilling {len(chains)} chains...")
            for i, entity in enumerate(chains, 1):
                gecko_id = entity.get('gecko_id')
                chain_name = entity.get('name') or gecko_id
                print(f"  [{i:3d}/{len(chains)}] {gecko_id}...", end=" ", flush=True)
                
                tvl_data = self.fetch_chain_tvl_history(chain_name)
                if tvl_data:
                    records = self.parse_timeseries(tvl_data, 'CHAIN_TVL', gecko_id, start_ts, end_ts)
                    all_records.extend(records)
                    print(f"{len(records)} records")
                else:
                    print("no data")
                
                time.sleep(REQUEST_DELAY)
        
        # Insert records
        total_inserted = 0
        if all_records:
            total_inserted = self.insert_historical_metrics(all_records)
        
        # Log
        conn = get_connection()
        cur = conn.cursor()
        notes = f"Backfill {start_date} to {end_dt.strftime('%Y-%m-%d')}"
        status = "success" if total_inserted > 0 else "no_data"
        cur.execute(
            """
            INSERT INTO pulls (source_name, pulled_at, status, records_count, notes)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (f"{self.source_name}_backfill", datetime.utcnow(), status, total_inserted, notes)
        )
        conn.commit()
        cur.close()
        conn.close()
        
        print("\n" + "=" * 70)
        print("BACKFILL COMPLETE")
        print(f"  API calls: {self._api_calls}")
        print(f"  Records inserted: {total_inserted}")
        print(f"  Errors: {self._errors}")
        print("=" * 70)
        
        return total_inserted


# Convenience function
def run_backfill(start_date: str, end_date: str = None, **kwargs) -> int:
    """
    Convenience function to run DefiLlama backfill.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        **kwargs: Additional arguments passed to backfill()
        
    Returns:
        int: Records inserted
    """
    source = DefiLlamaBackfillSource()
    return source.backfill(start_date, end_date, **kwargs)
