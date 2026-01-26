"""
Artemis Historical Backfill Source Module

Integrates with the existing sources/ structure for historical data backfills.
Place this file in your sources/ directory.

Usage:
    from sources.artemis_backfill import ArtemisBackfillSource
    
    source = ArtemisBackfillSource()
    source.backfill(start_date="2024-01-01", end_date="2024-06-30")
"""

import os
import csv
import time
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from sources.base import BaseSource
from db.setup import get_connection

# ============================================================================
# CONFIGURATION
# ============================================================================

GARBAGE_VALUES = {
    "Metric not found.",
    "Metric not available for asset.",
    "Market data not available for asset.",
    "Latest data not available for this asset.",
}

# Batch sizes
BATCH_SIZE = 50
MARKET_BATCH_SIZE = 100

# Rate limiting
REQUEST_DELAY = 0.15
RETRY_DELAY = 2
MAX_RETRIES = 3

# Date chunking
MAX_DAYS_PER_REQUEST = 365

# Metrics to exclude
EXCLUDED_METRICS = {
    "VOLATILITY_90D_ANN",
    "90-Day ANN Volatility",
    "STABLECOIN_AVG_DAU",
    "Stablecoins Average DAUs",
    "TOKENIZED_SHARES_TRADING_VOLUME",
    "Stock Trading Volume",
    "FDMV_NAV_RATIO",
    "FDMV / NAV",
}

MARKET_METRICS = {"PRICE", "MC", "FDMC", "SPOT_VOLUME", "PERP_VOLUME", "OPEN_INTEREST"}

FRIENDLY_TO_API_ID = {
    "Fees": "FEES",
    "Open Interest": "OPEN_INTEREST",
    "Perpetuals Volume": "PERP_VOLUME",
    "Revenue": "REVENUE",
    "Daily Active Users": "DAU",
    "New Users": "NEW_USERS",
    "Transactions": "TXNS",
    "Perpetual Liquidations": "PERP_LIQUIDATION",
    "Circulating Supply": "CIRCULATING_SUPPLY_NATIVE",
    "Total Supply Native": "TOTAL_SUPPLY_NATIVE",
    "Spot Volume": "SPOT_VOLUME",
    "Spot Fees": "SPOT_FEES",
    "Price": "PRICE",
    "FDMC / Fees": "FDMC_FEES_RATIO",
    "FDMC / Revenue": "FDMC_REVENUE_RATIO",
    "Market Cap": "MC",
    "Stablecoin Transfer Volume": "STABLECOIN_TRANSFER_VOLUME",
    "Gross Emissions": "GROSS_EMISSIONS",
    "Fully Diluted Market Cap": "FDMC",
    "Enterprise Value": "EQ_EV",
    "Latest Cash": "EQ_CASH_AND_CASH_EQUIVALENTS",
    "Debt": "EQ_TOTAL_DEBT",
    "Stablecoin Supply (USD)": "STABLECOIN_MC",
    "Filtered Stablecoin Transactions": "STABLECOIN_DAILY_TXNS",
    "Daily Token Trading Volume": "24H_VOLUME",
    "Net Asset Value": "NAV",
    "Stablecoin Average Transaction Value": "AVERAGE_TRANSACTION_VALUE",
    "Average Transaction Fee": "AVG_TXN_FEE",
    "Perpetual Fees": "PERP_FEES",
    "Perpetual Transactions": "PERP_TXNS",
    "Lending Deposits": "LENDING_DEPOSITS",
    "Lending Borrows": "LENDING_BORROWS",
}


class ArtemisBackfillSource(BaseSource):
    """Artemis historical backfill source - extends BaseSource for consistency."""
    
    @property
    def source_name(self) -> str:
        return "artemis"
    
    def __init__(self):
        self.api_key = os.environ.get("ARTEMIS_API_KEY")
        if not self.api_key:
            raise ValueError("ARTEMIS_API_KEY environment variable not set")
        self.config_path = "artemis_config.csv"
        self.base_url = "https://api.artemisxyz.com/data"
        
        # Stats tracking
        self._api_calls = 0
        self._errors = 0
    
    def load_config_from_csv(self) -> Dict[str, List[str]]:
        """Load pull configuration from CSV file."""
        pull_config = {}
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            
            pull_idx = headers.index('Pull') if 'Pull' in headers else -1
            metric_cols = headers[pull_idx + 1:] if pull_idx >= 0 else []
            
            for row in reader:
                if row['Pull'] in ['1', '1.0', 1, 1.0]:
                    asset_id = row['asset']
                    
                    for metric in metric_cols:
                        if metric in EXCLUDED_METRICS:
                            continue
                        if row.get(metric) in ['1', '1.0', 1, 1.0]:
                            api_id = FRIENDLY_TO_API_ID.get(metric, metric)
                            if api_id in EXCLUDED_METRICS:
                                continue
                            if api_id not in pull_config:
                                pull_config[api_id] = []
                            if asset_id not in pull_config[api_id]:
                                pull_config[api_id].append(asset_id)
        
        return pull_config
    
    def fetch_historical(self, metric: str, symbols: List[str], 
                         start_date: str, end_date: str) -> dict:
        """Fetch historical data from Artemis API."""
        params = {
            "symbols": ",".join(symbols),
            "APIKey": self.api_key,
            "startDate": start_date,
            "endDate": end_date,
        }
        
        url = f"{self.base_url}/{metric.lower()}/"
        
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(url, params=params, timeout=120)
                self._api_calls += 1
                
                if resp.status_code == 200 and resp.text.strip():
                    return resp.json()
                elif resp.status_code == 429:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    print(f"    Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                elif resp.status_code == 400:
                    return {}
                else:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                    continue
                    
            except requests.exceptions.Timeout:
                print(f"    Timeout on attempt {attempt + 1}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * 2)
            except Exception as e:
                print(f"    Error: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        
        self._errors += 1
        return {}
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date string formats."""
        formats = [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%fZ",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str[:len(fmt.replace('%', '').replace('f', '000'))], fmt)
            except ValueError:
                continue
        
        # Try unix timestamp
        try:
            if date_str.isdigit():
                ts = int(date_str)
                if ts > 1e12:
                    ts = ts / 1000
                return datetime.fromtimestamp(ts)
        except (ValueError, OSError):
            pass
        
        return None
    
    def _is_date_string(self, s: str) -> bool:
        """Check if a string looks like a date."""
        if not isinstance(s, str):
            return False
        if len(s) >= 8 and (s[4] == '-' or s[2] == '-'):
            return True
        if s.isdigit() and len(s) >= 8:
            return True
        return False
    
    def parse_response(self, response_data: dict, metric: str, 
                       assets: List[str]) -> List[Dict[str, Any]]:
        """Parse API response into records."""
        records = []
        
        if not response_data:
            return records
        
        data = response_data.get("data", response_data)
        if "symbols" in data:
            data = data["symbols"]
        
        for asset in assets:
            asset_data = data.get(asset) or data.get(asset.lower()) or data.get(asset.upper())
            
            if asset_data is None:
                continue
            
            if isinstance(asset_data, dict):
                metric_data = asset_data.get(metric.lower()) or asset_data.get(metric)
                if metric_data and isinstance(metric_data, dict):
                    asset_data = metric_data
                
                for date_str, value in asset_data.items():
                    if not self._is_date_string(date_str):
                        continue
                    
                    if value is None or (isinstance(value, str) and value in GARBAGE_VALUES):
                        continue
                    
                    try:
                        numeric_value = float(value)
                        parsed_date = self._parse_date(date_str)
                        if parsed_date:
                            records.append({
                                "date": parsed_date,
                                "asset": asset,
                                "metric_name": metric,
                                "value": numeric_value,
                            })
                    except (ValueError, TypeError):
                        continue
                        
            elif isinstance(asset_data, list):
                for item in asset_data:
                    if isinstance(item, dict):
                        date_str = item.get("date") or item.get("timestamp")
                        value = item.get("value") or item.get(metric.lower())
                        
                        if date_str and value is not None:
                            try:
                                numeric_value = float(value)
                                parsed_date = self._parse_date(date_str)
                                if parsed_date:
                                    records.append({
                                        "date": parsed_date,
                                        "asset": asset,
                                        "metric_name": metric,
                                        "value": numeric_value,
                                    })
                            except (ValueError, TypeError):
                                continue
        
        return records
    
    def insert_historical_metrics(self, records: List[Dict[str, Any]]) -> int:
        """Insert historical records with upsert behavior."""
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
    
    def pull(self) -> int:
        """Standard pull method - pulls latest data only (for compatibility)."""
        # For backfill, use the backfill() method instead
        print("Use backfill() method for historical data")
        return 0
    
    def backfill(self, start_date: str, end_date: str = None,
                 metrics: List[str] = None, assets: List[str] = None) -> int:
        """
        Backfill historical data from Artemis.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (default: yesterday)
            metrics: Optional list of specific metrics to backfill
            assets: Optional list of specific assets to backfill
            
        Returns:
            int: Total records inserted
        """
        # Parse dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        if end_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end_dt = datetime.now() - timedelta(days=1)
        
        print("=" * 70)
        print("ARTEMIS HISTORICAL BACKFILL")
        print("=" * 70)
        print(f"Date range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
        
        # Load config
        try:
            pull_config = self.load_config_from_csv()
        except FileNotFoundError:
            print(f"Config file not found: {self.config_path}")
            self.log_pull("error", 0)
            return 0
        
        # Apply filters
        if metrics:
            metrics_upper = [m.upper() for m in metrics]
            pull_config = {k: v for k, v in pull_config.items() if k in metrics_upper}
        
        if assets:
            assets_lower = [a.lower() for a in assets]
            for metric in pull_config:
                pull_config[metric] = [a for a in pull_config[metric] if a in assets_lower]
            pull_config = {k: v for k, v in pull_config.items() if v}
        
        all_metrics = list(pull_config.keys())
        all_assets = sorted(set(a for v in pull_config.values() for a in v))
        
        print(f"Metrics: {len(all_metrics)}")
        print(f"Assets: {len(all_assets)}")
        print("=" * 70)
        
        # Calculate date chunks
        date_chunks = []
        chunk_start = start_dt
        while chunk_start <= end_dt:
            chunk_end = min(chunk_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1), end_dt)
            date_chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end + timedelta(days=1)
        
        total_records = 0
        
        for metric_idx, metric in enumerate(all_metrics, 1):
            assets_for_metric = pull_config[metric]
            batch_size = MARKET_BATCH_SIZE if metric in MARKET_METRICS else BATCH_SIZE
            
            print(f"\n[{metric_idx:2d}/{len(all_metrics)}] {metric} ({len(assets_for_metric)} assets)")
            
            metric_records = 0
            
            for batch_start in range(0, len(assets_for_metric), batch_size):
                batch = assets_for_metric[batch_start:batch_start + batch_size]
                batch_num = batch_start // batch_size + 1
                total_batches = (len(assets_for_metric) + batch_size - 1) // batch_size
                
                for chunk_start, chunk_end in date_chunks:
                    start_str = chunk_start.strftime("%Y-%m-%d")
                    end_str = chunk_end.strftime("%Y-%m-%d")
                    
                    print(f"    Batch {batch_num}/{total_batches}, "
                          f"{start_str} to {end_str}...", end=" ", flush=True)
                    
                    response = self.fetch_historical(metric, batch, start_str, end_str)
                    
                    if not response:
                        print("no data")
                        time.sleep(REQUEST_DELAY)
                        continue
                    
                    records = self.parse_response(response, metric, batch)
                    
                    if records:
                        inserted = self.insert_historical_metrics(records)
                        metric_records += inserted
                        print(f"{inserted} records")
                    else:
                        print("0 records")
                    
                    time.sleep(REQUEST_DELAY)
            
            print(f"    Subtotal: {metric_records} records")
            total_records += metric_records
        
        # Log the operation
        notes = f"Backfill {start_date} to {end_dt.strftime('%Y-%m-%d')}"
        status = "success" if total_records > 0 else "no_data"
        
        # Log to pulls table with special source name
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pulls (source_name, pulled_at, status, records_count, notes)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (f"{self.source_name}_backfill", datetime.utcnow(), status, total_records, notes)
        )
        conn.commit()
        cur.close()
        conn.close()
        
        print("\n" + "=" * 70)
        print("BACKFILL COMPLETE")
        print(f"  API calls: {self._api_calls}")
        print(f"  Records inserted: {total_records}")
        print(f"  Errors: {self._errors}")
        print("=" * 70)
        
        return total_records


# Convenience function for quick backfills
def run_backfill(start_date: str, end_date: str = None, **kwargs) -> int:
    """
    Convenience function to run Artemis backfill.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD), defaults to yesterday
        **kwargs: Additional arguments passed to backfill()
        
    Returns:
        int: Records inserted
    """
    source = ArtemisBackfillSource()
    return source.backfill(start_date, end_date, **kwargs)
