"""
Artemis Historical Backfill Script

Backfills historical data from Artemis API for all configured assets and metrics.
Uses the same configuration as the live pull script (artemis_config.csv).

Usage:
    python artemis_backfill.py --start-date 2024-01-01 --end-date 2024-06-30
    python artemis_backfill.py --start-date 2024-01-01  # defaults end to yesterday
    python artemis_backfill.py --days 90  # last 90 days
    python artemis_backfill.py --start-date 2024-01-01 --dry-run  # preview without inserting

Environment:
    ARTEMIS_API_KEY - Your Artemis API key
"""

import os
import sys
import csv
import time
import argparse
import requests
from datetime import datetime, timedelta
from typing import Optional

# Import from your existing structure - adjust path as needed
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

GARBAGE_VALUES = {
    "Metric not found.",
    "Metric not available for asset.",
    "Market data not available for asset.",
    "Latest data not available for this asset.",
}

# Batch sizes - Artemis has limits on assets per request
BATCH_SIZE = 50  # Max ~50-75 assets for on-chain metrics
MARKET_BATCH_SIZE = 100  # Can do more for market metrics (PRICE, MC, FDMC)

# Rate limiting
REQUEST_DELAY = 0.15  # ~7 req/sec to be safe
RETRY_DELAY = 2  # Initial retry delay in seconds
MAX_RETRIES = 3

# Date chunking - for very long backfills, split into chunks
MAX_DAYS_PER_REQUEST = 365  # API may have limits on date ranges

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

# Market metrics that support larger batches
MARKET_METRICS = {"PRICE", "MC", "FDMC", "SPOT_VOLUME", "PERP_VOLUME", "OPEN_INTEREST"}

# Mapping from friendly names in config to API metric IDs
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


# ============================================================================
# BACKFILL CLASS
# ============================================================================

class ArtemisBackfill:
    """Handles historical data backfill from Artemis API."""
    
    def __init__(self, api_key: str, config_path: str = "artemis_config.csv"):
        self.api_key = api_key
        self.config_path = config_path
        self.base_url = "https://api.artemisxyz.com/data"
        self.source_name = "artemis"
        
        # Statistics
        self.stats = {
            "api_calls": 0,
            "records_inserted": 0,
            "errors": 0,
            "skipped_existing": 0,
        }
    
    def load_config_from_csv(self) -> dict:
        """Load pull configuration from CSV file.
        
        Returns:
            dict: {metric_id: [list of asset_ids]}
        """
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
    
    def fetch_historical(self, metric: str, symbols: list, 
                         start_date: str, end_date: str) -> dict:
        """Fetch historical data for a metric and symbols.
        
        Args:
            metric: The metric ID (e.g., 'FEES', 'DAU')
            symbols: List of asset symbols
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            dict: API response data
        """
        # Build URL with date parameters
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
                self.stats["api_calls"] += 1
                
                if resp.status_code == 200 and resp.text.strip():
                    return resp.json()
                elif resp.status_code == 429:
                    # Rate limited - wait and retry
                    wait_time = RETRY_DELAY * (attempt + 1)
                    print(f"    Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                elif resp.status_code == 400:
                    # Bad request - likely invalid metric/symbol combo
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
        
        self.stats["errors"] += 1
        return {}
    
    def parse_response(self, response_data: dict, metric: str, 
                       assets: list) -> list:
        """Parse API response into records for database insertion.
        
        Args:
            response_data: Raw API response
            metric: The metric ID
            assets: List of assets that were requested
            
        Returns:
            list: Records with (date, asset, metric_name, value)
        """
        records = []
        
        if not response_data:
            return records
        
        # Navigate to the data - Artemis nests data in different ways
        data = response_data.get("data", response_data)
        
        # Check for symbols wrapper
        if "symbols" in data:
            data = data["symbols"]
        
        # Iterate through each asset
        for asset in assets:
            asset_data = data.get(asset) or data.get(asset.lower()) or data.get(asset.upper())
            
            if asset_data is None:
                continue
            
            # Handle different response formats
            # Format 1: {date: value, date: value, ...}
            # Format 2: {metric: {date: value, ...}}
            # Format 3: [{date: ..., value: ...}, ...]
            
            if isinstance(asset_data, dict):
                # Check if it's metric-nested
                metric_data = asset_data.get(metric.lower()) or asset_data.get(metric)
                if metric_data and isinstance(metric_data, dict):
                    asset_data = metric_data
                
                # Now parse date:value pairs
                for date_str, value in asset_data.items():
                    # Skip if it's a metric key, not a date
                    if not self._is_date_string(date_str):
                        continue
                    
                    if value is None:
                        continue
                    if isinstance(value, str) and value in GARBAGE_VALUES:
                        continue
                    
                    try:
                        numeric_value = float(value)
                        # Parse date string to datetime
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
                # Format: [{date: ..., value: ...}, ...]
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
    
    def _is_date_string(self, s: str) -> bool:
        """Check if a string looks like a date."""
        if not isinstance(s, str):
            return False
        # Check for common date patterns
        if len(s) >= 8 and (s[4] == '-' or s[2] == '-' or s[2] == '/'):
            return True
        if s.isdigit() and len(s) >= 8:  # Unix timestamp or YYYYMMDD
            return True
        return False
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date string formats."""
        formats = [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y/%m/%d",
            "%m/%d/%Y",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str[:len(fmt.replace('%', ''))], fmt)
            except ValueError:
                continue
        
        # Try unix timestamp (milliseconds)
        try:
            if date_str.isdigit():
                ts = int(date_str)
                if ts > 1e12:  # milliseconds
                    ts = ts / 1000
                return datetime.fromtimestamp(ts)
        except (ValueError, OSError):
            pass
        
        return None
    
    def insert_records(self, records: list, skip_existing: bool = True) -> int:
        """Insert records into the metrics table.
        
        Args:
            records: List of record dicts with date, asset, metric_name, value
            skip_existing: If True, skip records that already exist
            
        Returns:
            int: Number of records inserted
        """
        if not records or not HAS_DB:
            return 0
        
        conn = get_connection()
        cur = conn.cursor()
        
        inserted = 0
        for r in records:
            # Normalize date to midnight UTC
            pulled_at = r["date"].replace(hour=0, minute=0, second=0, microsecond=0)
            
            try:
                cur.execute("""
                    INSERT INTO metrics (pulled_at, source, asset, metric_name, value, exchange, granularity)
                    VALUES (%s, %s, %s, %s, %s, %s, 'daily')
                    ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) 
                    DO UPDATE SET value = EXCLUDED.value
                """, (pulled_at, self.source_name, r["asset"], r["metric_name"], r["value"], None))
                inserted += 1
            except Exception as e:
                print(f"    Insert error: {e}")
                self.stats["errors"] += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        return inserted
    
    def log_pull(self, status: str, records_count: int, notes: str = None):
        """Log the backfill operation to pulls table."""
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
    
    def run_backfill(self, start_date: datetime, end_date: datetime, 
                     dry_run: bool = False, metrics_filter: list = None,
                     assets_filter: list = None) -> dict:
        """Run the backfill process.
        
        Args:
            start_date: Start date for backfill
            end_date: End date for backfill
            dry_run: If True, don't insert data, just show what would be done
            metrics_filter: Optional list of specific metrics to backfill
            assets_filter: Optional list of specific assets to backfill
            
        Returns:
            dict: Statistics about the backfill
        """
        print("=" * 70)
        print("ARTEMIS HISTORICAL BACKFILL")
        print("=" * 70)
        print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"Dry run: {dry_run}")
        print()
        
        # Load configuration
        try:
            pull_config = self.load_config_from_csv()
        except FileNotFoundError:
            print(f"ERROR: Config file not found: {self.config_path}")
            return self.stats
        
        # Apply filters
        if metrics_filter:
            pull_config = {k: v for k, v in pull_config.items() 
                         if k in metrics_filter}
        
        if assets_filter:
            for metric in pull_config:
                pull_config[metric] = [a for a in pull_config[metric] 
                                      if a in assets_filter]
            pull_config = {k: v for k, v in pull_config.items() if v}
        
        all_metrics = list(pull_config.keys())
        all_assets = sorted(set(a for assets in pull_config.values() for a in assets))
        
        print(f"Metrics to backfill: {len(all_metrics)}")
        print(f"Assets to backfill: {len(all_assets)}")
        
        # Calculate total date chunks needed
        total_days = (end_date - start_date).days + 1
        date_chunks = []
        
        chunk_start = start_date
        while chunk_start <= end_date:
            chunk_end = min(chunk_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1), end_date)
            date_chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end + timedelta(days=1)
        
        print(f"Date chunks: {len(date_chunks)}")
        print("=" * 70)
        
        # Process each metric
        total_records = 0
        
        for metric_idx, metric in enumerate(all_metrics, 1):
            assets_for_metric = pull_config[metric]
            
            # Determine batch size based on metric type
            batch_size = MARKET_BATCH_SIZE if metric in MARKET_METRICS else BATCH_SIZE
            
            print(f"\n[{metric_idx:2d}/{len(all_metrics)}] {metric} ({len(assets_for_metric)} assets)")
            
            metric_records = 0
            
            # Process in batches
            for batch_start in range(0, len(assets_for_metric), batch_size):
                batch = assets_for_metric[batch_start:batch_start + batch_size]
                batch_num = batch_start // batch_size + 1
                total_batches = (len(assets_for_metric) + batch_size - 1) // batch_size
                
                # Process each date chunk
                for chunk_idx, (chunk_start, chunk_end) in enumerate(date_chunks):
                    start_str = chunk_start.strftime("%Y-%m-%d")
                    end_str = chunk_end.strftime("%Y-%m-%d")
                    
                    print(f"    Batch {batch_num}/{total_batches}, "
                          f"Dates {start_str} to {end_str}...", end=" ", flush=True)
                    
                    if dry_run:
                        print(f"[DRY RUN] Would fetch {len(batch)} assets")
                        continue
                    
                    # Fetch data
                    response = self.fetch_historical(metric, batch, start_str, end_str)
                    
                    if not response:
                        print("no data")
                        time.sleep(REQUEST_DELAY)
                        continue
                    
                    # Parse response
                    records = self.parse_response(response, metric, batch)
                    
                    if records:
                        # Insert records
                        inserted = self.insert_records(records)
                        metric_records += inserted
                        print(f"{inserted} records")
                    else:
                        print("0 records")
                    
                    time.sleep(REQUEST_DELAY)
            
            print(f"    Total for {metric}: {metric_records} records")
            total_records += metric_records
        
        # Log the operation
        self.stats["records_inserted"] = total_records
        
        if not dry_run and HAS_DB:
            notes = f"Backfill {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            status = "success" if total_records > 0 else "no_data"
            self.log_pull(status, total_records, notes)
        
        # Print summary
        print("\n" + "=" * 70)
        print("BACKFILL COMPLETE")
        print("=" * 70)
        print(f"  API calls:        {self.stats['api_calls']}")
        print(f"  Records inserted: {self.stats['records_inserted']}")
        print(f"  Errors:           {self.stats['errors']}")
        print("=" * 70)
        
        return self.stats


# ============================================================================
# CLI INTERFACE
# ============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Backfill historical data from Artemis API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill specific date range
  python artemis_backfill.py --start-date 2024-01-01 --end-date 2024-06-30

  # Backfill last 90 days
  python artemis_backfill.py --days 90

  # Backfill with dry run (no database writes)
  python artemis_backfill.py --start-date 2024-01-01 --dry-run

  # Backfill specific metrics only
  python artemis_backfill.py --start-date 2024-01-01 --metrics FEES,REVENUE,DAU

  # Backfill specific assets only
  python artemis_backfill.py --start-date 2024-01-01 --assets btc,eth,sol

Environment Variables:
  ARTEMIS_API_KEY    Your Artemis API key (required)
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
        default="artemis_config.csv",
        help="Path to config CSV file (default: artemis_config.csv)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be done without inserting data"
    )
    
    parser.add_argument(
        "--metrics", "-m",
        type=str,
        default=None,
        help="Comma-separated list of specific metrics to backfill"
    )
    
    parser.add_argument(
        "--assets", "-a",
        type=str,
        default=None,
        help="Comma-separated list of specific assets to backfill"
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Get API key
    api_key = os.environ.get("ARTEMIS_API_KEY")
    if not api_key:
        print("ERROR: ARTEMIS_API_KEY environment variable not set")
        print("Set it with: export ARTEMIS_API_KEY='your_key_here'")
        sys.exit(1)
    
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
    
    if end_date > datetime.now():
        print("WARNING: End date is in the future, adjusting to today")
        end_date = datetime.now()
    
    # Parse filters
    metrics_filter = None
    if args.metrics:
        metrics_filter = [m.strip().upper() for m in args.metrics.split(",")]
    
    assets_filter = None
    if args.assets:
        assets_filter = [a.strip().lower() for a in args.assets.split(",")]
    
    # Run backfill
    backfill = ArtemisBackfill(api_key, args.config)
    stats = backfill.run_backfill(
        start_date=start_date,
        end_date=end_date,
        dry_run=args.dry_run,
        metrics_filter=metrics_filter,
        assets_filter=assets_filter,
    )
    
    # Exit with appropriate code
    sys.exit(0 if stats["errors"] == 0 else 1)


if __name__ == "__main__":
    main()
