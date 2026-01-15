#!/usr/bin/env python3
"""
Artemis API - Optimized Targeted Pull
======================================
Maximum request efficiency: uses API's 250 symbol limit per request.
31 metrics = 31 requests (vs 82 with batch size 50).

Features:
- Reads config from CSV (editable in any spreadsheet app)
- Batches up to 250 symbols per request (API maximum)
- Auto-retry on rate limits or empty responses
- Filters garbage API responses (error strings become empty cells)

Usage:
    python artemis_optimized_pull.py

Config file:
    Place artemis_pull_config.csv in the same directory.
    CSV format: name, asset, category, Pull, [metric columns...]
    - Pull=1 marks assets to query
    - Metric=1 marks which metrics to pull for that asset

Output:
    - artemis_data_YYYYMMDD_HHMMSS.json (raw API responses)
    - artemis_data_YYYYMMDD_HHMMSS.csv (clean data, garbage filtered)

Requires:
    - requests (pip install requests)
"""

import requests
import json
import csv
import time
import os
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

API_KEY = "CXDPqeI6WtowV13pHKKhOm0PFjrUJWSGUJpa-kuSMzY"
BASE_URL = "https://api.artemisxyz.com"
REQUEST_DELAY = 0.3  # Seconds between requests
MAX_BATCH = 250      # API maximum symbols per request

# API error strings to treat as empty/null values
GARBAGE_VALUES = {
    "Metric not found.",
    "Metric not available for asset.",
    "Market data not available for asset.",
    "Latest data not available for this asset.",
}

# Metric name mapping (friendly -> API ID)
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
    "Stock Trading Volume": "TOKENIZED_SHARES_TRADING_VOLUME",
    "Stablecoin Supply (USD)": "STABLECOIN_MC",
    "Filtered Stablecoin Transactions": "STABLECOIN_DAILY_TXNS",
    "90-Day ANN Volatility": "VOLATILITY_90D_ANN",
    "Daily Token Trading Volume": "TOKEN_TRADING_VOLUME",
    "Net Asset Value": "NAV",
    "FDMV / NAV": "FDMV_NAV_RATIO",
    "Stablecoins Average DAUs": "STABLECOIN_AVG_DAU",
    "Stablecoin Average Transaction Value": "AVERAGE_TRANSACTION_VALUE",
}


def extract_value(response_data, asset, metric):
    """Extract value from API response - handles all response structures."""
    if not response_data:
        return None
    
    # Navigate: data -> symbols -> asset -> metric
    inner = response_data.get("data", response_data)
    if "symbols" in inner:
        inner = inner["symbols"]
    
    # Find asset (try different cases)
    asset_data = inner.get(asset) or inner.get(asset.lower()) or inner.get(asset.upper())
    if asset_data is None:
        return None
    
    if isinstance(asset_data, dict):
        # Try metric key variations
        metric_lower = metric.lower()
        value = asset_data.get(metric_lower) or asset_data.get(metric) or asset_data.get(metric.upper())
        
        # If single key in dict, use that value
        if value is None and len(asset_data) == 1:
            value = list(asset_data.values())[0]
        
        return value
    
    return asset_data


def load_config_from_csv(csv_path):
    """Load pull configuration from CSV file."""
    pull_config = {}
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        # Find metric columns (everything after 'Pull')
        pull_idx = headers.index('Pull')
        metric_cols = headers[pull_idx + 1:]
        
        for row in reader:
            # Check if this asset should be pulled
            if row['Pull'] in ['1', '1.0', 1, 1.0]:
                asset_id = row['asset']
                
                # Check each metric column
                for metric in metric_cols:
                    if row[metric] in ['1', '1.0', 1, 1.0]:
                        api_id = FRIENDLY_TO_API_ID.get(metric, metric)
                        if api_id not in pull_config:
                            pull_config[api_id] = []
                        if asset_id not in pull_config[api_id]:
                            pull_config[api_id].append(asset_id)
    
    return pull_config


def load_config_from_json(json_path):
    """Load pull configuration from JSON file."""
    with open(json_path, 'r') as f:
        return json.load(f)


def run_pull(pull_config, output_dir="."):
    """Execute the API pull with maximum efficiency."""
    os.makedirs(output_dir, exist_ok=True)
    
    all_metrics = list(pull_config.keys())
    all_assets = sorted(set(a for assets in pull_config.values() for a in assets))
    
    # Calculate request count
    total_requests = sum((len(assets) + MAX_BATCH - 1) // MAX_BATCH 
                         for assets in pull_config.values())
    
    print("=" * 60)
    print("ARTEMIS OPTIMIZED PULL")
    print("=" * 60)
    print(f"Metrics: {len(all_metrics)}")
    print(f"Assets: {len(all_assets)}")
    print(f"API requests: {total_requests}")
    print("=" * 60)
    
    results = {asset: {} for asset in all_assets}
    api_calls = 0
    
    for idx, metric in enumerate(all_metrics):
        assets_for_metric = pull_config[metric]
        print(f"[{idx+1:2d}/{len(all_metrics)}] {metric} ({len(assets_for_metric)})...", end=" ", flush=True)
        
        values_found = 0
        for batch_start in range(0, len(assets_for_metric), MAX_BATCH):
            batch = assets_for_metric[batch_start:batch_start + MAX_BATCH]
            url = f"{BASE_URL}/data/{metric.lower()}/?symbols={','.join(batch)}&APIKey={API_KEY}"
            
            # Retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    resp = requests.get(url, timeout=60)
                    api_calls += 1
                    
                    if resp.status_code == 200 and resp.text.strip():
                        data = resp.json()
                        for asset in batch:
                            value = extract_value(data, asset, metric)
                            results[asset][metric] = value
                            if value is not None:
                                values_found += 1
                        break  # Success, exit retry loop
                    elif resp.status_code == 429 or not resp.text.strip():
                        # Rate limited or empty response - wait and retry
                        wait_time = 2 * (attempt + 1)
                        time.sleep(wait_time)
                        continue
                    else:
                        break  # Other error, don't retry
                except Exception as e:
                    if attempt == max_retries - 1:
                        print(f"\n  Error: {e}")
                    time.sleep(1)
            
            time.sleep(REQUEST_DELAY)
        
        print(f"✓ {values_found}/{len(assets_for_metric)}")
    
    # Save outputs
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    json_file = os.path.join(output_dir, f"artemis_data_{ts}.json")
    with open(json_file, 'w') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "api_calls": api_calls,
            "metrics": all_metrics,
            "assets": all_assets,
            "results": results
        }, f, indent=2)
    
    csv_file = os.path.join(output_dir, f"artemis_data_{ts}.csv")
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['asset'] + all_metrics)
        for asset in all_assets:
            row = [asset]
            for metric in all_metrics:
                val = results[asset].get(metric)
                if val is None or val in GARBAGE_VALUES:
                    row.append('')
                elif isinstance(val, (dict, list)):
                    row.append(json.dumps(val))
                else:
                    row.append(val)
            writer.writerow(row)
    
    # Count valid data points (excluding garbage)
    filled = sum(1 for a in all_assets for m in all_metrics 
                 if results[a].get(m) is not None and results[a].get(m) not in GARBAGE_VALUES)
    
    # Count garbage values filtered
    garbage_count = sum(1 for a in all_assets for m in all_metrics 
                        if results[a].get(m) in GARBAGE_VALUES)
    
    print("\n" + "=" * 60)
    print(f"COMPLETE")
    print(f"  API calls: {api_calls}")
    print(f"  Valid data points: {filled}")
    print(f"  Garbage filtered: {garbage_count}")
    print(f"  JSON: {json_file}")
    print(f"  CSV: {csv_file}")
    print("=" * 60)
    
    return json_file, csv_file


if __name__ == "__main__":
    import sys
    
    # Find config file (CSV preferred)
    csv_paths = [
        "artemis_pull_config.csv",
        "artemis_unified_matrix.csv",
    ]
    json_path = "pull_config.json"
    
    config = None
    
    for path in csv_paths:
        if os.path.exists(path):
            print(f"Loading from: {path}")
            config = load_config_from_csv(path)
            break
    
    if config is None and os.path.exists(json_path):
        print(f"Loading from: {json_path}")
        config = load_config_from_json(json_path)
    
    if config is None:
        print("ERROR: No config file found.")
        print("Expected: artemis_pull_config.csv or pull_config.json")
        sys.exit(1)
    
    run_pull(config, output_dir="artemis_output")
