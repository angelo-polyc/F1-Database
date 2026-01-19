import os
import csv
import time
import requests
from sources.base import BaseSource

GARBAGE_VALUES = {
    "Metric not found.",
    "Metric not available for asset.",
    "Market data not available for asset.",
    "Latest data not available for this asset.",
}

BATCH_SIZE = 250
REQUEST_DELAY = 0.3

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

class ArtemisSource(BaseSource):
    
    @property
    def source_name(self) -> str:
        return "artemis"
    
    def __init__(self):
        self.api_key = os.environ.get("ARTEMIS_API_KEY")
        if not self.api_key:
            raise ValueError("ARTEMIS_API_KEY environment variable not set")
        self.config_path = "artemis_config.csv"
        self.base_url = "https://api.artemisxyz.com/data"
    
    def load_config_from_csv(self) -> dict:
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
    
    def extract_value(self, response_data, asset, metric):
        if not response_data:
            return None
        
        inner = response_data.get("data", response_data)
        if "symbols" in inner:
            inner = inner["symbols"]
        
        asset_data = inner.get(asset) or inner.get(asset.lower()) or inner.get(asset.upper())
        if asset_data is None:
            return None
        
        if isinstance(asset_data, dict):
            metric_lower = metric.lower()
            value = asset_data.get(metric_lower) or asset_data.get(metric) or asset_data.get(metric.upper())
            
            if value is None and len(asset_data) == 1:
                value = list(asset_data.values())[0]
            
            return value
        
        return asset_data
    
    def fetch_metric(self, metric: str, symbols: list) -> dict:
        url = f"{self.base_url}/{metric.lower()}/?symbols={','.join(symbols)}&APIKey={self.api_key}"
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = requests.get(url, timeout=60)
                
                if resp.status_code == 200 and resp.text.strip():
                    return resp.json()
                elif resp.status_code == 429 or not resp.text.strip():
                    wait_time = 2 * (attempt + 1)
                    time.sleep(wait_time)
                    continue
                else:
                    return {}
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"    Error: {e}")
                time.sleep(1)
        
        return {}
    
    def pull(self) -> int:
        try:
            pull_config = self.load_config_from_csv()
        except FileNotFoundError:
            print(f"Config file not found: {self.config_path}")
            self.log_pull("error", 0)
            return 0
        except ValueError as e:
            print(f"Config error: {e}")
            self.log_pull("error", 0)
            return 0
        
        all_metrics = list(pull_config.keys())
        all_assets = sorted(set(a for assets in pull_config.values() for a in assets))
        
        total_requests = sum((len(assets) + BATCH_SIZE - 1) // BATCH_SIZE 
                             for assets in pull_config.values())
        
        print("=" * 60)
        print("ARTEMIS DATA PULL")
        print("=" * 60)
        print(f"Metrics: {len(all_metrics)}")
        print(f"Assets: {len(all_assets)}")
        print(f"API requests: {total_requests}")
        print("=" * 60)
        
        all_records = []
        api_calls = 0
        errors = 0
        
        for idx, metric in enumerate(all_metrics):
            assets_for_metric = pull_config[metric]
            print(f"[{idx+1:2d}/{len(all_metrics)}] {metric} ({len(assets_for_metric)})...", end=" ", flush=True)
            
            values_found = 0
            for batch_start in range(0, len(assets_for_metric), BATCH_SIZE):
                batch = assets_for_metric[batch_start:batch_start + BATCH_SIZE]
                
                data = self.fetch_metric(metric, batch)
                api_calls += 1
                
                if not data:
                    errors += 1
                    continue
                
                for asset in batch:
                    value = self.extract_value(data, asset, metric)
                    
                    if value is None:
                        continue
                    if isinstance(value, str) and value in GARBAGE_VALUES:
                        continue
                    
                    try:
                        numeric_value = float(value)
                        all_records.append({
                            "asset": asset,
                            "metric_name": metric,
                            "value": numeric_value
                        })
                        values_found += 1
                    except (ValueError, TypeError):
                        continue
                
                time.sleep(REQUEST_DELAY)
            
            print(f"found {values_found}/{len(assets_for_metric)}")
        
        total_records = 0
        if all_records:
            total_records = self.insert_metrics(all_records)
        
        status = "error" if errors == api_calls else ("success" if total_records > 0 else "no_data")
        self.log_pull(status, total_records)
        
        print("\n" + "=" * 60)
        print(f"COMPLETE")
        print(f"  API calls: {api_calls}")
        print(f"  Records inserted: {total_records}")
        print(f"  Errors: {errors}")
        print("=" * 60)
        
        return total_records
