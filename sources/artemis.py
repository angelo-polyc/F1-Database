import os
import csv
import requests
from sources.base import BaseSource

GARBAGE_VALUES = {
    "Metric not found.",
    "Metric not available for asset.",
    "Market data not available for asset.",
    "Latest data not available for this asset.",
}

BATCH_SIZE = 250

class ArtemisSource(BaseSource):
    
    @property
    def source_name(self) -> str:
        return "artemis"
    
    def __init__(self):
        self.api_key = os.environ.get("ARTEMIS_API_KEY")
        if not self.api_key:
            raise ValueError("ARTEMIS_API_KEY environment variable not set")
        self.config_path = "artemis_pull_config.csv"
        self.base_url = "https://api.artemisxyz.com/data"
    
    def load_config(self) -> list:
        configs = []
        with open(self.config_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                configs.append({
                    "metric": row["metric"].strip(),
                    "symbols": [s.strip() for s in row["symbols"].split("|")]
                })
        return configs
    
    def fetch_metric(self, metric: str, symbols: list) -> dict:
        symbols_str = ",".join(symbols)
        url = f"{self.base_url}/{metric}/"
        params = {
            "symbols": symbols_str,
            "APIKey": self.api_key
        }
        
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    
    def parse_response(self, metric: str, data: dict) -> list:
        records = []
        
        if isinstance(data, dict):
            for asset, value in data.items():
                if isinstance(value, str) and value in GARBAGE_VALUES:
                    continue
                if value is None:
                    continue
                try:
                    numeric_value = float(value)
                    records.append({
                        "asset": asset,
                        "metric_name": metric,
                        "value": numeric_value
                    })
                except (ValueError, TypeError):
                    continue
        
        return records
    
    def pull(self) -> int:
        try:
            configs = self.load_config()
        except FileNotFoundError:
            print(f"Config file not found: {self.config_path}")
            self.log_pull("error", 0)
            return 0
        
        total_records = 0
        all_records = []
        
        for config in configs:
            metric = config["metric"]
            symbols = config["symbols"]
            
            for i in range(0, len(symbols), BATCH_SIZE):
                batch = symbols[i:i + BATCH_SIZE]
                
                try:
                    data = self.fetch_metric(metric, batch)
                    records = self.parse_response(metric, data)
                    all_records.extend(records)
                    print(f"  Fetched {len(records)} records for {metric} (batch {i // BATCH_SIZE + 1})")
                except requests.RequestException as e:
                    print(f"  Error fetching {metric}: {e}")
                    continue
        
        if all_records:
            total_records = self.insert_metrics(all_records)
        
        status = "success" if total_records > 0 else "no_data"
        self.log_pull(status, total_records)
        
        print(f"Artemis pull complete: {total_records} records inserted")
        return total_records
