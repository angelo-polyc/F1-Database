import os
import csv
import time
import requests
from datetime import datetime
from sources.base import BaseSource

REQUEST_DELAY = 2.5  # 30 req/min tier = 2 seconds between requests, add buffer

OVERVIEW_METRICS = [
    'MarketCapitalization',
    'EBITDA',
    'PERatio',
    'PEGRatio',
    'BookValue',
    'DividendPerShare',
    'DividendYield',
    'EPS',
    'RevenuePerShareTTM',
    'ProfitMargin',
    'OperatingMarginTTM',
    'ReturnOnAssetsTTM',
    'ReturnOnEquityTTM',
    'RevenueTTM',
    'GrossProfitTTM',
    'DilutedEPSTTM',
    'QuarterlyEarningsGrowthYOY',
    'QuarterlyRevenueGrowthYOY',
    'AnalystTargetPrice',
    'AnalystRatingStrongBuy',
    'AnalystRatingBuy',
    'AnalystRatingHold',
    'AnalystRatingSell',
    'AnalystRatingStrongSell',
    'TrailingPE',
    'ForwardPE',
    'PriceToSalesRatioTTM',
    'PriceToBookRatio',
    'EVToRevenue',
    'EVToEBITDA',
    'Beta',
    '52WeekHigh',
    '52WeekLow',
    '50DayMovingAverage',
    '200DayMovingAverage',
    'SharesOutstanding',
    'SharesFloat',
    'SharesShort',
    'ShortRatio',
    'ShortPercentOutstanding',
    'ShortPercentFloat',
]

class AlphaVantageSource(BaseSource):
    
    @property
    def source_name(self) -> str:
        return "alphavantage"
    
    def __init__(self):
        self.api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY environment variable not set")
        self.config_path = "alphavantage_config.csv"
        self.base_url = "https://www.alphavantage.co/query"
    
    def load_tickers(self) -> list:
        tickers = []
        with open(self.config_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('pull', '').strip() == '1':
                    tickers.append({
                        'symbol': row['symbol'].strip(),
                        'exchange': row.get('exchange', '').strip(),
                        'category': row.get('category', '').strip()
                    })
        return tickers
    
    def fetch_overview(self, symbol: str) -> dict:
        """Fetch company overview/fundamental data."""
        params = {
            "function": "OVERVIEW",
            "symbol": symbol,
            "apikey": self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if "Error Message" in data:
                return None
            if "Note" in data or "Information" in data:
                return None
            if not data or "Symbol" not in data:
                return None
            
            overview = {"symbol": symbol}
            for metric in OVERVIEW_METRICS:
                val = data.get(metric)
                if val and val != "None" and val != "-":
                    try:
                        overview[metric] = float(val)
                    except (ValueError, TypeError):
                        pass
            
            return overview
            
        except requests.exceptions.RequestException:
            return None
        except (KeyError, ValueError):
            return None
    
    def fetch_daily_data(self, symbol: str) -> dict:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.api_key,
            "outputsize": "compact"
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if "Error Message" in data:
                print(f"  {symbol}: API Error - {data['Error Message']}")
                return None
            
            if "Note" in data:
                print(f"  {symbol}: Rate limit hit - {data['Note']}")
                return None
            
            if "Information" in data:
                print(f"  {symbol}: API Info - {data['Information']}")
                return None
            
            time_series = data.get("Time Series (Daily)", {})
            if not time_series:
                print(f"  {symbol}: No time series data")
                return None
            
            latest_date = max(time_series.keys())
            latest = time_series[latest_date]
            
            return {
                "symbol": symbol,
                "date": latest_date,
                "open": float(latest["1. open"]),
                "high": float(latest["2. high"]),
                "low": float(latest["3. low"]),
                "close": float(latest["4. close"]),
                "volume": int(latest["5. volume"]),
                "dollar_volume": float(latest["4. close"]) * int(latest["5. volume"])
            }
            
        except requests.exceptions.RequestException as e:
            print(f"  {symbol}: Request failed - {e}")
            return None
        except (KeyError, ValueError) as e:
            print(f"  {symbol}: Parse error - {e}")
            return None
    
    def pull(self) -> int:
        print("Starting pull from: alphavantage")
        print("=" * 60)
        print("ALPHA VANTAGE HOURLY PULL (Price + Overview)")
        print("=" * 60)
        
        tickers = self.load_tickers()
        print(f"Tickers to fetch: {len(tickers)}")
        print("=" * 60)
        
        records = []
        success_count = 0
        overview_count = 0
        error_count = 0
        
        for i, ticker_info in enumerate(tickers, 1):
            symbol = ticker_info['symbol']
            print(f"[{i:2}/{len(tickers)}] {symbol}...", end=" ")
            
            data = self.fetch_daily_data(symbol)
            
            if data:
                success_count += 1
                print(f"${data['close']:.2f}, Vol: {data['volume']:,}", end="")
                
                for metric_name in ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'DOLLAR_VOLUME']:
                    value_key = metric_name.lower()
                    records.append({
                        "asset": symbol.lower(),
                        "metric_name": metric_name,
                        "value": data[value_key]
                    })
                
                time.sleep(REQUEST_DELAY)
                
                overview = self.fetch_overview(symbol)
                if overview:
                    overview_count += 1
                    overview_metrics = 0
                    for metric_name, value in overview.items():
                        if metric_name != "symbol" and isinstance(value, (int, float)):
                            records.append({
                                "asset": symbol.lower(),
                                "metric_name": f"OVERVIEW_{metric_name}",
                                "value": value
                            })
                            overview_metrics += 1
                    print(f" +{overview_metrics} overview")
                else:
                    print(" (no overview)")
            else:
                error_count += 1
                print("failed")
            
            if i < len(tickers):
                time.sleep(REQUEST_DELAY)
        
        inserted = self.insert_metrics(records)
        status = "success" if success_count > 0 else "no_data"
        self.log_pull(status, inserted)
        
        print("=" * 60)
        print("COMPLETE")
        print(f"  Tickers: {success_count}/{len(tickers)} successful")
        print(f"  Overview data: {overview_count}/{len(tickers)}")
        print(f"  Records inserted: {inserted}")
        print(f"  Errors: {error_count}")
        print("=" * 60)
        
        print(f"Pull complete. Total records: {inserted}")
        return inserted
