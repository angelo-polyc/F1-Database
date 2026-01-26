import os
import csv
import time
import requests
from datetime import datetime, timezone
from sources.base import BaseSource
from db.setup import get_connection

COINGECKO_API_KEY = os.environ.get('COINGECKO_API_KEY', '')
USE_PRO_API = bool(COINGECKO_API_KEY)

DEMO_BASE_URL = "https://api.coingecko.com/api/v3"
PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"
BASE_URL = PRO_BASE_URL if USE_PRO_API else DEMO_BASE_URL

RATE_LIMIT_CALLS_PER_MIN = 500 if USE_PRO_API else 30
REQUEST_DELAY = 60 / RATE_LIMIT_CALLS_PER_MIN + 0.1

MAX_COINS_PER_REQUEST = 250

METRIC_MAP = {
    'current_price': 'PRICE',
    'total_volume': 'VOLUME_24H',
    'market_cap': 'MARKET_CAP',
    'market_cap_rank': 'MARKET_CAP_RANK',
    'high_24h': 'HIGH_24H',
    'low_24h': 'LOW_24H',
    'price_change_24h': 'PRICE_CHANGE_24H',
    'price_change_percentage_24h': 'PRICE_CHANGE_PCT_24H',
    'price_change_percentage_1h_in_currency': 'PRICE_CHANGE_PCT_1H',
    'price_change_percentage_7d_in_currency': 'PRICE_CHANGE_PCT_7D',
    'circulating_supply': 'CIRCULATING_SUPPLY',
    'total_supply': 'TOTAL_SUPPLY',
    'max_supply': 'MAX_SUPPLY',
    'ath': 'ATH',
    'atl': 'ATL',
    'fully_diluted_valuation': 'FDV',
}


class CoinGeckoSource(BaseSource):
    
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
    
    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: dict = None):
        self._rate_limit()
        url = f"{BASE_URL}/{endpoint}"
        
        if not USE_PRO_API and COINGECKO_API_KEY:
            params = params or {}
            params['x_cg_demo_api_key'] = COINGECKO_API_KEY
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 429:
                print("[CoinGecko] Rate limited - waiting 60 seconds")
                time.sleep(60)
                return self._make_request(endpoint, params)
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"[CoinGecko] API request failed: {endpoint} - {e}")
            return None
    
    def _load_entity_cache(self):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT source_id, entity_id
            FROM entity_source_ids
            WHERE source = 'coingecko'
        """)
        for row in cur.fetchall():
            self.entity_cache[row[0]] = row[1]
        cur.close()
        conn.close()
        print(f"[CoinGecko] Loaded {len(self.entity_cache)} entity mappings")
    
    def load_config(self):
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'coingecko_config.csv')
        if not os.path.exists(config_path):
            print(f"[CoinGecko] Config file not found: {config_path}")
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
            print(f"[CoinGecko] Loaded {len(mappings)} symbols from config")
            return mappings
        except Exception as e:
            print(f"[CoinGecko] Failed to load config: {e}")
            return {}
    
    def get_coins_markets(self, coin_ids: list, vs_currency: str = 'usd'):
        if not coin_ids:
            return []
        
        params = {
            'vs_currency': vs_currency,
            'ids': ','.join(coin_ids),
            'order': 'market_cap_desc',
            'per_page': min(len(coin_ids), MAX_COINS_PER_REQUEST),
            'page': 1,
            'sparkline': 'false',
            'price_change_percentage': '1h,24h,7d'
        }
        
        result = self._make_request('coins/markets', params)
        return result if result else []
    
    def insert_metrics_hourly(self, records: list) -> int:
        if not records:
            return 0
        
        conn = get_connection()
        cur = conn.cursor()
        
        now_utc = datetime.now(timezone.utc)
        pulled_at = now_utc.replace(minute=0, second=0, microsecond=0)
        
        rows = [(pulled_at, self.source_name, r["asset"], r["metric_name"], r["value"], None) for r in records]
        
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
    
    def pull(self) -> int:
        print("\n" + "=" * 60)
        print("COINGECKO DATA PULL")
        print("=" * 60)
        
        if not COINGECKO_API_KEY:
            print("[CoinGecko] Warning: No API key set. Using demo API (30 calls/min limit)")
        else:
            print(f"[CoinGecko] Using Pro API (500 calls/min limit)")
        
        self._load_entity_cache()
        
        symbol_to_id = self.load_config()
        if not symbol_to_id:
            print("[CoinGecko] No symbols configured")
            self.log_pull("no_data", 0)
            return 0
        
        coin_ids = list(set(symbol_to_id.values()))
        print(f"[CoinGecko] Fetching data for {len(coin_ids)} unique coins")
        
        all_data = []
        api_calls = 0
        
        for i in range(0, len(coin_ids), MAX_COINS_PER_REQUEST):
            batch = coin_ids[i:i + MAX_COINS_PER_REQUEST]
            batch_num = i // MAX_COINS_PER_REQUEST + 1
            total_batches = (len(coin_ids) + MAX_COINS_PER_REQUEST - 1) // MAX_COINS_PER_REQUEST
            print(f"[CoinGecko] Fetching batch {batch_num}/{total_batches}: {len(batch)} coins")
            
            data = self.get_coins_markets(batch)
            api_calls += 1
            
            if data:
                all_data.extend(data)
                print(f"[CoinGecko]   Retrieved {len(data)} records")
            else:
                print(f"[CoinGecko]   Batch returned no data")
        
        print(f"[CoinGecko] Total records retrieved: {len(all_data)}")
        
        retrieved_ids = {item['id'] for item in all_data}
        missing = [sym for sym, cg_id in symbol_to_id.items() if cg_id not in retrieved_ids]
        if missing:
            print(f"[CoinGecko] Missing data for {len(missing)} symbols: {missing[:10]}{'...' if len(missing) > 10 else ''}")
        
        all_records = []
        for item in all_data:
            cg_id = item.get('id', '')
            symbol = item.get('symbol', '').upper()
            
            asset = self.entity_cache.get(cg_id, cg_id)
            
            for api_field, metric_name in METRIC_MAP.items():
                value = item.get(api_field)
                if value is not None and value != '':
                    try:
                        float_val = float(value)
                        all_records.append({
                            "asset": asset,
                            "metric_name": metric_name,
                            "value": float_val
                        })
                    except (ValueError, TypeError):
                        pass
        
        print(f"[CoinGecko] Generated {len(all_records)} metric records")
        
        total_records = 0
        if all_records:
            total_records = self.insert_metrics_hourly(all_records)
        
        status = "success" if total_records > 0 else "no_data"
        self.log_pull(status, total_records)
        
        print("\n" + "=" * 60)
        print("COMPLETE")
        print(f"  Coins: {len(all_data)}/{len(coin_ids)}")
        print(f"  Records inserted: {total_records}")
        print(f"  API calls: {api_calls}")
        print("=" * 60)
        
        return total_records
