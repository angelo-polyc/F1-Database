import os
import csv
import time
import threading
import requests
from requests.adapters import HTTPAdapter
from typing import Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from sources.base import BaseSource

_thread_local = threading.local()

METRIC_MAP = {
    'protocols_tvl': 'TVL',
    'chains_tvl': 'CHAIN_TVL',
    'protocols_staking': 'STAKING',
    'protocols_borrowed': 'BORROWED',
    'protocols_pool2': 'POOL2',
    'protocols_change_1h': 'CHANGE_1H',
    'protocols_change_1d': 'CHANGE_1D',
    'protocols_change_7d': 'CHANGE_7D',
    'fees_total24h': 'FEES_24H',
    'fees_total7d': 'FEES_7D',
    'fees_total30d': 'FEES_30D',
    'fees_total1y': 'FEES_1Y',
    'fees_totalAllTime': 'FEES_ALL_TIME',
    'revenue_total24h': 'REVENUE_24H',
    'revenue_total7d': 'REVENUE_7D',
    'revenue_total30d': 'REVENUE_30D',
    'revenue_total1y': 'REVENUE_1Y',
    'revenue_totalAllTime': 'REVENUE_ALL_TIME',
    'fees_average1y': 'FEES_AVERAGE_1Y',
    'fees_change_1d': 'FEES_CHANGE_1D',
    'fees_change_7d': 'FEES_CHANGE_7D',
    'fees_change_1m': 'FEES_CHANGE_1M',
    'dexs_total24h': 'DEX_VOLUME_24H',
    'dexs_total7d': 'DEX_VOLUME_7D',
    'dexs_total30d': 'DEX_VOLUME_30D',
    'dexs_totalAllTime': 'DEX_VOLUME_ALL_TIME',
    'dexs_change_1d': 'DEX_CHANGE_1D',
    'dexs_change_7d': 'DEX_CHANGE_7D',
    'dexs_change_1m': 'DEX_CHANGE_1M',
    'derivatives_total24h': 'DERIVATIVES_VOLUME_24H',
    'derivatives_total7d': 'DERIVATIVES_VOLUME_7D',
    'derivatives_total30d': 'DERIVATIVES_VOLUME_30D',
    'derivatives_change_1d': 'DERIVATIVES_CHANGE_1D',
    'derivatives_change_7d': 'DERIVATIVES_CHANGE_7D',
    'options_total24h': 'OPTIONS_VOLUME_24H',
    'options_total7d': 'OPTIONS_VOLUME_7D',
    'options_total30d': 'OPTIONS_VOLUME_30D',
    'options_dailyNotionalVolume': 'OPTIONS_NOTIONAL_24H',
    'options_dailyPremiumVolume': 'OPTIONS_PREMIUM_24H',
    'aggregators_total24h': 'AGGREGATOR_VOLUME_24H',
    'aggregators_total7d': 'AGGREGATOR_VOLUME_7D',
    'aggregators_total30d': 'AGGREGATOR_VOLUME_30D',
    'bridges_lastDailyVolume': 'BRIDGE_VOLUME_24H',
    'bridges_weeklyVolume': 'BRIDGE_VOLUME_7D',
    'bridges_monthlyVolume': 'BRIDGE_VOLUME_30D',
    'stablecoins_circulating_peggedUSD': 'STABLECOIN_SUPPLY',
    'stablecoins_circulatingPrevDay_peggedUSD': 'STABLECOIN_SUPPLY_PREV_DAY',
    'stablecoins_circulatingPrevWeek_peggedUSD': 'STABLECOIN_SUPPLY_PREV_WEEK',
    'stablecoins_price': 'STABLECOIN_PRICE',
    'inflows': 'INFLOW',
    'outflows': 'OUTFLOW',
    'chain_fees_24h': 'CHAIN_FEES_24H',
    'chain_revenue_24h': 'CHAIN_REVENUE_24H',
    'chain_dex_volume_24h': 'CHAIN_DEX_VOLUME_24H',
    'chain_perps_volume_24h': 'CHAIN_PERPS_VOLUME_24H',
    'chain_app_fees_24h': 'CHAIN_APP_FEES_24H',
    'chain_app_revenue_24h': 'CHAIN_APP_REVENUE_24H',
    'chain_token_incentives_24h': 'CHAIN_TOKEN_INCENTIVES_24H',
    'protocol_earnings': 'EARNINGS',
    'protocol_incentives': 'INCENTIVES',
    'chain_options_volume_24h': 'CHAIN_OPTIONS_VOLUME_24H',
    'open_interest': 'OPEN_INTEREST',
}

MAX_WORKERS = 10  # Concurrent requests
RATE_LIMIT_PER_SEC = 15  # Stay under 1000/min = 16.6/sec

STABLECOIN_IDS = {
    'dai': 5,
    'usds': 209,
    'ethena-usde': 146,
    'ondo-us-dollar-yield': 129,
    'resolv-usr': 197,
    'agora-dollar': 205,
    'f-x-protocol-usd': 168,
    'magic-internet-money': 10,
    'gyroscope-gyd': 185,
    'celo-dollar': 24,
    'musd': 26,
    'celo-euro': 52,
    'celo-real-creal': 199,
}

BRIDGE_GECKO_IDS = {
    'wormhole', 'octus-bridge', 'connext', 'debridge', 'across-protocol',
    'celer-network', 'hop-protocol', 'multichain', 'layerzero', 'synapse-2',
    'orbit-chain', 'graviton', 'pnetwork'
}

EXTRA_CHAINS = {'ronin', 'stride', 'babylon genesis', 'babylon'}

class DefiLlamaSource(BaseSource):
    
    @property
    def source_name(self) -> str:
        return "defillama"
    
    def __init__(self):
        self.config_path = "defillama_config.csv"
        self.api_key = os.environ.get("DEFILLAMA_API_KEY")
        self.base_url = "https://pro-api.llama.fi" if self.api_key else "https://api.llama.fi"
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
    
    def fetch_json(self, url: str) -> Optional[dict]:
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"  Error fetching {url}: {e}")
            return None
    
    def fetch_pro_json(self, endpoint: str) -> Optional[dict]:
        if not self.api_key:
            return None
        url = f"{self.base_url}/{self.api_key}{endpoint}"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return None
    
    def _get_thread_session(self):
        if not hasattr(_thread_local, 'session'):
            _thread_local.session = requests.Session()
            adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5)
            _thread_local.session.mount('https://', adapter)
            _thread_local.session.mount('http://', adapter)
        return _thread_local.session
    
    def _fetch_json_threadsafe(self, url: str) -> Optional[dict]:
        try:
            session = self._get_thread_session()
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None
    
    def fetch_urls_parallel(self, urls: list) -> dict:
        results = {}
        total = len(urls)
        batch_size = min(MAX_WORKERS * 3, total)
        
        for i in range(0, total, batch_size):
            batch = urls[i:i + batch_size]
            batch_start = time.time()
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_url = {executor.submit(self._fetch_json_threadsafe, url): url for url in batch}
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        results[url] = future.result()
                    except Exception:
                        results[url] = None
            
            # Rate limiting: ensure we don't exceed RATE_LIMIT_PER_SEC
            batch_time = time.time() - batch_start
            min_time = len(batch) / RATE_LIMIT_PER_SEC
            if batch_time < min_time:
                time.sleep(min_time - batch_time)
        
        return results
    
    def fetch_inflows(self, protocol_slug: str) -> dict:
        if not self.api_key:
            return {}
        today_ts = int(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        data = self.fetch_pro_json(f"/api/inflows/{protocol_slug}/{today_ts}")
        if data:
            return {
                'inflows': data.get('inflows'),
                'outflows': data.get('outflows')
            }
        return {}
    
    def fetch_all_open_interest(self) -> dict:
        if not self.api_key:
            return {}
        data = self.fetch_pro_json("/yields/perps")
        if not data or not data.get('data'):
            return {}
        oi_by_market = {}
        for pool in data.get('data', []):
            market = pool.get('market', '').lower()
            oi = pool.get('openInterest')
            marketplace = pool.get('marketplace', '')
            if market and oi is not None:
                key = f"{marketplace}_{market}"
                oi_by_market[key] = {
                    'open_interest': oi,
                    'marketplace': marketplace,
                    'market': market,
                    'base_asset': pool.get('baseAsset', ''),
                    'funding_rate': pool.get('fundingRate'),
                    'index_price': pool.get('indexPrice')
                }
        return oi_by_market
    
    def get_official_chains(self) -> set:
        chains_data = self.fetch_json('https://api.llama.fi/chains')
        chain_names = set(EXTRA_CHAINS)
        if chains_data:
            for c in chains_data:
                if c.get('name'):
                    chain_names.add(c['name'].lower())
                if c.get('gecko_id'):
                    chain_names.add(c['gecko_id'].lower())
        return chain_names
    
    def fetch_stablecoin_circulating(self, stablecoin_id: int) -> Optional[float]:
        data = self.fetch_json(f'https://stablecoins.llama.fi/stablecoin/{stablecoin_id}')
        if data:
            tokens = data.get('tokens', [])
            if tokens:
                latest = tokens[-1]
                circulating = latest.get('circulating', {})
                return circulating.get('peggedUSD')
        return None
    
    def fetch_bridge_volume(self, bridge_id: int) -> dict:
        data = self.fetch_json(f'https://bridges.llama.fi/bridge/{bridge_id}')
        if data:
            return {
                'bridges_lastDailyVolume': data.get('lastDailyVolume'),
                'bridges_weeklyVolume': data.get('weeklyVolume'),
                'bridges_monthlyVolume': data.get('monthlyVolume'),
                'bridges_currentDayVolume': data.get('currentDayVolume'),
            }
        return {}
    
    def fetch_chain_metrics(self, chain_name: str, gecko_id: str = '', slug: str = '') -> dict:
        metrics = {}
        if slug:
            chain_slug = slug.lower()
        elif chain_name:
            chain_slug = chain_name.lower().replace(' ', '-')
        else:
            chain_slug = gecko_id.lower()
        
        if not chain_slug:
            return metrics
        
        fees_data = self.fetch_json(f'https://api.llama.fi/overview/fees/{chain_slug}')
        if fees_data:
            metrics['chain_fees_24h'] = fees_data.get('total24h')
            
        revenue_data = self.fetch_json(f'https://api.llama.fi/overview/fees/{chain_slug}?dataType=dailyRevenue')
        if revenue_data:
            metrics['chain_revenue_24h'] = revenue_data.get('total24h')
        
        app_fees_data = self.fetch_json(f'https://api.llama.fi/overview/fees/{chain_slug}?excludeTotalDataChart=true')
        if app_fees_data:
            total_fees = app_fees_data.get('total24h', 0) or 0
            if total_fees > 0:
                metrics['chain_app_fees_24h'] = total_fees
        
        app_revenue_data = self.fetch_json(f'https://api.llama.fi/overview/fees/{chain_slug}?dataType=dailyRevenue&excludeTotalDataChart=true')
        if app_revenue_data:
            total_revenue = app_revenue_data.get('total24h', 0) or 0
            if total_revenue > 0:
                metrics['chain_app_revenue_24h'] = total_revenue
        
        dexs_data = self.fetch_json(f'https://api.llama.fi/overview/dexs/{chain_slug}')
        if dexs_data:
            metrics['chain_dex_volume_24h'] = dexs_data.get('total24h')
        
        deriv_data = self.fetch_json(f'https://api.llama.fi/overview/derivatives/{chain_slug}')
        if deriv_data:
            metrics['chain_perps_volume_24h'] = deriv_data.get('total24h')
        
        options_data = self.fetch_json(f'https://api.llama.fi/overview/options/{chain_slug}')
        if options_data:
            metrics['chain_options_volume_24h'] = options_data.get('total24h')
        
        return metrics
    
    def fetch_protocol_earnings(self, protocol_slug: str) -> dict:
        metrics = {}
        
        fees_data = self.fetch_json(f'https://api.llama.fi/summary/fees/{protocol_slug}')
        if fees_data:
            total_fees = fees_data.get('total24h', 0) or 0
            if total_fees > 0:
                metrics['fees_total24h'] = total_fees
            
        revenue_data = self.fetch_json(f'https://api.llama.fi/summary/fees/{protocol_slug}?dataType=dailyRevenue')
        if revenue_data:
            daily_revenue = revenue_data.get('total24h', 0) or 0
            if daily_revenue > 0:
                metrics['protocol_earnings'] = daily_revenue
        
        return metrics
    
    def build_lookup(self, data: list, key_fields: list) -> dict:
        lookup = {}
        for record in data:
            for field in key_fields:
                key = str(record.get(field, '')).lower().strip()
                if key:
                    lookup[key] = record
        return lookup
    
    def _get_chain_slug(self, chain_name: str, gecko_id: str, slug: str) -> str:
        if slug:
            return slug.lower()
        if chain_name:
            return chain_name.lower().replace(' ', '-')
        return gecko_id.lower()
    
    def load_config(self) -> list:
        entities = []
        with open(self.config_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('gecko_id'):
                    entities.append(row)
        return entities
    
    def pull(self) -> int:
        try:
            config_entities = self.load_config()
        except FileNotFoundError:
            print(f"Config file not found: {self.config_path}")
            self.log_pull("error", 0)
            return 0
        
        print("=" * 60)
        print("DEFILLAMA DATA PULL")
        print("=" * 60)
        print(f"Entities to process: {len(config_entities)}")
        print("=" * 60)
        
        print("Fetching DefiLlama bulk data (parallel)...")
        start_bulk = time.time()
        
        bulk_urls = {
            'protocols': 'https://api.llama.fi/protocols',
            'chains': 'https://api.llama.fi/v2/chains',
            'fees': 'https://api.llama.fi/overview/fees',
            'revenue': 'https://api.llama.fi/overview/fees?dataType=dailyRevenue',
            'dexs': 'https://api.llama.fi/overview/dexs',
            'derivatives': 'https://api.llama.fi/overview/derivatives',
            'options': 'https://api.llama.fi/overview/options',
            'aggregators': 'https://api.llama.fi/overview/aggregators',
            'bridges': 'https://bridges.llama.fi/bridges',
            'stablecoins': 'https://stablecoins.llama.fi/stablecoins',
            'chains_list': 'https://api.llama.fi/chains',
        }
        
        url_results = self.fetch_urls_parallel(list(bulk_urls.values()))
        
        protocols = url_results.get(bulk_urls['protocols']) or []
        chains = url_results.get(bulk_urls['chains']) or []
        
        fees_resp = url_results.get(bulk_urls['fees']) or {}
        fees = fees_resp.get('protocols', []) if isinstance(fees_resp, dict) else []
        
        revenue_resp = url_results.get(bulk_urls['revenue']) or {}
        revenue = revenue_resp.get('protocols', []) if isinstance(revenue_resp, dict) else []
        
        dexs_resp = url_results.get(bulk_urls['dexs']) or {}
        dexs = dexs_resp.get('protocols', []) if isinstance(dexs_resp, dict) else []
        
        deriv_resp = url_results.get(bulk_urls['derivatives']) or {}
        derivatives = deriv_resp.get('protocols', []) if isinstance(deriv_resp, dict) else []
        
        opts_resp = url_results.get(bulk_urls['options']) or {}
        options = opts_resp.get('protocols', []) if isinstance(opts_resp, dict) else []
        
        agg_resp = url_results.get(bulk_urls['aggregators']) or {}
        aggregators = agg_resp.get('protocols', []) if isinstance(agg_resp, dict) else []
        
        bridges_resp = url_results.get(bulk_urls['bridges']) or {}
        bridges = bridges_resp.get('bridges', []) if isinstance(bridges_resp, dict) else []
        
        stables_resp = url_results.get(bulk_urls['stablecoins']) or {}
        stablecoins = stables_resp.get('peggedAssets', []) if isinstance(stables_resp, dict) else []
        
        chains_list_resp = url_results.get(bulk_urls['chains_list']) or []
        
        print(f"  Bulk fetch completed in {time.time() - start_bulk:.1f}s")
        
        print("  Fetching open interest (Pro API)...")
        open_interest_data = self.fetch_all_open_interest()
        
        protocols_lookup = self.build_lookup(protocols, ['gecko_id', 'slug', 'name'])
        chains_lookup = self.build_lookup(chains, ['gecko_id', 'name'])
        fees_lookup = self.build_lookup(fees, ['slug', 'name', 'displayName'])
        revenue_lookup = self.build_lookup(revenue, ['slug', 'name', 'displayName'])
        dexs_lookup = self.build_lookup(dexs, ['slug', 'name', 'displayName'])
        derivatives_lookup = self.build_lookup(derivatives, ['slug', 'name', 'displayName'])
        options_lookup = self.build_lookup(options, ['slug', 'name', 'displayName'])
        aggregators_lookup = self.build_lookup(aggregators, ['slug', 'name', 'displayName'])
        bridges_lookup = self.build_lookup(bridges, ['name', 'displayName'])
        stablecoins_lookup = self.build_lookup(stablecoins, ['gecko_id', 'name', 'symbol'])
        
        print("  Building official chains set...")
        official_chains = set()
        for c in chains_list_resp:
            if c.get('name'):
                official_chains.add(c['name'].lower())
            if c.get('gecko_id'):
                official_chains.add(c['gecko_id'].lower())
        official_chains.update(EXTRA_CHAINS)
        print(f"    Found {len(official_chains)} official chains")
        
        print(f"\nProcessing {len(config_entities)} entities...")
        start_entity = time.time()
        
        entity_url_requests = []
        entity_metadata = []
        
        for entity in config_entities:
            gecko_id = entity.get('gecko_id', '').lower()
            slug = entity.get('slug', '').lower()
            name = entity.get('name', '').lower()
            
            if not gecko_id:
                continue
            
            is_chain = (name in official_chains or gecko_id in official_chains or 
                       slug in official_chains or name in EXTRA_CHAINS or gecko_id in EXTRA_CHAINS)
            
            p = protocols_lookup.get(gecko_id) or protocols_lookup.get(slug) or protocols_lookup.get(name)
            protocol_slug = p.get('slug', '') if p else ''
            
            chain_slug = self._get_chain_slug(entity.get('name', ''), gecko_id, slug)
            
            urls_for_entity = {}
            
            if is_chain and chain_slug:
                urls_for_entity['chain_fees'] = f'https://api.llama.fi/overview/fees/{chain_slug}'
                urls_for_entity['chain_revenue'] = f'https://api.llama.fi/overview/fees/{chain_slug}?dataType=dailyRevenue'
                urls_for_entity['chain_dex'] = f'https://api.llama.fi/overview/dexs/{chain_slug}'
                urls_for_entity['chain_perps'] = f'https://api.llama.fi/overview/derivatives/{chain_slug}'
                urls_for_entity['chain_options'] = f'https://api.llama.fi/overview/options/{chain_slug}'
            elif protocol_slug:
                urls_for_entity['protocol_fees'] = f'https://api.llama.fi/summary/fees/{protocol_slug}'
                urls_for_entity['protocol_revenue'] = f'https://api.llama.fi/summary/fees/{protocol_slug}?dataType=dailyRevenue'
            
            if gecko_id in STABLECOIN_IDS:
                stablecoin_api_id = STABLECOIN_IDS[gecko_id]
                urls_for_entity['stablecoin'] = f'https://stablecoins.llama.fi/stablecoin/{stablecoin_api_id}'
            
            if gecko_id in BRIDGE_GECKO_IDS:
                br_data = bridges_lookup.get(name)
                if br_data and br_data.get('id'):
                    urls_for_entity['bridge'] = f'https://bridges.llama.fi/bridge/{br_data["id"]}'
            
            for url_key, url in urls_for_entity.items():
                entity_url_requests.append(url)
                entity_metadata.append({
                    'gecko_id': gecko_id,
                    'slug': slug,
                    'name': name,
                    'is_chain': is_chain,
                    'url_key': url_key,
                    'protocol': p,
                    'entity': entity
                })
        
        print(f"  Fetching {len(entity_url_requests)} per-entity URLs in parallel...")
        entity_results = self.fetch_urls_parallel(entity_url_requests)
        print(f"  Entity fetch completed in {time.time() - start_entity:.1f}s")
        
        entity_data = {}
        for i, url in enumerate(entity_url_requests):
            meta = entity_metadata[i]
            gecko_id = meta['gecko_id']
            url_key = meta['url_key']
            if gecko_id not in entity_data:
                entity_data[gecko_id] = {'meta': meta, 'responses': {}}
            entity_data[gecko_id]['responses'][url_key] = entity_results.get(url)
        
        all_records = []
        entities_with_data = 0
        
        for entity in config_entities:
            gecko_id = entity.get('gecko_id', '').lower()
            slug = entity.get('slug', '').lower()
            name = entity.get('name', '').lower()
            
            if not gecko_id:
                continue
            
            raw_metrics = {}
            
            is_chain = (name in official_chains or gecko_id in official_chains or 
                       slug in official_chains or name in EXTRA_CHAINS or gecko_id in EXTRA_CHAINS)
            
            p = protocols_lookup.get(gecko_id) or protocols_lookup.get(slug) or protocols_lookup.get(name)
            if p:
                raw_metrics['protocols_tvl'] = p.get('tvl')
                raw_metrics['protocols_staking'] = p.get('staking')
                raw_metrics['protocols_borrowed'] = p.get('borrowed')
                raw_metrics['protocols_pool2'] = p.get('pool2')
                raw_metrics['protocols_change_1h'] = p.get('change_1h')
                raw_metrics['protocols_change_1d'] = p.get('change_1d')
                raw_metrics['protocols_change_7d'] = p.get('change_7d')
            
            c = chains_lookup.get(gecko_id) or chains_lookup.get(name)
            if c:
                raw_metrics['chains_tvl'] = c.get('tvl')
            
            f = fees_lookup.get(slug) or fees_lookup.get(name)
            if f:
                raw_metrics['fees_total24h'] = f.get('total24h')
                raw_metrics['fees_total7d'] = f.get('total7d')
                raw_metrics['fees_total30d'] = f.get('total30d')
                raw_metrics['fees_total1y'] = f.get('total1y')
                raw_metrics['fees_totalAllTime'] = f.get('totalAllTime')
                raw_metrics['fees_average1y'] = f.get('average1y')
                raw_metrics['fees_change_1d'] = f.get('change_1d')
                raw_metrics['fees_change_7d'] = f.get('change_7d')
                raw_metrics['fees_change_1m'] = f.get('change_1m')
            
            r = revenue_lookup.get(slug) or revenue_lookup.get(name)
            if r:
                raw_metrics['revenue_total24h'] = r.get('total24h')
                raw_metrics['revenue_total7d'] = r.get('total7d')
                raw_metrics['revenue_total30d'] = r.get('total30d')
                raw_metrics['revenue_total1y'] = r.get('total1y')
                raw_metrics['revenue_totalAllTime'] = r.get('totalAllTime')
            
            d = dexs_lookup.get(slug) or dexs_lookup.get(name)
            if d:
                raw_metrics['dexs_total24h'] = d.get('total24h')
                raw_metrics['dexs_total7d'] = d.get('total7d')
                raw_metrics['dexs_total30d'] = d.get('total30d')
                raw_metrics['dexs_totalAllTime'] = d.get('totalAllTime')
                raw_metrics['dexs_change_1d'] = d.get('change_1d')
                raw_metrics['dexs_change_7d'] = d.get('change_7d')
                raw_metrics['dexs_change_1m'] = d.get('change_1m')
            
            deriv = derivatives_lookup.get(slug) or derivatives_lookup.get(name)
            if deriv:
                raw_metrics['derivatives_total24h'] = deriv.get('total24h')
                raw_metrics['derivatives_total7d'] = deriv.get('total7d')
                raw_metrics['derivatives_total30d'] = deriv.get('total30d')
                raw_metrics['derivatives_change_1d'] = deriv.get('change_1d')
                raw_metrics['derivatives_change_7d'] = deriv.get('change_7d')
            
            opt = options_lookup.get(slug) or options_lookup.get(name)
            if opt:
                raw_metrics['options_total24h'] = opt.get('total24h')
                raw_metrics['options_total7d'] = opt.get('total7d')
                raw_metrics['options_total30d'] = opt.get('total30d')
                raw_metrics['options_dailyNotionalVolume'] = opt.get('dailyNotionalVolume')
                raw_metrics['options_dailyPremiumVolume'] = opt.get('dailyPremiumVolume')
            
            agg = aggregators_lookup.get(slug) or aggregators_lookup.get(name)
            if agg:
                raw_metrics['aggregators_total24h'] = agg.get('total24h')
                raw_metrics['aggregators_total7d'] = agg.get('total7d')
                raw_metrics['aggregators_total30d'] = agg.get('total30d')
            
            br = bridges_lookup.get(name)
            if br:
                raw_metrics['bridges_lastDailyVolume'] = br.get('lastDailyVolume')
                raw_metrics['bridges_weeklyVolume'] = br.get('weeklyVolume')
                raw_metrics['bridges_monthlyVolume'] = br.get('monthlyVolume')
            
            st = stablecoins_lookup.get(gecko_id) or stablecoins_lookup.get(name)
            if st:
                circ = st.get('circulating', {})
                raw_metrics['stablecoins_circulating_peggedUSD'] = circ.get('peggedUSD') if isinstance(circ, dict) else circ
                circ_prev = st.get('circulatingPrevDay', {})
                raw_metrics['stablecoins_circulatingPrevDay_peggedUSD'] = circ_prev.get('peggedUSD') if isinstance(circ_prev, dict) else None
                circ_week = st.get('circulatingPrevWeek', {})
                raw_metrics['stablecoins_circulatingPrevWeek_peggedUSD'] = circ_week.get('peggedUSD') if isinstance(circ_week, dict) else None
                raw_metrics['stablecoins_price'] = st.get('price')
            
            ed = entity_data.get(gecko_id, {}).get('responses', {})
            
            if is_chain:
                chain_fees_data = ed.get('chain_fees')
                if chain_fees_data:
                    raw_metrics['chain_fees_24h'] = chain_fees_data.get('total24h')
                    raw_metrics['chain_app_fees_24h'] = chain_fees_data.get('totalDataChartBreakdown', [{}])[-1].get('Fees') if chain_fees_data.get('totalDataChartBreakdown') else None
                
                chain_rev_data = ed.get('chain_revenue')
                if chain_rev_data:
                    raw_metrics['chain_revenue_24h'] = chain_rev_data.get('total24h')
                
                chain_dex_data = ed.get('chain_dex')
                if chain_dex_data:
                    raw_metrics['chain_dex_volume_24h'] = chain_dex_data.get('total24h')
                
                chain_perps_data = ed.get('chain_perps')
                if chain_perps_data:
                    raw_metrics['chain_perps_volume_24h'] = chain_perps_data.get('total24h')
                
                chain_opts_data = ed.get('chain_options')
                if chain_opts_data:
                    raw_metrics['chain_options_volume_24h'] = chain_opts_data.get('total24h')
            else:
                proto_fees_data = ed.get('protocol_fees')
                if proto_fees_data:
                    total_fees = proto_fees_data.get('total24h', 0) or 0
                    if total_fees > 0:
                        raw_metrics['fees_total24h'] = total_fees
                
                proto_rev_data = ed.get('protocol_revenue')
                if proto_rev_data:
                    daily_revenue = proto_rev_data.get('total24h', 0) or 0
                    if daily_revenue > 0:
                        raw_metrics['protocol_earnings'] = daily_revenue
            
            stable_data = ed.get('stablecoin')
            if stable_data:
                circulating = stable_data.get('currentChainBalances', {})
                total_circ = sum(circulating.values()) if isinstance(circulating, dict) else 0
                if total_circ > 0:
                    raw_metrics['stablecoins_circulating_peggedUSD'] = total_circ
            
            bridge_data = ed.get('bridge')
            if bridge_data:
                raw_metrics['bridges_lastDailyVolume'] = bridge_data.get('lastDailyVolume')
                raw_metrics['bridges_weeklyVolume'] = bridge_data.get('weeklyVolume')
                raw_metrics['bridges_monthlyVolume'] = bridge_data.get('monthlyVolume')
            
            entity_records = 0
            for raw_field, value in raw_metrics.items():
                if value is not None and value != '' and value != 0:
                    try:
                        numeric_value = float(value)
                        metric_name = METRIC_MAP.get(raw_field, raw_field.upper())
                        all_records.append({
                            "asset": gecko_id,
                            "metric_name": metric_name,
                            "value": numeric_value
                        })
                        entity_records += 1
                    except (ValueError, TypeError):
                        continue
            
            if entity_records > 0:
                entities_with_data += 1
        
        if open_interest_data:
            print(f"\n  Processing {len(open_interest_data)} open interest markets...")
            for key, oi_data in open_interest_data.items():
                oi_value = oi_data.get('open_interest')
                if oi_value is not None and oi_value > 0:
                    all_records.append({
                        "asset": key,
                        "metric_name": "OPEN_INTEREST",
                        "value": float(oi_value)
                    })
        
        total_records = 0
        if all_records:
            total_records = self.insert_metrics(all_records)
        
        status = "success" if total_records > 0 else "no_data"
        self.log_pull(status, total_records)
        
        print("\n" + "=" * 60)
        print("COMPLETE")
        print(f"  Entities with data: {entities_with_data}/{len(config_entities)}")
        print(f"  Open interest markets: {len(open_interest_data)}")
        print(f"  Records inserted: {total_records}")
        print("=" * 60)
        
        return total_records
