import os
import csv
import time
import requests
from typing import Optional
from datetime import datetime
from sources.base import BaseSource

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
}

REQUEST_DELAY = 1.0

class DefiLlamaSource(BaseSource):
    
    @property
    def source_name(self) -> str:
        return "defillama"
    
    def __init__(self):
        self.config_path = "defillama_config.csv"
        self.api_key = os.environ.get("DEFILLAMA_API_KEY")
        self.base_url = "https://pro-api.llama.fi" if self.api_key else "https://api.llama.fi"
    
    def fetch_json(self, url: str) -> Optional[dict]:
        try:
            resp = requests.get(url, timeout=30)
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
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return None
    
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
    
    def get_official_chains(self) -> set:
        chains_data = self.fetch_json('https://api.llama.fi/chains')
        if chains_data:
            chain_names = set()
            for c in chains_data:
                if c.get('name'):
                    chain_names.add(c['name'].lower())
                if c.get('gecko_id'):
                    chain_names.add(c['gecko_id'].lower())
            return chain_names
        return set()
    
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
        
        incentives_data = self.fetch_json(f'https://api.llama.fi/overview/fees/{chain_slug}?dataType=dailyTokenIncentives')
        if incentives_data:
            token_incentives = incentives_data.get('total24h', 0) or 0
            if token_incentives > 0:
                metrics['chain_token_incentives_24h'] = token_incentives
        
        dexs_data = self.fetch_json(f'https://api.llama.fi/overview/dexs/{chain_slug}')
        if dexs_data:
            metrics['chain_dex_volume_24h'] = dexs_data.get('total24h')
        
        deriv_data = self.fetch_json(f'https://api.llama.fi/overview/derivatives/{chain_slug}')
        if deriv_data:
            metrics['chain_perps_volume_24h'] = deriv_data.get('total24h')
        
        return metrics
    
    def fetch_protocol_earnings(self, protocol_slug: str) -> dict:
        metrics = {}
        
        fees_data = self.fetch_json(f'https://api.llama.fi/summary/fees/{protocol_slug}')
        if fees_data:
            total_fees = fees_data.get('total24h', 0) or 0
            
        revenue_data = self.fetch_json(f'https://api.llama.fi/summary/fees/{protocol_slug}?dataType=dailyRevenue')
        if revenue_data:
            daily_revenue = revenue_data.get('total24h', 0) or 0
            if daily_revenue > 0:
                metrics['protocol_earnings'] = daily_revenue
        
        incentives_data = self.fetch_json(f'https://api.llama.fi/summary/fees/{protocol_slug}?dataType=dailyTokenIncentives')
        if incentives_data:
            token_incentives = incentives_data.get('total24h', 0) or 0
            if token_incentives > 0:
                metrics['protocol_incentives'] = token_incentives
        
        return metrics
    
    def build_lookup(self, data: list, key_fields: list) -> dict:
        lookup = {}
        for record in data:
            for field in key_fields:
                key = str(record.get(field, '')).lower().strip()
                if key:
                    lookup[key] = record
        return lookup
    
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
        
        print("Fetching DefiLlama data...")
        
        print("  Fetching protocols...")
        protocols = self.fetch_json('https://api.llama.fi/protocols') or []
        time.sleep(REQUEST_DELAY)
        
        print("  Fetching chains...")
        chains = self.fetch_json('https://api.llama.fi/v2/chains') or []
        time.sleep(REQUEST_DELAY)
        
        print("  Fetching fees...")
        fees_resp = self.fetch_json('https://api.llama.fi/overview/fees') or {}
        fees = fees_resp.get('protocols', []) if isinstance(fees_resp, dict) else []
        time.sleep(REQUEST_DELAY)
        
        print("  Fetching revenue...")
        revenue_resp = self.fetch_json('https://api.llama.fi/overview/fees?dataType=dailyRevenue') or {}
        revenue = revenue_resp.get('protocols', []) if isinstance(revenue_resp, dict) else []
        time.sleep(REQUEST_DELAY)
        
        print("  Fetching dexs...")
        dexs_resp = self.fetch_json('https://api.llama.fi/overview/dexs') or {}
        dexs = dexs_resp.get('protocols', []) if isinstance(dexs_resp, dict) else []
        time.sleep(REQUEST_DELAY)
        
        print("  Fetching derivatives...")
        deriv_resp = self.fetch_json('https://api.llama.fi/overview/derivatives') or {}
        derivatives = deriv_resp.get('protocols', []) if isinstance(deriv_resp, dict) else []
        time.sleep(REQUEST_DELAY)
        
        print("  Fetching options...")
        opts_resp = self.fetch_json('https://api.llama.fi/overview/options') or {}
        options = opts_resp.get('protocols', []) if isinstance(opts_resp, dict) else []
        time.sleep(REQUEST_DELAY)
        
        print("  Fetching aggregators...")
        agg_resp = self.fetch_json('https://api.llama.fi/overview/aggregators') or {}
        aggregators = agg_resp.get('protocols', []) if isinstance(agg_resp, dict) else []
        time.sleep(REQUEST_DELAY)
        
        print("  Fetching bridges...")
        bridges_resp = self.fetch_json('https://bridges.llama.fi/bridges') or {}
        bridges = bridges_resp.get('bridges', []) if isinstance(bridges_resp, dict) else []
        time.sleep(REQUEST_DELAY)
        
        print("  Fetching stablecoins...")
        stables_resp = self.fetch_json('https://stablecoins.llama.fi/stablecoins') or {}
        stablecoins = stables_resp.get('peggedAssets', []) if isinstance(stables_resp, dict) else []
        
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
        official_chains = self.get_official_chains()
        print(f"    Found {len(official_chains)} official chains")
        
        print(f"\nProcessing {len(config_entities)} entities...")
        
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
                       slug in official_chains)
            
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
            
            if self.api_key and p:
                protocol_slug = p.get('slug', '')
                if protocol_slug:
                    inflow_data = self.fetch_inflows(protocol_slug)
                    raw_metrics['inflows'] = inflow_data.get('inflows')
                    raw_metrics['outflows'] = inflow_data.get('outflows')
            
            if is_chain:
                chain_metrics = self.fetch_chain_metrics(entity.get('name', ''), gecko_id, slug)
                raw_metrics.update(chain_metrics)
                time.sleep(0.5)
            else:
                if p:
                    protocol_slug = p.get('slug', '')
                    if protocol_slug:
                        earnings_metrics = self.fetch_protocol_earnings(protocol_slug)
                        raw_metrics.update(earnings_metrics)
                        time.sleep(0.3)
            
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
        
        total_records = 0
        if all_records:
            total_records = self.insert_metrics(all_records)
        
        status = "success" if total_records > 0 else "no_data"
        self.log_pull(status, total_records)
        
        print("\n" + "=" * 60)
        print("COMPLETE")
        print(f"  Entities with data: {entities_with_data}/{len(config_entities)}")
        print(f"  Records inserted: {total_records}")
        print("=" * 60)
        
        return total_records
