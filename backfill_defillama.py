import os
import csv
import time
import argparse
import threading
import requests
from requests.adapters import HTTPAdapter
import psycopg2
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_WORKERS = 10  # Concurrent requests (1000/min limit = 16.6/sec)
BATCH_SIZE = 1000
MAX_RETRIES = 3
RETRY_DELAY = 2

API_KEY = os.environ.get('DEFILLAMA_API_KEY')
PRO_BASE_URL = "https://pro-api.llama.fi"

_thread_local = threading.local()

def get_thread_session():
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5)
        _thread_local.session.mount('https://', adapter)
        _thread_local.session.mount('http://', adapter)
    return _thread_local.session

main_session = requests.Session()
main_adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
main_session.mount('https://', main_adapter)
main_session.mount('http://', main_adapter)

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
    'wormhole': 1,
    'octus-bridge': 2,
    'connext': 3,
    'debridge': 4,
    'across-protocol': 5,
    'celer-network': 6,
    'hop-protocol': 7,
    'multichain': 8,
    'layerzero': 9,
    'synapse-2': 10,
}

ENDPOINTS = {
    'fees': {
        'url': 'https://api.llama.fi/summary/fees/{slug}',
        'chart_key': 'totalDataChart',
        'metric': 'FEES'
    },
    'revenue': {
        'url': 'https://api.llama.fi/summary/fees/{slug}?dataType=dailyRevenue',
        'chart_key': 'totalDataChart',
        'metric': 'REVENUE'
    },
    'dexs': {
        'url': 'https://api.llama.fi/summary/dexs/{slug}',
        'chart_key': 'totalDataChart',
        'metric': 'DEX_VOLUME'
    },
    'derivatives': {
        'url': 'https://api.llama.fi/summary/derivatives/{slug}',
        'chart_key': 'totalDataChart',
        'metric': 'DERIVATIVES_VOLUME'
    },
    'aggregators': {
        'url': 'https://api.llama.fi/summary/aggregators/{slug}',
        'chart_key': 'totalDataChart',
        'metric': 'AGGREGATOR_VOLUME'
    },
    'tvl': {
        'url': 'https://api.llama.fi/protocol/{slug}',
        'chart_key': 'tvl',
        'metric': 'TVL'
    }
}

def get_db_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])

RATE_LIMIT_PER_SEC = 15  # Stay under 1000/min = 16.6/sec

# Global rate limiter using a simple token bucket
import threading
_rate_lock = threading.Lock()
_last_request_time = [0.0]  # Mutable to allow modification in nested function
_min_interval = 1.0 / RATE_LIMIT_PER_SEC

def _wait_for_rate_limit():
    """Wait if needed to respect rate limit"""
    with _rate_lock:
        now = time.time()
        elapsed = now - _last_request_time[0]
        if elapsed < _min_interval:
            time.sleep(_min_interval - elapsed)
        _last_request_time[0] = time.time()

def fetch_json(url):
    """Rate-limited fetch with retries"""
    for attempt in range(MAX_RETRIES):
        _wait_for_rate_limit()
        try:
            resp = main_session.get(url, timeout=60)
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
    return None

def fetch_json_threadsafe(url):
    """Thread-safe, rate-limited fetch with retries"""
    for attempt in range(MAX_RETRIES):
        _wait_for_rate_limit()
        try:
            session = get_thread_session()
            resp = session.get(url, timeout=60)
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
    return None

def fetch_pro_json(endpoint):
    """Rate-limited Pro API fetch"""
    _wait_for_rate_limit()
    if not API_KEY:
        return None
    url = f"{PRO_BASE_URL}/{API_KEY}{endpoint}"
    try:
        resp = main_session.get(url, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception as e:
        return None

def fetch_urls_parallel(urls):
    results = {}
    total = len(urls)
    
    # Use parallel fetch but each worker respects global rate limit
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(fetch_json_threadsafe, url): url for url in urls}
        done = 0
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                results[url] = future.result()
            except Exception:
                results[url] = None
            done += 1
            if done % 100 == 0:
                print(f"    Fetched {done}/{total} URLs...")
    
    return results

def fetch_stablecoin_historical(stablecoin_api_id, gecko_id):
    records = []
    url = f"https://stablecoins.llama.fi/stablecoincharts/all?stablecoin={stablecoin_api_id}"
    data = fetch_json(url)
    
    if data and isinstance(data, list):
        for entry in data:
            try:
                ts = entry.get('date')
                total = entry.get('totalCirculating', {})
                value = total.get('peggedUSD')
                
                if ts and value is not None and value > 0:
                    dt = datetime.utcfromtimestamp(ts)
                    records.append({
                        'asset': gecko_id,
                        'metric_name': 'STABLECOIN_CIRCULATING',
                        'value': float(value),
                        'pulled_at': dt
                    })
            except (ValueError, TypeError, KeyError):
                continue
    
    return records


def fetch_bridge_historical(bridge_name, gecko_id):
    records = []
    
    bridges_data = fetch_json('https://bridges.llama.fi/bridges')
    if not bridges_data:
        return records
    
    bridges = bridges_data.get('bridges', [])
    bridge_id = None
    for b in bridges:
        if (b.get('name', '').lower() == bridge_name.lower() or 
            b.get('displayName', '').lower() == bridge_name.lower()):
            bridge_id = b.get('id')
            break
    
    if not bridge_id:
        return records
    
    url = f"https://bridges.llama.fi/bridgevolume/{bridge_id}"
    data = fetch_json(url)
    
    if data and isinstance(data, list):
        for entry in data:
            try:
                ts = entry.get('date')
                
                deposit_usd = entry.get('depositUSD', 0) or 0
                withdraw_usd = entry.get('withdrawUSD', 0) or 0
                total_volume = deposit_usd + withdraw_usd
                
                if ts and total_volume > 0:
                    dt = datetime.utcfromtimestamp(ts)
                    records.append({
                        'asset': gecko_id,
                        'metric_name': 'BRIDGE_VOLUME',
                        'value': float(total_volume),
                        'pulled_at': dt
                    })
            except (ValueError, TypeError, KeyError):
                continue
    
    return records


def fetch_historical_inflows(slug, gecko_id, days=30):
    records = []
    if not API_KEY:
        return records
    
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)
    current = start_date
    
    while current < end_date:
        ts = int(current.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        data = fetch_pro_json(f"/api/inflows/{slug}/{ts}")
        
        if data:
            inflows = data.get('inflows')
            outflows = data.get('outflows')
            dt = current.replace(hour=0, minute=0, second=0, microsecond=0)
            
            if inflows is not None and inflows != 0:
                records.append({
                    'asset': gecko_id,
                    'metric_name': 'INFLOW',
                    'value': float(inflows),
                    'pulled_at': dt
                })
            if outflows is not None and outflows != 0:
                records.append({
                    'asset': gecko_id,
                    'metric_name': 'OUTFLOW',
                    'value': float(outflows),
                    'pulled_at': dt
                })
        
        current += timedelta(days=1)
    
    return records


def get_chain_slug(chain_name, slug='', gecko_id=''):
    if slug:
        return slug.lower()
    if chain_name:
        return chain_name.lower().replace(' ', '-')
    return gecko_id.lower()

def fetch_chain_historical_tvl(chain_name, gecko_id, slug=''):
    records = []
    chain_slug = get_chain_slug(chain_name, slug, gecko_id)
    if not chain_slug:
        return records
    url = f"https://api.llama.fi/v2/historicalChainTvl/{chain_slug}"
    data = fetch_json(url)
    
    if data and isinstance(data, list):
        for entry in data:
            try:
                ts = entry.get('date')
                tvl = entry.get('tvl')
                if ts and tvl is not None and tvl != 0:
                    dt = datetime.utcfromtimestamp(ts)
                    records.append({
                        'asset': gecko_id,
                        'metric_name': 'CHAIN_TVL',
                        'value': float(tvl),
                        'pulled_at': dt
                    })
            except (ValueError, TypeError):
                continue
    
    return records

def fetch_chain_fees(chain_name, gecko_id, slug=''):
    records = []
    chain_slug = get_chain_slug(chain_name, slug, gecko_id)
    if not chain_slug:
        return records
    url = f"https://api.llama.fi/overview/fees/{chain_slug}"
    data = fetch_json(url)
    
    if data:
        chart = data.get('totalDataChart', [])
        for entry in chart:
            try:
                if isinstance(entry, list) and len(entry) >= 2:
                    ts, value = entry[0], entry[1]
                elif isinstance(entry, dict):
                    ts = entry.get('date') or entry.get('timestamp')
                    value = entry.get('value') or entry.get('fees')
                else:
                    continue
                
                if ts and value is not None and value != 0:
                    dt = datetime.utcfromtimestamp(ts)
                    records.append({
                        'asset': gecko_id,
                        'metric_name': 'CHAIN_FEES',
                        'value': float(value),
                        'pulled_at': dt
                    })
            except (ValueError, TypeError):
                continue
    
    return records

def fetch_chain_dex_volume(chain_name, gecko_id, slug=''):
    records = []
    chain_slug = get_chain_slug(chain_name, slug, gecko_id)
    if not chain_slug:
        return records
    url = f"https://api.llama.fi/overview/dexs/{chain_slug}"
    data = fetch_json(url)
    
    if data:
        chart = data.get('totalDataChart', [])
        for entry in chart:
            try:
                if isinstance(entry, list) and len(entry) >= 2:
                    ts, value = entry[0], entry[1]
                elif isinstance(entry, dict):
                    ts = entry.get('date') or entry.get('timestamp')
                    value = entry.get('value') or entry.get('volume')
                else:
                    continue
                
                if ts and value is not None and value != 0:
                    dt = datetime.utcfromtimestamp(ts)
                    records.append({
                        'asset': gecko_id,
                        'metric_name': 'CHAIN_DEX_VOLUME',
                        'value': float(value),
                        'pulled_at': dt
                    })
            except (ValueError, TypeError):
                continue
    
    return records

def fetch_chain_derivatives(chain_name, gecko_id, slug=''):
    records = []
    chain_slug = get_chain_slug(chain_name, slug, gecko_id)
    if not chain_slug:
        return records
    url = f"https://api.llama.fi/overview/derivatives/{chain_slug}"
    data = fetch_json(url)
    
    if data:
        chart = data.get('totalDataChart', [])
        for entry in chart:
            try:
                if isinstance(entry, list) and len(entry) >= 2:
                    ts, value = entry[0], entry[1]
                else:
                    continue
                
                if ts and value is not None and value != 0:
                    dt = datetime.utcfromtimestamp(ts)
                    records.append({
                        'asset': gecko_id,
                        'metric_name': 'CHAIN_PERPS_VOLUME',
                        'value': float(value),
                        'pulled_at': dt
                    })
            except (ValueError, TypeError):
                continue
    
    return records

def fetch_chain_options(chain_name, gecko_id, slug=''):
    records = []
    chain_slug = get_chain_slug(chain_name, slug, gecko_id)
    if not chain_slug:
        return records
    url = f"https://api.llama.fi/overview/options/{chain_slug}"
    data = fetch_json(url)
    
    if data:
        chart = data.get('totalDataChart', [])
        for entry in chart:
            try:
                if isinstance(entry, list) and len(entry) >= 2:
                    ts, value = entry[0], entry[1]
                else:
                    continue
                
                if ts and value is not None and value != 0:
                    dt = datetime.utcfromtimestamp(ts)
                    records.append({
                        'asset': gecko_id,
                        'metric_name': 'CHAIN_OPTIONS_VOLUME',
                        'value': float(value),
                        'pulled_at': dt
                    })
            except (ValueError, TypeError):
                continue
    
    return records

def fetch_chain_revenue(chain_name, gecko_id, slug=''):
    records = []
    chain_slug = get_chain_slug(chain_name, slug, gecko_id)
    if not chain_slug:
        return records
    url = f"https://api.llama.fi/overview/fees/{chain_slug}?dataType=dailyRevenue"
    data = fetch_json(url)
    
    if data:
        chart = data.get('totalDataChart', [])
        for entry in chart:
            try:
                if isinstance(entry, list) and len(entry) >= 2:
                    ts, value = entry[0], entry[1]
                else:
                    continue
                
                if ts and value is not None and value != 0:
                    dt = datetime.utcfromtimestamp(ts)
                    records.append({
                        'asset': gecko_id,
                        'metric_name': 'CHAIN_REVENUE',
                        'value': float(value),
                        'pulled_at': dt
                    })
            except (ValueError, TypeError):
                continue
    
    return records


def extract_historical_data(data, chart_key, metric_name, asset_id, start_ts=None, end_ts=None):
    records = []
    
    if not data:
        return records
    
    chart_data = data.get(chart_key, [])
    
    if not chart_data:
        return records
    
    for entry in chart_data:
        try:
            if isinstance(entry, list) and len(entry) >= 2:
                timestamp = entry[0]
                value = entry[1]
            elif isinstance(entry, dict):
                timestamp = entry.get('date')
                value = entry.get('totalLiquidityUSD') or entry.get('tvl')
            else:
                continue
            
            if value is None or value == 0:
                continue
            
            # Apply date filter
            if start_ts is not None and timestamp < start_ts:
                continue
            if end_ts is not None and timestamp > end_ts:
                continue
            
            dt = datetime.utcfromtimestamp(timestamp)
            
            records.append({
                'asset': asset_id,
                'metric_name': metric_name,
                'value': float(value),
                'pulled_at': dt
            })
        except (ValueError, TypeError, KeyError):
            continue
    
    return records

def insert_records_batch(conn, records):
    if not records:
        return 0
    
    cur = conn.cursor()
    inserted = 0
    
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        values = []
        for r in batch:
            metric_date = r['pulled_at'].date() if hasattr(r['pulled_at'], 'date') else r['pulled_at']
            values.append((
                r['pulled_at'],
                'defillama',
                r['asset'],
                r['metric_name'],
                r['value'],
                metric_date
            ))
        
        cur.executemany("""
            INSERT INTO metrics (pulled_at, source, asset, metric_name, value, metric_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, asset, metric_name, pulled_at, COALESCE(exchange, '')) DO NOTHING
        """, values)
        inserted += len(batch)
    
    conn.commit()
    cur.close()
    return inserted

def get_official_chains():
    data = fetch_json('https://api.llama.fi/chains')
    if data:
        chain_names = set()
        for c in data:
            if c.get('name'):
                chain_names.add(c['name'].lower())
            if c.get('gecko_id'):
                chain_names.add(c['gecko_id'].lower())
        return chain_names
    return set()

def load_config():
    official_chains = get_official_chains()
    print(f"Found {len(official_chains)} official chains from DefiLlama API")
    
    entities = []
    with open('defillama_config.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('gecko_id'):
                gecko_id = row.get('gecko_id', '').lower()
                slug = row.get('slug', '').lower()
                name = row.get('name', '').lower()
                
                is_chain = (name in official_chains or gecko_id in official_chains or 
                           slug in official_chains)
                
                entities.append({
                    'gecko_id': gecko_id,
                    'slug': slug,
                    'name': row.get('name', ''),
                    'category': 'Chain' if is_chain else row.get('category', ''),
                    'is_chain': is_chain
                })
    return entities


def build_entity_urls(entity):
    gecko_id = entity['gecko_id']
    slug = entity['slug'] or gecko_id
    name = entity.get('name', slug)
    is_chain = entity.get('is_chain', False)
    
    urls = {}
    chain_slug = get_chain_slug(name, slug, gecko_id)
    
    if is_chain:
        urls['chain_tvl'] = f'https://api.llama.fi/v2/historicalChainTvl/{chain_slug}'
        urls['chain_fees'] = f'https://api.llama.fi/overview/fees/{chain_slug}'
        urls['chain_revenue'] = f'https://api.llama.fi/overview/fees/{chain_slug}?dataType=dailyRevenue'
        urls['chain_dex'] = f'https://api.llama.fi/overview/dexs/{chain_slug}'
        urls['chain_perps'] = f'https://api.llama.fi/overview/derivatives/{chain_slug}'
        urls['chain_options'] = f'https://api.llama.fi/overview/options/{chain_slug}'
    else:
        for endpoint_name, config in ENDPOINTS.items():
            urls[endpoint_name] = config['url'].format(slug=slug)
    
    if gecko_id in STABLECOIN_IDS:
        stablecoin_api_id = STABLECOIN_IDS[gecko_id]
        urls['stablecoin'] = f'https://stablecoins.llama.fi/stablecoincharts/all?stablecoin={stablecoin_api_id}'
    
    if gecko_id in BRIDGE_GECKO_IDS:
        urls['bridges_list'] = 'https://bridges.llama.fi/bridges'
    
    return urls


def process_entity_data(entity, url_results, conn, start_ts=None, end_ts=None):
    gecko_id = entity['gecko_id']
    slug = entity['slug'] or gecko_id
    name = entity.get('name', slug)
    is_chain = entity.get('is_chain', False)
    
    all_records = []
    metrics_found = []
    
    def is_in_range(ts):
        """Check if timestamp is within date range."""
        if start_ts is None and end_ts is None:
            return True
        if start_ts is not None and ts < start_ts:
            return False
        if end_ts is not None and ts > end_ts:
            return False
        return True
    
    if is_chain:
        chain_slug = get_chain_slug(name, slug, gecko_id)
        
        tvl_url = f'https://api.llama.fi/v2/historicalChainTvl/{chain_slug}'
        tvl_data = url_results.get(tvl_url)
        if tvl_data and isinstance(tvl_data, list):
            for entry in tvl_data:
                try:
                    ts = entry.get('date')
                    value = entry.get('tvl')
                    if ts and value is not None and value > 0 and is_in_range(ts):
                        dt = datetime.utcfromtimestamp(ts)
                        all_records.append({
                            'asset': gecko_id,
                            'metric_name': 'CHAIN_TVL',
                            'value': float(value),
                            'pulled_at': dt
                        })
                except (ValueError, TypeError):
                    continue
            if all_records:
                metrics_found.append(f"CHAIN_TVL({len(all_records)})")
        
        fees_url = f'https://api.llama.fi/overview/fees/{chain_slug}'
        fees_data = url_results.get(fees_url)
        if fees_data:
            chart = fees_data.get('totalDataChart', [])
            count = 0
            for entry in chart:
                try:
                    if isinstance(entry, list) and len(entry) >= 2:
                        ts, value = entry[0], entry[1]
                        if ts and value is not None and value != 0 and is_in_range(ts):
                            dt = datetime.utcfromtimestamp(ts)
                            all_records.append({
                                'asset': gecko_id,
                                'metric_name': 'CHAIN_FEES',
                                'value': float(value),
                                'pulled_at': dt
                            })
                            count += 1
                except (ValueError, TypeError):
                    continue
            if count > 0:
                metrics_found.append(f"CHAIN_FEES({count})")
        
        rev_url = f'https://api.llama.fi/overview/fees/{chain_slug}?dataType=dailyRevenue'
        rev_data = url_results.get(rev_url)
        if rev_data:
            chart = rev_data.get('totalDataChart', [])
            count = 0
            for entry in chart:
                try:
                    if isinstance(entry, list) and len(entry) >= 2:
                        ts, value = entry[0], entry[1]
                        if ts and value is not None and value != 0 and is_in_range(ts):
                            dt = datetime.utcfromtimestamp(ts)
                            all_records.append({
                                'asset': gecko_id,
                                'metric_name': 'CHAIN_REVENUE',
                                'value': float(value),
                                'pulled_at': dt
                            })
                            count += 1
                except (ValueError, TypeError):
                    continue
            if count > 0:
                metrics_found.append(f"CHAIN_REVENUE({count})")
        
        dex_url = f'https://api.llama.fi/overview/dexs/{chain_slug}'
        dex_data = url_results.get(dex_url)
        if dex_data:
            chart = dex_data.get('totalDataChart', [])
            count = 0
            for entry in chart:
                try:
                    if isinstance(entry, list) and len(entry) >= 2:
                        ts, value = entry[0], entry[1]
                        if ts and value is not None and value != 0 and is_in_range(ts):
                            dt = datetime.utcfromtimestamp(ts)
                            all_records.append({
                                'asset': gecko_id,
                                'metric_name': 'CHAIN_DEX_VOLUME',
                                'value': float(value),
                                'pulled_at': dt
                            })
                            count += 1
                except (ValueError, TypeError):
                    continue
            if count > 0:
                metrics_found.append(f"CHAIN_DEX({count})")
        
        perps_url = f'https://api.llama.fi/overview/derivatives/{chain_slug}'
        perps_data = url_results.get(perps_url)
        if perps_data:
            chart = perps_data.get('totalDataChart', [])
            count = 0
            for entry in chart:
                try:
                    if isinstance(entry, list) and len(entry) >= 2:
                        ts, value = entry[0], entry[1]
                        if ts and value is not None and value != 0 and is_in_range(ts):
                            dt = datetime.utcfromtimestamp(ts)
                            all_records.append({
                                'asset': gecko_id,
                                'metric_name': 'CHAIN_PERPS_VOLUME',
                                'value': float(value),
                                'pulled_at': dt
                            })
                            count += 1
                except (ValueError, TypeError):
                    continue
            if count > 0:
                metrics_found.append(f"CHAIN_PERPS({count})")
        
        options_url = f'https://api.llama.fi/overview/options/{chain_slug}'
        options_data = url_results.get(options_url)
        if options_data:
            chart = options_data.get('totalDataChart', [])
            count = 0
            for entry in chart:
                try:
                    if isinstance(entry, list) and len(entry) >= 2:
                        ts, value = entry[0], entry[1]
                        if ts and value is not None and value != 0 and is_in_range(ts):
                            dt = datetime.utcfromtimestamp(ts)
                            all_records.append({
                                'asset': gecko_id,
                                'metric_name': 'CHAIN_OPTIONS_VOLUME',
                                'value': float(value),
                                'pulled_at': dt
                            })
                            count += 1
                except (ValueError, TypeError):
                    continue
            if count > 0:
                metrics_found.append(f"CHAIN_OPTIONS({count})")
    else:
        for endpoint_name, config in ENDPOINTS.items():
            url = config['url'].format(slug=slug)
            data = url_results.get(url)
            
            if data:
                records = extract_historical_data(
                    data, 
                    config['chart_key'], 
                    config['metric'],
                    gecko_id,
                    start_ts,
                    end_ts
                )
                if records:
                    all_records.extend(records)
                    metrics_found.append(f"{config['metric']}({len(records)})")
    
    if gecko_id in STABLECOIN_IDS:
        stablecoin_api_id = STABLECOIN_IDS[gecko_id]
        stable_url = f'https://stablecoins.llama.fi/stablecoincharts/all?stablecoin={stablecoin_api_id}'
        stable_data = url_results.get(stable_url)
        if stable_data and isinstance(stable_data, list):
            count = 0
            for entry in stable_data:
                try:
                    ts = entry.get('date')
                    total = entry.get('totalCirculating', {})
                    value = total.get('peggedUSD')
                    if ts and value is not None and value > 0 and is_in_range(ts):
                        dt = datetime.utcfromtimestamp(ts)
                        all_records.append({
                            'asset': gecko_id,
                            'metric_name': 'STABLECOIN_CIRCULATING',
                            'value': float(value),
                            'pulled_at': dt
                        })
                        count += 1
                except (ValueError, TypeError, KeyError):
                    continue
            if count > 0:
                metrics_found.append(f"STABLECOIN_CIRC({count})")
    
    if gecko_id in BRIDGE_GECKO_IDS:
        bridges_list_url = 'https://bridges.llama.fi/bridges'
        bridges_data = url_results.get(bridges_list_url)
        if bridges_data:
            bridges_list = bridges_data.get('bridges', [])
            bridge_id = None
            for b in bridges_list:
                if (b.get('name', '').lower() == name.lower() or 
                    b.get('displayName', '').lower() == name.lower()):
                    bridge_id = b.get('id')
                    break
            
            if bridge_id:
                bridge_vol_url = f'https://bridges.llama.fi/bridgevolume/{bridge_id}'
                bridge_vol_data = fetch_json(bridge_vol_url)
                if bridge_vol_data and isinstance(bridge_vol_data, list):
                    count = 0
                    for entry in bridge_vol_data:
                        try:
                            ts = entry.get('date')
                            deposit_usd = entry.get('depositUSD', 0) or 0
                            withdraw_usd = entry.get('withdrawUSD', 0) or 0
                            total_volume = deposit_usd + withdraw_usd
                            if ts and total_volume > 0 and is_in_range(ts):
                                dt = datetime.utcfromtimestamp(ts)
                                all_records.append({
                                    'asset': gecko_id,
                                    'metric_name': 'BRIDGE_VOLUME',
                                    'value': float(total_volume),
                                    'pulled_at': dt
                                })
                                count += 1
                        except (ValueError, TypeError, KeyError):
                            continue
                    if count > 0:
                        metrics_found.append(f"BRIDGE_VOL({count})")
    
    inserted = 0
    if all_records:
        inserted = insert_records_batch(conn, all_records)
    
    return inserted, metrics_found


def filter_records_by_date(records, start_ts, end_ts):
    """Filter records to only include those within date range."""
    if start_ts is None and end_ts is None:
        return records
    
    filtered = []
    for r in records:
        ts = int(r['pulled_at'].timestamp())
        if (start_ts is None or ts >= start_ts) and (end_ts is None or ts <= end_ts):
            filtered.append(r)
    return filtered


def main():
    parser = argparse.ArgumentParser(description='DefiLlama Historical Backfill')
    parser.add_argument('--days', type=int, help='Number of days to backfill (default: 1095 = 3 years)')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD, default: yesterday)')
    parser.add_argument('--dry-run', action='store_true', help='Preview without inserting')
    args = parser.parse_args()
    
    # Calculate date range
    end_date = datetime.now() - timedelta(days=1)
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    elif args.days:
        start_date = end_date - timedelta(days=args.days)
    else:
        start_date = end_date - timedelta(days=365 * 3)  # Default 3 years
    
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.replace(hour=23, minute=59, second=59).timestamp())
    
    print("=" * 70)
    print("DEFILLAMA HISTORICAL BACKFILL (PARALLEL)")
    print("=" * 70)
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    if args.dry_run:
        print("DRY RUN - no data will be inserted")
    
    entities = load_config()
    print(f"Entities to backfill: {len(entities)}")
    print("=" * 70)
    
    if args.dry_run:
        print("\n[DRY RUN] Would process the above configuration")
        return
    
    conn = get_db_connection()
    
    print("\nBuilding URL list for all entities...")
    all_urls = []
    entity_url_map = {}
    
    for entity in entities:
        gecko_id = entity['gecko_id']
        urls = build_entity_urls(entity)
        entity_url_map[gecko_id] = urls
        all_urls.extend(urls.values())
    
    print(f"Total URLs to fetch: {len(all_urls)}")
    print(f"Estimated time at 16 req/sec: {len(all_urls) / 16:.0f}s")
    print("=" * 70)
    
    print("\nFetching all historical data in parallel...")
    start_time = time.time()
    
    url_results = fetch_urls_parallel(all_urls)
    
    fetch_time = time.time() - start_time
    print(f"Fetch completed in {fetch_time:.1f}s ({len(all_urls) / fetch_time:.1f} req/sec)")
    
    print("\nProcessing and inserting records...")
    
    total_records = 0
    filtered_records = 0
    entities_processed = 0
    
    for i, entity in enumerate(entities):
        gecko_id = entity['gecko_id']
        
        inserted, metrics = process_entity_data(entity, url_results, conn, start_ts, end_ts)
        total_records += inserted
        entities_processed += 1
        
        if metrics:
            metrics_str = ", ".join(metrics)
            print(f"[{i+1:3d}/{len(entities)}] {gecko_id:30s} +{inserted:6d} records ({metrics_str})")
        
        if (i + 1) % 100 == 0:
            elapsed = time.time() - start_time
            print(f"    --- Progress: {total_records:,} records, {entities_processed} entities ---")
    
    conn.close()
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("BACKFILL COMPLETE")
    print("=" * 70)
    print(f"  Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"  Entities processed: {entities_processed}")
    print(f"  Total records inserted: {total_records:,}")
    print(f"  Time elapsed: {elapsed:.1f} seconds")
    if elapsed > 0:
        print(f"  Average: {total_records / elapsed:.0f} records/sec")
    print("=" * 70)

if __name__ == "__main__":
    main()
