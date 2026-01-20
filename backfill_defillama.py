import os
import csv
import time
import requests
import psycopg2
from datetime import datetime, timedelta

REQUEST_DELAY = 0.08  # ~12 req/sec (Pro limit: 1000/min = 16.6/sec)
BATCH_SIZE = 1000

API_KEY = os.environ.get('DEFILLAMA_API_KEY')
PRO_BASE_URL = "https://pro-api.llama.fi"

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

def fetch_json(url):
    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception as e:
        return None

def fetch_pro_json(endpoint):
    if not API_KEY:
        return None
    url = f"{PRO_BASE_URL}/{API_KEY}{endpoint}"
    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception as e:
        return None

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
        time.sleep(0.08)
    
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


def extract_historical_data(data, chart_key, metric_name, asset_id):
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
            values.append((
                r['pulled_at'],
                'defillama',
                r['asset'],
                r['metric_name'],
                r['value']
            ))
        
        cur.executemany("""
            INSERT INTO metrics (pulled_at, source, asset, metric_name, value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source, asset, metric_name, pulled_at) DO NOTHING
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

def backfill_entity(entity, conn):
    gecko_id = entity['gecko_id']
    slug = entity['slug'] or gecko_id
    name = entity.get('name', slug)
    is_chain = entity.get('is_chain', False)
    
    all_records = []
    metrics_found = []
    
    if is_chain:
        chain_tvl = fetch_chain_historical_tvl(name, gecko_id, slug)
        if chain_tvl:
            all_records.extend(chain_tvl)
            metrics_found.append(f"CHAIN_TVL({len(chain_tvl)})")
        time.sleep(REQUEST_DELAY)
        
        chain_fees = fetch_chain_fees(name, gecko_id, slug)
        if chain_fees:
            all_records.extend(chain_fees)
            metrics_found.append(f"CHAIN_FEES({len(chain_fees)})")
        time.sleep(REQUEST_DELAY)
        
        chain_revenue = fetch_chain_revenue(name, gecko_id, slug)
        if chain_revenue:
            all_records.extend(chain_revenue)
            metrics_found.append(f"CHAIN_REVENUE({len(chain_revenue)})")
        time.sleep(REQUEST_DELAY)
        
        chain_dex = fetch_chain_dex_volume(name, gecko_id, slug)
        if chain_dex:
            all_records.extend(chain_dex)
            metrics_found.append(f"CHAIN_DEX({len(chain_dex)})")
        time.sleep(REQUEST_DELAY)
        
        chain_deriv = fetch_chain_derivatives(name, gecko_id, slug)
        if chain_deriv:
            all_records.extend(chain_deriv)
            metrics_found.append(f"CHAIN_PERPS({len(chain_deriv)})")
        time.sleep(REQUEST_DELAY)
        
        chain_opts = fetch_chain_options(name, gecko_id, slug)
        if chain_opts:
            all_records.extend(chain_opts)
            metrics_found.append(f"CHAIN_OPTIONS({len(chain_opts)})")
        time.sleep(REQUEST_DELAY)
    else:
        for endpoint_name, config in ENDPOINTS.items():
            url = config['url'].format(slug=slug)
            data = fetch_json(url)
            
            if data:
                records = extract_historical_data(
                    data, 
                    config['chart_key'], 
                    config['metric'],
                    gecko_id
                )
                if records:
                    all_records.extend(records)
                    metrics_found.append(f"{config['metric']}({len(records)})")
            
            time.sleep(REQUEST_DELAY)
    
    time.sleep(REQUEST_DELAY)
    
    if API_KEY:
        inflow_records = fetch_historical_inflows(slug, gecko_id, days=30)
        if inflow_records:
            all_records.extend(inflow_records)
            metrics_found.append(f"INFLOW({len(inflow_records)})")
    
    if gecko_id in STABLECOIN_IDS:
        stablecoin_records = fetch_stablecoin_historical(STABLECOIN_IDS[gecko_id], gecko_id)
        if stablecoin_records:
            all_records.extend(stablecoin_records)
            metrics_found.append(f"STABLECOIN_CIRC({len(stablecoin_records)})")
        time.sleep(REQUEST_DELAY)
    
    if gecko_id in BRIDGE_GECKO_IDS:
        bridge_records = fetch_bridge_historical(name, gecko_id)
        if bridge_records:
            all_records.extend(bridge_records)
            metrics_found.append(f"BRIDGE_VOL({len(bridge_records)})")
        time.sleep(REQUEST_DELAY)
    
    inserted = 0
    if all_records:
        inserted = insert_records_batch(conn, all_records)
    
    return inserted, metrics_found

def main():
    print("=" * 70)
    print("DEFILLAMA HISTORICAL BACKFILL")
    print("=" * 70)
    
    entities = load_config()
    print(f"Entities to backfill: {len(entities)}")
    print("=" * 70)
    
    conn = get_db_connection()
    
    total_records = 0
    entities_processed = 0
    start_time = time.time()
    
    for i, entity in enumerate(entities):
        gecko_id = entity['gecko_id']
        
        inserted, metrics = backfill_entity(entity, conn)
        total_records += inserted
        entities_processed += 1
        
        if metrics:
            metrics_str = ", ".join(metrics)
            print(f"[{i+1:3d}/{len(entities)}] {gecko_id:30s} +{inserted:6d} records ({metrics_str})")
        else:
            print(f"[{i+1:3d}/{len(entities)}] {gecko_id:30s} no historical data")
        
        if (i + 1) % 50 == 0:
            elapsed = time.time() - start_time
            rate = total_records / elapsed if elapsed > 0 else 0
            print(f"    --- Progress: {total_records:,} records, {rate:.0f} rec/sec ---")
    
    conn.close()
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("BACKFILL COMPLETE")
    print("=" * 70)
    print(f"  Entities processed: {entities_processed}")
    print(f"  Total records inserted: {total_records:,}")
    print(f"  Time elapsed: {elapsed:.1f} seconds")
    print("=" * 70)

if __name__ == "__main__":
    main()
