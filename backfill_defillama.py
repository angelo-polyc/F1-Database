import os
import csv
import time
import requests
import psycopg2
from datetime import datetime, timedelta

REQUEST_DELAY = 0.15
BATCH_SIZE = 1000

API_KEY = os.environ.get('DEFILLAMA_API_KEY')
PRO_BASE_URL = "https://pro-api.llama.fi"

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

def fetch_historical_prices(gecko_id):
    return []


def fetch_batch_historical_prices(gecko_ids, days=365):
    records = []
    
    coin_ids_str = ','.join([f"coingecko:{gid}" for gid in gecko_ids])
    end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    
    for day_offset in range(days):
        dt = end_date - timedelta(days=day_offset)
        ts = int(dt.timestamp())
        url = f"https://coins.llama.fi/prices/historical/{ts}/{coin_ids_str}?searchWidth=4h"
        data = fetch_json(url)
        
        if data and 'coins' in data:
            for coin_id, coin_data in data['coins'].items():
                try:
                    price = coin_data.get('price')
                    gecko_id = coin_id.replace('coingecko:', '')
                    if price is not None and price != 0:
                        records.append({
                            'asset': gecko_id,
                            'metric_name': 'PRICE',
                            'value': float(price),
                            'pulled_at': dt
                        })
                except (ValueError, TypeError):
                    continue
        
        time.sleep(REQUEST_DELAY)
        
        if (day_offset + 1) % 30 == 0:
            print(f"    Price backfill: {day_offset + 1}/{days} days processed, {len(records)} records")
    
    return records

def fetch_chain_historical_tvl(chain_name, gecko_id):
    records = []
    chain_slug = chain_name.lower().replace(' ', '-')
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

def fetch_chain_fees(chain_name, gecko_id):
    records = []
    chain_slug = chain_name.lower().replace(' ', '-')
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

def fetch_chain_dex_volume(chain_name, gecko_id):
    records = []
    chain_slug = chain_name.lower().replace(' ', '-')
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

def fetch_mcap_fdv(slug, gecko_id):
    records = []
    url = f"https://api.llama.fi/protocol/{slug}"
    data = fetch_json(url)
    
    if data:
        now = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        
        mcap = data.get('mcap')
        if mcap is not None and mcap != 0:
            records.append({
                'asset': gecko_id,
                'metric_name': 'MCAP',
                'value': float(mcap),
                'pulled_at': now
            })
        
        fdv = data.get('fdv')
        if fdv is not None and fdv != 0:
            records.append({
                'asset': gecko_id,
                'metric_name': 'FDV',
                'value': float(fdv),
                'pulled_at': now
            })
    
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

def load_config():
    entities = []
    with open('defillama_config.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('gecko_id'):
                category = row.get('category', '').strip()
                entities.append({
                    'gecko_id': row.get('gecko_id', '').lower(),
                    'slug': row.get('slug', '').lower(),
                    'name': row.get('name', ''),
                    'category': category,
                    'is_chain': category == 'Chain'
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
        chain_tvl = fetch_chain_historical_tvl(name, gecko_id)
        if chain_tvl:
            all_records.extend(chain_tvl)
            metrics_found.append(f"CHAIN_TVL({len(chain_tvl)})")
        time.sleep(REQUEST_DELAY)
        
        chain_fees = fetch_chain_fees(name, gecko_id)
        if chain_fees:
            all_records.extend(chain_fees)
            metrics_found.append(f"CHAIN_FEES({len(chain_fees)})")
        time.sleep(REQUEST_DELAY)
        
        chain_dex = fetch_chain_dex_volume(name, gecko_id)
        if chain_dex:
            all_records.extend(chain_dex)
            metrics_found.append(f"CHAIN_DEX({len(chain_dex)})")
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
        
        mcap_fdv = fetch_mcap_fdv(slug, gecko_id)
        if mcap_fdv:
            all_records.extend(mcap_fdv)
            for r in mcap_fdv:
                metrics_found.append(r['metric_name'])
        time.sleep(REQUEST_DELAY)
    
    time.sleep(REQUEST_DELAY)
    
    if API_KEY:
        inflow_records = fetch_historical_inflows(slug, gecko_id, days=30)
        if inflow_records:
            all_records.extend(inflow_records)
            metrics_found.append(f"INFLOW({len(inflow_records)})")
    
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
    
    print("\n" + "-" * 70)
    print("BATCH PRICE BACKFILL (365 days)")
    print("-" * 70)
    
    all_gecko_ids = [e['gecko_id'] for e in entities]
    batch_size = 30
    
    for batch_start in range(0, len(all_gecko_ids), batch_size):
        batch_ids = all_gecko_ids[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (len(all_gecko_ids) + batch_size - 1) // batch_size
        print(f"  Batch {batch_num}/{total_batches}: {len(batch_ids)} assets...")
        
        price_records = fetch_batch_historical_prices(batch_ids, days=365)
        if price_records:
            inserted = insert_records_batch(conn, price_records)
            total_records += inserted
            print(f"    +{inserted:,} price records")
    
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
