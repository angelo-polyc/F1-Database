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
    records = []
    if not API_KEY:
        return records
    
    coin_id = f"coingecko:{gecko_id}"
    data = fetch_pro_json(f"/coins/chart/{coin_id}?period=365d&span=24")
    
    if data and 'coins' in data:
        coin_data = data.get('coins', {}).get(coin_id, {})
        prices = coin_data.get('prices', [])
        
        for entry in prices:
            try:
                ts = entry.get('timestamp')
                price = entry.get('price')
                if ts and price is not None and price != 0:
                    dt = datetime.utcfromtimestamp(ts)
                    records.append({
                        'asset': gecko_id,
                        'metric_name': 'PRICE',
                        'value': float(price),
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

def load_config():
    entities = []
    with open('defillama_config.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('gecko_id'):
                entities.append({
                    'gecko_id': row.get('gecko_id', '').lower(),
                    'slug': row.get('slug', '').lower(),
                    'name': row.get('name', '')
                })
    return entities

def backfill_entity(entity, conn):
    gecko_id = entity['gecko_id']
    slug = entity['slug'] or gecko_id
    
    all_records = []
    metrics_found = []
    
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
    
    if API_KEY:
        price_records = fetch_historical_prices(gecko_id)
        if price_records:
            all_records.extend(price_records)
            metrics_found.append(f"PRICE({len(price_records)})")
        time.sleep(REQUEST_DELAY)
        
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
