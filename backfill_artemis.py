import os
import csv
import time
import requests
import psycopg2
from datetime import datetime, timedelta

REQUEST_DELAY = 0.35
BATCH_SIZE = 50

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
    "Stock Trading Volume": "TOKENIZED_SHARES_TRADING_VOLUME",
    "Stablecoin Supply (USD)": "STABLECOIN_MC",
    "Filtered Stablecoin Transactions": "STABLECOIN_DAILY_TXNS",
    "90-Day ANN Volatility": "VOLATILITY_90D_ANN",
    "Daily Token Trading Volume": "TOKEN_TRADING_VOLUME",
    "Net Asset Value": "NAV",
    "FDMV / NAV": "FDMV_NAV_RATIO",
    "Stablecoins Average DAUs": "STABLECOIN_AVG_DAU",
    "Stablecoin Average Transaction Value": "AVERAGE_TRANSACTION_VALUE",
}

def load_config():
    pull_config = {}
    
    with open('artemis_config.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        pull_idx = headers.index('Pull')
        metric_cols = headers[pull_idx + 1:]
        
        for row in reader:
            if row['Pull'] in ['1', '1.0', 1, 1.0]:
                asset_id = row['asset']
                
                for metric in metric_cols:
                    if row.get(metric) in ['1', '1.0', 1, 1.0]:
                        api_id = FRIENDLY_TO_API_ID.get(metric, metric)
                        if api_id not in pull_config:
                            pull_config[api_id] = []
                        if asset_id not in pull_config[api_id]:
                            pull_config[api_id].append(asset_id)
    
    return pull_config

def fetch_historical(api_key, metric, symbols, start_date, end_date):
    url = f"https://api.artemisxyz.com/data/{metric.lower()}/"
    params = {
        'symbols': ','.join(symbols),
        'startDate': start_date,
        'endDate': end_date,
        'APIKey': api_key
    }
    
    try:
        resp = requests.get(url, params=params, timeout=120)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            time.sleep(5)
            return None
        else:
            return None
    except Exception as e:
        print(f"    Error: {e}")
        return None

def extract_records(data, metric_name):
    records = []
    if not data or 'data' not in data:
        return records
    
    symbols_data = data.get('data', {}).get('symbols', {})
    
    for asset, asset_data in symbols_data.items():
        metric_key = metric_name.lower()
        metric_values = asset_data.get(metric_key, [])
        
        if isinstance(metric_values, list):
            for entry in metric_values:
                if isinstance(entry, dict) and 'date' in entry and 'val' in entry:
                    try:
                        date_str = entry['date']
                        value = entry['val']
                        if value is None:
                            continue
                        dt = datetime.strptime(date_str, '%Y-%m-%d')
                        records.append((dt, 'artemis', asset, metric_name, float(value)))
                    except (ValueError, TypeError):
                        continue
    
    return records

def main():
    api_key = os.environ.get('ARTEMIS_API_KEY')
    if not api_key:
        print("ERROR: ARTEMIS_API_KEY not set")
        return
    
    print("=" * 60)
    print("ARTEMIS HISTORICAL BACKFILL")
    print("=" * 60)
    
    pull_config = load_config()
    all_metrics = list(pull_config.keys())
    all_assets = sorted(set(a for assets in pull_config.values() for a in assets))
    
    print(f"Metrics to backfill: {len(all_metrics)}")
    print(f"Assets to backfill: {len(all_assets)}")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 5)
    
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print("=" * 60)
    
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    
    total_records = 0
    api_calls = 0
    start_time = time.time()
    
    for idx, metric in enumerate(all_metrics):
        assets_for_metric = pull_config[metric]
        print(f"\n[{idx+1:2d}/{len(all_metrics)}] {metric} ({len(assets_for_metric)} assets)")
        
        metric_records = 0
        
        for batch_start in range(0, len(assets_for_metric), BATCH_SIZE):
            batch = assets_for_metric[batch_start:batch_start + BATCH_SIZE]
            batch_num = batch_start // BATCH_SIZE + 1
            total_batches = (len(assets_for_metric) + BATCH_SIZE - 1) // BATCH_SIZE
            
            print(f"    Batch {batch_num}/{total_batches} ({len(batch)} assets)...", end=" ", flush=True)
            
            data = fetch_historical(
                api_key, 
                metric, 
                batch, 
                start_date.strftime('%Y-%m-%d'), 
                end_date.strftime('%Y-%m-%d')
            )
            api_calls += 1
            
            if data:
                records = extract_records(data, metric)
                if records:
                    cur.executemany('''
                        INSERT INTO metrics (pulled_at, source, asset, metric_name, value)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    ''', records)
                    conn.commit()
                    metric_records += len(records)
                    print(f"+{len(records)} records")
                else:
                    print("no data")
            else:
                print("failed")
            
            time.sleep(REQUEST_DELAY)
        
        total_records += metric_records
        elapsed = time.time() - start_time
        rate = total_records / elapsed if elapsed > 0 else 0
        print(f"    Subtotal: {metric_records:,} records | Total: {total_records:,} | {rate:.0f} rec/sec")
    
    conn.close()
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print("BACKFILL COMPLETE")
    print("=" * 60)
    print(f"Total records: {total_records:,}")
    print(f"API calls: {api_calls}")
    print(f"Time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"Rate: {total_records/elapsed:.0f} records/sec")
    print("=" * 60)

if __name__ == "__main__":
    main()
