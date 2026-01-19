#!/usr/bin/env python3
"""
DefiLlama Pull Script for Unified Data Pipeline

Outputs data in long format matching Artemis structure:
  entity_id, metric_name, value, pulled_at, source

Designed to be run alongside Artemis pulls and inserted into same database.
"""

import csv
import json
import requests
from datetime import datetime
from typing import Optional
import os

# Configuration
DEFILLAMA_API_KEY = os.environ.get('DEFILLAMA_API_KEY', 'eaedd58ed0b70e8da98a1142564207e777ebad423c29c8175b93e09ff2a18bc7')
OUTPUT_DIR = os.environ.get('OUTPUT_DIR', '.')
CONFIG_FILE = os.environ.get('DEFILLAMA_CONFIG', 'defillama_config.csv')

# Metric mapping: DefiLlama field -> Standardized name
METRIC_MAP = {
    'protocols_tvl': 'TVL',
    'chains_tvl': 'CHAIN_TVL',
    'protocols_mcap': 'MC',
    'protocols_fdv': 'FDMC',
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
    'fees_dailyRevenue': 'REVENUE_24H',
    'fees_dailyProtocolRevenue': 'PROTOCOL_REVENUE_24H',
    'fees_dailySupplySideRevenue': 'SUPPLY_SIDE_REVENUE_24H',
    'fees_dailyUserFees': 'USER_FEES_24H',
    'fees_dailyHoldersRevenue': 'HOLDERS_REVENUE_24H',
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
}


def fetch_json(url: str) -> Optional[dict]:
    """Fetch JSON from URL with error handling."""
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  Error fetching {url}: {e}")
        return None


def build_lookup(data: list, key_fields: list) -> dict:
    """Build lookup dict from list of records."""
    lookup = {}
    for record in data:
        for field in key_fields:
            key = str(record.get(field, '')).lower().strip()
            if key:
                lookup[key] = record
    return lookup


def extract_nested(data: dict, key: str) -> Optional[float]:
    """Extract value from potentially nested dict like {peggedUSD: value}."""
    val = data.get(key)
    if isinstance(val, dict):
        return val.get('peggedUSD') or val.get('peggedEUR') or list(val.values())[0] if val else None
    return val


def pull_defillama_data(config_entities: list) -> list:
    """
    Pull data from DefiLlama for specified entities.
    Returns list of dicts in long format: {entity_id, metric_name, value, pulled_at, source}
    """
    
    pulled_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    results = []
    
    print("Fetching DefiLlama data...")
    
    # Fetch all endpoint data
    print("  Fetching protocols...")
    protocols = fetch_json('https://api.llama.fi/protocols') or []
    
    print("  Fetching chains...")
    chains = fetch_json('https://api.llama.fi/v2/chains') or []
    
    print("  Fetching fees...")
    fees_resp = fetch_json('https://api.llama.fi/overview/fees') or {}
    fees = fees_resp.get('protocols', []) if isinstance(fees_resp, dict) else []
    
    print("  Fetching dexs...")
    dexs_resp = fetch_json('https://api.llama.fi/overview/dexs') or {}
    dexs = dexs_resp.get('protocols', []) if isinstance(dexs_resp, dict) else []
    
    print("  Fetching derivatives...")
    deriv_resp = fetch_json('https://api.llama.fi/overview/derivatives') or {}
    derivatives = deriv_resp.get('protocols', []) if isinstance(deriv_resp, dict) else []
    
    print("  Fetching options...")
    opts_resp = fetch_json('https://api.llama.fi/overview/options') or {}
    options = opts_resp.get('protocols', []) if isinstance(opts_resp, dict) else []
    
    print("  Fetching aggregators...")
    agg_resp = fetch_json('https://api.llama.fi/overview/aggregators') or {}
    aggregators = agg_resp.get('protocols', []) if isinstance(agg_resp, dict) else []
    
    print("  Fetching bridges...")
    bridges_resp = fetch_json('https://bridges.llama.fi/bridges') or {}
    bridges = bridges_resp.get('bridges', []) if isinstance(bridges_resp, dict) else []
    
    print("  Fetching stablecoins...")
    stables_resp = fetch_json('https://stablecoins.llama.fi/stablecoins') or {}
    stablecoins = stables_resp.get('peggedAssets', []) if isinstance(stables_resp, dict) else []
    
    # Build lookups
    protocols_lookup = build_lookup(protocols, ['gecko_id', 'slug', 'name'])
    chains_lookup = build_lookup(chains, ['gecko_id', 'name'])
    fees_lookup = build_lookup(fees, ['slug', 'name', 'displayName'])
    dexs_lookup = build_lookup(dexs, ['slug', 'name', 'displayName'])
    derivatives_lookup = build_lookup(derivatives, ['slug', 'name', 'displayName'])
    options_lookup = build_lookup(options, ['slug', 'name', 'displayName'])
    aggregators_lookup = build_lookup(aggregators, ['slug', 'name', 'displayName'])
    bridges_lookup = build_lookup(bridges, ['name', 'displayName'])
    stablecoins_lookup = build_lookup(stablecoins, ['gecko_id', 'name', 'symbol'])
    
    print(f"\nProcessing {len(config_entities)} entities...")
    
    for entity in config_entities:
        gecko_id = entity.get('gecko_id', '').lower()
        slug = entity.get('slug', '').lower()
        name = entity.get('name', '').lower()
        
        if not gecko_id:
            continue
        
        raw_metrics = {}
        
        # Protocols
        p = protocols_lookup.get(gecko_id) or protocols_lookup.get(slug) or protocols_lookup.get(name)
        if p:
            raw_metrics['protocols_tvl'] = p.get('tvl')
            raw_metrics['protocols_mcap'] = p.get('mcap')
            raw_metrics['protocols_fdv'] = p.get('fdv')
            raw_metrics['protocols_staking'] = p.get('staking')
            raw_metrics['protocols_borrowed'] = p.get('borrowed')
            raw_metrics['protocols_pool2'] = p.get('pool2')
            raw_metrics['protocols_change_1h'] = p.get('change_1h')
            raw_metrics['protocols_change_1d'] = p.get('change_1d')
            raw_metrics['protocols_change_7d'] = p.get('change_7d')
        
        # Chains
        c = chains_lookup.get(gecko_id) or chains_lookup.get(name)
        if c:
            raw_metrics['chains_tvl'] = c.get('tvl')
        
        # Fees
        f = fees_lookup.get(slug) or fees_lookup.get(name)
        if f:
            raw_metrics['fees_total24h'] = f.get('total24h')
            raw_metrics['fees_total7d'] = f.get('total7d')
            raw_metrics['fees_total30d'] = f.get('total30d')
            raw_metrics['fees_total1y'] = f.get('total1y')
            raw_metrics['fees_totalAllTime'] = f.get('totalAllTime')
            raw_metrics['fees_dailyRevenue'] = f.get('dailyRevenue')
            raw_metrics['fees_dailyProtocolRevenue'] = f.get('dailyProtocolRevenue')
            raw_metrics['fees_dailySupplySideRevenue'] = f.get('dailySupplySideRevenue')
            raw_metrics['fees_dailyUserFees'] = f.get('dailyUserFees')
            raw_metrics['fees_dailyHoldersRevenue'] = f.get('dailyHoldersRevenue')
            raw_metrics['fees_average1y'] = f.get('average1y')
            raw_metrics['fees_change_1d'] = f.get('change_1d')
            raw_metrics['fees_change_7d'] = f.get('change_7d')
            raw_metrics['fees_change_1m'] = f.get('change_1m')
        
        # DEXs
        d = dexs_lookup.get(slug) or dexs_lookup.get(name)
        if d:
            raw_metrics['dexs_total24h'] = d.get('total24h')
            raw_metrics['dexs_total7d'] = d.get('total7d')
            raw_metrics['dexs_total30d'] = d.get('total30d')
            raw_metrics['dexs_totalAllTime'] = d.get('totalAllTime')
            raw_metrics['dexs_change_1d'] = d.get('change_1d')
            raw_metrics['dexs_change_7d'] = d.get('change_7d')
            raw_metrics['dexs_change_1m'] = d.get('change_1m')
        
        # Derivatives
        deriv = derivatives_lookup.get(slug) or derivatives_lookup.get(name)
        if deriv:
            raw_metrics['derivatives_total24h'] = deriv.get('total24h')
            raw_metrics['derivatives_total7d'] = deriv.get('total7d')
            raw_metrics['derivatives_total30d'] = deriv.get('total30d')
            raw_metrics['derivatives_change_1d'] = deriv.get('change_1d')
            raw_metrics['derivatives_change_7d'] = deriv.get('change_7d')
        
        # Options
        opt = options_lookup.get(slug) or options_lookup.get(name)
        if opt:
            raw_metrics['options_total24h'] = opt.get('total24h')
            raw_metrics['options_total7d'] = opt.get('total7d')
            raw_metrics['options_total30d'] = opt.get('total30d')
            raw_metrics['options_dailyNotionalVolume'] = opt.get('dailyNotionalVolume')
            raw_metrics['options_dailyPremiumVolume'] = opt.get('dailyPremiumVolume')
        
        # Aggregators
        agg = aggregators_lookup.get(slug) or aggregators_lookup.get(name)
        if agg:
            raw_metrics['aggregators_total24h'] = agg.get('total24h')
            raw_metrics['aggregators_total7d'] = agg.get('total7d')
            raw_metrics['aggregators_total30d'] = agg.get('total30d')
        
        # Bridges
        br = bridges_lookup.get(name)
        if br:
            raw_metrics['bridges_lastDailyVolume'] = br.get('lastDailyVolume')
            raw_metrics['bridges_weeklyVolume'] = br.get('weeklyVolume')
            raw_metrics['bridges_monthlyVolume'] = br.get('monthlyVolume')
        
        # Stablecoins
        st = stablecoins_lookup.get(gecko_id) or stablecoins_lookup.get(name)
        if st:
            circ = st.get('circulating', {})
            raw_metrics['stablecoins_circulating_peggedUSD'] = circ.get('peggedUSD') if isinstance(circ, dict) else circ
            circ_prev = st.get('circulatingPrevDay', {})
            raw_metrics['stablecoins_circulatingPrevDay_peggedUSD'] = circ_prev.get('peggedUSD') if isinstance(circ_prev, dict) else None
            circ_week = st.get('circulatingPrevWeek', {})
            raw_metrics['stablecoins_circulatingPrevWeek_peggedUSD'] = circ_week.get('peggedUSD') if isinstance(circ_week, dict) else None
            raw_metrics['stablecoins_price'] = st.get('price')
        
        # Convert to long format
        for raw_field, value in raw_metrics.items():
            if value is not None and value != '' and value != 0:
                metric_name = METRIC_MAP.get(raw_field, raw_field.upper())
                results.append({
                    'asset': gecko_id,
                    'metric_name': metric_name,
                    'value': value,
                    'pulled_at': pulled_at,
                    'source': 'defillama'
                })
    
    return results


def load_config(config_file: str) -> list:
    """Load entity config from CSV."""
    entities = []
    with open(config_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Include if has gecko_id
            if row.get('gecko_id'):
                entities.append(row)
    return entities


def save_results(results: list, output_file: str):
    """Save results to CSV."""
    if not results:
        print("No results to save.")
        return
    
    fieldnames = ['asset', 'metric_name', 'value', 'pulled_at', 'source']
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\nSaved {len(results)} rows to {output_file}")


def main():
    """Main entry point."""
    print("=" * 60)
    print("DefiLlama Data Pull")
    print("=" * 60)
    
    # Load config
    print(f"\nLoading config from {CONFIG_FILE}...")
    try:
        entities = load_config(CONFIG_FILE)
        print(f"  Loaded {len(entities)} entities")
    except FileNotFoundError:
        print(f"  Config file not found: {CONFIG_FILE}")
        print("  Using default entity list...")
        entities = []
    
    if not entities:
        print("No entities to pull. Create a config file with gecko_id column.")
        return
    
    # Pull data
    results = pull_defillama_data(entities)
    
    # Save results
    output_file = os.path.join(OUTPUT_DIR, 'defillama_latest_pull.csv')
    save_results(results, output_file)
    
    # Summary
    entities_with_data = len(set(r['asset'] for r in results))
    metrics_pulled = len(set(r['metric_name'] for r in results))
    print(f"\n=== SUMMARY ===")
    print(f"  Entities with data: {entities_with_data}")
    print(f"  Unique metrics: {metrics_pulled}")
    print(f"  Total data points: {len(results)}")


if __name__ == '__main__':
    main()
