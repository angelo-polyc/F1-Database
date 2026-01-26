#!/usr/bin/env python3
"""
CoinGecko Price & Volume Data Pull Script
==========================================
Pulls hourly price and volume data from CoinGecko API and stores in PostgreSQL.

Author: Generated for Angelo's crypto analytics infrastructure
Created: January 2026

Usage:
    python coingecko_pull.py                    # Full pull using config file
    python coingecko_pull.py --refresh-ids      # Refresh CoinGecko ID mappings
    python coingecko_pull.py --dry-run          # Test without DB write
    
Scheduling (cron):
    0 * * * * /usr/bin/python3 /path/to/coingecko_pull.py >> /path/to/logs/coingecko.log 2>&1
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
import csv

import requests
import psycopg2
from psycopg2.extras import execute_values

# =============================================================================
# CONFIGURATION
# =============================================================================

# API Configuration
COINGECKO_API_KEY = os.environ.get('COINGECKO_API_KEY', '')  # Set via environment variable
USE_PRO_API = bool(COINGECKO_API_KEY)  # Auto-detect based on API key presence

# API Endpoints
DEMO_BASE_URL = "https://api.coingecko.com/api/v3"
PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"
BASE_URL = PRO_BASE_URL if USE_PRO_API else DEMO_BASE_URL

# Rate Limiting
RATE_LIMIT_CALLS_PER_MIN = 500 if USE_PRO_API else 30
REQUEST_DELAY = 60 / RATE_LIMIT_CALLS_PER_MIN + 0.1  # Add buffer

# Batch Configuration
MAX_COINS_PER_REQUEST = 250  # CoinGecko limit for /coins/markets

# Database Configuration - Update these for your setup
DB_CONFIG = {
    'host': os.environ.get('PGHOST', 'localhost'),
    'port': os.environ.get('PGPORT', '5432'),
    'database': os.environ.get('PGDATABASE', 'crypto_analytics'),
    'user': os.environ.get('PGUSER', 'postgres'),
    'password': os.environ.get('PGPASSWORD', '')
}

# File Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, 'coingecko_config.csv')
ID_CACHE_FILE = os.path.join(SCRIPT_DIR, 'coingecko_id_cache.json')
LOG_DIR = os.path.join(SCRIPT_DIR, 'logs')

# =============================================================================
# LOGGING SETUP
# =============================================================================

os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOG_DIR, f'coingecko_{datetime.now().strftime("%Y%m%d")}.log'))
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# API CLIENT
# =============================================================================

class CoinGeckoClient:
    """CoinGecko API client with rate limiting and error handling."""
    
    def __init__(self, api_key: str = '', use_pro: bool = False):
        self.api_key = api_key
        self.use_pro = use_pro and bool(api_key)
        self.base_url = PRO_BASE_URL if self.use_pro else DEMO_BASE_URL
        self.session = requests.Session()
        self.last_request_time = 0
        
        # Set up headers
        if self.use_pro:
            self.session.headers['x-cg-pro-api-key'] = api_key
        
        self.session.headers['Accept'] = 'application/json'
        
        logger.info(f"CoinGecko client initialized - Pro API: {self.use_pro}")
    
    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY:
            sleep_time = REQUEST_DELAY - elapsed
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: dict = None) -> Optional[dict]:
        """Make a rate-limited API request with error handling."""
        self._rate_limit()
        
        url = f"{self.base_url}/{endpoint}"
        
        # Add API key to params for demo API
        if not self.use_pro and self.api_key:
            params = params or {}
            params['x_cg_demo_api_key'] = self.api_key
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 429:
                logger.warning("Rate limited - waiting 60 seconds")
                time.sleep(60)
                return self._make_request(endpoint, params)
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {endpoint} - {e}")
            return None
    
    def get_coins_list(self) -> List[dict]:
        """Fetch complete list of coins with IDs and symbols."""
        logger.info("Fetching complete coins list from CoinGecko...")
        result = self._make_request('coins/list', {'include_platform': 'false'})
        
        if result:
            logger.info(f"Retrieved {len(result)} coins from CoinGecko")
            return result
        return []
    
    def get_coins_markets(self, coin_ids: List[str], vs_currency: str = 'usd') -> List[dict]:
        """Fetch market data for specified coins."""
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


# =============================================================================
# ID MAPPING MANAGER
# =============================================================================

class IDMappingManager:
    """Manages symbol-to-ID mappings for CoinGecko."""
    
    def __init__(self, client: CoinGeckoClient, config_file: str, cache_file: str):
        self.client = client
        self.config_file = config_file
        self.cache_file = cache_file
        self.id_cache = {}
        self._load_cache()
    
    def _load_cache(self):
        """Load cached ID mappings."""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    self.id_cache = json.load(f)
                logger.info(f"Loaded {len(self.id_cache)} IDs from cache")
            except Exception as e:
                logger.warning(f"Failed to load ID cache: {e}")
                self.id_cache = {}
    
    def _save_cache(self):
        """Save ID mappings to cache."""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.id_cache, f, indent=2)
            logger.info(f"Saved {len(self.id_cache)} IDs to cache")
        except Exception as e:
            logger.error(f"Failed to save ID cache: {e}")
    
    def refresh_id_mappings(self, symbols: List[str]) -> Dict[str, dict]:
        """
        Refresh ID mappings from CoinGecko API.
        Returns dict of symbol -> {id, name, symbol} for each match.
        For ambiguous symbols, returns the highest market cap match.
        """
        coins_list = self.client.get_coins_list()
        if not coins_list:
            logger.error("Failed to fetch coins list")
            return {}
        
        # Build symbol -> [coins] mapping
        symbol_map = {}
        for coin in coins_list:
            sym = coin.get('symbol', '').upper()
            if sym not in symbol_map:
                symbol_map[sym] = []
            symbol_map[sym].append(coin)
        
        # Match requested symbols
        mappings = {}
        ambiguous = []
        not_found = []
        
        for sym in symbols:
            sym_upper = sym.upper()
            matches = symbol_map.get(sym_upper, [])
            
            if len(matches) == 0:
                not_found.append(sym)
            elif len(matches) == 1:
                mappings[sym_upper] = {
                    'coingecko_id': matches[0]['id'],
                    'name': matches[0]['name'],
                    'symbol': matches[0]['symbol']
                }
            else:
                # Multiple matches - flag as ambiguous
                ambiguous.append((sym_upper, matches))
                # Default to first match (usually highest ranked)
                mappings[sym_upper] = {
                    'coingecko_id': matches[0]['id'],
                    'name': matches[0]['name'],
                    'symbol': matches[0]['symbol'],
                    'ambiguous': True,
                    'alternatives': [m['id'] for m in matches[:5]]
                }
        
        if not_found:
            logger.warning(f"Symbols not found: {not_found}")
        
        if ambiguous:
            logger.warning(f"Ambiguous symbols (review config): {[a[0] for a in ambiguous]}")
            for sym, matches in ambiguous[:5]:
                logger.info(f"  {sym}: {[m['id'] for m in matches[:5]]}")
        
        self.id_cache = mappings
        self._save_cache()
        
        return mappings
    
    def load_config(self) -> Dict[str, str]:
        """Load symbol -> CoinGecko ID mappings from config file."""
        mappings = {}
        
        if not os.path.exists(self.config_file):
            logger.warning(f"Config file not found: {self.config_file}")
            return mappings
        
        try:
            with open(self.config_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('enabled', 'true').lower() == 'true':
                        symbol = row.get('symbol', '').upper().strip()
                        cg_id = row.get('coingecko_id', '').strip()
                        if symbol and cg_id:
                            mappings[symbol] = cg_id
            
            logger.info(f"Loaded {len(mappings)} symbols from config")
            return mappings
            
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {}
    
    def generate_config(self, symbols: List[str]):
        """Generate initial config file from symbols list."""
        mappings = self.refresh_id_mappings(symbols)
        
        rows = []
        for sym in symbols:
            sym_upper = sym.upper()
            mapping = mappings.get(sym_upper, {})
            rows.append({
                'symbol': sym_upper,
                'coingecko_id': mapping.get('coingecko_id', ''),
                'name': mapping.get('name', ''),
                'enabled': 'true',
                'notes': 'AMBIGUOUS - VERIFY' if mapping.get('ambiguous') else ''
            })
        
        # Write config file
        with open(self.config_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['symbol', 'coingecko_id', 'name', 'enabled', 'notes'])
            writer.writeheader()
            writer.writerows(rows)
        
        logger.info(f"Generated config file: {self.config_file}")
        
        # Log ambiguous entries
        ambiguous_entries = [r for r in rows if 'AMBIGUOUS' in r.get('notes', '')]
        if ambiguous_entries:
            logger.warning(f"\n{'='*60}")
            logger.warning("AMBIGUOUS SYMBOLS - Please verify in config file:")
            logger.warning(f"{'='*60}")
            for entry in ambiguous_entries:
                sym = entry['symbol']
                mapping = mappings.get(sym, {})
                alts = mapping.get('alternatives', [])
                logger.warning(f"  {sym}: Selected '{mapping.get('coingecko_id')}', alternatives: {alts}")


# =============================================================================
# DATABASE MANAGER
# =============================================================================

class DatabaseManager:
    """PostgreSQL database manager for CoinGecko data."""
    
    SCHEMA_SQL = """
    -- CoinGecko price/volume data table
    CREATE TABLE IF NOT EXISTS coingecko_hourly (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        coingecko_id VARCHAR(100) NOT NULL,
        name VARCHAR(200),
        price_usd NUMERIC(28, 12),
        volume_24h_usd NUMERIC(28, 2),
        market_cap_usd NUMERIC(28, 2),
        market_cap_rank INTEGER,
        high_24h NUMERIC(28, 12),
        low_24h NUMERIC(28, 12),
        price_change_24h NUMERIC(28, 12),
        price_change_pct_1h NUMERIC(12, 6),
        price_change_pct_24h NUMERIC(12, 6),
        price_change_pct_7d NUMERIC(12, 6),
        circulating_supply NUMERIC(28, 4),
        total_supply NUMERIC(28, 4),
        max_supply NUMERIC(28, 4),
        ath_usd NUMERIC(28, 12),
        ath_date TIMESTAMPTZ,
        atl_usd NUMERIC(28, 12),
        atl_date TIMESTAMPTZ,
        last_updated TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(timestamp, coingecko_id)
    );
    
    -- Index for common queries
    CREATE INDEX IF NOT EXISTS idx_coingecko_hourly_symbol_ts 
        ON coingecko_hourly(symbol, timestamp DESC);
    CREATE INDEX IF NOT EXISTS idx_coingecko_hourly_cgid_ts 
        ON coingecko_hourly(coingecko_id, timestamp DESC);
    CREATE INDEX IF NOT EXISTS idx_coingecko_hourly_ts 
        ON coingecko_hourly(timestamp DESC);
    
    -- Metadata table for tracking pulls
    CREATE TABLE IF NOT EXISTS coingecko_pull_log (
        id SERIAL PRIMARY KEY,
        pull_timestamp TIMESTAMPTZ NOT NULL,
        symbols_requested INTEGER,
        symbols_retrieved INTEGER,
        symbols_failed TEXT[],
        duration_seconds NUMERIC(10, 2),
        api_calls INTEGER,
        status VARCHAR(50),
        error_message TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    """
    
    def __init__(self, config: dict):
        self.config = config
        self.conn = None
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.config)
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def initialize_schema(self):
        """Create tables if they don't exist."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(self.SCHEMA_SQL)
            self.conn.commit()
            logger.info("Database schema initialized")
            return True
        except Exception as e:
            logger.error(f"Schema initialization failed: {e}")
            self.conn.rollback()
            return False
    
    def insert_market_data(self, data: List[dict], timestamp: datetime) -> Tuple[int, List[str]]:
        """
        Insert market data records.
        Returns (inserted_count, failed_symbols).
        """
        if not data:
            return 0, []
        
        insert_sql = """
            INSERT INTO coingecko_hourly (
                timestamp, symbol, coingecko_id, name, price_usd, volume_24h_usd,
                market_cap_usd, market_cap_rank, high_24h, low_24h, price_change_24h,
                price_change_pct_1h, price_change_pct_24h, price_change_pct_7d,
                circulating_supply, total_supply, max_supply,
                ath_usd, ath_date, atl_usd, atl_date, last_updated
            ) VALUES %s
            ON CONFLICT (timestamp, coingecko_id) 
            DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                volume_24h_usd = EXCLUDED.volume_24h_usd,
                market_cap_usd = EXCLUDED.market_cap_usd,
                market_cap_rank = EXCLUDED.market_cap_rank,
                high_24h = EXCLUDED.high_24h,
                low_24h = EXCLUDED.low_24h,
                price_change_24h = EXCLUDED.price_change_24h,
                price_change_pct_1h = EXCLUDED.price_change_pct_1h,
                price_change_pct_24h = EXCLUDED.price_change_pct_24h,
                price_change_pct_7d = EXCLUDED.price_change_pct_7d,
                last_updated = EXCLUDED.last_updated
        """
        
        def parse_datetime(dt_str):
            if not dt_str:
                return None
            try:
                return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            except:
                return None
        
        rows = []
        for item in data:
            pct_changes = item.get('price_change_percentage_1h_in_currency') or {}
            
            rows.append((
                timestamp,
                item.get('symbol', '').upper(),
                item.get('id', ''),
                item.get('name', ''),
                item.get('current_price'),
                item.get('total_volume'),
                item.get('market_cap'),
                item.get('market_cap_rank'),
                item.get('high_24h'),
                item.get('low_24h'),
                item.get('price_change_24h'),
                item.get('price_change_percentage_1h_in_currency'),
                item.get('price_change_percentage_24h'),
                item.get('price_change_percentage_7d_in_currency'),
                item.get('circulating_supply'),
                item.get('total_supply'),
                item.get('max_supply'),
                item.get('ath'),
                parse_datetime(item.get('ath_date')),
                item.get('atl'),
                parse_datetime(item.get('atl_date')),
                parse_datetime(item.get('last_updated'))
            ))
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, insert_sql, rows, page_size=100)
            self.conn.commit()
            logger.info(f"Inserted {len(rows)} records")
            return len(rows), []
        except Exception as e:
            logger.error(f"Insert failed: {e}")
            self.conn.rollback()
            return 0, [item.get('symbol', 'UNKNOWN') for item in data]
    
    def log_pull(self, timestamp: datetime, requested: int, retrieved: int, 
                 failed: List[str], duration: float, api_calls: int, 
                 status: str, error: str = None):
        """Log pull metadata."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO coingecko_pull_log 
                    (pull_timestamp, symbols_requested, symbols_retrieved, 
                     symbols_failed, duration_seconds, api_calls, status, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (timestamp, requested, retrieved, failed, duration, api_calls, status, error))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to log pull: {e}")
            self.conn.rollback()


# =============================================================================
# MAIN PULL LOGIC
# =============================================================================

def run_pull(dry_run: bool = False):
    """Execute the hourly data pull."""
    start_time = time.time()
    pull_timestamp = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    api_calls = 0
    
    logger.info(f"\n{'='*60}")
    logger.info(f"CoinGecko Pull Started - {pull_timestamp.isoformat()}")
    logger.info(f"{'='*60}")
    
    # Initialize client
    client = CoinGeckoClient(COINGECKO_API_KEY, USE_PRO_API)
    
    # Load ID mappings from config
    id_manager = IDMappingManager(client, CONFIG_FILE, ID_CACHE_FILE)
    symbol_to_id = id_manager.load_config()
    
    if not symbol_to_id:
        logger.error("No symbols configured. Run with --refresh-ids first.")
        return False
    
    # Get list of CoinGecko IDs to fetch
    coin_ids = list(set(symbol_to_id.values()))
    logger.info(f"Fetching data for {len(coin_ids)} unique coins")
    
    # Fetch in batches
    all_data = []
    for i in range(0, len(coin_ids), MAX_COINS_PER_REQUEST):
        batch = coin_ids[i:i + MAX_COINS_PER_REQUEST]
        logger.info(f"Fetching batch {i//MAX_COINS_PER_REQUEST + 1}: {len(batch)} coins")
        
        data = client.get_coins_markets(batch)
        api_calls += 1
        
        if data:
            all_data.extend(data)
            logger.info(f"  Retrieved {len(data)} records")
        else:
            logger.warning(f"  Batch returned no data")
    
    logger.info(f"Total records retrieved: {len(all_data)}")
    
    # Find missing symbols
    retrieved_ids = {item['id'] for item in all_data}
    missing = [sym for sym, cg_id in symbol_to_id.items() if cg_id not in retrieved_ids]
    if missing:
        logger.warning(f"Missing data for symbols: {missing}")
    
    if dry_run:
        logger.info("DRY RUN - Skipping database write")
        logger.info(f"Sample data: {json.dumps(all_data[:2], indent=2, default=str)}")
        return True
    
    # Database operations
    db = DatabaseManager(DB_CONFIG)
    if not db.connect():
        return False
    
    try:
        db.initialize_schema()
        inserted, failed = db.insert_market_data(all_data, pull_timestamp)
        
        duration = time.time() - start_time
        status = 'SUCCESS' if not failed else 'PARTIAL'
        
        db.log_pull(
            timestamp=pull_timestamp,
            requested=len(symbol_to_id),
            retrieved=len(all_data),
            failed=failed + missing,
            duration=duration,
            api_calls=api_calls,
            status=status
        )
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Pull Complete - Duration: {duration:.2f}s")
        logger.info(f"  Requested: {len(symbol_to_id)} | Retrieved: {len(all_data)} | Inserted: {inserted}")
        logger.info(f"{'='*60}\n")
        
        return True
        
    finally:
        db.close()


def refresh_ids(symbols: List[str]):
    """Refresh ID mappings and generate/update config file."""
    client = CoinGeckoClient(COINGECKO_API_KEY, USE_PRO_API)
    id_manager = IDMappingManager(client, CONFIG_FILE, ID_CACHE_FILE)
    id_manager.generate_config(symbols)
    
    logger.info(f"\nConfig file generated: {CONFIG_FILE}")
    logger.info("Please review the config file, especially entries marked AMBIGUOUS")


# =============================================================================
# CLI
# =============================================================================

# Default symbols list from user request
DEFAULT_SYMBOLS = """
RIVER KNTQ AXS VVV PAAL STABLE STG ZRO IP RENDER CC RLB PUMP RAIN G MANA XTZ PEPE SAFE SAND ARC SQD POL CVX NATIX STX PAXG AKT AO NS ATOM VIRTUAL BLAST LPT GNO ICP AXL SXT SIGN DEEP OP BONK MINA DOT TRX SPK 2Z SHIB BERA BGB RAY WCT GEOD TAO FET SHELL BNB RON TAIKO CAKE PENDLE XMR LUNA PENGU RED WIF ZRX BLUR ME RUNE FIL SUSHI FRAX SKY META HONEY DAI USDT USDC WAL SNT SUI NOT XRP BEAM USUAL CFG BABY LBTC BTC IO CETUS IMX DOGE BARD ALGO RPL MET LA GALA SOL YFI TRUMP TON ADA GRT AR AEVO stETH CRO CELO OSMO BIO SOPH ETH AAVE SCR DRIFT RETARDIO ETC PNUT MORPHO XLM ALT MOVE LINK NEAR DBR NXPC PYTH OMNI JUP ZK TIA AERO SEI DYDX GOAT ORCA TORN DRV BLUE AVAX INJ FARTCOIN UMA SNX ILV 1INCH CORE SSV SWELL XAI ENS POPCAT REZ YGG MAV SYRUP S 0G AIXBT ARKM MOODENG WLD COW AVAIL L3 ATH CRV SAHARA MAGIC COMP B3 LDO APT SHFL ACX INIT BANANA KMNO ARB LTC HYPE OM ONDO ASTER NTRN APE FTT GRASS PI STRK XPL U SOLV DEGEN NEON LQTY NYM NIL GFI PLUME JTO YNE ETHFI PEAQ PUFFER LAYER SWEAT MNT BAL W EIGEN UNI GMX DYM KAITO ALEO VANA SAGA ENA MON CLANKER HNT ZEC SPX LUNA TNSR HONEY MPLX SPEC CPOOL EUL PIPPIN XAN OLAS LIT WET ACT MOR ZEREBRO MERL MEGA DEUS POLY MASK
""".split()


def main():
    parser = argparse.ArgumentParser(description='CoinGecko Price & Volume Data Pull')
    parser.add_argument('--refresh-ids', action='store_true', 
                        help='Refresh CoinGecko ID mappings')
    parser.add_argument('--dry-run', action='store_true',
                        help='Test run without database write')
    parser.add_argument('--symbols', type=str, default='',
                        help='Comma-separated symbols (overrides default list)')
    
    args = parser.parse_args()
    
    if args.refresh_ids:
        symbols = args.symbols.split(',') if args.symbols else DEFAULT_SYMBOLS
        symbols = [s.strip() for s in symbols if s.strip()]
        refresh_ids(symbols)
    else:
        run_pull(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
