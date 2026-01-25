#!/usr/bin/env python3
"""
Alpha Vantage Daily Price & Volume Pull
Pulls daily OHLCV data for specified tickers after market close.

Schedule with cron to run at 4:05pm ET (after market close):
    05 16 * * 1-5 cd /path/to/script && python alphavantage_daily_pull.py

Or for systems using UTC (4:05pm ET = 21:05 UTC during EST, 20:05 UTC during EDT):
    05 21 * * 1-5 cd /path/to/script && python alphavantage_daily_pull.py
"""

import requests
import time
import csv
import os
import json
from datetime import datetime, date
from typing import Optional, Dict, List, Tuple
import logging

# =============================================================================
# CONFIGURATION
# =============================================================================

API_KEY = "56NX9XU6WPN2J3BQ"
BASE_URL = "https://www.alphavantage.co/query"

# Rate limiting: Free tier = 5 requests/min, 25/day
# Premium tiers allow more. Adjust as needed.
REQUESTS_PER_MINUTE = 5
REQUEST_DELAY = 60 / REQUESTS_PER_MINUTE + 0.5  # ~12.5 seconds between requests

# Output settings
OUTPUT_DIR = "./data"
CSV_FILENAME = "alphavantage_daily_{date}.csv"
MASTER_CSV = "alphavantage_daily_master.csv"

# Tickers to pull - organized by exchange for reference
TICKERS = {
    # Crypto/Fintech - Nasdaq unless noted
    "COIN": "NASDAQ",   # Coinbase
    "HOOD": "NASDAQ",   # Robinhood
    "CRCL": "NASDAQ",   # Circle (if listed)
    "GLXY": "NASDAQ",   # Galaxy Digital
    "SOFI": "NASDAQ",   # SoFi
    "BLSH": "NYSE",     # Blockchain Coinvestors
    "FIGR": "NASDAQ",   # Figure Technologies
    "GEMI": "NASDAQ",   # Gemini (if listed)
    "ETOR": "NASDAQ",   # eToro
    "XYZ": "NASDAQ",    # Block Inc
    "PYPL": "NASDAQ",   # PayPal
    "EXOD": "AMEX",     # Exodus Movement
    "WU": "NYSE",       # Western Union
    "IBKR": "NASDAQ",   # Interactive Brokers
    "BTGO": "NASDAQ",   # BIT Mining
    
    # ETFs
    "QQQ": "NASDAQ",    # Nasdaq 100 ETF
    "SPY": "NYSE",      # S&P 500 ETF
    
    # Crypto/Betting/Other
    "SBET": "NASDAQ",   # SharpLink Gaming
    "BMNR": "AMEX",     # Bitmine Immersion
    "DFDV": "NASDAQ",   # 
    "HODL": "NASDAQ",   # VanEck Bitcoin ETF
    "ETHZ": "NASDAQ",   # 
    
    # Bitcoin Treasury / Mining
    "MSTR": "NASDAQ",   # MicroStrategy
    "FWDI": "NASDAQ",   # Forward Industries
    "HSDT": "NASDAQ",   # Helius Medical
    "IPST": "NASDAQ",   # Heritage Distilling
    "HYPD": "NASDAQ",   # 
    "PURR": "NASDAQ",   # 
    "THAR": "NASDAQ",   # Tharimmune
    "GNLN": "NASDAQ",   # Greenlane
    "CIFR": "NASDAQ",   # Cipher Mining
    "RIOT": "NASDAQ",   # Riot Platforms
    "WULF": "NASDAQ",   # TeraWulf
    "CORZ": "NASDAQ",   # Core Scientific
    "MARA": "NASDAQ",   # Marathon Digital
    "HUT": "NASDAQ",    # Hut 8 Mining
    "CLSK": "NASDAQ",   # CleanSpark
    "BTDR": "NASDAQ",   # Bitdeer
    "IREN": "NASDAQ",   # Iris Energy
}

TICKER_LIST = list(TICKERS.keys())

# =============================================================================
# LOGGING SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('alphavantage_pull.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# API FUNCTIONS
# =============================================================================

def fetch_daily_data(symbol: str) -> Optional[Dict]:
    """
    Fetch daily time series data for a symbol.
    Returns the most recent trading day's data.
    """
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact"  # Last 100 days
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Check for API errors
        if "Error Message" in data:
            logger.error(f"{symbol}: API Error - {data['Error Message']}")
            return None
        
        if "Note" in data:
            logger.warning(f"{symbol}: API Note (likely rate limit) - {data['Note']}")
            return None
        
        if "Information" in data:
            logger.warning(f"{symbol}: API Info - {data['Information']}")
            return None
            
        time_series = data.get("Time Series (Daily)", {})
        if not time_series:
            logger.warning(f"{symbol}: No time series data returned")
            return None
        
        # Get the most recent date
        latest_date = max(time_series.keys())
        latest_data = time_series[latest_date]
        
        return {
            "symbol": symbol,
            "date": latest_date,
            "open": float(latest_data["1. open"]),
            "high": float(latest_data["2. high"]),
            "low": float(latest_data["3. low"]),
            "close": float(latest_data["4. close"]),
            "volume": int(latest_data["5. volume"]),
            "dollar_volume": float(latest_data["4. close"]) * int(latest_data["5. volume"])
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"{symbol}: Request failed - {e}")
        return None
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        logger.error(f"{symbol}: Parse error - {e}")
        return None


def fetch_all_tickers(tickers: List[str]) -> Tuple[List[Dict], List[str]]:
    """
    Fetch data for all tickers with rate limiting.
    Returns (successful_results, failed_tickers)
    """
    results = []
    failed = []
    
    total = len(tickers)
    for i, symbol in enumerate(tickers, 1):
        logger.info(f"[{i}/{total}] Fetching {symbol}...")
        
        data = fetch_daily_data(symbol)
        
        if data:
            results.append(data)
            logger.info(f"  ✓ {symbol}: ${data['close']:.2f}, Vol: {data['volume']:,}, $Vol: ${data['dollar_volume']:,.0f}")
        else:
            failed.append(symbol)
            logger.warning(f"  ✗ {symbol}: FAILED")
        
        # Rate limiting - skip delay on last ticker
        if i < total:
            time.sleep(REQUEST_DELAY)
    
    return results, failed


# =============================================================================
# OUTPUT FUNCTIONS
# =============================================================================

def save_to_csv(results: List[Dict], filename: str, append: bool = False):
    """Save results to CSV file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    mode = 'a' if append else 'w'
    write_header = not append or not os.path.exists(filepath)
    
    fieldnames = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'dollar_volume']
    
    with open(filepath, mode, newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(results)
    
    logger.info(f"Saved {len(results)} records to {filepath}")
    return filepath


def save_to_json(results: List[Dict], filename: str):
    """Save results to JSON file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    with open(filepath, 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"Saved {len(results)} records to {filepath}")
    return filepath


# =============================================================================
# POSTGRESQL INTEGRATION (OPTIONAL)
# =============================================================================

def save_to_postgres(results: List[Dict], 
                     host: str = "localhost",
                     database: str = "your_database",
                     user: str = "your_user",
                     password: str = "your_password",
                     table: str = "alphavantage_daily"):
    """
    Save results to PostgreSQL database.
    
    Requires: pip install psycopg2-binary
    
    Expected table schema:
    CREATE TABLE alphavantage_daily (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(10) NOT NULL,
        date DATE NOT NULL,
        open NUMERIC(12,4),
        high NUMERIC(12,4),
        low NUMERIC(12,4),
        close NUMERIC(12,4),
        volume BIGINT,
        dollar_volume NUMERIC(18,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(symbol, date)
    );
    
    CREATE INDEX idx_alphavantage_daily_symbol ON alphavantage_daily(symbol);
    CREATE INDEX idx_alphavantage_daily_date ON alphavantage_daily(date);
    """
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError:
        logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        return False
    
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        
        # Upsert query (insert or update on conflict)
        query = f"""
            INSERT INTO {table} (symbol, date, open, high, low, close, volume, dollar_volume)
            VALUES %s
            ON CONFLICT (symbol, date) 
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                dollar_volume = EXCLUDED.dollar_volume
        """
        
        values = [
            (r['symbol'], r['date'], r['open'], r['high'], r['low'], 
             r['close'], r['volume'], r['dollar_volume'])
            for r in results
        ]
        
        execute_values(cursor, query, values)
        conn.commit()
        
        logger.info(f"Saved {len(results)} records to PostgreSQL table {table}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"PostgreSQL save failed: {e}")
        return False


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main execution function."""
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"Alpha Vantage Daily Pull - Started at {start_time}")
    logger.info(f"Tickers to fetch: {len(TICKER_LIST)}")
    logger.info("=" * 60)
    
    # Fetch all data
    results, failed = fetch_all_tickers(TICKER_LIST)
    
    # Generate filenames with today's date
    today_str = date.today().strftime("%Y%m%d")
    daily_csv = CSV_FILENAME.format(date=today_str)
    daily_json = f"alphavantage_daily_{today_str}.json"
    
    # Save outputs
    if results:
        # Save daily snapshot
        save_to_csv(results, daily_csv)
        save_to_json(results, daily_json)
        
        # Append to master file
        save_to_csv(results, MASTER_CSV, append=True)
        
        # Uncomment below to enable PostgreSQL saving
        # save_to_postgres(
        #     results,
        #     host="localhost",
        #     database="your_database",
        #     user="your_user", 
        #     password="your_password",
        #     table="alphavantage_daily"
        # )
    
    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Successful: {len(results)}/{len(TICKER_LIST)}")
    logger.info(f"Failed: {len(failed)}")
    if failed:
        logger.info(f"Failed tickers: {', '.join(failed)}")
    logger.info(f"Duration: {duration:.1f} seconds")
    logger.info(f"Data date: {results[0]['date'] if results else 'N/A'}")
    logger.info("=" * 60)
    
    # Print quick summary table
    if results:
        print("\n" + "=" * 80)
        print(f"{'Symbol':<8} {'Close':>10} {'Volume':>15} {'$ Volume':>18}")
        print("=" * 80)
        for r in sorted(results, key=lambda x: x['dollar_volume'], reverse=True):
            print(f"{r['symbol']:<8} ${r['close']:>9.2f} {r['volume']:>15,} ${r['dollar_volume']:>17,.0f}")
        print("=" * 80)
    
    return results, failed


if __name__ == "__main__":
    main()
