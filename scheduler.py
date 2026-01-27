import time
import subprocess
import sys
import os
import fcntl
from datetime import datetime, timezone
from db.setup import get_connection, setup_database

LOCK_FILE = "/tmp/scheduler.lock"

ARTEMIS_HOUR = 0
ARTEMIS_MINUTE = 5

DEFILLAMA_MINUTE = 5
VELO_MINUTE = 5  # Velo is real-time, pull alongside DefiLlama
COINGECKO_MINUTE = 5  # CoinGecko hourly, same schedule as DefiLlama/Velo
ALPHAVANTAGE_MINUTE = 5  # Alpha Vantage hourly (real-time API key)

last_artemis_date = None
last_defillama_hour = None
last_velo_hour = None
last_coingecko_hour = None
last_alphavantage_hour = None

def clear_all_data():
    """Clear all metrics and pulls data for a fresh start."""
    print("\n" + "=" * 60)
    print("CLEARING ALL DATA FOR FRESH START")
    print("=" * 60)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE metrics, pulls RESTART IDENTITY CASCADE;")
    conn.commit()
    cur.close()
    conn.close()
    print("All data cleared successfully.")

def run_backfill(source: str):
    """Run backfill script for a source."""
    script = f"backfill_{source}.py"
    print(f"\n[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] Starting {source} backfill...")
    try:
        result = subprocess.run(
            ["python", script],
            capture_output=False,
            timeout=14400
        )
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {source} backfill completed")
    except subprocess.TimeoutExpired:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {source} backfill timed out")
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {source} backfill error: {e}")

def run_pull(source: str):
    # Velo with large gaps needs more time, normal operation is ~2 min
    timeout_seconds = 900 if source == 'velo' else 600
    timeout_min = timeout_seconds // 60
    
    print(f"\n[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] Starting {source} pull...")
    try:
        result = subprocess.run(
            ["python", "main.py", "pull", source],
            capture_output=False,
            timeout=timeout_seconds
        )
        if result.returncode != 0:
            print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {source} pull failed with exit code {result.returncode}")
        else:
            print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {source} pull completed")
    except subprocess.TimeoutExpired:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {source} pull TIMED OUT after {timeout_min} minutes")
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {source} pull error: {e}")

def should_run_artemis(now_utc):
    """Check if Artemis should run: daily at 00:05 UTC."""
    global last_artemis_date
    
    if now_utc.hour == ARTEMIS_HOUR and now_utc.minute >= ARTEMIS_MINUTE:
        today = now_utc.date()
        if last_artemis_date != today:
            return True
    return False

def should_run_defillama(now_utc):
    """Check if DefiLlama should run: at minute 5 of every hour."""
    global last_defillama_hour
    
    if now_utc.minute >= DEFILLAMA_MINUTE:
        current_hour = (now_utc.date(), now_utc.hour)
        if last_defillama_hour != current_hour:
            return True
    return False

def should_run_velo(now_utc):
    """Check if Velo should run: at minute 5 of every hour (same as DefiLlama)."""
    global last_velo_hour
    
    if now_utc.minute >= VELO_MINUTE:
        current_hour = (now_utc.date(), now_utc.hour)
        if last_velo_hour != current_hour:
            return True
    return False

def should_run_coingecko(now_utc):
    """Check if CoinGecko should run: at minute 5 of every hour (same as DefiLlama/Velo)."""
    global last_coingecko_hour
    
    if now_utc.minute >= COINGECKO_MINUTE:
        current_hour = (now_utc.date(), now_utc.hour)
        if last_coingecko_hour != current_hour:
            return True
    return False

def should_run_alphavantage(now_utc):
    """Check if Alpha Vantage should run: at minute 5 of every hour (real-time data)."""
    global last_alphavantage_hour
    
    if now_utc.minute >= ALPHAVANTAGE_MINUTE:
        current_hour = (now_utc.date(), now_utc.hour)
        if last_alphavantage_hour != current_hour:
            return True
    return False

def acquire_lock():
    """Acquire exclusive lock to prevent multiple scheduler instances."""
    try:
        lock_fd = open(LOCK_FILE, 'w')
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
        return lock_fd
    except (IOError, OSError):
        return None

def get_last_pull_time(source_name: str):
    """Get the last successful pull time for a source from the database."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT pulled_at FROM pulls 
        WHERE source_name = %s AND status = 'success'
        ORDER BY pulled_at DESC LIMIT 1
    """, (source_name,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None

def main():
    global last_artemis_date, last_defillama_hour, last_velo_hour, last_coingecko_hour, last_alphavantage_hour
    
    lock_fd = acquire_lock()
    if lock_fd is None:
        print("ERROR: Another scheduler instance is already running!")
        print("Kill the other instance or remove /tmp/scheduler.lock")
        sys.exit(1)
    
    fresh_start = "--fresh" in sys.argv
    skip_backfill = "--no-backfill" in sys.argv
    
    print("=" * 60)
    print("DATA PIPELINE SCHEDULER")
    print("=" * 60)
    
    print("Setting up database (tables, entities, views)...")
    setup_database()
    
    print(f"\n  Artemis: daily at {ARTEMIS_HOUR:02d}:{ARTEMIS_MINUTE:02d} UTC")
    print(f"  DefiLlama: hourly at XX:{DEFILLAMA_MINUTE:02d} UTC")
    print(f"  Velo: hourly at XX:{VELO_MINUTE:02d} UTC")
    print(f"  CoinGecko: hourly at XX:{COINGECKO_MINUTE:02d} UTC")
    print(f"  AlphaVantage: hourly at XX:{ALPHAVANTAGE_MINUTE:02d} UTC (real-time API)")
    if fresh_start:
        print("  Mode: FRESH START (clearing all data)")
    if skip_backfill:
        print("  Mode: NO BACKFILL (skipping historical data)")
    print("=" * 60)
    
    if fresh_start:
        clear_all_data()
    
    if not skip_backfill:
        print("\n" + "=" * 60)
        print("RUNNING FULL BACKFILLS")
        print("=" * 60)
        run_backfill("defillama")
        run_backfill("artemis")
        run_backfill("velo")
    else:
        print("\nSkipping backfills (--no-backfill flag set)")
    
    print("\nRunning initial pulls...")
    now_utc = datetime.now(timezone.utc)
    
    run_pull("artemis")
    last_artemis_date = now_utc.date()
    
    run_pull("defillama")
    last_defillama_hour = (now_utc.date(), now_utc.hour)
    
    run_pull("velo")
    last_velo_hour = (now_utc.date(), now_utc.hour)
    
    run_pull("coingecko")
    last_coingecko_hour = (now_utc.date(), now_utc.hour)
    
    run_pull("alphavantage")
    last_alphavantage_hour = (now_utc.date(), now_utc.hour)
    
    print(f"\n[{now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}] Scheduler running.")
    print(f"  Next Artemis pull: {ARTEMIS_HOUR:02d}:{ARTEMIS_MINUTE:02d} UTC tomorrow")
    print(f"  Next DefiLlama pull: XX:{DEFILLAMA_MINUTE:02d} UTC (next hour)")
    print(f"  Next Velo pull: XX:{VELO_MINUTE:02d} UTC (next hour)")
    print(f"  Next CoinGecko pull: XX:{COINGECKO_MINUTE:02d} UTC (next hour)")
    print(f"  Next AlphaVantage pull: XX:{ALPHAVANTAGE_MINUTE:02d} UTC (next hour)")
    
    while True:
        now_utc = datetime.now(timezone.utc)
        
        if should_run_artemis(now_utc):
            run_pull("artemis")
            last_artemis_date = now_utc.date()
            print(f"[{now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}] Next Artemis pull: {ARTEMIS_HOUR:02d}:{ARTEMIS_MINUTE:02d} UTC tomorrow")
        
        if should_run_defillama(now_utc):
            run_pull("defillama")
            last_defillama_hour = (now_utc.date(), now_utc.hour)
            print(f"[{now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}] Next DefiLlama pull: XX:{DEFILLAMA_MINUTE:02d} UTC (next hour)")
        
        if should_run_velo(now_utc):
            run_pull("velo")
            last_velo_hour = (now_utc.date(), now_utc.hour)
            print(f"[{now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}] Next Velo pull: XX:{VELO_MINUTE:02d} UTC (next hour)")
        
        if should_run_coingecko(now_utc):
            run_pull("coingecko")
            last_coingecko_hour = (now_utc.date(), now_utc.hour)
            print(f"[{now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}] Next CoinGecko pull: XX:{COINGECKO_MINUTE:02d} UTC (next hour)")
        
        if should_run_alphavantage(now_utc):
            run_pull("alphavantage")
            last_alphavantage_hour = (now_utc.date(), now_utc.hour)
            print(f"[{now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}] Next AlphaVantage pull: XX:{ALPHAVANTAGE_MINUTE:02d} UTC (next hour)")
        
        time.sleep(30)

if __name__ == "__main__":
    main()
