import time
import subprocess
import sys
from datetime import datetime, timezone
from db.setup import get_connection, setup_database

ARTEMIS_HOUR = 0
ARTEMIS_MINUTE = 5

DEFILLAMA_MINUTE = 5
VELO_MINUTE = 5  # Velo is real-time, pull alongside DefiLlama

last_artemis_date = None
last_defillama_hour = None
last_velo_hour = None

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
    print(f"\n[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] Starting {source} pull...")
    try:
        result = subprocess.run(
            ["python", "main.py", "pull", source],
            capture_output=False,
            timeout=300
        )
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {source} pull completed")
    except subprocess.TimeoutExpired:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {source} pull timed out")
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

def main():
    global last_artemis_date, last_defillama_hour, last_velo_hour
    
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
    
    print(f"\n[{now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}] Scheduler running.")
    print(f"  Next Artemis pull: {ARTEMIS_HOUR:02d}:{ARTEMIS_MINUTE:02d} UTC tomorrow")
    print(f"  Next DefiLlama pull: XX:{DEFILLAMA_MINUTE:02d} UTC (next hour)")
    print(f"  Next Velo pull: XX:{VELO_MINUTE:02d} UTC (next hour)")
    
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
        
        time.sleep(30)

if __name__ == "__main__":
    main()
