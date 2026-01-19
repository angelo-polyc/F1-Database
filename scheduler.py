import os
import time
import subprocess
import psycopg2
from datetime import datetime

ARTEMIS_INTERVAL = 24 * 60 * 60  # 24 hours in seconds
DEFILLAMA_INTERVAL = 60 * 60     # 1 hour in seconds

BACKFILL_THRESHOLD = {
    'artemis': 100000,    # Expect ~1.8M records when backfilled
    'defillama': 50000    # Expect ~500K records when backfilled
}

last_artemis_pull = 0
last_defillama_pull = 0

def get_record_count(source: str) -> int:
    """Check how many records exist for a given source."""
    try:
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM metrics WHERE source = %s", (source,))
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception as e:
        print(f"Error checking record count for {source}: {e}")
        return 0

def run_backfill(source: str):
    """Run backfill script for a source."""
    script = f"backfill_{source}.py"
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting {source} backfill...")
    try:
        result = subprocess.run(
            ["python", script],
            capture_output=False,
            timeout=7200  # 2 hour timeout for backfills
        )
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {source} backfill completed")
    except subprocess.TimeoutExpired:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {source} backfill timed out")
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {source} backfill error: {e}")

def check_and_backfill():
    """Check each source and run backfill if data is missing."""
    print("\n" + "=" * 60)
    print("CHECKING FOR BACKFILL REQUIREMENTS")
    print("=" * 60)
    
    for source, threshold in BACKFILL_THRESHOLD.items():
        count = get_record_count(source)
        print(f"  {source}: {count:,} records (threshold: {threshold:,})")
        
        if count < threshold:
            print(f"  -> Running backfill for {source} (below threshold)")
            run_backfill(source)
        else:
            print(f"  -> Skipping backfill for {source} (sufficient data)")
    
    print("=" * 60)

def run_pull(source: str):
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting {source} pull...")
    try:
        result = subprocess.run(
            ["python", "main.py", "pull", source],
            capture_output=False,
            timeout=300
        )
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {source} pull completed")
    except subprocess.TimeoutExpired:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {source} pull timed out")
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {source} pull error: {e}")

def main():
    global last_artemis_pull, last_defillama_pull
    
    print("=" * 60)
    print("DATA PIPELINE SCHEDULER")
    print("=" * 60)
    print(f"  Artemis interval: every {ARTEMIS_INTERVAL // 3600} hours")
    print(f"  DefiLlama interval: every {DEFILLAMA_INTERVAL // 3600} hour(s)")
    print("=" * 60)
    
    # Check and run backfills if needed (only runs if data is missing)
    check_and_backfill()
    
    # Run both pulls immediately on startup
    print("\nRunning initial pulls...")
    run_pull("artemis")
    last_artemis_pull = time.time()
    
    run_pull("defillama")
    last_defillama_pull = time.time()
    
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scheduler running. Waiting for next interval...")
    
    while True:
        current_time = time.time()
        
        # Check if it's time for Artemis pull (every 24h)
        if current_time - last_artemis_pull >= ARTEMIS_INTERVAL:
            run_pull("artemis")
            last_artemis_pull = current_time
        
        # Check if it's time for DefiLlama pull (every 1h)
        if current_time - last_defillama_pull >= DEFILLAMA_INTERVAL:
            run_pull("defillama")
            last_defillama_pull = current_time
        
        # Sleep for 1 minute, then check again
        time.sleep(60)

if __name__ == "__main__":
    main()
