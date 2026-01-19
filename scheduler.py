import time
import subprocess
from datetime import datetime

ARTEMIS_INTERVAL = 24 * 60 * 60  # 24 hours in seconds
DEFILLAMA_INTERVAL = 60 * 60     # 1 hour in seconds

last_artemis_pull = 0
last_defillama_pull = 0

def run_backfill(source: str):
    """Run backfill script for a source."""
    script = f"backfill_{source}.py"
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting {source} backfill...")
    try:
        result = subprocess.run(
            ["python", script],
            capture_output=False,
            timeout=14400  # 4 hour timeout for backfills
        )
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {source} backfill completed")
    except subprocess.TimeoutExpired:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {source} backfill timed out")
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {source} backfill error: {e}")

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
    
    # Run full backfills on startup
    print("\n" + "=" * 60)
    print("RUNNING FULL BACKFILLS")
    print("=" * 60)
    run_backfill("defillama")
    run_backfill("artemis")
    
    # Run both pulls after backfill
    print("\nRunning initial pulls...")
    run_pull("artemis")
    last_artemis_pull = time.time()
    
    run_pull("defillama")
    last_defillama_pull = time.time()
    
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scheduler running. Waiting for next interval...")
    
    while True:
        current_time = time.time()
        
        if current_time - last_artemis_pull >= ARTEMIS_INTERVAL:
            run_pull("artemis")
            last_artemis_pull = current_time
        
        if current_time - last_defillama_pull >= DEFILLAMA_INTERVAL:
            run_pull("defillama")
            last_defillama_pull = current_time
        
        time.sleep(60)

if __name__ == "__main__":
    main()
