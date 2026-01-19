import time
import subprocess
import threading
from datetime import datetime

ARTEMIS_INTERVAL = 24 * 60 * 60  # 24 hours in seconds
DEFILLAMA_INTERVAL = 60 * 60     # 1 hour in seconds

last_artemis_pull = 0
last_defillama_pull = 0

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
    
    # Run both immediately on startup
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
        
        # Calculate next pull times for status
        next_artemis = last_artemis_pull + ARTEMIS_INTERVAL - current_time
        next_defillama = last_defillama_pull + DEFILLAMA_INTERVAL - current_time
        
        # Sleep for 1 minute, then check again
        time.sleep(60)

if __name__ == "__main__":
    main()
