"""
API Server: FastAPI REST API on port 5000 + Scheduler
For LLM access to cryptocurrency and equity data
"""
import uvicorn
import subprocess
import threading
import sys
import os
from api import app

def run_scheduler():
    """Run the scheduler in a subprocess with output visible."""
    print("Starting scheduler process...", flush=True)
    try:
        process = subprocess.Popen(
            [sys.executable, "-u", "scheduler.py"],
            stdout=sys.stdout,
            stderr=sys.stderr,
            bufsize=1
        )
        process.wait()
    except Exception as e:
        print(f"Scheduler error: {e}", flush=True)

if __name__ == "__main__":
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print("Scheduler thread started, launching API server...", flush=True)
    
    uvicorn.run(app, host="0.0.0.0", port=5000)
