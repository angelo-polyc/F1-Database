"""
API Server: FastAPI REST API on port 5000 + Scheduler
For LLM access to cryptocurrency and equity data
"""
import uvicorn
import threading
import sys
import os
from api import app

def run_scheduler():
    """Run the scheduler in the same process as a background thread."""
    print("Starting scheduler in background thread...", flush=True)
    try:
        from scheduler import main as scheduler_main
        scheduler_main()
    except Exception as e:
        print(f"Scheduler error: {e}", flush=True)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Check for API-only mode (no scheduler)
    api_only = os.environ.get("API_ONLY", "").lower() in ("1", "true", "yes")
    
    if api_only:
        print("API-ONLY MODE: Scheduler disabled", flush=True)
    else:
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        print("Scheduler thread started, launching API server...", flush=True)
    
    uvicorn.run(app, host="0.0.0.0", port=5000)
