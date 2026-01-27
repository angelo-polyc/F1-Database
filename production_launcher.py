"""
Production Launcher: Runs both API server and scheduler
Designed for Replit deployment with proper port handling
"""
import subprocess
import threading
import os
import sys

def run_scheduler():
    """Run scheduler in background thread"""
    subprocess.run([sys.executable, "scheduler.py"])

if __name__ == "__main__":
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print("Scheduler started in background")
    
    import uvicorn
    from api import app
    uvicorn.run(app, host="0.0.0.0", port=5000)
