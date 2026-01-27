"""
Production Launcher: Runs both API server and scheduler
Designed for Replit deployment with proper port handling
"""
import multiprocessing
import subprocess
import sys
import os

def run_scheduler():
    """Run scheduler as separate process"""
    os.execvp(sys.executable, [sys.executable, "scheduler.py"])

def run_server():
    """Run API server"""
    import uvicorn
    from api import app
    uvicorn.run(app, host="0.0.0.0", port=5000)

if __name__ == "__main__":
    print("=" * 60)
    print("PRODUCTION LAUNCHER")
    print("=" * 60)
    
    scheduler_process = multiprocessing.Process(target=run_scheduler, daemon=False)
    scheduler_process.start()
    print(f"Scheduler started (PID: {scheduler_process.pid})")
    
    print("Starting API server on port 5000...")
    run_server()
