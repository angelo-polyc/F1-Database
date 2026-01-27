#!/usr/bin/env python3
"""
Test gap detection and initial pull for Artemis and DefiLlama only.
Does NOT affect any ongoing backfills.
"""
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from db.setup import get_connection

SOURCES_TO_TEST = ['artemis', 'defillama']

def log(msg: str):
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    print(f"[{ts}] {msg}")

def detect_gaps(source: str, days_to_check: int = 30):
    """Detect gaps in daily data for a source."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        WITH date_range AS (
            SELECT generate_series(
                CURRENT_DATE - INTERVAL '%s days',
                CURRENT_DATE - INTERVAL '1 day',
                INTERVAL '1 day'
            )::date AS expected_date
        ),
        actual_dates AS (
            SELECT DISTINCT DATE(pulled_at) as actual_date
            FROM metrics
            WHERE source = %s
            AND pulled_at >= CURRENT_DATE - INTERVAL '%s days'
        )
        SELECT expected_date
        FROM date_range
        LEFT JOIN actual_dates ON date_range.expected_date = actual_dates.actual_date
        WHERE actual_dates.actual_date IS NULL
        ORDER BY expected_date
    """, (days_to_check, source, days_to_check))
    
    missing = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    
    return missing

def get_source_status(source: str):
    """Get record count and date range for a source."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT COUNT(*), MIN(pulled_at), MAX(pulled_at), CURRENT_DATE
        FROM metrics WHERE source = %s
    """, (source,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    
    return {
        'count': row[0] or 0,
        'earliest': row[1],
        'latest': row[2],
        'today': row[3]
    }

def run_pull(source: str):
    """Run a regular data pull for a source."""
    log(f"Running {source} pull...")
    try:
        result = subprocess.run(
            ["python", "main.py", "pull", source],
            capture_output=False,
            timeout=600
        )
        if result.returncode == 0:
            log(f"{source} pull completed successfully")
        else:
            log(f"{source} pull failed with exit code {result.returncode}")
        return result.returncode == 0
    except Exception as e:
        log(f"{source} pull error: {e}")
        return False

def main():
    print("\n" + "=" * 60)
    print("GAP DETECTION TEST - Artemis & DefiLlama ONLY")
    print("=" * 60)
    
    for source in SOURCES_TO_TEST:
        print(f"\n--- {source.upper()} ---")
        
        status = get_source_status(source)
        print(f"  Records: {status['count']:,}")
        print(f"  Earliest: {status['earliest']}")
        print(f"  Latest: {status['latest']}")
        print(f"  Today: {status['today']}")
        
        if status['latest']:
            gap_to_today = (status['today'] - status['latest'].date()).days
            print(f"  Gap to today: {gap_to_today} day(s)")
        
        gaps = detect_gaps(source, days_to_check=30)
        if gaps:
            print(f"  Missing dates in last 30 days: {len(gaps)}")
            for date in gaps[:5]:
                print(f"    - {date}")
            if len(gaps) > 5:
                print(f"    ... and {len(gaps) - 5} more")
        else:
            print(f"  No gaps in last 30 days")
    
    print("\n" + "=" * 60)
    print("RUNNING INITIAL PULLS TO FILL TODAY'S GAP")
    print("=" * 60)
    
    for source in SOURCES_TO_TEST:
        run_pull(source)
    
    print("\n" + "=" * 60)
    print("VERIFICATION - CHECKING FOR REMAINING GAPS")
    print("=" * 60)
    
    for source in SOURCES_TO_TEST:
        status = get_source_status(source)
        print(f"\n--- {source.upper()} ---")
        print(f"  Records: {status['count']:,}")
        print(f"  Latest: {status['latest']}")
        
        if status['latest']:
            gap_to_today = (status['today'] - status['latest'].date()).days
            print(f"  Gap to today: {gap_to_today} day(s)")
            
            if gap_to_today == 0:
                print(f"  STATUS: UP TO DATE")
            else:
                print(f"  STATUS: STILL HAS GAP")
    
    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()
