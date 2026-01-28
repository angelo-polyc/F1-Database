"""
Smart Data Pipeline Scheduler

Features:
- Detects empty database and runs full 3-year backfills
- Detects gaps in data and fills them automatically
- Periodic gap checking during operation
- Self-healing: ensures no holes in time series data
"""
import time
import subprocess
import sys
import os
import fcntl
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from db.setup import get_connection, setup_database

LOCK_FILE = "/tmp/scheduler.lock"

ARTEMIS_HOUR = 0
ARTEMIS_MINUTE = 5
DEFILLAMA_MINUTE = 5
VELO_MINUTE = 5
COINGECKO_MINUTE = 5
ALPHAVANTAGE_MINUTE = 5

GAP_CHECK_INTERVAL_HOURS = 6

SOURCE_CONFIG = {
    'artemis': {
        'granularity': 'daily',
        'lookback_years': 3,
        'backfill_script': 'backfill_artemis.py',
    },
    'defillama': {
        'granularity': 'daily',
        'lookback_years': 3,
        'backfill_script': 'backfill_defillama.py',
    },
    'velo': {
        'granularity': 'hourly',
        'lookback_years': 3,
        'backfill_script': 'backfill_velo.py',
    },
    'coingecko': {
        'granularity': 'hourly',
        'lookback_years': 3,
        'backfill_script': 'backfill_coingecko.py',
    },
    'alphavantage': {
        'granularity': 'hourly',
        'lookback_years': 3,
        'backfill_script': 'backfill_alphavantage.py',
    },
}

last_artemis_date = None
last_defillama_hour = None
last_velo_hour = None
last_coingecko_hour = None
last_alphavantage_hour = None
last_gap_check = None


def log(msg: str):
    """Print timestamped log message."""
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    print(f"[{ts}] {msg}")


def get_source_status():
    """Get record counts and date ranges for each source."""
    conn = get_connection()
    cur = conn.cursor()
    
    status = {}
    for source in SOURCE_CONFIG.keys():
        cur.execute("""
            SELECT COUNT(*), MIN(pulled_at), MAX(pulled_at)
            FROM metrics WHERE source = %s
        """, (source,))
        row = cur.fetchone()
        if row:
            status[source] = {
                'count': row[0] or 0,
                'earliest': row[1],
                'latest': row[2],
            }
        else:
            status[source] = {'count': 0, 'earliest': None, 'latest': None}
    
    cur.close()
    conn.close()
    return status


def detect_gaps(source: str, days_to_check: int = 7):
    """
    Detect gaps in data for a source.
    Returns list of (start_date, end_date) tuples for missing periods.
    """
    config = SOURCE_CONFIG[source]
    granularity = config['granularity']
    
    conn = get_connection()
    cur = conn.cursor()
    
    if granularity == 'daily':
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
    else:
        cur.execute("""
            WITH hour_range AS (
                SELECT generate_series(
                    DATE_TRUNC('hour', NOW() - INTERVAL '%s days'),
                    DATE_TRUNC('hour', NOW() - INTERVAL '1 hour'),
                    INTERVAL '1 hour'
                ) AS expected_hour
            ),
            actual_hours AS (
                SELECT DISTINCT DATE_TRUNC('hour', pulled_at) as actual_hour
                FROM metrics
                WHERE source = %s
                AND pulled_at >= NOW() - INTERVAL '%s days'
            )
            SELECT expected_hour
            FROM hour_range
            LEFT JOIN actual_hours ON hour_range.expected_hour = actual_hours.actual_hour
            WHERE actual_hours.actual_hour IS NULL
            ORDER BY expected_hour
        """, (days_to_check, source, days_to_check))
    
    missing = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    
    if not missing:
        return []
    
    gaps = []
    gap_start = missing[0]
    gap_end = missing[0]
    
    for i in range(1, len(missing)):
        expected_next = gap_end + (timedelta(days=1) if granularity == 'daily' else timedelta(hours=1))
        if missing[i] == expected_next:
            gap_end = missing[i]
        else:
            gaps.append((gap_start, gap_end))
            gap_start = missing[i]
            gap_end = missing[i]
    
    gaps.append((gap_start, gap_end))
    return gaps


def run_backfill(source: str, start_date=None, end_date=None, days=None):
    """Run backfill script for a source with optional date range."""
    script = SOURCE_CONFIG[source]['backfill_script']
    cmd = ["python", script]
    
    if days:
        cmd.extend(["--days", str(days)])
    elif start_date and end_date:
        cmd.extend(["--start", start_date, "--end", end_date])
    
    log(f"Starting {source} backfill...")
    try:
        timeout = 50400 if source == 'velo' else 14400
        result = subprocess.run(cmd, capture_output=False, timeout=timeout)
        log(f"{source} backfill completed (exit code: {result.returncode})")
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        log(f"{source} backfill timed out")
        return False
    except Exception as e:
        log(f"{source} backfill error: {e}")
        return False


def run_pull(source: str):
    """Run a regular data pull for a source."""
    timeout_seconds = 900 if source == 'velo' else 600
    
    log(f"Starting {source} pull...")
    try:
        result = subprocess.run(
            ["python", "main.py", "pull", source],
            capture_output=False,
            timeout=timeout_seconds
        )
        if result.returncode != 0:
            log(f"{source} pull failed with exit code {result.returncode}")
        else:
            log(f"{source} pull completed")
    except subprocess.TimeoutExpired:
        log(f"{source} pull TIMED OUT")
    except Exception as e:
        log(f"{source} pull error: {e}")


def fill_gaps(source: str, days_to_check: int = 30):
    """Detect and fill gaps for a source."""
    gaps = detect_gaps(source, days_to_check=days_to_check)
    
    if not gaps:
        log(f"{source}: No gaps detected (checked last {days_to_check} days)")
        return True
    
    log(f"{source}: Found {len(gaps)} gap(s) in last {days_to_check} days")
    
    now = datetime.now(timezone.utc)
    
    for gap_start, gap_end in gaps:
        if hasattr(gap_start, 'strftime'):
            start_str = gap_start.strftime('%Y-%m-%d')
        else:
            start_str = str(gap_start)[:10]
        if hasattr(gap_end, 'strftime'):
            end_str = gap_end.strftime('%Y-%m-%d')
        else:
            end_str = str(gap_end)[:10]
        
        log(f"{source}: Filling gap from {start_str} to {end_str}")
        
        if hasattr(gap_start, 'date'):
            days_ago = (now.date() - gap_start.date()).days
        elif hasattr(gap_start, 'day'):
            days_ago = (now.date() - gap_start).days
        else:
            days_ago = 7
        
        days_to_backfill = max(days_ago + 3, 7)
        run_backfill(source, days=days_to_backfill)
    
    return True


def smart_startup():
    """
    Smart startup sequence:
    1. Check data status for each source
    2. Empty sources get full backfill
    3. Sources with data get gap detection and filling
    4. Run initial pulls to bring everything current
    """
    print("\n" + "=" * 60)
    print("SMART STARTUP - ANALYZING DATA STATUS")
    print("=" * 60)
    
    status = get_source_status()
    
    sources_needing_backfill = []
    sources_needing_gap_fill = []
    sources_ok = []
    
    now = datetime.now(timezone.utc)
    three_years_ago = now - timedelta(days=365 * 3)
    
    for source, info in status.items():
        config = SOURCE_CONFIG[source]
        
        if info['count'] == 0:
            print(f"  {source}: EMPTY - needs full backfill")
            sources_needing_backfill.append(source)
        elif info['earliest']:
            earliest = info['earliest']
            if earliest.tzinfo is None:
                earliest = earliest.replace(tzinfo=timezone.utc)
            if earliest > three_years_ago + timedelta(days=30):
                print(f"  {source}: {info['count']:,} records, but only from {info['earliest'].date()} - needs backfill")
                sources_needing_backfill.append(source)
            else:
                latest = info['latest']
                if latest and latest.tzinfo is None:
                    latest = latest.replace(tzinfo=timezone.utc)
                age_hours = (now - latest).total_seconds() / 3600 if latest else float('inf')
                threshold = 2 if config['granularity'] == 'hourly' else 26
                
                if age_hours > threshold:
                    print(f"  {source}: {info['count']:,} records, last update {age_hours:.1f}h ago - needs gap fill")
                    sources_needing_gap_fill.append(source)
                else:
                    print(f"  {source}: {info['count']:,} records, up to date")
                    sources_ok.append(source)
        else:
            latest = info['latest']
            if latest and latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            age_hours = (now - latest).total_seconds() / 3600 if latest else float('inf')
            threshold = 2 if config['granularity'] == 'hourly' else 26
            
            if age_hours > threshold:
                print(f"  {source}: {info['count']:,} records, last update {age_hours:.1f}h ago - needs gap fill")
                sources_needing_gap_fill.append(source)
            else:
                print(f"  {source}: {info['count']:,} records, up to date")
                sources_ok.append(source)
    
    if sources_needing_backfill:
        print("\n" + "=" * 60)
        print(f"RUNNING FULL BACKFILLS FOR {len(sources_needing_backfill)} SOURCE(S) SEQUENTIALLY")
        print("=" * 60)
        
        backfill_start = datetime.now(timezone.utc)
        
        # Run backfills in specific order: artemis, defillama, coingecko, alphavantage, velo
        backfill_order = ['artemis', 'defillama', 'coingecko', 'alphavantage', 'velo']
        ordered_sources = [s for s in backfill_order if s in sources_needing_backfill]
        # Add any sources not in the predefined order (shouldn't happen, but just in case)
        ordered_sources += [s for s in sources_needing_backfill if s not in backfill_order]
        
        for source in ordered_sources:
            log(f"Starting sequential backfill for {source}...")
            try:
                result = run_backfill(source)
                log(f"{source} backfill completed (success={result})")
            except Exception as e:
                log(f"{source} backfill error: {e}")
        
        backfill_end = datetime.now(timezone.utc)
        hours_elapsed = (backfill_end - backfill_start).total_seconds() / 3600
        
        if hours_elapsed >= 1:
            print("\n" + "-" * 40)
            log(f"Backfills took {hours_elapsed:.1f} hours - running catch-up")
            for source in ordered_sources:
                if SOURCE_CONFIG[source]['granularity'] == 'hourly':
                    run_backfill(source, days=1)
    
    all_sources_with_data = sources_needing_gap_fill + sources_ok
    
    if all_sources_with_data:
        print("\n" + "=" * 60)
        print(f"FULL HISTORY GAP SCAN FOR {len(all_sources_with_data)} SOURCE(S)")
        print("=" * 60)
        print("Scanning full 3-year history for any gaps...")
        
        for source in all_sources_with_data:
            fill_gaps(source, days_to_check=1095)
    
    print("\n" + "=" * 60)
    print("RUNNING INITIAL PULLS")
    print("=" * 60)
    
    for source in SOURCE_CONFIG.keys():
        run_pull(source)
    
    print("\nStartup complete - all sources initialized")


def periodic_gap_check():
    """Run periodic gap detection and filling for all sources."""
    global last_gap_check
    
    now = datetime.now(timezone.utc)
    
    if last_gap_check is None:
        last_gap_check = now
        return
    
    hours_since_check = (now - last_gap_check).total_seconds() / 3600
    
    if hours_since_check < GAP_CHECK_INTERVAL_HOURS:
        return
    
    log(f"Running periodic gap check (every {GAP_CHECK_INTERVAL_HOURS}h)")
    
    for source in SOURCE_CONFIG.keys():
        gaps = detect_gaps(source, days_to_check=7)
        if gaps:
            log(f"{source}: Found {len(gaps)} gap(s) - filling")
            fill_gaps(source)
    
    last_gap_check = now
    log("Periodic gap check complete")


def should_run_artemis(now_utc):
    global last_artemis_date
    if now_utc.hour == ARTEMIS_HOUR and now_utc.minute >= ARTEMIS_MINUTE:
        today = now_utc.date()
        if last_artemis_date != today:
            return True
    return False


def should_run_defillama(now_utc):
    global last_defillama_hour
    if now_utc.minute >= DEFILLAMA_MINUTE:
        current_hour = (now_utc.date(), now_utc.hour)
        if last_defillama_hour != current_hour:
            return True
    return False


def should_run_velo(now_utc):
    global last_velo_hour
    if now_utc.minute >= VELO_MINUTE:
        current_hour = (now_utc.date(), now_utc.hour)
        if last_velo_hour != current_hour:
            return True
    return False


def should_run_coingecko(now_utc):
    global last_coingecko_hour
    if now_utc.minute >= COINGECKO_MINUTE:
        current_hour = (now_utc.date(), now_utc.hour)
        if last_coingecko_hour != current_hour:
            return True
    return False


def should_run_alphavantage(now_utc):
    global last_alphavantage_hour
    if now_utc.minute >= ALPHAVANTAGE_MINUTE:
        current_hour = (now_utc.date(), now_utc.hour)
        if last_alphavantage_hour != current_hour:
            return True
    return False


def acquire_lock():
    try:
        if os.path.exists(LOCK_FILE):
            try:
                with open(LOCK_FILE, 'r') as f:
                    old_pid = int(f.read().strip())
                try:
                    os.kill(old_pid, 0)
                except OSError:
                    print(f"Stale lock file found (PID {old_pid} not running), removing...")
                    os.remove(LOCK_FILE)
            except (ValueError, IOError):
                print("Invalid lock file, removing...")
                os.remove(LOCK_FILE)
        
        lock_fd = open(LOCK_FILE, 'w')
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
        return lock_fd
    except (IOError, OSError):
        return None


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


def main():
    global last_artemis_date, last_defillama_hour, last_velo_hour
    global last_coingecko_hour, last_alphavantage_hour, last_gap_check
    
    lock_fd = acquire_lock()
    if lock_fd is None:
        print("ERROR: Another scheduler instance is already running!")
        print("Kill the other instance or remove /tmp/scheduler.lock")
        sys.exit(1)
    
    fresh_start = "--fresh" in sys.argv
    skip_smart_startup = "--no-startup" in sys.argv
    
    print("=" * 60)
    print("SMART DATA PIPELINE SCHEDULER")
    print("=" * 60)
    
    log("Setting up database (tables, entities, views)...")
    setup_database()
    
    print(f"\nSchedule:")
    print(f"  Artemis: daily at {ARTEMIS_HOUR:02d}:{ARTEMIS_MINUTE:02d} UTC")
    print(f"  DefiLlama: hourly at XX:{DEFILLAMA_MINUTE:02d} UTC")
    print(f"  Velo: hourly at XX:{VELO_MINUTE:02d} UTC")
    print(f"  CoinGecko: hourly at XX:{COINGECKO_MINUTE:02d} UTC")
    print(f"  AlphaVantage: hourly at XX:{ALPHAVANTAGE_MINUTE:02d} UTC")
    print(f"  Gap Check: every {GAP_CHECK_INTERVAL_HOURS} hours")
    
    if fresh_start:
        print("\n  Mode: FRESH START (clearing all data)")
        clear_all_data()
    
    if not skip_smart_startup:
        smart_startup()
    else:
        print("\nSkipping smart startup (--no-startup flag)")
    
    now_utc = datetime.now(timezone.utc)
    last_artemis_date = now_utc.date()
    last_defillama_hour = (now_utc.date(), now_utc.hour)
    last_velo_hour = (now_utc.date(), now_utc.hour)
    last_coingecko_hour = (now_utc.date(), now_utc.hour)
    last_alphavantage_hour = (now_utc.date(), now_utc.hour)
    last_gap_check = now_utc
    
    print("\n" + "=" * 60)
    log("Scheduler running - entering main loop")
    print("=" * 60)
    
    while True:
        now_utc = datetime.now(timezone.utc)
        
        if should_run_artemis(now_utc):
            run_pull("artemis")
            last_artemis_date = now_utc.date()
        
        if should_run_defillama(now_utc):
            run_pull("defillama")
            last_defillama_hour = (now_utc.date(), now_utc.hour)
        
        if should_run_velo(now_utc):
            run_pull("velo")
            last_velo_hour = (now_utc.date(), now_utc.hour)
        
        if should_run_coingecko(now_utc):
            run_pull("coingecko")
            last_coingecko_hour = (now_utc.date(), now_utc.hour)
        
        if should_run_alphavantage(now_utc):
            run_pull("alphavantage")
            last_alphavantage_hour = (now_utc.date(), now_utc.hour)
        
        periodic_gap_check()
        
        time.sleep(30)


if __name__ == "__main__":
    main()
