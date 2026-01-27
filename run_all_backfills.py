#!/usr/bin/env python3
"""
Run All Backfill Scripts

Convenience script to run all 5 backfill scripts with consistent date ranges.
Designed to fill historical data BEFORE the first production pull.

Usage:
    python run_all_backfills.py                    # 3 years ending Jan 25, 2026
    python run_all_backfills.py --dry-run          # Preview without inserting
    python run_all_backfills.py --source artemis   # Run only one source

Production First Pull Timestamps (for reference):
    Artemis:      2026-01-26T19:56:29.035Z
    DefiLlama:    2026-01-26T20:06:00.267Z
    Velo:         2026-01-26T20:10:20.376Z
    CoinGecko:    2026-01-26T20:11:45.501Z
    AlphaVantage: 2026-01-26T20:12:42.523Z
"""

import subprocess
import sys
import time
import argparse
from datetime import datetime, timezone

# Backfill dates: 3 years of history ending before first prod pull
START_DATE = "2023-01-26"
END_DATE = "2026-01-25"

# Order of execution (fastest to slowest)
SOURCES = [
    ("alphavantage", "backfill_alphavantage.py"),
    ("coingecko", "backfill_coingecko.py"),
    ("artemis", "backfill_artemis.py"),
    ("defillama", "backfill_defillama.py"),
    ("velo", "backfill_velo.py"),
]


def run_backfill(name: str, script: str, start_date: str, end_date: str, dry_run: bool = False) -> bool:
    """Run a single backfill script."""
    print("\n" + "=" * 70)
    print(f"RUNNING: {name.upper()} BACKFILL")
    print("=" * 70)
    
    cmd = ["python", script, "--start-date", start_date, "--end-date", end_date]
    if dry_run:
        cmd.append("--dry-run")
    
    print(f"Command: {' '.join(cmd)}")
    print()
    
    start_time = time.time()
    
    try:
        result = subprocess.run(cmd, timeout=14400)  # 4 hour timeout per script
        elapsed = time.time() - start_time
        
        if result.returncode == 0:
            print(f"\n[{name}] Completed in {elapsed:.0f}s ({elapsed/60:.1f} min)")
            return True
        else:
            print(f"\n[{name}] FAILED with exit code {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"\n[{name}] TIMED OUT after 4 hours")
        return False
    except Exception as e:
        print(f"\n[{name}] ERROR: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Run all backfill scripts with consistent date ranges',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without inserting data')
    parser.add_argument('--source', type=str, choices=[s[0] for s in SOURCES],
                        help='Run only a specific source')
    parser.add_argument('--start-date', type=str, default=START_DATE,
                        help=f'Override start date (default: {START_DATE})')
    parser.add_argument('--end-date', type=str, default=END_DATE,
                        help=f'Override end date (default: {END_DATE})')
    
    args = parser.parse_args()
    
    start_date = args.start_date
    end_date = args.end_date
    
    print("=" * 70)
    print("ALL SOURCES BACKFILL")
    print("=" * 70)
    print(f"Date range: {start_date} to {end_date}")
    print(f"Dry run: {args.dry_run}")
    print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    sources_to_run = SOURCES
    if args.source:
        sources_to_run = [(n, s) for n, s in SOURCES if n == args.source]
    
    print(f"\nSources to process: {', '.join(s[0] for s in sources_to_run)}")
    
    results = {}
    total_start = time.time()
    
    for name, script in sources_to_run:
        success = run_backfill(name, script, start_date, end_date, args.dry_run)
        results[name] = success
    
    total_elapsed = time.time() - total_start
    
    print("\n" + "=" * 70)
    print("BACKFILL SUMMARY")
    print("=" * 70)
    print(f"Total time: {total_elapsed:.0f}s ({total_elapsed/60:.1f} min)")
    print()
    
    for name, success in results.items():
        status = "SUCCESS" if success else "FAILED"
        print(f"  {name}: {status}")
    
    failed = sum(1 for s in results.values() if not s)
    if failed > 0:
        print(f"\n{failed} source(s) failed")
        return 1
    else:
        print("\nAll backfills completed successfully!")
        return 0


if __name__ == "__main__":
    exit(main())
