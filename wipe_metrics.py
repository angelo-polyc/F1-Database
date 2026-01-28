#!/usr/bin/env python3
"""
Wipe metrics table in small batches to avoid timeouts.
Run this in the shell: python wipe_metrics.py
Leave it running until it finishes.
"""
import psycopg2
import os
import time

conn = psycopg2.connect(os.environ['DATABASE_URL'])
conn.autocommit = True
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM metrics")
total = cur.fetchone()[0]
print(f"Starting rows: {total:,}")

deleted_total = 0
batch_size = 50000  # Small batches to avoid timeout

while True:
    start = time.time()
    cur.execute(f"DELETE FROM metrics WHERE ctid IN (SELECT ctid FROM metrics LIMIT {batch_size})")
    deleted = cur.rowcount
    elapsed = time.time() - start
    
    if deleted == 0:
        print("Done! Table is empty.")
        break
    
    deleted_total += deleted
    remaining = total - deleted_total
    print(f"Deleted {deleted:,} rows in {elapsed:.1f}s | Total deleted: {deleted_total:,} | Remaining: ~{remaining:,}")

cur.close()
conn.close()
print("Wipe complete!")
