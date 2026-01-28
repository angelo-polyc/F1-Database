#!/usr/bin/env python3
"""
Wipe metrics table in small batches to avoid timeouts.
Run this in the shell: python wipe_metrics.py
Leave it running until it finishes.
"""
import psycopg2
import os
import time

print("Connecting to database...")
conn = psycopg2.connect(os.environ['DATABASE_URL'])
conn.autocommit = True
cur = conn.cursor()
print("Connected! Starting deletion...")

deleted_total = 0
batch_size = 10000  # Smaller batches for faster feedback

while True:
    start = time.time()
    cur.execute(f"DELETE FROM metrics WHERE ctid IN (SELECT ctid FROM metrics LIMIT {batch_size})")
    deleted = cur.rowcount
    elapsed = time.time() - start
    
    if deleted == 0:
        print("\nDone! Table is empty.")
        break
    
    deleted_total += deleted
    print(f"Deleted {deleted:,} rows in {elapsed:.1f}s | Total deleted: {deleted_total:,}")

cur.close()
conn.close()
print("Wipe complete!")
