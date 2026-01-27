#!/usr/bin/env python3
"""Test Velo API rate limits with configurable workers and delay."""

import os
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.auth import HTTPBasicAuth
import requests

API_KEY = os.environ.get('VELO_API_KEY')
BASE_URL = "https://api.velo.xyz/api/v1"

def test_request(request_num, delay):
    """Make a single test request."""
    start = time.time()
    try:
        time.sleep(delay)
        url = (
            f"{BASE_URL}/rows?"
            f"type=futures&"
            f"exchanges=binance-futures&"
            f"coins=BTC&"
            f"columns=close_price,funding_rate&"
            f"begin=1704067200000&"
            f"end=1704153600000&"
            f"resolution=15m"
        )
        response = requests.get(
            url,
            auth=HTTPBasicAuth("api", API_KEY),
            timeout=30
        )
        elapsed = time.time() - start
        status = response.status_code
        if status == 429:
            return (request_num, 'RATE_LIMITED', elapsed)
        elif status == 200:
            return (request_num, 'OK', elapsed)
        else:
            return (request_num, f'ERROR_{status}', elapsed)
    except Exception as e:
        return (request_num, f'EXCEPTION: {str(e)[:30]}', time.time() - start)

def run_test(workers, delay, num_requests):
    """Run rate limit test."""
    print(f"\n{'='*60}")
    print(f"VELO RATE LIMIT TEST")
    print(f"{'='*60}")
    print(f"Workers: {workers}")
    print(f"Delay: {delay}s per request")
    print(f"Target rate: {workers / delay:.1f} req/s")
    print(f"Requests: {num_requests}")
    print(f"{'='*60}\n")
    
    results = {'OK': 0, 'RATE_LIMITED': 0, 'ERROR': 0}
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(test_request, i, delay): i for i in range(num_requests)}
        
        for future in as_completed(futures):
            req_num, status, elapsed = future.result()
            if 'OK' in status:
                results['OK'] += 1
                print(f"  [{req_num:3d}] OK ({elapsed:.2f}s)")
            elif 'RATE' in status:
                results['RATE_LIMITED'] += 1
                print(f"  [{req_num:3d}] RATE LIMITED ({elapsed:.2f}s)")
            else:
                results['ERROR'] += 1
                print(f"  [{req_num:3d}] {status} ({elapsed:.2f}s)")
    
    total_time = time.time() - start_time
    actual_rate = num_requests / total_time
    
    print(f"\n{'='*60}")
    print(f"RESULTS")
    print(f"{'='*60}")
    print(f"OK: {results['OK']}/{num_requests}")
    print(f"Rate Limited: {results['RATE_LIMITED']}/{num_requests}")
    print(f"Errors: {results['ERROR']}/{num_requests}")
    print(f"Total time: {total_time:.1f}s")
    print(f"Actual rate: {actual_rate:.2f} req/s")
    print(f"{'='*60}")
    
    if results['RATE_LIMITED'] > 0:
        print("\n⚠️  RATE LIMITING DETECTED - reduce workers or increase delay")
    else:
        print("\n✅ No rate limiting detected")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, default=3)
    parser.add_argument('--delay', type=float, default=0.4)
    parser.add_argument('--requests', type=int, default=20)
    args = parser.parse_args()
    
    if not API_KEY:
        print("Error: VELO_API_KEY not set")
        exit(1)
    
    run_test(args.workers, args.delay, args.requests)
