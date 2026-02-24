#!/usr/bin/env python3
"""Scan Redis idempotency keys and calculate latency statistics.

Calculates end-to-end latency (created_at → persisted_at) for event_ids
stored in Redis with format: idempotency:{event_id}
"""

import argparse
import json
import sys
from datetime import datetime
from typing import List, Optional

import redis


def parse_iso_timestamp(ts_str: str) -> Optional[datetime]:
    """Parse ISO 8601 timestamp string to datetime."""
    try:
        # Handle both with and without microseconds
        if '.' in ts_str:
            return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        else:
            return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None


def calculate_latency_ms(created_at: str, persisted_at: str) -> Optional[float]:
    """Calculate latency in milliseconds between created_at and persisted_at."""
    created = parse_iso_timestamp(created_at)
    persisted = parse_iso_timestamp(persisted_at)
    
    if created and persisted:
        delta = persisted - created
        return delta.total_seconds() * 1000.0
    return None


def calculate_percentile(values: List[float], percentile: float) -> float:
    """Calculate percentile from sorted list of values."""
    if not values:
        return 0.0
    
    sorted_values = sorted(values)
    index = int(len(sorted_values) * percentile) - 1
    if index < 0:
        index = 0
    
    return sorted_values[index]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scan Redis idempotency keys and calculate latency statistics"
    )
    parser.add_argument(
        "--limit", 
        type=int, 
        default=1000, 
        help="Number of records to scan (default: 1000, 0 = all)"
    )
    parser.add_argument(
        "--host", 
        default="localhost", 
        help="Redis host (default: localhost)"
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=6379, 
        help="Redis port (default: 6379)"
    )
    parser.add_argument(
        "--db", 
        type=int, 
        default=0, 
        help="Redis database number (default: 0)"
    )
    args = parser.parse_args()

    try:
        r = redis.Redis(host=args.host, port=args.port, db=args.db, decode_responses=True)
        r.ping()
        print(f"Connected to Redis at {args.host}:{args.port}/{args.db}\n")
    except redis.ConnectionError as e:
        print(f"❌ Failed to connect to Redis: {e}", file=sys.stderr)
        return 1

    # Scan for idempotency keys
    pattern = "idempotency:*"
    print(f"Scanning for keys matching '{pattern}'...")
    
    cursor = 0
    keys = []
    while True:
        cursor, batch = r.scan(cursor, match=pattern, count=100)
        keys.extend(batch)
        if cursor == 0:
            break
        if args.limit > 0 and len(keys) >= args.limit:
            keys = keys[:args.limit]
            break
    
    if not keys:
        print(f"No keys found matching pattern '{pattern}'")
        return 0
    
    print(f"Found {len(keys)} idempotency keys\n")
    
    # Process keys and calculate latencies
    latencies = []
    records_with_persisted_at = 0
    records_without_persisted_at = 0
    parse_errors = 0
    
    for key in keys:
        try:
            value_str = r.get(key)
            if not value_str:
                continue
                
            value = json.loads(value_str)
            
            # Check if both timestamps exist
            if 'created_at' in value and 'persisted_at' in value:
                latency = calculate_latency_ms(value['created_at'], value['persisted_at'])
                if latency is not None:
                    latencies.append(latency)
                    records_with_persisted_at += 1
                else:
                    parse_errors += 1
            else:
                records_without_persisted_at += 1
                
        except (json.JSONDecodeError, KeyError) as e:
            parse_errors += 1
            continue
    
    # Display results
    print("=" * 60)
    print("LATENCY STATISTICS (created_at → persisted_at)")
    print("=" * 60)
    print(f"Total keys scanned:              {len(keys)}")
    print(f"Records with persisted_at:       {records_with_persisted_at}")
    print(f"Records without persisted_at:    {records_without_persisted_at}")
    if parse_errors > 0:
        print(f"Parse errors:                    {parse_errors}")
    print()
    
    if not latencies:
        print("⚠️  No records with both created_at and persisted_at timestamps")
        return 0
    
    # Calculate statistics
    avg_latency = sum(latencies) / len(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)
    p95_latency = calculate_percentile(latencies, 0.95)
    p99_latency = calculate_percentile(latencies, 0.99)
    
    print(f"avg_latency_ms:  {avg_latency:>10.2f}")
    print(f"min_latency_ms:  {min_latency:>10.2f}")
    print(f"max_latency_ms:  {max_latency:>10.2f}")
    print(f"p95_latency_ms:  {p95_latency:>10.2f}")
    print(f"p99_latency_ms:  {p99_latency:>10.2f}")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
