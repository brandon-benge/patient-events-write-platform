#!/usr/bin/env python3
"""Fetch and display data from Redis by key."""

import argparse
import json
import sys

import redis


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch data from Redis by key or pattern")
    parser.add_argument("key", help="Redis key to fetch (or partial key to search)")
    parser.add_argument("--host", default="localhost", help="Redis host (default: localhost)")
    parser.add_argument("--port", type=int, default=6379, help="Redis port (default: 6379)")
    parser.add_argument("--db", type=int, default=0, help="Redis database number (default: 0)")
    parser.add_argument("--list", action="store_true", help="List all keys matching pattern (* wildcard)")
    args = parser.parse_args()

    try:
        r = redis.Redis(host=args.host, port=args.port, db=args.db, decode_responses=True)
        r.ping()  # Verify connection
        print(f"Connected to Redis at {args.host}:{args.port}/{args.db}")

        if args.list:
            # List mode: show all keys matching pattern
            pattern = args.key if "*" in args.key else f"*{args.key}*"
            keys = r.keys(pattern)
            if not keys:
                print(f"No keys matching pattern '{pattern}'")
                return 1
            print(f"Found {len(keys)} key(s) matching '{pattern}':")
            for i, key in enumerate(sorted(keys), 1):
                print(f"  {i}. {key}")
            return 0

        # Fetch mode: try exact key first, then common prefixes
        value = r.get(args.key)
        found_key = args.key

        if value is None:
            # Try common prefixes
            common_prefixes = ["idempotency:", "cache:", "session:", "data:"]
            for prefix in common_prefixes:
                prefixed_key = f"{prefix}{args.key}"
                value = r.get(prefixed_key)
                if value is not None:
                    found_key = prefixed_key
                    break

        if value is None:
            print(f"Key '{args.key}' not found in Redis")
            print(f"Hint: Try '--list {args.key}' to show all keys matching this substring")
            return 1

        print(f"Key: {found_key}")
        print(f"Value type: {type(value).__name__}")
        print(f"Value length: {len(value)} bytes")
        print()

        # Try to parse as JSON for better display
        try:
            parsed = json.loads(value)
            print("Value (parsed as JSON):")
            print(json.dumps(parsed, indent=2))
        except (json.JSONDecodeError, ValueError):
            # Not JSON, print as-is
            print("Value (raw):")
            print(value)

        return 0

    except redis.ConnectionError as e:
        print(f"ERROR: Could not connect to Redis at {args.host}:{args.port}: {e}")
        return 1
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
