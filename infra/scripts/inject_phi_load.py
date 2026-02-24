#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import time
import uuid
from collections import Counter

import aiohttp


FIRST_NAMES = ["Jane", "John", "Alex", "Sam", "Taylor", "Jordan", "Casey", "Morgan"]
LAST_NAMES = ["Doe", "Smith", "Brown", "Johnson", "Miller", "Wilson", "Davis", "Clark"]
COLORS = ["red", "blue", "green", "orange", "yellow", "teal"]


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    i = int((len(sorted_values) - 1) * p)
    return sorted_values[i]


def random_payload() -> dict:
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    year = random.randint(1940, 2010)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return {
        "event_id": str(uuid.uuid4()),
        "name": name,
        "dob": f"{year:04d}-{month:02d}-{day:02d}",
        "favorite_color": random.choice(COLORS),
    }


async def worker(
    worker_id: int,
    session: aiohttp.ClientSession,
    url: str,
    queue: "asyncio.Queue[tuple[float, int, str | None]]",
    status_counts: Counter,
    latencies: list[float],
    created_phi_ids: list[str],
) -> None:
    while True:
        scheduled_at, task_type, phi_id = await queue.get()
        if scheduled_at < 0:
            queue.task_done()
            return

        now = asyncio.get_running_loop().time()
        if scheduled_at > now:
            await asyncio.sleep(scheduled_at - now)

        start = time.perf_counter()
        status = "ERR"
        
        try:
            # task_type: 0 = create, 1 = update
            if task_type == 0:
                # CREATE new patient
                payload = random_payload()
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    response_text = await resp.text()
                    status = str(resp.status)
                    # Extract phi_id from response if successful
                    if resp.status == 202:
                        try:
                            import json
                            resp_json = json.loads(response_text)
                            if "phi_id" in resp_json:
                                created_phi_ids.append(resp_json["phi_id"])
                        except Exception:
                            pass
            else:
                # UPDATE existing patient
                if phi_id is not None:
                    payload = random_payload()
                    update_url = url.rstrip("/") + f"/{phi_id}"
                    async with session.put(update_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        await resp.read()
                        status = str(resp.status)
                else:
                    status = "SKIP"  # No phi_ids yet, skip update
        except asyncio.TimeoutError:
            status = "TIMEOUT"
        except Exception:
            status = "ERR"
        finally:
            latencies.append((time.perf_counter() - start) * 1000.0)
            status_counts[status] += 1
            queue.task_done()


async def run_load(url: str, inserts_per_second: int, num_inserts: int, concurrency: int, update_ratio: float = 100.0) -> None:
    duration_seconds = num_inserts
    total_inserts = inserts_per_second * duration_seconds
    total_requests = 0
    
    status_counts: Counter = Counter()
    latencies: list[float] = []
    created_phi_ids: list[str] = []

    queue: asyncio.Queue[tuple[float, int, str | None]] = asyncio.Queue(maxsize=max(concurrency * 4, 1000))

    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        insert_workers = [
            asyncio.create_task(
                worker(i, session, url, queue, status_counts, latencies, created_phi_ids)
            )
            for i in range(concurrency)
        ]

        start_loop = asyncio.get_running_loop().time()
        start_wall = time.perf_counter()

        # Phase 1: schedule inserts only
        insert_count = 0
        for i in range(total_inserts):
            scheduled_at = start_loop + (i / inserts_per_second)
            await queue.put((scheduled_at, 0, None))
            insert_count += 1

        for _ in range(concurrency):
            await queue.put((-1, -1, None))

        await queue.join()
        await asyncio.gather(*insert_workers)

        captured_phi_ids = len(created_phi_ids)

        # Phase 2: schedule updates only, based on captured phi_ids and ratio
        # Ratio applies per captured phi_id:
        # - whole part: every phi_id gets that many updates
        # - fractional part: that percentage of phi_ids gets one extra update
        update_count = 0
        if update_ratio > 0 and created_phi_ids:
            whole_updates = int(update_ratio)
            fractional_updates = update_ratio - whole_updates

            phi_ids_for_updates: list[str] = []
            for phi_id in created_phi_ids:
                for _ in range(whole_updates):
                    phi_ids_for_updates.append(phi_id)

            extra_updates = int(round(fractional_updates * captured_phi_ids))
            if extra_updates > 0:
                extra_updates = min(extra_updates, captured_phi_ids)
                phi_ids_for_updates.extend(random.sample(created_phi_ids, extra_updates))

            total_updates = len(phi_ids_for_updates)

            update_workers = [
                asyncio.create_task(
                    worker(i, session, url, queue, status_counts, latencies, created_phi_ids)
                )
                for i in range(concurrency)
            ]

            updates_rate = max(inserts_per_second, 1)
            update_start = asyncio.get_running_loop().time()
            random.shuffle(phi_ids_for_updates)
            for i, chosen_phi_id in enumerate(phi_ids_for_updates):
                scheduled_at = update_start + (i / updates_rate)
                await queue.put((scheduled_at, 1, chosen_phi_id))
                update_count += 1

            for _ in range(concurrency):
                await queue.put((-1, -1, None))

            await queue.join()
            await asyncio.gather(*update_workers)
        else:
            total_updates = 0

        elapsed = time.perf_counter() - start_wall

    total_done = sum(status_counts.values())
    total_requests = total_inserts + total_updates
    sorted_lat = sorted(latencies)

    print("Load test completed")
    print(f"Inserts per second: {inserts_per_second}")
    print(f"Num inserts: {num_inserts} (duration seconds)")
    print(f"Concurrency: {concurrency}")
    print(f"Update ratio: {update_ratio}")
    print(f"Total inserts: {insert_count}")
    print(f"Captured phi_ids: {captured_phi_ids}")
    print(f"Total updates: {update_count}")
    print(f"Total requests: {total_done}")
    print(f"Elapsed: {elapsed:.2f}s")
    print(f"Throughput: {total_done / elapsed:.2f} req/s")
    print("Status counts:")
    for code, count in sorted(status_counts.items()):
        print(f"  {code}: {count}")

    if sorted_lat:
        print("Latency ms:")
        print(f"  min: {sorted_lat[0]:.2f}")
        print(f"  p50: {percentile(sorted_lat, 0.50):.2f}")
        print(f"  p95: {percentile(sorted_lat, 0.95):.2f}")
        print(f"  p99: {percentile(sorted_lat, 0.99):.2f}")
        print(f"  max: {sorted_lat[-1]:.2f}")
        print(f"  mean: {statistics.fmean(sorted_lat):.2f}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inject PHI write load into Admission API")
    parser.add_argument("--url", default="http://localhost:8080/api/v1/patient", help="Target POST URL")
    parser.add_argument(
        "--inserts-per-second",
        "--rate",
        dest="inserts_per_second",
        type=int,
        default=1000,
        help="Rate of inserts per second",
    )
    parser.add_argument(
        "--num-inserts",
        "--seconds",
        dest="num_inserts",
        type=int,
        default=30,
        help="Duration in seconds (total inserts = inserts-per-second * num-inserts)",
    )
    parser.add_argument("--concurrency", type=int, default=200, help="Number of concurrent workers")
    parser.add_argument(
        "--update-ratio",
        "--updates-per-insert",
        dest="update_ratio",
        type=float,
        default=100.0,
        help="Updates per insert ratio (supports decimals, e.g. 0.25, 1, 1000)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.inserts_per_second <= 0 or args.num_inserts <= 0 or args.concurrency <= 0:
        raise SystemExit("--inserts-per-second, --num-inserts, and --concurrency must be > 0")
    if args.update_ratio < 0:
        raise SystemExit("--update-ratio must be >= 0")

    asyncio.run(run_load(args.url, args.inserts_per_second, args.num_inserts, args.concurrency, args.update_ratio))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
