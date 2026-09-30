#!/usr/bin/env python3
"""
Benchmark Assignment 02 - Part 2 read challenge for PostgreSQL.

This script simulates GET /orders/{id} by querying PostgreSQL directly and
returning full order detail: user + order + order items + product snapshot.

Default connection is loaded from .env:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

Install dependencies:
  pip install -r requirements.txt

Example:
  python "3. benchmark_sql_read.py"
  python "3. benchmark_sql_read.py" --requests 5000 --warmup 100
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import statistics
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from config_env import load_env_file

load_env_file()

try:
    import psycopg
    from psycopg.rows import dict_row
    from tqdm import tqdm
except ImportError:
    print(
        "Missing dependency. Install required packages with: pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise


ORDER_DETAIL_SQL = """
    SELECT
        o.order_id,
        o.order_status,
        o.order_date,
        o.currency,
        o.total_amount,
        u.user_id,
        u.email,
        u.full_name,
        oi.order_item_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.line_total,
        p.sku,
        p.product_name,
        p.category
    FROM orders o
    JOIN users u ON u.user_id = o.user_id
    JOIN order_items oi ON oi.order_id = o.order_id
    JOIN products p ON p.product_id = oi.product_id
    WHERE o.order_id = %s
    ORDER BY oi.order_item_id
"""


def parse_count(value: str) -> int:
    number = int(value.replace("_", "").replace(",", ""))
    if number <= 0:
        raise argparse.ArgumentTypeError("value must be greater than 0")
    return number


def json_default(value: Any) -> str | int | float:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = max(0, math.ceil((percent / 100.0) * len(sorted_values)) - 1)
    return sorted_values[index]


def get_order_id_bounds(conn: "psycopg.Connection") -> tuple[int, int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(order_id), MAX(order_id), COUNT(*) FROM orders")
        min_id, max_id, row_count = cur.fetchone()

    if min_id is None or max_id is None or row_count == 0:
        raise RuntimeError("orders table is empty. Run the SQL data generator first.")

    return int(min_id), int(max_id), int(row_count)


def fetch_order_detail(cur: "psycopg.Cursor", order_id: int) -> dict[str, Any] | None:
    cur.execute(ORDER_DETAIL_SQL, (order_id,))
    rows = cur.fetchall()
    if not rows:
        return None

    first = rows[0]
    return {
        "orderId": first["order_id"],
        "status": first["order_status"],
        "orderDate": first["order_date"],
        "currency": first["currency"],
        "totalAmount": first["total_amount"],
        "user": {
            "userId": first["user_id"],
            "email": first["email"],
            "fullName": first["full_name"],
        },
        "items": [
            {
                "orderItemId": row["order_item_id"],
                "productId": row["product_id"],
                "sku": row["sku"],
                "name": row["product_name"],
                "category": row["category"],
                "quantity": row["quantity"],
                "unitPrice": row["unit_price"],
                "lineTotal": row["line_total"],
            }
            for row in rows
        ],
    }


def benchmark_reads(
    conn: "psycopg.Connection",
    *,
    request_count: int,
    warmup_count: int,
    min_order_id: int,
    max_order_id: int,
    seed: int,
) -> dict[str, Any]:
    rng = random.Random(seed)
    latencies_ms: list[float] = []
    hit_count = 0
    miss_count = 0

    with conn.cursor(row_factory=dict_row) as cur:
        for _ in tqdm(range(warmup_count), desc="SQL warmup", unit="req", dynamic_ncols=True):
            order_id = rng.randint(min_order_id, max_order_id)
            fetch_order_detail(cur, order_id)

        started = time.perf_counter()
        for _ in tqdm(range(request_count), desc="SQL read requests", unit="req", dynamic_ncols=True):
            order_id = rng.randint(min_order_id, max_order_id)
            request_started = time.perf_counter_ns()
            order = fetch_order_detail(cur, order_id)
            elapsed_ms = (time.perf_counter_ns() - request_started) / 1_000_000
            latencies_ms.append(elapsed_ms)

            if order is None:
                miss_count += 1
            else:
                hit_count += 1
        total_elapsed_s = time.perf_counter() - started

    return {
        "engine": "postgresql",
        "measuredRequests": request_count,
        "warmupRequests": warmup_count,
        "minOrderId": min_order_id,
        "maxOrderId": max_order_id,
        "hitCount": hit_count,
        "missCount": miss_count,
        "averageResponseTimeMs": statistics.fmean(latencies_ms) if latencies_ms else 0.0,
        "p95ResponseTimeMs": percentile(latencies_ms, 95),
        "minResponseTimeMs": min(latencies_ms) if latencies_ms else 0.0,
        "maxResponseTimeMs": max(latencies_ms) if latencies_ms else 0.0,
        "totalElapsedSeconds": total_elapsed_s,
        "requestsPerSecond": request_count / total_elapsed_s if total_elapsed_s > 0 else 0.0,
    }


def print_summary(result: dict[str, Any]) -> None:
    print("\nSQL Read Benchmark Result")
    print(f"Measured requests      : {result['measuredRequests']:,}")
    print(f"Warmup requests        : {result['warmupRequests']:,}")
    print(f"Order ID range         : {result['minOrderId']:,} -> {result['maxOrderId']:,}")
    print(f"Hits / misses          : {result['hitCount']:,} / {result['missCount']:,}")
    print(f"Average response time  : {result['averageResponseTimeMs']:.3f} ms")
    print(f"P95 response time      : {result['p95ResponseTimeMs']:.3f} ms")
    print(f"Min / max response time: {result['minResponseTimeMs']:.3f} / {result['maxResponseTimeMs']:.3f} ms")
    print(f"Requests per second    : {result['requestsPerSecond']:.2f}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark PostgreSQL JOIN reads for GET /orders/{id}."
    )
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5436")))
    parser.add_argument("--database", default=os.getenv("PGDATABASE", "ecommerce"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "assignment2"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", ""))
    parser.add_argument("--requests", type=parse_count, default=parse_count(os.getenv("READ_REQUESTS", "5000")))
    parser.add_argument("--warmup", type=int, default=int(os.getenv("READ_WARMUP", "0")))
    parser.add_argument("--min-order-id", type=int, default=None)
    parser.add_argument("--max-order-id", type=int, default=None)
    parser.add_argument("--seed", type=int, default=int(os.getenv("READ_SEED", "20260525")))
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON result after the summary.")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.warmup < 0:
        raise ValueError("--warmup must be greater than or equal to 0")
    if (args.min_order_id is None) != (args.max_order_id is None):
        raise ValueError("--min-order-id and --max-order-id must be provided together")
    if args.min_order_id is not None and args.min_order_id > args.max_order_id:
        raise ValueError("--min-order-id must be less than or equal to --max-order-id")


def main() -> int:
    args = parse_args()
    validate_args(args)

    with psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
        application_name="assignment2_sql_read_benchmark",
    ) as conn:
        if args.min_order_id is None or args.max_order_id is None:
            min_order_id, max_order_id, row_count = get_order_id_bounds(conn)
            print(f"Detected {row_count:,} SQL orders")
        else:
            min_order_id = args.min_order_id
            max_order_id = args.max_order_id

        result = benchmark_reads(
            conn,
            request_count=args.requests,
            warmup_count=args.warmup,
            min_order_id=min_order_id,
            max_order_id=max_order_id,
            seed=args.seed,
        )

    print_summary(result)
    if args.json:
        print("\nJSON Result")
        print(json.dumps(result, indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
