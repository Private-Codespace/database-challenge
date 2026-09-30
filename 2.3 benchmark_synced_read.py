#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import os
import random
import statistics
import sys
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from config_env import build_mongo_uri, load_env_file

load_env_file()

try:
    import psycopg
    from bson import ObjectId
    from bson.decimal128 import Decimal128
    from psycopg.rows import dict_row
    from pymongo import ASCENDING, DESCENDING, MongoClient
    from tqdm import tqdm
except ImportError:
    print(
        "Missing dependency. Install required packages with: pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise


DEFAULT_MONGO_URI = build_mongo_uri()
DEFAULT_GENERATED_ID_CAP = 1_000_000_000


SQL_ORDER_DETAIL = """
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


def progress(message: str) -> None:
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    tqdm.write(f"[{stamp}] {message}")


def json_default(value: Any) -> str | int | float:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Decimal128):
        return str(value.to_decimal())
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = max(0, math.ceil((percent / 100.0) * len(sorted_values)) - 1)
    return sorted_values[index]


def cents_from_decimal(value: Decimal) -> int:
    return int((value * Decimal("100")).to_integral_value())


def utc_iso_seconds(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def postgres_connect(args: argparse.Namespace) -> "psycopg.Connection":
    return psycopg.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
        application_name="assignment2_synced_read_benchmark",
    )


def mongo_connect(args: argparse.Namespace) -> MongoClient:
    return MongoClient(args.mongo_uri, tz_aware=True, tzinfo=timezone.utc)


def detect_sql_range(conn: "psycopg.Connection", generated_id_cap: int) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MIN(order_id), MAX(order_id), COUNT(*)
            FROM orders
            WHERE order_id BETWEEN 1 AND %s
            """,
            (generated_id_cap,),
        )
        min_id, max_id, count = cur.fetchone()

    if min_id is None or max_id is None or count == 0:
        raise RuntimeError("PostgreSQL orders table has no generated orders in the expected id range.")

    return {
        "minOrderId": int(min_id),
        "maxOrderId": int(max_id),
        "orderCountInGeneratedRange": int(count),
    }


def detect_mongo_range(db, generated_id_cap: int) -> dict[str, int]:
    id_filter = {"_id": {"$gte": 1, "$lte": generated_id_cap}}
    min_doc = db.orders.find_one(id_filter, projection={"_id": 1}, sort=[("_id", ASCENDING)])
    max_doc = db.orders.find_one(id_filter, projection={"_id": 1}, sort=[("_id", DESCENDING)])

    if min_doc is None or max_doc is None:
        raise RuntimeError("MongoDB orders collection has no generated orders in the expected id range.")

    return {
        "minOrderId": int(min_doc["_id"]),
        "maxOrderId": int(max_doc["_id"]),
        "orderCountInGeneratedRange": int(db.orders.count_documents(id_filter)),
    }


def common_order_range(
    pg_conn: "psycopg.Connection",
    mongo_db,
    args: argparse.Namespace,
) -> dict[str, Any]:
    if args.min_order_id is not None and args.max_order_id is not None:
        return {
            "minOrderId": args.min_order_id,
            "maxOrderId": args.max_order_id,
            "sql": None,
            "mongo": None,
        }

    sql_range = detect_sql_range(pg_conn, args.generated_id_cap)
    mongo_range = detect_mongo_range(mongo_db, args.generated_id_cap)
    min_order_id = max(sql_range["minOrderId"], mongo_range["minOrderId"])
    max_order_id = min(sql_range["maxOrderId"], mongo_range["maxOrderId"])

    if min_order_id > max_order_id:
        raise RuntimeError(
            "PostgreSQL and MongoDB do not have an overlapping generated order_id range. "
            "Regenerate both with 1.3 generate_synced_data.py --reset."
        )

    return {
        "minOrderId": min_order_id,
        "maxOrderId": max_order_id,
        "sql": sql_range,
        "mongo": mongo_range,
    }


def fetch_sql_order_detail(cur: "psycopg.Cursor", order_id: int) -> dict[str, Any] | None:
    cur.execute(SQL_ORDER_DETAIL, (order_id,))
    rows = cur.fetchall()
    if not rows:
        return None

    first = rows[0]
    return {
        "orderId": int(first["order_id"]),
        "status": first["order_status"],
        "orderDate": utc_iso_seconds(first["order_date"]),
        "currency": first["currency"],
        "totalAmountCents": cents_from_decimal(first["total_amount"]),
        "user": {
            "userId": int(first["user_id"]),
            "email": first["email"],
            "fullName": first["full_name"],
        },
        "items": [
            {
                "productId": int(row["product_id"]),
                "sku": row["sku"],
                "name": row["product_name"],
                "category": row["category"],
                "quantity": int(row["quantity"]),
                "unitPriceCents": cents_from_decimal(row["unit_price"]),
                "lineTotalCents": cents_from_decimal(row["line_total"]),
            }
            for row in rows
        ],
    }


def fetch_mongo_order_detail(mongo_db, order_id: int) -> dict[str, Any] | None:
    doc = mongo_db.orders.find_one({"_id": order_id})
    if doc is None:
        return None

    return {
        "orderId": int(doc["orderId"]),
        "status": doc["status"],
        "orderDate": utc_iso_seconds(doc["orderDate"]),
        "currency": doc["currency"],
        "totalAmountCents": int(doc["totalAmountCents"]),
        "user": {
            "userId": int(doc["user"]["userId"]),
            "email": doc["user"]["email"],
            "fullName": doc["user"]["fullName"],
        },
        "items": [
            {
                "productId": int(item["productId"]),
                "sku": item["sku"],
                "name": item["name"],
                "category": item["category"],
                "quantity": int(item["quantity"]),
                "unitPriceCents": int(item["unitPriceCents"]),
                "lineTotalCents": int(item["lineTotalCents"]),
            }
            for item in doc["items"]
        ],
    }


def random_order_ids(
    *,
    count: int,
    min_order_id: int,
    max_order_id: int,
    seed: int,
) -> list[int]:
    rng = random.Random(seed)
    return [rng.randint(min_order_id, max_order_id) for _ in range(count)]


def validate_payloads(
    pg_conn: "psycopg.Connection",
    mongo_db,
    *,
    order_ids: list[int],
) -> dict[str, Any]:
    if not order_ids:
        return {"checked": 0, "passed": True, "mismatches": []}

    mismatches: list[dict[str, Any]] = []
    checked = 0
    with pg_conn.cursor(row_factory=dict_row) as cur:
        for order_id in tqdm(order_ids, desc="Payload validation", unit="order", dynamic_ncols=True):
            checked += 1
            sql_order = fetch_sql_order_detail(cur, order_id)
            mongo_order = fetch_mongo_order_detail(mongo_db, order_id)
            if sql_order != mongo_order:
                mismatches.append(
                    {
                        "orderId": order_id,
                        "sql": sql_order,
                        "mongo": mongo_order,
                    }
                )
                if len(mismatches) >= 5:
                    break

    return {
        "checked": checked,
        "passed": not mismatches,
        "mismatches": mismatches,
    }


def benchmark_sql(
    pg_conn: "psycopg.Connection",
    *,
    warmup_ids: list[int],
    measured_ids: list[int],
) -> dict[str, Any]:
    latencies_ms: list[float] = []
    hit_count = 0
    miss_count = 0

    with pg_conn.cursor(row_factory=dict_row) as cur:
        for order_id in tqdm(warmup_ids, desc="SQL warmup", unit="req", dynamic_ncols=True):
            fetch_sql_order_detail(cur, order_id)

        started = time.perf_counter()
        for order_id in tqdm(measured_ids, desc="SQL read requests", unit="req", dynamic_ncols=True):
            request_started = time.perf_counter_ns()
            order = fetch_sql_order_detail(cur, order_id)
            elapsed_ms = (time.perf_counter_ns() - request_started) / 1_000_000
            latencies_ms.append(elapsed_ms)
            if order is None:
                miss_count += 1
            else:
                hit_count += 1
        total_elapsed_s = time.perf_counter() - started

    return benchmark_result("postgresql", latencies_ms, hit_count, miss_count, total_elapsed_s, len(warmup_ids))


def benchmark_mongo(
    mongo_db,
    *,
    warmup_ids: list[int],
    measured_ids: list[int],
) -> dict[str, Any]:
    latencies_ms: list[float] = []
    hit_count = 0
    miss_count = 0

    for order_id in tqdm(warmup_ids, desc="Mongo warmup", unit="req", dynamic_ncols=True):
        fetch_mongo_order_detail(mongo_db, order_id)

    started = time.perf_counter()
    for order_id in tqdm(measured_ids, desc="Mongo read requests", unit="req", dynamic_ncols=True):
        request_started = time.perf_counter_ns()
        order = fetch_mongo_order_detail(mongo_db, order_id)
        elapsed_ms = (time.perf_counter_ns() - request_started) / 1_000_000
        latencies_ms.append(elapsed_ms)
        if order is None:
            miss_count += 1
        else:
            hit_count += 1
    total_elapsed_s = time.perf_counter() - started

    return benchmark_result("mongodb", latencies_ms, hit_count, miss_count, total_elapsed_s, len(warmup_ids))


def benchmark_result(
    engine: str,
    latencies_ms: list[float],
    hit_count: int,
    miss_count: int,
    total_elapsed_s: float,
    warmup_count: int,
) -> dict[str, Any]:
    measured_count = len(latencies_ms)
    return {
        "engine": engine,
        "measuredRequests": measured_count,
        "warmupRequests": warmup_count,
        "hitCount": hit_count,
        "missCount": miss_count,
        "averageResponseTimeMs": statistics.fmean(latencies_ms) if latencies_ms else 0.0,
        "p95ResponseTimeMs": percentile(latencies_ms, 95),
        "minResponseTimeMs": min(latencies_ms) if latencies_ms else 0.0,
        "maxResponseTimeMs": max(latencies_ms) if latencies_ms else 0.0,
        "totalElapsedSeconds": total_elapsed_s,
        "requestsPerSecond": measured_count / total_elapsed_s if total_elapsed_s > 0 else 0.0,
    }


def run_benchmarks(
    pg_conn: "psycopg.Connection",
    mongo_db,
    args: argparse.Namespace,
) -> dict[str, Any]:
    detected_range = common_order_range(pg_conn, mongo_db, args)
    min_order_id = int(detected_range["minOrderId"])
    max_order_id = int(detected_range["maxOrderId"])

    warmup_ids = random_order_ids(
        count=args.warmup,
        min_order_id=min_order_id,
        max_order_id=max_order_id,
        seed=args.seed + 17,
    )
    measured_ids = random_order_ids(
        count=args.requests,
        min_order_id=min_order_id,
        max_order_id=max_order_id,
        seed=args.seed,
    )
    validation_ids = random_order_ids(
        count=args.validate_sample,
        min_order_id=min_order_id,
        max_order_id=max_order_id,
        seed=args.seed + 31,
    )

    validation = validate_payloads(pg_conn, mongo_db, order_ids=validation_ids)
    if not validation["passed"]:
        print(json.dumps(validation["mismatches"], indent=2, default=json_default), file=sys.stderr)
        raise RuntimeError(
            "Payload validation failed. Regenerate data with 1.3 generate_synced_data.py --reset."
        )

    if args.run_order == "mongo-first":
        mongo_result = benchmark_mongo(mongo_db, warmup_ids=warmup_ids, measured_ids=measured_ids)
        sql_result = benchmark_sql(pg_conn, warmup_ids=warmup_ids, measured_ids=measured_ids)
    else:
        sql_result = benchmark_sql(pg_conn, warmup_ids=warmup_ids, measured_ids=measured_ids)
        mongo_result = benchmark_mongo(mongo_db, warmup_ids=warmup_ids, measured_ids=measured_ids)

    return {
        "benchmark": "synced-read",
        "requestIdsSeed": args.seed,
        "orderIdRange": {
            "minOrderId": min_order_id,
            "maxOrderId": max_order_id,
            "detected": detected_range,
        },
        "validation": {
            "checked": validation["checked"],
            "passed": validation["passed"],
        },
        "sql": sql_result,
        "mongo": mongo_result,
        "sameMeasuredOrderIds": True,
        "runOrder": args.run_order,
    }


def print_engine_summary(label: str, result: dict[str, Any]) -> None:
    print(f"\n{label}")
    print(f"Measured requests       : {result['measuredRequests']:,}")
    print(f"Warmup requests         : {result['warmupRequests']:,}")
    print(f"Hits / misses           : {result['hitCount']:,} / {result['missCount']:,}")
    print(f"Average response time   : {result['averageResponseTimeMs']:.3f} ms")
    print(f"P95 response time       : {result['p95ResponseTimeMs']:.3f} ms")
    print(f"Min / max response time : {result['minResponseTimeMs']:.3f} / {result['maxResponseTimeMs']:.3f} ms")
    print(f"Requests per second     : {result['requestsPerSecond']:.2f}")


def print_summary(result: dict[str, Any]) -> None:
    order_range = result["orderIdRange"]
    sql_result = result["sql"]
    mongo_result = result["mongo"]
    avg_ratio = (
        sql_result["averageResponseTimeMs"] / mongo_result["averageResponseTimeMs"]
        if mongo_result["averageResponseTimeMs"] > 0
        else None
    )
    p95_ratio = (
        sql_result["p95ResponseTimeMs"] / mongo_result["p95ResponseTimeMs"]
        if mongo_result["p95ResponseTimeMs"] > 0
        else None
    )

    print("\nSynchronized Read Benchmark Result")
    print(f"Same measured order IDs : {result['sameMeasuredOrderIds']}")
    print(f"Run order               : {result['runOrder']}")
    print(f"Order ID range          : {order_range['minOrderId']:,} -> {order_range['maxOrderId']:,}")
    print(f"Payload validation      : {result['validation']['checked']:,} checked, passed={result['validation']['passed']}")

    print_engine_summary("PostgreSQL JOIN", sql_result)
    print_engine_summary("MongoDB document", mongo_result)

    print("\nComparison")
    if avg_ratio is not None:
        print(f"Average SQL/Mongo ratio : {avg_ratio:.3f}x")
    if p95_ratio is not None:
        print(f"P95 SQL/Mongo ratio     : {p95_ratio:.3f}x")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark synchronized SQL JOIN reads and MongoDB document reads using the same order IDs."
    )
    parser.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PGPORT", "5436")))
    parser.add_argument("--pg-database", default=os.getenv("PGDATABASE", "ecommerce"))
    parser.add_argument("--pg-user", default=os.getenv("PGUSER", "assignment2"))
    parser.add_argument("--pg-password", default=os.getenv("PGPASSWORD", ""))
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", DEFAULT_MONGO_URI))
    parser.add_argument("--mongo-database", default=os.getenv("MONGO_DATABASE", "ecommerce"))
    parser.add_argument("--requests", type=parse_count, default=parse_count(os.getenv("READ_REQUESTS", "5000")))
    parser.add_argument("--warmup", type=int, default=int(os.getenv("READ_WARMUP", "0")))
    parser.add_argument("--min-order-id", type=int, default=None)
    parser.add_argument("--max-order-id", type=int, default=None)
    parser.add_argument("--generated-id-cap", type=parse_count, default=parse_count(os.getenv("GENERATED_ID_CAP", str(DEFAULT_GENERATED_ID_CAP))))
    parser.add_argument("--seed", type=int, default=int(os.getenv("READ_SEED", "20260525")))
    parser.add_argument("--validate-sample", type=int, default=int(os.getenv("READ_VALIDATE_SAMPLE", "50")))
    parser.add_argument(
        "--run-order",
        choices=("sql-first", "mongo-first"),
        default=os.getenv("READ_RUN_ORDER", "sql-first"),
        help="Which database to benchmark first.",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON result after the summary.")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.warmup < 0:
        raise ValueError("--warmup must be greater than or equal to 0")
    if args.validate_sample < 0:
        raise ValueError("--validate-sample must be greater than or equal to 0")
    if (args.min_order_id is None) != (args.max_order_id is None):
        raise ValueError("--min-order-id and --max-order-id must be provided together")
    if args.min_order_id is not None and args.min_order_id > args.max_order_id:
        raise ValueError("--min-order-id must be less than or equal to --max-order-id")


def main() -> int:
    args = parse_args()
    validate_args(args)

    pg_conn = postgres_connect(args)
    mongo_client = mongo_connect(args)
    mongo_db = mongo_client[args.mongo_database]
    try:
        result = run_benchmarks(pg_conn, mongo_db, args)
    finally:
        pg_conn.close()
        mongo_client.close()

    print_summary(result)
    if args.json:
        print("\nJSON Result")
        print(json.dumps(result, indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
