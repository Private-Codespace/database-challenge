#!/usr/bin/env python3
"""
Assignment 02 - Part 3 concurrency challenge for MongoDB.

The script creates/resets one flash-sale product with stockQuantity = 1,
then starts 50 concurrent purchase attempts by default.

Race-condition protection:
  find_one_and_update(
      {"_id": product_id, "stockQuantity": {"$gt": 0}},
      {"$inc": {"stockQuantity": -1}, "$push": {"purchaseLog": ...}}
  )

MongoDB guarantees atomicity for a single-document update. Only one thread can
match stockQuantity > 0 and decrement stock from 1 to 0; the other threads
receive no updated product and must fail without creating an order.

Default connection is loaded from .env:
  MONGO_HOST, MONGO_PORT, MONGO_USER, MONGO_PASSWORD, MONGO_AUTH_SOURCE

Install dependencies:
  pip install -r requirements.txt

Example:
  python "3.2 concurrency_nosql.py"
  python "3.2 concurrency_nosql.py" --threads 50
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from config_env import build_mongo_uri, load_env_file

load_env_file()

try:
    from bson import ObjectId
    from pymongo import ASCENDING, MongoClient, ReturnDocument
    from tqdm import tqdm
except ImportError:
    print(
        "Missing dependency. Install required packages with: pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise


DEFAULT_MONGO_URI = build_mongo_uri()
DEFAULT_FLASH_USER_ID = 9_000_000_001
DEFAULT_FLASH_PRODUCT_ID = 9_000_000_001
DEFAULT_PRICE_CENTS = 19_990
CURRENCY = "USD"


@dataclass
class AttemptResult:
    thread_index: int
    success: bool
    elapsed_ms: float
    order_id: int | None = None
    final_stock_seen: int | None = None
    error: str | None = None


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
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def connect(args: argparse.Namespace) -> MongoClient:
    return MongoClient(args.uri)


def user_snapshot(user_id: int) -> dict[str, Any]:
    return {
        "userId": user_id,
        "email": f"flash-sale-user-{user_id}@example.com",
        "fullName": "Flash Sale Test User",
    }


def item_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "productId": args.flash_product_id,
        "sku": f"FLASH-SALE-{args.flash_product_id}",
        "name": "Flash Sale Product",
        "category": "flash-sale",
        "quantity": 1,
        "unitPriceCents": args.price_cents,
        "lineTotalCents": args.price_cents,
    }


def setup_flash_sale_data(db, args: argparse.Namespace) -> None:
    progress("Preparing MongoDB flash-sale user and product with stockQuantity = 1")
    now = datetime.now(timezone.utc)

    db.users.update_one(
        {"_id": args.flash_user_id},
        {
            "$set": {
                "userId": args.flash_user_id,
                "email": f"flash-sale-user-{args.flash_user_id}@example.com",
                "fullName": "Flash Sale Test User",
                "updatedAt": now,
            },
            "$setOnInsert": {"createdAt": now},
        },
        upsert=True,
    )
    db.products.update_one(
        {"_id": args.flash_product_id},
        {
            "$set": {
                "productId": args.flash_product_id,
                "sku": f"FLASH-SALE-{args.flash_product_id}",
                "name": "Flash Sale Product",
                "category": "flash-sale",
                "currentPriceCents": args.price_cents,
                "currency": CURRENCY,
                "stockQuantity": 1,
                "purchaseLog": [],
                "updatedAt": now,
            },
            "$setOnInsert": {"createdAt": now},
        },
        upsert=True,
    )
    db.orders.create_index([("concurrencyRunId", ASCENDING)], name="idx_orders_concurrency_run_id")


def attempt_purchase(
    db,
    args: argparse.Namespace,
    *,
    thread_index: int,
    barrier: threading.Barrier,
    run_id: str,
    base_order_id: int,
) -> AttemptResult:
    order_id = base_order_id + thread_index

    try:
        barrier.wait(timeout=args.barrier_timeout)
        started_ns = time.perf_counter_ns()
        now = datetime.now(timezone.utc)

        updated_product = db.products.find_one_and_update(
            {
                "_id": args.flash_product_id,
                "stockQuantity": {"$gt": 0},
            },
            {
                "$inc": {"stockQuantity": -1},
                "$set": {"updatedAt": now},
                "$push": {
                    "purchaseLog": {
                        "runId": run_id,
                        "orderId": order_id,
                        "threadIndex": thread_index,
                        "purchasedAt": now,
                    }
                },
            },
            projection={"stockQuantity": 1},
            return_document=ReturnDocument.AFTER,
        )

        if updated_product is None:
            elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
            return AttemptResult(thread_index=thread_index, success=False, elapsed_ms=elapsed_ms)

        order_document = {
            "_id": order_id,
            "orderId": order_id,
            "concurrencyRunId": run_id,
            "user": user_snapshot(args.flash_user_id),
            "status": "PAID",
            "orderDate": now,
            "currency": CURRENCY,
            "totalAmountCents": args.price_cents,
            "items": [item_snapshot(args)],
            "createdAt": now,
        }
        db.orders.insert_one(order_document)

        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        return AttemptResult(
            thread_index=thread_index,
            success=True,
            elapsed_ms=elapsed_ms,
            order_id=order_id,
            final_stock_seen=int(updated_product["stockQuantity"]),
        )
    except threading.BrokenBarrierError as exc:
        return AttemptResult(thread_index=thread_index, success=False, elapsed_ms=0.0, error=str(exc))
    except Exception as exc:
        return AttemptResult(thread_index=thread_index, success=False, elapsed_ms=0.0, error=repr(exc))


def get_final_state(db, args: argparse.Namespace, *, run_id: str) -> dict[str, int]:
    product = db.products.find_one(
        {"_id": args.flash_product_id},
        projection={"stockQuantity": 1, "purchaseLog": 1},
    )
    if product is None:
        return {
            "finalStock": -1,
            "purchaseLogEntriesInThisRun": 0,
            "createdOrdersInThisRun": 0,
        }

    purchase_log = product.get("purchaseLog", [])
    purchase_log_entries = sum(1 for entry in purchase_log if entry.get("runId") == run_id)
    created_orders = db.orders.count_documents({"concurrencyRunId": run_id})

    return {
        "finalStock": int(product.get("stockQuantity", -1)),
        "purchaseLogEntriesInThisRun": int(purchase_log_entries),
        "createdOrdersInThisRun": int(created_orders),
    }


def run_concurrency_test(args: argparse.Namespace) -> dict[str, Any]:
    client = connect(args)
    db = client[args.database]

    try:
        setup_flash_sale_data(db, args)

        run_id = f"mongo-concurrency-{int(time.time() * 1_000_000)}"
        base_order_id = int(time.time() * 1_000_000)
        barrier = threading.Barrier(args.threads)
        results: list[AttemptResult] = []

        progress(f"Starting {args.threads} concurrent MongoDB purchase attempts")
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = [
                executor.submit(
                    attempt_purchase,
                    db,
                    args,
                    thread_index=thread_index,
                    barrier=barrier,
                    run_id=run_id,
                    base_order_id=base_order_id,
                )
                for thread_index in range(args.threads)
            ]
            for future in tqdm(as_completed(futures), total=len(futures), desc="Mongo threads", unit="thread"):
                results.append(future.result())

        total_elapsed_s = time.perf_counter() - started
        final_state = get_final_state(db, args, run_id=run_id)
    finally:
        client.close()

    success_count = sum(1 for result in results if result.success)
    failed_count = args.threads - success_count
    errored_count = sum(1 for result in results if result.error)
    expected_success = 1
    oversold = success_count > expected_success or final_state["finalStock"] < 0
    passed = (
        success_count == expected_success
        and final_state["purchaseLogEntriesInThisRun"] == expected_success
        and final_state["createdOrdersInThisRun"] == expected_success
        and final_state["finalStock"] == 0
        and not oversold
    )

    latencies = [result.elapsed_ms for result in results if result.elapsed_ms > 0]
    return {
        "engine": "mongodb",
        "mechanism": "Atomic single-document find_one_and_update: stockQuantity decrements only when stockQuantity > 0.",
        "threads": args.threads,
        "initialStock": 1,
        "successCount": success_count,
        "failedCount": failed_count,
        "erroredCount": errored_count,
        "finalStock": final_state["finalStock"],
        "purchaseLogEntriesInThisRun": final_state["purchaseLogEntriesInThisRun"],
        "createdOrdersInThisRun": final_state["createdOrdersInThisRun"],
        "oversold": oversold,
        "passed": passed,
        "totalElapsedSeconds": total_elapsed_s,
        "averageAttemptLatencyMs": sum(latencies) / len(latencies) if latencies else 0.0,
        "attempts": [asdict(result) for result in sorted(results, key=lambda item: item.thread_index)],
    }


def print_summary(result: dict[str, Any]) -> None:
    print("\nMongoDB Concurrency Result")
    print(f"Mechanism                  : {result['mechanism']}")
    print(f"Threads                    : {result['threads']}")
    print(f"Initial stock              : {result['initialStock']}")
    print(f"Successful purchases       : {result['successCount']}")
    print(f"Failed purchases           : {result['failedCount']}")
    print(f"Errored attempts           : {result['erroredCount']}")
    print(f"Purchase-log entries       : {result['purchaseLogEntriesInThisRun']}")
    print(f"Created orders this run    : {result['createdOrdersInThisRun']}")
    print(f"Final stock                : {result['finalStock']}")
    print(f"Oversold                   : {result['oversold']}")
    print(f"Passed                     : {result['passed']}")
    print(f"Average attempt latency    : {result['averageAttemptLatencyMs']:.3f} ms")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run MongoDB flash-sale concurrency test with atomic stock update."
    )
    parser.add_argument("--uri", default=os.getenv("MONGO_URI", DEFAULT_MONGO_URI))
    parser.add_argument("--database", default=os.getenv("MONGO_DATABASE", "ecommerce"))
    parser.add_argument("--threads", type=parse_count, default=parse_count(os.getenv("CONCURRENCY_THREADS", "50")))
    parser.add_argument("--flash-user-id", type=int, default=int(os.getenv("FLASH_USER_ID", str(DEFAULT_FLASH_USER_ID))))
    parser.add_argument("--flash-product-id", type=int, default=int(os.getenv("FLASH_PRODUCT_ID", str(DEFAULT_FLASH_PRODUCT_ID))))
    parser.add_argument("--price-cents", type=parse_count, default=parse_count(os.getenv("FLASH_PRICE_CENTS", str(DEFAULT_PRICE_CENTS))))
    parser.add_argument("--barrier-timeout", type=float, default=float(os.getenv("BARRIER_TIMEOUT_SECONDS", "30")))
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON result after the summary.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_concurrency_test(args)
    print_summary(result)
    if args.json:
        print("\nJSON Result")
        print(json.dumps(result, indent=2, default=json_default))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
