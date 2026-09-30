#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from config_env import build_mongo_uri, load_env_file

load_env_file()

try:
    import psycopg
    from bson import ObjectId
    from bson.decimal128 import Decimal128
    from pymongo import ASCENDING, MongoClient, ReturnDocument
    from tqdm import tqdm
except ImportError:
    print(
        "Missing dependency. Install required packages with: pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise


DEFAULT_MONGO_URI = build_mongo_uri()
DEFAULT_FLASH_USER_ID = 1_000_000_001
DEFAULT_FLASH_PRODUCT_ID = 1_000_000_001
DEFAULT_FLASH_ORDER_ID = 1_000_000_001
DEFAULT_FLASH_ORDER_ITEM_ID = 1_000_000_001
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


def money_from_cents(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"


def decimal128_from_cents(cents: int) -> Decimal128:
    return Decimal128(money_from_cents(cents))


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


def postgres_connect(args: argparse.Namespace) -> "psycopg.Connection":
    return psycopg.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
        application_name="assignment2_synced_concurrency",
    )


def mongo_connect(args: argparse.Namespace) -> MongoClient:
    return MongoClient(args.mongo_uri, tz_aware=True, tzinfo=timezone.utc)


def user_email(user_id: int) -> str:
    return f"flash-sale-user-{user_id}@example.com"


def user_snapshot(user_id: int) -> dict[str, Any]:
    return {
        "userId": user_id,
        "email": user_email(user_id),
        "fullName": "Flash Sale Test User",
    }


def item_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "productId": args.flash_product_id,
        "sku": f"FLASH-SALE-{args.flash_product_id}",
        "name": "Flash Sale Product",
        "category": "flash-sale",
        "quantity": 1,
        "unitPrice": decimal128_from_cents(args.price_cents),
        "unitPriceCents": args.price_cents,
        "lineTotal": decimal128_from_cents(args.price_cents),
        "lineTotalCents": args.price_cents,
    }


def cleanup_sql_flash_sale(conn: "psycopg.Connection", args: argparse.Namespace) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM orders
            WHERE order_id IN (
                SELECT order_id
                FROM order_items
                WHERE product_id = %s
            )
            OR order_id = %s
            """,
            (args.flash_product_id, args.flash_order_id),
        )
    conn.commit()


def setup_sql_flash_sale(args: argparse.Namespace) -> None:
    progress("Preparing PostgreSQL flash-sale data")
    now = datetime.now(timezone.utc)
    price = money_from_cents(args.price_cents)

    with postgres_connect(args) as conn:
        cleanup_sql_flash_sale(conn, args)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, email, full_name, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE
                SET email = EXCLUDED.email,
                    full_name = EXCLUDED.full_name
                """,
                (
                    args.flash_user_id,
                    user_email(args.flash_user_id),
                    "Flash Sale Test User",
                    now,
                ),
            )
            cur.execute(
                """
                INSERT INTO products (
                    product_id, sku, product_name, category,
                    current_price, stock_quantity, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE
                SET sku = EXCLUDED.sku,
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category,
                    current_price = EXCLUDED.current_price,
                    stock_quantity = EXCLUDED.stock_quantity
                """,
                (
                    args.flash_product_id,
                    f"FLASH-SALE-{args.flash_product_id}",
                    "Flash Sale Product",
                    "flash-sale",
                    price,
                    1,
                    now,
                ),
            )
        conn.commit()


def setup_mongo_flash_sale(mongo_db, args: argparse.Namespace) -> None:
    progress("Preparing MongoDB flash-sale data")
    now = datetime.now(timezone.utc)

    mongo_db.orders.delete_many(
        {
            "$or": [
                {"_id": args.flash_order_id},
                {"items.productId": args.flash_product_id},
            ]
        }
    )
    mongo_db.users.update_one(
        {"_id": args.flash_user_id},
        {
            "$set": {
                "userId": args.flash_user_id,
                "email": user_email(args.flash_user_id),
                "fullName": "Flash Sale Test User",
                "updatedAt": now,
            },
            "$setOnInsert": {"createdAt": now},
        },
        upsert=True,
    )
    mongo_db.products.update_one(
        {"_id": args.flash_product_id},
        {
            "$set": {
                "productId": args.flash_product_id,
                "sku": f"FLASH-SALE-{args.flash_product_id}",
                "name": "Flash Sale Product",
                "category": "flash-sale",
                "currentPrice": decimal128_from_cents(args.price_cents),
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
    mongo_db.orders.create_index([("items.productId", ASCENDING)], name="idx_orders_items_product_id")


def sql_attempt_purchase(
    args: argparse.Namespace,
    *,
    thread_index: int,
    barrier: threading.Barrier,
) -> AttemptResult:
    conn: psycopg.Connection | None = None
    try:
        conn = postgres_connect(args)
        barrier.wait(timeout=args.barrier_timeout)

        started_ns = time.perf_counter_ns()
        now = datetime.now(timezone.utc)
        price = money_from_cents(args.price_cents)

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE products
                SET stock_quantity = stock_quantity - 1
                WHERE product_id = %s
                  AND stock_quantity > 0
                RETURNING stock_quantity
                """,
                (args.flash_product_id,),
            )
            stock_row = cur.fetchone()

            if stock_row is None:
                conn.rollback()
                elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
                return AttemptResult(thread_index=thread_index, success=False, elapsed_ms=elapsed_ms)

            remaining_stock = int(stock_row[0])
            cur.execute(
                """
                INSERT INTO orders (
                    order_id, user_id, order_status, order_date,
                    currency, total_amount, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    args.flash_order_id,
                    args.flash_user_id,
                    "PAID",
                    now,
                    CURRENCY,
                    price,
                    now,
                ),
            )
            cur.execute(
                """
                INSERT INTO order_items (
                    order_item_id, order_id, product_id,
                    quantity, unit_price, line_total
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    args.flash_order_item_id,
                    args.flash_order_id,
                    args.flash_product_id,
                    1,
                    price,
                    price,
                ),
            )
            conn.commit()

        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        return AttemptResult(
            thread_index=thread_index,
            success=True,
            elapsed_ms=elapsed_ms,
            order_id=args.flash_order_id,
            final_stock_seen=remaining_stock,
        )
    except threading.BrokenBarrierError as exc:
        if conn is not None:
            conn.rollback()
        return AttemptResult(thread_index=thread_index, success=False, elapsed_ms=0.0, error=str(exc))
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        return AttemptResult(thread_index=thread_index, success=False, elapsed_ms=0.0, error=repr(exc))
    finally:
        if conn is not None:
            conn.close()


def mongo_attempt_purchase(
    mongo_db,
    args: argparse.Namespace,
    *,
    thread_index: int,
    barrier: threading.Barrier,
) -> AttemptResult:
    try:
        barrier.wait(timeout=args.barrier_timeout)
        started_ns = time.perf_counter_ns()
        now = datetime.now(timezone.utc)

        updated_product = mongo_db.products.find_one_and_update(
            {
                "_id": args.flash_product_id,
                "stockQuantity": {"$gt": 0},
            },
            {
                "$inc": {"stockQuantity": -1},
                "$set": {"updatedAt": now},
                "$push": {
                    "purchaseLog": {
                        "orderId": args.flash_order_id,
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
            "_id": args.flash_order_id,
            "orderId": args.flash_order_id,
            "user": user_snapshot(args.flash_user_id),
            "status": "PAID",
            "orderDate": now,
            "currency": CURRENCY,
            "totalAmount": decimal128_from_cents(args.price_cents),
            "totalAmountCents": args.price_cents,
            "items": [item_snapshot(args)],
            "createdAt": now,
        }
        mongo_db.orders.insert_one(order_document)

        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        return AttemptResult(
            thread_index=thread_index,
            success=True,
            elapsed_ms=elapsed_ms,
            order_id=args.flash_order_id,
            final_stock_seen=int(updated_product["stockQuantity"]),
        )
    except threading.BrokenBarrierError as exc:
        return AttemptResult(thread_index=thread_index, success=False, elapsed_ms=0.0, error=str(exc))
    except Exception as exc:
        return AttemptResult(thread_index=thread_index, success=False, elapsed_ms=0.0, error=repr(exc))


def run_threaded_attempts(
    *,
    label: str,
    thread_count: int,
    attempt_fn: Callable[[int, threading.Barrier], AttemptResult],
) -> tuple[list[AttemptResult], float]:
    barrier = threading.Barrier(thread_count)
    results: list[AttemptResult] = []
    progress(f"Starting {thread_count} concurrent {label} purchase attempts")

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [
            executor.submit(attempt_fn, thread_index, barrier)
            for thread_index in range(thread_count)
        ]
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"{label} threads", unit="thread"):
            results.append(future.result())

    return results, time.perf_counter() - started


def sql_final_state(args: argparse.Namespace) -> dict[str, int]:
    with postgres_connect(args) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT stock_quantity FROM products WHERE product_id = %s",
                (args.flash_product_id,),
            )
            stock_row = cur.fetchone()
            final_stock = int(stock_row[0]) if stock_row is not None else -1

            cur.execute(
                """
                SELECT COUNT(DISTINCT o.order_id)
                FROM orders o
                JOIN order_items oi ON oi.order_id = o.order_id
                WHERE o.order_id = %s
                  AND oi.product_id = %s
                """,
                (args.flash_order_id, args.flash_product_id),
            )
            created_orders = int(cur.fetchone()[0])

    return {
        "finalStock": final_stock,
        "createdOrders": created_orders,
    }


def mongo_final_state(mongo_db, args: argparse.Namespace) -> dict[str, int]:
    product = mongo_db.products.find_one(
        {"_id": args.flash_product_id},
        projection={"stockQuantity": 1, "purchaseLog": 1},
    )
    if product is None:
        return {
            "finalStock": -1,
            "createdOrders": 0,
            "purchaseLogEntries": 0,
        }

    created_orders = mongo_db.orders.count_documents(
        {
            "_id": args.flash_order_id,
            "items.productId": args.flash_product_id,
        }
    )
    purchase_log_entries = len(product.get("purchaseLog", []))

    return {
        "finalStock": int(product.get("stockQuantity", -1)),
        "createdOrders": int(created_orders),
        "purchaseLogEntries": int(purchase_log_entries),
    }


def summarize_engine_result(
    *,
    engine: str,
    mechanism: str,
    thread_count: int,
    results: list[AttemptResult],
    total_elapsed_s: float,
    final_state: dict[str, int],
) -> dict[str, Any]:
    success_count = sum(1 for result in results if result.success)
    failed_count = thread_count - success_count
    errored_count = sum(1 for result in results if result.error)
    latencies = [result.elapsed_ms for result in results if result.elapsed_ms > 0]

    expected_success = 1
    oversold = success_count > expected_success or final_state["finalStock"] < 0
    passed = (
        success_count == expected_success
        and failed_count == thread_count - expected_success
        and errored_count == 0
        and final_state["createdOrders"] == expected_success
        and final_state["finalStock"] == 0
        and not oversold
    )

    if engine == "mongodb":
        passed = passed and final_state.get("purchaseLogEntries") == expected_success

    return {
        "engine": engine,
        "mechanism": mechanism,
        "threads": thread_count,
        "initialStock": 1,
        "successCount": success_count,
        "failedCount": failed_count,
        "erroredCount": errored_count,
        "finalStock": final_state["finalStock"],
        "createdOrders": final_state["createdOrders"],
        "purchaseLogEntries": final_state.get("purchaseLogEntries"),
        "oversold": oversold,
        "passed": passed,
        "totalElapsedSeconds": total_elapsed_s,
        "averageAttemptLatencyMs": statistics.fmean(latencies) if latencies else 0.0,
        "attempts": [asdict(result) for result in sorted(results, key=lambda item: item.thread_index)],
    }


def run_sql_test(args: argparse.Namespace) -> dict[str, Any]:
    setup_sql_flash_sale(args)
    results, total_elapsed_s = run_threaded_attempts(
        label="SQL",
        thread_count=args.threads,
        attempt_fn=lambda thread_index, barrier: sql_attempt_purchase(
            args,
            thread_index=thread_index,
            barrier=barrier,
        ),
    )
    return summarize_engine_result(
        engine="postgresql",
        mechanism="Atomic conditional UPDATE inside one transaction.",
        thread_count=args.threads,
        results=results,
        total_elapsed_s=total_elapsed_s,
        final_state=sql_final_state(args),
    )


def run_mongo_test(mongo_db, args: argparse.Namespace) -> dict[str, Any]:
    setup_mongo_flash_sale(mongo_db, args)
    results, total_elapsed_s = run_threaded_attempts(
        label="Mongo",
        thread_count=args.threads,
        attempt_fn=lambda thread_index, barrier: mongo_attempt_purchase(
            mongo_db,
            args,
            thread_index=thread_index,
            barrier=barrier,
        ),
    )
    return summarize_engine_result(
        engine="mongodb",
        mechanism="Atomic find_one_and_update on one product document.",
        thread_count=args.threads,
        results=results,
        total_elapsed_s=total_elapsed_s,
        final_state=mongo_final_state(mongo_db, args),
    )


def run_synced_concurrency(args: argparse.Namespace) -> dict[str, Any]:
    mongo_client = mongo_connect(args)
    mongo_db = mongo_client[args.mongo_database]
    try:
        if args.run_order == "mongo-first":
            mongo_result = run_mongo_test(mongo_db, args)
            sql_result = run_sql_test(args)
        else:
            sql_result = run_sql_test(args)
            mongo_result = run_mongo_test(mongo_db, args)
    finally:
        mongo_client.close()

    return {
        "challenge": "synced-concurrency",
        "runOrder": args.run_order,
        "flashUserId": args.flash_user_id,
        "flashProductId": args.flash_product_id,
        "flashOrderId": args.flash_order_id,
        "priceCents": args.price_cents,
        "sql": sql_result,
        "mongo": mongo_result,
        "passed": sql_result["passed"] and mongo_result["passed"],
    }


def print_engine_summary(title: str, result: dict[str, Any]) -> None:
    print(f"\n{title}")
    print(f"Mechanism               : {result['mechanism']}")
    print(f"Threads                 : {result['threads']}")
    print(f"Initial stock           : {result['initialStock']}")
    print(f"Successful purchases    : {result['successCount']}")
    print(f"Failed purchases        : {result['failedCount']}")
    print(f"Errored attempts        : {result['erroredCount']}")
    print(f"Created orders          : {result['createdOrders']}")
    if result["purchaseLogEntries"] is not None:
        print(f"Purchase-log entries    : {result['purchaseLogEntries']}")
    print(f"Final stock             : {result['finalStock']}")
    print(f"Oversold                : {result['oversold']}")
    print(f"Passed                  : {result['passed']}")
    print(f"Average attempt latency : {result['averageAttemptLatencyMs']:.3f} ms")


def print_summary(result: dict[str, Any]) -> None:
    print("\nSynchronized Concurrency Result")
    print(f"Run order        : {result['runOrder']}")
    print(f"Flash user id    : {result['flashUserId']}")
    print(f"Flash product id : {result['flashProductId']}")
    print(f"Flash order id   : {result['flashOrderId']}")
    print(f"Overall passed   : {result['passed']}")

    print_engine_summary("PostgreSQL", result["sql"])
    print_engine_summary("MongoDB", result["mongo"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the flash-sale race-condition test on PostgreSQL and MongoDB."
    )
    parser.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PGPORT", "5436")))
    parser.add_argument("--pg-database", default=os.getenv("PGDATABASE", "ecommerce"))
    parser.add_argument("--pg-user", default=os.getenv("PGUSER", "assignment2"))
    parser.add_argument("--pg-password", default=os.getenv("PGPASSWORD", ""))
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", DEFAULT_MONGO_URI))
    parser.add_argument("--mongo-database", default=os.getenv("MONGO_DATABASE", "ecommerce"))
    parser.add_argument("--threads", type=parse_count, default=parse_count(os.getenv("CONCURRENCY_THREADS", "50")))
    parser.add_argument("--flash-user-id", type=int, default=int(os.getenv("FLASH_USER_ID", str(DEFAULT_FLASH_USER_ID))))
    parser.add_argument("--flash-product-id", type=int, default=int(os.getenv("FLASH_PRODUCT_ID", str(DEFAULT_FLASH_PRODUCT_ID))))
    parser.add_argument("--flash-order-id", type=int, default=int(os.getenv("FLASH_ORDER_ID", str(DEFAULT_FLASH_ORDER_ID))))
    parser.add_argument("--flash-order-item-id", type=int, default=int(os.getenv("FLASH_ORDER_ITEM_ID", str(DEFAULT_FLASH_ORDER_ITEM_ID))))
    parser.add_argument("--price-cents", type=parse_count, default=parse_count(os.getenv("FLASH_PRICE_CENTS", str(DEFAULT_PRICE_CENTS))))
    parser.add_argument("--barrier-timeout", type=float, default=float(os.getenv("BARRIER_TIMEOUT_SECONDS", "30")))
    parser.add_argument(
        "--run-order",
        choices=("sql-first", "mongo-first"),
        default=os.getenv("CONCURRENCY_RUN_ORDER", "sql-first"),
        help="Which database to test first.",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON result after the summary.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_synced_concurrency(args)
    print_summary(result)
    if args.json:
        print("\nJSON Result")
        print(json.dumps(result, indent=2, default=json_default))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
