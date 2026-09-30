#!/usr/bin/env python3
"""
Assignment 02 - Part 3 concurrency challenge for PostgreSQL.

The script creates/resets one flash-sale product with stock_quantity = 1,
then starts 50 concurrent purchase attempts by default.

Race-condition protection:
  UPDATE products
  SET stock_quantity = stock_quantity - 1
  WHERE product_id = :id AND stock_quantity > 0
  RETURNING stock_quantity

PostgreSQL executes this as an atomic row update. Only one transaction can
successfully decrement stock from 1 to 0; all other transactions see no row
updated and must fail without creating an order.

Default connection is loaded from .env:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

Install dependencies:
  pip install -r requirements.txt

Example:
  python "3.1 concurrency_sql.py"
  python "3.1 concurrency_sql.py" --threads 50
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
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from config_env import load_env_file

load_env_file()

try:
    import psycopg
    from tqdm import tqdm
except ImportError:
    print(
        "Missing dependency. Install required packages with: pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise


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


def money_from_cents(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"


def progress(message: str) -> None:
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    tqdm.write(f"[{stamp}] {message}")


def json_default(value: Any) -> str | int | float:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def connect(args: argparse.Namespace) -> "psycopg.Connection":
    return psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
        application_name="assignment2_sql_concurrency",
    )


def setup_flash_sale_data(args: argparse.Namespace) -> None:
    progress("Preparing PostgreSQL flash-sale user and product with stock_quantity = 1")
    now = datetime.now(timezone.utc)
    price = money_from_cents(args.price_cents)

    with connect(args) as conn:
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
                    f"flash-sale-user-{args.flash_user_id}@example.com",
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


def attempt_purchase(
    args: argparse.Namespace,
    *,
    thread_index: int,
    barrier: threading.Barrier,
    base_order_id: int,
    base_order_item_id: int,
) -> AttemptResult:
    conn: psycopg.Connection | None = None
    order_id = base_order_id + thread_index
    order_item_id = base_order_item_id + thread_index

    try:
        conn = connect(args)
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
                return AttemptResult(
                    thread_index=thread_index,
                    success=False,
                    elapsed_ms=elapsed_ms,
                    final_stock_seen=None,
                )

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
                    order_id,
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
                    order_item_id,
                    order_id,
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
            order_id=order_id,
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


def get_final_state(
    args: argparse.Namespace,
    *,
    base_order_id: int,
) -> dict[str, int]:
    with connect(args) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT stock_quantity FROM products WHERE product_id = %s",
                (args.flash_product_id,),
            )
            stock_row = cur.fetchone()
            final_stock = int(stock_row[0]) if stock_row is not None else -1

            cur.execute(
                """
                SELECT COUNT(*)
                FROM orders o
                JOIN order_items oi ON oi.order_id = o.order_id
                WHERE oi.product_id = %s
                  AND o.order_id >= %s
                  AND o.order_id < %s
                """,
                (args.flash_product_id, base_order_id, base_order_id + args.threads),
            )
            created_orders = int(cur.fetchone()[0])

    return {
        "finalStock": final_stock,
        "createdOrdersInThisRun": created_orders,
    }


def run_concurrency_test(args: argparse.Namespace) -> dict[str, Any]:
    setup_flash_sale_data(args)

    base_order_id = int(time.time() * 1_000_000)
    base_order_item_id = base_order_id + 1_000_000
    barrier = threading.Barrier(args.threads)
    results: list[AttemptResult] = []

    progress(f"Starting {args.threads} concurrent PostgreSQL purchase attempts")
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = [
            executor.submit(
                attempt_purchase,
                args,
                thread_index=thread_index,
                barrier=barrier,
                base_order_id=base_order_id,
                base_order_item_id=base_order_item_id,
            )
            for thread_index in range(args.threads)
        ]
        for future in tqdm(as_completed(futures), total=len(futures), desc="SQL threads", unit="thread"):
            results.append(future.result())

    total_elapsed_s = time.perf_counter() - started
    final_state = get_final_state(args, base_order_id=base_order_id)

    success_count = sum(1 for result in results if result.success)
    failed_count = args.threads - success_count
    errored_count = sum(1 for result in results if result.error)
    expected_success = 1
    oversold = success_count > expected_success or final_state["finalStock"] < 0
    passed = (
        success_count == expected_success
        and final_state["createdOrdersInThisRun"] == expected_success
        and final_state["finalStock"] == 0
        and not oversold
    )

    latencies = [result.elapsed_ms for result in results if result.elapsed_ms > 0]
    return {
        "engine": "postgresql",
        "mechanism": "Atomic conditional UPDATE inside a transaction: stock_quantity decrements only when stock_quantity > 0.",
        "threads": args.threads,
        "initialStock": 1,
        "successCount": success_count,
        "failedCount": failed_count,
        "erroredCount": errored_count,
        "finalStock": final_state["finalStock"],
        "createdOrdersInThisRun": final_state["createdOrdersInThisRun"],
        "oversold": oversold,
        "passed": passed,
        "totalElapsedSeconds": total_elapsed_s,
        "averageAttemptLatencyMs": sum(latencies) / len(latencies) if latencies else 0.0,
        "attempts": [asdict(result) for result in sorted(results, key=lambda item: item.thread_index)],
    }


def print_summary(result: dict[str, Any]) -> None:
    print("\nPostgreSQL Concurrency Result")
    print(f"Mechanism                : {result['mechanism']}")
    print(f"Threads                  : {result['threads']}")
    print(f"Initial stock            : {result['initialStock']}")
    print(f"Successful purchases     : {result['successCount']}")
    print(f"Failed purchases         : {result['failedCount']}")
    print(f"Errored attempts         : {result['erroredCount']}")
    print(f"Created orders this run  : {result['createdOrdersInThisRun']}")
    print(f"Final stock              : {result['finalStock']}")
    print(f"Oversold                 : {result['oversold']}")
    print(f"Passed                   : {result['passed']}")
    print(f"Average attempt latency  : {result['averageAttemptLatencyMs']:.3f} ms")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run PostgreSQL flash-sale concurrency test with atomic stock update."
    )
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5436")))
    parser.add_argument("--database", default=os.getenv("PGDATABASE", "ecommerce"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "assignment2"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", ""))
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
