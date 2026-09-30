#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Iterator, Sequence

from config_env import build_mongo_uri, load_env_file

load_env_file()

try:
    import psycopg
    from bson.decimal128 import Decimal128
    from psycopg.rows import dict_row
    from pymongo import ASCENDING, MongoClient
    from tqdm import tqdm
except ImportError:
    print(
        "Missing dependency. Install required packages with: pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise


STATUSES = ("PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED")
CATEGORIES = (
    "electronics",
    "fashion",
    "home",
    "beauty",
    "sports",
    "books",
    "toys",
    "grocery",
)
CURRENCY = "USD"
TWO_YEARS_IN_SECONDS = 730 * 24 * 60 * 60
DEFAULT_MONGO_URI = build_mongo_uri()


def parse_count(value: str) -> int:
    number = int(value.replace("_", "").replace(",", ""))
    if number <= 0:
        raise argparse.ArgumentTypeError("value must be greater than 0")
    return number


def parse_base_time(value: str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc).replace(microsecond=0)

    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def progress(message: str) -> None:
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    tqdm.write(f"[{stamp}] {message}")


def money_from_cents(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"


def cents_from_decimal(value: Decimal) -> int:
    return int((value * Decimal("100")).to_integral_value())


def decimal128_from_cents(cents: int) -> Decimal128:
    return Decimal128(money_from_cents(cents))


def product_price_cents(product_id: int) -> int:
    # Stable pseudo-random price from 5.00 to 1,999.99.
    return 500 + ((product_id * 7919) % 199_500)


def product_category(product_id: int) -> str:
    return CATEGORIES[(product_id - 1) % len(CATEGORIES)]


def product_sku(product_id: int) -> str:
    return f"SKU-{product_id:08d}"


def product_name(product_id: int) -> str:
    return f"{product_category(product_id).title()} Product {product_id:08d}"


def user_email(user_id: int) -> str:
    return f"user{user_id:09d}@example.com"


def user_full_name(user_id: int) -> str:
    return f"User {user_id:09d}"


def pick_status(rng: random.Random) -> str:
    value = rng.random()
    if value < 0.08:
        return "PENDING"
    if value < 0.63:
        return "PAID"
    if value < 0.82:
        return "SHIPPED"
    if value < 0.94:
        return "DELIVERED"
    if value < 0.985:
        return "CANCELLED"
    return "REFUNDED"


def random_order_date(rng: random.Random, base_time: datetime) -> datetime:
    offset_seconds = rng.randrange(TWO_YEARS_IN_SECONDS)
    return base_time - timedelta(seconds=offset_seconds)


def batched_range(start: int, end: int, batch_size: int) -> Iterator[tuple[int, int]]:
    current = start
    while current <= end:
        batch_end = min(current + batch_size - 1, end)
        yield current, batch_end
        current = batch_end + 1


def batch_total(record_count: int, batch_size: int) -> int:
    return (record_count + batch_size - 1) // batch_size


def copy_rows(conn: "psycopg.Connection", copy_sql: str, rows: Iterable[Sequence[object]]) -> None:
    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            for row in rows:
                copy.write_row(row)


def postgres_connect(args: argparse.Namespace) -> "psycopg.Connection":
    return psycopg.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
        application_name="assignment2_synced_data_generator",
    )


def mongo_connect(args: argparse.Namespace) -> MongoClient:
    return MongoClient(args.mongo_uri, tz_aware=True, tzinfo=timezone.utc)


def reset_postgres(conn: "psycopg.Connection") -> None:
    progress("Dropping existing PostgreSQL tables")
    statements = (
        "DROP TABLE IF EXISTS order_items",
        "DROP TABLE IF EXISTS orders",
        "DROP TABLE IF EXISTS products",
        "DROP TABLE IF EXISTS users",
    )
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
    conn.commit()


def reset_mongo(db) -> None:
    progress("Dropping existing MongoDB collections")
    db.orders.drop()
    db.users.drop()
    db.products.drop()


def create_postgres_schema(conn: "psycopg.Connection") -> None:
    progress("Creating normalized PostgreSQL schema")
    status_values = ", ".join(f"'{status}'" for status in STATUSES)
    statements = (
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            full_name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id BIGINT PRIMARY KEY,
            sku TEXT NOT NULL UNIQUE,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            current_price NUMERIC(12, 2) NOT NULL CHECK (current_price > 0),
            stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0),
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS orders (
            order_id BIGINT PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(user_id),
            order_status TEXT NOT NULL CHECK (order_status IN ({status_values})),
            order_date TIMESTAMPTZ NOT NULL,
            currency CHAR(3) NOT NULL DEFAULT '{CURRENCY}',
            total_amount NUMERIC(14, 2) NOT NULL CHECK (total_amount >= 0),
            created_at TIMESTAMPTZ NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id BIGINT PRIMARY KEY,
            order_id BIGINT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
            product_id BIGINT NOT NULL REFERENCES products(product_id),
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
            line_total NUMERIC(14, 2) NOT NULL CHECK (line_total >= 0),
            UNIQUE (order_id, product_id)
        )
        """,
    )
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
    conn.commit()


def create_postgres_indexes(conn: "psycopg.Connection") -> None:
    progress("Creating PostgreSQL indexes")
    statements = (
        "CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date)",
        "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(order_status)",
        "CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id)",
    )
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
    conn.commit()


def create_mongo_indexes(db) -> None:
    progress("Creating MongoDB indexes")
    db.users.create_index([("email", ASCENDING)], unique=True, name="uq_users_email")
    db.products.create_index([("sku", ASCENDING)], unique=True, name="uq_products_sku")
    db.orders.create_index([("orderDate", ASCENDING)], name="idx_orders_order_date")
    db.orders.create_index([("status", ASCENDING), ("orderDate", ASCENDING)], name="idx_orders_status_date")
    db.orders.create_index([("user.userId", ASCENDING)], name="idx_orders_user_id")


def analyze_postgres(conn: "psycopg.Connection") -> None:
    progress("Running PostgreSQL ANALYZE")
    with conn.cursor() as cur:
        for table_name in ("users", "products", "orders", "order_items"):
            cur.execute(f"ANALYZE {table_name}")
    conn.commit()


def insert_many(collection, documents: list[dict[str, Any]]) -> None:
    if documents:
        collection.insert_many(documents, ordered=False, bypass_document_validation=True)


def user_payload(user_id: int, base_time: datetime) -> tuple[tuple[object, ...], dict[str, Any]]:
    created_at = base_time - timedelta(days=user_id % 1095)
    email = user_email(user_id)
    full_name = user_full_name(user_id)
    sql_row = (user_id, email, full_name, created_at)
    mongo_doc = {
        "_id": user_id,
        "userId": user_id,
        "email": email,
        "fullName": full_name,
        "createdAt": created_at,
    }
    return sql_row, mongo_doc


def product_payload(product_id: int, base_time: datetime) -> tuple[tuple[object, ...], dict[str, Any]]:
    created_at = base_time - timedelta(days=product_id % 730)
    price_cents = product_price_cents(product_id)
    category = product_category(product_id)
    sku = product_sku(product_id)
    name = product_name(product_id)
    stock_quantity = (product_id * 13) % 500
    sql_row = (
        product_id,
        sku,
        name,
        category,
        money_from_cents(price_cents),
        stock_quantity,
        created_at,
    )
    mongo_doc = {
        "_id": product_id,
        "productId": product_id,
        "sku": sku,
        "name": name,
        "category": category,
        "currentPrice": decimal128_from_cents(price_cents),
        "currentPriceCents": price_cents,
        "currency": CURRENCY,
        "stockQuantity": stock_quantity,
        "createdAt": created_at,
    }
    return sql_row, mongo_doc


def user_snapshot(user_id: int) -> dict[str, Any]:
    return {
        "userId": user_id,
        "email": user_email(user_id),
        "fullName": user_full_name(user_id),
    }


def item_snapshot(product_id: int, quantity: int) -> dict[str, Any]:
    unit_price_cents = product_price_cents(product_id)
    line_total_cents = unit_price_cents * quantity
    return {
        "productId": product_id,
        "sku": product_sku(product_id),
        "name": product_name(product_id),
        "category": product_category(product_id),
        "quantity": quantity,
        "unitPrice": decimal128_from_cents(unit_price_cents),
        "unitPriceCents": unit_price_cents,
        "lineTotal": decimal128_from_cents(line_total_cents),
        "lineTotalCents": line_total_cents,
    }


def build_order_batch(
    *,
    order_start: int,
    order_end: int,
    next_order_item_id: int,
    user_count: int,
    product_count: int,
    min_items: int,
    max_items: int,
    rng: random.Random,
    base_time: datetime,
) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]], list[dict[str, Any]], int]:
    sql_order_rows: list[tuple[object, ...]] = []
    sql_item_rows: list[tuple[object, ...]] = []
    mongo_order_docs: list[dict[str, Any]] = []
    product_id_space = range(1, product_count + 1)

    for order_id in range(order_start, order_end + 1):
        item_count = rng.randint(min_items, max_items)
        product_ids = rng.sample(product_id_space, item_count)
        items: list[dict[str, Any]] = []
        total_cents = 0

        for product_id in product_ids:
            quantity = rng.randint(1, 4)
            item = item_snapshot(product_id, quantity)
            unit_price_cents = int(item["unitPriceCents"])
            line_total_cents = int(item["lineTotalCents"])
            total_cents += line_total_cents
            items.append(item)

            sql_item_rows.append(
                (
                    next_order_item_id,
                    order_id,
                    product_id,
                    quantity,
                    money_from_cents(unit_price_cents),
                    money_from_cents(line_total_cents),
                )
            )
            next_order_item_id += 1

        order_date = random_order_date(rng, base_time)
        user_id = rng.randint(1, user_count)
        status = pick_status(rng)
        total_amount = money_from_cents(total_cents)

        sql_order_rows.append(
            (
                order_id,
                user_id,
                status,
                order_date,
                CURRENCY,
                total_amount,
                order_date,
            )
        )
        mongo_order_docs.append(
            {
                "_id": order_id,
                "orderId": order_id,
                "user": user_snapshot(user_id),
                "status": status,
                "orderDate": order_date,
                "currency": CURRENCY,
                "totalAmount": decimal128_from_cents(total_cents),
                "totalAmountCents": total_cents,
                "items": items,
                "createdAt": order_date,
            }
        )

    return sql_order_rows, sql_item_rows, mongo_order_docs, next_order_item_id


def seed_users(
    pg_conn: "psycopg.Connection",
    mongo_db,
    *,
    user_count: int,
    batch_size: int,
    base_time: datetime,
) -> None:
    progress(f"Seeding {user_count:,} synchronized users")
    copy_sql = "COPY users (user_id, email, full_name, created_at) FROM STDIN"
    started = time.perf_counter()

    batches = tqdm(
        batched_range(1, user_count, batch_size),
        total=batch_total(user_count, batch_size),
        desc="Synced users",
        unit="batch",
        dynamic_ncols=True,
    )
    for batch_start, batch_end in batches:
        sql_rows: list[tuple[object, ...]] = []
        mongo_docs: list[dict[str, Any]] = []
        for user_id in range(batch_start, batch_end + 1):
            sql_row, mongo_doc = user_payload(user_id, base_time)
            sql_rows.append(sql_row)
            mongo_docs.append(mongo_doc)

        try:
            copy_rows(pg_conn, copy_sql, sql_rows)
            insert_many(mongo_db.users, mongo_docs)
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()
            raise

    progress(f"Finished synchronized users in {time.perf_counter() - started:.1f}s")


def seed_products(
    pg_conn: "psycopg.Connection",
    mongo_db,
    *,
    product_count: int,
    batch_size: int,
    base_time: datetime,
) -> None:
    progress(f"Seeding {product_count:,} synchronized products")
    copy_sql = """
        COPY products (
            product_id, sku, product_name, category, current_price, stock_quantity, created_at
        ) FROM STDIN
    """
    started = time.perf_counter()

    batches = tqdm(
        batched_range(1, product_count, batch_size),
        total=batch_total(product_count, batch_size),
        desc="Synced products",
        unit="batch",
        dynamic_ncols=True,
    )
    for batch_start, batch_end in batches:
        sql_rows: list[tuple[object, ...]] = []
        mongo_docs: list[dict[str, Any]] = []
        for product_id in range(batch_start, batch_end + 1):
            sql_row, mongo_doc = product_payload(product_id, base_time)
            sql_rows.append(sql_row)
            mongo_docs.append(mongo_doc)

        try:
            copy_rows(pg_conn, copy_sql, sql_rows)
            insert_many(mongo_db.products, mongo_docs)
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()
            raise

    progress(f"Finished synchronized products in {time.perf_counter() - started:.1f}s")


def seed_orders(
    pg_conn: "psycopg.Connection",
    mongo_db,
    *,
    order_count: int,
    user_count: int,
    product_count: int,
    batch_size: int,
    min_items: int,
    max_items: int,
    seed: int,
    base_time: datetime,
) -> None:
    progress(f"Seeding {order_count:,} synchronized orders with {min_items}-{max_items} items each")
    copy_orders_sql = """
        COPY orders (
            order_id, user_id, order_status, order_date, currency, total_amount, created_at
        ) FROM STDIN
    """
    copy_items_sql = """
        COPY order_items (
            order_item_id, order_id, product_id, quantity, unit_price, line_total
        ) FROM STDIN
    """
    rng = random.Random(seed)
    next_order_item_id = 1
    started = time.perf_counter()

    order_batches = tqdm(
        batched_range(1, order_count, batch_size),
        total=batch_total(order_count, batch_size),
        desc="Synced orders",
        unit="batch",
        dynamic_ncols=True,
    )
    for batch_start, batch_end in order_batches:
        sql_order_rows, sql_item_rows, mongo_docs, next_order_item_id = build_order_batch(
            order_start=batch_start,
            order_end=batch_end,
            next_order_item_id=next_order_item_id,
            user_count=user_count,
            product_count=product_count,
            min_items=min_items,
            max_items=max_items,
            rng=rng,
            base_time=base_time,
        )

        try:
            copy_rows(pg_conn, copy_orders_sql, sql_order_rows)
            copy_rows(pg_conn, copy_items_sql, sql_item_rows)
            insert_many(mongo_db.orders, mongo_docs)
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()
            raise

        order_batches.set_postfix(
            orders=f"{batch_end:,}",
            items=f"{next_order_item_id - 1:,}",
        )

    progress(f"Finished synchronized orders in {time.perf_counter() - started:.1f}s")


def utc_iso_seconds(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def sql_order_for_compare(pg_conn: "psycopg.Connection", order_id: int) -> dict[str, Any] | None:
    query = """
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
    with pg_conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (order_id,))
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


def mongo_order_for_compare(mongo_db, order_id: int) -> dict[str, Any] | None:
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


def validate_sample(
    pg_conn: "psycopg.Connection",
    mongo_db,
    *,
    order_count: int,
    sample_size: int,
    seed: int,
) -> None:
    if sample_size <= 0:
        return

    progress(f"Validating {sample_size:,} sampled orders across PostgreSQL and MongoDB")
    rng = random.Random(seed + 1_000_003)
    mismatches: list[dict[str, Any]] = []

    for _ in tqdm(range(sample_size), desc="Sync validation", unit="order", dynamic_ncols=True):
        order_id = rng.randint(1, order_count)
        sql_order = sql_order_for_compare(pg_conn, order_id)
        mongo_order = mongo_order_for_compare(mongo_db, order_id)
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

    if mismatches:
        print(json.dumps(mismatches, indent=2, default=json_default), file=sys.stderr)
        raise RuntimeError("Synchronized data validation failed. See mismatches above.")

    progress("Synchronized data validation passed")


def json_default(value: Any) -> str | int | float:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Decimal128):
        return str(value.to_decimal())
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one identical dataset into PostgreSQL and MongoDB."
    )
    parser.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PGPORT", "5436")))
    parser.add_argument("--pg-database", default=os.getenv("PGDATABASE", "ecommerce"))
    parser.add_argument("--pg-user", default=os.getenv("PGUSER", "assignment2"))
    parser.add_argument("--pg-password", default=os.getenv("PGPASSWORD", ""))
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", DEFAULT_MONGO_URI))
    parser.add_argument("--mongo-database", default=os.getenv("MONGO_DATABASE", "ecommerce"))
    parser.add_argument("--orders", type=parse_count, default=parse_count(os.getenv("ORDER_COUNT", "10_000_000")))
    parser.add_argument("--users", type=parse_count, default=parse_count(os.getenv("USER_COUNT", "1_000_000")))
    parser.add_argument("--products", type=parse_count, default=parse_count(os.getenv("PRODUCT_COUNT", "100_000")))
    parser.add_argument("--batch-size", type=parse_count, default=parse_count(os.getenv("SYNC_BATCH_SIZE", "10_000")))
    parser.add_argument("--min-items", type=parse_count, default=parse_count(os.getenv("MIN_ITEMS", "1")))
    parser.add_argument("--max-items", type=parse_count, default=parse_count(os.getenv("MAX_ITEMS", "5")))
    parser.add_argument("--seed", type=int, default=int(os.getenv("DATA_SEED", "20260525")))
    parser.add_argument("--base-time", default=os.getenv("DATA_BASE_TIME"))
    parser.add_argument("--reset", action="store_true", help="Drop PostgreSQL tables and MongoDB collections first.")
    parser.add_argument("--schema-only", action="store_true", help="Create schema/indexes without inserting data.")
    parser.add_argument("--no-indexes", action="store_true", help="Skip secondary index creation.")
    parser.add_argument("--no-analyze", action="store_true", help="Skip PostgreSQL ANALYZE after loading data.")
    parser.add_argument("--validate-sample", type=int, default=int(os.getenv("VALIDATE_SAMPLE", "100")))
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.min_items > args.max_items:
        raise ValueError("--min-items must be less than or equal to --max-items")
    if args.products < args.max_items:
        raise ValueError("--products must be greater than or equal to --max-items")
    if args.validate_sample < 0:
        raise ValueError("--validate-sample must be greater than or equal to 0")


def main() -> int:
    args = parse_args()
    validate_args(args)
    base_time = parse_base_time(args.base_time)

    progress(
        "Connecting to PostgreSQL "
        f"{args.pg_user}@{args.pg_host}:{args.pg_port}/{args.pg_database}"
    )
    pg_conn = postgres_connect(args)
    progress(f"Connecting to MongoDB database '{args.mongo_database}'")
    mongo_client = mongo_connect(args)
    mongo_db = mongo_client[args.mongo_database]

    try:
        with pg_conn.cursor() as cur:
            cur.execute("SET synchronous_commit = OFF")
        pg_conn.commit()

        if args.reset:
            reset_postgres(pg_conn)
            reset_mongo(mongo_db)

        create_postgres_schema(pg_conn)

        if args.schema_only:
            if not args.no_indexes:
                create_postgres_indexes(pg_conn)
                create_mongo_indexes(mongo_db)
            progress("Schema-only mode completed")
            return 0

        progress(
            "Generating synchronized dataset with "
            f"base_time={base_time.isoformat()}, seed={args.seed}"
        )
        seed_users(
            pg_conn,
            mongo_db,
            user_count=args.users,
            batch_size=args.batch_size,
            base_time=base_time,
        )
        seed_products(
            pg_conn,
            mongo_db,
            product_count=args.products,
            batch_size=args.batch_size,
            base_time=base_time,
        )
        seed_orders(
            pg_conn,
            mongo_db,
            order_count=args.orders,
            user_count=args.users,
            product_count=args.products,
            batch_size=args.batch_size,
            min_items=args.min_items,
            max_items=args.max_items,
            seed=args.seed,
            base_time=base_time,
        )

        if not args.no_indexes:
            create_postgres_indexes(pg_conn)
            create_mongo_indexes(mongo_db)
        if not args.no_analyze:
            analyze_postgres(pg_conn)

        validate_sample(
            pg_conn,
            mongo_db,
            order_count=args.orders,
            sample_size=args.validate_sample,
            seed=args.seed,
        )
    finally:
        pg_conn.close()
        mongo_client.close()

    progress("Synchronized data generation completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
