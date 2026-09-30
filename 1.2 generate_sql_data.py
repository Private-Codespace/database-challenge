from __future__ import annotations

import argparse
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator, Sequence

from config_env import load_env_file

load_env_file()

try:
    import psycopg
    from tqdm import tqdm
except ImportError:
    print(
        'Missing dependency. Install required packages with: pip install -r requirements.txt',
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


def parse_count(value: str) -> int:
    number = int(value.replace("_", "").replace(",", ""))
    if number <= 0:
        raise argparse.ArgumentTypeError("value must be greater than 0")
    return number


def money_from_cents(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"


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


def random_order_date(rng: random.Random, now: datetime) -> str:
    offset_seconds = rng.randrange(TWO_YEARS_IN_SECONDS)
    return (now - timedelta(seconds=offset_seconds)).isoformat(timespec="seconds")


def progress(message: str) -> None:
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    tqdm.write(f"[{stamp}] {message}")


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


def reset_schema(conn: "psycopg.Connection") -> None:
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


def create_schema(conn: "psycopg.Connection") -> None:
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


def create_indexes(conn: "psycopg.Connection") -> None:
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


def analyze_tables(conn: "psycopg.Connection") -> None:
    progress("Running ANALYZE")
    with conn.cursor() as cur:
        for table_name in ("users", "products", "orders", "order_items"):
            cur.execute(f"ANALYZE {table_name}")
    conn.commit()


def seed_users(conn: "psycopg.Connection", user_count: int, batch_size: int, now: datetime) -> None:
    progress(f"Seeding {user_count:,} users")
    copy_sql = "COPY users (user_id, email, full_name, created_at) FROM STDIN"
    started = time.perf_counter()
    for batch_start, batch_end in tqdm(
        batched_range(1, user_count, batch_size),
        total=batch_total(user_count, batch_size),
        desc="SQL users",
        unit="batch",
        dynamic_ncols=True,
    ):
        rows = (
            (
                user_id,
                user_email(user_id),
                user_full_name(user_id),
                (now - timedelta(days=user_id % 1095)).isoformat(timespec="seconds"),
            )
            for user_id in range(batch_start, batch_end + 1)
        )
        copy_rows(conn, copy_sql, rows)
        conn.commit()
    progress(f"Finished users in {time.perf_counter() - started:.1f}s")


def seed_products(conn: "psycopg.Connection", product_count: int, batch_size: int, now: datetime) -> None:
    progress(f"Seeding {product_count:,} products")
    copy_sql = """
        COPY products (
            product_id, sku, product_name, category, current_price, stock_quantity, created_at
        ) FROM STDIN
    """
    started = time.perf_counter()
    for batch_start, batch_end in tqdm(
        batched_range(1, product_count, batch_size),
        total=batch_total(product_count, batch_size),
        desc="SQL products",
        unit="batch",
        dynamic_ncols=True,
    ):
        rows = (
            (
                product_id,
                product_sku(product_id),
                product_name(product_id),
                product_category(product_id),
                money_from_cents(product_price_cents(product_id)),
                (product_id * 13) % 500,
                (now - timedelta(days=product_id % 730)).isoformat(timespec="seconds"),
            )
            for product_id in range(batch_start, batch_end + 1)
        )
        copy_rows(conn, copy_sql, rows)
        conn.commit()
    progress(f"Finished products in {time.perf_counter() - started:.1f}s")


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
    now: datetime,
) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]], int]:
    order_rows: list[tuple[object, ...]] = []
    item_rows: list[tuple[object, ...]] = []
    product_id_space = range(1, product_count + 1)

    for order_id in range(order_start, order_end + 1):
        item_count = rng.randint(min_items, max_items)
        product_ids = rng.sample(product_id_space, item_count)
        total_cents = 0

        for product_id in product_ids:
            quantity = rng.randint(1, 4)
            unit_price_cents = product_price_cents(product_id)
            line_total_cents = unit_price_cents * quantity
            total_cents += line_total_cents

            item_rows.append(
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

        order_date = random_order_date(rng, now)
        order_rows.append(
            (
                order_id,
                rng.randint(1, user_count),
                pick_status(rng),
                order_date,
                CURRENCY,
                money_from_cents(total_cents),
                order_date,
            )
        )

    return order_rows, item_rows, next_order_item_id


def seed_orders_and_items(
    conn: "psycopg.Connection",
    *,
    order_count: int,
    user_count: int,
    product_count: int,
    batch_size: int,
    min_items: int,
    max_items: int,
    seed: int,
    now: datetime,
) -> None:
    progress(f"Seeding {order_count:,} orders with {min_items}-{max_items} items each")
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
        desc="SQL orders",
        unit="batch",
        dynamic_ncols=True,
    )
    for batch_start, batch_end in order_batches:
        order_rows, item_rows, next_order_item_id = build_order_batch(
            order_start=batch_start,
            order_end=batch_end,
            next_order_item_id=next_order_item_id,
            user_count=user_count,
            product_count=product_count,
            min_items=min_items,
            max_items=max_items,
            rng=rng,
            now=now,
        )
        copy_rows(conn, copy_orders_sql, order_rows)
        copy_rows(conn, copy_items_sql, item_rows)
        conn.commit()
        order_batches.set_postfix(
            orders=f"{batch_end:,}",
            items=f"{next_order_item_id - 1:,}",
        )

    progress(f"Finished orders and order_items in {time.perf_counter() - started:.1f}s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create PostgreSQL 3NF schema and seed e-commerce benchmark data."
    )
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5436")))
    parser.add_argument("--database", default=os.getenv("PGDATABASE", "ecommerce"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "assignment2"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", ""))
    parser.add_argument("--orders", type=parse_count, default=parse_count(os.getenv("ORDER_COUNT", "10_000_000")))
    parser.add_argument("--users", type=parse_count, default=parse_count(os.getenv("USER_COUNT", "1_000_000")))
    parser.add_argument("--products", type=parse_count, default=parse_count(os.getenv("PRODUCT_COUNT", "100_000")))
    parser.add_argument("--batch-size", type=parse_count, default=parse_count(os.getenv("SQL_BATCH_SIZE", "50_000")))
    parser.add_argument("--min-items", type=parse_count, default=parse_count(os.getenv("MIN_ITEMS", "1")))
    parser.add_argument("--max-items", type=parse_count, default=parse_count(os.getenv("MAX_ITEMS", "5")))
    parser.add_argument("--seed", type=int, default=int(os.getenv("DATA_SEED", "20260525")))
    parser.add_argument("--reset", action="store_true", help="Drop and recreate tables before inserting data.")
    parser.add_argument("--schema-only", action="store_true", help="Create schema and indexes without inserting data.")
    parser.add_argument("--no-indexes", action="store_true", help="Skip secondary index creation.")
    parser.add_argument("--no-analyze", action="store_true", help="Skip ANALYZE after loading data.")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.min_items > args.max_items:
        raise ValueError("--min-items must be less than or equal to --max-items")
    if args.products < args.max_items:
        raise ValueError("--products must be greater than or equal to --max-items")


def main() -> int:
    args = parse_args()
    validate_args(args)
    now = datetime.now(timezone.utc)

    progress(
        "Connecting to PostgreSQL "
        f"{args.user}@{args.host}:{args.port}/{args.database}"
    )
    with psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
        application_name="assignment2_sql_data_generator",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SET synchronous_commit = OFF")
        conn.commit()

        if args.reset:
            reset_schema(conn)
        create_schema(conn)

        if args.schema_only:
            if not args.no_indexes:
                create_indexes(conn)
            progress("Schema-only mode completed")
            return 0

        seed_users(conn, args.users, args.batch_size, now)
        seed_products(conn, args.products, args.batch_size, now)
        seed_orders_and_items(
            conn,
            order_count=args.orders,
            user_count=args.users,
            product_count=args.products,
            batch_size=args.batch_size,
            min_items=args.min_items,
            max_items=args.max_items,
            seed=args.seed,
            now=now,
        )

        if not args.no_indexes:
            create_indexes(conn)
        if not args.no_analyze:
            analyze_tables(conn)

    progress("PostgreSQL data generation completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
