from __future__ import annotations

import argparse
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

from config_env import build_mongo_uri, load_env_file

load_env_file()

try:
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


def random_order_date(rng: random.Random, now: datetime) -> datetime:
    offset_seconds = rng.randrange(TWO_YEARS_IN_SECONDS)
    return now - timedelta(seconds=offset_seconds)


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


def reset_collections(db) -> None:
    progress("Dropping existing MongoDB collections")
    db.orders.drop()
    db.users.drop()
    db.products.drop()


def create_indexes(db) -> None:
    progress("Creating MongoDB indexes")
    db.users.create_index([("email", ASCENDING)], unique=True, name="uq_users_email")
    db.products.create_index([("sku", ASCENDING)], unique=True, name="uq_products_sku")
    db.orders.create_index([("orderDate", ASCENDING)], name="idx_orders_order_date")
    db.orders.create_index([("status", ASCENDING), ("orderDate", ASCENDING)], name="idx_orders_status_date")
    db.orders.create_index([("user.userId", ASCENDING)], name="idx_orders_user_id")


def user_document(user_id: int, now: datetime) -> dict:
    return {
        "_id": user_id,
        "userId": user_id,
        "email": user_email(user_id),
        "fullName": user_full_name(user_id),
        "createdAt": now - timedelta(days=user_id % 1095),
    }


def product_document(product_id: int, now: datetime) -> dict:
    return {
        "_id": product_id,
        "productId": product_id,
        "sku": product_sku(product_id),
        "name": product_name(product_id),
        "category": product_category(product_id),
        "currentPriceCents": product_price_cents(product_id),
        "currency": CURRENCY,
        "stockQuantity": (product_id * 13) % 500,
        "createdAt": now - timedelta(days=product_id % 730),
    }


def user_snapshot(user_id: int) -> dict:
    return {
        "userId": user_id,
        "email": user_email(user_id),
        "fullName": user_full_name(user_id),
    }


def item_snapshot(product_id: int, quantity: int) -> dict:
    unit_price_cents = product_price_cents(product_id)
    return {
        "productId": product_id,
        "sku": product_sku(product_id),
        "name": product_name(product_id),
        "category": product_category(product_id),
        "quantity": quantity,
        "unitPriceCents": unit_price_cents,
        "lineTotalCents": unit_price_cents * quantity,
    }


def order_document(
    *,
    order_id: int,
    user_count: int,
    product_count: int,
    min_items: int,
    max_items: int,
    rng: random.Random,
    now: datetime,
) -> dict:
    item_count = rng.randint(min_items, max_items)
    product_ids = rng.sample(range(1, product_count + 1), item_count)
    items = [item_snapshot(product_id, rng.randint(1, 4)) for product_id in product_ids]
    total_cents = sum(item["lineTotalCents"] for item in items)
    order_date = random_order_date(rng, now)

    return {
        "_id": order_id,
        "orderId": order_id,
        "user": user_snapshot(rng.randint(1, user_count)),
        "status": pick_status(rng),
        "orderDate": order_date,
        "currency": CURRENCY,
        "totalAmountCents": total_cents,
        "items": items,
        "createdAt": order_date,
    }


def insert_many(collection, documents: list[dict]) -> None:
    if documents:
        collection.insert_many(documents, ordered=False, bypass_document_validation=True)


def seed_users(db, user_count: int, batch_size: int, now: datetime) -> None:
    progress(f"Seeding {user_count:,} MongoDB users")
    started = time.perf_counter()
    for batch_start, batch_end in tqdm(
        batched_range(1, user_count, batch_size),
        total=batch_total(user_count, batch_size),
        desc="Mongo users",
        unit="batch",
        dynamic_ncols=True,
    ):
        documents = [user_document(user_id, now) for user_id in range(batch_start, batch_end + 1)]
        insert_many(db.users, documents)
    progress(f"Finished users in {time.perf_counter() - started:.1f}s")


def seed_products(db, product_count: int, batch_size: int, now: datetime) -> None:
    progress(f"Seeding {product_count:,} MongoDB products")
    started = time.perf_counter()
    for batch_start, batch_end in tqdm(
        batched_range(1, product_count, batch_size),
        total=batch_total(product_count, batch_size),
        desc="Mongo products",
        unit="batch",
        dynamic_ncols=True,
    ):
        documents = [product_document(product_id, now) for product_id in range(batch_start, batch_end + 1)]
        insert_many(db.products, documents)
    progress(f"Finished products in {time.perf_counter() - started:.1f}s")


def seed_orders(
    db,
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
    progress(f"Seeding {order_count:,} MongoDB order documents")
    rng = random.Random(seed)
    started = time.perf_counter()

    order_batches = tqdm(
        batched_range(1, order_count, batch_size),
        total=batch_total(order_count, batch_size),
        desc="Mongo orders",
        unit="batch",
        dynamic_ncols=True,
    )
    for batch_start, batch_end in order_batches:
        documents = [
            order_document(
                order_id=order_id,
                user_count=user_count,
                product_count=product_count,
                min_items=min_items,
                max_items=max_items,
                rng=rng,
                now=now,
            )
            for order_id in range(batch_start, batch_end + 1)
        ]
        insert_many(db.orders, documents)
        order_batches.set_postfix(orders=f"{batch_end:,}")

    progress(f"Finished orders in {time.perf_counter() - started:.1f}s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create MongoDB document-oriented collections and seed benchmark data."
    )
    parser.add_argument("--uri", default=os.getenv("MONGO_URI", DEFAULT_MONGO_URI))
    parser.add_argument("--database", default=os.getenv("MONGO_DATABASE", "ecommerce"))
    parser.add_argument("--orders", type=parse_count, default=parse_count(os.getenv("ORDER_COUNT", "10_000_000")))
    parser.add_argument("--users", type=parse_count, default=parse_count(os.getenv("USER_COUNT", "1_000_000")))
    parser.add_argument("--products", type=parse_count, default=parse_count(os.getenv("PRODUCT_COUNT", "100_000")))
    parser.add_argument("--batch-size", type=parse_count, default=parse_count(os.getenv("MONGO_BATCH_SIZE", "10_000")))
    parser.add_argument("--min-items", type=parse_count, default=parse_count(os.getenv("MIN_ITEMS", "1")))
    parser.add_argument("--max-items", type=parse_count, default=parse_count(os.getenv("MAX_ITEMS", "5")))
    parser.add_argument("--seed", type=int, default=int(os.getenv("DATA_SEED", "20260525")))
    parser.add_argument("--reset", action="store_true", help="Drop collections before inserting data.")
    parser.add_argument("--schema-only", action="store_true", help="Create indexes without inserting data.")
    parser.add_argument("--no-indexes", action="store_true", help="Skip index creation.")
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

    progress(f"Connecting to MongoDB database '{args.database}'")
    client = MongoClient(args.uri)
    db = client[args.database]

    try:
        if args.reset:
            reset_collections(db)

        if args.schema_only:
            if not args.no_indexes:
                create_indexes(db)
            progress("Schema-only mode completed")
            return 0

        seed_users(db, args.users, args.batch_size, now)
        seed_products(db, args.products, args.batch_size, now)
        seed_orders(
            db,
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
            create_indexes(db)
    finally:
        client.close()

    progress("MongoDB data generation completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
