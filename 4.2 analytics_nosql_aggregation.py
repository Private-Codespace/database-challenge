#!/usr/bin/env python3
"""
Assignment 02 - Part 4 analytics challenge for MongoDB.

The script uses MongoDB Aggregation Framework to calculate:
  total revenue by month in the last 24 months by default.

Pipeline shape:
  $match orderDate range
  $group by YYYY-MM month
  $sum totalAmountCents
  $sort by month

Default connection is loaded from .env:
  MONGO_HOST, MONGO_PORT, MONGO_USER, MONGO_PASSWORD, MONGO_AUTH_SOURCE

Install dependencies:
  pip install -r requirements.txt

Example:
  python "4.2 analytics_nosql_aggregation.py"
  python "4.2 analytics_nosql_aggregation.py" --months 24 --explain --json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from config_env import build_mongo_uri, load_env_file

load_env_file()

try:
    from bson import ObjectId
    from pymongo import ASCENDING, MongoClient
    from tqdm import tqdm
except ImportError:
    print(
        "Missing dependency. Install required packages with: pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise


DEFAULT_MONGO_URI = build_mongo_uri()


def parse_count(value: str) -> int:
    number = int(value.replace("_", "").replace(",", ""))
    if number <= 0:
        raise argparse.ArgumentTypeError("value must be greater than 0")
    return number


def parse_date(value: str) -> datetime:
    parsed = datetime.strptime(value, "%Y-%m-%d")
    return parsed.replace(tzinfo=timezone.utc)


def json_default(value: Any) -> str | int | float:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def progress(message: str) -> None:
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    tqdm.write(f"[{stamp}] {message}")


def add_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    return value.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def month_start(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def default_report_period(months: int) -> tuple[datetime, datetime]:
    current_month = month_start(datetime.now(timezone.utc))
    start_at = add_months(current_month, -(months - 1))
    end_at = add_months(current_month, 1)
    return start_at, end_at


def build_pipeline(start_at: datetime, end_at: datetime) -> list[dict[str, Any]]:
    return [
        {
            "$match": {
                "orderDate": {
                    "$gte": start_at,
                    "$lt": end_at,
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m",
                        "date": "$orderDate",
                        "timezone": "UTC",
                    }
                },
                "orderCount": {"$sum": 1},
                "totalRevenueCents": {"$sum": "$totalAmountCents"},
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "month": "$_id",
                "orderCount": 1,
                "totalRevenueCents": 1,
                "totalRevenue": {"$divide": ["$totalRevenueCents", 100]},
            }
        },
    ]


def ensure_indexes(db) -> None:
    progress("Ensuring MongoDB analytics index on orders.orderDate")
    db.orders.create_index([("orderDate", ASCENDING)], name="idx_orders_order_date")


def run_aggregation(db, pipeline: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], float]:
    with tqdm(total=1, desc="Mongo aggregation", unit="query", dynamic_ncols=True) as bar:
        started_ns = time.perf_counter_ns()
        rows = list(db.orders.aggregate(pipeline, allowDiskUse=True))
        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        bar.update(1)
    return rows, elapsed_ms


def explain_aggregation(db, pipeline: list[dict[str, Any]]) -> dict[str, Any]:
    return db.command(
        "explain",
        {
            "aggregate": "orders",
            "pipeline": pipeline,
            "cursor": {},
            "allowDiskUse": True,
        },
        verbosity="executionStats",
    )


def summarize_explain(explain: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "explainVersion": explain.get("explainVersion"),
        "serverInfo": explain.get("serverInfo", {}),
    }

    if "stages" in explain:
        summary["stageCount"] = len(explain["stages"])
        summary["stages"] = [
            next(iter(stage.keys())) if isinstance(stage, dict) and stage else "unknown"
            for stage in explain["stages"]
        ]

    query_planner = explain.get("queryPlanner")
    if query_planner:
        summary["winningPlan"] = query_planner.get("winningPlan")

    execution_stats = explain.get("executionStats")
    if execution_stats:
        summary["executionStats"] = {
            "executionSuccess": execution_stats.get("executionSuccess"),
            "executionTimeMillis": execution_stats.get("executionTimeMillis"),
            "nReturned": execution_stats.get("nReturned"),
            "totalDocsExamined": execution_stats.get("totalDocsExamined"),
            "totalKeysExamined": execution_stats.get("totalKeysExamined"),
        }

    return summary


def run_analytics(args: argparse.Namespace) -> dict[str, Any]:
    if args.start_date and args.end_date:
        start_at = parse_date(args.start_date)
        end_at = parse_date(args.end_date)
    else:
        start_at, end_at = default_report_period(args.months)

    if start_at >= end_at:
        raise ValueError("report start date must be before end date")

    client = MongoClient(args.uri)
    db = client[args.database]
    try:
        if not args.no_index:
            ensure_indexes(db)

        pipeline = build_pipeline(start_at, end_at)
        progress("Running MongoDB aggregation pipeline")
        rows, elapsed_ms = run_aggregation(db, pipeline)

        explain = None
        explain_summary = None
        if args.explain:
            progress("Running MongoDB aggregation explain")
            explain = explain_aggregation(db, pipeline)
            explain_summary = summarize_explain(explain)

        estimated_orders = db.orders.estimated_document_count()
    finally:
        client.close()

    total_orders = sum(int(row.get("orderCount", 0)) for row in rows)
    total_revenue_cents = sum(int(row.get("totalRevenueCents", 0)) for row in rows)

    return {
        "engine": "mongodb",
        "reportStart": start_at,
        "reportEnd": end_at,
        "estimatedOrdersCollectionCount": estimated_orders,
        "matchedOrders": total_orders,
        "totalRevenueCents": total_revenue_cents,
        "totalRevenue": total_revenue_cents / 100,
        "monthRows": len(rows),
        "executionTimeMs": elapsed_ms,
        "pipeline": pipeline,
        "rows": rows,
        "explainSummary": explain_summary,
        "explain": explain if args.include_full_explain else None,
    }


def print_summary(result: dict[str, Any], show_rows: bool) -> None:
    print("\nMongoDB Analytics Aggregation Result")
    print(f"Report period                 : {result['reportStart'].date()} -> {result['reportEnd'].date()}")
    print(f"Estimated collection count    : {result['estimatedOrdersCollectionCount']:,}")
    print(f"Matched orders                : {result['matchedOrders']:,}")
    print(f"Month rows                    : {result['monthRows']:,}")
    print(f"Total revenue                 : {result['totalRevenue']:,.2f}")
    print(f"Aggregation execution time    : {result['executionTimeMs']:.3f} ms")

    if show_rows:
        print("\nMonthly revenue")
        print("month       | orders       | revenue")
        print("------------|--------------|----------------")
        for row in result["rows"]:
            print(
                f"{row['month']:<11} | "
                f"{int(row['orderCount']):>12,} | "
                f"{float(row['totalRevenue']):>14,.2f}"
            )

    if result["explainSummary"] is not None:
        print("\nExplain summary")
        print(json.dumps(result["explainSummary"], indent=2, default=json_default))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run MongoDB monthly revenue aggregation for the analytics challenge."
    )
    parser.add_argument("--uri", default=os.getenv("MONGO_URI", DEFAULT_MONGO_URI))
    parser.add_argument("--database", default=os.getenv("MONGO_DATABASE", "ecommerce"))
    parser.add_argument("--months", type=parse_count, default=parse_count(os.getenv("ANALYTICS_MONTHS", "24")))
    parser.add_argument("--start-date", default=os.getenv("ANALYTICS_START_DATE"))
    parser.add_argument("--end-date", default=os.getenv("ANALYTICS_END_DATE"))
    parser.add_argument("--no-index", action="store_true", help="Skip creating the orderDate index before aggregation.")
    parser.add_argument("--no-rows", action="store_true", help="Do not print the monthly rows table.")
    parser.add_argument("--explain", action="store_true", help="Run aggregation explain with executionStats.")
    parser.add_argument("--include-full-explain", action="store_true", help="Include full explain document in JSON output.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON result after the summary.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_analytics(args)
    print_summary(result, show_rows=not args.no_rows)
    if args.json:
        print("\nJSON Result")
        print(json.dumps(result, indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
