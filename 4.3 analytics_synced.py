#!/usr/bin/env python3

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
    import psycopg
    from bson import ObjectId
    from psycopg import sql
    from pymongo import ASCENDING, MongoClient
    from tqdm import tqdm
except ImportError:
    print(
        "Missing dependency. Install required packages with: pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise


DEFAULT_MONGO_URI = build_mongo_uri()
DEFAULT_GENERATED_ID_CAP = 1_000_000_000
ORDER_STATUSES = ("PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED")


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


def month_ranges(start_at: datetime, end_at: datetime) -> list[tuple[datetime, datetime]]:
    ranges: list[tuple[datetime, datetime]] = []
    current = month_start(start_at)
    stop = month_start(end_at)
    while current < stop:
        next_month = add_months(current, 1)
        ranges.append((current, next_month))
        current = next_month
    return ranges


def postgres_connect(args: argparse.Namespace) -> "psycopg.Connection":
    return psycopg.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
        application_name="assignment2_synced_analytics",
    )


def mongo_connect(args: argparse.Namespace) -> MongoClient:
    return MongoClient(args.mongo_uri, tz_aware=True, tzinfo=timezone.utc)


def table_exists(conn: "psycopg.Connection", table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
        return cur.fetchone()[0] is not None


def count_table(conn: "psycopg.Connection", table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
        return int(cur.fetchone()[0])


def generated_source_info(
    conn: "psycopg.Connection",
    *,
    source_table: str,
    min_order_id: int,
    max_order_id: int,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT
                    COUNT(*) AS order_count,
                    MIN(order_date) AS min_order_date,
                    MAX(order_date) AS max_order_date
                FROM {}
                WHERE order_id >= %s
                  AND order_id <= %s
                """
            ).format(sql.Identifier(source_table)),
            (min_order_id, max_order_id),
        )
        order_count, min_order_date, max_order_date = cur.fetchone()

    if order_count == 0 or min_order_date is None or max_order_date is None:
        raise RuntimeError(
            f"{source_table} has no generated orders in range "
            f"{min_order_id:,} -> {max_order_id:,}. Run 1.3 generate_synced_data.py first."
        )

    return {
        "orderCount": int(order_count),
        "minOrderDate": min_order_date,
        "maxOrderDate": max_order_date,
    }


def partition_name(parent_table: str, start_at: datetime) -> str:
    safe_parent = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in parent_table)
    return f"{safe_parent}_{start_at.year}_{start_at.month:02d}"


def create_partitioned_parent(conn: "psycopg.Connection", partitioned_table: str) -> None:
    status_values = ", ".join(f"'{status}'" for status in ORDER_STATUSES)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                f"""
                CREATE TABLE IF NOT EXISTS {{}} (
                    order_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    order_status TEXT NOT NULL CHECK (order_status IN ({status_values})),
                    order_date TIMESTAMPTZ NOT NULL,
                    currency CHAR(3) NOT NULL DEFAULT 'USD',
                    total_amount NUMERIC(14, 2) NOT NULL CHECK (total_amount >= 0),
                    created_at TIMESTAMPTZ NOT NULL
                ) PARTITION BY RANGE (order_date)
                """
            ).format(sql.Identifier(partitioned_table))
        )
    conn.commit()


def create_monthly_partitions(
    conn: "psycopg.Connection",
    *,
    partitioned_table: str,
    min_order_date: datetime,
    max_order_date: datetime,
) -> int:
    ranges = month_ranges(month_start(min_order_date), add_months(month_start(max_order_date), 1))
    progress(f"Ensuring {len(ranges):,} monthly partitions on {partitioned_table}")

    with conn.cursor() as cur:
        for start_at, end_at in tqdm(ranges, desc="SQL partitions", unit="partition", dynamic_ncols=True):
            cur.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {} PARTITION OF {} FOR VALUES FROM ({}) TO ({})").format(
                    sql.Identifier(partition_name(partitioned_table, start_at)),
                    sql.Identifier(partitioned_table),
                    sql.Literal(start_at),
                    sql.Literal(end_at),
                )
            )
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {} PARTITION OF {} DEFAULT").format(
                sql.Identifier(f"{partitioned_table}_default"),
                sql.Identifier(partitioned_table),
            )
        )
    conn.commit()
    return len(ranges)


def create_partitioned_indexes(conn: "psycopg.Connection", partitioned_table: str) -> None:
    progress(f"Creating indexes on {partitioned_table}")
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (order_date)").format(
                sql.Identifier(f"idx_{partitioned_table}_order_date"),
                sql.Identifier(partitioned_table),
            )
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (order_id)").format(
                sql.Identifier(f"idx_{partitioned_table}_order_id"),
                sql.Identifier(partitioned_table),
            )
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (order_status, order_date)").format(
                sql.Identifier(f"idx_{partitioned_table}_status_date"),
                sql.Identifier(partitioned_table),
            )
        )
    conn.commit()


def analyze_table(conn: "psycopg.Connection", table_name: str) -> None:
    progress(f"Running PostgreSQL ANALYZE on {table_name}")
    with conn.cursor() as cur:
        cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(table_name)))
    conn.commit()


def copy_generated_orders_to_partitioned(
    conn: "psycopg.Connection",
    *,
    source_table: str,
    partitioned_table: str,
    min_order_id: int,
    max_order_id: int,
    min_order_date: datetime,
    max_order_date: datetime,
) -> None:
    ranges = month_ranges(month_start(min_order_date), add_months(month_start(max_order_date), 1))
    progress(f"Copying generated orders into {partitioned_table} in {len(ranges):,} monthly batches")

    insert_sql = sql.SQL(
        """
        INSERT INTO {} (
            order_id, user_id, order_status, order_date,
            currency, total_amount, created_at
        )
        SELECT
            order_id, user_id, order_status, order_date,
            currency, total_amount, created_at
        FROM {}
        WHERE order_id >= %s
          AND order_id <= %s
          AND order_date >= %s
          AND order_date < %s
        """
    ).format(sql.Identifier(partitioned_table), sql.Identifier(source_table))

    inserted_total = 0
    with conn.cursor() as cur:
        load_bar = tqdm(ranges, desc="SQL partition load", unit="month", dynamic_ncols=True)
        for start_at, end_at in load_bar:
            cur.execute(insert_sql, (min_order_id, max_order_id, start_at, end_at))
            inserted_total += max(cur.rowcount, 0)
            conn.commit()
            load_bar.set_postfix(inserted=f"{inserted_total:,}")


def prepare_partitioned_table(conn: "psycopg.Connection", args: argparse.Namespace) -> dict[str, Any]:
    info = generated_source_info(
        conn,
        source_table=args.source_table,
        min_order_id=args.min_order_id,
        max_order_id=args.max_order_id,
    )

    if args.reset_partitioned and table_exists(conn, args.partitioned_table):
        progress(f"Dropping existing {args.partitioned_table}")
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(args.partitioned_table))
            )
        conn.commit()

    create_partitioned_parent(conn, args.partitioned_table)
    partition_count = create_monthly_partitions(
        conn,
        partitioned_table=args.partitioned_table,
        min_order_date=info["minOrderDate"],
        max_order_date=info["maxOrderDate"],
    )

    current_count = count_table(conn, args.partitioned_table)
    if current_count == 0:
        copy_generated_orders_to_partitioned(
            conn,
            source_table=args.source_table,
            partitioned_table=args.partitioned_table,
            min_order_id=args.min_order_id,
            max_order_id=args.max_order_id,
            min_order_date=info["minOrderDate"],
            max_order_date=info["maxOrderDate"],
        )
    elif current_count == info["orderCount"]:
        progress(f"{args.partitioned_table} already has {current_count:,} rows; skipping reload")
    else:
        raise RuntimeError(
            f"{args.partitioned_table} has {current_count:,} rows, but generated source has "
            f"{info['orderCount']:,}. Re-run with --reset-partitioned."
        )

    create_partitioned_indexes(conn, args.partitioned_table)
    analyze_table(conn, args.partitioned_table)

    return {
        "sourceGeneratedOrderCount": info["orderCount"],
        "partitionedOrderCount": count_table(conn, args.partitioned_table),
        "partitionCount": partition_count,
        "minOrderDate": info["minOrderDate"],
        "maxOrderDate": info["maxOrderDate"],
    }


def sql_monthly_query(table_name: str) -> sql.Composed:
    return sql.SQL(
        """
        SELECT
            TO_CHAR(DATE_TRUNC('month', order_date), 'YYYY-MM') AS month,
            COUNT(*)::BIGINT AS order_count,
            SUM((total_amount * 100)::BIGINT)::NUMERIC AS total_revenue_cents
        FROM {}
        WHERE order_id >= %s
          AND order_id <= %s
          AND order_date >= %s
          AND order_date < %s
        GROUP BY 1
        ORDER BY 1
        """
    ).format(sql.Identifier(table_name))


def run_sql_monthly_query(
    conn: "psycopg.Connection",
    *,
    table_name: str,
    min_order_id: int,
    max_order_id: int,
    start_at: datetime,
    end_at: datetime,
) -> dict[str, Any]:
    query = sql_monthly_query(table_name)
    params = (min_order_id, max_order_id, start_at, end_at)

    progress(f"Running PostgreSQL monthly revenue query on {table_name}")
    started_ns = time.perf_counter_ns()
    with conn.cursor() as cur:
        cur.execute(query, params)
        raw_rows = cur.fetchall()
    elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000

    rows = [
        {
            "month": row[0],
            "orderCount": int(row[1]),
            "totalRevenueCents": int(row[2]) if row[2] is not None else 0,
            "totalRevenue": (int(row[2]) if row[2] is not None else 0) / 100,
        }
        for row in raw_rows
    ]
    return summarize_monthly_result("postgresql", table_name, rows, elapsed_ms)


def explain_sql_query(
    conn: "psycopg.Connection",
    *,
    table_name: str,
    min_order_id: int,
    max_order_id: int,
    start_at: datetime,
    end_at: datetime,
    analyze: bool,
) -> str:
    query = sql_monthly_query(table_name)
    params = (min_order_id, max_order_id, start_at, end_at)
    explain_prefix = "EXPLAIN (ANALYZE, BUFFERS) " if analyze else "EXPLAIN (BUFFERS) "

    with conn.cursor() as cur:
        cur.execute(sql.SQL(explain_prefix) + query, params)
        return "\n".join(row[0] for row in cur.fetchall())


def ensure_mongo_indexes(mongo_db) -> None:
    progress("Ensuring MongoDB analytics indexes")
    mongo_db.orders.create_index([("orderDate", ASCENDING)], name="idx_orders_order_date")
    mongo_db.orders.create_index([("orderDate", ASCENDING), ("_id", ASCENDING)], name="idx_orders_order_date_id")
    mongo_db.orders.create_index(
        [("orderDate", ASCENDING), ("_id", ASCENDING), ("totalAmountCents", ASCENDING)],
        name="idx_orders_order_date_id_amount",
    )


def mongo_pipeline(
    *,
    min_order_id: int,
    max_order_id: int,
    start_at: datetime,
    end_at: datetime,
) -> list[dict[str, Any]]:
    return [
        {
            "$match": {
                "_id": {
                    "$gte": min_order_id,
                    "$lte": max_order_id,
                },
                "orderDate": {
                    "$gte": start_at,
                    "$lt": end_at,
                },
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


def run_mongo_monthly_query(
    mongo_db,
    *,
    min_order_id: int,
    max_order_id: int,
    start_at: datetime,
    end_at: datetime,
    hint: str | None,
    max_time_ms: int | None,
) -> dict[str, Any]:
    pipeline = mongo_pipeline(
        min_order_id=min_order_id,
        max_order_id=max_order_id,
        start_at=start_at,
        end_at=end_at,
    )
    progress("Running MongoDB monthly revenue aggregation")
    with tqdm(total=1, desc="Mongo aggregation", unit="query", dynamic_ncols=True) as bar:
        started_ns = time.perf_counter_ns()
        aggregate_options: dict[str, Any] = {"allowDiskUse": True}
        if hint:
            aggregate_options["hint"] = hint
        if max_time_ms:
            aggregate_options["maxTimeMS"] = max_time_ms
        rows = list(mongo_db.orders.aggregate(pipeline, **aggregate_options))
        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        bar.update(1)

    normalized_rows = [
        {
            "month": row["month"],
            "orderCount": int(row["orderCount"]),
            "totalRevenueCents": int(row["totalRevenueCents"]),
            "totalRevenue": int(row["totalRevenueCents"]) / 100,
        }
        for row in rows
    ]
    return summarize_monthly_result("mongodb", "orders", normalized_rows, elapsed_ms)


def summarize_monthly_result(
    engine: str,
    table_or_collection: str,
    rows: list[dict[str, Any]],
    elapsed_ms: float,
) -> dict[str, Any]:
    total_orders = sum(row["orderCount"] for row in rows)
    total_revenue_cents = sum(row["totalRevenueCents"] for row in rows)
    return {
        "engine": engine,
        "tableOrCollection": table_or_collection,
        "executionTimeMs": elapsed_ms,
        "monthRows": len(rows),
        "matchedOrders": total_orders,
        "totalRevenueCents": total_revenue_cents,
        "totalRevenue": total_revenue_cents / 100,
        "rows": rows,
    }


def explain_mongo_aggregation(
    mongo_db,
    *,
    min_order_id: int,
    max_order_id: int,
    start_at: datetime,
    end_at: datetime,
    hint: str | None,
) -> dict[str, Any]:
    pipeline = mongo_pipeline(
        min_order_id=min_order_id,
        max_order_id=max_order_id,
        start_at=start_at,
        end_at=end_at,
    )
    command: dict[str, Any] = {
        "aggregate": "orders",
        "pipeline": pipeline,
        "cursor": {},
        "allowDiskUse": True,
    }
    if hint:
        command["hint"] = hint

    return mongo_db.command("explain", command, verbosity="executionStats")


def summarize_mongo_explain(explain: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "explainVersion": explain.get("explainVersion"),
    }
    if "stages" in explain:
        summary["stageCount"] = len(explain["stages"])
        summary["stages"] = [
            next(iter(stage.keys())) if isinstance(stage, dict) and stage else "unknown"
            for stage in explain["stages"]
        ]
    if "queryPlanner" in explain:
        summary["winningPlan"] = explain["queryPlanner"].get("winningPlan")
    if "executionStats" in explain:
        execution_stats = explain["executionStats"]
        summary["executionStats"] = {
            "executionSuccess": execution_stats.get("executionSuccess"),
            "executionTimeMillis": execution_stats.get("executionTimeMillis"),
            "nReturned": execution_stats.get("nReturned"),
            "totalDocsExamined": execution_stats.get("totalDocsExamined"),
            "totalKeysExamined": execution_stats.get("totalKeysExamined"),
        }
    return summary


def compare_monthly_rows(
    sql_rows: list[dict[str, Any]],
    mongo_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    comparable_sql = [
        {
            "month": row["month"],
            "orderCount": row["orderCount"],
            "totalRevenueCents": row["totalRevenueCents"],
        }
        for row in sql_rows
    ]
    comparable_mongo = [
        {
            "month": row["month"],
            "orderCount": row["orderCount"],
            "totalRevenueCents": row["totalRevenueCents"],
        }
        for row in mongo_rows
    ]
    return {
        "passed": comparable_sql == comparable_mongo,
        "sqlRows": comparable_sql,
        "mongoRows": comparable_mongo,
    }


def run_synced_analytics(args: argparse.Namespace) -> dict[str, Any]:
    if args.start_date and args.end_date:
        start_at = parse_date(args.start_date)
        end_at = parse_date(args.end_date)
    else:
        start_at, end_at = default_report_period(args.months)

    if start_at >= end_at:
        raise ValueError("report start date must be before end date")

    pg_conn = postgres_connect(args)
    mongo_client = mongo_connect(args)
    mongo_db = mongo_client[args.mongo_database]

    try:
        preparation = None
        if not args.skip_prepare:
            preparation = prepare_partitioned_table(pg_conn, args)

        if not args.no_mongo_index:
            ensure_mongo_indexes(mongo_db)

        sql_unpartitioned = run_sql_monthly_query(
            pg_conn,
            table_name=args.source_table,
            min_order_id=args.min_order_id,
            max_order_id=args.max_order_id,
            start_at=start_at,
            end_at=end_at,
        )
        sql_partitioned = run_sql_monthly_query(
            pg_conn,
            table_name=args.partitioned_table,
            min_order_id=args.min_order_id,
            max_order_id=args.max_order_id,
            start_at=start_at,
            end_at=end_at,
        )
        mongo_result = run_mongo_monthly_query(
            mongo_db,
            min_order_id=args.min_order_id,
            max_order_id=args.max_order_id,
            start_at=start_at,
            end_at=end_at,
            hint=args.mongo_hint,
            max_time_ms=args.mongo_max_time_ms,
        )

        sql_plans = None
        if args.print_plans:
            progress("Collecting PostgreSQL EXPLAIN plans")
            sql_plans = {
                "unpartitioned": explain_sql_query(
                    pg_conn,
                    table_name=args.source_table,
                    min_order_id=args.min_order_id,
                    max_order_id=args.max_order_id,
                    start_at=start_at,
                    end_at=end_at,
                    analyze=args.analyze_plans,
                ),
                "partitioned": explain_sql_query(
                    pg_conn,
                    table_name=args.partitioned_table,
                    min_order_id=args.min_order_id,
                    max_order_id=args.max_order_id,
                    start_at=start_at,
                    end_at=end_at,
                    analyze=args.analyze_plans,
                ),
            }

        mongo_explain_summary = None
        if args.mongo_explain:
            progress("Collecting MongoDB aggregation explain")
            mongo_explain_summary = summarize_mongo_explain(
                explain_mongo_aggregation(
                    mongo_db,
                    min_order_id=args.min_order_id,
                    max_order_id=args.max_order_id,
                    start_at=start_at,
                    end_at=end_at,
                    hint=args.mongo_hint,
                )
            )
    finally:
        pg_conn.close()
        mongo_client.close()

    validation = compare_monthly_rows(sql_unpartitioned["rows"], mongo_result["rows"])
    partition_validation = compare_monthly_rows(sql_unpartitioned["rows"], sql_partitioned["rows"])

    partition_speed_ratio = None
    if sql_partitioned["executionTimeMs"] > 0:
        partition_speed_ratio = sql_unpartitioned["executionTimeMs"] / sql_partitioned["executionTimeMs"]

    mongo_vs_sql_ratio = None
    if mongo_result["executionTimeMs"] > 0:
        mongo_vs_sql_ratio = sql_partitioned["executionTimeMs"] / mongo_result["executionTimeMs"]

    return {
        "challenge": "synced-analytics",
        "reportStart": start_at,
        "reportEnd": end_at,
        "generatedOrderIdRange": {
            "minOrderId": args.min_order_id,
            "maxOrderId": args.max_order_id,
        },
        "preparation": preparation,
        "sqlUnpartitioned": sql_unpartitioned,
        "sqlPartitioned": sql_partitioned,
        "mongo": mongo_result,
        "sqlVsMongoValidation": {
            "passed": validation["passed"],
            "mismatchPreview": None if validation["passed"] else validation,
        },
        "sqlPartitionValidation": {
            "passed": partition_validation["passed"],
            "mismatchPreview": None if partition_validation["passed"] else partition_validation,
        },
        "partitionSpeedRatioUnpartitionedOverPartitioned": partition_speed_ratio,
        "sqlPartitionedOverMongoSpeedRatio": mongo_vs_sql_ratio,
        "sqlPlans": sql_plans,
        "mongoExplainSummary": mongo_explain_summary,
    }


def print_result_summary(label: str, result: dict[str, Any]) -> None:
    print(f"\n{label}")
    print(f"Execution time : {result['executionTimeMs']:.3f} ms")
    print(f"Matched orders : {result['matchedOrders']:,}")
    print(f"Month rows     : {result['monthRows']:,}")
    print(f"Total revenue  : {result['totalRevenue']:,.2f}")


def print_monthly_rows(rows: list[dict[str, Any]]) -> None:
    print("\nMonthly Revenue")
    print("month       | orders       | revenue")
    print("------------|--------------|----------------")
    for row in rows:
        print(
            f"{row['month']:<11} | "
            f"{row['orderCount']:>12,} | "
            f"{row['totalRevenue']:>14,.2f}"
        )


def print_summary(result: dict[str, Any], show_rows: bool) -> None:
    print("\nSynchronized Analytics Result")
    print(f"Report period              : {result['reportStart'].date()} -> {result['reportEnd'].date()}")
    print(
        "Generated order ID range   : "
        f"{result['generatedOrderIdRange']['minOrderId']:,} -> "
        f"{result['generatedOrderIdRange']['maxOrderId']:,}"
    )

    if result["preparation"] is not None:
        prep = result["preparation"]
        print(f"Source generated rows      : {prep['sourceGeneratedOrderCount']:,}")
        print(f"Partitioned rows           : {prep['partitionedOrderCount']:,}")
        print(f"Monthly partitions         : {prep['partitionCount']:,}")

    print(f"SQL partition data matches : {result['sqlPartitionValidation']['passed']}")
    print(f"SQL vs Mongo rows match    : {result['sqlVsMongoValidation']['passed']}")

    print_result_summary("PostgreSQL unpartitioned", result["sqlUnpartitioned"])
    print_result_summary("PostgreSQL partitioned", result["sqlPartitioned"])
    print_result_summary("MongoDB aggregation", result["mongo"])

    print("\nComparison")
    if result["partitionSpeedRatioUnpartitionedOverPartitioned"] is not None:
        print(
            "SQL unpartitioned / partitioned : "
            f"{result['partitionSpeedRatioUnpartitionedOverPartitioned']:.3f}x"
        )
    if result["sqlPartitionedOverMongoSpeedRatio"] is not None:
        print(
            "SQL partitioned / MongoDB        : "
            f"{result['sqlPartitionedOverMongoSpeedRatio']:.3f}x"
        )

    if show_rows:
        print_monthly_rows(result["sqlPartitioned"]["rows"])

    if result["sqlPlans"] is not None:
        print("\nPostgreSQL EXPLAIN - Unpartitioned")
        print(result["sqlPlans"]["unpartitioned"])
        print("\nPostgreSQL EXPLAIN - Partitioned")
        print(result["sqlPlans"]["partitioned"])

    if result["mongoExplainSummary"] is not None:
        print("\nMongoDB Explain Summary")
        print(json.dumps(result["mongoExplainSummary"], indent=2, default=json_default))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run synchronized monthly revenue analytics on PostgreSQL and MongoDB."
    )
    parser.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PGPORT", "5436")))
    parser.add_argument("--pg-database", default=os.getenv("PGDATABASE", "ecommerce"))
    parser.add_argument("--pg-user", default=os.getenv("PGUSER", "assignment2"))
    parser.add_argument("--pg-password", default=os.getenv("PGPASSWORD", ""))
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", DEFAULT_MONGO_URI))
    parser.add_argument("--mongo-database", default=os.getenv("MONGO_DATABASE", "ecommerce"))
    parser.add_argument("--source-table", default=os.getenv("SQL_SOURCE_ORDERS_TABLE", "orders"))
    parser.add_argument(
        "--partitioned-table",
        default=os.getenv("SQL_PARTITIONED_ORDERS_TABLE", "orders_partitioned_synced"),
    )
    parser.add_argument("--min-order-id", type=int, default=int(os.getenv("GENERATED_MIN_ORDER_ID", "1")))
    parser.add_argument(
        "--max-order-id",
        type=int,
        default=int(os.getenv("GENERATED_MAX_ORDER_ID", str(DEFAULT_GENERATED_ID_CAP))),
    )
    parser.add_argument("--months", type=parse_count, default=parse_count(os.getenv("ANALYTICS_MONTHS", "24")))
    parser.add_argument("--start-date", default=os.getenv("ANALYTICS_START_DATE"))
    parser.add_argument("--end-date", default=os.getenv("ANALYTICS_END_DATE"))
    parser.add_argument("--reset-partitioned", action="store_true", help="Drop and rebuild the partitioned SQL copy.")
    parser.add_argument("--skip-prepare", action="store_true", help="Do not create/load the partitioned table first.")
    parser.add_argument("--no-mongo-index", action="store_true", help="Skip creating MongoDB analytics indexes.")
    parser.add_argument("--no-rows", action="store_true", help="Do not print monthly rows table.")
    parser.add_argument("--print-plans", action="store_true", help="Print PostgreSQL EXPLAIN plans.")
    parser.add_argument("--analyze-plans", action="store_true", help="Use EXPLAIN ANALYZE for PostgreSQL plans.")
    parser.add_argument("--mongo-explain", action="store_true", help="Run MongoDB aggregation explain with executionStats.")
    parser.add_argument(
        "--mongo-hint",
        default=os.getenv("MONGO_ANALYTICS_HINT", "idx_orders_order_date_id_amount"),
        help="MongoDB index hint for the analytics aggregation; pass an empty string to disable.",
    )
    parser.add_argument(
        "--mongo-max-time-ms",
        type=int,
        default=int(os.getenv("MONGO_ANALYTICS_MAX_TIME_MS", "0")),
        help="Optional maxTimeMS for MongoDB aggregation; 0 means no limit.",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON result after the summary.")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.min_order_id <= 0:
        raise ValueError("--min-order-id must be greater than 0")
    if args.max_order_id < args.min_order_id:
        raise ValueError("--max-order-id must be greater than or equal to --min-order-id")
    if (args.start_date is None) != (args.end_date is None):
        raise ValueError("--start-date and --end-date must be provided together")
    if args.mongo_hint == "":
        args.mongo_hint = None
    if args.mongo_max_time_ms <= 0:
        args.mongo_max_time_ms = None


def main() -> int:
    args = parse_args()
    validate_args(args)
    result = run_synced_analytics(args)
    print_summary(result, show_rows=not args.no_rows)
    if args.json:
        print("\nJSON Result")
        print(json.dumps(result, indent=2, default=json_default))
    passed = result["sqlVsMongoValidation"]["passed"] and result["sqlPartitionValidation"]["passed"]
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
