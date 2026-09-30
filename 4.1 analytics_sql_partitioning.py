#!/usr/bin/env python3
"""
Assignment 02 - Part 4 analytics and scalability challenge for PostgreSQL.

The script compares monthly revenue analytics on:
  1. the original unpartitioned orders table
  2. a month-range partitioned copy named orders_partitioned by default

It keeps the original orders table untouched. The partitioned copy is loaded
from orders only when it is empty, or recreated when --reset-partitioned is used.

Default connection is loaded from .env:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

Install dependencies:
  pip install -r requirements.txt

Example:
  python "4.1 analytics_sql_partitioning.py" --reset-partitioned
  python "4.1 analytics_sql_partitioning.py" --months 24 --json --print-plans
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from config_env import load_env_file

load_env_file()

try:
    import psycopg
    from psycopg import sql
    from tqdm import tqdm
except ImportError:
    print(
        "Missing dependency. Install required packages with: pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise


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


def table_exists(conn: "psycopg.Connection", table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
        return cur.fetchone()[0] is not None


def table_count(conn: "psycopg.Connection", table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
        return int(cur.fetchone()[0])


def source_table_info(conn: "psycopg.Connection", source_table: str) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT
                    COUNT(*) AS order_count,
                    MIN(order_date) AS min_order_date,
                    MAX(order_date) AS max_order_date,
                    MIN(order_id) AS min_order_id,
                    MAX(order_id) AS max_order_id
                FROM {}
                """
            ).format(sql.Identifier(source_table))
        )
        row = cur.fetchone()

    if row[0] == 0 or row[1] is None or row[2] is None:
        raise RuntimeError(f"{source_table} is empty. Run the SQL data generator first.")

    return {
        "orderCount": int(row[0]),
        "minOrderDate": row[1],
        "maxOrderDate": row[2],
        "minOrderId": int(row[3]),
        "maxOrderId": int(row[4]),
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
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (order_status, order_date)").format(
                sql.Identifier(f"idx_{partitioned_table}_status_date"),
                sql.Identifier(partitioned_table),
            )
        )
    conn.commit()


def analyze_table(conn: "psycopg.Connection", table_name: str) -> None:
    progress(f"Running ANALYZE on {table_name}")
    with conn.cursor() as cur:
        cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(table_name)))
    conn.commit()


def copy_source_to_partitioned(
    conn: "psycopg.Connection",
    *,
    source_table: str,
    partitioned_table: str,
    min_order_date: datetime,
    max_order_date: datetime,
) -> None:
    ranges = month_ranges(month_start(min_order_date), add_months(month_start(max_order_date), 1))
    progress(f"Copying orders into {partitioned_table} in {len(ranges):,} monthly batches")

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
        WHERE order_date >= %s
          AND order_date < %s
        """
    ).format(sql.Identifier(partitioned_table), sql.Identifier(source_table))

    inserted_total = 0
    with conn.cursor() as cur:
        load_bar = tqdm(ranges, desc="SQL partition load", unit="month", dynamic_ncols=True)
        for start_at, end_at in load_bar:
            cur.execute(insert_sql, (start_at, end_at))
            inserted_total += max(cur.rowcount, 0)
            conn.commit()
            load_bar.set_postfix(inserted=f"{inserted_total:,}")


def prepare_partitioned_table(conn: "psycopg.Connection", args: argparse.Namespace) -> dict[str, Any]:
    info = source_table_info(conn, args.source_table)

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

    current_count = table_count(conn, args.partitioned_table)
    if current_count == 0:
        copy_source_to_partitioned(
            conn,
            source_table=args.source_table,
            partitioned_table=args.partitioned_table,
            min_order_date=info["minOrderDate"],
            max_order_date=info["maxOrderDate"],
        )
    elif current_count == info["orderCount"]:
        progress(f"{args.partitioned_table} already has {current_count:,} rows; skipping reload")
    else:
        raise RuntimeError(
            f"{args.partitioned_table} has {current_count:,} rows, but {args.source_table} has "
            f"{info['orderCount']:,}. Re-run with --reset-partitioned to rebuild it."
        )

    create_partitioned_indexes(conn, args.partitioned_table)
    analyze_table(conn, args.partitioned_table)

    return {
        "sourceOrderCount": info["orderCount"],
        "partitionedOrderCount": table_count(conn, args.partitioned_table),
        "partitionCount": partition_count,
        "minOrderDate": info["minOrderDate"],
        "maxOrderDate": info["maxOrderDate"],
    }


def analytics_query(table_name: str) -> sql.Composed:
    return sql.SQL(
        """
        SELECT
            DATE_TRUNC('month', order_date)::date AS month,
            COUNT(*) AS order_count,
            SUM(total_amount) AS total_revenue
        FROM {}
        WHERE order_date >= %s
          AND order_date < %s
        GROUP BY 1
        ORDER BY 1
        """
    ).format(sql.Identifier(table_name))


def explain_json(
    conn: "psycopg.Connection",
    *,
    table_name: str,
    start_at: datetime,
    end_at: datetime,
    include_full_plan: bool,
) -> dict[str, Any]:
    query = analytics_query(table_name)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + query, (start_at, end_at))
        payload = cur.fetchone()[0]

    if isinstance(payload, str):
        payload = json.loads(payload)

    plan_doc = payload[0]
    plan = plan_doc["Plan"]
    result = {
        "table": table_name,
        "planningTimeMs": float(plan_doc.get("Planning Time", 0.0)),
        "executionTimeMs": float(plan_doc.get("Execution Time", 0.0)),
        "topNodeType": plan.get("Node Type"),
        "actualRows": int(plan.get("Actual Rows", 0)),
        "planSummary": summarize_plan(plan),
    }
    if include_full_plan:
        result["plan"] = plan_doc
    return result


def summarize_plan(plan: dict[str, Any]) -> dict[str, Any]:
    summary = {
        "nodeType": plan.get("Node Type"),
        "relationName": plan.get("Relation Name"),
        "indexName": plan.get("Index Name"),
        "actualRows": plan.get("Actual Rows"),
        "actualLoops": plan.get("Actual Loops"),
        "sharedHitBlocks": plan.get("Shared Hit Blocks"),
        "sharedReadBlocks": plan.get("Shared Read Blocks"),
        "children": [],
    }
    children = plan.get("Plans", [])
    for child in children[:8]:
        summary["children"].append(summarize_plan(child))
    if len(children) > 8:
        summary["children"].append({"omittedChildren": len(children) - 8})
    return summary


def estimated_explain_text(
    conn: "psycopg.Connection",
    *,
    table_name: str,
    start_at: datetime,
    end_at: datetime,
) -> str:
    query = analytics_query(table_name)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("EXPLAIN (BUFFERS) ") + query, (start_at, end_at))
        return "\n".join(row[0] for row in cur.fetchall())


def run_analytics_comparison(conn: "psycopg.Connection", args: argparse.Namespace) -> dict[str, Any]:
    if args.start_date and args.end_date:
        start_at = parse_date(args.start_date)
        end_at = parse_date(args.end_date)
    else:
        start_at, end_at = default_report_period(args.months)

    if start_at >= end_at:
        raise ValueError("report start date must be before end date")

    with conn.cursor() as cur:
        cur.execute("SET enable_partition_pruning = on")
        cur.execute("SET enable_partitionwise_aggregate = on")
    conn.commit()

    preparation: dict[str, Any] | None = None
    if not args.skip_prepare:
        preparation = prepare_partitioned_table(conn, args)

    progress(f"Running EXPLAIN ANALYZE on {args.source_table}")
    unpartitioned = explain_json(
        conn,
        table_name=args.source_table,
        start_at=start_at,
        end_at=end_at,
        include_full_plan=args.include_full_plan,
    )

    progress(f"Running EXPLAIN ANALYZE on {args.partitioned_table}")
    partitioned = explain_json(
        conn,
        table_name=args.partitioned_table,
        start_at=start_at,
        end_at=end_at,
        include_full_plan=args.include_full_plan,
    )

    text_plans = None
    if args.print_plans:
        text_plans = {
            "unpartitioned": estimated_explain_text(
                conn,
                table_name=args.source_table,
                start_at=start_at,
                end_at=end_at,
            ),
            "partitioned": estimated_explain_text(
                conn,
                table_name=args.partitioned_table,
                start_at=start_at,
                end_at=end_at,
            ),
        }

    improvement = None
    if partitioned["executionTimeMs"] > 0:
        improvement = unpartitioned["executionTimeMs"] / partitioned["executionTimeMs"]

    return {
        "engine": "postgresql",
        "reportStart": start_at,
        "reportEnd": end_at,
        "sourceTable": args.source_table,
        "partitionedTable": args.partitioned_table,
        "preparation": preparation,
        "unpartitioned": unpartitioned,
        "partitioned": partitioned,
        "executionTimeRatioUnpartitionedOverPartitioned": improvement,
        "textPlans": text_plans,
    }


def print_summary(result: dict[str, Any]) -> None:
    unpartitioned = result["unpartitioned"]
    partitioned = result["partitioned"]
    ratio = result["executionTimeRatioUnpartitionedOverPartitioned"]

    print("\nPostgreSQL Analytics / Partitioning Result")
    print(f"Report period                  : {result['reportStart'].date()} -> {result['reportEnd'].date()}")
    if result["preparation"] is not None:
        prep = result["preparation"]
        print(f"Source rows                    : {prep['sourceOrderCount']:,}")
        print(f"Partitioned rows               : {prep['partitionedOrderCount']:,}")
        print(f"Monthly partitions             : {prep['partitionCount']:,}")
    print(f"Unpartitioned execution time   : {unpartitioned['executionTimeMs']:.3f} ms")
    print(f"Partitioned execution time     : {partitioned['executionTimeMs']:.3f} ms")
    print(f"Unpartitioned top plan node    : {unpartitioned['topNodeType']}")
    print(f"Partitioned top plan node      : {partitioned['topNodeType']}")
    if ratio is not None:
        print(f"Speed ratio before/after       : {ratio:.3f}x")

    if result["textPlans"] is not None:
        print("\nEXPLAIN - Unpartitioned")
        print(result["textPlans"]["unpartitioned"])
        print("\nEXPLAIN - Partitioned")
        print(result["textPlans"]["partitioned"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare PostgreSQL monthly revenue analytics before and after table partitioning."
    )
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5436")))
    parser.add_argument("--database", default=os.getenv("PGDATABASE", "ecommerce"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "assignment2"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", ""))
    parser.add_argument("--source-table", default=os.getenv("SQL_SOURCE_ORDERS_TABLE", "orders"))
    parser.add_argument("--partitioned-table", default=os.getenv("SQL_PARTITIONED_ORDERS_TABLE", "orders_partitioned"))
    parser.add_argument("--months", type=parse_count, default=parse_count(os.getenv("ANALYTICS_MONTHS", "24")))
    parser.add_argument("--start-date", default=os.getenv("ANALYTICS_START_DATE"))
    parser.add_argument("--end-date", default=os.getenv("ANALYTICS_END_DATE"))
    parser.add_argument(
        "--batch-size",
        type=parse_count,
        default=parse_count(os.getenv("SQL_PARTITION_LOAD_BATCH_SIZE", "100_000")),
        help="Compatibility option kept from the old loader; partition loading is now grouped by month.",
    )
    parser.add_argument("--reset-partitioned", action="store_true", help="Drop and rebuild the partitioned copy.")
    parser.add_argument("--skip-prepare", action="store_true", help="Do not create/load the partitioned table before benchmarking.")
    parser.add_argument("--print-plans", action="store_true", help="Print text EXPLAIN plans without re-running ANALYZE.")
    parser.add_argument("--include-full-plan", action="store_true", help="Include full JSON EXPLAIN ANALYZE plan in --json output.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON result after the summary.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    with psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
        application_name="assignment2_sql_analytics_partitioning",
    ) as conn:
        result = run_analytics_comparison(conn, args)

    print_summary(result)
    if args.json:
        print("\nJSON Result")
        print(json.dumps(result, indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
