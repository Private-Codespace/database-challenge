# Database Challenge Report

## Environment

- PostgreSQL Docker: `localhost:5436`, database `ecommerce`, user `assignment2`
- MongoDB Docker: `localhost:27017`, database `ecommerce`
- Python environment: `myenv`, dependencies in `requirements.txt`
- Generated dataset: synchronized by `1.3 generate_synced_data.py`
- Main generated order range: `1 -> 10,000,000`
- Flash-sale test order/product id: `1,000,000,001`

## Source Code

| Part | File | Purpose |
|---|---|---|
| 1 | `1.3 generate_synced_data.py` | Generate identical PostgreSQL and MongoDB data from one source stream |
| 2 | `2.3 benchmark_synced_read.py` | Benchmark `GET /orders/{id}` using the same random order ids |
| 3 | `3.3 concurrency_synced.py` | Test flash-sale race condition on both databases |
| 4 | `4.3 analytics_synced.py` | Compare SQL analytics before/after partitioning and MongoDB aggregation |

## Experimental Methodology

The experiment compares PostgreSQL and MongoDB on the same logical e-commerce dataset. The most important control variable is data synchronization: each generated order is created once and then written to both databases. Therefore, the benchmark does not compare two different datasets; it compares two different physical data models and query execution paths for the same business facts.

The benchmark is intentionally workload-specific. It does not prove that one database is globally faster than the other. It answers narrower questions:

- For a point read of one full order detail, which model is faster: normalized SQL joins or one denormalized MongoDB document?
- For a flash-sale stock update, which database can prevent overselling under concurrent requests?
- For monthly revenue analytics over nearly the whole 2-year dataset, how do unpartitioned SQL, partitioned SQL, and MongoDB aggregation behave?

Timing values include the database driver and Python-side result reconstruction. They are therefore application-level response times, not pure engine-only execution times. The report does not separately measure connection behavior, Docker resource allocation, OS scheduling, cache state, or repeated-run variance.

## Evidence Log from Script Runs

All comparative conclusions in this report are tied to values printed by the scripts. When the report explains why one approach is faster or safer, the explanation is treated as an inference from these measured facts and from the query/write path implemented in the corresponding script.

| Topic | Script Evidence |
|---|---|
| Data consistency | Part 2 output: same measured order ids = `True`; payload validation = `50 checked, passed=True`. Part 4 output: SQL vs Mongo rows match = `True`; SQL partitioned rows match SQL source rows = `True`. |
| Part 2 point read | PostgreSQL avg/P95/RPS = `0.554 ms / 1.327 ms / 1799.94`; MongoDB avg/P95/RPS = `0.385 ms / 0.474 ms / 2593.23`. |
| Part 3 concurrency | PostgreSQL success/failed/errors/final stock/oversold/passed = `1 / 49 / 0 / 0 / False / True`; MongoDB = `1 / 49 / 0 / 0 / False / True`. |
| Part 4 analytics | PostgreSQL unpartitioned = `3773.823 ms`; PostgreSQL partitioned = `4471.211 ms`; MongoDB aggregation = `6701.931 ms`; all matched `9,906,399` orders and revenue `74,485,791,782.77`. |
| Part 4 SQL plans | `--print-plans` output showed unpartitioned plan using `Parallel Seq Scan on orders`; partitioned plan using `Parallel Append` over monthly partitions. |

## Part 1 - Data Architecture

### PostgreSQL Schema

The SQL design is normalized to 3NF:

- `users(user_id, email, full_name, created_at)`
- `products(product_id, sku, product_name, category, current_price, stock_quantity, created_at)`
- `orders(order_id, user_id, order_status, order_date, currency, total_amount, created_at)`
- `order_items(order_item_id, order_id, product_id, quantity, unit_price, line_total)`

Primary keys, foreign keys, unique constraints, and check constraints are used to maintain integrity.

### MongoDB Schema

MongoDB stores one order as one document:

```js
{
  _id: 123,
  orderId: 123,
  user: { userId, email, fullName },
  status,
  orderDate,
  currency,
  totalAmount,
  totalAmountCents,
  items: [
    { productId, sku, name, category, quantity, unitPrice, unitPriceCents, lineTotal, lineTotalCents }
  ]
}
```

This is intentionally denormalized for the read path in Part 2. `GET /orders/{id}` can return the full order detail with a single document lookup.

### Randomness Requirements

The generator satisfies the required randomness:

- `order_date/orderDate`: random within the last 730 days from `base_time`
- `order_status/status`: random weighted status among `PENDING`, `PAID`, `SHIPPED`, `DELIVERED`, `CANCELLED`, `REFUNDED`
- Monetary values: product prices vary from `5.00` to `1,999.99`; order totals vary by random products, random quantities, and random item counts

The data is synchronized because each batch is generated once and written to both databases.

## Part 2 - Read Challenge

Command:

```bash
python "2.3 benchmark_synced_read.py" --json
```

Validation:

- Same measured order ids: `True`
- Payload validation: `50 checked, passed=True`
- Order id range: `1 -> 10,000,000`
- Requests: `5,000`

| Database | Query Style | Avg Response Time (ms) | P95 (ms) | Min (ms) | Max (ms) | RPS |
|---|---:|---:|---:|---:|---:|---:|
| PostgreSQL | JOIN `orders + users + order_items + products` | `0.554` | `1.327` | `0.213` | `16.905` | `1799.94` |
| MongoDB | Single document lookup | `0.385` | `0.474` | `0.242` | `5.166` | `2593.23` |

### Interpretation

Measured evidence: MongoDB had lower average latency (`0.385 ms` vs `0.554 ms`), lower P95 latency (`0.474 ms` vs `1.327 ms`), and higher throughput (`2593.23 RPS` vs `1799.94 RPS`). Based on these script outputs, MongoDB was faster for Part 2 in this run.

Implementation evidence: `2.3 benchmark_synced_read.py` uses a SQL query that joins `orders + users + order_items + products`, while MongoDB uses one `find_one({"_id": order_id})` lookup on the `orders` collection. Therefore, the explanation is not guessed from database reputation; it follows from the measured result plus the query path used by the script.

The most defensible inference is that MongoDB benefited from the denormalized document shape: the full order detail is already stored as one document. PostgreSQL had to reconstruct the same response through joins and Python-side nesting. This inference is limited to the measured schema and query path.

What the numbers do not prove: they do not prove MongoDB is generally faster than PostgreSQL. They only prove that, for this 5,000-request point-read benchmark with this data model, MongoDB returned the full order detail faster.

## Part 3 - Concurrency Challenge

Command:

```bash
python "3.3 concurrency_synced.py" --json
```

Scenario:

- Threads: `50`
- Initial stock: `1`
- Expected success: `1`
- Expected failure: `49`
- Expected final stock: `0`

| Database | Mechanism | Success | Failed | Errors | Final Stock | Oversold | Passed | Avg Attempt Latency (ms) |
|---|---|---:|---:|---:|---:|---|---|---:|
| PostgreSQL | Atomic conditional `UPDATE ... WHERE stock_quantity > 0 RETURNING` | `1` | `49` | `0` | `0` | `False` | `True` | `29.379` |
| MongoDB | Atomic `find_one_and_update` with `stockQuantity > 0` | `1` | `49` | `0` | `0` | `False` | `True` | `10.079` |

Winning request in this run:

- PostgreSQL: `thread_index = 33`
- MongoDB: `thread_index = 49`

The report records the winning thread from this script run only. It does not use the winner index as a performance or correctness conclusion. The correctness conclusion is based on final state: exactly one successful purchase, exactly one created order, and final stock equal to zero.

### Interpretation

Correctness evidence: both databases produced exactly `1` success, `49` failures, `0` errors, final stock `0`, oversold `False`, and passed `True`. Therefore, the report concludes that both implementations prevented overselling in this measured 50-thread scenario.

Implementation evidence: the PostgreSQL path uses `UPDATE ... WHERE stock_quantity > 0 RETURNING` inside a transaction. The MongoDB path uses `find_one_and_update` with condition `stockQuantity > 0` and `$inc`. The correctness claim is therefore supported by both the script logic and the measured final state.

Latency evidence: MongoDB had lower average attempt latency in this run (`10.079 ms` vs `29.379 ms`). The report does not use this number to claim MongoDB is always faster for writes. It only states that the MongoDB implementation was faster for this particular flash-sale script run.

## Part 4 - Analytics & Scalability

Command:

```bash
python "4.3 analytics_synced.py" --skip-prepare --json --print-plans --mongo-max-time-ms 300000
```

The first preparation run created and loaded `orders_partitioned_synced` with `10,000,000` generated orders. The final benchmark run used `--skip-prepare`.

Report period:

- `2024-06-01 -> 2026-06-01`
- Matched generated orders: `9,906,399`
- SQL vs Mongo rows match: `True`
- SQL partitioned rows match SQL source rows: `True`

| Database / Table | Method | Execution Time (ms) | Matched Orders | Month Rows | Total Revenue |
|---|---|---:|---:|---:|---:|
| PostgreSQL `orders` | Unpartitioned table | `3773.823` | `9,906,399` | `24` | `74,485,791,782.77` |
| PostgreSQL `orders_partitioned_synced` | Monthly range partitioning | `4471.211` | `9,906,399` | `24` | `74,485,791,782.77` |
| MongoDB `orders` | Aggregation Framework | `6701.931` | `9,906,399` | `24` | `74,485,791,782.77` |

Comparison:

- SQL unpartitioned / partitioned ratio: `0.844x`
- SQL partitioned / MongoDB ratio: `0.667x`

### Interpretation

Measured evidence: all three analytics paths matched the same `9,906,399` orders, returned `24` month rows, and produced the same total revenue `74,485,791,782.77`. Because the result sets match, the speed comparison is based on equivalent analytical output.

Speed evidence: PostgreSQL unpartitioned was fastest at `3773.823 ms`. PostgreSQL partitioned took `4471.211 ms`, which is `697.388 ms` slower than unpartitioned in this run. MongoDB aggregation took `6701.931 ms`, which is slower than both PostgreSQL variants for this full-period analytics query.

Plan evidence from `--print-plans`: the unpartitioned SQL plan included `Parallel Seq Scan on orders`; the partitioned plan included `Parallel Append` across monthly partitions. Therefore, the explanation for the partitioned result is tied to the observed plan and measured period: the report returned `24` month rows over `2024-06-01 -> 2026-06-01`, so the recorded run did not show a selective partition-pruning speedup.

What the numbers do not prove: this run does not prove partitioning is bad. It proves that monthly partitioning did not improve the measured full 24-month report. To claim partitioning helps selective time windows, an additional script run with a one-month or three-month period would be needed.

### Monthly Revenue

| Month | Orders | Revenue |
|---|---:|---:|
| 2024-06 | 410,453 | 3,087,982,546.62 |
| 2024-07 | 425,195 | 3,204,162,385.44 |
| 2024-08 | 424,688 | 3,187,099,167.31 |
| 2024-09 | 410,247 | 3,087,460,516.52 |
| 2024-10 | 424,271 | 3,188,325,712.99 |
| 2024-11 | 411,248 | 3,089,717,719.75 |
| 2024-12 | 426,007 | 3,203,554,956.52 |
| 2025-01 | 424,259 | 3,192,357,104.63 |
| 2025-02 | 383,674 | 2,882,127,150.97 |
| 2025-03 | 424,954 | 3,197,833,881.16 |
| 2025-04 | 412,237 | 3,101,034,648.01 |
| 2025-05 | 424,022 | 3,186,582,408.65 |
| 2025-06 | 411,232 | 3,093,004,196.36 |
| 2025-07 | 423,881 | 3,185,152,026.14 |
| 2025-08 | 424,744 | 3,195,612,686.26 |
| 2025-09 | 410,029 | 3,087,242,722.15 |
| 2025-10 | 425,064 | 3,195,305,104.69 |
| 2025-11 | 412,212 | 3,094,352,701.43 |
| 2025-12 | 424,267 | 3,188,154,436.65 |
| 2026-01 | 424,893 | 3,192,189,254.16 |
| 2026-02 | 382,992 | 2,880,131,771.73 |
| 2026-03 | 424,373 | 3,194,339,062.84 |
| 2026-04 | 410,957 | 3,091,481,857.00 |
| 2026-05 | 330,500 | 2,480,587,764.79 |

## Technical Analysis

### 1. Evidence-Based Claim: MongoDB Was Faster for Part 2

Claim: MongoDB was faster than PostgreSQL for `GET /orders/{id}` in this benchmark.

Evidence from `2.3 benchmark_synced_read.py`:

| Metric | PostgreSQL | MongoDB | Evidence-Based Comparison |
|---|---:|---:|---|
| Average response time | `0.554 ms` | `0.385 ms` | MongoDB lower by `0.169 ms`; SQL/Mongo ratio about `1.439x` |
| P95 response time | `1.327 ms` | `0.474 ms` | MongoDB lower by `0.853 ms`; SQL/Mongo ratio about `2.800x` |
| Requests per second | `1799.94` | `2593.23` | MongoDB about `1.441x` higher |
| Payload validation | `50 checked` | `passed=True` | Both databases returned equivalent sampled payloads |

Inference supported by code path: the SQL script uses a four-table join to construct one order detail response; the MongoDB script fetches one document by `_id`. Because the measured output shows MongoDB was faster and the script shows a shorter read path, the report explains the result as an effect of denormalized document reads for this access pattern.

### 2. Evidence-Based Claim: Both Databases Prevented Overselling

Claim: both implementations handled the 50-thread flash-sale race correctly.

Evidence from `3.3 concurrency_synced.py`:

| Metric | PostgreSQL | MongoDB |
|---|---:|---:|
| Threads | `50` | `50` |
| Successful purchases | `1` | `1` |
| Failed purchases | `49` | `49` |
| Errors | `0` | `0` |
| Final stock | `0` | `0` |
| Oversold | `False` | `False` |
| Passed | `True` | `True` |

Inference supported by code path: PostgreSQL combines the stock condition and decrement in one `UPDATE ... WHERE stock_quantity > 0 RETURNING` statement inside a transaction. MongoDB combines the stock condition and decrement in one `find_one_and_update` operation. The final measured state proves that no overselling occurred in this run.

The latency comparison is also evidence-based, but it is not used as a general database ranking:

| Metric | PostgreSQL | MongoDB |
|---|---:|---:|
| Average attempt latency | `29.379 ms` | `10.079 ms` |

This only supports the statement that MongoDB's implementation was faster in this specific script run.

### 3. Evidence-Based Claim: PostgreSQL Was Faster for Full-Period Analytics

Claim: PostgreSQL was faster than MongoDB for the 24-month revenue report in this benchmark.

Evidence from `4.3 analytics_synced.py`:

| Engine / Table | Execution Time | Matched Orders | Month Rows | Total Revenue |
|---|---:|---:|---:|---:|
| PostgreSQL `orders` | `3773.823 ms` | `9,906,399` | `24` | `74,485,791,782.77` |
| PostgreSQL `orders_partitioned_synced` | `4471.211 ms` | `9,906,399` | `24` | `74,485,791,782.77` |
| MongoDB `orders` | `6701.931 ms` | `9,906,399` | `24` | `74,485,791,782.77` |

The rows and revenue match, so the comparison is not between different result sets. It is between three implementations producing the same monthly revenue output.

Measured comparisons:

- PostgreSQL unpartitioned was `2928.108 ms` faster than MongoDB aggregation.
- PostgreSQL partitioned was `2230.720 ms` faster than MongoDB aggregation.
- PostgreSQL unpartitioned was `697.388 ms` faster than PostgreSQL partitioned.

Inference supported by plan output: `--print-plans` showed `Parallel Seq Scan on orders` for the unpartitioned table and `Parallel Append` for the partitioned table. The report period covered `2024-06-01 -> 2026-06-01`, producing `24` month rows and matching `9,906,399` orders. Therefore, the measured partitioned query did not gain enough pruning benefit in this full-period run.

### 4. Evidence-Based Claim: Partitioning Did Not Help This Specific Query

Claim: partitioning did not improve the measured 24-month analytics query.

Evidence:

- Unpartitioned execution time: `3773.823 ms`
- Partitioned execution time: `4471.211 ms`
- Difference: partitioned was `697.388 ms` slower
- Ratio printed by script: unpartitioned / partitioned = `0.844x`
- Report period: `2024-06-01 -> 2026-06-01`
- Month rows returned: `24`

Inference: because the measured report returned `24` month rows and matched `9,906,399` orders, the script output and plan shape support the conclusion that monthly partitioning did not improve this full-range query. The report does not claim partitioning would be faster for shorter date ranges, because that would require extra script output for one-month or three-month tests.

### 5. Limits of the Evidence

The report intentionally limits its conclusions to the data collected from the scripts:

- It does not claim one database is always faster than the other.
- It does not claim partitioning is generally good or bad.
- It does not claim the same numbers will appear on another machine.
- It does not claim statistical confidence intervals, because each benchmark result in the report comes from the recorded script run rather than repeated trials.

Additional claims would require additional measurements, for example repeated runs, reversed run order, CPU/memory/I/O collection, one-month analytics, three-month analytics, `$lookup`-based MongoDB reads, or denormalized PostgreSQL read models.

### 6. Final Conclusion

The final conclusions are exactly the conclusions supported by the recorded script outputs:

- Part 2: MongoDB was faster for point reads because measured avg/P95/RPS were better, and the script used one document lookup instead of a SQL join response reconstruction.
- Part 3: both PostgreSQL and MongoDB prevented overselling because both produced `1` success, `49` failures, final stock `0`, and `passed=True`.
- Part 4: PostgreSQL was faster for the full 24-month analytics query because its measured execution times were lower than MongoDB's while producing the same matched order count and revenue.
- Partitioning did not help the measured full-period analytics query because the partitioned table was slower than the unpartitioned table in the script output.
