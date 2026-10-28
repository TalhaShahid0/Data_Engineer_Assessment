# Architecture and Design Decisions

This document explains how the pipeline is built and why. Setup and run
instructions are in the [README](../README.md).

---

## 1. Overview

A batch ELT pipeline that loads the public **Jaffle Shop** CSV files into
**PostgreSQL**, validates them, and builds a star schema with three metric
tables.

| Choice | Why |
|---|---|
| **PostgreSQL** | Real primary/foreign keys, CHECK constraints, transactions and `ON CONFLICT` upserts, which are what make the pipeline safe to rerun. Free, runs locally. |
| **SQL for the logic** | Data-quality rules, cleaning, upserts, metrics and reconciliation are all SQL (`sql/`), so the rules are readable in one place and run inside the database, next to the data. |
| **Python as thin glue** | Downloads files, checks headers, bulk-loads with `COPY`, runs the SQL files in order, records checkpoints and logs. No pandas: the data never needs to be held in Python. |
| **No orchestrator / dbt / Docker** | One command runs everything. For 16 small files from 6 source tables and a few seconds of processing, Airflow or dbt would add setup without adding reliability. Section 8 explains how it would be scheduled. |

---

## 2. Data flow

```mermaid
flowchart LR
    SRC["GitHub<br/>jaffle-shop-data"] -->|download_data.py<br/>retry + backoff| FILES["data/sample<br/>CSV files<br/>(monthly batches)"]
    FILES -->|ingest.py<br/>header check + COPY| RAW[("raw.*<br/>all TEXT<br/>append-only")]
    RAW -->|data_quality.sql<br/>rules + typed upsert| STG[("staging.*<br/>typed, clean,<br/>1 row per key")]
    RAW -->|failed rules| REJ[("audit.rejected_records")]
    STG -->|transformations.sql| ANA[("analytics.*<br/>dims, facts,<br/>metrics")]
    ANA -->|COPY TO| OUT["outputs/*.csv"]
    LOG[("audit.pipeline_runs<br/>audit.ingestion_log")] -.- RAW
    LOG -.- STG
```

| Layer | Component | Where |
|---|---|---|
| Source and ingestion | Download with retries; per-file header check; `COPY` into raw | `src/ingestion/` |
| Raw storage | One table per source, every expected column as `TEXT`, plus `batch_id`, `source_file`, `row_num`, `loaded_at`. Bad values are stored as received, so a batch can be re-validated from here (done automatically for batches with held rows); the source CSVs remain the recoverable copy; rows with the wrong number of fields are kept as JSON in `audit.rejected_records`, and unexpected extra columns are ignored (with a warning). | `raw` schema |
| Validation | SQL rules write failures to `audit.rejected_records`; clean rows are cast and upserted into staging | `sql/data_quality.sql`, `src/validation/` |
| Transformation | Dimensions and facts upserted from staging; metrics rebuilt | `sql/transformations.sql`, `src/transformation/` |
| Target | Star schema + metric tables | `analytics` schema |
| Orchestration | One CLI; per-file transactions; checkpoint by file hash; scheduled with Windows Task Scheduler or cron (example in the README) | `src/pipeline/run_pipeline.py` |
| Testing | 21 pytest tests on a hand-checked fixture dataset in a separate database | `tests/` |
| Logging and monitoring | Log file + console; run and file history in `audit`; reconciliation views; freshness check; exit code 1 on failure | `src/pipeline/logger.py`, `audit` schema |

**Order of work in one run**

1. Create the schema if it does not exist (`--reset` recreates it).
2. For every file, parents before children (stores, customers, products, supplies, orders, items), in **one transaction per file**:
   hash → skip if already loaded → header check → `COPY` to raw → `data_quality.sql` → mark `SUCCESS` → commit.
3. Re-check batches holding `INVESTIGATE` rows (late-arriving parents).
4. Run `transformations.sql` (one transaction) and check `audit.layer_reconciliation`.
5. Export CSVs, check freshness, record the run result.

---

## 3. Data model

### Schemas

| Schema | Purpose | Keys |
|---|---|---|
| `raw` | Exact copy of each file, all `TEXT` | `(batch_id, row_num)` |
| `staging` | Typed, cleaned, one row per business key | business key (e.g. `order_id`) |
| `analytics` | Star schema and metrics, with enforced foreign keys | no surrogate keys: the stable business keys (UUIDs, product SKU) are used |
| `audit` | Runs, file log, rejected records, reconciliation views | `run_id`, `batch_id` |

### Analytics model

```mermaid
erDiagram
    dim_customers ||--o{ fct_orders : places
    dim_stores    ||--o{ fct_orders : sells
    fct_orders    ||--o{ fct_order_items : contains
    dim_products  ||--o{ fct_order_items : "sold as"

    dim_customers {
        uuid customer_id PK
        text customer_name
    }
    dim_stores {
        uuid store_id PK
        text store_name
        timestamp opened_at
        numeric tax_rate
    }
    dim_products {
        text sku PK
        text product_name
        text product_type
        numeric price
        numeric unit_cost
        text description
    }
    fct_orders {
        uuid order_id PK
        uuid customer_id FK
        uuid store_id FK
        timestamp ordered_at
        date order_date
        numeric subtotal
        numeric tax_paid
        numeric order_total
        int item_count
    }
    fct_order_items {
        uuid item_id PK
        uuid order_id FK
        text sku FK
        numeric unit_price
        numeric unit_cost
    }
```

Metric tables (rebuilt every run): `monthly_store_revenue (month, store_id)`,
`customer_summary (customer_id)`, `monthly_product_sales (month, sku)`.

### Modelling decisions

| Decision | Reason |
|---|---|
| Star schema: 3 dimensions, 2 facts | The data is transactional (orders and items), so facts and dimensions fit the questions asked: revenue by month/store, customers, products. |
| Two fact grains: order and item | Order totals (with tax) live on the order; product sales need one row per item. Keeping both avoids splitting tax across items. |
| `UUID` type for ids | The source ids are UUIDs; the type rejects malformed ids and stores them in 16 bytes. |
| `NUMERIC(10,2)` dollars | Source amounts are integer cents; converted once in staging. `NUMERIC` avoids floating-point rounding in sums. |
| Supplies keyed by `(supply_id, sku)` | The source `id` is **not unique**: one supply (e.g. a napkin) is used by several products. |
| `dim_products.unit_cost` = sum of supply costs | Gives a cost per unit sold, so gross margin can be calculated. |
| Item price captured at first load | The source has no item price. The product's price at load time is stored on the item, so a later price change does not rewrite past sales. |
| `fct_orders.item_count` | Identifies empty orders (no items) so metrics can exclude them; recalculated every run, so late items update their order. |
| Raw columns are all `TEXT` | A bad value (e.g. `"abc"` as an amount) can still be stored and then rejected with a reason, instead of failing the load. |

### Updates and history

* **Current approach - SCD Type 1:** dimensions keep only the latest value
  (`ON CONFLICT ... DO UPDATE`). The source has no change history, and the
  metrics only need current names.
* **If history were needed - SCD Type 2:** for example, product price changes
  would add `valid_from`, `valid_to` and `is_current` to `dim_products`; a
  change closes the current row and inserts a new one, and facts join on the
  version valid at `ordered_at`. Not implemented because the source cannot
  show a price change.
* **Corrected facts:** a corrected order arriving in a later file replaces the
  existing row (same `order_id`), and the metrics are rebuilt, so the
  correction flows through (covered by a test).

---

## 4. Data quality

### Issues found in the source data

| Issue | Where | Handling |
|---|---|---|
| `id` is not unique on its own | `raw_supplies` | Composite key `(supply_id, sku)` |
| Trailing space in a name (`"for richer or pourover "`) | `raw_products` | `TRIM` and collapse repeated spaces in staging |
| Amounts stored as integer cents | orders, products, supplies | Converted to dollars (`/ 100.0`) in staging |
| Timestamps without a time zone | orders, stores | Stored as `TIMESTAMP` (local shop time); documented assumption |
| No item quantity or price | `raw_items` | One row = one unit; price taken from products |
| 125 orders with total 0 and **no items** | `raw_orders` | Loaded, flagged as `EXCEPTION`, excluded from order metrics |
| `tax_paid` is 1 cent below `subtotal × tax_rate` on 939 of 15,774 orders (e.g. 6% of $19.00 = $1.14, source has $1.13). All 15,774 match `trunc(subtotal_dollars × tax_rate × 100)` in floating point, so the source system truncates a float instead of rounding | `raw_orders` | Accepted as the source's rule (customers paid that amount). The data-quality check compares `subtotal + tax_paid` with `order_total`, which always match |

### Three categories

| Category | Meaning | Loaded? |
|---|---|---|
| **INVALID** | The row is broken and cannot be used (missing field, wrong type, negative amount, duplicate) | No |
| **INVESTIGATE** | The row may be correct but needs a person or more data (unknown customer that may arrive later, order before the store opened, totals that do not add up) | No - held, and re-checked on every run |
| **EXCEPTION** | A legitimate but unusual business case (zero-value order, 1-cent rounding) | Yes - loaded and flagged |

### Rules (all in `sql/data_quality.sql`)

| Rule | Tables | Category |
|---|---|---|
| `malformed_row` - wrong number of fields (recorded by `ingest.py`) | all | INVALID |
| `missing_required` - required field empty | all | INVALID |
| `invalid_uuid` | stores, customers, orders, items | INVALID |
| `invalid_timestamp` - must start with an ISO date and parse | stores, orders | INVALID |
| `invalid_amount` - must be whole cents, plain digits only (PostgreSQL would also accept `0x10`, `1_000`) | products, supplies, orders | INVALID |
| `amount_out_of_range` - above 9,999,999,999 cents (would overflow `NUMERIC(10,2)`) | products, supplies, orders | INVALID |
| `invalid_key` - SKU / supply id longer than 50 characters (keys are indexed; very long keys would exceed PostgreSQL's index row size and fail the whole file) | products, supplies | INVALID |
| `invalid_tax_rate` - number between 0 and 1 | stores | INVALID |
| `invalid_boolean` | supplies | INVALID |
| `negative_amount` | products, supplies, orders | INVALID |
| `duplicate_key` - same key twice in one file (UUIDs compared in normalised form, so `{…}` and upper case match); later copies are rejected | all | INVALID |
| `unknown_customer`, `unknown_store` | orders | INVESTIGATE |
| `unknown_order` (may be late), `unknown_product` | items, supplies | INVESTIGATE |
| `before_store_opened` | orders | INVESTIGATE |
| `total_mismatch` - `subtotal + tax_paid` differs from `order_total` by more than 1 cent | orders | INVESTIGATE |
| `total_rounding` - differs by exactly 1 cent | orders | EXCEPTION |
| `zero_value_order` | orders | EXCEPTION |

File-level checks in `ingest.py`: a **missing or duplicated column**, a file
that **cannot be opened**, or text that is **not valid UTF-8** (or contains NUL
bytes) fails the whole file (nothing loaded, logged as `FAILED` with the
error); an **extra column** is logged as a warning and ignored; an **empty
file** is logged as `EMPTY` (its header is still checked); **blank lines** are ignored (a line of only commas is a row of empty values and is rejected). "Empty" values
include whitespace-only values (spaces, tabs).

Each rejected record stores the batch, file row number, rule, a readable
reason (including the bad value) and the original row as JSON, so it can be
investigated without opening the file.

### Reconciliation

* `audit.batch_reconciliation` - per file: `rows_read = rows_accepted + rows_rejected` (accepted rows are counted independently from raw, so this is a real check), and every source row is in raw or recorded as malformed.
* `audit.layer_reconciliation` - 8 checks from staging to facts to metrics: counts, revenue totals, units, and **order subtotal = sum of item prices**.

Both are exported to `outputs/` on every run; any failed layer check marks the run `FAILED`.

---

## 5. Metrics

| # | Table | Definition |
|---|---|---|
| 1 | `monthly_store_revenue` | Per calendar month and store: `order_count` = non-empty orders; `revenue` = sum of `order_total`; `avg_order_value` = revenue / order_count |
| 2 | `customer_summary` | Per customer with at least one non-empty order: first and last order date, `order_count`, `lifetime_value` = sum of `order_total`, `is_repeat_customer` = more than one order |
| 3 | `monthly_product_sales` | Per calendar month and product: `units_sold` = item rows; `revenue` = sum of item prices (pre-tax); `total_cost` = sum of supply cost; `gross_margin` = revenue - total_cost |

**Assumptions that affect the results**

* Revenue in metrics 1 and 2 **includes tax** (it is what the customer paid); metric 3 is **pre-tax**, because tax is charged per order, not per product.
* Empty orders (no items) are excluded from metrics 1 and 2. In this data they all have a total of 0, so revenue is unaffected but order counts and average order value are not inflated.
* Only rows that passed data quality (or are EXCEPTION) are counted; INVALID and INVESTIGATE rows are excluded until fixed.
* Months use `ordered_at` as given (no time-zone conversion).
* The sample covers Sep 2016 - Feb 2017, when only the Philadelphia store was open.

---

## 6. Incremental processing and recovery

The source has **no change tracking** (no `updated_at`, no change feed), so
the pipeline uses two alternatives:

1. **File-level: content hash as the checkpoint.** Each file's SHA-256 is
   stored in `audit.ingestion_log`. A file whose hash was already loaded
   successfully is `SKIPPED`; a partial unique index
   (`file_hash WHERE status = 'SUCCESS'`) makes a double load impossible even
   if the code had a bug. A new or changed file has a new hash and is loaded.
2. **Record-level: business-key upserts.** Every load into staging and
   analytics is `INSERT ... ON CONFLICT (key) DO UPDATE ... WHERE ... IS DISTINCT FROM ...`.
   The same record twice gives one row; a corrected record replaces the old
   one; re-running the same batch rewrites nothing. Each staging row records
   the newest batch that delivered it (an identical re-send only updates that
   batch number), and an upsert only applies when
   `existing.batch_id <= new.batch_id`, so re-processing an **older** batch
   (e.g. the re-check of held rows) can never overwrite newer data.

| Question | Answer |
|---|---|
| How is processed data identified? | File hash with status `SUCCESS` (or `EMPTY`) in `audit.ingestion_log`. |
| How are duplicates prevented? | Checkpoint (same file) + primary keys and upserts (same record in different files) + `duplicate_key` rule (same record within a file). |
| How are corrections applied? | A later file containing the same key overwrites the row (the most recently loaded batch wins; an older batch never overwrites a newer one); metrics are rebuilt every run. Exception: a file whose exact content was loaded before is skipped, so reverting a file to an earlier version needs `--reset` (see README, Known limitations). |
| Checkpoint / state? | `audit.ingestion_log` (per file) and `audit.pipeline_runs` (per run). |
| How are failures detected? | Any error in a file rolls back that file's transaction and records `FAILED` with the error message; the run is marked `FAILED` and the process exits with code 1. The run result is written in a `finally` block on a fresh connection, so a lost connection or Ctrl+C is still recorded as `FAILED`. |
| Two runs at once? | Prevented: each run takes a PostgreSQL advisory lock; a second run is refused and exits 1. |
| How are failures recovered? | Run the same command again. The failed file never reached `SUCCESS`, so it is retried; files that succeeded are skipped. |
| Late-arriving records? | A child whose parent has not arrived (e.g. an item before its order) is held as `INVESTIGATE`. Every run re-checks held batches and loads the rows whose parent now exists. Demonstrated: a failed orders file left 3,442 items held; the next run loaded the orders and then the items. |

**Why per-file transactions:** raw load, data-quality results and staging
rows of a file are committed together or not at all, so a crash never leaves
half a file behind. Transformations run afterwards in their own transaction;
they are idempotent, so if they fail the next run simply runs them again.

---

## 7. Testing

`tests/test_pipeline.py` - 21 tests, each running the real pipeline on a small
dataset whose metrics were calculated by hand, in a separate `jaffle_test`
database. They cover schema compliance (including a **deliberately invalid
input**), invalid-record handling and categories, business-key uniqueness and
corrections, referential integrity, record-count and aggregate
reconciliation, transformation correctness, idempotency, failure recovery,
empty and blank input, and run-level safety (concurrent and interrupted runs,
unreachable database, missing files). The full list is in the [README](../README.md#5-tests).

Results: `outputs/test_results.txt` (21 passed).

---

## 8. Performance, observability and reliability

Measured numbers are in `outputs/benchmark.txt`: a full run of the
6-month sample (39,599 rows) takes **~11 s** on a laptop, and a rerun with
no new files **~2 s**. Everything below marked *proposal* is not
implemented.

| Topic | Current solution | At larger scale (*proposal*) |
|---|---|---|
| **Memory and partitioning** | One file is processed at a time; it is read fully into memory and sent to PostgreSQL with one `COPY`, so peak memory is about 6x the file size (measured once with a generated 82 MB, 1M-row file: ~530 MB; see `outputs/benchmark.txt`). Data is already split into monthly batches, which keeps files small. | Read files in chunks; range-partition `raw` and the fact tables by month so old partitions are untouched and can be archived. |
| **SQL optimisation and indexing** | Primary keys on every business key; indexes on fact foreign keys and `order_date`; `(batch_id, row_num)` key on raw so each batch is read by index; unchanged rows skipped by `IS DISTINCT FROM`. | Check heavy queries with `EXPLAIN ANALYZE`; replace per-row PL/pgSQL safe casts (the main cost in validation) with set-based regex checks; rebuild only the affected months of metrics instead of all. |
| **Batch or distributed processing** | Single-node batch is enough: the measured rate is ~5,000 rows/s for validation (a generated 1M-row file loaded in ~3 minutes, measured once) and the full public dataset is only ~4x the sample. | Spark or a cloud warehouse would only be justified at hundreds of millions of rows; the same layered design and SQL would carry over. |
| **Retry and controlled failure** | Downloads retry 3 times with backoff (2 s, 4 s). Data errors are never retried blindly: bad rows are rejected with a reason, bad files fail alone and are retried on the next run. | Scheduler-level retry of the whole run with a limit, then alert. |
| **Structured logging and monitoring** | Every step logs timestamp, level, component, file, batch id, counts and duration to `outputs/logs/pipeline.log` and the console. Full history is queryable in `audit.pipeline_runs`, `audit.ingestion_log` and `audit.rejected_records`. | JSON log lines shipped to a log platform; a dashboard on the audit tables (rejection rate per rule, run duration). |
| **Freshness and completion** | Run status (`SUCCESS`/`FAILED`) with file counts; reconciliation must pass; freshness check logs the newest order and warns when it is older than `monitoring.freshness_max_age_days` (disabled for this historical sample). | Expected-files check per day (e.g. alert if no orders file arrived). |
| **Alerting, recovery and support** | Exit code 1 on failure lets any scheduler alert; recovery is "fix the cause, run the same command again". See the runbook in the README. | Schedule with Windows Task Scheduler or cron (or an Airflow DAG with one task per stage); alert on failed run, failed reconciliation, rejection rate above a threshold, or stale data. |
