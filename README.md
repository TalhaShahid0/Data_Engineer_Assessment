# Jaffle Shop Data Pipeline

An end-to-end batch pipeline that loads the public **Jaffle Shop** sample data
into **PostgreSQL**, validates it with SQL data-quality rules, builds a star
schema, and produces three analytical metric tables. It is safe to rerun:
files already loaded are skipped, failed files are rolled back and retried,
and records are upserted by business key, so nothing is duplicated.

* **Stack:** Python 3.12 (thin orchestration) + PostgreSQL 16 (all data logic in SQL) + pytest
* **Design and decisions:** [docs/architecture.md](docs/architecture.md)

```
data/sample/*.csv ──► raw (all TEXT) ──► staging (typed, clean) ──► analytics (star schema + metrics) ──► outputs/*.csv
                                     └─► audit.rejected_records (rule, reason, category)
```

---

## 1. Dataset

| | |
|---|---|
| **Source** | [dbt-labs/jaffle-shop-data](https://github.com/dbt-labs/jaffle-shop-data), folder `jaffle-data/` (public, synthetic) |
| **Subset used** | All reference files + the **first 6 months** of orders and items (Sep 2016 - Feb 2017), committed in `data/sample/` |
| **Incremental batches** | Orders and items are split into one file per month (`orders_2016_09.csv`, `items_2016_09.csv`, ...) to simulate new batches arriving |
| **Rebuild the subset** | `python -m src.ingestion.download_data` (downloads from GitHub, retries on failure) |

| File | Rows | Expected columns | Key |
|---|---|---|---|
| `raw_stores.csv` | 6 | id, name, opened_at, tax_rate | id |
| `raw_customers.csv` | 930 | id, name | id |
| `raw_products.csv` | 10 | sku, name, type, price, description | sku |
| `raw_supplies.csv` | 65 | id, name, cost, perishable, sku | **id + sku** |
| `orders_YYYY_MM.csv` (6 files) | 15,774 | id, customer, ordered_at, store_id, subtotal, tax_paid, order_total | id |
| `items_YYYY_MM.csv` (6 files) | 22,814 | id, order_id, sku | id |

Expected columns and required fields per file are configured in `config/config.yaml`.

**Limitations of the data:** synthetic; amounts in integer cents; timestamps
without a time zone; no item quantity or price (one row = one unit); no
change tracking; supply `id` is not unique; only the Philadelphia store is
open during the 6 sampled months. All findings are listed in
[docs/architecture.md § 4](docs/architecture.md#4-data-quality).

---

## 2. Project structure

```
├── README.md
├── requirements.txt
├── config/config.yaml            # database, paths, expected schema per file, monitoring
├── data/sample/                  # input CSV files (subset, monthly batches)
├── src/
│   ├── ingestion/                # download_data.py, ingest.py (header check + COPY to raw)
│   ├── validation/validate.py    # runs sql/data_quality.sql, exports DQ reports
│   ├── transformation/transform.py  # runs sql/transformations.sql, exports metrics
│   └── pipeline/                 # run_pipeline.py (entry point), db.py, logger.py
├── sql/
│   ├── schema.sql                # all DDL: schemas, tables, keys, indexes, views
│   ├── data_quality.sql          # validation rules + typed load into staging
│   └── transformations.sql       # dimensions, facts and metric tables
├── tests/                        # pytest suite + fixtures (good data, dirty demo file)
├── docs/architecture.md          # architecture, data model, DQ rules, metrics, operations
└── outputs/                      # generated: metrics, DQ reports, reconciliation, logs, test results, benchmark
```

---

## 3. Setup (Windows)

**Requirements:** Python 3.12+ and PostgreSQL 16 (any recent version should work).

```powershell
# 1. Install the tools (or download them from python.org / postgresql.org)
winget install -e --id Python.Python.3.12
winget install -e --id PostgreSQL.PostgreSQL.16 --interactive   # choose a password for user "postgres"

# 2. Create a virtual environment and install the dependencies
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# 3. Give the pipeline the database password (never stored in the project)
[Environment]::SetEnvironmentVariable('PGPASSWORD', '<your password>', 'User')
#    then open a new terminal so the variable is visible

# 4. Create the database
& "C:\Program Files\PostgreSQL\16\bin\psql.exe" -U postgres -h localhost -c "CREATE DATABASE jaffle"
```

Connection settings (host, port, database, user) are in `config/config.yaml`.
On macOS/Linux the steps are the same with `source venv/bin/activate` and
`export PGPASSWORD=...`.

---

## 4. Running the pipeline

```powershell
python -m src.pipeline.run_pipeline --reset     # first run: create all tables, load everything
python -m src.pipeline.run_pipeline             # later runs: only new or failed files are processed
```

| Option | Effect |
|---|---|
| `--reset` | Drop and recreate all schemas (`sql/schema.sql`) before running |
| `--data-dir PATH` | Read input files from another folder (default `data/sample`) |
| `--simulate-failure FILE` | Fail on purpose while processing that file (recovery demo) |

The process exits with code **0** on success and **1** on failure, so a
scheduler (Windows Task Scheduler, cron, Airflow) can alert on it.

A full run of the sample takes about **10 seconds**; a rerun with nothing new about **1.6 seconds**.

### Outputs (regenerated on every run)

| File | Content |
|---|---|
| `outputs/metrics/monthly_store_revenue.csv` | Metric 1: orders, revenue and average order value per month and store |
| `outputs/metrics/customer_summary.csv` | Metric 2: first/last order, order count, lifetime value, repeat flag per customer |
| `outputs/metrics/monthly_product_sales.csv` | Metric 3: units, revenue, cost and gross margin per month and product |
| `outputs/rejected_records.csv` | Every record that broke a rule: file, row, category, rule, reason, original row |
| `outputs/dq_summary.csv` | Count of records per table / category / rule |
| `outputs/reconciliation.csv` | Per file: rows read = accepted + rejected, all rows reached raw |
| `outputs/layer_reconciliation.csv` | 8 checks from staging to facts to metrics (all must pass) |
| `outputs/logs/pipeline.log` | Run log (the committed log shows: clean run, rerun, failure, recovery, dirty file, final run) |
| `outputs/test_results.txt` | Output of the test suite |
| `outputs/benchmark.txt` | Measured timings per step |
| `outputs/demo_dirty_file_rejected_records.csv` | Rejected records from the dirty-file demo (section 6) |

**Results on the sample:** 15,774 orders and 22,814 items loaded, $170,294.45
revenue, 15,649 non-empty orders, 338 customers with orders, all
reconciliation checks passed. The only rule hit by the real data is
`zero_value_order` (125 empty orders, loaded and flagged).

---

## 5. Tests

```powershell
python -m pytest -v
```

The tests create and use a separate database, `jaffle_test`, so the main
database is not touched. 9 tests run the real pipeline on a small dataset
(`tests/fixtures/good/`) whose metrics were calculated by hand:

| Test | Checks |
|---|---|
| `test_file_missing_required_column_is_rejected` | Schema compliance - **deliberately invalid input**: file FAILED, nothing loaded, other files unaffected |
| `test_invalid_rows_are_rejected_with_rule_and_category` | Each bad row gets the right rule and category; only valid / EXCEPTION rows load |
| `test_corrected_record_updates_instead_of_duplicating` | Business-key uniqueness; a correction in a later file replaces the old value |
| `test_no_orphan_records_reach_the_facts` | Referential integrity; unknown order / product held for investigation |
| `test_reconciliation_balances_with_rejected_rows` | Record counts and totals reconcile even with rejected and malformed rows |
| `test_metrics_match_hand_calculated_values` | Transformation correctness for all 3 metrics |
| `test_rerun_skips_loaded_files_and_changes_nothing` | Idempotency: second run skips everything, tables identical |
| `test_failed_file_is_rolled_back_and_recovered_on_next_run` | Failure is rolled back; next run retries and completes |
| `test_empty_file_is_logged_and_does_not_fail_the_run` | Empty input handling |

Latest result: **9 passed** (`outputs/test_results.txt`).

---

## 6. Demonstrations

**Safe rerun** - run the pipeline twice; the second run reports `skipped 16` and no table changes.

**Failure and recovery**
```powershell
python -m src.pipeline.run_pipeline --reset --simulate-failure orders_2016_11.csv
#   -> orders_2016_11.csv FAILED and was rolled back; its 3,442 items are held (unknown_order); exit code 1
python -m src.pipeline.run_pipeline
#   -> orders_2016_11.csv retried -> SUCCESS; "3442 previously held row(s) now loaded"; totals identical to a clean run
```

**A dirty file arrives** - `tests/fixtures/dirty/orders_2017_03_dirty.csv` has 12 rows, each breaking a different rule:
```powershell
copy tests\fixtures\dirty\orders_2017_03_dirty.csv data\sample\
python -m src.pipeline.run_pipeline
#   -> read 12, accepted 2, rejected 10, flagged 1; reasons in outputs\rejected_records.csv
del data\sample\orders_2017_03_dirty.csv
python -m src.pipeline.run_pipeline --reset        # back to the clean state
```

---

## 7. Operational notes

### Failure handling

| What fails | What happens | What to do |
|---|---|---|
| Download | Retried 3 times (2 s, 4 s backoff), then a clear error | Check the network / source URL and rerun `download_data` |
| A file has a missing column or cannot be read | That file is rolled back and logged `FAILED`; other files continue; run exits 1 | Fix or replace the file, rerun the pipeline: only that file is processed |
| Bad rows inside a file | Rows rejected with a reason (`INVALID` / `INVESTIGATE`); the rest of the file loads | Review `outputs/rejected_records.csv`; send a corrected file - its rows upsert over the old ones |
| A row's parent has not arrived yet | Row held as `INVESTIGATE` | Nothing: it loads automatically on the first run after the parent arrives |
| Database unavailable / crash mid-run | The current file's transaction is rolled back, nothing half-loaded | Rerun the same command |
| Reconciliation check fails | Run marked `FAILED`, check logged as ERROR | Inspect `outputs/layer_reconciliation.csv` |

### Investigating a problem

```sql
-- Recent runs and their result
SELECT * FROM audit.pipeline_runs ORDER BY run_id DESC LIMIT 5;

-- Files of the last run: status, counts and error messages
SELECT batch_id, file_name, status, rows_read, rows_accepted, rows_rejected, rows_flagged, error_message
FROM audit.ingestion_log WHERE run_id = (SELECT max(run_id) FROM audit.pipeline_runs);

-- Why records were rejected, with the original row
SELECT row_num, category, rule_name, reason, record
FROM audit.rejected_records WHERE batch_id = <batch_id> ORDER BY row_num;

-- Trace a record from the source file to the facts
SELECT * FROM raw.orders            WHERE id = '<order id>';
SELECT * FROM staging.orders        WHERE order_id = '<order id>';
SELECT * FROM analytics.fct_orders  WHERE order_id = '<order id>';
```

### Known limitations

* Only 6 months of the public data are used (enough to show every behaviour; the full dataset is ~4x larger, estimated ~40 s per full run - see `outputs/benchmark.txt`).
* Dimensions are SCD Type 1 (latest value only); history would need SCD Type 2 (design in the architecture doc).
* Records deleted from the source are not detected (the source has no delete information).
* A file with the same name but new content is treated as a new batch; its records upsert over earlier versions (latest file wins).
* Batches with `INVESTIGATE` rows are re-checked on every run; with many permanently-invalid rows this should get an age limit.
* Metrics are fully rebuilt each run (fine at this size; see future improvements).
* Freshness alerting is disabled because the sample is historical data.

### Future improvements

* Schedule with Task Scheduler / cron, or an Airflow DAG with one task per stage, with alerts on failed runs, rejection-rate thresholds and stale data.
* Rebuild only the months affected by a batch instead of all metrics.
* Partition `raw` and fact tables by month; archive old raw partitions.
* Replace per-row PL/pgSQL safe casts with set-based checks for higher throughput.
* SCD Type 2 for products if price history becomes available.
* Run tests automatically on every push (GitHub Actions with a PostgreSQL service).
