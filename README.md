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

# 2. Get the code (or unzip the submission) and go into the project folder
git clone https://github.com/TalhaShahid0/Data_Engineer_Assessment
cd Data_Engineer_Assessment

# 3. Give the pipeline the database password (never stored in the project)
[Environment]::SetEnvironmentVariable('PGPASSWORD', '<your password>', 'User')
#    then open a NEW terminal in the project folder so the variable is visible

# 4. Create a virtual environment and install the dependencies
python -m venv venv
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned   # once, only if activate says "running scripts is disabled"
venv\Scripts\activate
pip install -r requirements.txt

# 5. Create the database
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
| `--simulate-failure FILE` | Fail on purpose while processing that file (recovery demo) |

The process exits with code **0** on success and **1** on failure, so a
scheduler can alert on it. Only one run can be active at a time (a PostgreSQL
advisory lock); a second run is refused.

**Scheduling** - for example a daily run at 02:00 with Windows Task Scheduler
(adjust the path to the project folder):

```powershell
schtasks /create /tn "JaffleShopPipeline" /sc daily /st 02:00 /tr "cmd /c cd /d C:\path\to\Data_Engineer_Assessment && venv\Scripts\python.exe -m src.pipeline.run_pipeline"
```

or with cron on Linux/macOS: `0 2 * * * cd /path/to/Data_Engineer_Assessment && venv/bin/python -m src.pipeline.run_pipeline`.
`PGPASSWORD` must be set for the account the task runs as.

A full run of the sample takes about **11 seconds**; a rerun with nothing new about **2 seconds**.

### Outputs

The pipeline rewrites the metric, data-quality and reconciliation CSVs on every
run and appends to the log. `test_results.txt`, `benchmark.txt` and the demo
file are evidence files produced once for this submission (sections 5 and 6).

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
database (and the project log) are not touched. 21 tests run the real
pipeline on a small dataset (`tests/fixtures/good/`) whose metrics were
calculated by hand:

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
| `test_empty_file_is_logged_and_does_not_fail_the_run` | Empty input handling; the empty file is not re-processed |
| `test_correction_is_not_undone_by_recheck_of_older_batch` | Re-checking an older batch never overwrites a newer correction |
| `test_same_uuid_in_different_spelling_is_a_duplicate` | `{uuid}` / `UUID` spellings of one key are rejected as duplicates |
| `test_out_of_range_values_are_rejected_without_failing_the_file` | Amounts that would overflow, `0x10`-style numbers and over-long keys are rejected row by row |
| `test_blank_lines_are_ignored` | Blank lines do not lose or reject rows; a line of only commas is rejected as missing values |
| `test_duplicate_header_column_fails_the_file` | Unexpected schema: the same column twice fails the file |
| `test_unreadable_file_fails_only_that_file` | A file that cannot be opened fails alone; the other files load |
| `test_second_run_is_refused_while_one_is_in_progress` | Only one run at a time |
| `test_interrupted_run_is_recorded_as_failed` | Ctrl+C mid-run is recorded as FAILED |
| `test_unreachable_database_gives_a_clear_error` | No database connection: one clear error message and exit code 1 |
| `test_missing_previously_loaded_file_is_reported` | A loaded file that disappears is reported; its records are kept |
| `test_header_only_file_with_wrong_columns_fails` | A file with no data rows still has its header checked |
| `test_recheck_never_overwrites_a_newer_identical_resend` | A held row re-checked later never overwrites a newer, unchanged re-send of the same record |

Latest result: **21 passed** (`outputs/test_results.txt`).

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
| A file has a missing or duplicate column, cannot be opened, or is not valid UTF-8 text | That file is rolled back and logged `FAILED` with the error; other files continue; run exits 1 | Fix or replace the file, rerun the pipeline: only that file is processed |
| Bad rows inside a file | Rows rejected with a reason (`INVALID` / `INVESTIGATE`); the rest of the file loads | Review `outputs/rejected_records.csv`; send a corrected file - its rows upsert over the old ones |
| A row's parent has not arrived yet | Row held as `INVESTIGATE` | Nothing: it loads automatically on the first run after the parent arrives |
| Database cannot be reached at startup (not running, wrong settings or password) | One clear error line ("Cannot connect to the database: ..."), nothing is changed, exit 1 | Start PostgreSQL / fix `config/config.yaml` or `PGPASSWORD`, then rerun |
| Database unavailable / crash / Ctrl+C mid-run | The current file's transaction is rolled back, nothing half-loaded; the run is recorded as `FAILED` (on a fresh connection when possible) | Rerun the same command |
| Reconciliation check fails | Run marked `FAILED`, check logged as ERROR | Inspect `outputs/layer_reconciliation.csv` |

### Investigating a problem

```sql
-- Recent runs and their result
SELECT * FROM audit.pipeline_runs ORDER BY run_id DESC LIMIT 5;

-- Files of the last run: status, counts and error messages
SELECT batch_id, file_name, status, rows_read, rows_accepted, rows_rejected, rows_flagged, error_message
FROM audit.ingestion_log WHERE run_id = (SELECT max(run_id) FROM audit.pipeline_runs)
ORDER BY batch_id;

-- Why records were rejected, with the original row
SELECT row_num, category, rule_name, reason, record
FROM audit.rejected_records WHERE batch_id = <batch_id> ORDER BY row_num;

-- Trace a record from the source file to the facts
SELECT * FROM raw.orders            WHERE id = '<order id>';
SELECT * FROM staging.orders        WHERE order_id = '<order id>';
SELECT * FROM analytics.fct_orders  WHERE order_id = '<order id>';
```

* `row_num` is the number of the data record in the file (1 = first record after
  the header; blank lines are not counted), not the text line number.
* Every run writes one `audit.ingestion_log` row per file, including files it
  skipped, so batch ids keep increasing across runs (e.g. after the demos a new
  file may get batch 44). This is expected; the file's `SUCCESS` row is the
  batch that actually loaded its data.
* `staging` and `analytics` only contain valid UUIDs, so the last two trace
  queries return an error for a malformed id such as `not-a-uuid`. Such records
  never got past validation: look them up in `raw.orders` and
  `audit.rejected_records` instead.

### Known limitations

* Only 6 months of the public data are used (enough to show every behaviour; the full dataset is ~4x larger, estimated ~40 s per full run - see `outputs/benchmark.txt`).
* Dimensions are SCD Type 1 (latest value only); history would need SCD Type 2 (design in the architecture doc).
* Records deleted from the source are not detected (the source has no delete information); if a previously loaded file disappears from the data folder, the run logs a warning and keeps its records.
* A file with the same name but new content is treated as a new batch; its records upsert over earlier versions (the most recently loaded file wins; an older batch never overwrites a newer one).
* The checkpoint is by file **content**: a file whose exact content was loaded before is skipped. So if a file is changed and then changed back to an earlier version, the revert is skipped and the newer values stay. Run `--reset` in that case (a full reload takes ~11 s); production would use a per-file version or a change log from the source.
* Changing `sql/schema.sql` or a data-quality rule only affects files loaded afterwards, because files already loaded are skipped. After such a change run `--reset` to rebuild everything (~11 s); production would use versioned migrations instead.
* Batches with `INVESTIGATE` rows are re-checked on every run; with many permanently-invalid rows this should get an age limit.
* An item held for an unknown product leaves its order's subtotal unmatched, so the reconciliation keeps the run `FAILED` until the item is resolved (intended: the data is incomplete).
* Each file is read fully into memory before `COPY`: peak memory is about 6x the file size (measured once with a generated 1M-row, 82 MB file: ~530 MB). Fine for monthly files of a few MB; very large files should be read in chunks.
* A file containing NUL bytes or non-UTF-8 text fails as a whole (with a clear error) rather than row by row.
* Timestamps with a time-zone offset (e.g. `+05:00`) are accepted and the offset is dropped (the source has no time zones).
* Metrics are fully rebuilt each run (fine at this size; see future improvements).
* Freshness alerting is disabled because the sample is historical data.

### Future improvements

* Move scheduling to an orchestrator (e.g. an Airflow DAG with one task per stage) with alerts on failed runs, rejection-rate thresholds and stale data.
* Rebuild only the months affected by a batch instead of all metrics.
* Partition `raw` and fact tables by month; archive old raw partitions.
* Replace per-row PL/pgSQL safe casts with set-based checks for higher throughput.
* SCD Type 2 for products if price history becomes available.
* Run tests automatically on every push (GitHub Actions with a PostgreSQL service).

---

## 8. Tools and assistance

Built with the help of an AI coding assistant (Claude Code). All design
decisions, code and results were reviewed by the author.
