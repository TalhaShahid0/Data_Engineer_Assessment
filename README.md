# Jaffle Shop Data Pipeline

A batch data pipeline that loads the public **Jaffle Shop** sample data into
**PostgreSQL**, checks its quality, models it as a star schema and produces
three business metrics.

It is built to be **safe to run again and again**:

- files that were already loaded are skipped,
- a file that fails is rolled back and retried on the next run,
- records are matched on their business key, so nothing is ever duplicated.

**Stack:** Python 3.12 (orchestration only) · PostgreSQL 16 (all data logic in SQL) · pytest

**Design and decisions:** see [docs/architecture.md](docs/architecture.md).

```
data/sample/*.csv ──► raw ──► staging ──► analytics ──► outputs/*.csv
                       │     (typed,      (star schema
                  (as text)   cleaned)     + metrics)
                       │
                       └──► audit.rejected_records (rule, reason, category)
```

---

## Quick start

Already have Python, PostgreSQL and the `PGPASSWORD` variable set up? Then:

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
& "C:\Program Files\PostgreSQL\16\bin\psql.exe" -U postgres -h localhost -c "CREATE DATABASE jaffle"
python -m src.pipeline.run_pipeline --reset
```

Results appear in `outputs/`. For a first-time setup, follow [Setup](#setup-windows) below.

---

## Dataset

- **Source:** [dbt-labs/jaffle-shop-data](https://github.com/dbt-labs/jaffle-shop-data), folder `jaffle-data/`. Public, synthetic data.
- **Subset used:** all reference files plus the **first 6 months** of orders and items (Sep 2016 – Feb 2017). It is committed in `data/sample/`, so no download is needed.
- **Monthly batches:** orders and items are split into one file per month (`orders_2016_09.csv`, `items_2016_09.csv`, …). Each file acts like a new batch arriving.
- **Rebuild the subset:** `python -m src.ingestion.download_data` (downloads from GitHub and retries on failure).

| File | Rows | Columns | Key |
|---|---|---|---|
| `raw_stores.csv` | 6 | id, name, opened_at, tax_rate | id |
| `raw_customers.csv` | 930 | id, name | id |
| `raw_products.csv` | 10 | sku, name, type, price, description | sku |
| `raw_supplies.csv` | 65 | id, name, cost, perishable, sku | **id + sku** |
| `orders_YYYY_MM.csv` (6 files) | 15,774 | id, customer, ordered_at, store_id, subtotal, tax_paid, order_total | id |
| `items_YYYY_MM.csv` (6 files) | 22,814 | id, order_id, sku | id |

The expected columns of each file are defined in `config/config.yaml`.

**Things to know about the data**

- Amounts are stored in whole cents.
- Timestamps have no time zone.
- Items have no quantity or price: one row is one unit sold.
- There is no change tracking (no "updated at" column).
- A supply `id` is not unique on its own; the key is `id + sku`.
- Only the Philadelphia store was open during the 6 sampled months.

All data-quality findings are listed in [docs/architecture.md](docs/architecture.md#data-quality).

---

## Project structure

```
├── README.md
├── requirements.txt
├── config/config.yaml              database, paths, expected columns per file, monitoring
├── data/sample/                    input CSV files (monthly batches)
├── src/
│   ├── ingestion/                  download_data.py, ingest.py (header check + load to raw)
│   ├── validation/validate.py      runs sql/data_quality.sql and exports the DQ reports
│   ├── transformation/transform.py runs sql/transformations.sql and exports the metrics
│   └── pipeline/                   run_pipeline.py (entry point), db.py, logger.py
├── sql/
│   ├── schema.sql                  all tables, keys, indexes and views
│   ├── data_quality.sql            data-quality rules + load into staging
│   └── transformations.sql         dimensions, facts and metric tables
├── tests/                          pytest tests and test data (incl. a "dirty" demo file)
├── docs/architecture.md            how it works and why
└── outputs/                        generated results, reports, logs, test results, benchmark
```

---

## Setup (Windows)

You need **Python 3.12+** and **PostgreSQL 16** (any recent version should work).

**1. Install the tools** (or download them from python.org and postgresql.org)

```powershell
winget install -e --id Python.Python.3.12
winget install -e --id PostgreSQL.PostgreSQL.16 --interactive
```

The PostgreSQL installer asks for a password for the `postgres` user. Remember it.

**2. Get the code** (or unzip the submission) and go into the project folder

```powershell
git clone https://github.com/TalhaShahid0/Data_Engineer_Assessment
cd Data_Engineer_Assessment
```

**3. Store the database password** in an environment variable. It is never saved in the project.

```powershell
[Environment]::SetEnvironmentVariable('PGPASSWORD', '<your password>', 'User')
```

Then **open a new terminal in the project folder**, so the variable is visible.

**4. Create a virtual environment and install the dependencies**

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

If `activate` fails with *"running scripts is disabled"*, run this once and try again:
`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

**5. Create the database**

```powershell
& "C:\Program Files\PostgreSQL\16\bin\psql.exe" -U postgres -h localhost -c "CREATE DATABASE jaffle"
```

Connection settings (host, port, database, user) are in `config/config.yaml`.
On macOS or Linux the steps are the same, using `source venv/bin/activate` and
`export PGPASSWORD=...`.

---

## Running the pipeline

```powershell
python -m src.pipeline.run_pipeline --reset   # first run: create all tables and load everything
python -m src.pipeline.run_pipeline           # later runs: only new or failed files are processed
```

| Option | What it does |
|---|---|
| `--reset` | Drops and recreates all tables before running |
| `--simulate-failure FILE` | Makes one file fail on purpose (for the recovery demo) |

- A full run takes about **12 seconds**; a run with nothing new takes about **2 seconds**.
- The exit code is **0** on success and **1** on failure, so a scheduler can raise an alert.
- Only one run can be active at a time (PostgreSQL advisory lock). A second run is refused.

### Scheduling

Daily at 02:00 with Windows Task Scheduler (adjust the folder path):

```powershell
schtasks /create /tn "JaffleShopPipeline" /sc daily /st 02:00 /tr "cmd /c cd /d C:\path\to\Data_Engineer_Assessment && venv\Scripts\python.exe -m src.pipeline.run_pipeline"
```

Or with cron on Linux/macOS:

```
0 2 * * * cd /path/to/Data_Engineer_Assessment && venv/bin/python -m src.pipeline.run_pipeline
```

`PGPASSWORD` must be set for the account the task runs as.

### Outputs

**Rewritten on every run:**

| File | Content |
|---|---|
| `outputs/metrics/monthly_store_revenue.csv` | Metric 1 – orders, revenue and average order value per month and store |
| `outputs/metrics/customer_summary.csv` | Metric 2 – first/last order, order count, lifetime value and repeat flag per customer |
| `outputs/metrics/monthly_product_sales.csv` | Metric 3 – units, revenue, cost and gross margin per month and product |
| `outputs/rejected_records.csv` | Every record that broke a rule, with the reason and the original row |
| `outputs/dq_summary.csv` | Number of records per table, category and rule |
| `outputs/reconciliation.csv` | Per file: rows read = accepted + rejected |
| `outputs/layer_reconciliation.csv` | 8 checks from staging to facts to metrics (all must pass) |
| `outputs/logs/pipeline.log` | Run log (new runs are appended) |

**Evidence files, produced once for this submission:**

| File | Content |
|---|---|
| `outputs/test_results.txt` | Output of the test suite |
| `outputs/benchmark.txt` | Measured timings |
| `outputs/demo_dirty_file_rejected_records.csv` | Rejected records from the dirty-file demo |

The committed log shows a clean run, a rerun, a failure, the recovery, a dirty
file and a final clean run. `--reset` restarts run numbers at 1; the log marks
those runs with *"after --reset"*.

### Results on the sample

- 15,774 orders and 22,814 items loaded
- $170,294.45 revenue, 15,649 non-empty orders, 338 customers with orders
- All reconciliation checks pass
- The only rule hit by the real data is `zero_value_order`: 125 empty orders, loaded and flagged

---

## Tests

```powershell
python -m pytest -v
```

The tests use their own database, `jaffle_test`, so your data and the project
log are not touched. Each of the **21 tests** runs the real pipeline on a small
dataset (`tests/fixtures/good/`) whose results were calculated by hand.

| Area | Test | What it checks |
|---|---|---|
| Schema | `test_file_missing_required_column_is_rejected` | **Deliberately invalid input:** the file fails, nothing is loaded, other files are unaffected |
| Schema | `test_duplicate_header_column_fails_the_file` | The same column twice fails the file |
| Schema | `test_header_only_file_with_wrong_columns_fails` | A file without data rows still has its header checked |
| Data quality | `test_invalid_rows_are_rejected_with_rule_and_category` | Each bad row gets the right rule and category; only valid rows load |
| Data quality | `test_same_uuid_in_different_spelling_is_a_duplicate` | `{uuid}` and upper-case spellings of one key count as duplicates |
| Data quality | `test_out_of_range_values_are_rejected_without_failing_the_file` | Too-large amounts, `0x10`-style numbers and over-long keys are rejected row by row |
| Data quality | `test_blank_lines_are_ignored` | Blank lines are skipped; a line of only commas is rejected as missing values |
| Integrity | `test_no_orphan_records_reach_the_facts` | Items with an unknown order or product are held, never loaded |
| Integrity | `test_reconciliation_balances_with_rejected_rows` | Counts and totals reconcile, even with rejected and malformed rows |
| Metrics | `test_metrics_match_hand_calculated_values` | All 3 metrics match values calculated by hand |
| Reruns | `test_rerun_skips_loaded_files_and_changes_nothing` | A second run skips every file and changes nothing |
| Reruns | `test_corrected_record_updates_instead_of_duplicating` | A corrected record in a later file replaces the old value |
| Reruns | `test_correction_is_not_undone_by_recheck_of_older_batch` | Re-checking an older batch never undoes a newer correction |
| Reruns | `test_recheck_never_overwrites_a_newer_identical_resend` | A held row re-checked later never overwrites a newer copy of the record |
| Recovery | `test_failed_file_is_rolled_back_and_recovered_on_next_run` | A failed file is rolled back and loaded on the next run |
| Recovery | `test_unreadable_file_fails_only_that_file` | A file that cannot be opened fails alone |
| Recovery | `test_interrupted_run_is_recorded_as_failed` | Ctrl+C during a run is recorded as FAILED |
| Recovery | `test_second_run_is_refused_while_one_is_in_progress` | Only one run at a time |
| Recovery | `test_unreachable_database_gives_a_clear_error` | No database: one clear error message and exit code 1 |
| Input files | `test_empty_file_is_logged_and_does_not_fail_the_run` | An empty file is logged and not processed again |
| Input files | `test_missing_previously_loaded_file_is_reported` | A loaded file that disappears is reported; its records are kept |

Latest result: **21 passed** (see `outputs/test_results.txt`).

---

## Demonstrations

### 1. Safe rerun

Run the pipeline twice. The second run reports `skipped 16` and changes nothing.

### 2. Failure and recovery

```powershell
python -m src.pipeline.run_pipeline --reset --simulate-failure orders_2016_11.csv
```

`orders_2016_11.csv` fails and is rolled back. Its 3,442 items are held
because their orders are missing (`unknown_order`). Exit code 1.

```powershell
python -m src.pipeline.run_pipeline
```

The failed file is retried and succeeds. The log says *"3442 previously held
row(s) now loaded"*, and the totals are the same as after a clean run.

### 3. A dirty file arrives

`tests/fixtures/dirty/orders_2017_03_dirty.csv` has 12 rows, each breaking a different rule.

```powershell
copy tests\fixtures\dirty\orders_2017_03_dirty.csv data\sample\
python -m src.pipeline.run_pipeline
```

Result: read 12, accepted 2, rejected 10, flagged 1. The reasons are in
`outputs\rejected_records.csv`. To go back to the clean state:

```powershell
del data\sample\orders_2017_03_dirty.csv
python -m src.pipeline.run_pipeline --reset
```

---

## Operations

### When something fails

| Problem | What the pipeline does | What you do |
|---|---|---|
| Download fails | Retries 3 times (after 2 s and 4 s), then shows an error | Check the network or source URL, then rerun `download_data` |
| A file is broken (missing or duplicate column, cannot be opened, not UTF-8 text) | Rolls back that file, logs it as `FAILED` with the error, continues with the other files; exit code 1 | Fix or replace the file and run again – only that file is processed |
| Some rows in a file are bad | Rejects those rows with a reason; the rest of the file loads | Check `outputs/rejected_records.csv`; send a corrected file |
| A row's parent has not arrived yet | Holds the row as `INVESTIGATE` | Nothing – it loads automatically once the parent arrives |
| The database cannot be reached (not running, wrong settings or password) | Prints one clear error line and changes nothing; exit code 1 | Start PostgreSQL or fix `config/config.yaml` / `PGPASSWORD`, then run again |
| Crash, lost connection or Ctrl+C during a run | Rolls back the current file (never half-loaded); records the run as `FAILED` | Run the same command again |
| A reconciliation check fails | Marks the run `FAILED` and logs the check as an error | Look at `outputs/layer_reconciliation.csv` |

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

-- Follow one record from the source file to the facts
SELECT * FROM raw.orders            WHERE id = '<order id>';
SELECT * FROM staging.orders        WHERE order_id = '<order id>';
SELECT * FROM analytics.fct_orders  WHERE order_id = '<order id>';
```

Good to know:

- **`row_num`** is the number of the record in the file (1 = first record after the header; blank lines are not counted). It is not the text line number.
- **Batch ids keep growing.** Every run logs one row per file, including skipped files, so batch numbers increase across runs (after the demos a new file may get batch 44). The file's `SUCCESS` row is the batch that loaded its data.
- **Malformed ids** (such as `not-a-uuid`) never get past validation, so the `staging` and `analytics` queries return an error for them. Look them up in `raw.orders` and `audit.rejected_records` instead.

### Known limitations

**Scope**

- Only 6 months of the public data are used. The full dataset is about 4× larger (estimated ~40 s per full run).
- Dimensions keep only the latest value (SCD Type 1). Keeping history would need SCD Type 2 (described in the architecture doc).
- Metrics are fully rebuilt on every run. Fine at this size.
- Freshness alerting is switched off, because the sample is historical data.

**Reloading and changes**

- Records deleted from the source are not detected (the source has no delete information). If a loaded file disappears from the data folder, the run logs a warning and keeps its records.
- A file with the same name but new content is loaded as a new batch. Its records replace older versions; an older batch never overwrites a newer one.
- Files are recognised by their **content**. A file whose exact content was loaded before is skipped. So if a file is changed and then changed back, the change back is skipped and the newer values stay. Run `--reset` in that case (about 12 s). In production this would use a per-file version or a change log from the source.
- Changing `sql/schema.sql` or a data-quality rule only affects files loaded afterwards. Run `--reset` after such a change. In production this would use versioned migrations.

**Data handling**

- Batches with held (`INVESTIGATE`) rows are re-checked on every run. With many rows that never get fixed, this should get an age limit.
- An item held for an unknown product leaves its order's subtotal unmatched, so reconciliation keeps the run `FAILED` until the item is fixed. This is intended: the data is incomplete.
- A file with NUL bytes or non-UTF-8 text fails as a whole (with a clear error), not row by row.
- Timestamps with a time-zone offset (such as `+05:00`) are accepted, but the offset is dropped.

**Performance**

- Each file is read fully into memory before loading. Peak memory is about 6× the file size (measured once with a generated 1M-row, 82 MB file: about 530 MB). Fine for monthly files of a few MB; very large files should be read in chunks.

### Future improvements

- Move scheduling to an orchestrator (for example an Airflow DAG with one task per stage) with alerts for failed runs, high rejection rates and stale data.
- Rebuild only the months affected by a batch instead of all metrics.
- Partition the `raw` and fact tables by month, and archive old raw data.
- Replace the per-row safe-cast functions with set-based checks for higher throughput.
- Keep product price history (SCD Type 2) if the source starts providing it.
- Run the tests automatically on every push (GitHub Actions with a PostgreSQL service).

---

## Tools and assistance

Built with the help of an AI coding assistant (Claude Code). All design
decisions, code and results were reviewed by the author.
