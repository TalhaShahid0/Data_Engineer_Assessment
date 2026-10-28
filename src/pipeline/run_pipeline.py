"""Pipeline entry point: ingest -> validate -> transform -> export.

Usage:
    python -m src.pipeline.run_pipeline                  # process new files
    python -m src.pipeline.run_pipeline --reset          # rebuild the database first
    python -m src.pipeline.run_pipeline --simulate-failure orders_2016_11.csv

How reruns stay safe:
  - Checkpoint: a file whose SHA-256 hash was already loaded successfully is SKIPPED.
  - Each file is processed in ONE transaction (raw load + data quality + staging).
    If anything fails, the whole file is rolled back, logged as FAILED, and it is
    retried automatically on the next run because it never reached SUCCESS.
  - All loads are upserts, so reprocessing the same data never creates duplicates.
  - Rows held as INVESTIGATE (e.g. an item whose order has not arrived yet) are
    re-checked on every run and loaded once their parent record exists.
  - Only one run at a time (PostgreSQL advisory lock); the run result is always
    recorded, even if the run is interrupted or the connection is lost.

Exit code is 1 when the run FAILED, so a scheduler can alert on it.
"""
import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2

from src.ingestion.ingest import already_loaded, file_sha256, ingest_file, log_batch
from src.pipeline.db import PROJECT_ROOT, display_path, fetch_all, get_connection, load_config, run_sql_file
from src.pipeline.logger import get_logger
from src.transformation.transform import export_metrics, run_transformations
from src.validation.validate import export_reports, validate_batch

log = get_logger("pipeline")

SCHEMA_SQL = PROJECT_ROOT / "sql" / "schema.sql"


class SimulatedFailure(Exception):
    """Raised on purpose by --simulate-failure to demonstrate recovery."""


def apply_schema(conn):
    run_sql_file(conn, SCHEMA_SQL)
    conn.commit()
    log.info("Schema created from %s", SCHEMA_SQL.name)


def schema_exists(conn):
    return fetch_all(conn, "SELECT to_regclass('audit.pipeline_runs') IS NOT NULL")[0][0]


def safe_rollback(conn):
    """Roll back, ignoring errors (e.g. when the connection is already lost)."""
    try:
        conn.rollback()
    except Exception:  # noqa: BLE001
        pass


def process_file(conn, run_id, table, path, simulate_failure=None):
    """Load and validate one file in a single transaction. Returns its final status."""
    file_hash = "unavailable"   # stays like this if the file cannot even be read
    started = time.perf_counter()
    try:
        file_hash = file_sha256(path)
        if already_loaded(conn, file_hash):
            log_batch(conn, run_id, table["name"], path.name, file_hash, "SKIPPED")
            conn.commit()
            log.info("%s: %s already processed (same content), skipped", table["name"], path.name)
            return "SKIPPED"

        result = ingest_file(conn, run_id, table, path, file_hash)
        if result["status"] == "RUNNING":
            validate_batch(conn, result["batch_id"])
            if simulate_failure == path.name:
                raise SimulatedFailure(f"simulated failure while processing {path.name}")
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE audit.ingestion_log SET status = 'SUCCESS', finished_at = clock_timestamp() "
                    "WHERE batch_id = %s",
                    (result["batch_id"],),
                )
            result["status"] = "SUCCESS"
        conn.commit()
        log.info("%s: %s -> %s in %.2fs", table["name"], path.name, result["status"],
                 time.perf_counter() - started)
        return result["status"]

    except Exception as exc:
        # Undo everything this file did, then record the failure in its own transaction.
        safe_rollback(conn)
        log_batch(conn, run_id, table["name"], path.name, file_hash, "FAILED", str(exc))
        conn.commit()
        log.error("%s: %s FAILED and was rolled back: %s", table["name"], path.name, exc)
        return "FAILED"


def retry_quarantined(conn, run_id, table_order):
    """Re-check batches from earlier runs that still hold INVESTIGATE rows
    (e.g. late-arriving parents). Batches of this run were just validated, after
    their parent tables, so re-checking them again would change nothing."""
    batches = fetch_all(
        conn,
        """SELECT DISTINCT r.batch_id, r.table_name, l.rows_rejected
           FROM audit.rejected_records r
           JOIN audit.ingestion_log l ON l.batch_id = r.batch_id AND l.status = 'SUCCESS'
           WHERE r.category = 'INVESTIGATE' AND l.run_id <> %s""",
        (run_id,),
    )
    for batch_id, table_name, rejected_before in sorted(batches, key=lambda b: (table_order.index(b[1]), b[0])):
        rejected_after = validate_batch(conn, batch_id, recheck=True)["rows_rejected"]
        conn.commit()
        if rejected_after < rejected_before:
            log.info("%s: batch %s re-checked, %s previously held row(s) now loaded",
                     table_name, batch_id, rejected_before - rejected_after)


def warn_missing_files(conn, data_dir):
    """Warn about files loaded earlier that are no longer in the data folder.
    Their records are kept: deletions in the source are not detected."""
    present = {path.name for path in data_dir.iterdir()} if data_dir.exists() else set()
    loaded = fetch_all(conn, "SELECT DISTINCT file_name FROM audit.ingestion_log WHERE status = 'SUCCESS'")
    for (file_name,) in sorted(loaded):
        if file_name not in present:
            log.warning("%s was loaded in an earlier run but is no longer in %s; its records are kept",
                        file_name, data_dir.name)


def check_freshness(conn, max_age_days):
    """Report the newest order; warn if it is older than the allowed age."""
    latest = fetch_all(conn, "SELECT max(ordered_at) FROM analytics.fct_orders")[0][0]
    if latest is None:
        log.warning("Freshness: no orders loaded yet")
        return
    age_days = (datetime.now() - latest).days
    if max_age_days is not None and age_days > max_age_days:
        log.warning("Freshness: newest order is %s (%s days old, limit %s)", latest, age_days, max_age_days)
    else:
        log.info("Freshness: newest order is %s", latest)


def finish_run(config, run_id, status, error_message, counts):
    """Record the run result on a fresh connection, so it is saved even if the
    main connection was lost. Never raises: the original error must stay visible."""
    try:
        conn = get_connection(config)
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE audit.pipeline_runs
                   SET status = %s, finished_at = clock_timestamp(), error_message = %s,
                       files_processed = %s, files_skipped = %s, files_failed = %s
                   WHERE run_id = %s""",
                (status, error_message, counts["SUCCESS"] + counts["EMPTY"], counts["SKIPPED"],
                 counts["FAILED"], run_id),
            )
        conn.commit()
        conn.close()
    except Exception as exc:  # noqa: BLE001
        log.error("Could not record the result of run %s: %s", run_id, exc)


def run_pipeline(config, data_dir=None, reset=False, simulate_failure=None):
    """Run the whole pipeline once. Returns a summary dict."""
    data_dir = Path(data_dir) if data_dir else PROJECT_ROOT / config["paths"]["sample_data"]
    out_dir = PROJECT_ROOT / config["paths"]["outputs"]
    table_order = [t["name"] for t in config["tables"]]
    counts = {"SUCCESS": 0, "SKIPPED": 0, "FAILED": 0, "EMPTY": 0}
    run_started = time.perf_counter()

    conn = get_connection(config)

    # Only one run at a time: the lock is held until this connection closes.
    if not fetch_all(conn, "SELECT pg_try_advisory_lock(hashtext('jaffle_pipeline'))")[0][0]:
        conn.close()
        log.error("Another pipeline run is in progress; this run was not started")
        return {"run_id": None, "status": "FAILED", "files": counts,
                "error": "another run is in progress"}

    if reset or not schema_exists(conn):
        apply_schema(conn)

    with conn.cursor() as cur:
        cur.execute("INSERT INTO audit.pipeline_runs DEFAULT VALUES RETURNING run_id")
        run_id = cur.fetchone()[0]
    conn.commit()

    # Pessimistic default: if the run is interrupted (e.g. Ctrl+C), it is recorded as FAILED.
    status, error_message = "FAILED", "run interrupted"
    try:
        log.info("===== Run %s started (data: %s) =====", run_id, display_path(data_dir))

        # 1. Ingest + validate every file, parents before children.
        for table in config["tables"]:
            for path in sorted(data_dir.glob(table["file_pattern"])):
                file_status = process_file(conn, run_id, table, path, simulate_failure)
                counts[file_status] += 1
        warn_missing_files(conn, data_dir)

        # 2. Re-check rows that were waiting for a parent record.
        retry_quarantined(conn, run_id, table_order)

        # 3. Build the analytics layer and verify it (one transaction).
        started = time.perf_counter()
        transform = run_transformations(conn)
        conn.commit()
        log.info("Transformations finished in %.2fs", time.perf_counter() - started)

        # 4. Reports, metrics and monitoring checks.
        export_reports(conn, out_dir)
        export_metrics(conn, out_dir)
        check_freshness(conn, config.get("monitoring", {}).get("freshness_max_age_days"))

        problems = []
        if counts["FAILED"]:
            problems.append(f"{counts['FAILED']} file(s) failed")
        if transform["reconciliation_failures"]:
            problems.append(f"{transform['reconciliation_failures']} reconciliation check(s) failed")
        status = "FAILED" if problems else "SUCCESS"
        error_message = "; ".join(problems) or None

    except Exception as exc:
        safe_rollback(conn)
        status, error_message = "FAILED", f"pipeline error: {exc}"
        log.exception("Run failed: %s", exc)

    finally:
        finish_run(config, run_id, status, error_message, counts)
        try:
            conn.close()   # also releases the run lock
        except Exception:  # noqa: BLE001
            pass

    log.info("===== Run %s %s in %.1fs | loaded %s, empty %s, skipped %s, failed %s%s =====",
             run_id, status, time.perf_counter() - run_started, counts["SUCCESS"], counts["EMPTY"],
             counts["SKIPPED"], counts["FAILED"], f" | {error_message}" if error_message else "")
    return {"run_id": run_id, "status": status, "files": counts, "error": error_message}


def main():
    parser = argparse.ArgumentParser(description="Run the Jaffle Shop data pipeline.")
    parser.add_argument("--reset", action="store_true",
                        help="drop and recreate all schemas before running")
    parser.add_argument("--simulate-failure", metavar="FILE_NAME",
                        help="fail on purpose while processing this file (recovery demo)")
    args = parser.parse_args()

    try:
        summary = run_pipeline(load_config(), reset=args.reset, simulate_failure=args.simulate_failure)
    except psycopg2.OperationalError as exc:
        # Database unreachable or login failed: one clear line instead of a traceback.
        log.error("Cannot connect to the database: %s", str(exc).strip().splitlines()[0])
        log.error("Check that PostgreSQL is running, the settings in config/config.yaml "
                  "and the PGPASSWORD environment variable")
        sys.exit(1)
    sys.exit(0 if summary["status"] == "SUCCESS" else 1)


if __name__ == "__main__":
    main()
