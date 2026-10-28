"""Run the SQL data-quality rules for a batch and export the results.

All rules live in sql/data_quality.sql; this module only runs that file for a
batch, reads back the batch statistics and writes the report CSVs.
validate_batch() does NOT commit: the caller owns the transaction.
"""
from src.pipeline.db import PROJECT_ROOT, export_query_to_csv, fetch_all, run_sql_file
from src.pipeline.logger import get_logger

log = get_logger("validate")

DATA_QUALITY_SQL = PROJECT_ROOT / "sql" / "data_quality.sql"


def validate_batch(conn, batch_id):
    """Apply the data-quality rules to one batch and load its clean rows to staging."""
    run_sql_file(conn, DATA_QUALITY_SQL, {"batch_id": batch_id})

    table, read, accepted, rejected, flagged = fetch_all(
        conn,
        """SELECT table_name, rows_read, rows_accepted, rows_rejected, rows_flagged
           FROM audit.ingestion_log WHERE batch_id = %s""",
        (batch_id,),
    )[0]

    rules = fetch_all(
        conn,
        """SELECT rule_name, category, count(*) FROM audit.rejected_records
           WHERE batch_id = %s GROUP BY rule_name, category ORDER BY count(*) DESC""",
        (batch_id,),
    )
    rule_summary = ", ".join(f"{rule} ({category}): {n}" for rule, category, n in rules) or "none"

    log.info("%s: batch %s validated - read %s, accepted %s, rejected %s, flagged %s | rules hit: %s",
             table, batch_id, read, accepted, rejected, flagged, rule_summary)
    return {"rows_read": read, "rows_accepted": accepted,
            "rows_rejected": rejected, "rows_flagged": flagged}


def export_reports(conn, out_dir):
    """Write rejected records, a data-quality summary and the reconciliation to CSV."""
    export_query_to_csv(
        conn,
        """SELECT r.batch_id, l.file_name, r.table_name, r.row_num, r.category,
                  r.rule_name, r.reason, r.record::text AS record
           FROM audit.rejected_records r
           JOIN audit.ingestion_log l USING (batch_id)
           ORDER BY r.batch_id, r.row_num, r.rule_name""",
        out_dir / "rejected_records.csv",
    )
    export_query_to_csv(
        conn,
        """SELECT table_name, category, rule_name, count(*) AS records
           FROM audit.rejected_records
           GROUP BY table_name, category, rule_name
           ORDER BY table_name, category, rule_name""",
        out_dir / "dq_summary.csv",
    )
    export_query_to_csv(
        conn,
        "SELECT * FROM audit.batch_reconciliation",
        out_dir / "reconciliation.csv",
    )
    log.info("Exported rejected_records.csv, dq_summary.csv and reconciliation.csv to %s", out_dir)
