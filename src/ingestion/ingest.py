"""Load one source CSV file into its raw.* table.

Steps for one file:
  1. Hash the file (SHA-256) - used as the checkpoint to skip files already loaded.
  2. Check the header against the expected schema in config/config.yaml:
       - missing required column -> SchemaError (file FAILED, nothing loaded)
       - unexpected extra column -> warning, column ignored
       - empty file / header only -> status EMPTY
  3. Create the batch in audit.ingestion_log (status RUNNING).
  4. COPY every well-formed row into raw.<table> with batch_id, source_file, row_num.
     Rows with the wrong number of fields are written to audit.rejected_records
     (rule malformed_row) instead.

ingest_file() does NOT commit: the caller owns the transaction, so a failure in
any later step (validation, transformation) rolls the raw load back too.
"""
import csv
import hashlib
import io
import json

from psycopg2 import sql

from src.pipeline.logger import get_logger

log = get_logger("ingest")


class SchemaError(Exception):
    """The file does not match the expected schema."""


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def already_loaded(conn, file_hash):
    """True if a file with exactly this content was already loaded successfully."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM audit.ingestion_log WHERE file_hash = %s AND status = 'SUCCESS'",
            (file_hash,),
        )
        return cur.fetchone() is not None


def log_batch(conn, run_id, table_name, file_name, file_hash, status, error_message=None):
    """Insert a batch row in audit.ingestion_log and return its batch_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.ingestion_log
                (run_id, table_name, file_name, file_hash, status, error_message,
                 finished_at)
            VALUES (%s, %s, %s, %s, %s, %s,
                    CASE WHEN %s = 'RUNNING' THEN NULL ELSE now() END)
            RETURNING batch_id
            """,
            (run_id, table_name, file_name, file_hash, status, error_message, status),
        )
        return cur.fetchone()[0]


def check_header(header, table):
    """Validate the file header; return the expected columns present in the file."""
    missing = [col for col in table["required"] if col not in header]
    if missing:
        raise SchemaError(f"{table['name']}: missing required column(s) {missing}; found {header}")

    extra = [col for col in header if col not in table["columns"]]
    if extra:
        log.warning("%s: unexpected column(s) %s will be ignored", table["name"], extra)

    return [col for col in table["columns"] if col in header]


def ingest_file(conn, run_id, table, path):
    """Load one file into raw.<table>. Returns a dict with batch_id, status, rows_read."""
    file_hash = file_sha256(path)

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = [col.strip().lower() for col in next(reader, [])]
        rows = list(reader)

    if not header or not rows:
        batch_id = log_batch(conn, run_id, table["name"], path.name, file_hash, "EMPTY",
                             "file is empty" if not header else "file has a header but no rows")
        log.warning("%s: %s is empty, nothing to load", table["name"], path.name)
        return {"batch_id": batch_id, "status": "EMPTY", "rows_read": 0}

    columns = check_header(header, table)
    batch_id = log_batch(conn, run_id, table["name"], path.name, file_hash, "RUNNING")

    # Build an in-memory CSV for COPY; set malformed rows aside.
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    positions = [header.index(col) for col in columns]
    malformed = []
    for row_num, row in enumerate(rows, start=1):
        if len(row) != len(header):
            malformed.append((row_num, row))
            continue
        writer.writerow([row[i] for i in positions] + [batch_id, path.name, row_num])
    buffer.seek(0)

    with conn.cursor() as cur:
        copy_sql = sql.SQL("COPY raw.{table} ({cols}) FROM STDIN WITH (FORMAT csv)").format(
            table=sql.Identifier(table["name"]),
            cols=sql.SQL(", ").join(
                sql.Identifier(c) for c in columns + ["batch_id", "source_file", "row_num"]
            ),
        )
        cur.copy_expert(copy_sql, buffer)

        for row_num, row in malformed:
            cur.execute(
                """
                INSERT INTO audit.rejected_records
                    (batch_id, table_name, row_num, record, rule_name, reason, category)
                VALUES (%s, %s, %s, %s, 'malformed_row', %s, 'INVALID')
                """,
                (batch_id, table["name"], row_num, json.dumps({"fields": row}),
                 f"expected {len(header)} fields, found {len(row)}"),
            )

        cur.execute(
            "UPDATE audit.ingestion_log SET rows_read = %s WHERE batch_id = %s",
            (len(rows), batch_id),
        )

    log.info("%s: loaded %s into raw (batch %s, %s rows read, %s malformed)",
             table["name"], path.name, batch_id, len(rows), len(malformed))
    return {"batch_id": batch_id, "status": "RUNNING", "rows_read": len(rows)}
