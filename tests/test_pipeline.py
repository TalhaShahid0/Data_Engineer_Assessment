"""Automated checks for the Jaffle Shop pipeline.

Every test runs the real pipeline (ingest -> SQL data quality -> transformations)
against the jaffle_test database, using the small dataset in tests/fixtures/good:

    1 store (tax 6%), 2 customers, 2 products, 3 supply rows
    orders_2017_01.csv : Alice  order 1 = $18.02 (jaffle + drink), order 2 = $11.66 (jaffle)
    orders_2017_02.csv : Bob    order 3 = $6.36  (drink)
"""
from decimal import Decimal

import pytest

from conftest import query, scalar

STORE = "aaaaaaaa-0000-0000-0000-000000000001"
ALICE = "cccccccc-0000-0000-0000-000000000001"
BOB = "cccccccc-0000-0000-0000-000000000002"
ORDER_HEADER = "id,customer,ordered_at,store_id,subtotal,tax_paid,order_total\n"


def table_snapshot(db, table):
    """A fingerprint of a table's full content, to compare before/after."""
    return scalar(db, f"SELECT md5(COALESCE(string_agg(t::text, '|' ORDER BY t::text), '')) FROM {table} t")


# ---------------------------------------------------------------------------
# 1. Schema compliance - deliberately invalid input
# ---------------------------------------------------------------------------
def test_file_missing_required_column_is_rejected(run, data_dir, db):
    """A file without a required column is FAILED, nothing from it is loaded,
    and the other files are still processed."""
    (data_dir / "orders_2017_03.csv").write_text(
        "id,customer,ordered_at,store_id,subtotal,tax_paid\n"            # no order_total
        f"dddddddd-0000-0000-0000-000000000009,{ALICE},2017-03-01T10:00:00,{STORE},1100,66\n"
    )

    result = run(data_dir, reset=True)

    assert result["status"] == "FAILED"
    assert result["files"]["FAILED"] == 1
    status, error = query(db, "SELECT status, error_message FROM audit.ingestion_log "
                              "WHERE file_name = 'orders_2017_03.csv'")[0]
    assert status == "FAILED"
    assert "order_total" in error
    assert scalar(db, "SELECT count(*) FROM raw.orders WHERE source_file = 'orders_2017_03.csv'") == 0
    assert scalar(db, "SELECT count(*) FROM analytics.fct_orders") == 3   # good files still loaded


# ---------------------------------------------------------------------------
# 2. Expected behaviour for invalid records
# ---------------------------------------------------------------------------
def test_invalid_rows_are_rejected_with_rule_and_category(run, data_dir, db):
    """Each bad row is caught by the right rule; only valid and EXCEPTION rows load."""
    (data_dir / "orders_2017_03.csv").write_text(
        ORDER_HEADER
        + f"dddddddd-0000-0000-0000-000000000010,,2017-03-01T10:00:00,{STORE},600,36,636\n"           # row 1
        + f"dddddddd-0000-0000-0000-000000000011,{ALICE},31-03-2017,{STORE},600,36,636\n"             # row 2
        + f"dddddddd-0000-0000-0000-000000000012,{ALICE},2017-03-01T10:00:00,{STORE},-600,0,-600\n"   # row 3
        + f"dddddddd-0000-0000-0000-000000000013,cccccccc-0000-0000-0000-000000000099,"
          f"2017-03-01T10:00:00,{STORE},600,36,636\n"                                                  # row 4
        + f"dddddddd-0000-0000-0000-000000000014,{ALICE},2017-03-01T10:00:00,{STORE},600,36,637\n"    # row 5
        + f"dddddddd-0000-0000-0000-000000000014,{ALICE},2017-03-01T10:00:00,{STORE},600,36,637\n"    # row 6
    )

    run(data_dir, reset=True)

    rejected = {(row_num, rule, category) for row_num, rule, category in query(
        db, "SELECT r.row_num, r.rule_name, r.category FROM audit.rejected_records r "
            "JOIN audit.ingestion_log l USING (batch_id) WHERE l.file_name = 'orders_2017_03.csv'")}
    assert rejected == {
        (1, "missing_required", "INVALID"),
        (2, "invalid_timestamp", "INVALID"),
        (3, "negative_amount", "INVALID"),
        (4, "unknown_customer", "INVESTIGATE"),
        (5, "total_rounding", "EXCEPTION"),
        (6, "total_rounding", "EXCEPTION"),
        (6, "duplicate_key", "INVALID"),
    }
    read, accepted, rejected_rows, flagged = query(
        db, "SELECT rows_read, rows_accepted, rows_rejected, rows_flagged FROM audit.ingestion_log "
            "WHERE file_name = 'orders_2017_03.csv'")[0]
    assert (read, accepted, rejected_rows, flagged) == (6, 1, 5, 1)
    # Only the 1-cent rounding EXCEPTION order was loaded, once.
    assert scalar(db, "SELECT count(*) FROM staging.orders WHERE batch_id = "
                      "(SELECT batch_id FROM audit.ingestion_log WHERE file_name = 'orders_2017_03.csv')") == 1


# ---------------------------------------------------------------------------
# 3. Business-key uniqueness - duplicates and corrections
# ---------------------------------------------------------------------------
def test_corrected_record_updates_instead_of_duplicating(run, data_dir, db):
    """The same order arriving again in a later file with a corrected value
    replaces the old version: one row per key, latest value wins."""
    run(data_dir, reset=True)
    (data_dir / "orders_2017_03_correction.csv").write_text(      # order 3: time corrected
        ORDER_HEADER
        + f"dddddddd-0000-0000-0000-000000000003,{BOB},2017-02-04T11:00:00,{STORE},600,36,636\n"
    )

    run(data_dir)

    for table, key in [("staging.orders", "order_id"), ("analytics.fct_orders", "order_id"),
                       ("analytics.fct_order_items", "item_id"), ("analytics.dim_customers", "customer_id"),
                       ("analytics.dim_products", "sku")]:
        duplicates = scalar(db, f"SELECT count(*) FROM (SELECT {key} FROM {table} "
                                f"GROUP BY {key} HAVING count(*) > 1) d")
        assert duplicates == 0, f"duplicate keys in {table}"
    assert scalar(db, "SELECT count(*) FROM analytics.fct_orders") == 3
    assert str(scalar(db, "SELECT ordered_at FROM analytics.fct_orders "
                          "WHERE order_id = 'dddddddd-0000-0000-0000-000000000003'")) == "2017-02-04 11:00:00"
    assert scalar(db, "SELECT last_order_date::text FROM analytics.customer_summary "
                      "WHERE customer_name = 'Bob Jones'") == "2017-02-04"


# ---------------------------------------------------------------------------
# 4. Referential integrity
# ---------------------------------------------------------------------------
def test_no_orphan_records_reach_the_facts(run, data_dir, db):
    """Items pointing to an unknown order or product are held for investigation,
    and every fact row has its parent rows."""
    (data_dir / "items_2017_03.csv").write_text(
        "id,order_id,sku\n"
        "eeeeeeee-0000-0000-0000-000000000010,dddddddd-0000-0000-0000-000000000099,JAF-001\n"
        "eeeeeeee-0000-0000-0000-000000000011,dddddddd-0000-0000-0000-000000000001,XXX-999\n"
    )

    run(data_dir, reset=True)

    assert {rule for (rule,) in query(db, "SELECT rule_name FROM audit.rejected_records "
                                          "WHERE category = 'INVESTIGATE'")} == {"unknown_order", "unknown_product"}
    orphans = {
        "orders without customer": "SELECT count(*) FROM analytics.fct_orders o "
                                   "LEFT JOIN analytics.dim_customers c USING (customer_id) WHERE c.customer_id IS NULL",
        "orders without store": "SELECT count(*) FROM analytics.fct_orders o "
                                "LEFT JOIN analytics.dim_stores s USING (store_id) WHERE s.store_id IS NULL",
        "items without order": "SELECT count(*) FROM analytics.fct_order_items i "
                               "LEFT JOIN analytics.fct_orders o USING (order_id) WHERE o.order_id IS NULL",
        "items without product": "SELECT count(*) FROM analytics.fct_order_items i "
                                 "LEFT JOIN analytics.dim_products p USING (sku) WHERE p.sku IS NULL",
    }
    for name, sql in orphans.items():
        assert scalar(db, sql) == 0, name
    assert scalar(db, "SELECT count(*) FROM analytics.fct_order_items") == 4


# ---------------------------------------------------------------------------
# 5. Record-count and aggregate reconciliation
# ---------------------------------------------------------------------------
def test_reconciliation_balances_with_rejected_rows(run, data_dir, db):
    """Every source row is accounted for (accepted + rejected), and totals match
    from staging through facts to metrics, even when some rows are rejected."""
    (data_dir / "orders_2017_03.csv").write_text(
        ORDER_HEADER
        + f"dddddddd-0000-0000-0000-000000000020,{ALICE},2017-03-01T10:00:00,{STORE},abc,36,636\n"
        + "too,few,fields\n"
    )

    run(data_dir, reset=True)

    batches = query(db, "SELECT file_name, counts_balance, raw_complete FROM audit.batch_reconciliation")
    assert len(batches) == 9          # 8 good files + the file with bad rows
    assert all(balance and complete for _, balance, complete in batches), batches
    failed_checks = query(db, "SELECT check_name FROM audit.layer_reconciliation WHERE NOT passed")
    assert failed_checks == []


# ---------------------------------------------------------------------------
# 6. Transformation correctness (values calculated by hand)
# ---------------------------------------------------------------------------
def test_metrics_match_hand_calculated_values(run, data_dir, db):
    run(data_dir, reset=True)

    # Metric 1: Jan = 18.02 + 11.66 over 2 orders; Feb = 6.36 over 1 order.
    assert query(db, "SELECT month::text, order_count, revenue, avg_order_value "
                     "FROM analytics.monthly_store_revenue ORDER BY month") == [
        ("2017-01-01", 2, Decimal("29.68"), Decimal("14.84")),
        ("2017-02-01", 1, Decimal("6.36"), Decimal("6.36")),
    ]

    # Metric 2: Alice ordered twice (repeat), Bob once.
    assert query(db, "SELECT customer_name, order_count, lifetime_value, is_repeat_customer, "
                     "first_order_date::text, last_order_date::text "
                     "FROM analytics.customer_summary ORDER BY customer_name") == [
        ("Alice Smith", 2, Decimal("29.68"), True, "2017-01-05", "2017-01-20"),
        ("Bob Jones", 1, Decimal("6.36"), False, "2017-02-03", "2017-02-03"),
    ]

    # Metric 3: jaffle cost = 0.07 + 3.00 = 3.07, drink cost = 0.07 (SUP-001 is shared).
    assert query(db, "SELECT month::text, sku, units_sold, revenue, total_cost, gross_margin "
                     "FROM analytics.monthly_product_sales ORDER BY month, sku") == [
        ("2017-01-01", "BEV-001", 1, Decimal("6.00"), Decimal("0.07"), Decimal("5.93")),
        ("2017-01-01", "JAF-001", 2, Decimal("22.00"), Decimal("6.14"), Decimal("15.86")),
        ("2017-02-01", "BEV-001", 1, Decimal("6.00"), Decimal("0.07"), Decimal("5.93")),
    ]


# ---------------------------------------------------------------------------
# 7. Idempotency after repeated execution
# ---------------------------------------------------------------------------
def test_rerun_skips_loaded_files_and_changes_nothing(run, data_dir, db):
    run(data_dir, reset=True)
    tables = ["staging.orders", "staging.items", "analytics.fct_orders", "analytics.fct_order_items",
              "analytics.monthly_store_revenue", "analytics.customer_summary",
              "analytics.monthly_product_sales"]
    before = {t: table_snapshot(db, t) for t in tables}

    second = run(data_dir)

    assert second["status"] == "SUCCESS"
    assert second["files"]["SKIPPED"] == 8 and second["files"]["SUCCESS"] == 0
    assert {t: table_snapshot(db, t) for t in tables} == before


# ---------------------------------------------------------------------------
# 8. Failure detection and recovery
# ---------------------------------------------------------------------------
def test_failed_file_is_rolled_back_and_recovered_on_next_run(run, data_dir, db):
    first = run(data_dir, reset=True, simulate_failure="orders_2017_02.csv")

    # Detected and rolled back: nothing from the failed file, its item is held.
    assert first["status"] == "FAILED"
    assert scalar(db, "SELECT count(*) FROM raw.orders WHERE source_file = 'orders_2017_02.csv'") == 0
    assert scalar(db, "SELECT count(*) FROM analytics.fct_orders") == 2
    assert query(db, "SELECT table_name, rule_name FROM audit.rejected_records") == [("items", "unknown_order")]

    second = run(data_dir)

    # Recovered: the failed file is retried, the held item is loaded, totals are complete.
    assert second["status"] == "SUCCESS"
    assert second["files"]["SUCCESS"] == 1
    assert scalar(db, "SELECT count(*) FROM analytics.fct_orders") == 3
    assert scalar(db, "SELECT count(*) FROM analytics.fct_order_items") == 4
    assert scalar(db, "SELECT count(*) FROM audit.rejected_records") == 0
    assert scalar(db, "SELECT sum(revenue) FROM analytics.monthly_store_revenue") == Decimal("36.04")


# ---------------------------------------------------------------------------
# 9. Empty input
# ---------------------------------------------------------------------------
def test_empty_file_is_logged_and_does_not_fail_the_run(run, data_dir, db):
    (data_dir / "orders_2017_03.csv").write_text("")

    result = run(data_dir, reset=True)

    assert result["status"] == "SUCCESS"
    assert scalar(db, "SELECT status FROM audit.ingestion_log WHERE file_name = 'orders_2017_03.csv'") == "EMPTY"
    assert scalar(db, "SELECT count(*) FROM analytics.fct_orders") == 3
    assert run(data_dir)["files"]["SKIPPED"] == 9        # the empty file is not re-processed


# ---------------------------------------------------------------------------
# Regression tests (found by code review and stress testing)
# ---------------------------------------------------------------------------
def order_row(order_id, ordered_at="2017-03-01T10:00:00", subtotal="600", tax="36", total="636",
              customer=ALICE):
    return f"{order_id},{customer},{ordered_at},{STORE},{subtotal},{tax},{total}\n"


def test_correction_is_not_undone_by_recheck_of_older_batch(run, data_dir, db):
    """A batch with a held row is re-checked on every run; that re-check must not
    overwrite a newer correction of another row from the same batch."""
    (data_dir / "orders_2017_03.csv").write_text(
        ORDER_HEADER
        + order_row("dddddddd-0000-0000-0000-000000000070")
        + order_row("dddddddd-0000-0000-0000-000000000071",
                    customer="cccccccc-0000-0000-0000-000000000099")       # held: unknown customer
    )
    run(data_dir, reset=True)
    (data_dir / "orders_2017_04.csv").write_text(                          # correction of order 70
        ORDER_HEADER + order_row("dddddddd-0000-0000-0000-000000000070", ordered_at="2017-03-09T09:09:00")
    )
    run(data_dir)
    run(data_dir)                                                          # re-check again, nothing new

    assert str(scalar(db, "SELECT ordered_at FROM analytics.fct_orders "
                          "WHERE order_id = 'dddddddd-0000-0000-0000-000000000070'")) == "2017-03-09 09:09:00"


def test_same_uuid_in_different_spelling_is_a_duplicate(run, data_dir, db):
    """'{uuid}' and 'UUID' are the same key: rejected as a duplicate, not a crash."""
    key = "dddddddd-0000-0000-0000-000000000080"
    (data_dir / "orders_2017_03.csv").write_text(
        ORDER_HEADER + order_row(key) + order_row("{" + key + "}") + order_row(key.upper())
    )

    result = run(data_dir, reset=True)

    assert result["status"] == "SUCCESS"
    assert query(db, "SELECT row_num, rule_name FROM audit.rejected_records ORDER BY row_num") == [
        (2, "duplicate_key"), (3, "duplicate_key")]
    assert scalar(db, f"SELECT count(*) FROM analytics.fct_orders WHERE order_id = '{key}'") == 1


def test_out_of_range_values_are_rejected_without_failing_the_file(run, data_dir, db):
    """Values that would overflow a column are rejected row by row; the file still loads."""
    (data_dir / "orders_2017_03.csv").write_text(
        ORDER_HEADER
        + order_row("dddddddd-0000-0000-0000-000000000090", subtotal="10000000000", tax="0",
                    total="10000000000")                                              # too large for NUMERIC(10,2)
        + order_row("dddddddd-0000-0000-0000-000000000091", subtotal="9000000000000000000",
                    tax="9000000000000000000", total="1")                             # would overflow BIGINT
        + order_row("dddddddd-0000-0000-0000-000000000092", subtotal="0x10", tax="0", total="16")
        + order_row("dddddddd-0000-0000-0000-000000000093")                           # valid
    )
    (data_dir / "raw_products.csv").write_text(
        (data_dir / "raw_products.csv").read_text() + f"{'X' * 60},long sku,jaffle,100,d\n"
    )

    result = run(data_dir, reset=True)

    assert result["files"]["FAILED"] == 0
    assert {(table, row_num, rule) for table, row_num, rule in query(
        db, "SELECT table_name, row_num, rule_name FROM audit.rejected_records "
            "WHERE category = 'INVALID'")} == {
        ("orders", 1, "amount_out_of_range"),
        ("orders", 2, "amount_out_of_range"),
        ("orders", 3, "invalid_amount"),
        ("products", 3, "invalid_key"),
    }
    assert scalar(db, "SELECT count(*) FROM staging.orders "
                      "WHERE order_id = 'dddddddd-0000-0000-0000-000000000093'") == 1


def test_blank_lines_are_ignored(run, data_dir, db):
    """Blank lines before the header or after the data do not lose or reject rows."""
    (data_dir / "orders_2017_03.csv").write_text(
        "\n\n" + ORDER_HEADER + order_row("dddddddd-0000-0000-0000-000000000100") + "\n\n"
    )

    run(data_dir, reset=True)

    assert query(db, "SELECT status, rows_read, rows_accepted, rows_rejected FROM audit.ingestion_log "
                     "WHERE file_name = 'orders_2017_03.csv'") == [("SUCCESS", 1, 1, 0)]


def test_duplicate_header_column_fails_the_file(run, data_dir, db):
    (data_dir / "orders_2017_03.csv").write_text(
        ORDER_HEADER.strip() + ",id\n" + order_row("dddddddd-0000-0000-0000-000000000101").strip() + ",x\n"
    )

    run(data_dir, reset=True)

    status, error = query(db, "SELECT status, error_message FROM audit.ingestion_log "
                              "WHERE file_name = 'orders_2017_03.csv'")[0]
    assert status == "FAILED" and "duplicate column" in error


def test_unreadable_file_fails_only_that_file(run, data_dir, db):
    """A file that cannot be opened is logged as FAILED; the other files still load."""
    (data_dir / "orders_2017_03.csv").mkdir()          # matches the pattern but cannot be read

    result = run(data_dir, reset=True)

    assert result["files"] == {"SUCCESS": 8, "SKIPPED": 0, "FAILED": 1, "EMPTY": 0}
    assert scalar(db, "SELECT status FROM audit.ingestion_log WHERE file_name = 'orders_2017_03.csv'") == "FAILED"
    assert scalar(db, "SELECT count(*) FROM analytics.fct_orders") == 3


def test_second_run_is_refused_while_one_is_in_progress(run, data_dir, db):
    """Two runs at the same time are not allowed (advisory lock)."""
    run(data_dir, reset=True)
    scalar(db, "SELECT pg_advisory_lock(hashtext('jaffle_pipeline'))")   # pretend a run is active
    try:
        result = run(data_dir)
    finally:
        scalar(db, "SELECT pg_advisory_unlock(hashtext('jaffle_pipeline'))")

    assert result["status"] == "FAILED" and result["run_id"] is None
    assert scalar(db, "SELECT count(*) FROM audit.pipeline_runs") == 1    # nothing was started


def test_interrupted_run_is_recorded_as_failed(run, data_dir, db, monkeypatch):
    """Ctrl+C in the middle of a run: the run is recorded as FAILED and the lock is released."""
    import src.pipeline.run_pipeline as pipeline

    def interrupt(conn):
        raise KeyboardInterrupt

    monkeypatch.setattr(pipeline, "run_transformations", interrupt)
    with pytest.raises(KeyboardInterrupt):
        run(data_dir, reset=True)
    monkeypatch.undo()

    assert query(db, "SELECT status, error_message FROM audit.pipeline_runs") == [("FAILED", "run interrupted")]
    assert run(data_dir)["status"] == "SUCCESS"                          # lock was released
