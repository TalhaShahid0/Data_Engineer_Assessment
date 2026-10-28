"""Build the analytics layer and export the metric tables.

All transformation logic lives in sql/transformations.sql; this module runs
it, checks the layer reconciliation and writes the CSV outputs.
run_transformations() does NOT commit: the caller owns the transaction.
"""
from src.pipeline.db import PROJECT_ROOT, export_query_to_csv, fetch_all, run_sql_file
from src.pipeline.logger import get_logger

log = get_logger("transform")

TRANSFORMATIONS_SQL = PROJECT_ROOT / "sql" / "transformations.sql"

METRIC_TABLES = {
    "monthly_store_revenue": "month, store_name",
    "customer_summary": "lifetime_value DESC, customer_id",
    "monthly_product_sales": "month, product_type, sku",
}


def run_transformations(conn):
    """Upsert dimensions/facts, rebuild the metrics and verify reconciliation."""
    run_sql_file(conn, TRANSFORMATIONS_SQL)

    counts = {
        table: fetch_all(conn, f"SELECT count(*) FROM analytics.{table}")[0][0]
        for table in ["dim_stores", "dim_customers", "dim_products",
                      "fct_orders", "fct_order_items", *METRIC_TABLES]
    }
    log.info("Analytics rebuilt: %s", ", ".join(f"{t}={n}" for t, n in counts.items()))

    failed = fetch_all(conn, "SELECT check_name, expected, actual FROM audit.layer_reconciliation "
                             "WHERE NOT passed")
    for check, expected, actual in failed:
        log.error("Reconciliation FAILED: %s (expected %s, actual %s)", check, expected, actual)
    if not failed:
        log.info("Layer reconciliation: all checks passed")
    return {"counts": counts, "reconciliation_failures": len(failed)}


def export_metrics(conn, out_dir):
    """Write each metric table and the layer reconciliation to CSV."""
    for table, order_by in METRIC_TABLES.items():
        export_query_to_csv(
            conn,
            f"SELECT * FROM analytics.{table} ORDER BY {order_by}",
            out_dir / "metrics" / f"{table}.csv",
        )
    export_query_to_csv(conn, "SELECT * FROM audit.layer_reconciliation",
                        out_dir / "layer_reconciliation.csv")
    log.info("Exported metric CSVs to %s", out_dir / "metrics")
