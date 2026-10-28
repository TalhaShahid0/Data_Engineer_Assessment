-- =====================================================================
-- schema.sql : full database schema for the Jaffle Shop pipeline
--
-- Layers:
--   audit     : pipeline runs, per-file ingestion log, rejected records
--   raw       : exact copy of every source file (all TEXT, append-only)
--   staging   : typed, cleaned, de-duplicated records (one row per key)
--   analytics : star schema (dimensions + facts) and metric tables
--
-- Re-running this file drops and recreates everything (used by --reset).
-- =====================================================================

DROP SCHEMA IF EXISTS analytics CASCADE;
DROP SCHEMA IF EXISTS staging   CASCADE;
DROP SCHEMA IF EXISTS raw       CASCADE;
DROP SCHEMA IF EXISTS audit     CASCADE;

CREATE SCHEMA audit;
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA analytics;


-- =====================================================================
-- HELPER FUNCTIONS (used by data_quality.sql)
-- "Safe casts": return NULL instead of raising an error, so a data-quality
-- rule can test whether a raw text value is a valid UUID / number / timestamp.
-- =====================================================================

CREATE FUNCTION staging.is_blank(v TEXT) RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE(TRIM(v), '') = '' $$;

CREATE FUNCTION staging.try_uuid(v TEXT) RETURNS UUID
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RETURN TRIM(v)::UUID;
EXCEPTION WHEN others THEN
    RETURN NULL;
END $$;

-- Whole numbers only (source amounts are integer cents).
CREATE FUNCTION staging.try_bigint(v TEXT) RETURNS BIGINT
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RETURN TRIM(v)::BIGINT;
EXCEPTION WHEN others THEN
    RETURN NULL;
END $$;

CREATE FUNCTION staging.try_numeric(v TEXT) RETURNS NUMERIC
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RETURN TRIM(v)::NUMERIC;
EXCEPTION WHEN others THEN
    RETURN NULL;
END $$;

-- Requires an ISO date at the start, so words like 'now' or 'today' are rejected.
CREATE FUNCTION staging.try_timestamp(v TEXT) RETURNS TIMESTAMP
LANGUAGE plpgsql STABLE AS $$
BEGIN
    IF TRIM(v) !~ '^\d{4}-\d{2}-\d{2}' THEN
        RETURN NULL;
    END IF;
    RETURN TRIM(v)::TIMESTAMP;
EXCEPTION WHEN others THEN
    RETURN NULL;
END $$;


-- =====================================================================
-- AUDIT
-- =====================================================================

-- One row per pipeline execution.
CREATE TABLE audit.pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    started_at      TIMESTAMP NOT NULL DEFAULT now(),
    finished_at     TIMESTAMP,
    status          TEXT NOT NULL DEFAULT 'RUNNING'
                    CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    files_processed INT NOT NULL DEFAULT 0,
    files_skipped   INT NOT NULL DEFAULT 0,
    files_failed    INT NOT NULL DEFAULT 0,
    error_message   TEXT
);

-- One row per source file per attempt (batch_id identifies the batch).
CREATE TABLE audit.ingestion_log (
    batch_id        SERIAL PRIMARY KEY,
    run_id          INT NOT NULL REFERENCES audit.pipeline_runs (run_id),
    table_name      TEXT NOT NULL,
    file_name       TEXT NOT NULL,
    file_hash       TEXT NOT NULL,              -- SHA-256 of the file content
    status          TEXT NOT NULL
                    CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'EMPTY', 'SKIPPED')),
    rows_read       INT NOT NULL DEFAULT 0,
    rows_accepted   INT NOT NULL DEFAULT 0,     -- loaded into staging
    rows_rejected   INT NOT NULL DEFAULT 0,     -- INVALID / INVESTIGATE, not loaded
    rows_flagged    INT NOT NULL DEFAULT 0,     -- EXCEPTION, loaded but flagged
    error_message   TEXT,
    started_at      TIMESTAMP NOT NULL DEFAULT now(),
    finished_at     TIMESTAMP
);

-- Checkpoint: the same file content can only be loaded successfully once.
CREATE UNIQUE INDEX ux_ingestion_log_success_hash
    ON audit.ingestion_log (file_hash) WHERE status = 'SUCCESS';

-- Every record that broke a data-quality rule, with the reason.
CREATE TABLE audit.rejected_records (
    reject_id       BIGSERIAL PRIMARY KEY,
    batch_id        INT  NOT NULL REFERENCES audit.ingestion_log (batch_id),
    table_name      TEXT NOT NULL,
    row_num         INT  NOT NULL,              -- line in the source file (1 = first data row)
    record          JSONB NOT NULL,             -- the raw row, for investigation
    rule_name       TEXT NOT NULL,
    reason          TEXT NOT NULL,
    category        TEXT NOT NULL
                    CHECK (category IN ('INVALID', 'INVESTIGATE', 'EXCEPTION')),
    rejected_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX ix_rejected_records_batch ON audit.rejected_records (batch_id);


-- =====================================================================
-- RAW  (all columns TEXT so that no source row is lost on load)
-- =====================================================================

CREATE TABLE raw.stores (
    id          TEXT,
    name        TEXT,
    opened_at   TEXT,
    tax_rate    TEXT,
    batch_id    INT  NOT NULL REFERENCES audit.ingestion_log (batch_id),
    source_file TEXT NOT NULL,
    row_num     INT  NOT NULL,
    loaded_at   TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, row_num)
);

CREATE TABLE raw.customers (
    id          TEXT,
    name        TEXT,
    batch_id    INT  NOT NULL REFERENCES audit.ingestion_log (batch_id),
    source_file TEXT NOT NULL,
    row_num     INT  NOT NULL,
    loaded_at   TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, row_num)
);

CREATE TABLE raw.products (
    sku         TEXT,
    name        TEXT,
    type        TEXT,
    price       TEXT,
    description TEXT,
    batch_id    INT  NOT NULL REFERENCES audit.ingestion_log (batch_id),
    source_file TEXT NOT NULL,
    row_num     INT  NOT NULL,
    loaded_at   TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, row_num)
);

CREATE TABLE raw.supplies (
    id          TEXT,
    name        TEXT,
    cost        TEXT,
    perishable  TEXT,
    sku         TEXT,
    batch_id    INT  NOT NULL REFERENCES audit.ingestion_log (batch_id),
    source_file TEXT NOT NULL,
    row_num     INT  NOT NULL,
    loaded_at   TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, row_num)
);

CREATE TABLE raw.orders (
    id          TEXT,
    customer    TEXT,
    ordered_at  TEXT,
    store_id    TEXT,
    subtotal    TEXT,
    tax_paid    TEXT,
    order_total TEXT,
    batch_id    INT  NOT NULL REFERENCES audit.ingestion_log (batch_id),
    source_file TEXT NOT NULL,
    row_num     INT  NOT NULL,
    loaded_at   TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, row_num)
);

CREATE TABLE raw.items (
    id          TEXT,
    order_id    TEXT,
    sku         TEXT,
    batch_id    INT  NOT NULL REFERENCES audit.ingestion_log (batch_id),
    source_file TEXT NOT NULL,
    row_num     INT  NOT NULL,
    loaded_at   TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, row_num)
);


-- =====================================================================
-- STAGING  (typed and cleaned; amounts converted from cents to dollars)
-- One row per business key; reloads update the row (ON CONFLICT).
-- =====================================================================

CREATE TABLE staging.stores (
    store_id    UUID PRIMARY KEY,
    store_name  TEXT NOT NULL,
    opened_at   TIMESTAMP NOT NULL,
    tax_rate    NUMERIC(5, 4) NOT NULL CHECK (tax_rate >= 0 AND tax_rate < 1),
    batch_id    INT NOT NULL,
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE staging.customers (
    customer_id   UUID PRIMARY KEY,
    customer_name TEXT NOT NULL,
    batch_id      INT NOT NULL,
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE staging.products (
    sku          TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    product_type TEXT NOT NULL,
    price        NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    description  TEXT,
    batch_id     INT NOT NULL,
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- Source "id" is not unique on its own: one supply can be used by several products.
CREATE TABLE staging.supplies (
    supply_id     TEXT NOT NULL,
    sku           TEXT NOT NULL,
    supply_name   TEXT NOT NULL,
    cost          NUMERIC(10, 2) NOT NULL CHECK (cost >= 0),
    is_perishable BOOLEAN NOT NULL,
    batch_id      INT NOT NULL,
    updated_at    TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (supply_id, sku)
);

CREATE TABLE staging.orders (
    order_id    UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    store_id    UUID NOT NULL,
    ordered_at  TIMESTAMP NOT NULL,
    subtotal    NUMERIC(10, 2) NOT NULL CHECK (subtotal >= 0),
    tax_paid    NUMERIC(10, 2) NOT NULL CHECK (tax_paid >= 0),
    order_total NUMERIC(10, 2) NOT NULL CHECK (order_total >= 0),
    batch_id    INT NOT NULL,
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE staging.items (
    item_id    UUID PRIMARY KEY,
    order_id   UUID NOT NULL,
    sku        TEXT NOT NULL,
    batch_id   INT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);


-- =====================================================================
-- ANALYTICS  (star schema: dimensions + facts, with enforced FKs)
-- =====================================================================

CREATE TABLE analytics.dim_stores (
    store_id   UUID PRIMARY KEY,
    store_name TEXT NOT NULL,
    opened_at  TIMESTAMP NOT NULL,
    tax_rate   NUMERIC(5, 4) NOT NULL
);

CREATE TABLE analytics.dim_customers (
    customer_id   UUID PRIMARY KEY,
    customer_name TEXT NOT NULL
);

CREATE TABLE analytics.dim_products (
    sku          TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    product_type TEXT NOT NULL,
    price        NUMERIC(10, 2) NOT NULL,
    unit_cost    NUMERIC(10, 2) NOT NULL DEFAULT 0,   -- sum of supply costs for the product
    description  TEXT
);

-- Grain: one row per order.
CREATE TABLE analytics.fct_orders (
    order_id    UUID PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES analytics.dim_customers (customer_id),
    store_id    UUID NOT NULL REFERENCES analytics.dim_stores (store_id),
    ordered_at  TIMESTAMP NOT NULL,
    order_date  DATE NOT NULL,
    subtotal    NUMERIC(10, 2) NOT NULL,
    tax_paid    NUMERIC(10, 2) NOT NULL,
    order_total NUMERIC(10, 2) NOT NULL,
    item_count  INT NOT NULL DEFAULT 0          -- 0 = empty order (excluded from order metrics)
);

CREATE INDEX ix_fct_orders_customer ON analytics.fct_orders (customer_id);
CREATE INDEX ix_fct_orders_store    ON analytics.fct_orders (store_id);
CREATE INDEX ix_fct_orders_date     ON analytics.fct_orders (order_date);

-- Grain: one row per item sold (each source item row = one unit).
CREATE TABLE analytics.fct_order_items (
    item_id    UUID PRIMARY KEY,
    order_id   UUID NOT NULL REFERENCES analytics.fct_orders (order_id),
    sku        TEXT NOT NULL REFERENCES analytics.dim_products (sku),
    unit_price NUMERIC(10, 2) NOT NULL,
    unit_cost  NUMERIC(10, 2) NOT NULL
);

CREATE INDEX ix_fct_order_items_order ON analytics.fct_order_items (order_id);
CREATE INDEX ix_fct_order_items_sku   ON analytics.fct_order_items (sku);


-- =====================================================================
-- ANALYTICS : metric tables (fully rebuilt on every run)
-- =====================================================================

-- Metric 1: monthly revenue, order count and average order value per store.
CREATE TABLE analytics.monthly_store_revenue (
    month           DATE NOT NULL,
    store_id        UUID NOT NULL,
    store_name      TEXT NOT NULL,
    order_count     INT NOT NULL,
    revenue         NUMERIC(12, 2) NOT NULL,
    avg_order_value NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (month, store_id)
);

-- Metric 2: customer lifetime value and repeat behaviour.
CREATE TABLE analytics.customer_summary (
    customer_id        UUID PRIMARY KEY,
    customer_name      TEXT NOT NULL,
    first_order_date   DATE NOT NULL,
    last_order_date    DATE NOT NULL,
    order_count        INT NOT NULL,
    lifetime_value     NUMERIC(12, 2) NOT NULL,
    is_repeat_customer BOOLEAN NOT NULL
);

-- Metric 3: monthly units, revenue and gross margin per product.
CREATE TABLE analytics.monthly_product_sales (
    month        DATE NOT NULL,
    sku          TEXT NOT NULL,
    product_name TEXT NOT NULL,
    product_type TEXT NOT NULL,
    units_sold   INT NOT NULL,
    revenue      NUMERIC(12, 2) NOT NULL,
    total_cost   NUMERIC(12, 2) NOT NULL,
    gross_margin NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (month, sku)
);


-- =====================================================================
-- RECONCILIATION (one row per successfully loaded batch)
--   rows_read  = rows in the source file
--   counts_balance : rows_read = rows_accepted + rows_rejected
--   raw_complete   : every source row is in raw, or was rejected as malformed
-- =====================================================================

CREATE VIEW audit.batch_reconciliation AS
WITH raw_counts AS (
    SELECT batch_id, count(*) AS n FROM raw.stores    GROUP BY batch_id UNION ALL
    SELECT batch_id, count(*)      FROM raw.customers GROUP BY batch_id UNION ALL
    SELECT batch_id, count(*)      FROM raw.products  GROUP BY batch_id UNION ALL
    SELECT batch_id, count(*)      FROM raw.supplies  GROUP BY batch_id UNION ALL
    SELECT batch_id, count(*)      FROM raw.orders    GROUP BY batch_id UNION ALL
    SELECT batch_id, count(*)      FROM raw.items     GROUP BY batch_id
),
malformed AS (
    SELECT batch_id, count(*) AS n
    FROM audit.rejected_records
    WHERE rule_name = 'malformed_row'
    GROUP BY batch_id
)
SELECT l.batch_id,
       l.table_name,
       l.file_name,
       l.rows_read,
       COALESCE(r.n, 0)  AS rows_in_raw,
       COALESCE(m.n, 0)  AS rows_malformed,
       l.rows_accepted,
       l.rows_rejected,
       l.rows_flagged,
       l.rows_read = l.rows_accepted + l.rows_rejected        AS counts_balance,
       l.rows_read = COALESCE(r.n, 0) + COALESCE(m.n, 0)      AS raw_complete
FROM audit.ingestion_log l
LEFT JOIN raw_counts r ON r.batch_id = l.batch_id
LEFT JOIN malformed  m ON m.batch_id = l.batch_id
WHERE l.status = 'SUCCESS'
ORDER BY l.batch_id;


-- =====================================================================
-- RECONCILIATION across layers (staging -> facts -> metrics)
-- Every check must have passed = true after a successful run.
-- =====================================================================

CREATE VIEW audit.layer_reconciliation AS
SELECT check_name, expected, actual, expected = actual AS passed
FROM (
SELECT 'orders: staging count = fct_orders count' AS check_name,
       (SELECT count(*) FROM staging.orders)::NUMERIC AS expected,
       (SELECT count(*) FROM analytics.fct_orders)::NUMERIC AS actual
UNION ALL
SELECT 'orders: staging order_total = fct_orders order_total',
       (SELECT COALESCE(sum(order_total), 0) FROM staging.orders),
       (SELECT COALESCE(sum(order_total), 0) FROM analytics.fct_orders)
UNION ALL
SELECT 'items: staging count = fct_order_items count',
       (SELECT count(*) FROM staging.items),
       (SELECT count(*) FROM analytics.fct_order_items)
UNION ALL
SELECT 'orders: subtotal = sum of item prices (mismatched orders)',
       0,
       (SELECT count(*) FROM analytics.fct_orders o
        WHERE o.item_count > 0
          AND o.subtotal <> (SELECT sum(i.unit_price) FROM analytics.fct_order_items i
                             WHERE i.order_id = o.order_id))
UNION ALL
SELECT 'metric 1: non-empty orders = sum(order_count)',
       (SELECT count(*) FROM analytics.fct_orders WHERE item_count > 0),
       (SELECT COALESCE(sum(order_count), 0) FROM analytics.monthly_store_revenue)
UNION ALL
SELECT 'metric 1: non-empty orders revenue = sum(revenue)',
       (SELECT COALESCE(sum(order_total), 0) FROM analytics.fct_orders WHERE item_count > 0),
       (SELECT COALESCE(sum(revenue), 0) FROM analytics.monthly_store_revenue)
UNION ALL
SELECT 'metric 2: non-empty orders revenue = sum(lifetime_value)',
       (SELECT COALESCE(sum(order_total), 0) FROM analytics.fct_orders WHERE item_count > 0),
       (SELECT COALESCE(sum(lifetime_value), 0) FROM analytics.customer_summary)
UNION ALL
SELECT 'metric 3: fct_order_items count = sum(units_sold)',
       (SELECT count(*) FROM analytics.fct_order_items),
       (SELECT COALESCE(sum(units_sold), 0) FROM analytics.monthly_product_sales)
) AS checks;
