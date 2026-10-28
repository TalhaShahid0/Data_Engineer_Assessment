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
    order_total NUMERIC(10, 2) NOT NULL
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
