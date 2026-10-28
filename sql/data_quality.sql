-- =====================================================================
-- data_quality.sql : validate one batch and load its clean rows to staging
--
-- Run once per batch with parameter batch_id. Only the raw table that holds
-- that batch has rows, so the sections for the other tables do nothing.
--
-- For every table:
--   1. RULES   : each failed rule writes one row to audit.rejected_records
--                  INVALID     -> broken data, row is NOT loaded
--                  INVESTIGATE -> may be valid but needs a person (for example
--                                 an unknown customer that may arrive late); NOT loaded
--                  EXCEPTION   -> legitimate but unusual business case; loaded and flagged
--   2. STAGING : rows without INVALID/INVESTIGATE rejections are typed, cleaned
--                and upserted (ON CONFLICT) so reprocessing never duplicates.
--                Unchanged rows are left untouched (IS DISTINCT FROM).
--
-- Cleaning applied: TRIM, repeated spaces collapsed in names, ids lower-case
-- UUIDs, SKUs upper-case, product types lower-case, cents converted to dollars.
--
-- Note: this file is run with parameters, so a literal percent sign would
-- have to be written twice. None is used.
-- =====================================================================

-- Re-runnable: clear this batch's previous rule results first
-- (malformed rows are recorded by ingestion, not by this file, so they stay).
DELETE FROM audit.rejected_records
WHERE batch_id = %(batch_id)s AND rule_name <> 'malformed_row';


-- =====================================================================
-- STORES
-- =====================================================================
WITH r AS (
    SELECT s.*,
           to_jsonb(s) - ARRAY['batch_id', 'source_file', 'row_num', 'loaded_at'] AS record,
           row_number() OVER (PARTITION BY lower(trim(s.id)) ORDER BY s.row_num) AS dup_rank
    FROM raw.stores s
    WHERE s.batch_id = %(batch_id)s
)
INSERT INTO audit.rejected_records (batch_id, table_name, row_num, record, rule_name, reason, category)
SELECT r.batch_id, 'stores', r.row_num, r.record, c.rule_name, c.reason, c.category
FROM r
CROSS JOIN LATERAL (VALUES
    ('missing_required', 'id, name, opened_at and tax_rate are required', 'INVALID',
        staging.is_blank(r.id) OR staging.is_blank(r.name)
        OR staging.is_blank(r.opened_at) OR staging.is_blank(r.tax_rate)),
    ('invalid_uuid', 'id is not a valid UUID: ' || r.id, 'INVALID',
        NOT staging.is_blank(r.id) AND staging.try_uuid(r.id) IS NULL),
    ('invalid_timestamp', 'opened_at is not a valid timestamp: ' || r.opened_at, 'INVALID',
        NOT staging.is_blank(r.opened_at) AND staging.try_timestamp(r.opened_at) IS NULL),
    ('invalid_tax_rate', 'tax_rate must be a number between 0 and 1: ' || r.tax_rate, 'INVALID',
        NOT staging.is_blank(r.tax_rate)
        AND COALESCE(staging.try_numeric(r.tax_rate) NOT BETWEEN 0 AND 0.9999, TRUE)),
    ('duplicate_key', 'id appears more than once in the file (first kept)', 'INVALID',
        r.dup_rank > 1 AND NOT staging.is_blank(r.id))
) AS c(rule_name, reason, category, failed)
WHERE c.failed;

INSERT INTO staging.stores (store_id, store_name, opened_at, tax_rate, batch_id)
SELECT staging.try_uuid(s.id),
       regexp_replace(trim(s.name), '\s+', ' ', 'g'),
       staging.try_timestamp(s.opened_at),
       staging.try_numeric(s.tax_rate),
       s.batch_id
FROM raw.stores s
WHERE s.batch_id = %(batch_id)s
  AND NOT EXISTS (SELECT 1 FROM audit.rejected_records j
                  WHERE j.batch_id = s.batch_id AND j.table_name = 'stores'
                    AND j.row_num = s.row_num AND j.category <> 'EXCEPTION')
ON CONFLICT (store_id) DO UPDATE
SET store_name = EXCLUDED.store_name,
    opened_at  = EXCLUDED.opened_at,
    tax_rate   = EXCLUDED.tax_rate,
    batch_id   = EXCLUDED.batch_id,
    updated_at = now()
WHERE (staging.stores.store_name, staging.stores.opened_at, staging.stores.tax_rate)
      IS DISTINCT FROM (EXCLUDED.store_name, EXCLUDED.opened_at, EXCLUDED.tax_rate);


-- =====================================================================
-- CUSTOMERS
-- =====================================================================
WITH r AS (
    SELECT s.*,
           to_jsonb(s) - ARRAY['batch_id', 'source_file', 'row_num', 'loaded_at'] AS record,
           row_number() OVER (PARTITION BY lower(trim(s.id)) ORDER BY s.row_num) AS dup_rank
    FROM raw.customers s
    WHERE s.batch_id = %(batch_id)s
)
INSERT INTO audit.rejected_records (batch_id, table_name, row_num, record, rule_name, reason, category)
SELECT r.batch_id, 'customers', r.row_num, r.record, c.rule_name, c.reason, c.category
FROM r
CROSS JOIN LATERAL (VALUES
    ('missing_required', 'id and name are required', 'INVALID',
        staging.is_blank(r.id) OR staging.is_blank(r.name)),
    ('invalid_uuid', 'id is not a valid UUID: ' || r.id, 'INVALID',
        NOT staging.is_blank(r.id) AND staging.try_uuid(r.id) IS NULL),
    ('duplicate_key', 'id appears more than once in the file (first kept)', 'INVALID',
        r.dup_rank > 1 AND NOT staging.is_blank(r.id))
) AS c(rule_name, reason, category, failed)
WHERE c.failed;

INSERT INTO staging.customers (customer_id, customer_name, batch_id)
SELECT staging.try_uuid(s.id),
       regexp_replace(trim(s.name), '\s+', ' ', 'g'),
       s.batch_id
FROM raw.customers s
WHERE s.batch_id = %(batch_id)s
  AND NOT EXISTS (SELECT 1 FROM audit.rejected_records j
                  WHERE j.batch_id = s.batch_id AND j.table_name = 'customers'
                    AND j.row_num = s.row_num AND j.category <> 'EXCEPTION')
ON CONFLICT (customer_id) DO UPDATE
SET customer_name = EXCLUDED.customer_name,
    batch_id      = EXCLUDED.batch_id,
    updated_at    = now()
WHERE staging.customers.customer_name IS DISTINCT FROM EXCLUDED.customer_name;


-- =====================================================================
-- PRODUCTS
-- =====================================================================
WITH r AS (
    SELECT s.*,
           to_jsonb(s) - ARRAY['batch_id', 'source_file', 'row_num', 'loaded_at'] AS record,
           row_number() OVER (PARTITION BY upper(trim(s.sku)) ORDER BY s.row_num) AS dup_rank
    FROM raw.products s
    WHERE s.batch_id = %(batch_id)s
)
INSERT INTO audit.rejected_records (batch_id, table_name, row_num, record, rule_name, reason, category)
SELECT r.batch_id, 'products', r.row_num, r.record, c.rule_name, c.reason, c.category
FROM r
CROSS JOIN LATERAL (VALUES
    ('missing_required', 'sku, name, type and price are required', 'INVALID',
        staging.is_blank(r.sku) OR staging.is_blank(r.name)
        OR staging.is_blank(r.type) OR staging.is_blank(r.price)),
    ('invalid_amount', 'price must be a whole number of cents: ' || r.price, 'INVALID',
        NOT staging.is_blank(r.price) AND staging.try_bigint(r.price) IS NULL),
    ('negative_amount', 'price cannot be negative: ' || r.price, 'INVALID',
        staging.try_bigint(r.price) < 0),
    ('duplicate_key', 'sku appears more than once in the file (first kept)', 'INVALID',
        r.dup_rank > 1 AND NOT staging.is_blank(r.sku))
) AS c(rule_name, reason, category, failed)
WHERE c.failed;

INSERT INTO staging.products (sku, product_name, product_type, price, description, batch_id)
SELECT upper(trim(s.sku)),
       regexp_replace(trim(s.name), '\s+', ' ', 'g'),
       lower(trim(s.type)),
       staging.try_bigint(s.price) / 100.0,
       NULLIF(trim(s.description), ''),
       s.batch_id
FROM raw.products s
WHERE s.batch_id = %(batch_id)s
  AND NOT EXISTS (SELECT 1 FROM audit.rejected_records j
                  WHERE j.batch_id = s.batch_id AND j.table_name = 'products'
                    AND j.row_num = s.row_num AND j.category <> 'EXCEPTION')
ON CONFLICT (sku) DO UPDATE
SET product_name = EXCLUDED.product_name,
    product_type = EXCLUDED.product_type,
    price        = EXCLUDED.price,
    description  = EXCLUDED.description,
    batch_id     = EXCLUDED.batch_id,
    updated_at   = now()
WHERE (staging.products.product_name, staging.products.product_type,
       staging.products.price, staging.products.description)
      IS DISTINCT FROM (EXCLUDED.product_name, EXCLUDED.product_type,
                        EXCLUDED.price, EXCLUDED.description);


-- =====================================================================
-- SUPPLIES  (business key is id + sku: one supply is used by many products)
-- =====================================================================
WITH r AS (
    SELECT s.*,
           to_jsonb(s) - ARRAY['batch_id', 'source_file', 'row_num', 'loaded_at'] AS record,
           row_number() OVER (PARTITION BY upper(trim(s.id)), upper(trim(s.sku))
                              ORDER BY s.row_num) AS dup_rank
    FROM raw.supplies s
    WHERE s.batch_id = %(batch_id)s
)
INSERT INTO audit.rejected_records (batch_id, table_name, row_num, record, rule_name, reason, category)
SELECT r.batch_id, 'supplies', r.row_num, r.record, c.rule_name, c.reason, c.category
FROM r
CROSS JOIN LATERAL (VALUES
    ('missing_required', 'id, name, cost, perishable and sku are required', 'INVALID',
        staging.is_blank(r.id) OR staging.is_blank(r.name) OR staging.is_blank(r.cost)
        OR staging.is_blank(r.perishable) OR staging.is_blank(r.sku)),
    ('invalid_amount', 'cost must be a whole number of cents: ' || r.cost, 'INVALID',
        NOT staging.is_blank(r.cost) AND staging.try_bigint(r.cost) IS NULL),
    ('negative_amount', 'cost cannot be negative: ' || r.cost, 'INVALID',
        staging.try_bigint(r.cost) < 0),
    ('invalid_boolean', 'perishable must be True or False: ' || r.perishable, 'INVALID',
        NOT staging.is_blank(r.perishable)
        AND lower(trim(r.perishable)) NOT IN ('true', 'false', 't', 'f', '1', '0', 'yes', 'no')),
    ('duplicate_key', 'id + sku appears more than once in the file (first kept)', 'INVALID',
        r.dup_rank > 1 AND NOT staging.is_blank(r.id) AND NOT staging.is_blank(r.sku)),
    ('unknown_product', 'sku not found in products: ' || r.sku, 'INVESTIGATE',
        NOT staging.is_blank(r.sku)
        AND NOT EXISTS (SELECT 1 FROM staging.products p WHERE p.sku = upper(trim(r.sku))))
) AS c(rule_name, reason, category, failed)
WHERE c.failed;

INSERT INTO staging.supplies (supply_id, sku, supply_name, cost, is_perishable, batch_id)
SELECT upper(trim(s.id)),
       upper(trim(s.sku)),
       regexp_replace(trim(s.name), '\s+', ' ', 'g'),
       staging.try_bigint(s.cost) / 100.0,
       trim(s.perishable)::BOOLEAN,
       s.batch_id
FROM raw.supplies s
WHERE s.batch_id = %(batch_id)s
  AND NOT EXISTS (SELECT 1 FROM audit.rejected_records j
                  WHERE j.batch_id = s.batch_id AND j.table_name = 'supplies'
                    AND j.row_num = s.row_num AND j.category <> 'EXCEPTION')
ON CONFLICT (supply_id, sku) DO UPDATE
SET supply_name   = EXCLUDED.supply_name,
    cost          = EXCLUDED.cost,
    is_perishable = EXCLUDED.is_perishable,
    batch_id      = EXCLUDED.batch_id,
    updated_at    = now()
WHERE (staging.supplies.supply_name, staging.supplies.cost, staging.supplies.is_perishable)
      IS DISTINCT FROM (EXCLUDED.supply_name, EXCLUDED.cost, EXCLUDED.is_perishable);


-- =====================================================================
-- ORDERS
-- =====================================================================
WITH r AS (
    SELECT s.*,
           to_jsonb(s) - ARRAY['batch_id', 'source_file', 'row_num', 'loaded_at'] AS record,
           row_number() OVER (PARTITION BY lower(trim(s.id)) ORDER BY s.row_num) AS dup_rank,
           staging.try_uuid(s.customer)       AS v_customer,
           staging.try_uuid(s.store_id)       AS v_store,
           staging.try_timestamp(s.ordered_at) AS v_ordered_at,
           staging.try_bigint(s.subtotal)     AS v_subtotal,
           staging.try_bigint(s.tax_paid)     AS v_tax,
           staging.try_bigint(s.order_total)  AS v_total
    FROM raw.orders s
    WHERE s.batch_id = %(batch_id)s
)
INSERT INTO audit.rejected_records (batch_id, table_name, row_num, record, rule_name, reason, category)
SELECT r.batch_id, 'orders', r.row_num, r.record, c.rule_name, c.reason, c.category
FROM r
CROSS JOIN LATERAL (VALUES
    -- INVALID: the row is broken
    ('missing_required', 'all order fields are required', 'INVALID',
        staging.is_blank(r.id) OR staging.is_blank(r.customer) OR staging.is_blank(r.ordered_at)
        OR staging.is_blank(r.store_id) OR staging.is_blank(r.subtotal)
        OR staging.is_blank(r.tax_paid) OR staging.is_blank(r.order_total)),
    ('invalid_uuid', 'id, customer or store_id is not a valid UUID', 'INVALID',
        (NOT staging.is_blank(r.id)       AND staging.try_uuid(r.id) IS NULL)
        OR (NOT staging.is_blank(r.customer) AND r.v_customer IS NULL)
        OR (NOT staging.is_blank(r.store_id) AND r.v_store IS NULL)),
    ('invalid_timestamp', 'ordered_at is not a valid timestamp: ' || r.ordered_at, 'INVALID',
        NOT staging.is_blank(r.ordered_at) AND r.v_ordered_at IS NULL),
    ('invalid_amount', 'subtotal, tax_paid and order_total must be whole numbers of cents', 'INVALID',
        (NOT staging.is_blank(r.subtotal)       AND r.v_subtotal IS NULL)
        OR (NOT staging.is_blank(r.tax_paid)    AND r.v_tax IS NULL)
        OR (NOT staging.is_blank(r.order_total) AND r.v_total IS NULL)),
    ('negative_amount', 'amounts cannot be negative', 'INVALID',
        r.v_subtotal < 0 OR r.v_tax < 0 OR r.v_total < 0),
    ('duplicate_key', 'order id appears more than once in the file (first kept)', 'INVALID',
        r.dup_rank > 1 AND NOT staging.is_blank(r.id)),

    -- INVESTIGATE: may be valid, needs a person to check
    ('unknown_customer', 'customer not found (may arrive late): ' || r.customer, 'INVESTIGATE',
        r.v_customer IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM staging.customers c WHERE c.customer_id = r.v_customer)),
    ('unknown_store', 'store not found: ' || r.store_id, 'INVESTIGATE',
        r.v_store IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM staging.stores st WHERE st.store_id = r.v_store)),
    ('before_store_opened', 'order placed before the store opened', 'INVESTIGATE',
        EXISTS (SELECT 1 FROM staging.stores st
                WHERE st.store_id = r.v_store AND r.v_ordered_at < st.opened_at)),
    ('total_mismatch', 'subtotal + tax_paid differs from order_total by more than 1 cent', 'INVESTIGATE',
        abs(r.v_subtotal + r.v_tax - r.v_total) > 1),

    -- EXCEPTION: legitimate but unusual, loaded and flagged
    ('total_rounding', 'subtotal + tax_paid differs from order_total by 1 cent (rounding)', 'EXCEPTION',
        abs(r.v_subtotal + r.v_tax - r.v_total) = 1),
    ('zero_value_order', 'order total is zero (for example a free promotion)', 'EXCEPTION',
        r.v_total = 0)
) AS c(rule_name, reason, category, failed)
WHERE c.failed;

INSERT INTO staging.orders (order_id, customer_id, store_id, ordered_at,
                            subtotal, tax_paid, order_total, batch_id)
SELECT staging.try_uuid(s.id),
       staging.try_uuid(s.customer),
       staging.try_uuid(s.store_id),
       staging.try_timestamp(s.ordered_at),
       staging.try_bigint(s.subtotal)    / 100.0,
       staging.try_bigint(s.tax_paid)    / 100.0,
       staging.try_bigint(s.order_total) / 100.0,
       s.batch_id
FROM raw.orders s
WHERE s.batch_id = %(batch_id)s
  AND NOT EXISTS (SELECT 1 FROM audit.rejected_records j
                  WHERE j.batch_id = s.batch_id AND j.table_name = 'orders'
                    AND j.row_num = s.row_num AND j.category <> 'EXCEPTION')
ON CONFLICT (order_id) DO UPDATE
SET customer_id = EXCLUDED.customer_id,
    store_id    = EXCLUDED.store_id,
    ordered_at  = EXCLUDED.ordered_at,
    subtotal    = EXCLUDED.subtotal,
    tax_paid    = EXCLUDED.tax_paid,
    order_total = EXCLUDED.order_total,
    batch_id    = EXCLUDED.batch_id,
    updated_at  = now()
WHERE (staging.orders.customer_id, staging.orders.store_id, staging.orders.ordered_at,
       staging.orders.subtotal, staging.orders.tax_paid, staging.orders.order_total)
      IS DISTINCT FROM (EXCLUDED.customer_id, EXCLUDED.store_id, EXCLUDED.ordered_at,
                        EXCLUDED.subtotal, EXCLUDED.tax_paid, EXCLUDED.order_total);


-- =====================================================================
-- ITEMS
-- =====================================================================
WITH r AS (
    SELECT s.*,
           to_jsonb(s) - ARRAY['batch_id', 'source_file', 'row_num', 'loaded_at'] AS record,
           row_number() OVER (PARTITION BY lower(trim(s.id)) ORDER BY s.row_num) AS dup_rank,
           staging.try_uuid(s.order_id) AS v_order
    FROM raw.items s
    WHERE s.batch_id = %(batch_id)s
)
INSERT INTO audit.rejected_records (batch_id, table_name, row_num, record, rule_name, reason, category)
SELECT r.batch_id, 'items', r.row_num, r.record, c.rule_name, c.reason, c.category
FROM r
CROSS JOIN LATERAL (VALUES
    ('missing_required', 'id, order_id and sku are required', 'INVALID',
        staging.is_blank(r.id) OR staging.is_blank(r.order_id) OR staging.is_blank(r.sku)),
    ('invalid_uuid', 'id or order_id is not a valid UUID', 'INVALID',
        (NOT staging.is_blank(r.id) AND staging.try_uuid(r.id) IS NULL)
        OR (NOT staging.is_blank(r.order_id) AND r.v_order IS NULL)),
    ('duplicate_key', 'item id appears more than once in the file (first kept)', 'INVALID',
        r.dup_rank > 1 AND NOT staging.is_blank(r.id)),
    ('unknown_order', 'order not found (may arrive late): ' || r.order_id, 'INVESTIGATE',
        r.v_order IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM staging.orders o WHERE o.order_id = r.v_order)),
    ('unknown_product', 'sku not found in products: ' || r.sku, 'INVESTIGATE',
        NOT staging.is_blank(r.sku)
        AND NOT EXISTS (SELECT 1 FROM staging.products p WHERE p.sku = upper(trim(r.sku))))
) AS c(rule_name, reason, category, failed)
WHERE c.failed;

INSERT INTO staging.items (item_id, order_id, sku, batch_id)
SELECT staging.try_uuid(s.id),
       staging.try_uuid(s.order_id),
       upper(trim(s.sku)),
       s.batch_id
FROM raw.items s
WHERE s.batch_id = %(batch_id)s
  AND NOT EXISTS (SELECT 1 FROM audit.rejected_records j
                  WHERE j.batch_id = s.batch_id AND j.table_name = 'items'
                    AND j.row_num = s.row_num AND j.category <> 'EXCEPTION')
ON CONFLICT (item_id) DO UPDATE
SET order_id   = EXCLUDED.order_id,
    sku        = EXCLUDED.sku,
    batch_id   = EXCLUDED.batch_id,
    updated_at = now()
WHERE (staging.items.order_id, staging.items.sku)
      IS DISTINCT FROM (EXCLUDED.order_id, EXCLUDED.sku);


-- =====================================================================
-- BATCH STATISTICS  (rows_read was set during ingestion)
--   rejected = distinct rows with an INVALID or INVESTIGATE rule (incl. malformed)
--   flagged  = loaded rows that only have EXCEPTION rules
--   accepted = rows_read - rejected
-- =====================================================================
WITH stats AS (
    SELECT count(DISTINCT row_num) FILTER (WHERE category <> 'EXCEPTION') AS rejected
    FROM audit.rejected_records
    WHERE batch_id = %(batch_id)s
),
flagged AS (
    SELECT count(DISTINCT e.row_num) AS n
    FROM audit.rejected_records e
    WHERE e.batch_id = %(batch_id)s
      AND e.category = 'EXCEPTION'
      AND NOT EXISTS (SELECT 1 FROM audit.rejected_records x
                      WHERE x.batch_id = e.batch_id AND x.row_num = e.row_num
                        AND x.category <> 'EXCEPTION')
)
UPDATE audit.ingestion_log l
SET rows_rejected = stats.rejected,
    rows_flagged  = flagged.n,
    rows_accepted = l.rows_read - stats.rejected
FROM stats, flagged
WHERE l.batch_id = %(batch_id)s;
