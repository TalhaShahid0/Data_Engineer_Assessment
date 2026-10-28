-- =====================================================================
-- transformations.sql : staging -> analytics (star schema + metrics)
--
-- 1. Dimensions and facts are UPSERTED from staging (ON CONFLICT), and a
--    row is only rewritten when something actually changed, so running this
--    file any number of times gives the same result (idempotent).
--    Dimensions are SCD Type 1: the latest value overwrites the old one.
-- 2. Metric tables are fully rebuilt (TRUNCATE + INSERT) from the facts, so
--    late-arriving or corrected records are always reflected.
--
-- Metric rules (see docs/architecture.md):
--   - revenue      = order_total (includes tax), in dollars
--   - empty orders = orders with no items (in the source these all have a
--                    total of 0); excluded from metrics 1 and 2
--   - only records that passed data quality reach staging, so rejected
--     records are excluded automatically
-- =====================================================================


-- =====================================================================
-- DIMENSIONS
-- =====================================================================

INSERT INTO analytics.dim_stores (store_id, store_name, opened_at, tax_rate)
SELECT store_id, store_name, opened_at, tax_rate
FROM staging.stores
ON CONFLICT (store_id) DO UPDATE
SET store_name = EXCLUDED.store_name,
    opened_at  = EXCLUDED.opened_at,
    tax_rate   = EXCLUDED.tax_rate
WHERE (analytics.dim_stores.store_name, analytics.dim_stores.opened_at, analytics.dim_stores.tax_rate)
      IS DISTINCT FROM (EXCLUDED.store_name, EXCLUDED.opened_at, EXCLUDED.tax_rate);


INSERT INTO analytics.dim_customers (customer_id, customer_name)
SELECT customer_id, customer_name
FROM staging.customers
ON CONFLICT (customer_id) DO UPDATE
SET customer_name = EXCLUDED.customer_name
WHERE analytics.dim_customers.customer_name IS DISTINCT FROM EXCLUDED.customer_name;


-- unit_cost = total cost of all supplies used to make one unit of the product.
INSERT INTO analytics.dim_products (sku, product_name, product_type, price, unit_cost, description)
SELECT p.sku,
       p.product_name,
       p.product_type,
       p.price,
       COALESCE(s.unit_cost, 0),
       p.description
FROM staging.products p
LEFT JOIN (SELECT sku, sum(cost) AS unit_cost
           FROM staging.supplies
           GROUP BY sku) s ON s.sku = p.sku
ON CONFLICT (sku) DO UPDATE
SET product_name = EXCLUDED.product_name,
    product_type = EXCLUDED.product_type,
    price        = EXCLUDED.price,
    unit_cost    = EXCLUDED.unit_cost,
    description  = EXCLUDED.description
WHERE (analytics.dim_products.product_name, analytics.dim_products.product_type,
       analytics.dim_products.price, analytics.dim_products.unit_cost,
       analytics.dim_products.description)
      IS DISTINCT FROM (EXCLUDED.product_name, EXCLUDED.product_type,
                        EXCLUDED.price, EXCLUDED.unit_cost, EXCLUDED.description);


-- =====================================================================
-- FACTS
-- =====================================================================

-- One row per order. item_count is recalculated every run, so items that
-- arrive in a later batch update their order.
INSERT INTO analytics.fct_orders (order_id, customer_id, store_id, ordered_at, order_date,
                                  subtotal, tax_paid, order_total, item_count)
SELECT o.order_id,
       o.customer_id,
       o.store_id,
       o.ordered_at,
       o.ordered_at::DATE,
       o.subtotal,
       o.tax_paid,
       o.order_total,
       COALESCE(i.item_count, 0)
FROM staging.orders o
JOIN analytics.dim_customers c ON c.customer_id = o.customer_id
JOIN analytics.dim_stores    s ON s.store_id    = o.store_id
LEFT JOIN (SELECT order_id, count(*) AS item_count
           FROM staging.items
           GROUP BY order_id) i ON i.order_id = o.order_id
ON CONFLICT (order_id) DO UPDATE
SET customer_id = EXCLUDED.customer_id,
    store_id    = EXCLUDED.store_id,
    ordered_at  = EXCLUDED.ordered_at,
    order_date  = EXCLUDED.order_date,
    subtotal    = EXCLUDED.subtotal,
    tax_paid    = EXCLUDED.tax_paid,
    order_total = EXCLUDED.order_total,
    item_count  = EXCLUDED.item_count
WHERE (analytics.fct_orders.customer_id, analytics.fct_orders.store_id,
       analytics.fct_orders.ordered_at, analytics.fct_orders.subtotal,
       analytics.fct_orders.tax_paid, analytics.fct_orders.order_total,
       analytics.fct_orders.item_count)
      IS DISTINCT FROM (EXCLUDED.customer_id, EXCLUDED.store_id, EXCLUDED.ordered_at,
                        EXCLUDED.subtotal, EXCLUDED.tax_paid, EXCLUDED.order_total,
                        EXCLUDED.item_count);


-- One row per item sold. The source has no item price, so the product's
-- price and cost are captured when the item is first loaded; a later price
-- change does not rewrite past sales.
INSERT INTO analytics.fct_order_items (item_id, order_id, sku, unit_price, unit_cost)
SELECT i.item_id,
       i.order_id,
       i.sku,
       p.price,
       p.unit_cost
FROM staging.items i
JOIN analytics.fct_orders   o ON o.order_id = i.order_id
JOIN analytics.dim_products p ON p.sku      = i.sku
ON CONFLICT (item_id) DO UPDATE
SET order_id   = EXCLUDED.order_id,
    sku        = EXCLUDED.sku,
    unit_price = EXCLUDED.unit_price,
    unit_cost  = EXCLUDED.unit_cost
WHERE (analytics.fct_order_items.order_id, analytics.fct_order_items.sku)
      IS DISTINCT FROM (EXCLUDED.order_id, EXCLUDED.sku);


-- =====================================================================
-- METRICS (rebuilt every run)
-- =====================================================================

-- Metric 1: monthly revenue, number of orders and average order value per store.
TRUNCATE analytics.monthly_store_revenue;
INSERT INTO analytics.monthly_store_revenue
    (month, store_id, store_name, order_count, revenue, avg_order_value)
SELECT date_trunc('month', o.order_date)::DATE    AS month,
       s.store_id,
       s.store_name,
       count(*)                                   AS order_count,
       sum(o.order_total)                         AS revenue,
       round(sum(o.order_total) / count(*), 2)    AS avg_order_value
FROM analytics.fct_orders o
JOIN analytics.dim_stores s ON s.store_id = o.store_id
WHERE o.item_count > 0
GROUP BY 1, s.store_id, s.store_name;


-- Metric 2: per customer - first/last order, number of orders, lifetime value,
-- and whether they came back (repeat customer = more than one order).
TRUNCATE analytics.customer_summary;
INSERT INTO analytics.customer_summary
    (customer_id, customer_name, first_order_date, last_order_date,
     order_count, lifetime_value, is_repeat_customer)
SELECT c.customer_id,
       c.customer_name,
       min(o.order_date),
       max(o.order_date),
       count(*),
       sum(o.order_total),
       count(*) > 1
FROM analytics.fct_orders o
JOIN analytics.dim_customers c ON c.customer_id = o.customer_id
WHERE o.item_count > 0
GROUP BY c.customer_id, c.customer_name;


-- Metric 3: monthly units sold, revenue (pre-tax item prices), cost and
-- gross margin per product.
TRUNCATE analytics.monthly_product_sales;
INSERT INTO analytics.monthly_product_sales
    (month, sku, product_name, product_type, units_sold, revenue, total_cost, gross_margin)
SELECT date_trunc('month', o.order_date)::DATE  AS month,
       p.sku,
       p.product_name,
       p.product_type,
       count(*)                                 AS units_sold,
       sum(i.unit_price)                        AS revenue,
       sum(i.unit_cost)                         AS total_cost,
       sum(i.unit_price) - sum(i.unit_cost)     AS gross_margin
FROM analytics.fct_order_items i
JOIN analytics.fct_orders   o ON o.order_id = i.order_id
JOIN analytics.dim_products p ON p.sku      = i.sku
GROUP BY 1, p.sku, p.product_name, p.product_type;
