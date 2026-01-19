/*
Redshift optimization guidance for the analytics queries.

Summary:
- Use DISTKEY to co-locate rows for large joins; use DISTSTYLE ALL for small dimension tables.
- Use SORTKEY on time columns used for filtering or ordering (created_at, returned_at).
- Use CTAS or CREATE TEMP TABLE AS for heavy aggregations to reduce repeated scans and redistribute work efficiently.
- Minimize COUNT(DISTINCT ...) on large sets; pre-aggregate or use APPROXIMATE techniques if acceptable.
- Run ANALYZE after bulk loads; VACUUM when there are many deletes/updates.
- Prefer appropriate encodings (e.g., zstd, lzo) and small numeric types.

Recommended DDL hints (apply while creating Redshift tables):
-- customers: small dimension -> replicate
CREATE TABLE customers (
  id BIGINT NOT NULL,
  name VARCHAR(256),
  email VARCHAR(256)
) DISTSTYLE ALL;

-- orders: large fact -> distribute by customer_id and sort by created_at
CREATE TABLE orders (
  id BIGINT NOT NULL,
  customer_id BIGINT NOT NULL,
  total_amount NUMERIC(12,2),
  created_at TIMESTAMPTZ
) DISTSTYLE KEY DISTKEY(customer_id) SORTKEY(created_at);

-- order_items: distribute by order_id (join to orders)
CREATE TABLE order_items (
  id BIGINT NOT NULL,
  order_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  quantity INT,
  unit_price NUMERIC(10,2)
) DISTSTYLE KEY DISTKEY(order_id) SORTKEY(product_id);

-- products: small/medium dimension; replicate if small
CREATE TABLE products (
  id BIGINT NOT NULL,
  category_id BIGINT,
  name VARCHAR(256)
) DISTSTYLE ALL;

CREATE TABLE categories (
  id BIGINT NOT NULL,
  name VARCHAR(128)
) DISTSTYLE ALL;

CREATE TABLE returns (
  id BIGINT NOT NULL,
  order_id BIGINT NOT NULL,
  returned_at TIMESTAMPTZ
) DISTSTYLE KEY DISTKEY(order_id) SORTKEY(returned_at);


Optimized query patterns

1) Top 3 customers by spending
-- Pre-aggregate orders into a temp table to avoid re-scanning the orders table
CREATE TEMP TABLE tmp_customer_spend DISTSTYLE KEY DISTKEY(customer_id) AS
SELECT customer_id, SUM(total_amount) AS total_spent
FROM orders
GROUP BY customer_id;
ANALYZE tmp_customer_spend;

SELECT c.id AS customer_id, c.name AS customer_name, t.total_spent
FROM tmp_customer_spend t
JOIN customers c ON c.id = t.customer_id
ORDER BY t.total_spent DESC
LIMIT 3;


2) Most popular product category per month (by quantity)
-- Aggregate order_items join once, store month + category totals
CREATE TEMP TABLE tmp_monthly_category_qty DISTSTYLE KEY DISTKEY(category_id) AS
SELECT date_trunc('month', o.created_at)::date AS month,
       p.category_id,
       SUM(oi.quantity) AS total_quantity
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN products p ON p.id = oi.product_id
GROUP BY 1, p.category_id;
ANALYZE tmp_monthly_category_qty;

-- small aggregated result: use window function to pick top category per month
SELECT q.month, q.category_id, cat.name AS category_name, q.total_quantity
FROM (
  SELECT month, category_id, total_quantity,
         ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_quantity DESC) AS rn
  FROM tmp_monthly_category_qty
) q
JOIN categories cat ON cat.id = q.category_id
WHERE q.rn = 1
ORDER BY q.month;


3) Average monthly order value
-- Single-pass aggregation on orders into a temp table
CREATE TEMP TABLE tmp_monthly_order_stats AS
SELECT date_trunc('month', created_at)::date AS month,
       AVG(total_amount) AS avg_order_value,
       SUM(total_amount) AS month_total_revenue,
       COUNT(*) AS orders_count
FROM orders
GROUP BY 1;
ANALYZE tmp_monthly_order_stats;

-- per-month
SELECT month, avg_order_value, orders_count FROM tmp_monthly_order_stats ORDER BY month;

-- overall average monthly revenue
SELECT AVG(month_total_revenue) AS avg_monthly_revenue, COUNT(*) AS months_count
FROM tmp_monthly_order_stats;


4) Orders returned each month
-- If returns table has one row per return, dedupe by order_id
CREATE TEMP TABLE tmp_returns_by_month AS
SELECT date_trunc('month', returned_at)::date AS month,
       COUNT(DISTINCT order_id) AS returned_orders_count
FROM returns
GROUP BY 1;
ANALYZE tmp_returns_by_month;

SELECT month, returned_orders_count FROM tmp_returns_by_month ORDER BY month;


Operational checklist
- After large bulk loads: RUN ANALYZE <table>; if many deletes/updates, RUN VACUUM <table>.
- Use DISTKEY on join columns to minimize network transfer; use DISTSTYLE ALL for small reference tables.
- Use SORTKEY on timestamp or frequently filtered columns to reduce IO during range scans.
- Use CTAS for heavy derived tables so Redshift can apply optimal compression and distribution.
- Monitor with STL_QUERY, SVV_TABLE_INFO and WLM queues; tune WLM concurrency for analytical workloads.
- If COUNT(DISTINCT) is expensive, consider pre-aggregating or using approximate methods (when acceptable).
*/
