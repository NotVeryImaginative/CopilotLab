--generate new file with queries where you answer following questions: 
--Who are the top 3 customers by spending?
--What is the most popular product category per month?
--What is the average monthly order value?
--How many orders were returned each month?


-- Assumed tables and key columns used by these queries:
-- customers(id, name, email)
-- orders(id, customer_id, total_amount, created_at)
-- order_items(id, order_id, product_id, quantity, unit_price)
-- products(id, category_id, name)
-- categories(id, name)
-- returns(id, order_id, returned_at)

-- 1) Top 3 customers by spending (total of orders.total_amount)
-- Returns: customer_id, customer_name, total_spent
WITH customer_spend AS (
  SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    SUM(o.total_amount) AS total_spent
  FROM customers c
  JOIN orders o ON o.customer_id = c.id
  GROUP BY c.id, c.name
)
SELECT customer_id, customer_name, total_spent
FROM customer_spend
ORDER BY total_spent DESC
LIMIT 3;


-- 2) Most popular product category per month (by total quantity sold)
-- Returns: month, category_id, category_name, total_quantity
WITH monthly_category_qty AS (
  SELECT
    date_trunc('month', o.created_at) AS month,
    p.category_id,
    SUM(oi.quantity) AS total_quantity
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.id
  JOIN products p ON p.id = oi.product_id
  GROUP BY date_trunc('month', o.created_at), p.category_id
),
ranked AS (
  SELECT
    mcq.*,
    ROW_NUMBER() OVER (PARTITION BY mcq.month ORDER BY mcq.total_quantity DESC) AS rn
  FROM monthly_category_qty mcq
)
SELECT
  r.month,
  r.category_id,
  cat.name AS category_name,
  r.total_quantity
FROM ranked r
LEFT JOIN categories cat ON cat.id = r.category_id
WHERE r.rn = 1
ORDER BY r.month;


-- 3) Average monthly order value
-- (a) Average order value per month: month, avg_order_value
-- (b) Overall average of monthly total revenue (i.e., average revenue per month)
-- Note: adjust to use orders.total_amount or compute from order_items if needed.

-- (a) average order value per month
SELECT
  date_trunc('month', created_at) AS month,
  AVG(total_amount) AS avg_order_value
FROM orders
GROUP BY date_trunc('month', created_at)
ORDER BY month;

-- (b) overall average monthly revenue (mean of monthly total revenue)
WITH monthly_revenue AS (
  SELECT
    date_trunc('month', created_at) AS month,
    SUM(total_amount) AS month_total_revenue
  FROM orders
  GROUP BY date_trunc('month', created_at)
)
SELECT
  AVG(month_total_revenue) AS avg_monthly_revenue,
  COUNT(*) AS months_count
FROM monthly_revenue;


-- 4) How many orders were returned each month?
-- Counts distinct returned orders grouped by the month of return (returns.returned_at).
-- If your returns table records returns per item, dedupe by order_id.
SELECT
  date_trunc('month', r.returned_at) AS month,
  COUNT(DISTINCT r.order_id) AS returned_orders_count
FROM returns r
GROUP BY date_trunc('month', r.returned_at)
ORDER BY month;
