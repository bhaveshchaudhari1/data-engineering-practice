-- Medium SQL Practice: Joins & Aggregations
-- Dataset: orders, customers
-- orders: order_id, customer_id, order_date, total_amount
-- customers: customer_id, first_name, last_name, region

-- Task: For each region, compute the total revenue and number of orders in Q1 2025.

SELECT
  c.region,
  COUNT(o.order_id) AS num_orders,
  SUM(o.total_amount) AS total_revenue
FROM orders o
JOIN customers c
  ON o.customer_id = c.customer_id
WHERE o.order_date BETWEEN '2025-01-01' AND '2025-03-31'
GROUP BY c.region
ORDER BY total_revenue DESC;
