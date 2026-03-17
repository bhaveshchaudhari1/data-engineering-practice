-- Easy SQL Practice: Basic SELECT and Filtering
-- Dataset: orders
-- Columns: order_id, customer_id, order_date, total_amount, status

-- Task: 1) Return all orders for customer_id = 42
--       2) Only include orders with total_amount > 100

SELECT
  order_id,
  customer_id,
  order_date,
  total_amount,
  status
FROM orders
WHERE customer_id = 42
  AND total_amount > 100
ORDER BY order_date DESC;
