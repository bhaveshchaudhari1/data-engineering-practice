-- Hard SQL Practice: CTEs, Window Functions, and Ranking
-- Dataset: sales
-- sales: sale_id, customer_id, sale_date, amount, product_category

-- Task: For each customer, find their top 2 purchase months by total amount.

WITH monthly_totals AS (
  SELECT
    customer_id,
    DATE_TRUNC('month', sale_date) AS month,
    SUM(amount) AS monthly_amount
  FROM sales
  GROUP BY customer_id, DATE_TRUNC('month', sale_date)
),
ranked AS (
  SELECT
    customer_id,
    month,
    monthly_amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_amount DESC) AS rank_by_amount
  FROM monthly_totals
)
SELECT
  customer_id,
  month,
  monthly_amount
FROM ranked
WHERE rank_by_amount <= 2
ORDER BY customer_id, rank_by_amount;
