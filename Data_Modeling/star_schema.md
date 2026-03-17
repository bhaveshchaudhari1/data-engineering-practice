# Data Modeling: Star Schema Example

This document introduces a simple star schema design for a sales analytics use case.

## 🧱 Fact Table: `fact_sales`
Stores the measurable events (sales transactions).

**Columns:**
- `sale_id` (PK)
- `date_id` (FK to dim_date)
- `customer_id` (FK to dim_customer)
- `product_id` (FK to dim_product)
- `store_id` (FK to dim_store)
- `quantity` (numeric)
- `unit_price` (numeric)
- `total_amount` (numeric)

## 🧩 Dimension Tables
### `dim_date`
- `date_id` (PK)
- `date` (date)
- `year` (int)
- `quarter` (string)
- `month` (int)
- `day_of_week` (string)

### `dim_customer`
- `customer_id` (PK)
- `first_name`, `last_name`
- `email`
- `customer_since` (date)
- `customer_segment` (string)

### `dim_product`
- `product_id` (PK)
- `product_name`
- `category`
- `brand`
- `list_price`

### `dim_store`
- `store_id` (PK)
- `store_name`
- `region`
- `city`

## ✅ Best Practices
- Keep dimension tables denormalized for query performance.
- Use surrogate keys (integers) for joins.
- Track slowly changing dimensions (SCD) where appropriate.
- Partition the fact table by `date_id` or `event_date`.

## 🔍 Next Steps
- Add a section for SCD Type 2 design patterns.
- Add example SQL to build each table from staging data.
