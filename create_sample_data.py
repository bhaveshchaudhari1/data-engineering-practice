import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

# Generate sample data
n_records = 100
start_date = datetime(2024, 1, 1)

data = {
    'sales_id': range(1001, 1001 + n_records),
    'transaction_date': [start_date + timedelta(days=random.randint(0, 365)) for _ in range(n_records)],
    'salesperson_name': np.random.choice(['John Smith', 'Sarah Johnson', 'Mike Davis', 'Emily Brown', 'Robert Wilson'], n_records),
    'product_name': np.random.choice(['Laptop', 'Monitor', 'Keyboard', 'Mouse', 'Headphones', 'Printer', 'Router'], n_records),
    'region': np.random.choice(['North', 'South', 'East', 'West', 'Central'], n_records),
    'quantity': np.random.randint(1, 50, n_records),
    'unit_price': np.random.uniform(50, 2000, n_records).round(2),
    'discount_percentage': np.random.uniform(0, 20, n_records).round(2),
    'total_sales': np.random.uniform(500, 50000, n_records).round(2),
    'commission_rate': np.random.uniform(2, 15, n_records).round(2),
    'commission_amount': np.random.uniform(10, 5000, n_records).round(2),
    'is_international': np.random.choice([True, False], n_records),
    'is_returned': np.random.choice([True, False], n_records),
    'payment_method': np.random.choice(['Credit Card', 'Debit Card', 'Wire Transfer', 'Check'], n_records),
    'customer_age': np.random.randint(18, 75, n_records),
    'customer_satisfaction_rating': np.random.uniform(1, 5, n_records).round(2),
    'sale_timestamp': [start_date + timedelta(days=random.randint(0, 365), hours=random.randint(0, 23), minutes=random.randint(0, 59)) for _ in range(n_records)],
    'tax_amount': np.random.uniform(20, 2000, n_records).round(2),
    'total_with_tax': np.random.uniform(520, 52000, n_records).round(2),
    'order_status': np.random.choice(['Completed', 'Pending', 'Shipped', 'Cancelled'], n_records),
    'net_profit_margin': np.random.uniform(5, 40, n_records).round(2),
}

# Create DataFrame
df = pd.DataFrame(data)

# Save as CSV
csv_path = 'sample_sales_data.csv'
df.to_csv(csv_path, index=False)
print(f"CSV file created: {csv_path}")

# Save as Parquet
parquet_path = 'sample_sales_data.parquet'
df.to_parquet(parquet_path, index=False)
print(f"Parquet file created: {parquet_path}")

# Display info
print(f"\nDataset Information:")
print(f"Total Records: {len(df)}")
print(f"Total Columns: {len(df.columns)}")
print(f"\nColumn Data Types:")
print(df.dtypes)
print(f"\nFirst 5 rows:")
print(df.head())
