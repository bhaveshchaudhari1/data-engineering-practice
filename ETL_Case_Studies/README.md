# ETL Case Studies

This folder contains end-to-end ETL case studies that simulate real-world data engineering workflows.

## 📁 Case Study Structure
Each case study typically includes:
- **Source data description** (CSV/JSON/Parquet files)
- **Transformation logic** (PySpark jobs or SQL scripts)
- **Validation checks** (row counts, schema assertions, null checks)
- **Output targets** (parquet tables, data marts)

## 🧩 Example Case Study: Sales Data Pipeline
- Ingest raw sales events
- Clean and enrich records
- Build a sales fact table partitioned by date
- Validate record counts and revenue consistency

> Tip: Use the `data/` folder (not included by default) to store sample inputs for running pipelines locally.
