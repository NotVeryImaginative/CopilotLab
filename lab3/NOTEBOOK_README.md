# Sales Transactions Notebook — Summary

This notebook demonstrates generating synthetic sales transaction data, persisting it in Unity Catalog (Delta), performing basic cleaning and feature engineering, and running validation and aggregation queries.

Overview
- Create SparkSession with limited parallelism (4 shuffle partitions).
- Define a strict schema for sales transactions.
- Build a small seed dataset and generate + append 1000 random rows (kept to 4 partitions).
- Save the combined DataFrame as a managed Delta table in Unity Catalog:
  - saveAsTable(catalog.schema.table)
- Clean the data:
  - drop rows with nulls, add `order_year` from `transaction_date`.
  - optional: persist cleaned table as `sales_transactions_uc_cleaned`.
- Run validation and aggregation queries (compatible with Spark Connect / Databricks Connect):
  - total sales per year
  - total by payment mode
  - identify negative transactions
  - customer-level credit/debit validation (credit < debit and credit <= 10% total)
- Optimized aggregations:
  - create a narrow TEMP VIEW containing only required columns, tune `spark.sql.shuffle.partitions`, run aggregations, then DROP VIEW.
  - Note: `CACHE TABLE` is not supported for serverless compute; the notebook avoids it.
- Additional utilities:
  - Generated calendar DataFrame for year 2024 (useful for joins/time analysis).
- Tests:
  - PyTest tests were created under `tests/test_sales_processing.py` to validate transformations locally with a local SparkSession.

How to run
1. Adjust `catalog` and `schema` variables in the write cells to your Unity Catalog names.
2. Run cells in order in Databricks Connect or a Databricks notebook. Use Spark Connect-compatible APIs (avoid `.rdd`, `dbutils.fs.ls` under Spark Connect limitations).
3. If running locally for tests:
   - Install pyspark and pytest.
   - Run `pytest /Users/manolis/lab_copilot/lab3/tests/test_sales_processing.py`

Unity Catalog notes
- Managed Delta tables created with `saveAsTable` do not require external locations.
- To register Parquet/CSV as UC external tables you must register an External Location and Storage Credential (admin task).
- Serverless environments may restrict operations like `CACHE TABLE`. The notebook uses a serverless-safe approach.

Files of interest
- Notebook: /Users/manolis/lab_copilot/lab3/Untitled Notebook 2025-12-02 23_02_15.ipynb
- Tests: /Users/manolis/lab_copilot/lab3/tests/test_sales_processing.py
- This README: /Users/manolis/lab_copilot/lab3/NOTEBOOK_README.md

Contact
- Use the notebook cells and tests as a starting point; adjust paths, UC names and cluster credentials as needed.
