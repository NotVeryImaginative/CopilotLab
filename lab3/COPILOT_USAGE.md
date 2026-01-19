# Usage Notes — GitHub Copilot assisted work

Summary
- Copilot helped draft Spark code, data-generation logic, Unity Catalog (UC) interactions, validation SQL, and unit tests.

What I did
- Generated synthetic sales transaction data and combined it into a Spark DataFrame.
- Cleaned and enriched the data (dropped nulls, added order_year).
- Persisted results as a managed Delta table in Unity Catalog using saveAsTable(...).
- Implemented aggregation and validation queries (sales per year, totals by payment mode, negative transactions, customer-level credit/debit checks).
- Created a calendar DataFrame for 2024 and pytest tests for local validation.

Process note
- All of the steps, transformations and code in this repository were produced following the Copilot adoption documentation and generated step-by-step using GitHub Copilot as the primary authoring assistant. The development followed the guidance and patterns in that documentation to ensure consistent, testable and maintainable code.

Challenge encountered
- Direct writing to workspace file paths (Parquet/CSV on DBFS or FileStore) was blocked in my environment, so I could not rely on writing raw files and then registering them.
- Attempts to list or use DBFS paths failed due to workspace restrictions.

Solution
- Instead of writing Parquet/CSV files, I saved the data as managed Delta tables in Unity Catalog (saveAsTable). Delta stores Parquet files internally while providing ACID, governance and UC metadata — this avoided filesystem access issues.
- All validation and checks use SQL and DataFrame APIs compatible with Spark Connect / serverless environments (avoiding unsupported rdd, CACHE TABLE, or dbutils operations).