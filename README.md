This project is an end-to-end analytics stack for synthetic e-commerce payments. It shows: raw events → cleaned parquet → queryable tables → dashboards 

--------

**What it does**

1. Generate data: 1M+ payment rows with Faker (transaction_id, user_id, amount, currency, timestamp).
2. Ingest: upload raw CSV to Amazon S3.
3. Transform: AWS Glue (PySpark) cleans and enriches data
– converts to Parquet, adds amount_usd, month, and an is_anomaly flag.
– stores the transformed output back to S3.
4. Load: Redshift Serverless ingests the Parquet folder via COPY FROM PARQUET.
5. Model: transactions_summary (monthly KPIs), currency_revenue (currency x month), anomalies (records flagged true).
6. Visualize: a few Tableau charts for revenue trends, top users, and breakdowns by currency.

--------

**Why this exists**

A clear, reproducible pattern for batch analytics on AWS: Storage (S3), scalable compute (Glue), SQL-friendly serving (Redshift), and BI on top. It’s the kind of pipeline you can lift into a real product with minimal changes.

**What you’ll see in results**

1. Fast SQL over 1M+ rows in Redshift.
2. Monthly revenue + counts (transactions_summary).
3. Currency breakdowns (currency_revenue).
4. A simple anomaly slice (anomalies) to prove the enrichment step.
5. Tableau views that answer: How much are we making over time? Which currency drives revenue? Who are the top spenders?

--------

**How to run**

1. Generate transactions_raw.csv with the Python script and upload to S3.
2. Run the Glue job to write Parquet to the transformed/ prefix.
3. In Redshift, run the provided SQL to COPY from that prefix and create the views.
4. Point Tableau to Redshift and build/refresh the dashboards.

> Note:

i. Data is synthetic and for demonstration only.
ii. Glue write schema (8 cols): transaction_id, user_id, amount, currency, timestamp, amount_usd, month, is_anomaly.

--------

 
<img width="898" height="507" alt="Screenshot 2025-09-10 at 10 52 10 PM" src="https://github.com/user-attachments/assets/c12bce91-5949-465d-80da-dda697fd7d2d" />





**Stack:** S3 · Glue (PySpark) · Redshift Serverless · SQL · Tableau · Python
