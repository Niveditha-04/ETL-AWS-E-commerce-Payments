-- 1) Clean out any partial/empty table
DROP TABLE IF EXISTS public.transactions_cleaned;

CREATE TABLE public.transactions_cleaned (
  transaction_id   VARCHAR(64),
  user_id          INT,
  amount           DOUBLE PRECISION,
  currency         VARCHAR(8),
  "timestamp"      TIMESTAMP,
  amount_usd       DOUBLE PRECISION,
  month            DATE,
  is_anomaly       BOOLEAN
);

-- 2) COPY ONLY the latest run (8-column Parquet)
COPY public.transactions_cleaned
FROM 's3://payments-etl-pipeline/transformed/run-1757235587957'
IAM_ROLE 'arn:aws:iam::570282482024:role/glue-redshift-role'
FORMAT AS PARQUET;

-- total rows
SELECT COUNT(*) AS rows_loaded
FROM public.transactions_cleaned;

-- anomaly distribution
SELECT is_anomaly, COUNT(*) AS n
FROM public.transactions_cleaned
GROUP BY 1
ORDER BY 2 DESC;

-- quick sample
SELECT transaction_id, amount, "timestamp", is_anomaly
FROM public.transactions_cleaned
LIMIT 20;

-- date range
SELECT MIN("timestamp") AS min_ts, MAX("timestamp") AS max_ts
FROM public.transactions_cleaned;

-- remove any old objects with same names (table or view)
DROP VIEW  IF EXISTS public.transactions_summary;
DROP TABLE IF EXISTS public.transactions_summary;
DROP VIEW  IF EXISTS public.currency_revenue;
DROP TABLE IF EXISTS public.currency_revenue;
DROP VIEW  IF EXISTS public.anomalies;
DROP TABLE IF EXISTS public.anomalies;

-- monthly summary (for KPI tiles / trend)
CREATE OR REPLACE VIEW public.transactions_summary AS
SELECT
  DATE_TRUNC('month', "timestamp") AS txn_month,
  COUNT(*)                         AS txn_count,
  SUM(amount_usd)                  AS total_revenue_usd
FROM public.transactions_cleaned
GROUP BY 1
ORDER BY 1;

-- revenue by currency per month (for breakdowns)
CREATE OR REPLACE VIEW public.currency_revenue AS
SELECT
  currency,
  DATE_TRUNC('month', "timestamp") AS txn_month,
  COUNT(*)                         AS txn_count,
  SUM(amount_usd)                  AS total_revenue_usd
FROM public.transactions_cleaned
GROUP BY 1, 2
ORDER BY total_revenue_usd DESC;

-- minimal anomaly view (proves the "anomaly detection" part)
CREATE OR REPLACE VIEW public.anomalies AS
SELECT *
FROM public.transactions_cleaned
WHERE is_anomaly = TRUE;

SELECT * FROM public.transactions_summary  LIMIT 10;
SELECT * FROM public.currency_revenue      LIMIT 10;
SELECT * FROM public.anomalies             LIMIT 10;
