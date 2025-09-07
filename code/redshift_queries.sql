COPY transactions_clean
FROM 's3://payments-etl-pipeline/transactions_clean.csv'
IAM_ROLE 'arn:aws:iam::570282482024:role/glue-redshift-role'
FORMAT AS CSV
IGNOREHEADER 1;
SELECT COUNT(*) FROM transactions_clean;
SELECT * FROM transactions_clean LIMIT 10;
SELECT * FROM transactions_summary LIMIT 10;
SELECT 
  currency,
  SUM(amount_usd) AS total_revenue
FROM transactions_clean
GROUP BY currency
ORDER BY total_revenue DESC
LIMIT 5;
SELECT
  DATE_TRUNC('month', timestamp) AS txn_month,
  COUNT(*) AS txn_count
FROM transactions_clean
GROUP BY txn_month
ORDER BY txn_month;
SELECT COUNT(*) FROM transactions_clean;
SELECT * FROM transactions_clean LIMIT 20;
SELECT SUM(amount_usd) AS total_revenue_usd FROM transactions_clean;
SELECT MIN(timestamp), MAX(timestamp) FROM transactions_clean;
SELECT
  user_id,
  SUM(amount_usd) AS total_spent
FROM transactions_clean
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10;
SELECT
  currency,
  SUM(amount_usd) AS total_revenue
FROM transactions_clean
GROUP BY currency
ORDER BY total_revenue DESC;
SELECT
  user_id,
  COUNT(*) AS txn_count,
  SUM(amount_usd) AS total_spent
FROM transactions_clean
GROUP BY user_id
HAVING COUNT(*) > 50 AND SUM(amount_usd) > 10000
ORDER BY total_spent DESC;
SELECT
  DATE_TRUNC('day', timestamp) AS txn_day,
  SUM(amount_usd) AS daily_revenue
FROM transactions_clean
GROUP BY txn_day
ORDER BY txn_day;
SELECT
  DATE_TRUNC('month', timestamp) AS txn_month,
  COUNT(*) AS txn_count,
  SUM(amount_usd) AS total_revenue_usd
FROM transactions_clean
GROUP BY txn_month
ORDER BY txn_month;
