CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_sales_daily_revenue` AS
SELECT
  DATE(created_at) AS sale_date,
  COUNT(*) AS order_count,
  ROUND(SUM(total_price), 2) AS revenue,
  ROUND(SUM(total_paid), 2) AS paid,
  COUNT(DISTINCT customer_id) AS distinct_customers,
  COUNT(DISTINCT sales_rep_id) AS distinct_reps_active
FROM `mfny-to-bigquery.canix_raw.bronze_sales_orders`
WHERE facility_id != '4475'
  AND status = 'accepted'
  AND DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY sale_date
ORDER BY sale_date DESC
