CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_sales_rep_activity` AS
SELECT
  sales_rep_name,
  customer_company_name,
  COUNT(*) AS total_orders,
  ROUND(SUM(total_price), 2) AS total_revenue,
  ROUND(SUM(total_paid), 2) AS total_paid,
  MAX(DATE(created_at)) AS last_order_date,
  MIN(DATE(created_at)) AS first_order_date,
  DATE_DIFF(CURRENT_DATE(), MAX(DATE(created_at)), DAY) AS days_since_last_order,
  COUNTIF(DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AS orders_last_30d,
  ROUND(SUM(IF(DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY), total_price, 0)), 2) AS revenue_last_30d
FROM `mfny-to-bigquery.canix_raw.bronze_sales_orders`
WHERE facility_id != '4475'
  AND status = 'accepted'
  AND sales_rep_name IS NOT NULL
  AND sales_rep_name != ''
  AND customer_company_name IS NOT NULL
  AND customer_company_name != ''
GROUP BY sales_rep_name, customer_company_name
ORDER BY sales_rep_name, last_order_date DESC
