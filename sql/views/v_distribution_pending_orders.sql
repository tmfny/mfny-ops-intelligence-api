CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_distribution_pending_orders` AS
SELECT
  id,
  name AS order_name,
  external_identifier,
  customer_company_name,
  customer_facility_license_number,
  sales_rep_name,
  status,
  display_status,
  delivery_date,
  payment_terms,
  subtotal,
  total_price,
  total_paid,
  remaining_balance,
  
  -- Derived: days until delivery (negative if past)
  CASE
    WHEN delivery_date IS NULL THEN NULL
    ELSE DATE_DIFF(delivery_date, CURRENT_DATE(), DAY)
  END AS days_to_delivery,
  
  -- Derived: days since order was created
  DATE_DIFF(CURRENT_DATE(), DATE(created_at), DAY) AS days_since_created,
  
  -- Urgency bucket — drives color treatment in the UI
  CASE
    WHEN delivery_date IS NULL THEN 'NO_DATE'
    WHEN delivery_date < CURRENT_DATE() THEN 'OVERDUE'
    WHEN delivery_date = CURRENT_DATE() THEN 'TODAY'
    ELSE 'UPCOMING'
  END AS urgency_bucket,
  
  -- Sort order for UI: OVERDUE first, then TODAY, then UPCOMING, then NO_DATE
  CASE
    WHEN delivery_date IS NULL THEN 4
    WHEN delivery_date < CURRENT_DATE() THEN 1
    WHEN delivery_date = CURRENT_DATE() THEN 2
    ELSE 3
  END AS urgency_sort,
  
  -- Pass through for deep-link to Canix
  invoice_url,
  
  -- Timestamps
  created_at,
  updated_at
  
FROM `mfny-to-bigquery.canix_raw.bronze_sales_orders`
WHERE
  facility_id = '4510'
  AND status IN ('created', 'approved', 'requested')
ORDER BY
  urgency_sort ASC,
  delivery_date ASC NULLS LAST,
  created_at ASC
