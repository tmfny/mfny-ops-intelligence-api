CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_sales_status_strip` AS
WITH mtd_sales AS (
  -- Month-to-date revenue and paid for "accepted" orders.
  -- "Accepted" is MFNY's terminal state (98% of orders); "shipped" status is
  -- barely used. We use created_at to anchor "month-to-date" since that's
  -- when the order was logged in Canix.
  SELECT
    SUM(total_price) AS mtd_revenue,
    SUM(total_paid) AS mtd_paid,
    COUNT(*) AS mtd_order_count
  FROM `mfny-to-bigquery.canix_raw.bronze_sales_orders`
  WHERE facility_id != '4475'
    AND status = 'accepted'
    AND DATE(created_at) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
),

sellable_value AS (
  -- Total sellable inventory value at facility 4510 (distribution).
  -- weight here = unit count (Canix uses "weight" as universal quantity
  -- field even when unit_of_measure is "Each"). Join to dim_wholesale_prices
  -- via item_subcategory. Only the SAMPLES subcategories lack pricing —
  -- expected, samples aren't sold.
  SELECT
    SUM(p.weight * dp.wholesale_price_per_unit) AS total_sellable_value,
    COUNT(*) AS sellable_package_count,
    SUM(p.weight) AS sellable_unit_count
  FROM `mfny-to-bigquery.canix_raw.bronze_packages` p
  LEFT JOIN `mfny-to-bigquery.canix_raw.dim_wholesale_prices` dp
    ON p.item_subcategory = dp.item_subcategory
  WHERE p.facility_id = '4510'
    AND p.status = 'Available To Sell'
),

pending_fulfillment AS (
  -- Orders awaiting fulfillment: created or approved status, not yet
  -- accepted. These are the orders Omar might need to push through or
  -- check on. Note: 'created' includes the stuck-drafts backlog Amber
  -- is reviewing — could surface separately if useful in v2.
  SELECT
    COUNT(*) AS pending_order_count,
    SUM(total_price) AS pending_order_value
  FROM `mfny-to-bigquery.canix_raw.bronze_sales_orders`
  WHERE facility_id != '4475'
    AND status IN ('created', 'approved', 'requested')
)

SELECT
  ROUND(COALESCE(m.mtd_revenue, 0), 2) AS mtd_revenue,
  ROUND(COALESCE(m.mtd_paid, 0), 2) AS mtd_paid,
  COALESCE(m.mtd_order_count, 0) AS mtd_order_count,
  ROUND(COALESCE(s.total_sellable_value, 0), 2) AS total_sellable_value,
  COALESCE(s.sellable_package_count, 0) AS sellable_package_count,
  COALESCE(s.sellable_unit_count, 0) AS sellable_unit_count,
  COALESCE(p.pending_order_count, 0) AS pending_order_count,
  ROUND(COALESCE(p.pending_order_value, 0), 2) AS pending_order_value
FROM mtd_sales m
CROSS JOIN sellable_value s
CROSS JOIN pending_fulfillment p
