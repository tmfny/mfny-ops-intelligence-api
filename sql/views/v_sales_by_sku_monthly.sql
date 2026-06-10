CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_sales_by_sku_monthly` AS
WITH
order_line_items AS (
  -- One row per (sales order, line item).
  -- weight_unit in line items is the numeric unit ID. weight_unit_name is the human string.
  SELECT
    so.id AS sales_order_id,
    so.facility_id,
    so.status,
    COALESCE(
      CAST(so.delivery_date AS TIMESTAMP),
      so.created_at
    ) AS sale_date,
    JSON_EXTRACT_SCALAR(item_json, '$.item.id') AS item_id,
    JSON_EXTRACT_SCALAR(item_json, '$.item.name') AS item_name,
    JSON_EXTRACT_SCALAR(item_json, '$.item.sku') AS item_sku,
    SAFE_CAST(JSON_EXTRACT_SCALAR(item_json, '$.weight') AS FLOAT64) AS units,
    JSON_EXTRACT_SCALAR(item_json, '$.weight_unit_name') AS weight_unit,
    SAFE_CAST(JSON_EXTRACT_SCALAR(item_json, '$.total_price') AS FLOAT64) AS revenue
  FROM `mfny-to-bigquery.canix_raw.bronze_sales_orders` so,
       UNNEST(JSON_EXTRACT_ARRAY(so._raw_json, '$.contents')) AS item_json
  WHERE so.facility_id = '4510'
),

filtered_line_items AS (
  -- Apply filters: Each-only sales, valid quantities, within 12-month trailing window.
  SELECT
    sales_order_id,
    sale_date,
    item_id,
    item_name,
    item_sku,
    units,
    revenue,
    DATE_TRUNC(DATE(sale_date), MONTH) AS sale_month
  FROM order_line_items
  WHERE weight_unit = 'Each'
    AND item_id IS NOT NULL
    AND item_name IS NOT NULL  -- required for the name-based grouping
    AND units IS NOT NULL AND units > 0
    AND sale_date IS NOT NULL
    AND DATE(sale_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
)

-- Group by item_name to deduplicate Canix's same-product-different-item_id situation.
-- Preserve underlying item_ids as an array for traceability ("which Canix records
-- contributed to this row").
SELECT
  item_name,
  ARRAY_AGG(DISTINCT item_id ORDER BY item_id) AS underlying_item_ids,
  MAX(item_sku) AS item_sku,
  sale_month,
  COUNT(DISTINCT sales_order_id) AS order_count,
  SUM(units) AS units_sold,
  ROUND(SUM(revenue), 2) AS revenue
FROM filtered_line_items
GROUP BY item_name, sale_month
ORDER BY sale_month DESC, units_sold DESC
