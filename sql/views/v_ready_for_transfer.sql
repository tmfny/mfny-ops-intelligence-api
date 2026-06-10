CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_ready_for_transfer` AS
SELECT
  item_name,
  item_category,
  strain_name,
  location_name,
  status,
  COUNT(*) as package_count,
  ROUND(SUM(weight), 2) as total_weight_grams,
  ROUND(SUM(weight) / 453.592, 2) as total_weight_lbs,
  ROUND(SUM(quantity), 2) as total_quantity,
  MIN(created_at) as oldest_package,
  MAX(created_at) as newest_package
FROM `mfny-to-bigquery.canix_raw.bronze_packages`
WHERE status = 'Available To Sell'
  AND weight IS NOT NULL
  AND weight > 0
  AND item_category NOT IN ('Wet Whole Plant')
  -- Exclude SANDBOX facility (4475) — not real production data
  AND facility_id != '4475'
GROUP BY
  item_name,
  item_category,
  strain_name,
  location_name,
  status
ORDER BY
  item_category,
  location_name,
  total_weight_grams DESC
