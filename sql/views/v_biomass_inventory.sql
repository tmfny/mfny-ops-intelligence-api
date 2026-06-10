CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_biomass_inventory` AS
SELECT
  item_name,
  item_category,
  strain_name,
  location_name,
  status,
  COUNT(*) as package_count,
  ROUND(SUM(weight), 2) as total_weight_grams,
  ROUND(SUM(weight) / 453.592, 2) as total_weight_lbs,
  MIN(created_at) as oldest_package_date,
  MAX(created_at) as newest_package_date
FROM `mfny-to-bigquery.canix_raw.bronze_packages`
WHERE
  status = 'Available To Sell'
  AND weight IS NOT NULL
  AND weight > 0
  AND LOWER(item_category) IN (
    'wet whole plant',
    'dry whole plant',
    'biomass',
    'trim',
    'fresh frozen',
    'shake',
    'hemp biomass'
  )
  -- Exclude SANDBOX facility (4475) — not real production data
  AND facility_id != '4475'
GROUP BY
  item_name,
  item_category,
  strain_name,
  location_name,
  status
ORDER BY
  total_weight_grams DESC
