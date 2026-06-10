CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_sellable_inventory_by_category` AS
SELECT
  p.item_category,
  COUNT(*) AS package_count,
  SUM(p.weight) AS total_units,
  COUNT(DISTINCT p.item_subcategory) AS distinct_subcategories,
  COUNT(DISTINCT p.item_name) AS distinct_skus,
  COUNT(DISTINCT p.strain_name) AS distinct_strains,
  ROUND(SUM(p.weight * COALESCE(dp.wholesale_price_per_unit, 0)), 2) AS estimated_value
FROM `mfny-to-bigquery.canix_raw.bronze_packages` p
LEFT JOIN `mfny-to-bigquery.canix_raw.dim_wholesale_prices` dp
  ON p.item_subcategory = dp.item_subcategory
WHERE p.facility_id = '4510'
  AND p.status = 'Available To Sell'
GROUP BY p.item_category
ORDER BY estimated_value DESC
