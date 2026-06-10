CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_sellable_inventory_by_sku` AS
SELECT
  item_name,
  item_category,
  item_subcategory,
  strain_name,
  unit_of_measure,
  COUNT(*)                                                  AS package_count,
  ROUND(SUM(weight), 2)                                     AS total_quantity,
  CASE
    WHEN unit_of_measure = 'Grams' THEN ROUND(SUM(weight) / 453.592, 2)
    WHEN unit_of_measure = 'Pounds' THEN ROUND(SUM(weight), 2)
    ELSE NULL
  END                                                       AS total_weight_lbs,
  CASE
    WHEN unit_of_measure = 'Grams' THEN ROUND(SUM(weight), 2)
    WHEN unit_of_measure = 'Pounds' THEN ROUND(SUM(weight) * 453.592, 2)
    ELSE NULL
  END                                                       AS total_weight_grams,
  MIN(packaged_date)                                        AS oldest_packaged_date,
  MAX(packaged_date)                                        AS newest_packaged_date,
  DATE_DIFF(CURRENT_DATE(), MIN(packaged_date), DAY)        AS days_since_oldest_packaged
FROM `mfny-to-bigquery.canix_raw.bronze_packages`
WHERE is_active = TRUE
  AND status = 'Available To Sell'
  AND weight IS NOT NULL
  AND weight > 0
  AND facility_id != '4475'
  AND item_category NOT IN (
    'Edible (count)',
    'Edible (weight)',
    'Vape Cart (count)',
    'Combined (count)',
    'Combined (weight)',
    'Concentrate (count)',
    'Concentrate (weight)',
    'Tincture (count)',
    'Extract (weight)',
    'Seeds (count)'
  )
GROUP BY
  item_name,
  item_category,
  item_subcategory,
  strain_name,
  unit_of_measure
ORDER BY
  item_category,
  item_name,
  total_quantity DESC
