CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_inventory_valuation_by_category` AS
SELECT
  display_category,
  COUNT(*)                                        AS package_count,
  CAST(ROUND(SUM(unit_count), 0) AS INT64)        AS total_units,
  ROUND(SUM(book_value), 2)                       AS book_value
FROM `mfny-to-bigquery.canix_raw.v_inventory_valuation`
GROUP BY display_category
ORDER BY book_value DESC
